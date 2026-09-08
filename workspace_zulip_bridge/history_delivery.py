"""Publish one immutable, completed history batch through the private API."""

import contextlib
import json
import re
import threading
import time
import urllib.parse
import uuid

import httpx

from workspace_zulip_bridge import (
    file_api,
    history,
    history_configuration,
    history_failure_reports,
    zulip_adapter,
)

PATH = "/v1/history-imports"
MAX_SOURCES = 512
ADMISSION_COOLDOWN_SECONDS = 5
ADMISSION_COOLDOWN_MAX_SECONDS = 60
# These failures describe the request/bytes/path, not observer credentials.
PERMANENT_FILE_ERRORS = frozenset({
    "invalid_provider_file_url", "invalid_provider_file_length",
    "history_file_account_not_assigned", "invalid_history_file_request",
})



def retryable_database_error(error):
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if getattr(error, "sqlstate", None) in {"55P03", "57014", "40001", "40P01"}:
            return True
        if getattr(error, "code", None) in {"55P03", "57014", "40001", "40P01"}:
            return True
        error = error.__cause__ or error.__context__
    return False


def sources_for(session, project_uuid, provider_realm_uuid):
    return session.execute(
        """SELECT account.resource_uuid::text AS account_uuid,
                  account.generation AS account_generation,
                  assignment.resource_uuid::text AS chat_uuid,
                  assignment.generation AS assignment_generation, job.state
           FROM desired_resources AS account
           JOIN desired_resources AS assignment
             ON assignment.resource_type = 'external_chat_assignment'
            AND assignment.body->>'external_account_uuid' = account.resource_uuid::text
            AND NOT assignment.deleted AND COALESCE((assignment.body->>'selected')::boolean, true)
           JOIN zulip_event_cursors AS cursor ON cursor.account_uuid = account.resource_uuid
            AND cursor.provider_account_generation = account.generation
           LEFT JOIN zulip_backfill_jobs AS job ON job.account_uuid = account.resource_uuid
            AND job.provider_chat_key = assignment.body->'provider_chat'->>'provider_chat_key'
           WHERE account.resource_type = 'external_account' AND NOT account.deleted
             AND COALESCE((account.body->>'synchronization_enabled')::boolean, false)
             AND assignment.body->>'project_id' = %s AND cursor.provider_realm_uuid = %s
           ORDER BY assignment.resource_uuid LIMIT %s""",
        (str(project_uuid), provider_realm_uuid, MAX_SOURCES + 1),
    ).fetchall()


def claim(store, *, allow_admission=True, prefer_admission=False):
    if not store.provider_is_enabled("zulip"):
        return None
    with store.session() as session:
        candidate = session.execute(
            """SELECT batch.project_uuid,batch.provider_realm_uuid,batch.from_id
               FROM zulip_history_batches AS batch
               JOIN zulip_history_scopes AS scope USING(project_uuid,provider_realm_uuid)
               WHERE NOT scope.directory_pending AND batch.import_status='pending'
                 AND batch.import_retry_at<=now()
                 AND (batch.import_lease_until IS NULL OR batch.import_lease_until<=now())
                 AND (%s OR batch.import_uuid IS NOT NULL)
               ORDER BY CASE WHEN batch.import_uuid IS NULL THEN %s ELSE %s END,
                        batch.import_retry_at,batch.project_uuid,batch.provider_realm_uuid,batch.from_id
               LIMIT 1""",
            (
                allow_admission,
                0 if prefer_admission else 1,
                1 if prefer_admission else 0,
            ),
        ).fetchone()
        if candidate is None:
            return None
        scope = (
            candidate["project_uuid"],
            candidate["provider_realm_uuid"],
            candidate["from_id"],
        )
        configuration = session.execute(
            "SELECT * FROM zulip_history_scopes WHERE project_uuid=%s AND provider_realm_uuid=%s FOR SHARE",
            scope[:2],
        ).fetchone()
        if configuration["directory_pending"] or configuration[
            "fingerprint"
        ] != store.history_configuration_fingerprint(session, *scope[:2]):
            return None
        row = session.execute(
            """SELECT * FROM zulip_history_batches WHERE project_uuid=%s AND provider_realm_uuid=%s AND from_id=%s
                 AND import_status='pending' AND (import_lease_until IS NULL OR import_lease_until<=now())
               FOR UPDATE SKIP LOCKED""",
            scope,
        ).fetchone()
        if row is None:
            return None
        sources = sources_for(session, *scope[:2])
        oversized = len(sources) > MAX_SOURCES
        if not oversized and (
            not sources or any(source["state"] != "complete" for source in sources)
        ):
            session.execute(
                "UPDATE zulip_history_batches SET import_retry_at=now()+interval '2 seconds' WHERE project_uuid=%s AND provider_realm_uuid=%s AND from_id=%s",
                scope,
            )
            return None
        sources = [
            {key: value for key, value in dict(source).items() if key != "state"}
            for source in sources
        ]
        sources_hash = history.digest({"sources": sources})
        lease = uuid.uuid4()
        session.execute(
            """UPDATE zulip_history_batches SET import_lease = %s,
                       import_lease_until = now() + interval '120 seconds',
                       import_mapping_cursor = CASE WHEN import_sources_hash = %s THEN import_mapping_cursor ELSE '{}'::jsonb END,
                       import_sources_hash = %s,
                       import_uuid = CASE WHEN import_sources_hash = %s THEN import_uuid END
               WHERE project_uuid = %s AND provider_realm_uuid = %s AND from_id = %s""",
            (lease, sources_hash, sources_hash, sources_hash, *scope),
        )
        import_uuid = (
            row["import_uuid"]
            if row["import_sources_hash"] == sources_hash
            else None
        )
        if not allow_admission and import_uuid is None:
            # A changed source set invalidates its old receipt. During capacity
            # backpressure it must not turn this active-only claim into a POST.
            session.execute(
                """UPDATE zulip_history_batches SET import_lease=NULL,
                           import_lease_until=NULL
                   WHERE project_uuid=%s AND provider_realm_uuid=%s AND from_id=%s
                     AND import_lease=%s""",
                (*scope, lease),
            )
            return None
        return {
            "validation_error": "history_source_limit_exceeded" if oversized else None,
            "mapping_cursor": row["import_mapping_cursor"]
            if row["import_sources_hash"] == sources_hash
            else {},
            "scope": scope,
            "lease": lease,
            "generation": configuration["generation"],
            "import_uuid": import_uuid,
            "envelope": {
                "schema_version": 1,
                "project_uuid": str(scope[0]),
                "provider_realm_uuid": str(scope[1]),
                "generation": configuration["generation"],
                "sources": sources,
                "batch": row["body"],
            },
        }


def current(store, item):
    if not store.provider_is_enabled("zulip"):
        return False
    with store.session() as session:
        row = session.execute(
            """SELECT 1 FROM zulip_history_batches AS batch
               JOIN zulip_history_scopes AS config ON config.project_uuid=batch.project_uuid
                AND config.provider_realm_uuid=batch.provider_realm_uuid AND NOT config.directory_pending AND config.generation = %s
               WHERE batch.project_uuid = %s AND batch.provider_realm_uuid = %s AND batch.from_id = %s
                 AND batch.import_lease = %s AND batch.import_lease_until > now()""",
            (item["generation"], *item["scope"], item["lease"]),
        ).fetchone()
        if row is None:
            return False
        sources = sources_for(session, *item["scope"][:2])
        configuration = session.execute(
            "SELECT generation,fingerprint,directory_pending FROM zulip_history_scopes WHERE project_uuid=%s AND provider_realm_uuid=%s",
            item["scope"][:2],
        ).fetchone()
        if configuration["generation"] != item["generation"] or configuration[
            "fingerprint"
        ] != store.history_configuration_fingerprint(session, *item["scope"][:2]):
            return False
        return item["envelope"]["sources"] == [
            {key: value for key, value in dict(source).items() if key != "state"}
            for source in sources
        ]


LEASE_RENEW_SECONDS = 30


def renew_lease(store, item):
    if not current(store, item):
        return False
    with store.session() as session:
        session.execute("SET LOCAL lock_timeout = '50ms'")
        session.execute("SET LOCAL statement_timeout = '500ms'")
        return (
            session.execute(
                """UPDATE zulip_history_batches SET import_lease_until = now() + interval '120 seconds'
               WHERE project_uuid = %s AND provider_realm_uuid = %s AND from_id = %s
                 AND import_lease = %s AND import_lease_until > now() RETURNING 1""",
                (*item["scope"], item["lease"]),
            ).fetchone()
            is not None
        )


@contextlib.contextmanager
def keep_lease_alive(store, item):
    stopped = threading.Event()

    def heartbeat():
        while not stopped.wait(LEASE_RENEW_SECONDS):
            try:
                if not renew_lease(store, item):
                    return
            except Exception:
                # Authority is checked again before upload/write. A transient
                # renewal failure must not terminate the live delivery thread.
                continue

    thread = threading.Thread(
        target=heartbeat, name="history-publish-lease", daemon=True
    )
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join(timeout=1)


def safe_error_code(value):
    if isinstance(value, dict):
        value = value.get("code")
    if value is None:
        return None
    if isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,127}", value):
        return value
    return "history_import_failed"


def response_error_code(response):
    try:
        body = response.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    return safe_error_code(body.get("error"))


def admission_retry_after(response):
    try:
        delay = int(response.headers.get("Retry-After", ""))
    except (TypeError, ValueError):
        delay = ADMISSION_COOLDOWN_SECONDS
    return max(1, min(delay, ADMISSION_COOLDOWN_MAX_SECONDS))


def release(
    store, item, *, status="pending", job_uuid=None, error=None, delay=1, reset=False
):
    with store.session() as session:
        return (
            session.execute(
                """UPDATE zulip_history_batches SET import_status = %s,
                       import_uuid = CASE WHEN %s THEN NULL ELSE COALESCE(%s, import_uuid) END,
                       import_mapping_cursor = CASE WHEN %s THEN '{}'::jsonb ELSE import_mapping_cursor END,
                       import_error = %s,
                       import_retry_at = now() + %s * interval '1 second',
                       import_lease = NULL, import_lease_until = NULL
               WHERE project_uuid = %s AND provider_realm_uuid = %s AND from_id = %s
                 AND import_lease = %s RETURNING 1""",
                (
                    status,
                    reset,
                    job_uuid,
                    reset,
                    safe_error_code(error),
                    delay,
                    *item["scope"],
                    item["lease"],
                ),
            ).fetchone()
            is not None
        )


class HistoryPublisher:
    def __init__(
        self,
        store,
        file_client,
        adapters,
        account_report=None,
        record_stat=None,
        clock=None,
    ):
        self.store = store
        self.file_client = file_client
        self.adapters = adapters
        self.account_report = account_report
        self.record_stat = record_stat
        self.clock = clock or time.monotonic
        self.lock = threading.Lock()
        self.probe_after = 0.0
        self.admission_probe_after = 0.0
        self.admission_probe_due = False
        self.supported = False

    def _record_stat(self, name):
        if self.record_stat is not None:
            self.record_stat(name)

    def fail(self, item, code):
        code = safe_error_code(code) or "history_import_failed"
        with self.store.transaction() as session:
            session.execute("SET LOCAL lock_timeout = '50ms'")
            session.execute("SET LOCAL statement_timeout = '500ms'")
            session.execute("LOCK TABLE desired_resources IN SHARE MODE")
            scope = session.execute(
                """SELECT 1 FROM zulip_history_scopes
                   WHERE project_uuid=%s AND provider_realm_uuid=%s FOR SHARE""",
                item["scope"][:2],
            ).fetchone()
            if scope is None or not current(self.store, item):
                # A newer configuration may already have retired this receipt.
                # Release only our own lease without marking the new scope bad.
                release(self.store, item)
                return False
            if not release(
                self.store,
                item,
                status="failed",
                job_uuid=item["import_uuid"],
                error=code,
            ):
                return False
            self.store.mark_health(
                history_configuration.health_component(item["scope"]),
                "degraded",
                code,
            )
            history_failure_reports.record(session, item, code)
        return True

    def run_once(self):
        if not self.lock.acquire(blocking=False):
            return False
        try:
            return self._run_once()
        except Exception as error:
            # A contended claim/release can safely wait for the next iteration
            # (or lease expiry), without terminating the live delivery process.
            if not retryable_database_error(error):
                raise
            return False
        finally:
            self.lock.release()

    def _run_once(self):
        try:
            reported = history_failure_reports.flush_once(
                self.store, self.account_report
            )
        except Exception as error:
            if not retryable_database_error(error):
                raise
            reported = False
        client = self.file_client.client
        if not self.supported:
            if self.clock() < self.probe_after:
                return reported
            self.probe_after = self.clock() + 60
            try:
                response = client.get(PATH, headers={"Content-Length": "0"})
                self.supported = (
                    response.status_code == 200
                    and response.json().get("schema_version") == 1
                )
            except (httpx.HTTPError, ValueError):
                return reported
            if not self.supported:
                return reported
        if self.clock() < self.admission_probe_after:
            item = claim(self.store, allow_admission=False)
        elif self.admission_probe_due:
            # Default selection always gives active receipts the first turn.
            # A single bounded probe after an active poll prevents those same
            # receipts from monopolizing every live-throttled history quantum
            # when the backend still has a free admission slot.
            self.admission_probe_due = False
            item = claim(self.store, prefer_admission=True)
        else:
            item = claim(self.store)
        if item is None:
            return reported
        if item.get("validation_error"):
            self.fail(item, item["validation_error"])
            return reported
        with keep_lease_alive(self.store, item):
            return self._publish(client, item) or reported

    def _publish(self, client, item):
        try:
            if not current(self.store, item):
                release(self.store, item)
                return False
            registered = item["import_uuid"] is not None
            if not registered:
                self._record_stat("history_admission_attempts")
                response = client.post(
                    PATH,
                    content=json.dumps(item["envelope"], ensure_ascii=False).encode(),
                    headers={"Content-Type": "application/json"},
                    timeout=60,
                )
            else:
                self._record_stat("history_active_polls")
                response = client.get(
                    f"{PATH}/{item['import_uuid']}", headers={"Content-Length": "0"}
                )
            if registered and response.status_code in {404, 409}:
                self.admission_probe_after = 0.0
                self.admission_probe_due = True
                release(self.store, item, error="history_receipt_changed", reset=True)
                return False
            if (
                not registered
                and response.status_code == 429
                and response_error_code(response) == "history_import_busy"
            ):
                delay = admission_retry_after(response)
                self.admission_probe_after = self.clock() + delay
                self._record_stat("history_admission_busy")
                release(
                    self.store,
                    item,
                    error="history_import_busy",
                    delay=delay,
                )
                return False
            if not registered and response.status_code in {400, 413, 422}:
                self.fail(item, "history_batch_rejected")
                return False
            response.raise_for_status()
            state = response.json()
            if registered:
                self.admission_probe_due = True
            item["import_uuid"] = uuid.UUID(state["uuid"])
            if state["status"] == "failed":
                if registered:
                    self.admission_probe_after = 0.0
                self.fail(item, state["safe_error"])
                return False
            if state["status"] == "superseded":
                if registered:
                    self.admission_probe_after = 0.0
                release(
                    self.store, item, error="history_receipt_superseded", reset=True
                )
                return False
            if state["status"] == "complete":
                if registered:
                    self.admission_probe_after = 0.0
                if current(self.store, item):
                    self.sync_references(client, item)
                release(self.store, item, job_uuid=item["import_uuid"])
                return True
            # One file per quantum bounds network/CPU work and lets live lanes run.
            files = state.get("files")
            if not isinstance(files, list):
                raise zulip_adapter.ZulipOperationError(
                    "invalid_history_file_request", retryable=False
                )
            if files:
                self._send_file(client, item, files[0])
            release(
                self.store,
                item,
                job_uuid=item["import_uuid"],
                error=state["safe_error"],
                delay=1,
            )
            return True
        except zulip_adapter.ZulipOperationError as error:
            if not error.retryable and error.code in PERMANENT_FILE_ERRORS:
                self.fail(item, error.code)
            else:
                release(self.store, item, job_uuid=item["import_uuid"],
                        error="history_delivery_unavailable", delay=5)
            return False
        except (
            httpx.HTTPError,
            ValueError,
            KeyError,
        ):
            release(
                self.store,
                item,
                job_uuid=item["import_uuid"],
                error="history_delivery_unavailable",
                delay=5,
            )
            return False

        except Exception as error:
            if not retryable_database_error(error):
                raise
            release(
                self.store,
                item,
                job_uuid=item["import_uuid"],
                error="history_database_contention",
                delay=1,
            )
            return False

    def sync_references(self, client, item):
        accounts = sorted(
            {source["account_uuid"] for source in item["envelope"]["sources"]}
        )
        cursor = item["mapping_cursor"] or {"account": 0, "kind": "users", "after": "0"}
        account = accounts[cursor["account"]]
        response = client.get(
            f"{PATH}/{item['import_uuid']}/mappings/{account}/{cursor['kind']}/{cursor['after']}",
            headers={"Content-Length": "0"},
        )
        response.raise_for_status()
        page = response.json()
        if not current(self.store, item):
            return
        complete = False
        if page["next_cursor"] is not None:
            cursor = {**cursor, "after": page["next_cursor"]}
        elif cursor["kind"] == "users":
            cursor = {
                **cursor,
                "kind": "messages",
                "after": str(item["envelope"]["batch"]["from_id"] - 1),
            }
        else:
            cursor = {"account": cursor["account"] + 1, "kind": "users", "after": "0"}
            complete = cursor["account"] == len(accounts)
        save_references(self.store, item, account, page["mappings"], cursor, complete)

    def _send_file(self, client, item, request):
        try:
            if not isinstance(request, dict) or not isinstance(request.get("uuid"), str):
                raise ValueError
            file_uuid = uuid.UUID(request["uuid"])
        except (ValueError, TypeError, AttributeError):
            raise zulip_adapter.ZulipOperationError(
                "invalid_history_file_request", retryable=False
            ) from None
        # UUID identity, rather than its spelling, fences observer selection.
        # Keep canonical keys for the adapter registry after deduplication.
        allowed = {
            str(uuid.UUID(source["account_uuid"]))
            for source in item["envelope"]["sources"]
        }
        requested = request.get("account_uuids")
        try:
            if not isinstance(requested, list) or not requested:
                raise ValueError
            accounts = list(
                dict.fromkeys(str(uuid.UUID(value)) for value in requested)
            )
            if not set(accounts).issubset(allowed):
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise zulip_adapter.ZulipOperationError(
                "history_file_account_not_assigned", retryable=False
            ) from None
        path = zulip_adapter.validated_upload_path(request.get("source_path"))
        downloaded = None
        retry_error = None
        permanent_error = None
        for account_uuid in accounts:
            if not current(self.store, item):
                return
            try:
                downloaded = self.adapters(account_uuid).download_file(
                    path,
                    max_bytes=self.store.effective_file_limit(file_api.MAX_FILE_BYTES),
                )
                break
            except zulip_adapter.ZulipOperationError as error:
                if not error.retryable and error.code in {
                    "provider_file_transfer_disabled",
                    "provider_file_too_large",
                }:
                    retry_error = permanent_error = None
                    break
                if not error.retryable and error.code in PERMANENT_FILE_ERRORS:
                    permanent_error = error
                elif error.retryable or error.code != "provider_file_unavailable":
                    retry_error = error
        if downloaded is None:
            # An observer with a temporary/auth failure may still provide valid
            # bytes later; a different observer's malformed response cannot
            # turn that retry into a permanent failure.
            if retry_error is not None:
                raise retry_error
            if permanent_error is not None:
                raise permanent_error
        if not current(self.store, item):
            return
        if downloaded is not None:
            limit = self.store.effective_file_limit(file_api.MAX_FILE_BYTES)
            if limit <= 0 or len(downloaded.content) > limit:
                downloaded = None
        target = f"{PATH}/{item['import_uuid']}/files/{file_uuid}"
        if downloaded is None:
            response = client.post(target + "/unavailable", content=b"")
        else:
            response = client.put(
                target,
                content=downloaded.content,
                timeout=60,
                headers={
                    "Content-Type": downloaded.content_type,
                    "X-File-Name": urllib.parse.quote(downloaded.name),
                },
            )
        response.raise_for_status()
        self._record_stat(
            "history_files_uploaded"
            if downloaded is not None
            else "history_files_unavailable"
        )


def save_references(store, item, account_uuid, mappings, cursor, complete):
    """Install missing routing records and their checkpoint atomically."""
    encoded = json.dumps(mappings)
    encoded_cursor = json.dumps(cursor)
    with store.session() as session:
        session.execute("SET LOCAL lock_timeout = '50ms'")
        session.execute("SET LOCAL statement_timeout = '500ms'")
        # Freeze source membership as well as existing rows: a concurrent new
        # assignment must not slip between validation and mapping installation.
        # This bridge-local lock spans only the bounded SQL receipt write.
        session.execute("LOCK TABLE desired_resources IN SHARE MODE")
        policy = session.execute(
            """SELECT body FROM desired_resources WHERE resource_type = 'external_provider_policy'
                 AND NOT deleted AND body->>'provider_kind' = 'zulip'
               ORDER BY generation DESC LIMIT 1 FOR SHARE"""
        ).fetchone()
        if (
            policy is None
            or policy["body"].get("enabled") is not True
            or policy["body"].get("emergency_suspended") is True
        ):
            return
        config = session.execute(
            "SELECT generation,fingerprint,directory_pending FROM zulip_history_scopes WHERE project_uuid=%s AND provider_realm_uuid=%s FOR SHARE",
            item["scope"][:2],
        ).fetchone()
        if (
            config["directory_pending"]
            or config["generation"] != item["generation"]
            or config["fingerprint"]
            != store.history_configuration_fingerprint(session, *item["scope"][:2])
        ):
            return
        sources = sources_for(session, *item["scope"][:2])
        if item["envelope"]["sources"] != [
            {k: v for k, v in dict(source).items() if k != "state"}
            for source in sources
        ]:
            return
        batch = session.execute(
            """SELECT 1 FROM zulip_history_batches WHERE project_uuid = %s
                 AND provider_realm_uuid = %s AND from_id = %s AND import_lease = %s
                 AND import_lease_until > now() FOR UPDATE""",
            (*item["scope"], item["lease"]),
        ).fetchone()
        if batch is None:
            return
        session.execute(
            """INSERT INTO provider_mappings
               (account_uuid, entity_kind, provider_id, workspace_uuid, metadata)
               SELECT %s, data.kind, data.provider_id, data.workspace_uuid, data.metadata
               FROM jsonb_to_recordset(%s::jsonb)
                   AS data(kind text, provider_id text, workspace_uuid uuid, metadata jsonb)
               ON CONFLICT DO NOTHING""",
            (account_uuid, encoded),
        )
        # An existing live row keeps its content/tombstone. A displaced local
        # placeholder may still need an alias for the server's canonical UUID.
        session.execute(
            """INSERT INTO provider_mapping_aliases
               (account_uuid, entity_kind, provider_id, workspace_uuid, metadata)
               SELECT %s, data.kind, data.provider_id, data.workspace_uuid, data.metadata
               FROM jsonb_to_recordset(%s::jsonb)
                   AS data(kind text, provider_id text, workspace_uuid uuid, metadata jsonb)
               JOIN provider_mappings AS existing ON existing.account_uuid = %s
                AND existing.entity_kind = data.kind AND existing.provider_id = data.provider_id
                AND existing.workspace_uuid <> data.workspace_uuid AND NOT existing.deleted
               ON CONFLICT DO NOTHING""",
            (account_uuid, encoded, account_uuid),
        )
        session.execute(
            """UPDATE zulip_history_batches SET import_mapping_cursor = %s::jsonb,
                       import_status = %s, import_uuid = %s, import_error = NULL,
                       import_lease = NULL, import_lease_until = NULL,
                       import_retry_at = now() + interval '100 milliseconds'
               WHERE project_uuid = %s AND provider_realm_uuid = %s AND from_id = %s
                 AND import_lease = %s""",
            (
                encoded_cursor,
                "complete" if complete else "pending",
                item["import_uuid"],
                *item["scope"],
                item["lease"],
            ),
        )
