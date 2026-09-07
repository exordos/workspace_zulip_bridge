"""Scoped capture invalidation and bounded directory-only refreshes."""

import json

from workspace_zulip_bridge import history, history_failure_reports, zulip_adapter


def sources(session, project_uuid=None, provider_realm_uuid=None):
    return [
        dict(row)
        for row in session.execute(
            """SELECT assignment.body->>'project_id' AS project_uuid,
                  cursor.provider_realm_uuid::text AS provider_realm_uuid,
                  account.resource_uuid::text AS account_uuid,
                  account.generation AS account_generation,
                  assignment.resource_uuid::text AS chat_uuid,
                  assignment.generation AS assignment_generation,
                  assignment.body->'provider_chat'->>'provider_chat_key' AS provider_chat_key,
                  assignment.body->>'history_depth' AS history_depth,
                  cursor.provider_owner_user_id
           FROM desired_resources AS account
           JOIN desired_resources AS assignment
             ON assignment.resource_type='external_chat_assignment'
            AND assignment.body->>'external_account_uuid'=account.resource_uuid::text
            AND NOT assignment.deleted AND COALESCE((assignment.body->>'selected')::boolean,true)
           JOIN zulip_event_cursors AS cursor ON cursor.account_uuid=account.resource_uuid
            AND cursor.provider_account_generation=account.generation
           WHERE account.resource_type='external_account' AND NOT account.deleted
             AND COALESCE((account.body->>'synchronization_enabled')::boolean,false)
             AND cursor.provider_realm_uuid IS NOT NULL
             AND (%s::uuid IS NULL OR assignment.body->>'project_id'=%s)
             AND (%s::uuid IS NULL OR cursor.provider_realm_uuid=%s::uuid)
           ORDER BY assignment.resource_uuid""",
            (project_uuid, str(project_uuid), provider_realm_uuid, provider_realm_uuid),
        ).fetchall()
    ]


def fingerprint(rows):
    # Catalog-only assignment revisions do not change the captured observations.
    return history.digest(
        {
            "capture_version": 3,
            "sources": [
                {
                    key: value
                    for key, value in row.items()
                    if key != "assignment_generation"
                }
                for row in rows
            ],
        }
    )


def next_generation(session):
    return session.execute(
        "UPDATE zulip_history_configuration SET generation=generation+1 WHERE singleton RETURNING generation"
    ).fetchone()["generation"]


def reconcile(store, session):
    # The same order is used by capture and receipt writes: desired state,
    # scope, capture job, batch. None of these locks spans a provider request.
    session.execute("LOCK TABLE desired_resources IN SHARE MODE")
    legacy = session.execute(
        "SELECT fingerprint,generation FROM zulip_history_configuration WHERE singleton FOR UPDATE"
    ).fetchone()
    legacy_fingerprint = store.history_configuration_fingerprint(session)
    grouped = {}
    for row in sources(session):
        grouped.setdefault(
            (row["project_uuid"], row["provider_realm_uuid"]), []
        ).append(row)
    dirty_realms = {
        str(row["provider_realm_uuid"]): row["generation"]
        for row in session.execute(
            "SELECT provider_realm_uuid,generation FROM zulip_history_directory_revisions"
        ).fetchall()
    }
    existing = {
        (str(row["project_uuid"]), str(row["provider_realm_uuid"])): dict(row)
        for row in session.execute(
            "SELECT project_uuid,provider_realm_uuid,fingerprint,sources,directory_revision,directory_ack_revision,directory_pending FROM zulip_history_scopes ORDER BY project_uuid,provider_realm_uuid"
        ).fetchall()
    }
    for row in session.execute(
        "SELECT DISTINCT project_uuid,provider_realm_uuid FROM zulip_history_batches"
    ).fetchall():
        grouped.setdefault(
            (str(row["project_uuid"]), str(row["provider_realm_uuid"])), []
        )
    for scope in sorted(set(grouped) | set(existing)):
        rows = grouped.get(scope, [])
        value = fingerprint(rows)
        old = existing.get(scope)
        if old is not None and old["fingerprint"] == value:
            if old["sources"] != rows:
                generation = next_generation(session)
                session.execute(
                    "UPDATE zulip_history_scopes SET sources=%s::jsonb,generation=%s WHERE project_uuid=%s AND provider_realm_uuid=%s",
                    (json.dumps(rows), generation, *scope),
                )
                history_failure_reports.carry_directory(session, generation)
                history_failure_reports.carry_capture(session, generation)
                # Catalog revisions preserve the captured observations, but
                # backend receipts must use the current assignment generations.
                session.execute(
                    """UPDATE zulip_history_batches SET import_status='pending',
                         import_uuid=NULL,import_mapping_cursor='{}'::jsonb,
                         import_sources_hash=NULL,import_lease=NULL,
                         import_lease_until=NULL,import_error=NULL,
                         import_retry_at=now(),updated_at=now()
                       WHERE project_uuid=%s AND provider_realm_uuid=%s""",
                    scope,
                )
                session.execute(
                    "DELETE FROM bridge_health WHERE component=%s",
                    (health_component(scope),),
                )
            continue
        preserve = old is None and legacy["fingerprint"] == legacy_fingerprint
        generation = legacy["generation"] if preserve else next_generation(session)
        revision = dirty_realms.get(scope[1], old["directory_revision"] if old else 0)
        acknowledged = old["directory_ack_revision"] if old else 0
        pending = revision != acknowledged or bool(old and old["directory_pending"])
        session.execute(
            """INSERT INTO zulip_history_scopes(project_uuid,provider_realm_uuid,generation,fingerprint,sources,directory_pending,directory_revision,directory_ack_revision)
               VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s)
               ON CONFLICT(project_uuid,provider_realm_uuid) DO UPDATE SET
                 generation=EXCLUDED.generation,fingerprint=EXCLUDED.fingerprint,sources=EXCLUDED.sources,
                 directory_pending=EXCLUDED.directory_pending,directory_revision=EXCLUDED.directory_revision,directory_ack_revision=EXCLUDED.directory_ack_revision,directory_account_cursor=0,directory_cursor=0,directory_users='[]'::jsonb,directory_retry_at=now(),directory_write_attempts=0""",
            (*scope, generation, value, json.dumps(rows), pending, revision, acknowledged),
        )
        if not preserve:
            affected = (old["sources"] if old else []) + rows
            session.execute(
                """UPDATE zulip_backfill_jobs AS job SET state='cancelled',next_anchor=NULL,cutoff_at=NULL,
                       lease_until=NULL,available_at=now(),retry_count=0,last_error_code=NULL,
                       capture_timeout_pending=false,updated_at=now()
                   WHERE EXISTS (SELECT 1 FROM jsonb_to_recordset(%s::jsonb)
                     AS source(account_uuid uuid,provider_chat_key text)
                     WHERE source.account_uuid=job.account_uuid AND source.provider_chat_key=job.provider_chat_key)""",
                (json.dumps(affected),),
            )
            session.execute(
                "DELETE FROM zulip_history_failure_reports WHERE project_uuid=%s AND provider_realm_uuid=%s",
                scope,
            )
            session.execute(
                "DELETE FROM zulip_history_batches WHERE project_uuid=%s AND provider_realm_uuid=%s",
                scope,
            )
            session.execute(
                "DELETE FROM bridge_health WHERE component=%s",
                (health_component(scope),),
            )
            session.execute(
                "DELETE FROM bridge_health WHERE component=%s",
                (directory_health_component(scope),),
            )
    session.execute(
        "UPDATE zulip_history_configuration SET fingerprint=%s WHERE singleton",
        (legacy_fingerprint,),
    )


def health_component(scope):
    return f"history_publication:{scope[0]}:{scope[1]}"


def directory_health_component(scope):
    return f"history_directory:{scope[0]}:{scope[1]}"


def invalidate_directory(session, account_uuid):
    generation = next_generation(session)
    session.execute(
        """INSERT INTO zulip_history_directory_revisions(provider_realm_uuid,generation)
           SELECT provider_realm_uuid,%s FROM zulip_event_cursors WHERE account_uuid=%s AND provider_realm_uuid IS NOT NULL
           ON CONFLICT(provider_realm_uuid) DO UPDATE SET generation=EXCLUDED.generation""",
        (generation, account_uuid),
    )
    session.execute(
        """UPDATE zulip_history_scopes SET generation=%s,directory_revision=%s,directory_pending=true,
                   directory_account_cursor=0,directory_cursor=0,directory_users='[]'::jsonb,directory_retry_at=now(),directory_write_attempts=0
           WHERE provider_realm_uuid=(SELECT provider_realm_uuid FROM zulip_event_cursors WHERE account_uuid=%s)""",
        (generation, generation, account_uuid),
    )
    session.execute(
        """DELETE FROM zulip_history_failure_reports AS report
           USING zulip_history_scopes AS scope
           WHERE report.project_uuid=scope.project_uuid AND report.provider_realm_uuid=scope.provider_realm_uuid
             AND scope.generation=%s AND report.safe_error_code=%s""",
        (generation, history_failure_reports.DIRECTORY_TIMEOUT),
    )
    session.execute(
        """DELETE FROM bridge_health AS health USING zulip_history_scopes AS scope
           WHERE scope.generation=%s AND health.component=
             'history_directory:'||scope.project_uuid::text||':'||scope.provider_realm_uuid::text""",
        (generation,),
    )
    history_failure_reports.carry_capture(session, generation)


def with_directory(batch, users):
    # Keep referenced historical identities that no current directory exposes.
    referenced = set()
    for message in batch["messages"]:
        referenced.add(message["sender_id"])
        referenced.update(message.get("recipient_ids", []))
        referenced.update(
            item["user_id"] for item in message["reactions"] + message["access"]
        )
    catalog = {user["id"]: user for user in batch["users"] if user["id"] in referenced}
    catalog.update((user["id"], user) for user in users)
    result = {**batch, "users": [catalog[key] for key in sorted(catalog)]}
    result["hash"] = history.digest(
        {key: value for key, value in result.items() if key != "hash"}
    )
    return result


def refresh_once(store, adapters, account_report=None):
    """One directory request or one batch rewrite, with preparation outside SQL."""
    if not store.provider_is_enabled("zulip"):
        return False
    with store.session() as session:
        row = session.execute(
            "SELECT * FROM zulip_history_scopes WHERE directory_pending AND directory_write_attempts<%s AND directory_retry_at<=now() ORDER BY directory_retry_at,project_uuid,provider_realm_uuid LIMIT 1",
            (history.DIRECTORY_WRITE_MAX_ATTEMPTS,),
        ).fetchone()
        if row is None:
            return False
        scope = (row["project_uuid"], row["provider_realm_uuid"])
        accounts = sorted({source["account_uuid"] for source in row["sources"]})
        cursor = row["directory_account_cursor"]
        batch = None
        if cursor >= len(accounts):
            batch = session.execute(
                "SELECT from_id,body FROM zulip_history_batches WHERE project_uuid=%s AND provider_realm_uuid=%s AND from_id>%s ORDER BY from_id LIMIT 1",
                (*scope, row["directory_cursor"]),
            ).fetchone()
    prepared = None
    if cursor < len(accounts):
        try:
            users = history.user_catalog(
                adapters(accounts[cursor]).history_users(force_refresh=True)
            )
        except (zulip_adapter.ZulipOperationError, ValueError):
            changed = _defer_directory(store, row, "history_directory_unavailable")
            if changed and account_report is not None:
                account_report(
                    accounts[cursor], "degraded", "history_directory_unavailable",
                    expected_generation=next(
                        source["account_generation"] for source in row["sources"]
                        if source["account_uuid"] == accounts[cursor]
                    ),
                )
            return False
        catalog = {user["id"]: user for user in row["directory_users"]}
        catalog.update((user["id"], user) for user in users)
        prepared = json.dumps([catalog[key] for key in sorted(catalog)])
    elif batch is not None:
        prepared = json.dumps(with_directory(batch["body"], row["directory_users"]))
    # dumps uses ASCII escaping, so this character count is the wire byte size.
    # Computing the budget and encoding stay outside the authority locks.
    budget = history.capture_write_budget_ms(len(prepared)) if prepared else 500
    try:
        return _apply_refresh(store, row, scope, accounts, cursor, batch, prepared, budget)
    except Exception as error:
        # Import locally because publication consumes this configuration module.
        from workspace_zulip_bridge import history_delivery
        if not history_delivery.retryable_database_error(error):
            raise
        code = (history_failure_reports.DIRECTORY_TIMEOUT
                if history.is_statement_timeout(error)
                else "history_directory_database_contention")
        _defer_directory(store, row, code)
        return False


def _apply_refresh(store, row, scope, accounts, cursor, batch, prepared, budget):
    with store.session() as session:
        session.execute("SET LOCAL lock_timeout = '50ms'")
        session.execute("SET LOCAL statement_timeout = '500ms'")
        session.execute("LOCK TABLE desired_resources IN SHARE MODE")
        if cursor >= len(accounts) and batch is None:
            # Reconcile/invalidation take the singleton before scope locks.
            # Freeze both new scopes and new realm revisions during final ack.
            session.execute("SELECT generation FROM zulip_history_configuration WHERE singleton FOR UPDATE")
        current = session.execute(
            "SELECT generation,fingerprint,directory_pending,directory_write_attempts,directory_revision,directory_cursor,directory_account_cursor FROM zulip_history_scopes WHERE project_uuid=%s AND provider_realm_uuid=%s FOR UPDATE",
            scope,
        ).fetchone()
        if (
            current is None
            or current["generation"] != row["generation"]
            or not current["directory_pending"]
            or current["directory_write_attempts"] >= history.DIRECTORY_WRITE_MAX_ATTEMPTS
            or current["directory_cursor"] != row["directory_cursor"]
            or current["directory_account_cursor"] != cursor
            or current["fingerprint"] != fingerprint(sources(session, *scope))
        ):
            return False
        if cursor < len(accounts):
            session.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{budget}ms",),
            )
            session.execute(
                "UPDATE zulip_history_scopes SET directory_users=%s::jsonb,directory_account_cursor=directory_account_cursor+1,directory_retry_at=now(),directory_write_attempts=0 WHERE project_uuid=%s AND provider_realm_uuid=%s",
                (prepared, *scope),
            )
            session.execute("SET LOCAL statement_timeout = '500ms'")
        elif batch is not None:
            # Capture holds this same scope lock, so its message merge cannot
            # be overwritten by a directory snapshot prepared before that merge.
            session.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{budget}ms",),
            )
            updated = session.execute(
                """UPDATE zulip_history_batches SET body=%s::jsonb,import_status='pending',
                     import_uuid=NULL,import_mapping_cursor='{}'::jsonb,import_lease=NULL,
                     import_lease_until=NULL,import_error=NULL,import_retry_at=now(),updated_at=now()
                   WHERE project_uuid=%s AND provider_realm_uuid=%s AND from_id=%s AND body->>'hash'=%s RETURNING 1""",
                (prepared, *scope, batch["from_id"], batch["body"]["hash"]),
            ).fetchone()
            session.execute("SET LOCAL statement_timeout = '500ms'")
            if updated is None:
                return False
            session.execute(
                "UPDATE zulip_history_scopes SET directory_cursor=%s,directory_retry_at=now(),directory_write_attempts=0 WHERE project_uuid=%s AND provider_realm_uuid=%s",
                (batch["from_id"], *scope),
            )
        else:
            if session.execute(
                "SELECT 1 FROM zulip_history_batches WHERE project_uuid=%s AND provider_realm_uuid=%s AND from_id>%s LIMIT 1",
                (*scope, row["directory_cursor"]),
            ).fetchone():
                return True
            session.execute(
                "UPDATE zulip_history_scopes SET directory_pending=false,directory_ack_revision=directory_revision,directory_write_attempts=0 WHERE project_uuid=%s AND provider_realm_uuid=%s",
                scope,
            )
            session.execute(
                """DELETE FROM zulip_history_directory_revisions AS revision
                   WHERE provider_realm_uuid=%s AND generation=%s
                     AND NOT EXISTS (
                       SELECT 1 FROM zulip_history_scopes AS other
                       WHERE other.provider_realm_uuid=revision.provider_realm_uuid
                         AND (other.directory_pending OR
                              other.directory_ack_revision<>revision.generation))""",
                (scope[1], current["directory_revision"]),
            )
            session.execute(
                "DELETE FROM bridge_health WHERE component=%s",
                (health_component(scope),),
            )
        session.execute(
            "DELETE FROM bridge_health WHERE component=%s",
            (directory_health_component(scope),),
        )
    return True


def _defer_directory(store, row, code):
    """Record a failed quantum only while its revision and cursors still match."""
    scope = (row["project_uuid"], row["provider_realm_uuid"])
    with store.transaction() as session:
        session.execute("SET LOCAL lock_timeout = '50ms'")
        session.execute("SET LOCAL statement_timeout = '500ms'")
        session.execute("LOCK TABLE desired_resources IN SHARE MODE")
        current = session.execute(
            "SELECT generation,fingerprint,directory_pending,directory_write_attempts,directory_revision,directory_cursor,directory_account_cursor FROM zulip_history_scopes WHERE project_uuid=%s AND provider_realm_uuid=%s FOR UPDATE",
            scope,
        ).fetchone()
        if (current is None or not current["directory_pending"]
            or current["directory_write_attempts"] >= history.DIRECTORY_WRITE_MAX_ATTEMPTS
            or any(current[key] != row[key] for key in (
                "generation", "directory_revision", "directory_account_cursor", "directory_cursor", "fingerprint"))
            or current["fingerprint"] != fingerprint(sources(session, *scope))):
            return False
        attempts = current["directory_write_attempts"] + (code == history_failure_reports.DIRECTORY_TIMEOUT)
        delay = (min(300, 5 * 2 ** (attempts - 1))
                 if code == history_failure_reports.DIRECTORY_TIMEOUT
                 else 30 if code == "history_directory_unavailable" else 1)
        session.execute(
            "UPDATE zulip_history_scopes SET directory_write_attempts=%s,directory_retry_at=now()+%s*interval '1 second' WHERE project_uuid=%s AND provider_realm_uuid=%s",
            (attempts, delay, *scope),
        )
        store.mark_health(directory_health_component(scope), "degraded", code)
        if attempts >= history.DIRECTORY_WRITE_MAX_ATTEMPTS:
            history_failure_reports.record(
                session, {"scope": scope, "generation": current["generation"]}, code
            )
        return True
