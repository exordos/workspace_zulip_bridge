"""Recover only classified missing-base updates without replaying their payloads."""

import copy
import json
import uuid

from workspace_zulip_bridge import canonical, converter, history_delivery, zulip_adapter

ERROR = "provider_message_base_missing"


class RecoveryLeaseExpired(RuntimeError):
    """Rollback staged mapping/outbox writes when the final lease fence fails."""


def remember(session, record_uuid):
    """Commit intent in the same transaction as the immutable record rejection."""
    session.execute(
        """INSERT INTO zulip_missing_message_recovery (
               operation_uuid, record_uuid, account_uuid, account_generation,
               assignment_uuid, assignment_generation, project_uuid,
               provider_realm_uuid, provider_message_id, provider_chat_key,
               provider_topic_id)
           SELECT delivery.operation_uuid, delivery.record_uuid,
                  delivery.account_uuid, delivery.account_generation,
                  delivery.assignment_uuid, delivery.assignment_generation,
                  (delivery.record->>'project_uuid')::uuid,
                  cursor.provider_realm_uuid,
                  delivery.record->'operation'->'provider'->>'entity_id',
                  delivery.record->'operation'->'provider'->>'chat_id',
                  topic.provider_id
           FROM workspace_delivery_outbox AS delivery
           LEFT JOIN zulip_event_cursors AS cursor
             ON cursor.account_uuid=delivery.account_uuid
           LEFT JOIN provider_mappings AS topic
             ON topic.account_uuid=delivery.account_uuid AND topic.entity_kind='topic'
            AND topic.workspace_uuid=(delivery.record->'operation'->'payload'->>'topic_uuid')::uuid
            AND NOT topic.deleted
           WHERE delivery.record_uuid=%s AND delivery.submission_state='rejected'
             AND delivery.submission_error_code=%s
             AND delivery.record->'operation'->>'kind'='message.update'
             AND NOT (delivery.record->'operation'->'payload' ? 'payload')
           ON CONFLICT (operation_uuid) DO NOTHING""",
        (record_uuid, ERROR),
    )


def save_references(session, item, account_uuid, mappings):
    """Keep the server's actual message placement even if a local mapping exists."""
    messages = [mapping for mapping in mappings if mapping["kind"] == "message"]
    if not messages:
        return
    session.execute(
        """UPDATE zulip_missing_message_recovery AS recovery
           SET reference=jsonb_build_object('workspace_uuid', incoming.workspace_uuid,
                                           'metadata', incoming.metadata),
               updated_at=now()
           FROM jsonb_to_recordset(%s::jsonb)
               AS incoming(provider_id text, workspace_uuid uuid, metadata jsonb)
           WHERE recovery.account_uuid=%s AND recovery.provider_message_id=incoming.provider_id
             AND recovery.project_uuid=%s AND recovery.provider_realm_uuid=%s
             AND recovery.state='pending'""",
        (json.dumps(messages), account_uuid, *item["scope"][:2]),
    )


def claim(store):
    with store.session() as session:
        row = session.execute(
            """SELECT * FROM zulip_missing_message_recovery
               WHERE state IN ('pending','delivering') AND available_at<=now()
                 AND (lease_until IS NULL OR lease_until<=now())
               ORDER BY available_at,operation_uuid LIMIT 1 FOR UPDATE SKIP LOCKED"""
        ).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["lease_uuid"] = uuid.uuid4()
        session.execute(
            """UPDATE zulip_missing_message_recovery
               SET lease_uuid=%s,lease_until=now()+interval '120 seconds',
                   attempts=attempts+1,updated_at=now()
               WHERE operation_uuid=%s""",
            (item["lease_uuid"], item["operation_uuid"]),
        )
        return item


def finish(store, item, state, error=None, *, operations=None, delay=30):
    with store.session() as session:
        return (
            session.execute(
                """UPDATE zulip_missing_message_recovery
               SET state=%s,error_code=%s,lease_uuid=NULL,lease_until=NULL,
                   recovery_operations=COALESCE(%s::uuid[],recovery_operations),
                   available_at=now()+%s*interval '1 second',updated_at=now()
               WHERE operation_uuid=%s AND lease_uuid=%s AND lease_until>now()
               RETURNING operation_uuid""",
                (
                    state,
                    error,
                    operations,
                    delay,
                    item["operation_uuid"],
                    item["lease_uuid"],
                ),
            ).fetchone()
            is not None
        )


def lease_current(session, item):
    return (
        session.execute(
            """SELECT 1 FROM zulip_missing_message_recovery
           WHERE operation_uuid=%s AND lease_uuid=%s AND lease_until>now()
             AND reference IS NOT DISTINCT FROM %s::jsonb
           FOR UPDATE""",
            (
                item["operation_uuid"],
                item["lease_uuid"],
                None if item["reference"] is None else json.dumps(item["reference"]),
            ),
        ).fetchone()
        is not None
    )


def context(store, item):
    """Return a comparable fence; callers recheck it after every provider read."""
    account_uuid = str(item["account_uuid"])
    message_id = item["provider_message_id"]
    account = store.account_resource(account_uuid)
    assignment = store.assignment_for_provider_chat(
        account_uuid, item["provider_chat_key"]
    )
    if (
        account is None
        or int(account["generation"]) != item["account_generation"]
        or assignment is None
        or int(assignment["generation"]) != item["assignment_generation"]
        or not assignment.get("selected", True)
        or str(assignment["project_id"]) != str(item["project_uuid"])
        or (
            item["assignment_uuid"] is not None
            and str(assignment["uuid"]) != str(item["assignment_uuid"])
        )
    ):
        return "superseded", None
    if not store.provider_is_enabled("zulip") or not store.provider_account_is_eligible(
        account_uuid
    ):
        return "pending", None
    if store.provider_message_tombstone(account_uuid, message_id) is not None:
        return "tombstoned", None
    with store.session() as session:
        cursor = session.execute(
            "SELECT provider_realm_uuid FROM zulip_event_cursors WHERE account_uuid=%s",
            (account_uuid,),
        ).fetchone()
        if cursor is None or item["provider_realm_uuid"] is None:
            return "pending", None
        if cursor["provider_realm_uuid"] != item["provider_realm_uuid"]:
            return "superseded", None
        scope = session.execute(
            """SELECT generation,fingerprint,directory_pending FROM zulip_history_scopes
               WHERE project_uuid=%s AND provider_realm_uuid=%s""",
            (item["project_uuid"], item["provider_realm_uuid"]),
        ).fetchone()
        sources = history_delivery.sources_for(
            session, item["project_uuid"], item["provider_realm_uuid"]
        )
        if (
            not sources
            or len(sources) > history_delivery.MAX_SOURCES
            or any(source["state"] != "complete" for source in sources)
            or (
                scope is not None
                and (
                    scope["directory_pending"]
                    or scope["fingerprint"]
                    != store.history_configuration_fingerprint(
                        session, item["project_uuid"], item["provider_realm_uuid"]
                    )
                )
            )
        ):
            return "pending", None
        if session.execute(
            """SELECT 1 FROM zulip_history_batches
               WHERE project_uuid=%s AND provider_realm_uuid=%s
                 AND import_status<>'complete' LIMIT 1""",
            (item["project_uuid"], item["provider_realm_uuid"]),
        ).fetchone():
            return "pending", None
        journal = session.execute(
            """SELECT queue_id,event_id,processing_state FROM zulip_provider_events
               WHERE account_uuid=%s AND (
                   body->>'message_id'=%s OR body->'message'->>'id'=%s
                   OR body->'message_ids' @> %s::jsonb)
               ORDER BY created_at DESC,event_id DESC""",
            (account_uuid, message_id, message_id, json.dumps([int(message_id)])),
        ).fetchall()
        if any(
            row["processing_state"] in {"pending", "processing", "delivering"}
            for row in journal
        ):
            return "pending", None
        if session.execute(
            """SELECT 1 FROM workspace_delivery_outbox
               WHERE account_uuid=%s AND record->'operation'->'provider'->>'entity_id'=%s
                 AND record->'operation'->>'kind' IN ('message.create','message.update','message.delete')
                 AND submission_state NOT IN ('sent','rejected') LIMIT 1""",
            (account_uuid, message_id),
        ).fetchone():
            return "pending", None
    mapping = store.provider_message_mapping(account_uuid, message_id)
    if mapping is not None and mapping.get("pending_tombstone"):
        return "tombstoned", None
    return "ready", {
        "account": account,
        "assignment": assignment,
        "scope": dict(scope) if scope else None,
        "journal": [dict(row) for row in journal],
        "mapping": copy.deepcopy(mapping),
    }


def placement_matches(reference, chat_key, topic_id):
    if not isinstance(reference, dict):
        return False
    metadata = reference.get("metadata", {})
    return (
        metadata.get("chat_key") == chat_key
        and metadata.get("topic_provider_id") == topic_id
    )


def snapshot_placement(message):
    chat_type, chat_key = converter.provider_chat_reference(message)
    topic = (
        converter.channel_topic_provider_id(
            message["stream_id"], str(message["subject"])
        )
        if chat_type == "channel"
        else f"{chat_key}:default"
    )
    return chat_key, topic


class SnapshotStore:
    """Stage conversion writes and allocate real lanes only on atomic enqueue."""

    def __init__(self, store):
        self.store = store
        self.mappings = {}

    def __getattr__(self, name):
        return getattr(self.store, name)

    def reconcile_assignment_projection(self, *_args):
        # Control synchronization owns materialization; conversion must stay
        # read-only until its final generation and mapping fences pass.
        return False

    def producer_lane_position(self, *_args):
        return 0, None

    def remember_provider_mapping(
        self,
        account_uuid,
        kind,
        provider_id,
        workspace_uuid,
        metadata,
        provider_revision=None,
    ):
        self.mappings[(account_uuid, kind, provider_id)] = {
            "workspace_uuid": workspace_uuid,
            "provider_id": provider_id,
            "provider_revision": provider_revision,
            "metadata": copy.deepcopy(metadata),
        }

    def provider_mapping(self, account_uuid, kind, provider_id):
        return self.mappings.get(
            (account_uuid, kind, provider_id)
        ) or self.store.provider_mapping(account_uuid, kind, provider_id)

    def provider_message_mapping(self, account_uuid, provider_id):
        mapping = self.mappings.get((account_uuid, "message", provider_id))
        if mapping is None:
            mapping = self.store.provider_message_mapping(account_uuid, provider_id)
        if mapping is None:
            return None
        mapping = copy.deepcopy(mapping)
        for key in list(mapping.get("metadata", {})):
            if key.startswith("confirmed_message_"):
                mapping["metadata"].pop(key)
        return mapping

    def confirmed_message_state(self, *_args):
        return None

    def persist(self):
        for (account_uuid, kind, provider_id), mapping in self.mappings.items():
            if kind in {"identity", "stream", "topic", "message"}:
                self.store.remember_provider_mapping(
                    account_uuid,
                    kind,
                    provider_id,
                    mapping["workspace_uuid"],
                    mapping["metadata"],
                    mapping["provider_revision"],
                )


def snapshot_event(item, message):
    digest = converter.provider_message_fingerprint(message)
    queue_id = f"missing-base:{item['operation_uuid']}:{digest}"
    return queue_id, {
        "id": int(item["provider_message_id"]),
        "type": "message",
        "message": message,
    }


def finalize_delivered(store, item):
    with store.session() as session:
        rows = session.execute(
            "SELECT terminal_outcome FROM operation_idempotency WHERE operation_uuid=ANY(%s::uuid[])",
            (item["recovery_operations"],),
        ).fetchall()
    outcomes = [row["terminal_outcome"] for row in rows]
    if len(outcomes) != len(item["recovery_operations"]):
        # Enqueue creates these atomically. Normal terminal pruning retains
        # idempotency; only a projection reset can retire a child operation.
        return finish(store, item, "superseded", "recovery_operation_retired")
    if any(value is not None and value != "committed" for value in outcomes):
        return finish(store, item, "blocked", "recovery_operation_rejected")
    if len(outcomes) == len(item["recovery_operations"]) and outcomes and all(outcomes):
        return finish(store, item, "complete")
    return finish(store, item, "delivering", delay=5)


def run_once(service):
    store = service.store
    item = store.claim_missing_message_recovery()
    if item is None:
        return False
    if item["state"] == "delivering":
        account = store.account_resource(str(item["account_uuid"]))
        if account is None or int(account["generation"]) != item["account_generation"]:
            return finish(store, item, "superseded", "account_generation_changed")
        return finalize_delivered(store, item)
    try:
        state, before = context(store, item)
        if state != "ready":
            return finish(
                store,
                item,
                state,
                "history_or_live_pending" if state == "pending" else state,
            )
        if placement_matches(
            item["reference"], item["provider_chat_key"], item["provider_topic_id"]
        ):
            # This receipt came from the real post-import canonical placement,
            # not from the bridge's pre-existing mapping assumption.
            return finish(store, item, "complete")
        account_uuid = str(item["account_uuid"])
        adapter = service.provider_adapters(account_uuid)
        message = adapter.message_by_id(int(item["provider_message_id"]))
        if message is None:
            # No access is not evidence of deletion. Keep an explicit terminal
            # outcome, never manufacture content or resurrect a tombstone.
            return finish(store, item, "blocked", "provider_message_unavailable")
        if str(message["id"]) != item["provider_message_id"]:
            return finish(store, item, "blocked", "provider_identity_conflict")
        current_chat, current_topic = snapshot_placement(message)
        assignment = store.assignment_for_provider_chat(account_uuid, current_chat)
        if assignment is None or not assignment.get("selected", True):
            return finish(store, item, "superseded", "provider_chat_not_selected")
        # A cross-project recovery must wait on that project's publication gate
        # as well; a later selection creates its own fresh history projection.
        if str(assignment["project_id"]) != str(item["project_uuid"]):
            return finish(store, item, "superseded", "provider_project_changed")
        staged = SnapshotStore(store)
        if item["reference"] is not None:
            # Import may converge a local UUID onto an existing canonical
            # message. The receipt is the authority for the recovery target.
            staged.remember_provider_mapping(
                account_uuid,
                "message",
                item["provider_message_id"],
                item["reference"]["workspace_uuid"],
                item["reference"]["metadata"],
            )
        queue_id, event = snapshot_event(item, message)
        records = service._event_records_with_file_fallback(
            adapter,
            account_uuid,
            str(assignment["uuid"]),
            queue_id,
            event,
            "live",
            staged,
        )
        records = [
            record
            for record in records
            if record["operation"]["kind"]
            in {
                "identity.upsert",
                "topic.upsert",
                "message.create",
                "message.update",
            }
        ]
        message_records = [
            record
            for record in records
            if record["operation"]["kind"].startswith("message.")
        ]
        if len(message_records) != 1:
            return finish(store, item, "blocked", "invalid_recovery_snapshot")
        operation = message_records[0]["operation"]
        operation["provider"]["revision"] = str(
            message.get("last_edit_timestamp") or message["timestamp"]
        )
        operation["extensions"]["missing_base_recovery"] = True
        message_records[0]["operation_sha256"] = canonical.operation_digest(
            message_records[0]
        )
        with store.transaction() as session:
            session.execute("SET LOCAL lock_timeout='100ms'")
            # Match the existing desired-state and message-mapping lock order.
            session.execute("LOCK TABLE desired_resources IN SHARE MODE")
            store.lock_missing_message_recovery(
                session, account_uuid, item["provider_message_id"]
            )
            if not lease_current(session, item):
                return False
            after_state, after = context(store, item)
            if after_state != "ready" or after != before:
                return finish(
                    store,
                    item,
                    after_state if after_state != "ready" else "pending",
                    "recovery_context_changed",
                )
            if placement_matches(item["reference"], current_chat, current_topic):
                return finish(store, item, "complete")
            staged.persist()
            for record in records:
                # Every child needs its own durable outcome and causal lane
                # position; topic coalescing would leave an unsubmitted child.
                store.enqueue_workspace_delivery(
                    record, priority=0, deduplicate_topics=False
                )
            completed = finish(
                store,
                item,
                "delivering",
                operations=[record["operation_uuid"] for record in records],
                delay=1,
            )
            if not completed:
                raise RecoveryLeaseExpired()
            return True
    except Exception as error:
        # Use the service's provider boundary without leaking provider details.
        if isinstance(error, RecoveryLeaseExpired):
            return False
        if isinstance(error, zulip_adapter.ZulipOperationError):
            return finish(
                store,
                item,
                "pending" if error.retryable else "blocked",
                "provider_snapshot_unavailable",
                delay=min(3600, 30 * 2 ** min(item["attempts"], 7)),
            )
        if history_delivery.retryable_database_error(error):
            return finish(store, item, "pending", "recovery_database_contention")
        if (
            isinstance(error, ValueError)
            and str(error) == "provider_chat_assignment_pending"
        ):
            return finish(store, item, "pending", "provider_chat_assignment_pending")
        if isinstance(error, (ValueError, KeyError, TypeError)):
            return finish(store, item, "blocked", "invalid_recovery_snapshot")
        raise
