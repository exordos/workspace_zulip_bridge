import contextlib
import datetime
import json
import pathlib
import uuid

import pytest

from workspace_zulip_bridge import storage


class Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class Session:
    def __init__(self, rows=()):
        self.rows = rows
        self.statements = []

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))
        return Result(self.rows)


def _store_with_session(session):
    store = storage.RestAlchemyStore("unused")

    @contextlib.contextmanager
    def open_session():
        yield session

    store.session = open_session
    return store


def test_transaction_reuses_one_session_for_nested_store_calls(monkeypatch):
    supplied = Session()

    class Engine:
        def __init__(self):
            self.opens = 0

        @contextlib.contextmanager
        def session_manager(self):
            self.opens += 1
            yield supplied

    engine = Engine()
    monkeypatch.setattr(storage, "_engine_for", lambda _connection_url: engine)
    store = storage.RestAlchemyStore("postgresql://unused")

    with store.transaction() as outer:
        with store.session() as nested:
            assert nested is outer

    assert engine.opens == 1


def test_pending_provider_event_probe_checks_ready_and_delivering_live_rows():
    session = Session(({"pending": True},))
    store = _store_with_session(session)

    assert store.has_pending_provider_events()
    assert session.statements[0] == ("SET LOCAL jit = off", None)
    statement, parameters = session.statements[1]
    assert "processing_state" in statement
    assert "'pending', 'delivering'" in statement
    assert "JOIN LATERAL" in statement
    assert "SELECT DISTINCT event.causal_lane" in statement
    assert "barrier.causal_lane IS NULL" in statement
    assert "journal.account_uuid" in statement
    assert "ORDER BY event.created_at" in statement
    assert "processing_state = 'delivering'" in statement
    assert "processing_state = 'pending'" in statement
    assert "event.available_at <= now()" in statement
    assert "workspace_delivery_outbox AS delivery" in statement
    assert "'awaiting_result'" in statement
    assert "scheduler.provider_state = 'ready'" in statement
    assert "'backoff'" in statement
    assert "scheduler.provider_retry_after" in statement
    assert parameters is None


def test_pending_provider_events_claim_fair_account_heads():
    session = Session(({"account_uuid": "account", "event_id": 7},))
    store = _store_with_session(session)

    assert store.pending_provider_events(limit=20) == [
        {"account_uuid": "account", "event_id": 7}
    ]
    assert session.statements[0] == ("SET LOCAL jit = off", None)
    statement, parameters = session.statements[1]
    assert "JOIN LATERAL" in statement
    assert "SELECT DISTINCT event.causal_lane" in statement
    assert "barrier.causal_lane IS NULL" in statement
    assert "global_event.causal_lane" in statement
    assert "last_provider_event_dispatched_at NULLS FIRST" in statement
    assert "FOR UPDATE OF journal SKIP LOCKED" in statement
    assert "UPDATE scheduler_accounts AS journal" in statement
    assert "INSERT INTO scheduler_provider_event_lanes" in statement
    assert "ON CONFLICT (account_uuid, causal_lane) DO UPDATE" in statement
    assert "ORDER BY event.created_at, event.event_id, event.queue_id" in statement
    assert parameters == (20,)


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        (
            {
                "type": "message",
                "message": {"type": "stream", "stream_id": 42},
            },
            "channel:42",
        ),
        (
            {
                "type": "message",
                "message": {
                    "type": "private",
                    "display_recipient": [{"id": 3}, {"id": 1}, {"id": 2}],
                },
            },
            "group_direct:1,2,3",
        ),
        (
            {
                "type": "message",
                "message": {
                    "type": "private",
                    "display_recipient": [{"id": 7}],
                },
            },
            "group_direct:7",
        ),
        ({"type": "user_topic", "stream_id": 43}, "channel:43"),
        (
            {
                "type": "subscription",
                "op": "peer_add",
                "stream_ids": [44],
            },
            "channel:44",
        ),
        (
            {
                "type": "subscription",
                "op": "peer_add",
                "stream_ids": [44, 45],
            },
            None,
        ),
        (
            {"type": "realm_user", "person": {"user_id": 7}},
            "identity:7",
        ),
        (
            {
                "type": "update_message",
                "message_id": 601,
                "stream_id": 42,
                "new_stream_id": 43,
            },
            None,
        ),
    ],
)
def test_provider_event_static_causal_lane(event, expected):
    assert storage._provider_event_static_causal_lane(event) == expected


def test_provider_event_message_ids_normalizes_and_preserves_order():
    assert storage._provider_event_message_ids(
        {
            "message_id": "003",
            "message_ids": [2, "3", True, "invalid", 1, 2],
        }
    ) == ["3", "2", "1"]


def test_provider_event_message_ids_scales_to_large_unique_batches():
    message_ids = list(range(100_000))

    assert storage._provider_event_message_ids({"messages": message_ids}) == [
        str(message_id) for message_id in message_ids
    ]


def test_cross_stream_message_move_requires_account_barrier():
    assert storage._provider_event_requires_account_barrier(
        {
            "type": "update_message",
            "message_id": 601,
            "stream_id": 42,
            "new_stream_id": 43,
        }
    )


def test_provider_event_causal_lane_skips_sources_when_mappings_are_complete():
    session = Session(
        (
            {"provider_id": "601", "causal_lane": "channel:42"},
            {"provider_id": "602", "causal_lane": "channel:42"},
        )
    )

    assert (
        storage.RestAlchemyStore._provider_event_causal_lane(
            session,
            "account",
            {
                "type": "update_message_flags",
                "message_ids": [601, 602],
            },
        )
        == "channel:42"
    )
    assert len(session.statements) == 1
    assert "FROM provider_mappings" in session.statements[0][0]


def test_eligible_accounts_are_generation_bound_and_backoff_aware():
    session = Session(({"resource_uuid": "account"},))
    store = _store_with_session(session)
    store.provider_is_enabled = lambda provider: provider == "zulip"

    assert store.eligible_account_uuids() == ["account"]
    statement = session.statements[0][0]
    assert "scheduler.provider_generation IS DISTINCT FROM" in statement
    assert "scheduler.provider_state = 'ready'" in statement
    assert "scheduler.provider_state = 'backoff'" in statement
    assert "scheduler.provider_retry_after <= now()" in statement


def test_pending_workspace_delivery_probe_requires_current_account_and_assignment():
    session = Session(({"pending": True},))
    store = _store_with_session(session)

    assert store.has_pending_workspace_deliveries(0, 0)
    statement, parameters = session.statements[0]
    assert "AND EXISTS" in statement
    assert "workspace_delivery_outbox" in statement
    assert "JOIN desired_resources AS account" in statement
    assert "LEFT JOIN desired_resources AS assignment" in statement
    assert "account.body->>'synchronization_enabled'" in statement
    assert "policy.body->>'enabled'" in statement
    assert "policy.body->>'emergency_suspended'" in statement
    assert "delivery.assignment_uuid IS NULL" in statement
    assert "OR assignment.resource_uuid IS NOT NULL" in statement
    assert "report.body->>'resource_uuid'" in statement
    assert "delivery.assignment_uuid::text" in statement
    assert parameters == (0, 0)


def test_pending_chat_materialization_probe_includes_deferred_upserts():
    session = Session(({"pending": True},))
    store = _store_with_session(session)

    assert store.has_pending_chat_materializations()
    statement, parameters = session.statements[0]
    assert "observed_report_outbox" in statement
    assert "report.completed_at IS NULL" in statement
    assert "report.available_at" not in statement
    assert "'external_chat_catalog'" in statement
    assert "report.body->>'status' = 'ready'" in statement
    assert "report.body->'catalog'->>'operation'" in statement
    assert "'upsert'" in statement
    assert parameters is None


def _desired_change():
    resource_uuid = str(uuid.uuid4())
    return {
        "change_uuid": str(uuid.uuid4()),
        "sequence": 1,
        "resource_type": "external_provider_policy",
        "resource_uuid": resource_uuid,
        "operation": "upsert",
        "generation": 1,
        "required_capabilities": {
            "messenger.chat_catalog": {"min_revision": 1, "limits": {}}
        },
        "resource": {
            "resource_type": "external_provider_policy",
            "uuid": resource_uuid,
            "generation": 1,
            "provider_kind": "zulip",
        },
    }


def test_provider_mapping_by_name_uses_case_insensitive_live_metadata_lookup():
    row = {
        "workspace_uuid": uuid.uuid4(),
        "provider_id": "channel:42",
        "provider_revision": None,
        "metadata": {"name": "Engineering"},
        "convergent_alias": False,
    }
    session = Session((row,))
    store = _store_with_session(session)
    account_uuid = str(uuid.uuid4())

    assert store.provider_mapping_by_name(account_uuid, "stream", "engineering") == row
    statement, parameters = session.statements[0]
    assert "LOWER(mapping.metadata->>'name') = LOWER(%s)" in statement
    assert "NOT mapping.deleted" in statement
    assert parameters == (account_uuid, "stream", "engineering")


def test_workspace_mapping_prefers_active_alias_over_stale_primary_mapping():
    alias_row = {
        "workspace_uuid": uuid.uuid4(),
        "provider_id": "42:current topic",
        "provider_revision": "3",
        "metadata": {"name": "current topic"},
    }
    session = Session((alias_row,))
    store = _store_with_session(session)
    account_uuid = str(uuid.uuid4())
    workspace_uuid = str(alias_row["workspace_uuid"])

    assert store.workspace_mapping(account_uuid, "topic", workspace_uuid) == alias_row
    statement, parameters = session.statements[0]
    assert "alias.metadata, 0 AS source_order" in statement
    assert "1 AS source_order" in statement
    assert parameters == (
        account_uuid,
        "topic",
        workspace_uuid,
        account_uuid,
        "topic",
        workspace_uuid,
    )


def test_tombstoned_workspace_mapping_retains_identity_profile_for_delivery():
    row = {
        "workspace_uuid": uuid.uuid4(),
        "provider_id": "42",
        "provider_revision": None,
        "metadata": {"display_name": "Former User", "active": False},
    }
    session = Session((row,))
    store = _store_with_session(session)
    account_uuid = str(uuid.uuid4())
    workspace_uuid = str(row["workspace_uuid"])

    assert (
        store.tombstoned_workspace_mapping(
            account_uuid,
            "identity",
            workspace_uuid,
        )
        == row
    )
    statement, parameters = session.statements[0]
    assert "workspace_uuid = %s AND deleted" in statement
    assert "ORDER BY updated_at DESC" in statement
    assert parameters == (account_uuid, "identity", workspace_uuid)


def test_accepted_provider_message_context_uses_immutable_delivery_record():
    context = {
        "project_uuid": str(uuid.uuid4()),
        "message_uuid": str(uuid.uuid4()),
        "chat_key": "channel:42",
        "stream_uuid": str(uuid.uuid4()),
        "topic_uuid": str(uuid.uuid4()),
        "author_uuid": str(uuid.uuid4()),
        "message_operation": {
            "kind": "message.create",
            "payload": {"payload": {"kind": "markdown", "content": "rendered"}},
        },
        "accepted_records": [
            {
                "operation_uuid": str(uuid.uuid4()),
                "operation": {"kind": "identity.upsert"},
            },
            {
                "operation_uuid": str(uuid.uuid4()),
                "operation": {"kind": "message.create"},
            },
        ],
        "accepted_records_complete": True,
    }
    session = Session((context,))
    store = _store_with_session(session)
    account_uuid = str(uuid.uuid4())
    queue_id = str(uuid.uuid4())

    assert (
        store.accepted_provider_message_context(account_uuid, queue_id, 17) == context
    )
    statement, parameters = session.statements[0]
    assert "WITH provider_event AS" in statement
    assert "provider_event.prepared_records" in statement
    assert "FROM workspace_delivery_outbox" in statement
    assert "provider_queue_id = %s" in statement
    assert "provider_event_id = %s" in statement
    assert "'message.create'" in statement
    assert "record->'operation' AS message_operation" in statement
    assert "jsonb_agg(" in statement
    assert "AS accepted_records" in statement
    assert "AS accepted_records_complete" in statement
    assert parameters == (
        account_uuid,
        queue_id,
        17,
        account_uuid,
        queue_id,
        17,
    )


def test_prepare_provider_event_records_allocates_and_persists_full_sequence():
    account_uuid = str(uuid.uuid4())
    queue_id = "queue"
    event_id = 17
    record = {
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": str(uuid.uuid4()),
        "account_uuid": account_uuid,
        "project_uuid": str(uuid.uuid4()),
        "origin": "zulip",
        "causal_lane": f"chat:{account_uuid}:channel:42",
        "sequence": 0,
        "predecessor_operation_uuid": None,
        "operation_sha256": "",
        "operation": {"kind": "message.create"},
    }

    class PreparingSession(Session):
        def execute(self, statement, parameters=None):
            self.statements.append((statement, parameters))
            if "SELECT processing_state, prepared_records" in statement:
                return Result(
                    (
                        {
                            "processing_state": "pending",
                            "prepared_records": None,
                            "body": {},
                        },
                    )
                )
            if "INSERT INTO producer_lane_counters" in statement:
                return Result(({"last_sequence": 0, "last_operation_uuid": None},))
            return Result()

    session = PreparingSession()
    store = _store_with_session(session)

    prepared = store.prepare_provider_event_records(
        account_uuid, queue_id, event_id, [record]
    )

    assert prepared[0]["sequence"] == 1
    assert prepared[0]["operation_sha256"]
    assert record["sequence"] == 0
    update = next(
        (statement, parameters)
        for statement, parameters in session.statements
        if "SET prepared_records = %s" in statement
    )
    assert json.loads(update[1][0]) == prepared
    assert update[1][1:] == (account_uuid, queue_id, event_id)


def test_catalog_participants_merge_is_monotonic_and_enriches_placeholders():
    current = [
        {
            "provider_user_id": "20",
            "display_name": "Full Name",
            "email": "full@example.test",
            "avatar_urn": "urn:avatar:20",
            "is_owner": False,
        },
        {
            "provider_user_id": "10",
            "display_name": "10",
            "email": None,
            "avatar_urn": None,
            "is_owner": False,
        },
    ]
    observed = [
        {
            "provider_user_id": "20",
            "display_name": "Mention Name",
            "email": None,
            "avatar_urn": None,
            "is_owner": False,
        },
        {
            "provider_user_id": "10",
            "display_name": "Discovered Name",
            "email": "discovered@example.test",
            "avatar_urn": "urn:avatar:10",
            "is_owner": True,
        },
    ]

    assert storage._merge_catalog_participants(current, observed) == [
        {
            "provider_user_id": "10",
            "display_name": "Discovered Name",
            "email": "discovered@example.test",
            "avatar_urn": "urn:avatar:10",
            "is_owner": True,
        },
        current[0],
    ]


def test_catalog_participants_refresh_local_provider_activity():
    current = [
        {
            "provider_user_id": "20",
            "display_name": "Unavailable Zulip user (ID 20)",
            "is_owner": False,
            "_provider_active": False,
        }
    ]
    observed = [
        {
            "provider_user_id": "20",
            "display_name": "Restored User",
            "is_owner": False,
            "_provider_active": True,
        }
    ]

    assert storage._merge_catalog_participants(
        current,
        observed,
        authoritative=True,
    ) == [
        {
            **current[0],
            "display_name": "Restored User",
            "_provider_active": True,
        }
    ]


def test_authoritative_catalog_participants_remove_stale_members_and_refresh_names():
    current = [
        {
            "provider_user_id": "10",
            "display_name": "Full Name",
            "email": "full@example.test",
            "avatar_urn": "urn:avatar:10",
            "is_owner": True,
        },
        {
            "provider_user_id": "20",
            "display_name": "Stale Member",
            "email": "stale@example.test",
            "avatar_urn": None,
            "is_owner": False,
        },
    ]
    observed = [
        {
            "provider_user_id": "10",
            "display_name": "Renamed User",
            "email": None,
            "avatar_urn": None,
            "is_owner": True,
        }
    ]

    assert storage._merge_catalog_participants(
        current,
        observed,
        authoritative=True,
    ) == [
        {
            **current[0],
            "display_name": "Renamed User",
        }
    ]


def test_authoritative_catalog_participants_keep_rich_name_over_id_placeholder():
    current = [
        {
            "provider_user_id": "10",
            "display_name": "Known User",
            "email": "known@example.test",
            "avatar_urn": "urn:avatar:10",
            "is_owner": True,
        }
    ]
    observed = [
        {
            "provider_user_id": "10",
            "display_name": "10",
            "email": None,
            "avatar_urn": None,
            "is_owner": True,
        }
    ]

    assert storage._merge_catalog_participants(
        current,
        observed,
        authoritative=True,
    ) == current


@pytest.mark.parametrize(
    "mutation",
    [
        lambda change: change["required_capabilities"].update(
            {"messenger.future": {"min_revision": 1, "limits": {}}}
        ),
        lambda change: change["resource"].update({"uuid": str(uuid.uuid4())}),
        lambda change: change["resource"].update({"generation": 2}),
    ],
)
def test_incremental_desired_batch_fails_closed_before_cursor_commit(mutation):
    session = Session()
    store = _store_with_session(session)
    change = _desired_change()
    mutation(change)

    with pytest.raises(ValueError):
        store.apply_desired_changes([change], "cursor-2")

    assert session.statements == []


@pytest.mark.parametrize(
    "mutation",
    [
        lambda change: change.update(resource_uuid="not-a-uuid"),
        lambda change: change.update(generation=0),
    ],
    ids=("invalid-resource-uuid", "non-positive-generation"),
)
def test_incremental_desired_delete_fails_closed_before_any_sql(mutation):
    session = Session()
    store = _store_with_session(session)
    change = {
        "change_uuid": str(uuid.uuid4()),
        "sequence": 1,
        "resource_type": "external_account",
        "resource_uuid": str(uuid.uuid4()),
        "operation": "delete",
        "generation": 2,
    }
    mutation(change)

    with pytest.raises(ValueError):
        store.apply_desired_changes([change], "cursor-2")

    assert session.statements == []


def test_full_snapshot_fails_closed_before_materialization_or_cursor_commit():
    session = Session()
    store = _store_with_session(session)
    resource = _desired_change()["resource"]
    resource["required_capabilities"] = {
        "messenger.future": {"min_revision": 1, "limits": {}}
    }

    with pytest.raises(ValueError, match="unsupported capability"):
        store.install_snapshot([resource], "anchor")

    assert session.statements == []


def test_applied_account_generation_invalidates_provider_event_cursor():
    class AppliedAccountSession(Session):
        def execute(self, statement, parameters=None):
            self.statements.append((statement, parameters))
            if "RETURNING body, deleted" in statement:
                return Result(({"body": {}, "deleted": False},))
            return Result()

    session = AppliedAccountSession()
    store = _store_with_session(session)
    resource_uuid = str(uuid.uuid4())
    resource = {
        "resource_type": "external_account",
        "uuid": resource_uuid,
        "generation": 2,
    }
    change = {
        "change_uuid": str(uuid.uuid4()),
        "sequence": 1,
        "resource_type": "external_account",
        "resource_uuid": resource_uuid,
        "operation": "upsert",
        "generation": 2,
        "required_capabilities": {},
        "resource": resource,
    }

    store.apply_desired_changes([change], "cursor-2")

    invalidation = next(
        item
        for item in session.statements
        if "DELETE FROM zulip_event_cursors" in item[0]
    )
    assert invalidation[1] == (resource_uuid,)


def test_snapshot_invalidates_cursors_from_other_account_generations():
    session = Session()
    store = _store_with_session(session)
    resource = {
        "resource_type": "external_account",
        "uuid": str(uuid.uuid4()),
        "generation": 2,
        "required_capabilities": {},
    }

    store.install_snapshot([resource], "anchor")

    statement = next(
        sql
        for sql, _parameters in session.statements
        if "DELETE FROM zulip_event_cursors AS cursor" in sql
    )
    assert "cursor.provider_account_generation IS NULL" in statement
    assert "account.generation" in statement
    assert "cursor.provider_account_generation" in statement
    alias_tombstone = next(
        sql
        for sql, _parameters in session.statements
        if "UPDATE provider_mapping_aliases" in sql
    )
    assert "entity_kind = 'topic'" in alias_tombstone
    assert "NOT deleted" in alias_tombstone


def test_expired_running_lease_reaper_is_atomic_and_idempotent():
    session = Session(({"record_uuid": "one"}, {"record_uuid": "two"}))
    store = _store_with_session(session)

    assert store.reap_expired_running() == 2
    statement = session.statements[0][0]
    assert "WHERE state = 'running' AND lease_until < now()" in statement
    assert "provider_attempted_at IS NOT NULL" in statement
    assert "provider_queue_id IS NOT NULL" in statement
    assert "provider_local_id IS NOT NULL" in statement
    assert "THEN 'uncertain'" in statement
    assert "ELSE 'pending'" in statement
    assert "lease_owner = NULL, lease_until = NULL" in statement

    session.rows = ()
    assert store.reap_expired_running() == 0


def test_uncertain_claim_does_not_steal_a_live_reconciliation_lease():
    session = Session()
    store = _store_with_session(session)

    assert store.claim_uncertain("worker") is None
    statement = session.statements[0][0]
    assert "operation.lease_until IS NULL" in statement
    assert "operation.lease_until < now()" in statement


def test_workspace_delivery_outbox_orders_live_before_backfill():
    session = Session()
    store = _store_with_session(session)

    assert (
        store.pending_workspace_deliveries(
            minimum_priority=2, maximum_priority=2, limit=101
        )
        == []
    )
    assert len(session.statements) == 4
    read_promotion = session.statements[0][0]
    reaction_promotion = session.statements[1][0]
    topic_promotion = session.statements[2][0]
    statement = session.statements[3][0]
    assert "SET priority = read_delivery.priority" in read_promotion
    assert "SET priority = reaction_delivery.priority" in reaction_promotion
    assert "'reaction.upsert', 'reaction.delete'" in reaction_promotion
    assert "SET priority = message_delivery.priority" in topic_promotion
    assert "AND %s = 0" in read_promotion
    assert "AND %s = 0" in reaction_promotion
    assert "AND %s = 0" in topic_promotion
    assert "read_delivery.priority BETWEEN %s AND %s" in read_promotion
    assert "reaction_delivery.priority BETWEEN %s AND %s" in reaction_promotion
    assert "message_delivery.priority BETWEEN %s AND %s" in topic_promotion
    assert session.statements[0][1] == (2, 2, 2)
    assert session.statements[1][1] == (2, 2, 2)
    assert session.statements[2][1] == (2, 2, 2)
    assert "submission_state IN ('pending', 'ambiguous')" in statement
    assert "submission_state = 'awaiting_result'" in statement
    assert "next_submission_at <= now()" in statement
    assert "delivery.priority BETWEEN %s AND %s" in statement
    assert session.statements[3][1] == (2, 2, 101)
    assert "topic_delivery.sent_at IS NULL" in statement
    assert "'message.create', 'message.update', 'read_state.set'" in statement
    assert "message_create.record->'operation'->>'kind'" in statement
    assert "jsonb_array_elements_text" in statement
    assert "workspace_delivery_state" in statement
    assert "provider_mappings AS message_mapping" in statement
    assert "report.body->>'resource_uuid'" in statement
    assert "delivery.assignment_uuid::text" in statement
    assert "message_mapping.workspace_uuid::text" not in statement
    assert "read_message.message_uuid::uuid" in statement
    assert ")::uuid" in statement
    assert "'reaction.upsert', 'reaction.delete'" in statement
    assert "ORDER BY priority, created_at" in statement


def test_live_dependency_promotion_scans_only_live_source_rows():
    session = Session()
    store = _store_with_session(session)

    assert (
        store.pending_workspace_deliveries(
            minimum_priority=0,
            maximum_priority=0,
            limit=20,
        )
        == []
    )
    assert [parameters for _statement, parameters in session.statements[:3]] == [
        (0, 0, 0),
        (0, 0, 0),
        (0, 0, 0),
    ]


class SharedDeliverySession:
    def __init__(self):
        self.operations = {}
        self.deliveries = {}
        self.mapping_updates = []

    def execute(self, statement, parameters=None):
        normalized = " ".join(statement.split())
        if normalized.startswith("SELECT generation FROM desired_resources"):
            return Result(({"generation": 1},))
        if normalized.startswith(
            "SELECT operation_sha256, terminal_outcome FROM operation_idempotency"
        ):
            operation = self.operations.get(parameters[0])
            return Result(() if operation is None else ({**operation},))
        if normalized.startswith("INSERT INTO operation_idempotency"):
            self.operations.setdefault(
                parameters[0],
                {
                    "operation_sha256": parameters[1],
                    "terminal_outcome": None,
                    "result_record_uuid": None,
                },
            )
            return Result()
        if normalized.startswith(
            "SELECT record FROM workspace_delivery_outbox WHERE operation_uuid"
        ):
            record = self.deliveries.get(parameters[0])
            return Result(() if record is None else ({"record": record},))
        if normalized.startswith(
            "SELECT operation_sha256, terminal_outcome, result_record_uuid"
        ):
            operation = self.operations.get(parameters[0])
            return Result(() if operation is None else ({**operation},))
        if normalized.startswith("SELECT operation.operation_sha256"):
            operation = self.operations.get(parameters[0])
            record = self.deliveries.get(parameters[0])
            if operation is None or record is None:
                return Result()
            return Result(({**operation, "record": record},))
        if normalized.startswith("INSERT INTO workspace_delivery_outbox"):
            operation_uuid = parameters[1]
            if operation_uuid in self.deliveries:
                return Result()
            self.deliveries[operation_uuid] = json.loads(parameters[5])
            return Result(({"record_uuid": parameters[0]},))
        if normalized.startswith("UPDATE operation_idempotency"):
            operation = self.operations[parameters[5]]
            operation["terminal_outcome"] = parameters[0]
            operation["result_record_uuid"] = parameters[1]
            return Result()
        if normalized.startswith("UPDATE workspace_delivery_outbox"):
            return Result()
        if normalized.startswith("UPDATE provider_mappings"):
            self.mapping_updates.append((normalized, parameters))
            return Result()
        raise AssertionError(normalized)


def test_workspace_delivery_result_survives_store_restart_round_trip():
    session = SharedDeliverySession()
    first_store = _store_with_session(session)
    operation_uuid = str(uuid.uuid4())
    record = {
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": operation_uuid,
        "operation_sha256": "a" * 64,
        "account_uuid": str(uuid.uuid4()),
        "project_uuid": str(uuid.uuid4()),
        "attempt": 1,
        "origin": "zulip",
        "causal_lane": "chat:one",
        "sequence": 1,
        "predecessor_operation_uuid": None,
    }

    assert first_store.enqueue_workspace_delivery(record, 0)

    restarted_store = _store_with_session(session)
    result = {
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": operation_uuid,
        "operation_sha256": "a" * 64,
        "in_reply_to_record_uuid": record["record_uuid"],
        **{
            field: record[field]
            for field in (
                "account_uuid",
                "project_uuid",
                "attempt",
                "origin",
                "causal_lane",
                "sequence",
                "predecessor_operation_uuid",
            )
        },
        "result": {
            "outcome": "committed",
            "provider_entity_id": "42",
            "provider_revision": None,
            "manual_retry_allowed": False,
        },
    }
    restarted_store.accept_result(result)

    assert session.operations[operation_uuid]["terminal_outcome"] == "committed"
    assert (
        session.operations[operation_uuid]["result_record_uuid"]
        == result["record_uuid"]
    )


def test_committed_inbound_topic_result_marks_projection_durable():
    session = SharedDeliverySession()
    store = _store_with_session(session)
    operation_uuid = str(uuid.uuid4())
    account_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    record = {
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": operation_uuid,
        "operation_sha256": "c" * 64,
        "account_uuid": account_uuid,
        "project_uuid": str(uuid.uuid4()),
        "attempt": 1,
        "origin": "zulip",
        "causal_lane": "chat:topic",
        "sequence": 1,
        "predecessor_operation_uuid": None,
        "operation": {
            "kind": "topic.upsert",
            "entity_uuid": topic_uuid,
            "provider": {"entity_id": "42:Topic"},
        },
    }
    session.operations[operation_uuid] = {
        "operation_sha256": record["operation_sha256"],
        "terminal_outcome": None,
        "result_record_uuid": None,
        "target_entity_id": None,
        "target_revision": None,
        "manual_retry_allowed": False,
    }
    session.deliveries[operation_uuid] = record
    store.accept_result(
        {
            "record_uuid": str(uuid.uuid4()),
            "operation_uuid": operation_uuid,
            "operation_sha256": "c" * 64,
            "in_reply_to_record_uuid": record["record_uuid"],
            **{
                field: record[field]
                for field in (
                    "account_uuid",
                    "project_uuid",
                    "attempt",
                    "origin",
                    "causal_lane",
                    "sequence",
                    "predecessor_operation_uuid",
                )
            },
            "result": {
                "outcome": "committed",
                "provider_entity_id": "42:Topic",
                "provider_revision": None,
                "manual_retry_allowed": False,
            },
        }
    )

    assert len(session.mapping_updates) == 1
    statement, parameters = session.mapping_updates[0]
    assert "workspace_delivery_state" in statement
    assert parameters == (account_uuid, "topic", "42:Topic", topic_uuid)


def test_initial_backfill_gate_ignores_delivery_outcomes_from_older_generation():
    session = Session(({"ready": True},))
    store = _store_with_session(session)

    assert store.initial_backfill_ready("00000000-0000-4000-8000-000000000001")
    statement = session.statements[0][0]
    normalized = " ".join(statement.split())
    assert "zulip_participant_sync" in normalized
    assert (
        "participant_sync.assignment_generation = assignment.generation" in normalized
    )
    assert "participant_sync.state = 'ready'" in normalized
    assert "account.resource_uuid = delivery.account_uuid" in normalized
    assert "delivery.account_generation = account.generation" in normalized
    assert "participant_sync.account_uuid::text" not in normalized
    assert "job.account_uuid::text" not in normalized
    assert "delivery.account_uuid::text" not in normalized


def test_catalog_readiness_queries_use_native_uuid_index_expressions():
    session = Session(({"accepted": True, "count": 0},))
    store = _store_with_session(session)
    account_uuid = "00000000-0000-4000-8000-000000000001"

    assert store.catalog_reports_accepted(account_uuid, 3)
    report_query = " ".join(session.statements[0][0].split())
    assert "external_account_uuid" in report_query
    assert ")::uuid = %s" in report_query
    assert "resource_uuid')::uuid" in report_query
    assert "observed_generation')::bigint DESC" in report_query
    assert "body->>'observed_at' DESC NULLS LAST" in report_query
    assert "created_at DESC, report_uuid DESC" in report_query


def test_inactive_provider_event_terminalization_preserves_audit_rows():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    session = Session(({"account_uuid": account_uuid},))
    store = _store_with_session(session)

    assert store.ignore_provider_event_for_inactive_account(
        account_uuid, "retired-queue", 7
    )
    ignored = " ".join(session.statements[0][0].split())
    cancelled = " ".join(session.statements[1][0].split())
    assert "processing_state = 'ignored'" in ignored
    assert "processing_reason = 'account_inactive'" in ignored
    assert "synchronization_enabled" in ignored
    assert "submission_state = 'cancelled'" in cancelled
    assert "submission_error_code = 'account_inactive'" in cancelled


def test_outside_history_reaction_terminalization_is_policy_guarded():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    session = Session(({"account_uuid": account_uuid},))
    store = _store_with_session(session)
    message_time = datetime.datetime(2026, 6, 1, tzinfo=datetime.UTC)

    assert store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "601",
        message_time,
        "queue",
        7,
    )
    statement, parameters = session.statements[0]
    normalized = " ".join(statement.split())
    assert "processing_state = 'ignored'" in normalized
    assert "processing_reason = 'provider_message_outside_history'" in normalized
    assert "job.state = 'complete'" in normalized
    assert "job.cutoff_at IS NOT NULL" in normalized
    assert "%s < job.cutoff_at" in normalized
    assert "assignment.body->>'history_depth' = job.history_depth" in normalized
    assert "NOT EXISTS ( SELECT 1 FROM provider_mappings" in normalized
    assert "FROM zulip_provider_events AS source_event" in normalized
    assert "source_event.event_type = 'message'" in normalized
    assert "source_event.processing_state IN ( 'pending', 'delivering' )" in normalized
    assert "source_event.body->'message'->>'id' = %s" in normalized
    assert "FROM zulip_provider_events AS echo_event" in normalized
    assert "JOIN bridge_operations AS operation" in normalized
    assert (
        "operation.provider_local_id = echo_event.body->>'local_message_id'"
        in normalized
    )
    assert "operation.provider_local_id IS NOT NULL" in normalized
    assert "operation.state IN ( 'pending', 'running', 'uncertain' )" in normalized
    assert "operation.record->'operation'->>'kind' = 'message.create'" in normalized
    assert "echo_event.body ? 'local_message_id'" in normalized
    assert "echo_event.body->'message'->>'id' = %s" in normalized
    assert "FROM zulip_queue_catchup_jobs AS catchup" in normalized
    assert "catchup.state <> 'complete'" in normalized
    assert parameters == (
        account_uuid,
        "queue",
        7,
        "601",
        "601",
        "601",
        "channel:42",
        "channel:42",
        message_time,
    )


def test_backfill_claim_requires_ready_participants_for_current_assignment():
    session = Session()
    store = _store_with_session(session)

    assert store.claim_backfill_job() is None

    statement = session.statements[0][0]
    assert "JOIN zulip_participant_sync AS participant_sync" in statement
    assert "participant_sync.assignment_generation =" in statement
    assert "assignment.generation" in statement
    assert "participant_sync.state = 'ready'" in statement


def test_participant_claim_only_refreshes_channels():
    session = Session()
    store = _store_with_session(session)

    assert store.claim_participant_sync() is None

    statement = session.statements[0][0]
    assert "JOIN desired_resources AS assignment" in statement
    assert "->>'chat_type' =" in statement
    assert "'channel'" in statement
    assert "participant_sync.state = 'ready'" in statement
    assert "make_interval(secs => %s)" in statement
    assert session.statements[0][1] == (
        storage.PARTICIPANT_RECHECK_INTERVAL_SECONDS,
        storage.PARTICIPANT_RECHECK_INTERVAL_SECONDS,
        1,
    )


def test_provider_event_invalidates_only_selected_participant_channels():
    session = Session()
    store = _store_with_session(session)
    account_uuid = str(uuid.uuid4())

    store.invalidate_participant_sync(
        account_uuid, ["channel:43", "channel:42", "channel:43"]
    )

    statement, parameters = session.statements[0]
    assert "UPDATE zulip_participant_sync" in statement
    assert "state = 'pending'" in statement
    assert "provider_chat_key = ANY(%s)" in statement
    assert parameters == (account_uuid, ["channel:42", "channel:43"])


def test_dead_queue_restarts_participants_and_configured_history():
    session = Session()
    store = _store_with_session(session)

    store.begin_provider_queue_catchup("00000000-0000-4000-8000-000000000001")

    assert len(session.statements) == 3
    participant_reset = session.statements[1][0]
    history_reset = session.statements[2][0]
    assert "UPDATE zulip_participant_sync" in participant_reset
    assert "state = 'pending'" in participant_reset
    assert "provider_user_ids = '[]'::jsonb" in participant_reset
    assert "UPDATE zulip_backfill_jobs" in history_reset
    assert "next_anchor = NULL" in history_reset
    assert "WHEN job.history_depth = 'new' THEN 'complete'" in history_reset
    assert "ELSE 'pending'" in history_reset


def test_live_assignment_report_is_queued_once_per_completed_generation():
    assignment = {
        "uuid": "00000000-0000-4000-8000-000000000042",
        "generation": 5,
    }
    session = Session(({"body": assignment},))
    store = _store_with_session(session)

    assert store.assignments_needing_live_report("account") == [assignment]

    statement, parameters = session.statements[0]
    assert "job.state = 'complete'" in statement
    assert "report.body->>'observed_generation'" in statement
    assert "assignment.generation" in statement
    assert "report.body->>'status' = 'live_ready'" in statement
    assert "report.result_status IS NULL" in statement
    assert "report.result_status IN ('applied', 'duplicate')" in statement
    assert parameters == ("account",)


def test_claim_allows_explicit_retry_after_lane_advanced_without_later_delete():
    session = Session()
    store = _store_with_session(session)

    assert store.claim("worker") is None

    statement = session.statements[0][0]
    assert "assignment.generation = operation.assignment_generation" in statement
    assert "operation.lane_sequence" in statement
    assert "COALESCE(lane.last_sequence, 0) + 1" in statement
    assert "IS NOT DISTINCT FROM lane.last_operation_uuid" in statement
    assert "operation.attempt > 1" in statement
    assert "NOT EXISTS" in statement
    assert "later_delete.state = 'committed'" in statement
    assert "scheduler.provider_state = 'ready'" in statement
    assert "scheduler.provider_state = 'backoff'" in statement
    assert "account.generation = scheduler.provider_generation" in statement


def test_terminal_claim_sweeps_expired_and_superseded_pending_work():
    session = Session()
    store = _store_with_session(session)

    assert store.claim_terminal("worker") is None

    statement = session.statements[0][0]
    assert "operation.expires_at <= now()" in statement
    assert "assignment.generation <> operation.assignment_generation" in statement
    assert "assignment.body->>'project_id'" in statement
    assert "account.body->>'synchronization_enabled'" in statement
    assert "scheduler.provider_state = 'auth_required'" in statement
    assert "THEN 'unauthorized_account'" in statement


def test_provider_send_attempt_never_replaces_live_queue_cursor(operation_record):
    session = Session()
    store = _store_with_session(session)
    item = storage.QueuedOperation(uuid.uuid4(), operation_record, 0)

    store.record_provider_attempt(item, "queue", "local", 7, "rendered")

    assert len(session.statements) == 1
    assert "UPDATE bridge_operations" in session.statements[0][0]
    assert "zulip_event_cursors" not in session.statements[0][0]


def test_delete_tombstone_and_provider_journal_finalize_share_one_transaction():
    session = Session()
    store = _store_with_session(session)

    store.finalize_provider_event("account", "queue", 7, True, ["601"])

    assert len(session.statements) == 2
    assert "UPDATE provider_mappings" in session.statements[0][0]
    assert "deleted = true" in session.statements[0][0]
    assert "UPDATE zulip_provider_events" in session.statements[1][0]
    assert "processing_state = 'pending'" in session.statements[1][0]


def test_lane_aware_finalization_locks_mapping_before_refresh_and_update():
    class LaneFinalizationSession(Session):
        def execute(self, statement, parameters=None):
            self.statements.append((statement, parameters))
            if "SELECT causal_lane" in statement and "FOR UPDATE" in statement:
                return Result(({"causal_lane": "message:601"},))
            if "RETURNING event_id" in statement:
                return Result(({"event_id": 7},))
            return Result()

    session = LaneFinalizationSession()
    store = _store_with_session(session)

    assert store.finalize_provider_event_if_lane_current(
        "account",
        "queue",
        7,
        {"type": "update_message", "message_id": 601},
        "message:601",
        True,
        [],
    )

    lock_statement, lock_parameters = session.statements[0]
    assert "pg_advisory_xact_lock" in lock_statement
    assert lock_parameters == (
        storage._provider_mapping_lock_key("account", "message", "601"),
    )
    assert "FOR UPDATE" in session.statements[1][0]
    assert "FROM provider_mappings" in session.statements[2][0]
    assert "UPDATE zulip_provider_events" in session.statements[-1][0]


def test_stale_result_cannot_replace_terminal_result():
    session = SharedDeliverySession()
    store = _store_with_session(session)
    operation_uuid = str(uuid.uuid4())
    operation_record = {
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": operation_uuid,
        "operation_sha256": "b" * 64,
        "account_uuid": str(uuid.uuid4()),
        "project_uuid": str(uuid.uuid4()),
        "attempt": 1,
        "origin": "zulip",
        "causal_lane": "chat:two",
        "sequence": 1,
        "predecessor_operation_uuid": None,
    }
    assert store.enqueue_workspace_delivery(operation_record, 0)
    base = {
        key: operation_record[key]
        for key in (
            "operation_uuid",
            "operation_sha256",
            "account_uuid",
            "project_uuid",
            "attempt",
            "origin",
            "causal_lane",
            "sequence",
            "predecessor_operation_uuid",
        )
    }
    base["in_reply_to_record_uuid"] = operation_record["record_uuid"]
    first = {
        **base,
        "record_uuid": str(uuid.uuid4()),
        "result": {"outcome": "committed", "manual_retry_allowed": False},
    }
    store.accept_result(first)

    stale = {
        **base,
        "record_uuid": str(uuid.uuid4()),
        "result": {"outcome": "rejected", "manual_retry_allowed": True},
    }
    with pytest.raises(ValueError, match="Stale result"):
        store.accept_result(stale)


def test_topic_projection_replacement_preserves_displaced_workspace_alias():
    session = Session()
    account_uuid = str(uuid.uuid4())
    workspace_uuid = str(uuid.uuid4())

    storage.RestAlchemyStore._replace_projection_mapping(
        session,
        account_uuid,
        "topic",
        workspace_uuid,
        "42:new-topic",
        {"name": "new topic"},
    )

    assert len(session.statements) == 5
    delete_statement, delete_parameters = session.statements[0]
    deactivate_statement, deactivate_parameters = session.statements[1]
    restore_statement, restore_parameters = session.statements[2]
    alias_statement, alias_parameters = session.statements[3]
    insert_statement, insert_parameters = session.statements[4]
    assert "DELETE FROM provider_mappings" in delete_statement
    assert delete_parameters == (
        account_uuid,
        "topic",
        workspace_uuid,
        "42:new-topic",
    )
    assert "UPDATE provider_mapping_aliases" in deactivate_statement
    assert "workspace_uuid = %s AND provider_id <> %s" in deactivate_statement
    assert deactivate_parameters == (
        account_uuid,
        "topic",
        workspace_uuid,
        "42:new-topic",
    )
    assert "UPDATE provider_mapping_aliases" in restore_statement
    assert "provider_id = %s" in restore_statement
    assert "workspace_uuid <> %s" in restore_statement
    assert "workspace_uuid = ANY(%s)" in restore_statement
    assert restore_parameters == (
        json.dumps({"name": "new topic"}),
        account_uuid,
        "topic",
        "42:new-topic",
        workspace_uuid,
        [workspace_uuid],
    )
    assert "INSERT INTO provider_mapping_aliases" in alias_statement
    assert "mapping.workspace_uuid <> %s" in alias_statement
    assert "provider_mappings.metadata || EXCLUDED.metadata" in insert_statement
    assert "mapping.workspace_uuid = ANY(%s)" in alias_statement
    assert alias_parameters == (
        "42:new-topic",
        json.dumps({"name": "new topic"}),
        account_uuid,
        "topic",
        "42:new-topic",
        workspace_uuid,
        [workspace_uuid],
    )
    assert "INSERT INTO provider_mappings" in insert_statement
    assert "WITH removed_stale_workspace_mapping" not in insert_statement
    assert insert_parameters[:4] == (
        account_uuid,
        "topic",
        workspace_uuid,
        "42:new-topic",
    )
    assert json.loads(insert_parameters[4]) == {"name": "new topic"}


def test_provider_topic_rename_moves_workspace_aliases_to_current_provider_id():
    account_uuid = str(uuid.uuid4())
    workspace_uuid = str(uuid.uuid4())
    row = {
        "workspace_uuid": workspace_uuid,
        "provider_id": "42:renamed",
        "provider_revision": "2",
        "metadata": {"name": "renamed"},
    }

    class RenameSession(Session):
        def execute(self, statement, parameters=None):
            self.statements.append((statement, parameters))
            if "WITH existing_target AS" in statement:
                return Result(({**row, "mapping_renamed": True},))
            return Result()

    session = RenameSession()
    store = _store_with_session(session)

    assert (
        store.rename_provider_mapping(
            account_uuid,
            "topic",
            "42:old",
            "42:renamed",
            {"name": "renamed"},
            "2",
        )
        == row
    )

    assert len(session.statements) == 3
    lock_statement, lock_parameters = session.statements[0]
    primary_statement, primary_parameters = session.statements[1]
    alias_statement, alias_parameters = session.statements[2]
    assert "pg_advisory_xact_lock" in lock_statement
    assert lock_parameters == (f"{account_uuid}:topic:42:renamed",)
    assert "existing_target AS" in primary_statement
    assert "metadata = CASE" in primary_statement
    assert "deleted = false" in primary_statement
    assert "UPDATE provider_mappings AS mapping" in primary_statement
    assert primary_parameters == (
        "2",
        json.dumps({"name": "renamed"}),
        account_uuid,
        "topic",
        "42:renamed",
        "42:renamed",
        "2",
        json.dumps({"name": "renamed"}),
        account_uuid,
        "topic",
        "42:old",
    )
    assert "UPDATE provider_mapping_aliases" in alias_statement
    assert alias_parameters == (
        "42:renamed",
        json.dumps({"name": "renamed"}),
        account_uuid,
        "topic",
        "42:old",
    )


def test_provider_topic_rename_returns_existing_target_without_mutating_source():
    account_uuid = str(uuid.uuid4())
    target_workspace_uuid = str(uuid.uuid4())
    target = {
        "workspace_uuid": target_workspace_uuid,
        "provider_id": "42:renamed",
        "provider_revision": "2",
        "metadata": {"name": "renamed", "workspace_delivery_state": "committed"},
        "mapping_renamed": False,
    }

    class ExistingTargetSession(Session):
        def execute(self, statement, parameters=None):
            self.statements.append((statement, parameters))
            if "WITH existing_target AS" in statement:
                return Result((target,))
            return Result()

    session = ExistingTargetSession()
    store = _store_with_session(session)

    assert store.rename_provider_mapping(
        account_uuid,
        "topic",
        "42:old",
        "42:renamed",
        {"name": "renamed"},
        "3",
    ) == {
        "workspace_uuid": target_workspace_uuid,
        "provider_id": "42:renamed",
        "provider_revision": "2",
        "metadata": {"name": "renamed", "workspace_delivery_state": "committed"},
    }

    assert len(session.statements) == 2
    assert "pg_advisory_xact_lock" in session.statements[0][0]
    assert "existing_target AS" in session.statements[1][0]


def test_workspace_projection_contract_materializes_first_outbound_mappings():
    session = Session(
        (
            {
                "participants": [
                    {
                        "provider_user_id": "2",
                        "_provider_active": False,
                    }
                ]
            },
        )
    )
    account_uuid = str(uuid.uuid4())
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    owner_uuid = str(uuid.uuid4())
    peer_uuid = str(uuid.uuid4())
    assignment = {
        "external_account_uuid": account_uuid,
        "project_id": str(uuid.uuid4()),
        "provider_chat": {
            "kind": "zulip",
            "chat_type": "direct",
            "provider_chat_key": "direct:1,2",
        },
        "workspace_projection": {
            "stream": {
                "uuid": stream_uuid,
                "name": "Owner, Peer",
                "description": "",
                "chat_kind": "personal_dm",
                "private": True,
                "default_topic_uuid": topic_uuid,
            },
            "participants": [
                {
                    "identity_uuid": owner_uuid,
                    "provider_user_id": "1",
                    "display_name": "Owner",
                    "email": "owner@example.invalid",
                    "avatar_urn": None,
                    "role": "owner",
                },
                {
                    "identity_uuid": peer_uuid,
                    "provider_user_id": "2",
                    "display_name": "Peer",
                    "email": "peer@example.invalid",
                    "avatar_urn": None,
                    "role": "member",
                },
            ],
            "topics": [
                {
                    "topic_uuid": topic_uuid,
                    "provider_topic_id": "direct:1,2:default",
                    "name": "Zulip",
                    "is_default": True,
                }
            ],
        },
    }

    storage.RestAlchemyStore._materialize_workspace_projection(session, assignment)

    assert len(session.statements) == 12
    inserts = [
        parameters
        for statement, parameters in session.statements
        if "INSERT INTO provider_mappings" in statement
    ]
    identity_parameters = inserts[0]
    peer_identity_parameters = inserts[1]
    stream_parameters = inserts[2]
    topic_parameters = inserts[3]
    assert identity_parameters[:4] == (
        account_uuid,
        "identity",
        owner_uuid,
        "1",
    )
    assert json.loads(identity_parameters[4])["active"] is True
    assert json.loads(peer_identity_parameters[4])["active"] is False
    assert stream_parameters[:4] == (
        account_uuid,
        "stream",
        stream_uuid,
        "direct:1,2",
    )
    assert topic_parameters[:4] == (
        account_uuid,
        "topic",
        topic_uuid,
        "direct:1,2:default",
    )
    stream_metadata = json.loads(stream_parameters[4])
    assert stream_metadata["participants"] == [owner_uuid, peer_uuid]
    assert stream_metadata["default_topic_uuid"] == topic_uuid


def test_exact_backend_assignment_fixture_materializes_owned_topology():
    fixture = json.loads(
        (
            pathlib.Path(__file__).parent
            / "fixtures"
            / "backend_external_chat_assignment.json"
        ).read_text(encoding="utf-8")
    )
    session = Session()
    storage.RestAlchemyStore._materialize_workspace_projection(session, fixture)
    assert len(session.statements) == 17
    materialized = []
    for statement, parameters in session.statements:
        if "INSERT INTO provider_mappings" not in statement:
            continue
        materialized.append(
            (
                parameters[1],
                parameters[2],
                parameters[3],
                json.loads(parameters[4]),
            )
        )
    stream_mapping = next(value for value in materialized if value[0] == "stream")
    assert stream_mapping[1:3] == (
        "60000000-0000-4000-8000-000000000006",
        "channel:42",
    )
    stream_metadata = stream_mapping[3]
    assert stream_metadata["private"] is False
    assert stream_metadata["description"] == "Backend-owned engineering projection"
    assert stream_metadata["default_topic_uuid"] == (
        "70000000-0000-4000-8000-000000000007"
    )
    assert stream_metadata["participants"] == [
        "80000000-0000-4000-8000-000000000008",
        "81000000-0000-4000-8000-000000000081",
    ]
    assert fixture["provider_chat"] == {
        "chat_type": "channel",
        "kind": "zulip",
        "provider_chat_key": "channel:42",
    }
    assert {(value[2], value[1]) for value in materialized if value[0] == "topic"} == {
        ("42:general", "70000000-0000-4000-8000-000000000007"),
        ("42:deployments", "71000000-0000-4000-8000-000000000071"),
    }
    assert {
        (value[2], value[1]) for value in materialized if value[0] == "identity"
    } == {
        ("100", "80000000-0000-4000-8000-000000000008"),
        ("101", "81000000-0000-4000-8000-000000000081"),
    }


def test_projection_tombstone_includes_all_assignment_owned_entities():
    fixture = json.loads(
        (
            pathlib.Path(__file__).parent
            / "fixtures"
            / "backend_external_chat_assignment.json"
        ).read_text(encoding="utf-8")
    )
    session = Session()
    storage.RestAlchemyStore._tombstone_workspace_projection(session, fixture)
    statement, parameters = session.statements[0]
    assert "entity_kind = 'identity'" in statement
    assert "entity_kind = 'stream'" in statement
    assert "entity_kind = 'topic'" in statement
    assert "metadata->>'stream_uuid'" not in statement
    assert set(parameters[-2]) == {
        "70000000-0000-4000-8000-000000000007",
        "71000000-0000-4000-8000-000000000071",
    }
    assert set(parameters[-1]) == {
        "80000000-0000-4000-8000-000000000008",
        "81000000-0000-4000-8000-000000000081",
    }
    alias_statement, alias_parameters = session.statements[1]
    assert "UPDATE provider_mapping_aliases" in alias_statement
    assert "metadata->>'stream_uuid' = %s" in alias_statement
    assert alias_parameters == (
        fixture["external_account_uuid"],
        [
            "70000000-0000-4000-8000-000000000007",
            "71000000-0000-4000-8000-000000000071",
        ],
        fixture["workspace_projection"]["stream"]["uuid"],
    )


def test_backfill_depth_is_assignment_owned():
    session = Session()
    store = _store_with_session(session)
    store.reconcile_backfill_jobs()
    statement = session.statements[0][0]
    assert "assignment.body->>'history_depth'" in statement
    assert "account.body->'settings'->>'history_depth'" not in statement


def test_backfill_reconcile_cancels_inactive_accounts_and_clears_stale_health():
    session = Session()
    store = _store_with_session(session)

    store.reconcile_backfill_jobs()

    cancellation = " ".join(session.statements[1][0].split())
    cleanup = " ".join(session.statements[2][0].split())
    assert "JOIN desired_resources AS account" in cancellation
    assert "AND NOT account.deleted" in cancellation
    assert "DELETE FROM bridge_health AS health" in cleanup
    assert "job.state <> 'failed'" in cleanup
    assert "job.account_uuid::text" in cleanup


def test_advance_backfill_job_clears_its_health_component():
    session = Session()
    store = _store_with_session(session)
    account_uuid = "00000000-0000-4000-8000-000000000001"

    store.advance_backfill_job(account_uuid, "direct:100,200", None, True)

    assert len(session.statements) == 2
    cleanup, parameters = session.statements[1]
    assert "DELETE FROM bridge_health" in cleanup
    assert parameters == (
        storage.backfill_health_component(account_uuid, "direct:100,200"),
    )


def test_committed_message_mapping_preserves_workspace_alias():
    session = Session()
    record = {
        "account_uuid": str(uuid.uuid4()),
        "project_uuid": str(uuid.uuid4()),
        "origin": "workspace",
        "causal_lane": f"message:{uuid.uuid4()}",
        "operation": {
            "kind": "message.create",
            "entity_uuid": str(uuid.uuid4()),
            "provider": {"chat_id": "channel:42"},
            "payload": {
                "stream_uuid": str(uuid.uuid4()),
                "topic_uuid": str(uuid.uuid4()),
                "author_uuid": str(uuid.uuid4()),
            },
        },
    }
    storage.RestAlchemyStore._persist_committed_mapping(session, record, "99", None)
    lock_statement, lock_parameters = session.statements[0]
    assert "pg_advisory_xact_lock" in lock_statement
    assert lock_parameters == (
        storage._provider_mapping_lock_key(record["account_uuid"], "message", "99"),
    )
    assert (
        "ON CONFLICT (account_uuid, entity_kind, provider_id)"
        in session.statements[1][0]
    )
    assert "INSERT INTO provider_mapping_aliases" in session.statements[2][0]


def test_committed_message_update_mapping_uses_provider_mapping_lock():
    session = Session()
    record = {
        "account_uuid": str(uuid.uuid4()),
        "project_uuid": str(uuid.uuid4()),
        "operation": {
            "kind": "message.update",
            "entity_uuid": str(uuid.uuid4()),
            "provider": {
                "chat_id": "channel:43",
                "entity_id": "601",
            },
            "payload": {
                "stream_uuid": str(uuid.uuid4()),
                "topic_uuid": str(uuid.uuid4()),
            },
            "extensions": {},
        },
    }

    storage.RestAlchemyStore._persist_committed_mapping(
        session,
        record,
        "601",
        None,
    )

    lock_statement, lock_parameters = session.statements[0]
    assert "pg_advisory_xact_lock" in lock_statement
    assert lock_parameters == (
        storage._provider_mapping_lock_key(
            record["account_uuid"], "message", "601"
        ),
    )
    assert "UPDATE provider_mappings" in session.statements[1][0]


def test_remembered_message_mapping_uses_provider_mapping_lock():
    session = Session()
    store = _store_with_session(session)

    store.remember_provider_mapping(
        "account",
        "message",
        "601",
        str(uuid.uuid4()),
        {"chat_key": "channel:42"},
    )

    lock_statement, lock_parameters = session.statements[0]
    assert "pg_advisory_xact_lock" in lock_statement
    assert lock_parameters == (
        storage._provider_mapping_lock_key("account", "message", "601"),
    )
    assert "INSERT INTO provider_mappings" in session.statements[1][0]


def test_committed_reaction_mapping_preserves_workspace_identity():
    session = Session()
    record = {
        "account_uuid": str(uuid.uuid4()),
        "project_uuid": str(uuid.uuid4()),
        "origin": "workspace",
        "operation": {
            "kind": "reaction.create",
            "entity_uuid": str(uuid.uuid4()),
            "provider": {"chat_id": "channel:42"},
            "payload": {
                "message_uuid": str(uuid.uuid4()),
                "user_uuid": str(uuid.uuid4()),
                "emoji_name": "heart",
            },
        },
    }

    storage.RestAlchemyStore._persist_committed_mapping(
        session,
        record,
        "99:1:heart",
        None,
    )

    statement, parameters = session.statements[0]
    assert "VALUES (%s, 'reaction', %s, %s, %s, %s, false)" in statement
    assert parameters[4:6] == (
        record["operation"]["entity_uuid"],
        "99:1:heart",
    )


def test_reaction_convergence_drops_deleted_canonical_even_for_same_uuid():
    account_uuid = str(uuid.uuid4())
    reaction_uuid = str(uuid.uuid4())
    canonical_provider_id = "601:2:unicode_emoji:270d"
    legacy_provider_id = "601:2:writing"
    metadata = {
        "emoji_name": "✍",
        "provider_emoji_name": "writing",
        "emoji_code": "270d",
        "reaction_type": "unicode_emoji",
    }
    session = Session(
        (
            {
                "workspace_uuid": reaction_uuid,
                "provider_id": legacy_provider_id,
                "provider_revision": None,
                "metadata": metadata,
                "deleted": False,
                "updated_at": datetime.datetime.now(datetime.UTC),
            },
            {
                "workspace_uuid": reaction_uuid,
                "provider_id": canonical_provider_id,
                "provider_revision": None,
                "metadata": metadata,
                "deleted": True,
                "updated_at": datetime.datetime.now(datetime.UTC),
            },
        )
    )
    store = _store_with_session(session)

    mapping, displaced = store._converge_reaction_mapping(
        session,
        account_uuid,
        "601",
        "2",
        canonical_provider_id,
        legacy_provider_id,
        reaction_uuid,
        metadata,
    )

    assert mapping is not None
    assert str(mapping["workspace_uuid"]) == reaction_uuid
    assert displaced == []
    delete_statement, delete_parameters = session.statements[2]
    assert "provider_id = %s AND deleted" in delete_statement
    assert "workspace_uuid <>" not in delete_statement
    assert delete_parameters == (account_uuid, canonical_provider_id)
    update_statement, update_parameters = session.statements[3]
    assert "workspace_uuid = %s AND provider_id = %s" in update_statement
    assert update_parameters[-2:] == (reaction_uuid, legacy_provider_id)


def test_stale_assignment_delivery_is_removed_and_provider_event_replayed():
    session = Session()
    store = _store_with_session(session)
    assert store.reset_stale_workspace_deliveries() == 0
    statement = next(
        statement
        for statement, _parameters in session.statements
        if "DELETE FROM workspace_delivery_outbox" in statement
    )
    assert "assignment.generation" in statement
    assert "delivery.assignment_generation" in statement
    assert "assignment.body->>'project_id'" in statement
    assert "RETURNING operation_uuid" in statement
    assert "priority, record" in statement
    assert "assignment_project_uuid" in statement
