import contextlib
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
    assert "lease_until IS NULL OR lease_until < now()" in statement


def test_workspace_delivery_outbox_orders_live_before_backfill():
    session = Session()
    store = _store_with_session(session)

    assert (
        store.pending_workspace_deliveries(
            minimum_priority=2, maximum_priority=2, limit=101
        )
        == []
    )
    statement = session.statements[0][0]
    assert "submission_state IN ('pending', 'ambiguous')" in statement
    assert "submission_state = 'awaiting_result'" in statement
    assert "next_submission_at <= now()" in statement
    assert "delivery.priority BETWEEN %s AND %s" in statement
    assert session.statements[0][1] == (2, 2, 101)
    assert "ORDER BY priority, created_at" in statement


class SharedDeliverySession:
    def __init__(self):
        self.operations = {}
        self.deliveries = {}

    def execute(self, statement, parameters=None):
        normalized = " ".join(statement.split())
        if normalized.startswith("SELECT generation FROM desired_resources"):
            return Result(({"generation": 1},))
        if normalized.startswith("SELECT operation_sha256 FROM operation_idempotency"):
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


def test_initial_backfill_gate_ignores_delivery_outcomes_from_older_generation():
    session = Session(({"ready": True},))
    store = _store_with_session(session)

    assert store.initial_backfill_ready("00000000-0000-4000-8000-000000000001")
    statement = session.statements[0][0]
    assert "account.resource_uuid = delivery.account_uuid" in statement
    assert "delivery.account_generation = account.generation" in statement


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


def test_terminal_claim_sweeps_expired_and_superseded_pending_work():
    session = Session()
    store = _store_with_session(session)

    assert store.claim_terminal("worker") is None

    statement = session.statements[0][0]
    assert "operation.expires_at <= now()" in statement
    assert "assignment.generation <> operation.assignment_generation" in statement
    assert "assignment.body->>'project_id'" in statement
    assert "account.body->>'synchronization_enabled'" in statement


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


def test_workspace_projection_contract_materializes_first_outbound_mappings():
    session = Session()
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
                    "name": "default",
                    "is_default": True,
                }
            ],
        },
    }

    storage.RestAlchemyStore._materialize_workspace_projection(session, assignment)

    assert len(session.statements) == 4
    identity_parameters = session.statements[0][1]
    stream_parameters = session.statements[2][1]
    topic_parameters = session.statements[3][1]
    assert identity_parameters[:3] == (account_uuid, owner_uuid, "1")
    assert stream_parameters[:3] == (account_uuid, stream_uuid, "direct:1,2")
    assert topic_parameters[:3] == (
        account_uuid,
        topic_uuid,
        "direct:1,2:default",
    )
    stream_metadata = json.loads(stream_parameters[6])
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
    assert len(session.statements) == 5
    materialized = []
    for statement, parameters in session.statements:
        entity_kind = next(
            kind for kind in ("identity", "stream", "topic") if f"'{kind}'" in statement
        )
        materialized.append(
            (entity_kind, parameters[4], parameters[5], json.loads(parameters[6]))
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
    assert set(parameters[-1]) == {
        "80000000-0000-4000-8000-000000000008",
        "81000000-0000-4000-8000-000000000081",
    }


def test_backfill_depth_is_assignment_owned():
    session = Session()
    store = _store_with_session(session)
    store.reconcile_backfill_jobs()
    statement = session.statements[0][0]
    assert "assignment.body->>'history_depth'" in statement
    assert "account.body->'settings'->>'history_depth'" not in statement


def test_committed_message_mapping_preserves_workspace_alias():
    session = Session()
    record = {
        "account_uuid": str(uuid.uuid4()),
        "project_uuid": str(uuid.uuid4()),
        "origin": "workspace",
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
    assert (
        "ON CONFLICT (account_uuid, entity_kind, provider_id)"
        in session.statements[0][0]
    )
    assert "INSERT INTO provider_mapping_aliases" in session.statements[1][0]


def test_stale_assignment_delivery_is_removed_and_provider_event_replayed():
    session = Session()
    store = _store_with_session(session)
    assert store.reset_stale_workspace_deliveries() == 0
    statement = session.statements[0][0]
    assert "assignment.generation" in statement
    assert "delivery.assignment_generation" in statement
    assert "assignment.body->>'project_id'" in statement
    assert "RETURNING operation_uuid" in statement
