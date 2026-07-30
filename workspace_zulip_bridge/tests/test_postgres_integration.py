import datetime
import json
import os
import pathlib
import subprocess
import sys
import uuid

import pytest

from workspace_zulip_bridge import (
    canonical,
    converter,
    provider_protocol,
    service,
    storage,
)

ROOT = pathlib.Path(__file__).parents[2]
MIGRATIONS = ROOT / "migrations"


def _apply_migrations(connection_url: str, config_path: pathlib.Path) -> None:
    config_path.write_text(
        f"[db]\nconnection_url = {connection_url}\n",
        encoding="utf-8",
    )
    config_path.chmod(0o600)
    executable = pathlib.Path(sys.executable).with_name("ra-apply-migration")
    result = subprocess.run(
        [
            str(executable),
            "--config-file",
            str(config_path),
            "--path",
            str(MIGRATIONS),
        ],
        check=False,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture(scope="session")
def migrated_postgres_dsn(tmp_path_factory):
    dsn = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    config_path = tmp_path_factory.mktemp("bridge-migrations") / "bridge.conf"
    _apply_migrations(dsn, config_path)
    return dsn


@pytest.fixture()
def postgres_store(migrated_postgres_dsn):
    store = storage.RestAlchemyStore(migrated_postgres_dsn)
    with store.session() as session:
        session.execute(
            """
            TRUNCATE desired_resources, provider_mappings,
                     provider_mapping_aliases, zulip_backfill_jobs,
                     zulip_queue_catchup_jobs, zulip_participant_sync,
                     workspace_delivery_outbox,
                     operation_idempotency, producer_lane_counters,
                     producer_operations, causal_lane_state, bridge_operations,
                     scheduler_accounts, observed_report_outbox,
                     zulip_provider_events, zulip_event_cursors CASCADE
            """
        )
    return store


def _explain_text(session, query: str, parameters=()) -> str:
    rows = session.execute(
        f"EXPLAIN (COSTS OFF) {query}",
        parameters,
    ).fetchall()
    return "\n".join(str(next(iter(row.values()))) for row in rows)


def _insert_account_and_assignment(
    store: storage.RestAlchemyStore, history_depth: str = "30_days"
) -> tuple[str, str]:
    account_uuid = str(uuid.uuid4())
    assignment_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    account = {
        "uuid": account_uuid,
        "generation": 1,
        "owner_user_uuid": str(uuid.uuid4()),
        "synchronization_enabled": True,
        "settings": {
            "selection_mode": "all",
            "default_project_id": project_uuid,
        },
    }
    assignment = {
        "uuid": assignment_uuid,
        "generation": 1,
        "external_account_uuid": account_uuid,
        "project_id": project_uuid,
        "selected": True,
        "history_depth": history_depth,
        "provider_chat": {
            "provider_chat_key": "channel:42",
            "chat_type": "channel",
        },
    }
    with store.session() as session:
        session.execute(
            """
            INSERT INTO desired_resources (
                resource_type, resource_uuid, generation, body, deleted
            ) VALUES ('external_account', %s, 1, %s, false),
                     ('external_chat_assignment', %s, 1, %s, false)
            """,
            (
                account_uuid,
                json.dumps(account),
                assignment_uuid,
                json.dumps(assignment),
            ),
        )
    store.update_provider_event_cursor(
        account_uuid,
        "test-registration",
        0,
        provider_realm_uuid=str(uuid.uuid4()),
        provider_owner_user_id="1",
        provider_account_generation=1,
    )
    store.reconcile_participant_sync()
    participant_job = store.claim_participant_sync()
    assert participant_job is not None
    store.complete_participant_sync(
        account_uuid,
        "channel:42",
        1,
        [],
        True,
    )
    return account_uuid, project_uuid


def _materialize_channel_projection(
    store: storage.RestAlchemyStore, account_uuid: str, project_uuid: str
) -> tuple[str, str, str]:
    account = store.account_resource(account_uuid)
    owner_uuid = str(account["owner_user_uuid"])
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    author_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        account_uuid,
        "identity",
        "2",
        author_uuid,
        {"display_name": "Other User", "active": True},
    )
    store.remember_provider_mapping(
        account_uuid,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": project_uuid,
            "participants": sorted([owner_uuid, author_uuid]),
            "name": "Engineering",
            "description": "",
            "private": True,
            "default_topic_uuid": None,
        },
    )
    store.remember_provider_mapping(
        account_uuid,
        "topic",
        "42:Topic",
        topic_uuid,
        {"stream_uuid": stream_uuid, "chat_key": "channel:42"},
    )
    return stream_uuid, topic_uuid, author_uuid


def _provider_history_message(provider_message_id: int) -> dict[str, object]:
    return {
        "id": provider_message_id,
        "type": "stream",
        "stream_id": 42,
        "display_recipient": "Engineering",
        "sender_id": 2,
        "sender_full_name": "Other User",
        "sender_email": "other@example.invalid",
        "subject": "Topic",
        "timestamp": 1_700_000_000,
        "content": "hello",
    }


def _backfill_service(store: storage.RestAlchemyStore):
    class Adapter:
        server_url = "https://zulip.example.invalid"

    instance = object.__new__(service.BridgeService)
    instance.store = store
    instance.file_client = None
    instance.provider_adapters = lambda account_uuid: Adapter()
    return instance


def test_desired_assignment_can_replace_provider_id_for_workspace_uuid(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    assignment_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())

    def change(generation, provider_topic_id, topic_name):
        resource = {
            "resource_type": "external_chat_assignment",
            "uuid": assignment_uuid,
            "generation": generation,
            "external_account_uuid": account_uuid,
            "project_id": project_uuid,
            "provider_chat": {
                "provider_chat_key": "channel:42",
                "chat_type": "channel",
            },
            "workspace_projection": {
                "stream": {
                    "uuid": stream_uuid,
                    "name": "Engineering",
                    "description": "",
                    "private": False,
                    "default_topic_uuid": None,
                },
                "participants": [],
                "topics": [
                    {
                        "topic_uuid": topic_uuid,
                        "provider_topic_id": provider_topic_id,
                        "name": topic_name,
                        "is_default": False,
                    }
                ],
            },
        }
        return {
            "change_uuid": str(uuid.uuid4()),
            "sequence": generation,
            "resource_type": "external_chat_assignment",
            "resource_uuid": assignment_uuid,
            "operation": "upsert",
            "generation": generation,
            "required_capabilities": {},
            "resource": resource,
        }

    postgres_store.apply_desired_changes(
        [change(1, "42:old-topic", "old topic")],
        "cursor-1",
    )
    postgres_store.apply_desired_changes(
        [change(2, "42:new-topic", "new topic")],
        "cursor-2",
    )

    assert postgres_store.provider_mapping(
        account_uuid, "topic", "42:old-topic"
    ) is None
    replacement = postgres_store.provider_mapping(
        account_uuid, "topic", "42:new-topic"
    )
    assert replacement is not None
    assert str(replacement["workspace_uuid"]) == topic_uuid
    assert replacement["metadata"]["name"] == "new topic"
    assert replacement["metadata"]["stream_uuid"] == stream_uuid
    assert postgres_store.control_cursor() == "cursor-2"


def test_assignment_projection_repair_restores_mapping_and_wakes_event(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    assignment_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    queue_id = "queue"
    event_id = 17
    resource = {
        "resource_type": "external_chat_assignment",
        "uuid": assignment_uuid,
        "generation": 1,
        "external_account_uuid": account_uuid,
        "project_id": project_uuid,
        "provider_chat": {
            "provider_chat_key": "channel:42",
            "chat_type": "channel",
        },
        "workspace_projection": {
            "stream": {
                "uuid": stream_uuid,
                "name": "Engineering",
                "description": "",
                "private": False,
                "default_topic_uuid": None,
            },
            "participants": [],
            "topics": [
                {
                    "topic_uuid": topic_uuid,
                    "provider_topic_id": "42:Topic",
                    "name": "Topic",
                    "is_default": False,
                }
            ],
        },
    }
    postgres_store.apply_desired_changes(
        [
            {
                "change_uuid": str(uuid.uuid4()),
                "sequence": 1,
                "resource_type": "external_chat_assignment",
                "resource_uuid": assignment_uuid,
                "operation": "upsert",
                "generation": 1,
                "required_capabilities": {},
                "resource": resource,
            }
        ],
        "cursor-1",
    )
    with postgres_store.session() as session:
        session.execute(
            """
            DELETE FROM provider_mappings
            WHERE account_uuid = %s AND entity_kind IN ('stream', 'topic')
            """,
            (account_uuid,),
        )
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state, processing_reason, available_at
            ) VALUES (
                %s, %s, %s, 'message', '{}'::jsonb, 'pending',
                'provider_chat_assignment_pending',
                now() + interval '5 minutes'
            )
            """,
            (account_uuid, queue_id, event_id),
        )

    assert postgres_store.reconcile_assignment_projection(
        account_uuid, "channel:42"
    )
    topic = postgres_store.provider_mapping(
        account_uuid, "topic", "42:Topic"
    )
    assert topic is not None
    assert str(topic["workspace_uuid"]) == topic_uuid
    with postgres_store.session() as session:
        event = session.execute(
            """
            SELECT available_at <= now() AS due
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert event["due"]


def test_topic_recanonicalization_keeps_previous_workspace_route(postgres_store):
    account_uuid = str(uuid.uuid4())
    assignment_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    stream_uuid = str(uuid.uuid4())
    original_topic_uuid = str(uuid.uuid4())
    canonical_topic_uuid = str(uuid.uuid4())

    def change(generation, topic_uuid, provider_topic_id, topic_name):
        resource = {
            "resource_type": "external_chat_assignment",
            "uuid": assignment_uuid,
            "generation": generation,
            "external_account_uuid": account_uuid,
            "project_id": project_uuid,
            "provider_chat": {
                "provider_chat_key": "channel:42",
                "chat_type": "channel",
            },
            "workspace_projection": {
                "stream": {
                    "uuid": stream_uuid,
                    "name": "Engineering",
                    "description": "",
                    "private": False,
                    "default_topic_uuid": None,
                },
                "participants": [],
                "topics": [
                    {
                        "topic_uuid": topic_uuid,
                        "provider_topic_id": provider_topic_id,
                        "name": topic_name,
                        "is_default": False,
                    }
                ],
            },
        }
        return {
            "change_uuid": str(uuid.uuid4()),
            "sequence": generation,
            "resource_type": "external_chat_assignment",
            "resource_uuid": assignment_uuid,
            "operation": "upsert",
            "generation": generation,
            "required_capabilities": {},
            "resource": resource,
        }

    postgres_store.apply_desired_changes(
        [change(1, original_topic_uuid, "42:topic", "topic")],
        "cursor-1",
    )
    renamed = postgres_store.rename_provider_mapping(
        account_uuid,
        "topic",
        "42:topic",
        "42:resolved topic",
        {
            "stream_uuid": stream_uuid,
            "chat_key": "channel:42",
            "name": "resolved topic",
            "is_default": False,
        },
        "2",
    )
    assert renamed is not None
    assert str(renamed["workspace_uuid"]) == original_topic_uuid

    postgres_store.apply_desired_changes(
        [
            change(
                2,
                canonical_topic_uuid,
                "42:resolved topic",
                "resolved topic",
            )
        ],
        "cursor-2",
    )

    current = postgres_store.provider_mapping(
        account_uuid, "topic", "42:resolved topic"
    )
    assert current is not None
    assert str(current["workspace_uuid"]) == canonical_topic_uuid
    previous_route = postgres_store.workspace_mapping(
        account_uuid, "topic", original_topic_uuid
    )
    assert previous_route is not None
    assert previous_route["provider_id"] == "42:resolved topic"
    assert previous_route["metadata"]["name"] == "resolved topic"
    canonical_route = postgres_store.workspace_mapping(
        account_uuid, "topic", canonical_topic_uuid
    )
    assert canonical_route is not None
    assert canonical_route["provider_id"] == "42:resolved topic"

    postgres_store.apply_desired_changes(
        [
            change(
                3,
                canonical_topic_uuid,
                "42:resolved topic",
                "resolved topic",
            )
        ],
        "cursor-3",
    )
    retained_route = postgres_store.workspace_mapping(
        account_uuid, "topic", original_topic_uuid
    )
    assert retained_route is not None
    assert retained_route["provider_id"] == "42:resolved topic"

    retained_snapshot = dict(
        change(
            4,
            canonical_topic_uuid,
            "42:resolved topic",
            "resolved topic",
        )["resource"]
    )
    retained_snapshot["required_capabilities"] = {}
    postgres_store.install_snapshot([retained_snapshot], "cursor-4")
    snapshot_route = postgres_store.workspace_mapping(
        account_uuid, "topic", original_topic_uuid
    )
    assert snapshot_route is not None
    assert snapshot_route["provider_id"] == "42:resolved topic"

    postgres_store.rename_provider_mapping(
        account_uuid,
        "topic",
        "42:resolved topic",
        "42:renamed again",
        {
            "stream_uuid": stream_uuid,
            "chat_key": "channel:42",
            "name": "renamed again",
            "is_default": False,
        },
        "5",
    )

    mixed_projection = change(
        5,
        canonical_topic_uuid,
        "42:renamed again",
        "renamed again",
    )
    projection = mixed_projection["resource"]["workspace_projection"]
    projection["topics"].insert(
        0,
        {
            "topic_uuid": original_topic_uuid,
            "provider_topic_id": "42:resolved topic",
            "name": "resolved topic",
            "is_default": False,
        },
    )
    postgres_store.apply_desired_changes([mixed_projection], "cursor-5")

    for topic_uuid in (original_topic_uuid, canonical_topic_uuid):
        route = postgres_store.workspace_mapping(account_uuid, "topic", topic_uuid)
        assert route is not None
        assert route["provider_id"] == "42:renamed again"
        assert route["metadata"]["name"] == "renamed again"


def test_observed_report_state_can_recover_to_a_previous_value(postgres_store):
    resource_uuid = str(uuid.uuid4())
    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store

    instance._queue_observed_report(
        "external_account", resource_uuid, 1, "live_ready", "live"
    )
    instance._queue_observed_report(
        "external_account", resource_uuid, 1, "live_ready", "live"
    )
    instance._queue_observed_report(
        "external_account",
        resource_uuid,
        1,
        "degraded",
        "retry",
        safe_error_code="bad_event_queue_id",
    )
    instance._queue_observed_report(
        "external_account", resource_uuid, 1, "live_ready", "live"
    )

    with postgres_store.session() as session:
        rows = session.execute(
            """
            SELECT report_uuid, body->>'status' AS status
            FROM observed_report_outbox
            WHERE body->>'resource_uuid' = %s
            ORDER BY created_at
            """,
            (resource_uuid,),
        ).fetchall()

    assert [row["status"] for row in rows] == [
        "live_ready",
        "degraded",
        "live_ready",
    ]
    assert len({row["report_uuid"] for row in rows}) == 3


def test_pending_observed_reports_supersede_older_unsent_resource_states(
    postgres_store,
):
    resource_uuid = str(uuid.uuid4())
    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance._queue_observed_report(
        "external_account", resource_uuid, 1, "live_ready", "live"
    )
    instance._queue_observed_report(
        "external_account",
        resource_uuid,
        1,
        "degraded",
        "retry",
        safe_error_code="provider_unavailable",
    )
    instance._queue_observed_report(
        "external_account", resource_uuid, 1, "live_ready", "live"
    )

    pending = postgres_store.pending_observed_reports()

    assert len(pending) == 1
    assert pending[0]["status"] == "live_ready"
    with postgres_store.session() as session:
        rows = session.execute(
            """
            SELECT result_status, count(*) AS count
            FROM observed_report_outbox
            WHERE body->>'resource_uuid' = %s
            GROUP BY result_status
            ORDER BY result_status NULLS FIRST
            """,
            (resource_uuid,),
        ).fetchall()
    assert rows == [
        {"result_status": None, "count": 1},
        {"result_status": "superseded", "count": 2},
    ]


def test_observed_report_hot_queries_use_migration_indexes(postgres_store):
    resource_uuid = str(uuid.uuid4())
    report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_account",
        "resource_uuid": resource_uuid,
        "observed_generation": 1,
        "status": "backfill",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    assert postgres_store.enqueue_observed_report(report)

    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO observed_report_outbox (
                report_uuid, body, completed_at
            )
            SELECT
                md5('completed-report-' || sample::text)::uuid,
                jsonb_build_object(
                    'resource_type', 'external_account',
                    'resource_uuid',
                    md5('completed-resource-' || sample::text)::uuid::text
                ),
                now()
            FROM generate_series(1, 1000) AS sample
            ON CONFLICT (report_uuid) DO NOTHING
            """
        )
        session.execute("ANALYZE observed_report_outbox")
        dedup_plan = _explain_text(
            session,
            """
            SELECT body FROM observed_report_outbox
            WHERE body->>'resource_type' = %s
              AND body->>'resource_uuid' = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            ("external_account", resource_uuid),
        )
        pending_rank_plan = _explain_text(
            session,
            """
            SELECT report_uuid,
                   row_number() OVER (
                       PARTITION BY body->>'resource_type',
                                    body->>'resource_uuid'
                       ORDER BY created_at DESC, report_uuid DESC
                   ) AS position
            FROM observed_report_outbox
            WHERE completed_at IS NULL
            """,
        )
        pending_order_plan = _explain_text(
            session,
            """
            SELECT body FROM observed_report_outbox
            WHERE completed_at IS NULL AND available_at <= now()
            ORDER BY created_at LIMIT 500
            """,
        )

    assert "observed_report_outbox_resource_latest_idx" in dedup_plan
    assert "observed_report_outbox_pending_order_idx" in pending_rank_plan
    assert "observed_report_outbox_pending_order_idx" in pending_order_plan


def _provider_record(
    account_uuid: str,
    project_uuid: str,
    chat_id: str = "channel:42",
    kind: str = "message.create",
) -> dict[str, object]:
    operation_uuid = str(uuid.uuid4())
    entity_uuid = str(uuid.uuid4())
    operation = {
        "kind": kind,
        "entity_uuid": entity_uuid,
        "actor_uuid": str(uuid.uuid4()),
        "occurred_at": "2026-07-18T12:00:00Z",
        "provider": {
            "kind": "zulip",
            "chat_id": chat_id,
            "entity_id": None,
            "revision": None,
        },
        "payload": (
            {
                "display_name": "User",
                "email": None,
                "avatar_urn": None,
                "active": True,
            }
            if kind == "identity.upsert"
            else {
                "stream_uuid": str(uuid.uuid4()),
                "topic_uuid": str(uuid.uuid4()),
                "author_uuid": str(uuid.uuid4()),
                "payload": {"kind": "markdown", "content": "hello"},
                "reply_to_message_uuid": None,
            }
        ),
        "extensions": {"provider_badge": "zulip"},
    }
    record = {
        "schema": "workspace.provider",
        "schema_version": 1,
        "record_kind": "operation",
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": operation_uuid,
        "attempt": 1,
        "operation_sha256": "",
        "account_uuid": account_uuid,
        "project_uuid": project_uuid,
        "origin": "zulip",
        "causal_lane": f"chat:{account_uuid}:{chat_id}",
        "sequence": 0,
        "predecessor_operation_uuid": None,
        "created_at": "2026-07-18T12:00:00Z",
        "expires_at": None,
        "operation": operation,
    }
    record["operation_sha256"] = canonical.operation_digest(record)
    return record


def _committed_result(
    record: dict[str, object],
    provider_entity_id: str | None = None,
) -> dict[str, object]:
    return {
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": record["operation_uuid"],
        "operation_sha256": record["operation_sha256"],
        "in_reply_to_record_uuid": record["record_uuid"],
        **{
            field: record[field]
            for field in (
                "attempt",
                "account_uuid",
                "project_uuid",
                "origin",
                "causal_lane",
                "sequence",
                "predecessor_operation_uuid",
            )
        },
        "result": {
            "outcome": "committed",
            "provider_entity_id": provider_entity_id,
            "provider_revision": None,
            "manual_retry_allowed": False,
        },
    }


def test_message_delivery_waits_for_one_durable_topic_projection(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _materialize_channel_projection(postgres_store, account_uuid, project_uuid)
    records = []
    for message_id in (101, 102):
        records.extend(
            converter.event_records(
                postgres_store,
                account_uuid,
                "backfill:channel:42",
                {
                    "id": message_id,
                    "type": "message",
                    "message": _provider_history_message(message_id),
                },
                "backfill",
            )
        )
    topic_records = [
        record
        for record in records
        if record["operation"]["kind"] == "topic.upsert"
    ]
    message_records = [
        record
        for record in records
        if record["operation"]["kind"] == "message.create"
    ]
    update_records = converter.event_records(
        postgres_store,
        account_uuid,
        "live:channel:42",
        {
            "id": 103,
            "type": "update_message",
            "message_id": 101,
            "message_ids": [101],
            "stream_id": 42,
            "subject": "Topic",
            "content": "edited",
            "edit_timestamp": 1_700_000_001,
        },
        "live",
    )
    update_topic = next(
        record
        for record in update_records
        if record["operation"]["kind"] == "topic.upsert"
    )
    update_message = next(
        record
        for record in update_records
        if record["operation"]["kind"] == "message.update"
    )

    assert postgres_store.enqueue_workspace_delivery(topic_records[0], 2)
    assert not postgres_store.enqueue_workspace_delivery(topic_records[1], 2)
    assert not postgres_store.enqueue_workspace_delivery(update_topic, 0)
    for record in message_records:
        assert postgres_store.enqueue_workspace_delivery(record, 0)
    assert postgres_store.enqueue_workspace_delivery(update_message, 0)

    assert postgres_store.pending_workspace_deliveries(
        minimum_priority=0, maximum_priority=0
    ) == [topic_records[0]]
    assert (
        postgres_store.pending_workspace_deliveries(
            minimum_priority=2, maximum_priority=2
        )
        == []
    )

    postgres_store.accept_result(_committed_result(topic_records[0]))

    assert postgres_store.pending_workspace_deliveries(
        minimum_priority=0, maximum_priority=0
    ) == message_records

    postgres_store.accept_result(
        _committed_result(message_records[0], provider_entity_id="101")
    )

    assert postgres_store.pending_workspace_deliveries(
        minimum_priority=0, maximum_priority=0
    ) == [message_records[1], update_message]


def test_read_state_waits_until_every_message_projection_is_committed(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    message_uuid = str(uuid.uuid4())
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "9258",
        message_uuid,
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "workspace_delivery_state": "pending",
        },
    )
    record = _provider_record(account_uuid, project_uuid)
    record["operation"]["kind"] = "read_state.set"
    record["operation"]["entity_uuid"] = stream_uuid
    record["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "reader_uuid": str(uuid.uuid4()),
        "message_uuids": [message_uuid],
        "read": True,
    }
    record["operation_sha256"] = canonical.operation_digest(record)
    message_record = _provider_record(account_uuid, project_uuid)
    message_record["operation"]["entity_uuid"] = message_uuid
    message_record["operation"]["provider"]["entity_id"] = "9258"
    message_record["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "author_uuid": str(uuid.uuid4()),
        "payload": {"kind": "markdown", "content": "history"},
        "reply_to_message_uuid": None,
    }
    message_record["operation_sha256"] = canonical.operation_digest(message_record)

    assert postgres_store.enqueue_workspace_delivery(message_record, 2)
    assert postgres_store.enqueue_workspace_delivery(record, 0)
    assert postgres_store.pending_workspace_deliveries() == [message_record]

    postgres_store.accept_result(
        _committed_result(message_record, provider_entity_id="9258")
    )

    assert postgres_store.pending_workspace_deliveries() == [record]


def test_reaction_waits_until_message_projection_is_committed(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    message_uuid = str(uuid.uuid4())
    reaction_uuid = str(uuid.uuid4())
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    actor_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "9259",
        message_uuid,
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "workspace_delivery_state": "pending",
        },
    )
    reaction_record = _provider_record(
        account_uuid,
        project_uuid,
        kind="reaction.upsert",
    )
    reaction_record["operation"]["entity_uuid"] = reaction_uuid
    reaction_record["operation"]["actor_uuid"] = actor_uuid
    reaction_record["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "message_uuid": message_uuid,
        "user_uuid": actor_uuid,
        "emoji_name": "thumbs_up",
    }
    reaction_record["operation_sha256"] = canonical.operation_digest(reaction_record)
    message_record = _provider_record(account_uuid, project_uuid)
    message_record["operation"]["entity_uuid"] = message_uuid
    message_record["operation"]["provider"]["entity_id"] = "9259"
    message_record["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "author_uuid": str(uuid.uuid4()),
        "payload": {"kind": "markdown", "content": "history"},
        "reply_to_message_uuid": None,
    }
    message_record["operation_sha256"] = canonical.operation_digest(message_record)

    assert postgres_store.enqueue_workspace_delivery(message_record, 2)
    assert postgres_store.enqueue_workspace_delivery(reaction_record, 0)
    assert postgres_store.pending_workspace_deliveries() == [message_record]

    postgres_store.accept_result(
        _committed_result(message_record, provider_entity_id="9259")
    )

    assert postgres_store.pending_workspace_deliveries() == [reaction_record]


def test_reconcile_repairs_legacy_pending_direct_participant_gate(postgres_store):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    direct_chat = {
        "provider_chat_key": "direct:9,10",
        "chat_type": "direct",
    }
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET body = jsonb_set(
                body, '{provider_chat}', %s::jsonb
            )
            WHERE resource_type = 'external_chat_assignment'
              AND body->>'external_account_uuid' = %s
            """,
            (json.dumps(direct_chat), account_uuid),
        )
        session.execute(
            """
            DELETE FROM zulip_participant_sync
            WHERE account_uuid = %s
            """,
            (account_uuid,),
        )
        session.execute(
            """
            INSERT INTO zulip_participant_sync (
                account_uuid, provider_chat_key,
                assignment_generation, state
            ) VALUES (%s, 'direct:9,10', 1, 'pending')
            """,
            (account_uuid,),
        )

    postgres_store.reconcile_participant_sync()

    with postgres_store.session() as session:
        participant = session.execute(
            """
            SELECT state, provider_user_ids
            FROM zulip_participant_sync
            WHERE account_uuid = %s AND provider_chat_key = 'direct:9,10'
            """,
            (account_uuid,),
        ).fetchone()
    assert participant == {"state": "ready", "provider_user_ids": []}
    assert postgres_store.claim_participant_sync() is None


def test_ready_channel_participants_are_rechecked_after_bounded_interval(
    postgres_store,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)

    assert postgres_store.claim_participant_sync() is None
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_participant_sync
            SET updated_at = now() - make_interval(secs => %s)
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (
                storage.PARTICIPANT_RECHECK_INTERVAL_SECONDS + 1,
                account_uuid,
            ),
        )

    claimed = postgres_store.claim_participant_sync()

    assert claimed is not None
    assert str(claimed["account_uuid"]) == account_uuid
    assert claimed["provider_chat_key"] == "channel:42"
    assert claimed["assignment_generation"] == 1


@pytest.mark.parametrize(
    ("status", "expected_code", "expected_manual", "expected_evidence"),
    [
        ("applied", None, False, []),
        (
            "rejected",
            "provider_result_rejected",
            True,
            [{"kind": "provider_result_response", "status": "rejected"}],
        ),
    ],
)
def test_provider_result_acknowledgement_types_nullable_sql_parameters(
    postgres_store,
    status,
    expected_code,
    expected_manual,
    expected_evidence,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(account_uuid, project_uuid)
    record["sequence"] = 1
    result_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO bridge_operations (
                record_uuid, operation_uuid, attempt, operation_sha256,
                account_uuid, project_uuid, origin, causal_lane, lane_sequence,
                priority, state, record, result_record
            ) VALUES (%s, %s, 1, %s, %s, %s, 'zulip', %s, 1, 0,
                      'committed', %s::jsonb, %s::jsonb)
            """,
            (
                record["record_uuid"],
                record["operation_uuid"],
                record["operation_sha256"],
                account_uuid,
                project_uuid,
                record["causal_lane"],
                json.dumps(record),
                json.dumps({"record_uuid": result_uuid}),
            ),
        )

    postgres_store.finalize_provider_result_response(result_uuid, status)

    with postgres_store.session() as session:
        row = session.execute(
            """
            SELECT result_sent_at, last_error_code,
                   manual_reconciliation_required, reconciliation_evidence
            FROM bridge_operations WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
    assert row["result_sent_at"] is not None
    assert row["last_error_code"] == expected_code
    assert row["manual_reconciliation_required"] is expected_manual
    assert row["reconciliation_evidence"] == expected_evidence


def test_reconcile_backfill_jobs_casts_json_account_uuid(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)

    postgres_store.reconcile_backfill_jobs()

    with postgres_store.session() as session:
        row = session.execute(
            """
            SELECT account_uuid, provider_chat_key, history_depth
            FROM zulip_backfill_jobs
            """
        ).fetchone()
    assert str(row["account_uuid"]) == account_uuid
    assert row["provider_chat_key"] == "channel:42"
    assert row["history_depth"] == "30_days"


def test_reconcile_jobs_does_not_rewrite_unchanged_sync_checkpoints(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_participant_sync()
    postgres_store.reconcile_backfill_jobs()
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET next_anchor = 42,
                cutoff_at = TIMESTAMPTZ '2026-01-01 00:00:00+00',
                updated_at = TIMESTAMPTZ '2026-01-02 00:00:00+00'
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        )
        session.execute(
            """
            UPDATE zulip_participant_sync
            SET updated_at = TIMESTAMPTZ '2026-01-03 00:00:00+00'
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        )

    postgres_store.reconcile_participant_sync()
    postgres_store.reconcile_backfill_jobs()

    with postgres_store.session() as session:
        backfill = session.execute(
            """
            SELECT next_anchor, cutoff_at, updated_at
            FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
        participant = session.execute(
            """
            SELECT updated_at
            FROM zulip_participant_sync
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
    assert backfill == {
        "next_anchor": 42,
        "cutoff_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        "updated_at": datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC),
    }
    assert participant["updated_at"] == datetime.datetime(
        2026, 1, 3, tzinfo=datetime.UTC
    )


def test_queue_loss_recovery_keeps_selected_account_uuid_typed(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO provider_mappings (
                account_uuid, entity_kind, workspace_uuid, provider_id, metadata
            ) VALUES (%s, 'message', %s, '99', %s)
            """,
            (
                account_uuid,
                str(uuid.uuid4()),
                json.dumps(
                    {
                        "chat_key": "channel:42",
                        "project_uuid": project_uuid,
                    }
                ),
            ),
        )

    postgres_store.begin_provider_queue_catchup(account_uuid)

    with postgres_store.session() as session:
        row = session.execute(
            """
            SELECT account_uuid, provider_chat_key,
                   checkpoint_provider_message_id
            FROM zulip_queue_catchup_jobs
            """
        ).fetchone()
    assert str(row["account_uuid"]) == account_uuid
    assert row["provider_chat_key"] == "channel:42"
    assert row["checkpoint_provider_message_id"] == 99


def test_reconcile_backfill_jobs_removes_deselected_queue_catchup(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    postgres_store.begin_provider_queue_catchup(account_uuid)
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_queue_catchup_jobs (
                account_uuid, provider_chat_key, state
            ) VALUES (%s, 'channel:99', 'pending')
            """,
            (account_uuid,),
        )

    postgres_store.reconcile_backfill_jobs()

    with postgres_store.session() as session:
        jobs = session.execute(
            """
            SELECT provider_chat_key, state
            FROM zulip_queue_catchup_jobs
            ORDER BY provider_chat_key
            """
        ).fetchall()
    assert jobs == [{"provider_chat_key": "channel:42", "state": "pending"}]


def test_queue_loss_catchup_completes_without_a_safe_error(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    postgres_store.begin_provider_queue_catchup(account_uuid)

    postgres_store.advance_provider_catchup(
        account_uuid,
        "channel:42",
        [99],
        None,
        True,
    )

    assert postgres_store.provider_catchup_ready(account_uuid)


def test_account_global_identity_delivery_uses_account_generation(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(
        account_uuid, project_uuid, chat_id="account", kind="identity.upsert"
    )

    assert postgres_store.enqueue_workspace_delivery(record, 0, "queue", 7)

    with postgres_store.session() as session:
        row = session.execute(
            """
            SELECT account_generation, assignment_uuid
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
    assert row["account_generation"] == 1
    assert row["assignment_uuid"] is None


def test_provider_identity_replay_keeps_first_accepted_record(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(
        account_uuid, project_uuid, chat_id="account", kind="identity.upsert"
    )
    record["operation"]["provider"]["entity_id"] = "42"
    record["causal_lane"] = (
        f"identity:{account_uuid}:{record['operation']['entity_uuid']}"
    )
    record["operation_sha256"] = canonical.operation_digest(record)
    assert postgres_store.enqueue_workspace_delivery(record, 0, "queue", 7)
    accepted = json.loads(json.dumps(record))

    canonical_replay = json.loads(json.dumps(record))
    canonical_replay["operation"]["entity_uuid"] = str(uuid.uuid4())
    canonical_replay["causal_lane"] = (
        f"identity:{account_uuid}:"
        f"{canonical_replay['operation']['entity_uuid']}"
    )
    canonical_replay["operation_sha256"] = canonical.operation_digest(
        canonical_replay
    )

    assert not postgres_store.enqueue_workspace_delivery(
        canonical_replay, 0, "queue", 7
    )
    with postgres_store.session() as session:
        stored = session.execute(
            """
            SELECT delivery.record, idempotency.operation_sha256
            FROM workspace_delivery_outbox AS delivery
            JOIN operation_idempotency AS idempotency USING (operation_uuid)
            WHERE delivery.operation_uuid = %s
            """,
            (record["operation_uuid"],),
        ).fetchone()
    assert stored["record"] == accepted
    assert stored["operation_sha256"] == accepted["operation_sha256"]

    changed_payload = json.loads(json.dumps(canonical_replay))
    changed_payload["operation"]["payload"]["display_name"] = "Changed"
    changed_payload["operation_sha256"] = canonical.operation_digest(changed_payload)
    with pytest.raises(
        ValueError, match="Operation UUID reused with a different digest"
    ):
        postgres_store.enqueue_workspace_delivery(changed_payload, 0, "queue", 7)


def test_outbound_commit_suppresses_queue_loss_history_duplicate(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store, account_uuid, project_uuid
    )
    workspace_message_uuid = str(uuid.uuid4())
    outbound = _provider_record(account_uuid, project_uuid)
    outbound["origin"] = "workspace"
    outbound["sequence"] = 1
    outbound["operation"]["entity_uuid"] = workspace_message_uuid
    outbound["operation"]["payload"].update(
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
        }
    )
    outbound["operation_sha256"] = canonical.operation_digest(outbound)
    with postgres_store.session() as session:
        postgres_store._persist_committed_mapping(session, outbound, "601", None)

    _backfill_service(postgres_store).enqueue_backfill(
        account_uuid, "channel:42", [_provider_history_message(601)]
    )

    mapping = postgres_store.provider_mapping(account_uuid, "message", "601")
    assert str(mapping["workspace_uuid"]) == workspace_message_uuid
    assert mapping["convergent_alias"] is True
    with postgres_store.session() as session:
        duplicate = session.execute(
            """
            SELECT record FROM workspace_delivery_outbox
            WHERE record->'operation'->>'kind' = 'message.create'
              AND record->'operation'->'provider'->>'entity_id' = '601'
            """
        ).fetchall()
    assert duplicate == []


def test_outbound_reaction_echo_reuses_workspace_identity(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store, account_uuid, project_uuid
    )
    message_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "601",
        message_uuid,
        {
            "project_uuid": project_uuid,
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": "channel:42",
            "workspace_delivery_state": "committed",
        },
    )
    workspace_reaction_uuid = str(uuid.uuid4())
    outbound = _provider_record(
        account_uuid,
        project_uuid,
        kind="reaction.create",
    )
    outbound["origin"] = "workspace"
    outbound["operation"]["entity_uuid"] = workspace_reaction_uuid
    outbound["operation"]["actor_uuid"] = author_uuid
    outbound["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "message_uuid": message_uuid,
        "user_uuid": author_uuid,
        "emoji_name": "thumbs_up",
    }
    with postgres_store.session() as session:
        postgres_store._persist_committed_mapping(
            session,
            outbound,
            "601:2:thumbs_up",
            None,
        )

    records = converter.event_records(
        postgres_store,
        account_uuid,
        "provider-reaction:601",
        {
            "id": 700,
            "type": "reaction",
            "op": "add",
            "message_id": 601,
            "user_id": 2,
            "emoji_name": "thumbs_up",
            "emoji_code": "1f44d",
            "reaction_type": "unicode_emoji",
        },
    )

    reaction = next(
        record
        for record in records
        if record["operation"]["kind"] == "reaction.upsert"
    )
    assert reaction["operation"]["entity_uuid"] == workspace_reaction_uuid


def test_provider_mapping_written_before_event_delivery_recovers_same_message(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _materialize_channel_projection(postgres_store, account_uuid, project_uuid)
    message = _provider_history_message(602)
    first_records = converter.event_records(
        postgres_store,
        account_uuid,
        "provider-message:602",
        {"id": 602, "type": "message", "message": message},
        "backfill",
    )
    first_create = next(
        record
        for record in first_records
        if record["operation"]["kind"] == "message.create"
    )
    pending_workspace_uuid = first_create["operation"]["entity_uuid"]

    _backfill_service(postgres_store).enqueue_backfill(
        account_uuid, "channel:42", [message]
    )

    with postgres_store.session() as session:
        recovered = session.execute(
            """
            SELECT record FROM workspace_delivery_outbox
            WHERE record->'operation'->>'kind' = 'message.create'
              AND record->'operation'->'provider'->>'entity_id' = '602'
            """
        ).fetchall()
    assert len(recovered) == 1
    assert recovered[0]["record"]["operation"]["entity_uuid"] == pending_workspace_uuid
    assert recovered[0]["record"]["record_uuid"] == first_create["record_uuid"]


def test_live_message_replay_reuses_accepted_link_after_topic_recanonicalization(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _, original_topic_uuid, _ = _materialize_channel_projection(
        postgres_store, account_uuid, project_uuid
    )
    postgres_store.rename_provider_mapping(
        account_uuid,
        "topic",
        "42:Topic",
        "42:✔ resolved topic",
        {"stream_uuid": str(uuid.uuid4()), "chat_key": "channel:42"},
    )
    message = _provider_history_message(602)
    message["subject"] = "✔ resolved topic"
    message["content"] = (
        "#**Engineering>✔ resolved topic** @**Mentioned user|99**"
    )
    message["flags"] = ["read"]
    queue_id = "provider-message:602"
    event_id = 17
    event = {"id": event_id, "type": "message", "message": message}
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    accepted = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        event,
    )
    accepted = postgres_store.prepare_provider_event_records(
        account_uuid, queue_id, event_id, accepted
    )
    assert accepted[-1]["operation"]["kind"] == "read_state.set"
    for record in accepted[:-1]:
        assert postgres_store.enqueue_workspace_delivery(
            record, 0, queue_id, event_id
        )
    accepted_message = next(
        record
        for record in accepted
        if record["operation"]["kind"] == "message.create"
    )
    assert any(
        record["operation"]["kind"] == "identity.upsert"
        and record["operation"]["provider"]["entity_id"] == "99"
        for record in accepted
    )
    assert (
        f"urn:topic:{original_topic_uuid}"
        in accepted_message["operation"]["payload"]["payload"]["content"]
    )

    canonical_topic_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE provider_mappings
            SET workspace_uuid = %s
            WHERE account_uuid = %s AND entity_kind = 'topic'
              AND provider_id = '42:✔ resolved topic'
            """,
            (canonical_topic_uuid, account_uuid),
        )
        session.execute(
            """
            UPDATE provider_mappings
            SET metadata = jsonb_set(metadata, '{topic_uuid}', to_jsonb(%s::text))
            WHERE account_uuid = %s AND entity_kind = 'message'
              AND provider_id = '602'
            """,
            (canonical_topic_uuid, account_uuid),
        )

    replay = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        event,
    )

    assert [
        (
            record["operation_uuid"],
            record["operation"]["kind"],
            record["operation_sha256"],
        )
        for record in replay
    ] == [
        (
            record["operation_uuid"],
            record["operation"]["kind"],
            record["operation_sha256"],
        )
        for record in accepted
    ]
    enqueue_results = [
        postgres_store.enqueue_workspace_delivery(record, 0, queue_id, event_id)
        for record in replay
    ]
    assert enqueue_results == [False] * (len(replay) - 1) + [True]
    with postgres_store.session() as session:
        stored_kinds = [
            row["kind"]
            for row in session.execute(
                """
                SELECT record->'operation'->>'kind' AS kind
                FROM workspace_delivery_outbox
                WHERE account_uuid = %s AND provider_queue_id = %s
                  AND provider_event_id = %s
                ORDER BY created_at
                """,
                (account_uuid, queue_id, event_id),
            ).fetchall()
        ]
    assert stored_kinds == [
        record["operation"]["kind"] for record in accepted
    ]

    # A sent prefix can survive an assignment replacement while an unsent tail
    # is discarded as stale. Keep the prepared snapshot through delivering so
    # the event can requeue the missing tail under the new generation.
    postgres_store.mark_provider_event_delivering(
        account_uuid, queue_id, event_id
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE workspace_delivery_outbox
            SET submission_state = 'sent', sent_at = now()
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
              AND record->'operation'->>'kind' != 'read_state.set'
            """,
            (account_uuid, queue_id, event_id),
        )
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1
            WHERE resource_type = 'external_chat_assignment'
            """
        )

    assert postgres_store.reset_stale_workspace_deliveries() == 1
    with postgres_store.session() as session:
        interrupted = session.execute(
            """
            SELECT processing_state, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert interrupted["processing_state"] == "pending"
    assert interrupted["prepared_records"] == accepted

    assignment_replay = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        event,
    )
    assignment_replay = postgres_store.prepare_provider_event_records(
        account_uuid, queue_id, event_id, assignment_replay
    )
    assert assignment_replay == accepted
    assignment_enqueue_results = [
        postgres_store.enqueue_workspace_delivery(record, 0, queue_id, event_id)
        for record in assignment_replay
    ]
    assert assignment_enqueue_results == [False] * (len(accepted) - 1) + [True]
    with postgres_store.session() as session:
        restored = session.execute(
            """
            SELECT count(*) AS total,
                   count(*) FILTER (
                       WHERE record->'operation'->>'kind' = 'message.create'
                   ) AS messages,
                   count(*) FILTER (
                       WHERE record->'operation'->>'kind' = 'read_state.set'
                   ) AS read_states
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert restored == {
        "total": len(accepted),
        "messages": 1,
        "read_states": 1,
    }
    postgres_store.mark_provider_event_delivering(
        account_uuid, queue_id, event_id
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE workspace_delivery_outbox
            SET submission_state = 'sent', sent_at = now()
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        )
    assert postgres_store.finalize_ready_provider_events() == 1
    with postgres_store.session() as session:
        completed = session.execute(
            """
            SELECT processing_state, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert completed == {
        "processing_state": "processed",
        "prepared_records": None,
    }


@pytest.mark.parametrize("assignment_change", ["project", "deselected", "deleted"])
def test_partial_live_event_finishes_when_assignment_target_is_removed(
    postgres_store,
    assignment_change,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _materialize_channel_projection(postgres_store, account_uuid, project_uuid)
    message = _provider_history_message(603)
    message["flags"] = ["read"]
    queue_id = "provider-message:603"
    event_id = 18
    event = {"id": event_id, "type": "message", "message": message}
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    prepared = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        event,
    )
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid, queue_id, event_id, prepared
    )
    for record in prepared:
        assert postgres_store.enqueue_workspace_delivery(
            record, 0, queue_id, event_id
        )
    postgres_store.mark_provider_event_delivering(
        account_uuid, queue_id, event_id
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE workspace_delivery_outbox
            SET submission_state = 'sent', sent_at = now()
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
              AND record->'operation'->>'kind' != 'read_state.set'
            """,
            (account_uuid, queue_id, event_id),
        )
        if assignment_change == "project":
            session.execute(
                """
                UPDATE desired_resources
                SET generation = generation + 1,
                    body = jsonb_set(
                        body,
                        '{project_id}',
                        to_jsonb(%s::text)
                    )
                WHERE resource_type = 'external_chat_assignment'
                """,
                (str(uuid.uuid4()),),
            )
        elif assignment_change == "deselected":
            session.execute(
                """
                UPDATE desired_resources
                SET generation = generation + 1,
                    body = jsonb_set(body, '{selected}', 'false'::jsonb)
                WHERE resource_type = 'external_chat_assignment'
                """
            )
        else:
            session.execute(
                """
                UPDATE desired_resources
                SET generation = generation + 1, deleted = true
                WHERE resource_type = 'external_chat_assignment'
                """
            )

    assert postgres_store.reset_stale_workspace_deliveries() == 1
    with postgres_store.session() as session:
        provider_event = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        deliveries = session.execute(
            """
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE sent_at IS NULL) AS unsent,
                   count(*) FILTER (
                       WHERE record->'operation'->>'kind' = 'message.create'
                   ) AS messages,
                   count(*) FILTER (
                       WHERE record->'operation'->>'kind' = 'read_state.set'
                   ) AS read_states
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert provider_event == {
        "processing_state": "processed",
        "processing_reason": "assignment_changed",
        "prepared_records": None,
    }
    assert deliveries == {
        "total": len(prepared) - 1,
        "unsent": 0,
        "messages": 1,
        "read_states": 0,
    }
    assert postgres_store.pending_provider_events() == []


def test_enqueue_assignment_change_race_requeues_unsubmitted_prefix(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    queue_id = "provider-message:604"
    event_id = 19
    event = {"id": event_id, "type": "heartbeat"}
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    records = [
        _provider_record(account_uuid, project_uuid),
        _provider_record(account_uuid, project_uuid),
    ]
    records = postgres_store.prepare_provider_event_records(
        account_uuid, queue_id, event_id, records
    )
    assert postgres_store.enqueue_workspace_delivery(
        records[0], 0, queue_id, event_id
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1,
                body = jsonb_set(
                    body,
                    '{project_id}',
                    to_jsonb(%s::text)
                )
            WHERE resource_type = 'external_chat_assignment'
            """,
            (str(uuid.uuid4()),),
        )
    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        postgres_store.enqueue_workspace_delivery(
            records[1], 0, queue_id, event_id
        )

    assert (
        postgres_store.finalize_provider_event_assignment_changed(
            account_uuid,
            queue_id,
            event_id,
        )
        is True
    )

    with postgres_store.session() as session:
        provider_event = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        pending_deliveries = session.execute(
            """
            SELECT count(*) AS count
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s AND sent_at IS NULL
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()["count"]
        idempotency_rows = session.execute(
            """
            SELECT count(*) AS count
            FROM operation_idempotency
            WHERE operation_uuid = ANY(%s)
            """,
            ([record["operation_uuid"] for record in records],),
        ).fetchone()["count"]
    assert provider_event == {
        "processing_state": "pending",
        "processing_reason": "assignment_changed",
        "prepared_records": None,
    }
    assert pending_deliveries == 0
    assert idempotency_rows == 0
    postgres_store.mark_provider_event_processed(
        account_uuid,
        queue_id,
        event_id,
        True,
    )


@pytest.mark.parametrize(
    "submit_topic_prefix",
    [False, True],
    ids=["pending-prefix", "submitted-topic-prefix"],
)
def test_assignment_move_before_message_enqueue_replays_create_and_update(
    postgres_store,
    submit_topic_prefix,
):
    account_uuid, old_project_uuid = _insert_account_and_assignment(postgres_store)
    _materialize_channel_projection(
        postgres_store,
        account_uuid,
        old_project_uuid,
    )
    message = _provider_history_message(605)
    create_queue_id = "provider-message:605"
    create_event_id = 20
    create_event = {
        "id": create_event_id,
        "type": "message",
        "message": message,
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        create_queue_id,
        create_event,
    )
    prepared = converter.event_records(
        postgres_store,
        account_uuid,
        create_queue_id,
        create_event,
    )
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        create_queue_id,
        create_event_id,
        prepared,
    )
    message_index = next(
        index
        for index, record in enumerate(prepared)
        if record["operation"]["kind"] == "message.create"
    )
    for record in prepared[:message_index]:
        assert postgres_store.enqueue_workspace_delivery(
            record,
            0,
            create_queue_id,
            create_event_id,
        )
    if submit_topic_prefix:
        topic_record = next(
            record
            for record in prepared[:message_index]
            if record["operation"]["kind"] == "topic.upsert"
        )
        assert postgres_store.mark_workspace_delivery_submitting(
            topic_record["record_uuid"]
        )
        postgres_store.mark_workspace_delivery_submitted(
            topic_record["record_uuid"]
        )

    pending_mapping = postgres_store.provider_mapping(
        account_uuid,
        "message",
        "605",
    )
    assert pending_mapping["metadata"]["workspace_delivery_state"] == "pending"
    assert pending_mapping["metadata"]["project_uuid"] == old_project_uuid

    new_project_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1,
                body = jsonb_set(
                    body,
                    '{project_id}',
                    to_jsonb(%s::text)
                )
            WHERE resource_type = 'external_chat_assignment'
            """,
            (new_project_uuid,),
        )

    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        postgres_store.enqueue_workspace_delivery(
            prepared[message_index],
            0,
            create_queue_id,
            create_event_id,
        )
    assert (
        postgres_store.finalize_provider_event_assignment_changed(
            account_uuid,
            create_queue_id,
            create_event_id,
        )
        is True
    )

    with postgres_store.session() as session:
        create_state = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, create_queue_id, create_event_id),
        ).fetchone()
        remaining_deliveries = session.execute(
            """
            SELECT count(*) AS count
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
            """,
            (account_uuid, create_queue_id, create_event_id),
        ).fetchone()["count"]
    assert create_state == {
        "processing_state": "pending",
        "processing_reason": "assignment_changed",
        "prepared_records": None,
    }
    assert remaining_deliveries == int(submit_topic_prefix)
    assert postgres_store.provider_mapping(account_uuid, "message", "605") is None

    replay = converter.event_records(
        postgres_store,
        account_uuid,
        create_queue_id,
        create_event,
    )
    replay = postgres_store.prepare_provider_event_records(
        account_uuid,
        create_queue_id,
        create_event_id,
        replay,
    )
    assert {record["project_uuid"] for record in replay} == {new_project_uuid}
    assert {
        record["operation_uuid"] for record in replay
    }.isdisjoint(
        record["operation_uuid"] for record in prepared
    )
    replay_results = [
        postgres_store.enqueue_workspace_delivery(
            record,
            0,
            create_queue_id,
            create_event_id,
        )
        for record in replay
    ]
    assert any(
        enqueued and record["operation"]["kind"] == "message.create"
        for record, enqueued in zip(replay, replay_results, strict=True)
    )

    moved_mapping = postgres_store.provider_mapping(
        account_uuid,
        "message",
        "605",
    )
    assert moved_mapping["metadata"]["workspace_delivery_state"] == "pending"
    assert moved_mapping["metadata"]["project_uuid"] == new_project_uuid

    update_queue_id = "provider-message-update:605"
    update_event_id = 21
    update_event = {
        "id": update_event_id,
        "type": "update_message",
        "message_id": 605,
        "message_ids": [605],
        "content": "edited after assignment move",
        "edit_timestamp": 1_700_000_001,
        "stream_id": 42,
        "orig_subject": "Topic",
        "subject": "Topic",
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        update_queue_id,
        update_event,
    )
    update_records = converter.event_records(
        postgres_store,
        account_uuid,
        update_queue_id,
        update_event,
    )
    update_records = postgres_store.prepare_provider_event_records(
        account_uuid,
        update_queue_id,
        update_event_id,
        update_records,
    )
    assert {record["project_uuid"] for record in update_records} == {
        new_project_uuid
    }
    assert any(
        record["operation"]["kind"] == "message.update"
        for record in update_records
    )
    update_enqueue_results = [
        postgres_store.enqueue_workspace_delivery(
            record,
            0,
            update_queue_id,
            update_event_id,
        )
        for record in update_records
    ]
    assert any(
        enqueued and record["operation"]["kind"] == "message.update"
        for record, enqueued in zip(
            update_records,
            update_enqueue_results,
            strict=True,
        )
    )
    postgres_store.mark_provider_event_processed(
        account_uuid,
        create_queue_id,
        create_event_id,
        True,
    )
    postgres_store.mark_provider_event_processed(
        account_uuid,
        update_queue_id,
        update_event_id,
        True,
    )


def test_stale_reset_replays_message_when_project_moves_before_submission(
    postgres_store,
):
    account_uuid, old_project_uuid = _insert_account_and_assignment(postgres_store)
    _materialize_channel_projection(
        postgres_store,
        account_uuid,
        old_project_uuid,
    )
    message = _provider_history_message(606)
    queue_id = "provider-message:606"
    event_id = 22
    event = {"id": event_id, "type": "message", "message": message}
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    prepared = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        event,
    )
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        event_id,
        prepared,
    )
    for record in prepared:
        assert postgres_store.enqueue_workspace_delivery(
            record,
            0,
            queue_id,
            event_id,
        )
    postgres_store.mark_provider_event_delivering(
        account_uuid,
        queue_id,
        event_id,
    )

    new_project_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1,
                body = jsonb_set(
                    body,
                    '{project_id}',
                    to_jsonb(%s::text)
                )
            WHERE resource_type = 'external_chat_assignment'
            """,
            (new_project_uuid,),
        )

    assert postgres_store.reset_stale_workspace_deliveries() == len(prepared)
    with postgres_store.session() as session:
        provider_event = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        remaining_deliveries = session.execute(
            """
            SELECT count(*) AS count
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()["count"]
    assert provider_event == {
        "processing_state": "pending",
        "processing_reason": "assignment_changed",
        "prepared_records": None,
    }
    assert remaining_deliveries == 0
    assert postgres_store.provider_mapping(account_uuid, "message", "606") is None

    replay = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        event,
    )
    assert {record["project_uuid"] for record in replay} == {new_project_uuid}
    postgres_store.mark_provider_event_processed(
        account_uuid,
        queue_id,
        event_id,
        True,
    )


def test_lane_allocation_is_atomic_with_durable_outbox(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    abandoned_uuid = str(uuid.uuid4())
    lane = f"chat:{account_uuid}:channel:42"
    assert postgres_store.producer_lane_position(abandoned_uuid, "zulip", lane) == (
        0,
        None,
    )
    with postgres_store.session() as session:
        assert (
            session.execute(
                "SELECT 1 FROM producer_lane_counters WHERE causal_lane = %s", (lane,)
            ).fetchone()
            is None
        )
    record = _provider_record(account_uuid, project_uuid)
    record["causal_lane"] = lane

    assert postgres_store.enqueue_workspace_delivery(record, 0)

    assert record["sequence"] == 1
    assert record["predecessor_operation_uuid"] is None
    with postgres_store.session() as session:
        rows = session.execute(
            """
            SELECT operation_uuid, lane_sequence
            FROM producer_operations ORDER BY lane_sequence
            """
        ).fetchall()
    assert [(str(row["operation_uuid"]), row["lane_sequence"]) for row in rows] == [
        (record["operation_uuid"], 1)
    ]


def test_pending_create_blocks_later_exact_read_in_same_causal_lane(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    create = _provider_record(account_uuid, project_uuid)
    create["origin"] = "workspace"
    create["sequence"] = 1
    create["operation_sha256"] = canonical.operation_digest(create)

    read = _provider_record(account_uuid, project_uuid, kind="read_state.set")
    read["origin"] = "workspace"
    read["causal_lane"] = create["causal_lane"]
    read["sequence"] = 2
    read["predecessor_operation_uuid"] = create["operation_uuid"]
    read["operation"]["payload"] = {
        "stream_uuid": str(uuid.uuid4()),
        "topic_uuid": str(uuid.uuid4()),
        "reader_uuid": str(uuid.uuid4()),
        "message_uuids": [create["operation"]["entity_uuid"]],
        "read": True,
    }
    read["operation_sha256"] = canonical.operation_digest(read)

    assert postgres_store.enqueue(create, 0)
    assert postgres_store.enqueue(read, 0)

    claimed = postgres_store.claim("worker-one")
    assert claimed is not None
    assert claimed.record["operation_uuid"] == create["operation_uuid"]
    assert postgres_store.claim("worker-two") is None


def test_create_then_edit_lease_waits_for_message_mapping(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    message_uuid = str(uuid.uuid4())
    lease_expires_at = (
        (datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=5))
        .isoformat()
        .replace("+00:00", "Z")
    )

    def leased(kind: str) -> dict[str, object]:
        return {
            "provider_operation_uuid": str(uuid.uuid4()),
            "external_operation_uuid": str(uuid.uuid4()),
            "lease_uuid": str(uuid.uuid4()),
            "lease_expires_at": lease_expires_at,
            "external_account_uuid": account_uuid,
            "project_id": project_uuid,
            "operation_kind": kind,
            "required_capability": (
                "messenger.message.send"
                if kind == "message.create"
                else "messenger.message.edit"
            ),
            "attempt": 1,
            "payload": {
                "uuid": message_uuid,
                "stream_uuid": stream_uuid,
                "topic_uuid": topic_uuid,
                "author_uuid": author_uuid,
                "payload": {"kind": "markdown", "content": "edited"},
            },
        }

    create = provider_protocol.leased_operation_record(
        postgres_store,
        leased("message.create"),
    )
    update = provider_protocol.leased_operation_record(
        postgres_store,
        leased("message.update"),
    )

    assert update["operation"]["provider"]["entity_id"] is None
    assert postgres_store.enqueue(create, 0)
    assert postgres_store.enqueue(update, 0)
    assert create["sequence"] == 1
    assert update["sequence"] == 2
    assert update["predecessor_operation_uuid"] == create["operation_uuid"]

    claimed = postgres_store.claim("create-worker")
    assert claimed is not None
    assert claimed.record["operation"]["kind"] == "message.create"
    assert postgres_store.claim("update-worker") is None


def test_exact_provider_read_lease_is_idempotent_and_ordered_in_postgres_scheduler(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, _author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    first_message_uuid = str(uuid.uuid4())
    last_message_uuid = str(uuid.uuid4())
    for provider_id, workspace_uuid in (
        ("901", first_message_uuid),
        ("902", last_message_uuid),
    ):
        postgres_store.remember_provider_mapping(
            account_uuid,
            "message",
            provider_id,
            workspace_uuid,
            {"chat_key": "channel:42"},
        )
    lease_expires_at = (
        (datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=5))
        .isoformat()
        .replace("+00:00", "Z")
    )
    leased = {
        "provider_operation_uuid": str(uuid.uuid4()),
        "external_operation_uuid": str(uuid.uuid4()),
        "lease_uuid": str(uuid.uuid4()),
        "lease_expires_at": lease_expires_at,
        "external_account_uuid": account_uuid,
        "project_id": project_uuid,
        "operation_kind": "read_state.set",
        "required_capability": "messenger.message.read",
        "attempt": 2,
        "payload": {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "reader_uuid": str(uuid.uuid4()),
            "message_uuids": [first_message_uuid, last_message_uuid],
            "read": True,
        },
    }
    record = provider_protocol.leased_operation_record(postgres_store, leased)

    assert postgres_store.enqueue(record, 0) is True
    assert record["sequence"] == 1
    assert record["predecessor_operation_uuid"] is None
    assert postgres_store.enqueue(record, 0) is False

    # Lease expiry is an independent fail-closed eligibility boundary. Exercise
    # it on the same otherwise-ready first causal-lane item, then restore the
    # active lease to verify the ordering path rather than bypassing it.
    with postgres_store.session() as session:
        session.execute(
            "UPDATE bridge_operations SET expires_at = now() - interval '1 second' "
            "WHERE record_uuid = %s",
            (record["record_uuid"],),
        )
    assert postgres_store.claim("expired-read-worker") is None
    with postgres_store.session() as session:
        session.execute(
            "UPDATE bridge_operations SET expires_at = %s WHERE record_uuid = %s",
            (lease_expires_at, record["record_uuid"]),
        )

    claimed = postgres_store.claim("read-worker")
    assert claimed is not None
    assert claimed.record["operation"]["payload"]["message_uuids"] == [
        first_message_uuid,
        last_message_uuid,
    ]
    assert claimed.record["operation"]["entity_uuid"] == last_message_uuid


def test_stale_backfill_delivery_restarts_chat_history(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET next_anchor = 42, state = 'complete'
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        )
    record = _provider_record(account_uuid, project_uuid)
    assert postgres_store.enqueue_workspace_delivery(record, 2)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1
            WHERE resource_type = 'external_chat_assignment'
            """
        )

    assert postgres_store.reset_stale_workspace_deliveries() == 1

    with postgres_store.session() as session:
        job = session.execute(
            """
            SELECT state, next_anchor
            FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
    assert job == {"state": "pending", "next_anchor": None}


def test_submitted_delivery_survives_assignment_change_as_ambiguous(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(account_uuid, project_uuid)
    assert postgres_store.enqueue_workspace_delivery(record, 0, "queue", 7)
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources SET deleted = true
            WHERE resource_type = 'external_chat_assignment'
            """
        )

    assert postgres_store.mark_interrupted_workspace_deliveries_ambiguous() == 1
    assert postgres_store.reset_stale_workspace_deliveries() == 0
    assert postgres_store.pending_workspace_deliveries() == []
    assert not postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])

    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
        idempotency = session.execute(
            """
            SELECT operation_uuid FROM operation_idempotency
            WHERE operation_uuid = %s
            """,
            (record["operation_uuid"],),
        ).fetchone()
    assert delivery["submission_state"] == "ambiguous"
    assert idempotency is not None
    result = _committed_result(record)
    postgres_store.accept_result(result)
    with postgres_store.session() as session:
        resolved = session.execute(
            """
            SELECT submission_state, sent_at FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
    assert resolved["submission_state"] == "sent"
    assert resolved["sent_at"] is not None


def test_permanent_provider_rejection_is_quarantined_with_safe_evidence(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    queue_id = "queue"
    event_id = 9
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state
            ) VALUES (%s, %s, %s, 'message', '{}'::jsonb, 'delivering')
            """,
            (account_uuid, queue_id, event_id),
        )
    record = _provider_record(account_uuid, project_uuid)
    assert postgres_store.enqueue_workspace_delivery(
        record, 0, queue_id, event_id
    )
    assert postgres_store.mark_workspace_delivery_submitting(
        record["record_uuid"]
    )

    assert postgres_store.reject_provider_event_submission(
        record["record_uuid"],
        "provider_api_http_422",
    )
    assert postgres_store.pending_workspace_deliveries() == []
    postgres_store.release_provider_event_submissions([record["record_uuid"]])

    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, submission_error_code, sent_at
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
        event = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert delivery == {
        "submission_state": "rejected",
        "submission_error_code": "provider_api_http_422",
        "sent_at": None,
    }
    assert event == {
        "processing_state": "delivering",
        "processing_reason": "workspace_delivery_rejected",
    }


def test_pre_provider_result_crash_retries_same_immutable_record_until_result(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(account_uuid, project_uuid)
    assert postgres_store.enqueue_workspace_delivery(record, 0, "queue", 8)

    original_record = dict(record)
    for _ in range(2):
        assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
        assert postgres_store.mark_interrupted_workspace_deliveries_ambiguous() == 1

        retry = postgres_store.pending_workspace_deliveries()
        assert retry == [original_record]
        assert retry[0]["record_uuid"] == record["record_uuid"]
        assert retry[0]["operation_uuid"] == record["operation_uuid"]
        assert retry[0]["operation_sha256"] == record["operation_sha256"]

    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, sent_at, record
            FROM workspace_delivery_outbox WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
        idempotency = session.execute(
            """
            SELECT operation_uuid, terminal_outcome
            FROM operation_idempotency WHERE operation_uuid = %s
            """,
            (record["operation_uuid"],),
        ).fetchone()

    assert delivery["submission_state"] == "ambiguous"
    assert delivery["sent_at"] is None
    assert delivery["record"] == original_record
    assert str(idempotency["operation_uuid"]) == record["operation_uuid"]
    assert idempotency["terminal_outcome"] is None

    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    postgres_store.mark_workspace_delivery_submitted(record["record_uuid"])
    assert postgres_store.pending_workspace_deliveries() == []

    with postgres_store.session() as session:
        awaiting = session.execute(
            """
            SELECT submission_state, submission_attempts, sent_at,
                   last_submitted_at, next_submission_at, record
            FROM workspace_delivery_outbox WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
        session.execute(
            """
            UPDATE workspace_delivery_outbox SET next_submission_at = now()
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        )

    assert awaiting["submission_state"] == "awaiting_result"
    assert awaiting["submission_attempts"] == 3
    assert awaiting["sent_at"] is None
    assert awaiting["last_submitted_at"] < awaiting["next_submission_at"]
    assert awaiting["record"] == original_record
    assert postgres_store.pending_workspace_deliveries() == [original_record]

    postgres_store.accept_result(_committed_result(record))
    assert postgres_store.pending_workspace_deliveries() == []
    with postgres_store.session() as session:
        terminal = session.execute(
            """
            SELECT submission_state, sent_at FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
    assert terminal["submission_state"] == "sent"
    assert terminal["sent_at"] is not None


def test_reselected_chat_restarts_cancelled_backfill(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_backfill_jobs (
                account_uuid, provider_chat_key, history_depth, state
            ) VALUES (%s, 'channel:42', '30_days', 'cancelled')
            """,
            (account_uuid,),
        )

    postgres_store.reconcile_backfill_jobs()

    with postgres_store.session() as session:
        state = session.execute("SELECT state FROM zulip_backfill_jobs").fetchone()[
            "state"
        ]
    assert state == "pending"


def test_changed_history_depth_restarts_backfill_from_newest(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET next_anchor = 42, state = 'complete'
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        )
        session.execute(
            """
            UPDATE desired_resources
            SET body = jsonb_set(body, '{history_depth}', '"all"'::jsonb)
            WHERE resource_type = 'external_chat_assignment'
              AND body->>'external_account_uuid' = %s
            """,
            (account_uuid,),
        )

    postgres_store.reconcile_backfill_jobs()

    with postgres_store.session() as session:
        job = session.execute(
            """
            SELECT history_depth, next_anchor, state
            FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
    assert job == {
        "history_depth": "all",
        "next_anchor": None,
        "state": "pending",
    }


def test_retryable_backfill_defer_is_durable_and_not_claimed_early(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    claimed = postgres_store.claim_backfill_job()

    assert str(claimed["account_uuid"]) == account_uuid
    assert claimed["retry_count"] == 0
    retry_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=5)
    postgres_store.defer_backfill_job(
        account_uuid,
        "channel:42",
        retry_at,
        "provider_unavailable",
    )

    assert postgres_store.claim_backfill_job() is None
    with postgres_store.session() as session:
        deferred = session.execute(
            """
            SELECT state, available_at, retry_count, last_error_code, lease_until
            FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
    assert deferred["state"] == "pending"
    assert deferred["available_at"] == retry_at
    assert deferred["retry_count"] == 1
    assert deferred["last_error_code"] == "provider_unavailable"
    assert deferred["lease_until"] is None


def test_non_retryable_backfill_failure_is_terminal_for_only_that_job(
    postgres_store,
):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    assert postgres_store.claim_backfill_job() is not None

    postgres_store.fail_backfill_job(
        account_uuid,
        "channel:42",
        "provider_forbidden",
    )

    assert postgres_store.claim_backfill_job() is None
    with postgres_store.session() as session:
        failed = session.execute(
            """
            SELECT state, last_error_code, lease_until
            FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
    assert failed["state"] == "failed"
    assert failed["last_error_code"] == "provider_forbidden"
    assert failed["lease_until"] is None


def test_explicit_manual_retry_remains_claimable_after_lane_advanced(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(account_uuid, project_uuid)
    record["attempt"] = 2
    record["sequence"] = 1
    record["operation_sha256"] = canonical.operation_digest(record)
    later_operation_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        assignment = session.execute(
            """
            SELECT resource_uuid, generation FROM desired_resources
            WHERE resource_type = 'external_chat_assignment'
            """
        ).fetchone()
        session.execute(
            """
            INSERT INTO causal_lane_state (
                origin, causal_lane, last_sequence, last_operation_uuid
            ) VALUES ('zulip', %s, 2, %s)
            """,
            (record["causal_lane"], later_operation_uuid),
        )
        session.execute(
            """
            INSERT INTO bridge_operations (
                record_uuid, operation_uuid, attempt, operation_sha256,
                account_uuid, project_uuid, origin, causal_lane,
                lane_sequence, predecessor_operation_uuid, assignment_uuid,
                assignment_generation, priority, state, record
            ) VALUES (%s, %s, 2, %s, %s, %s, 'zulip', %s, 1, NULL,
                      %s, %s, 0, 'pending', %s)
            """,
            (
                record["record_uuid"],
                record["operation_uuid"],
                record["operation_sha256"],
                account_uuid,
                project_uuid,
                record["causal_lane"],
                assignment["resource_uuid"],
                assignment["generation"],
                json.dumps(record),
            ),
        )

    claimed = postgres_store.claim("worker")

    assert claimed is not None
    assert claimed.record["operation_uuid"] == record["operation_uuid"]
    assert claimed.record["attempt"] == 2
