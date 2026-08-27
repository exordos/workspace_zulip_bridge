import copy
import datetime
import json
import os
import pathlib
import subprocess
import sys
import threading
import uuid

import pytest

from workspace_zulip_bridge import (
    canonical,
    converter,
    provider_protocol,
    scheduler,
    service,
    storage,
    zulip_adapter,
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
                     zulip_provider_events, zulip_event_cursors,
                     bridge_health CASCADE
            """
        )
        session.execute(
            """
            UPDATE observed_report_prune_state
            SET last_completed_at = NULL, last_report_uuid = NULL
            """
        )
    return store


def test_provider_account_breaker_survives_restart_and_reopens_on_generation(
    postgres_store, migrated_postgres_dsn
):
    blocked_uuid = str(uuid.uuid4())
    healthy_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        policy_uuid = str(uuid.uuid4())
        session.execute(
            """
            INSERT INTO desired_resources (
                resource_type, resource_uuid, generation, body, deleted
            ) VALUES (
                'external_provider_policy', %s, 1,
                jsonb_build_object(
                    'uuid', %s::text,
                    'generation', 1,
                    'provider_kind', 'zulip',
                    'enabled', true,
                    'emergency_suspended', false
                ),
                false
            )
            """,
            (policy_uuid, policy_uuid),
        )
        for account_uuid in (blocked_uuid, healthy_uuid):
            session.execute(
                """
                INSERT INTO desired_resources (
                    resource_type, resource_uuid, generation, body, deleted
                ) VALUES (
                    'external_account', %s, 1,
                    jsonb_build_object(
                        'uuid', %s::text,
                        'generation', 1,
                        'synchronization_enabled', true
                    ),
                    false
                )
                """,
                (account_uuid, account_uuid),
            )
    postgres_store.reconcile_participant_sync()
    postgres_store.record_provider_event(
        blocked_uuid, "blocked-queue", {"id": 1, "type": "realm_user"}
    )
    postgres_store.record_provider_event(
        healthy_uuid, "healthy-queue", {"id": 1, "type": "realm_user"}
    )

    state = postgres_store.record_provider_account_failure(
        blocked_uuid, 1, "unauthorized_account", False
    )

    assert state is not None
    assert state["provider_state"] == "auth_required"
    raced = postgres_store.record_provider_account_failure(
        blocked_uuid, 1, "provider_unavailable", True
    )
    assert raced is not None
    assert raced["provider_state"] == "auth_required"
    assert raced["provider_error_code"] == "unauthorized_account"
    assert postgres_store.eligible_account_uuids() == [healthy_uuid]
    assert not postgres_store.record_provider_account_success(blocked_uuid, 1)
    assert [
        str(row["account_uuid"]) for row in postgres_store.pending_provider_events()
    ] == [healthy_uuid]

    restarted = storage.RestAlchemyStore(migrated_postgres_dsn)
    for _ in range(100):
        assert restarted.eligible_account_uuids() == [healthy_uuid]

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = 2,
                body = jsonb_set(body, '{generation}', '2'::jsonb),
                updated_at = now()
            WHERE resource_type = 'external_account' AND resource_uuid = %s
            """,
            (blocked_uuid,),
        )
    postgres_store.reconcile_participant_sync()

    assert postgres_store.eligible_account_uuids() == sorted(
        [blocked_uuid, healthy_uuid]
    )
    with postgres_store.session() as session:
        reopened = session.execute(
            """
            SELECT provider_generation, provider_state, provider_retry_count,
                   provider_error_code
            FROM scheduler_accounts WHERE account_uuid = %s
            """,
            (blocked_uuid,),
        ).fetchone()
    assert reopened == {
        "provider_generation": 2,
        "provider_state": "ready",
        "provider_retry_count": 0,
        "provider_error_code": None,
    }


def test_stale_provider_result_cannot_change_new_account_generation(postgres_store):
    account_uuid = str(uuid.uuid4())
    policy_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO desired_resources (
                resource_type, resource_uuid, generation, body, deleted
            ) VALUES (
                'external_provider_policy', %s, 1,
                jsonb_build_object(
                    'uuid', %s::text,
                    'generation', 1,
                    'provider_kind', 'zulip',
                    'enabled', true,
                    'emergency_suspended', false
                ),
                false
            )
            """,
            (policy_uuid, policy_uuid),
        )
        session.execute(
            """
            INSERT INTO desired_resources (
                resource_type, resource_uuid, generation, body, deleted
            ) VALUES (
                'external_account', %s, 1,
                jsonb_build_object(
                    'uuid', %s::text,
                    'generation', 1,
                    'synchronization_enabled', true
                ),
                false
            )
            """,
            (account_uuid, account_uuid),
        )
    postgres_store.reconcile_participant_sync()

    attempted_generation = 1
    request_started = threading.Event()
    complete_old_request = threading.Event()
    stale_result: dict[str, object] = {}

    def finish_old_request() -> None:
        request_started.set()
        assert complete_old_request.wait(timeout=2)
        stale_result["failure"] = postgres_store.record_provider_account_failure(
            account_uuid,
            attempted_generation,
            "unauthorized_account",
            False,
        )

    request = threading.Thread(target=finish_old_request)
    request.start()
    assert request_started.wait(timeout=1)

    # Credential rotation advances desired state while the generation 1
    # provider request remains in flight.
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = 2,
                body = jsonb_set(body, '{generation}', '2'::jsonb),
                updated_at = now()
            WHERE resource_type = 'external_account' AND resource_uuid = %s
            """,
            (account_uuid,),
        )
    postgres_store.reconcile_participant_sync()
    complete_old_request.set()
    request.join(timeout=2)

    assert not request.is_alive()
    assert stale_result == {"failure": None}
    assert (
        postgres_store.record_provider_account_success(
            account_uuid, attempted_generation
        )
        is None
    )
    assert postgres_store.eligible_account_uuids() == [account_uuid]
    with postgres_store.session() as session:
        current = session.execute(
            """
            SELECT provider_generation, provider_state, provider_retry_count,
                   provider_error_code
            FROM scheduler_accounts WHERE account_uuid = %s
            """,
            (account_uuid,),
        ).fetchone()
    assert current == {
        "provider_generation": 2,
        "provider_state": "ready",
        "provider_retry_count": 0,
        "provider_error_code": None,
    }


def _explain_text(session, query: str, parameters=()) -> str:
    rows = session.execute(
        f"EXPLAIN (COSTS OFF) {query}",
        parameters,
    ).fetchall()
    return "\n".join(str(next(iter(row.values()))) for row in rows)


def _explain_json(session, query: str, parameters=()) -> dict[str, object]:
    row = session.execute(
        f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON) {query}",
        parameters,
    ).fetchone()
    result = next(iter(row.values()))
    if isinstance(result, str):
        result = json.loads(result)
    return result[0]["Plan"]


def _max_plan_actual_rows(plan: dict[str, object]) -> int:
    nested = [
        _max_plan_actual_rows(child)
        for child in plan.get("Plans", [])
        if isinstance(child, dict)
    ]
    return max([int(plan.get("Actual Rows", 0)), *nested])


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


def _enable_zulip_provider(store: storage.RestAlchemyStore) -> None:
    policy_uuid = str(uuid.uuid4())
    with store.session() as session:
        session.execute(
            """
            INSERT INTO desired_resources (
                resource_type, resource_uuid, generation, body, deleted
            ) VALUES (
                'external_provider_policy', %s, 1,
                jsonb_build_object(
                    'uuid', %s::text,
                    'generation', 1,
                    'provider_kind', 'zulip',
                    'enabled', true,
                    'emergency_suspended', false
                ),
                false
            )
            """,
            (policy_uuid, policy_uuid),
        )


def _insert_channel_assignment(
    store: storage.RestAlchemyStore,
    account_uuid: str,
    project_uuid: str,
    channel_id: int,
    history_depth: str = "all",
) -> None:
    assignment = {
        "uuid": str(uuid.uuid4()),
        "generation": 1,
        "external_account_uuid": account_uuid,
        "project_id": project_uuid,
        "selected": True,
        "history_depth": history_depth,
        "provider_chat": {
            "provider_chat_key": f"channel:{channel_id}",
            "chat_type": "channel",
        },
    }
    with store.session() as session:
        session.execute(
            """
            INSERT INTO desired_resources (
                resource_type, resource_uuid, generation, body, deleted
            ) VALUES ('external_chat_assignment', %s, 1, %s, false)
            """,
            (assignment["uuid"], json.dumps(assignment)),
        )


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


def _materialize_destination_channel(
    store: storage.RestAlchemyStore,
    account_uuid: str,
    project_uuid: str,
    channel_id: int,
) -> tuple[str, str]:
    _insert_channel_assignment(store, account_uuid, project_uuid, channel_id)
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    chat_key = f"channel:{channel_id}"
    store.remember_provider_mapping(
        account_uuid,
        "stream",
        chat_key,
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": project_uuid,
            "participants": [],
            "name": f"Channel {channel_id}",
            "description": "",
            "private": True,
            "default_topic_uuid": None,
        },
    )
    store.remember_provider_mapping(
        account_uuid,
        "topic",
        f"{channel_id}:Topic",
        topic_uuid,
        {"stream_uuid": stream_uuid, "chat_key": chat_key},
    )
    return stream_uuid, topic_uuid


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

    assert (
        postgres_store.provider_mapping(account_uuid, "topic", "42:old-topic") is None
    )
    replacement = postgres_store.provider_mapping(account_uuid, "topic", "42:new-topic")
    assert replacement is not None
    assert str(replacement["workspace_uuid"]) == topic_uuid
    assert replacement["metadata"]["name"] == "new topic"
    assert replacement["metadata"]["stream_uuid"] == stream_uuid
    assert postgres_store.control_cursor() == "cursor-2"


def test_assignment_rematerialization_preserves_notification_mapping_state(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    assignment_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())

    def change(generation, stream_name, topic_name):
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
                    "name": stream_name,
                    "description": "",
                    "private": False,
                    "default_topic_uuid": None,
                },
                "participants": [],
                "topics": [
                    {
                        "topic_uuid": topic_uuid,
                        "provider_topic_id": "42:topic",
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
        [change(1, "Engineering", "topic")],
        "cursor-1",
    )
    stream = postgres_store.provider_mapping(account_uuid, "stream", "channel:42")
    topic = postgres_store.provider_mapping(account_uuid, "topic", "42:topic")
    assert stream is not None
    assert topic is not None
    postgres_store.remember_provider_mapping(
        account_uuid,
        "stream",
        "channel:42",
        str(stream["workspace_uuid"]),
        {
            **stream["metadata"],
            "notification_mode": "all_messages",
            "notification_updated_at": "2026-08-24T08:00:00Z",
            "notification_global_desktop_notifications": True,
        },
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "42:topic",
        str(topic["workspace_uuid"]),
        {
            **topic["metadata"],
            "notification_mode": "follow",
            "notification_provider_updated_at": 1_800_000_020,
            "notification_updated_at": "2027-01-15T08:00:20Z",
        },
    )

    postgres_store.apply_desired_changes(
        [change(2, "Platform", "renamed topic")],
        "cursor-2",
    )

    stream = postgres_store.provider_mapping(account_uuid, "stream", "channel:42")
    topic = postgres_store.provider_mapping(account_uuid, "topic", "42:topic")
    assert stream is not None
    assert topic is not None
    assert stream["metadata"]["name"] == "Platform"
    assert stream["metadata"]["notification_mode"] == "all_messages"
    assert stream["metadata"]["notification_global_desktop_notifications"] is True
    assert topic["metadata"]["name"] == "renamed topic"
    assert topic["metadata"]["notification_mode"] == "follow"
    assert topic["metadata"]["notification_provider_updated_at"] == 1_800_000_020
    assert postgres_store.provider_topic_mappings(account_uuid) == [
        {"provider_id": "42:topic", "metadata": topic["metadata"]}
    ]


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

    assert postgres_store.reconcile_assignment_projection(account_uuid, "channel:42")
    topic = postgres_store.provider_mapping(account_uuid, "topic", "42:Topic")
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


@pytest.mark.parametrize("control_path", ["changes", "snapshot"])
def test_assignment_control_update_wakes_backed_off_event(
    postgres_store,
    control_path,
):
    account_uuid = str(uuid.uuid4())
    assignment_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    queue_id = "queue"
    event_id = 18
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
                "uuid": str(uuid.uuid4()),
                "name": "Engineering",
                "description": "",
                "private": False,
                "default_topic_uuid": None,
            },
            "participants": [],
            "topics": [
                {
                    "topic_uuid": str(uuid.uuid4()),
                    "provider_topic_id": "42:New topic",
                    "name": "New topic",
                    "is_default": False,
                }
            ],
        },
    }
    with postgres_store.session() as session:
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

    if control_path == "changes":
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
    else:
        postgres_store.install_snapshot(
            [{**resource, "required_capabilities": {}}],
            "cursor-1",
        )

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
    page_routes = postgres_store.workspace_mappings(
        account_uuid,
        "topic",
        [original_topic_uuid, canonical_topic_uuid, str(uuid.uuid4())],
    )
    assert set(page_routes) == {original_topic_uuid, canonical_topic_uuid}
    assert page_routes[original_topic_uuid]["provider_id"] == "42:resolved topic"
    assert page_routes[canonical_topic_uuid]["provider_id"] == "42:resolved topic"

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

    original_route = postgres_store.workspace_mapping(
        account_uuid, "topic", original_topic_uuid
    )
    assert original_route is not None
    assert original_route["provider_id"] == "42:resolved topic"
    assert original_route["metadata"]["name"] == "resolved topic"
    canonical_route = postgres_store.workspace_mapping(
        account_uuid, "topic", canonical_topic_uuid
    )
    assert canonical_route is not None
    assert canonical_route["provider_id"] == "42:renamed again"
    assert canonical_route["metadata"]["name"] == "renamed again"


def test_provider_topic_rename_is_idempotent_when_target_mapping_already_exists(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    source_workspace_uuid = str(uuid.uuid4())
    target_workspace_uuid = str(uuid.uuid4())
    source_metadata = {
        "name": "TopicA",
        "workspace_delivery_state": "committed",
    }
    target_metadata = {
        "name": "TopicB",
        "workspace_delivery_state": "committed",
    }
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "42:TopicA",
        source_workspace_uuid,
        source_metadata,
        "1",
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "42:TopicB",
        target_workspace_uuid,
        target_metadata,
        "2",
    )

    renamed = postgres_store.rename_provider_mapping(
        account_uuid,
        "topic",
        "42:TopicA",
        "42:TopicB",
        source_metadata,
        "3",
    )

    assert renamed is not None
    assert str(renamed["workspace_uuid"]) == target_workspace_uuid
    assert renamed["provider_revision"] == "2"
    assert renamed["metadata"] == target_metadata
    assert (
        str(
            postgres_store.provider_mapping(account_uuid, "topic", "42:TopicA")[
                "workspace_uuid"
            ]
        )
        == source_workspace_uuid
    )
    assert (
        str(
            postgres_store.provider_mapping(account_uuid, "topic", "42:TopicB")[
                "workspace_uuid"
            ]
        )
        == target_workspace_uuid
    )


def test_provider_topic_rename_reactivates_tombstoned_target_mapping(postgres_store):
    account_uuid = str(uuid.uuid4())
    source_workspace_uuid = str(uuid.uuid4())
    target_workspace_uuid = str(uuid.uuid4())
    current_stream_uuid = str(uuid.uuid4())
    retired_stream_uuid = str(uuid.uuid4())
    metadata = {
        "workspace_delivery_state": "committed",
        "stream_uuid": current_stream_uuid,
        "chat_key": "channel:42",
    }
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "42:TopicA",
        source_workspace_uuid,
        {**metadata, "name": "TopicA"},
        "1",
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "42:TopicB",
        target_workspace_uuid,
        {
            **metadata,
            "stream_uuid": retired_stream_uuid,
            "name": "TopicB",
        },
        "2",
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE provider_mappings SET deleted = true
            WHERE account_uuid = %s AND entity_kind = 'topic'
              AND provider_id = '42:TopicB'
            """,
            (account_uuid,),
        )

    renamed = postgres_store.rename_provider_mapping(
        account_uuid,
        "topic",
        "42:TopicA",
        "42:TopicB",
        {**metadata, "name": "TopicA"},
        "3",
    )

    assert renamed is not None
    assert str(renamed["workspace_uuid"]) == target_workspace_uuid
    assert renamed["provider_revision"] == "3"
    assert renamed["metadata"] == {**metadata, "name": "TopicA"}
    assert (
        str(
            postgres_store.provider_mapping(account_uuid, "topic", "42:TopicA")[
                "workspace_uuid"
            ]
        )
        == source_workspace_uuid
    )
    assert (
        str(
            postgres_store.provider_mapping(account_uuid, "topic", "42:TopicB")[
                "workspace_uuid"
            ]
        )
        == target_workspace_uuid
    )


def test_provider_event_records_are_enqueued_atomically(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    queue_id = "provider-message:atomic"
    event_id = 21
    assert postgres_store.record_provider_event(
        account_uuid,
        queue_id,
        {"id": event_id, "type": "heartbeat"},
    )
    valid = _provider_record(account_uuid, project_uuid)
    invalid = _provider_record(account_uuid, project_uuid, chat_id="channel:999")

    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        postgres_store.enqueue_provider_event_records(
            [valid, invalid],
            0,
            account_uuid,
            queue_id,
            event_id,
        )

    with postgres_store.session() as session:
        delivery_count = session.execute(
            """
            SELECT count(*) AS count FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()["count"]
        provider_event = session.execute(
            """
            SELECT processing_state FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert delivery_count == 0
    assert provider_event["processing_state"] == "pending"


def test_concurrent_provider_topic_renames_converge_on_one_target(postgres_store):
    account_uuid = str(uuid.uuid4())
    source_ids = ["42:TopicA", "42:TopicB"]
    source_workspace_uuids = [str(uuid.uuid4()), str(uuid.uuid4())]
    for provider_id, workspace_uuid in zip(
        source_ids, source_workspace_uuids, strict=True
    ):
        postgres_store.remember_provider_mapping(
            account_uuid,
            "topic",
            provider_id,
            workspace_uuid,
            {"name": provider_id.partition(":")[2]},
            "1",
        )

    barrier = threading.Barrier(2)
    results = []
    errors = []

    def rename(provider_id):
        barrier.wait(timeout=5)
        try:
            results.append(
                postgres_store.rename_provider_mapping(
                    account_uuid,
                    "topic",
                    provider_id,
                    "42:Target",
                    {"name": "Target"},
                    "2",
                )
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=rename, args=(provider_id,))
        for provider_id in source_ids
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(results) == 2
    assert results[0] is not None
    assert results[1] is not None
    assert results[0]["workspace_uuid"] == results[1]["workspace_uuid"]
    assert str(results[0]["workspace_uuid"]) in source_workspace_uuids
    with postgres_store.session() as session:
        active = session.execute(
            """
            SELECT provider_id FROM provider_mappings
            WHERE account_uuid = %s AND entity_kind = 'topic' AND NOT deleted
            ORDER BY provider_id
            """,
            (account_uuid,),
        ).fetchall()
    assert [row["provider_id"] for row in active] in [
        ["42:Target", "42:TopicA"],
        ["42:Target", "42:TopicB"],
    ]


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


def test_rejected_observed_report_retries_after_cooldown(postgres_store):
    resource_uuid = str(uuid.uuid4())
    observed_at = datetime.datetime.now(datetime.UTC).isoformat()
    report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_chat_catalog",
        "resource_uuid": resource_uuid,
        "observed_generation": 1,
        "status": "ready",
        "observed_at": observed_at,
    }
    assert postgres_store.enqueue_observed_report(report)
    postgres_store.apply_observed_report_results(
        [
            {
                "report_uuid": report["report_uuid"],
                "status": "rejected",
                "safe_error": {"retryable": False},
            }
        ]
    )
    retry = {
        **report,
        "report_uuid": str(uuid.uuid4()),
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    assert not postgres_store.enqueue_observed_report(retry)

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE observed_report_outbox
            SET completed_at = clock_timestamp() - interval '5 minutes 1 second'
            WHERE report_uuid = %s
            """,
            (report["report_uuid"],),
        )

    assert postgres_store.enqueue_observed_report(retry)
    assert postgres_store.pending_observed_reports() == [retry]


@pytest.mark.parametrize(
    ("event_kind", "chat_kind"),
    [
        ("message", "channel"),
        ("message", "direct"),
        ("reaction", "channel"),
        ("reaction", "direct"),
        ("user_topic", "channel"),
    ],
)
@pytest.mark.parametrize("terminal_status", ["rejected", "stale"])
def test_unapplied_catalog_report_releases_event_marker_and_republishes(
    postgres_store,
    event_kind,
    chat_kind,
    terminal_status,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    provider_message = (
        {
            "id": 701,
            "type": "stream",
            "stream_id": 77,
            "display_recipient": "Operations",
            "subject": "New dependency",
            "timestamp": 1_800_000_000,
        }
        if chat_kind == "channel"
        else {
            "id": 701,
            "type": "private",
            "display_recipient": [
                {"id": 14, "full_name": "Peer"},
                {"id": 1, "full_name": "Owner", "is_me": True},
            ],
            "timestamp": 1_800_000_000,
        }
    )
    if event_kind == "message":
        provider_event = {"id": 7, "type": "message", "message": provider_message}
    elif event_kind == "reaction":
        provider_event = {
            "id": 7,
            "type": "reaction",
            "message_id": 701,
            "user_id": 2,
            "emoji_name": "thumbs_up",
            "emoji_code": "1f44d",
            "reaction_type": "unicode_emoji",
            "op": "add",
        }
    else:
        provider_event = {
            "id": 7,
            "type": "user_topic",
            "stream_id": 77,
            "topic_name": "New dependency",
            "visibility_policy": 3,
            "last_updated": 1_800_000_000,
        }
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        provider_event,
    )
    if event_kind == "reaction":
        assert (
            postgres_store.cache_provider_event_message_context(
                account_uuid,
                "queue",
                7,
                provider_message,
            )
            == provider_message
        )
    if event_kind == "user_topic":
        postgres_store.remember_provider_mapping(
            account_uuid,
            "stream",
            "channel:77",
            str(uuid.uuid4()),
            {"name": "Operations"},
        )

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    catalog_event = (
        provider_event
        if event_kind == "user_topic"
        else {"id": 7, "type": "message", "message": provider_message}
    )

    def publish_catalog() -> bool:
        return instance._queue_event_catalog(
            account_uuid,
            catalog_event,
            "https://zulip.example.invalid",
            (account_uuid, "queue", 7),
        )

    assert publish_catalog()
    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        7,
        "provider_chat_assignment_pending",
    )
    report = postgres_store.pending_observed_reports()[0]
    result = {
        "report_uuid": report["report_uuid"],
        "status": terminal_status,
    }
    if terminal_status == "rejected":
        result["safe_error"] = {"retryable": False}
    postgres_store.apply_observed_report_results([result])

    with postgres_store.session() as session:
        marker = session.execute(
            """
            SELECT assignment_catalog_reported_at
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 7
            """,
            (account_uuid,),
        ).fetchone()
    assert marker["assignment_catalog_reported_at"] is None

    assert not publish_catalog()
    if terminal_status == "rejected":
        with postgres_store.session() as session:
            session.execute(
                """
                UPDATE observed_report_outbox
                SET completed_at =
                    clock_timestamp() - interval '5 minutes 1 second'
                WHERE report_uuid = %s
                """,
                (report["report_uuid"],),
            )
    else:
        account = postgres_store.desired_resource(
            "external_account",
            account_uuid,
        )
        cursor = postgres_store.provider_event_cursor(account_uuid)
        assert account is not None
        assert cursor is not None
        generation = 2
        account = {
            **account,
            "resource_type": "external_account",
            "generation": generation,
        }
        postgres_store.apply_desired_changes(
            [
                {
                    "change_uuid": str(uuid.uuid4()),
                    "sequence": generation,
                    "resource_type": "external_account",
                    "resource_uuid": account_uuid,
                    "operation": "upsert",
                    "generation": generation,
                    "required_capabilities": {},
                    "resource": account,
                }
            ],
            "cursor-new-account-generation",
        )
        postgres_store.update_provider_event_cursor(
            account_uuid,
            "registration-new-account-generation",
            int(cursor["last_event_id"]),
            provider_realm_uuid=str(cursor["provider_realm_uuid"]),
            provider_owner_user_id=str(cursor["provider_owner_user_id"]),
            provider_account_generation=generation,
        )

    assert publish_catalog()
    with postgres_store.session() as session:
        state = session.execute(
            """
            SELECT event.assignment_catalog_reported_at,
                   count(report.report_uuid) AS report_count,
                   array_agg(
                       (report.body->>'observed_generation')::bigint
                       ORDER BY report.created_at
                   ) AS report_generations
            FROM zulip_provider_events AS event
            CROSS JOIN observed_report_outbox AS report
            WHERE event.account_uuid = %s
              AND event.queue_id = 'queue' AND event.event_id = 7
              AND report.body->>'resource_type' = 'external_chat_catalog'
            GROUP BY event.assignment_catalog_reported_at
            """,
            (account_uuid,),
        ).fetchone()
    assert state["assignment_catalog_reported_at"] is not None
    assert state["report_count"] == 2
    assert state["report_generations"] == (
        [1, 2] if terminal_status == "stale" else [1, 1]
    )


@pytest.mark.parametrize("terminal_status", ["rejected", "stale"])
def test_unapplied_catalog_result_serializes_with_event_marker_update(
    postgres_store,
    monkeypatch,
    terminal_status,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    provider_message = {
        "id": 701,
        "type": "stream",
        "stream_id": 77,
        "display_recipient": "Operations",
        "subject": "New dependency",
        "timestamp": 1_800_000_000,
    }
    provider_event = {"id": 7, "type": "message", "message": provider_message}
    assert postgres_store.record_provider_event(account_uuid, "queue", provider_event)

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    assert instance._queue_event_catalog(
        account_uuid,
        provider_event,
        "https://zulip.example.invalid",
    )
    report = postgres_store.pending_observed_reports()[0]

    original_enqueue = storage.RestAlchemyStore._enqueue_observed_report_in_session
    report_locked = threading.Event()
    release_marker_update = threading.Event()

    def pause_after_report_ensure(session, supplied_report, *, confirm_existing):
        durable = original_enqueue(
            session,
            supplied_report,
            confirm_existing=confirm_existing,
        )
        report_locked.set()
        assert release_marker_update.wait(timeout=5)
        return durable

    monkeypatch.setattr(
        storage.RestAlchemyStore,
        "_enqueue_observed_report_in_session",
        staticmethod(pause_after_report_ensure),
    )
    failures = []
    ensure_results = []
    result_started = threading.Event()
    result_finished = threading.Event()

    def ensure_and_mark():
        try:
            ensure_results.append(
                postgres_store.ensure_provider_event_catalog_report(
                    report,
                    account_uuid,
                    "queue",
                    7,
                )
            )
        except Exception as exc:  # pragma: no cover - reported by assertion below
            failures.append(exc)

    def apply_terminal_result():
        result_started.set()
        try:
            postgres_store.apply_observed_report_results(
                [
                    {
                        "report_uuid": report["report_uuid"],
                        "status": terminal_status,
                        "safe_error": {"retryable": False},
                    }
                ]
            )
        except Exception as exc:  # pragma: no cover - reported by assertion below
            failures.append(exc)
        finally:
            result_finished.set()

    ensure_thread = threading.Thread(target=ensure_and_mark)
    result_thread = threading.Thread(target=apply_terminal_result)
    ensure_thread.start()
    assert report_locked.wait(timeout=5)
    result_thread.start()
    assert result_started.wait(timeout=5)
    assert not result_finished.wait(timeout=0.2)

    release_marker_update.set()
    ensure_thread.join(timeout=5)
    result_thread.join(timeout=5)

    assert not ensure_thread.is_alive()
    assert not result_thread.is_alive()
    assert failures == []
    assert ensure_results == [True]
    with postgres_store.session() as session:
        final_state = session.execute(
            """
            SELECT event.assignment_catalog_reported_at, report.result_status
            FROM zulip_provider_events AS event
            JOIN observed_report_outbox AS report
              ON report.report_uuid = %s
            WHERE event.account_uuid = %s
              AND event.queue_id = 'queue' AND event.event_id = 7
            """,
            (report["report_uuid"], account_uuid),
        ).fetchone()
    assert final_state == {
        "assignment_catalog_reported_at": None,
        "result_status": terminal_status,
    }


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


def test_pending_observed_reports_keep_semantically_latest_resource_state(
    postgres_store,
):
    resource_uuid = str(uuid.uuid4())
    newer = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_account",
        "resource_uuid": resource_uuid,
        "observed_generation": 1,
        "status": "live_ready",
        "reason": "live",
        "observed_at": "2026-08-27T12:00:01Z",
    }
    older = {
        **newer,
        "report_uuid": str(uuid.uuid4()),
        "status": "degraded",
        "reason": "retry",
        "observed_at": "2026-08-27T12:00:00Z",
    }

    assert postgres_store.enqueue_observed_report(newer)
    assert postgres_store.enqueue_observed_report(older)

    assert postgres_store.pending_observed_reports() == [newer]
    with postgres_store.session() as session:
        rows = session.execute(
            """
            SELECT report_uuid, result_status
            FROM observed_report_outbox
            WHERE body->>'resource_uuid' = %s
            ORDER BY body->>'observed_at'
            """,
            (resource_uuid,),
        ).fetchall()
    assert rows == [
        {
            "report_uuid": uuid.UUID(str(older["report_uuid"])),
            "result_status": "superseded",
        },
        {
            "report_uuid": uuid.UUID(str(newer["report_uuid"])),
            "result_status": None,
        },
    ]


def test_pending_observed_reports_keep_highest_resource_generation(postgres_store):
    resource_uuid = str(uuid.uuid4())
    current = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_account",
        "resource_uuid": resource_uuid,
        "observed_generation": 2,
        "status": "live_ready",
        "observed_at": "2026-08-27T12:00:00Z",
    }
    stale = {
        **current,
        "report_uuid": str(uuid.uuid4()),
        "observed_generation": 1,
        "status": "degraded",
        "observed_at": "2026-08-27T13:00:00Z",
    }

    assert postgres_store.enqueue_observed_report(stale)
    assert postgres_store.enqueue_observed_report(current)

    assert postgres_store.pending_observed_reports() == [current]


def test_catalog_marker_does_not_cover_changed_direct_participants(postgres_store):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    base_message = {
        "type": "private",
        "display_recipient": [
            {
                "id": 1,
                "full_name": "Owner",
                "email": "owner@example.invalid",
                "is_me": True,
            },
            {
                "id": 2,
                "full_name": "Old name",
                "email": "peer@example.invalid",
                "is_me": False,
            },
        ],
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {"id": 1, "type": "message", "message": {"id": 701, **base_message}},
    )
    renamed_message = json.loads(json.dumps(base_message))
    renamed_message["display_recipient"][1]["full_name"] = "New name"
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {"id": 2, "type": "message", "message": {"id": 702, **renamed_message}},
    )
    report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_chat_catalog",
        "resource_uuid": str(uuid.uuid4()),
        "observed_generation": 1,
        "status": "ready",
        "observed_at": "2026-08-27T12:00:00Z",
    }

    assert postgres_store.ensure_provider_event_catalog_report(
        report,
        account_uuid,
        "queue",
        1,
    )

    with postgres_store.session() as session:
        rows = session.execute(
            """
            SELECT event_id, assignment_catalog_reported_at IS NOT NULL AS marked
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue'
            ORDER BY event_id
            """,
            (account_uuid,),
        ).fetchall()
    assert rows == [
        {"event_id": 1, "marked": True},
        {"event_id": 2, "marked": False},
    ]


def test_pending_observed_reports_round_robin_catalog_accounts(postgres_store):
    first_account_uuid = str(uuid.uuid4())
    second_account_uuid = str(uuid.uuid4())

    for account_uuid, chat_ids in (
        (first_account_uuid, (1, 2, 3)),
        (second_account_uuid, (4,)),
    ):
        for chat_id in chat_ids:
            report = {
                "report_uuid": str(uuid.uuid4()),
                "resource_type": "external_chat_catalog",
                "resource_uuid": str(uuid.uuid4()),
                "observed_generation": 1,
                "status": "ready",
                "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
                "catalog": {
                    "external_account_uuid": account_uuid,
                    "provider_chat_key": f"channel:{chat_id}",
                },
            }
            assert postgres_store.enqueue_observed_report(report)

    pending = postgres_store.pending_observed_reports(limit=2)

    assert {report["catalog"]["external_account_uuid"] for report in pending} == {
        first_account_uuid,
        second_account_uuid,
    }


def test_pending_observed_reports_prioritize_account_control_state(postgres_store):
    catalog_report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_chat_catalog",
        "resource_uuid": str(uuid.uuid4()),
        "observed_generation": 1,
        "status": "ready",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "catalog": {
            "external_account_uuid": str(uuid.uuid4()),
            "provider_chat_key": "channel:1",
        },
    }
    account_report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_account",
        "resource_uuid": str(uuid.uuid4()),
        "observed_generation": 1,
        "status": "live_ready",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    assert postgres_store.enqueue_observed_report(catalog_report)
    assert postgres_store.enqueue_observed_report(account_report)

    assert postgres_store.pending_observed_reports(limit=1) == [account_report]


def test_pending_observed_reports_prioritize_chat_materialization_over_status(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    status_report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_chat_assignment",
        "resource_uuid": str(uuid.uuid4()),
        "observed_generation": 1,
        "status": "backfill",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    materialization_report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_chat_catalog",
        "resource_uuid": str(uuid.uuid4()),
        "observed_generation": 1,
        "status": "ready",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "catalog": {
            "operation": "upsert",
            "external_account_uuid": account_uuid,
            "source": {"provider_chat_key": "direct:9,15"},
        },
    }
    assert postgres_store.enqueue_observed_report(status_report)
    assert postgres_store.enqueue_observed_report(materialization_report)

    assert postgres_store.has_pending_chat_materializations()
    assert postgres_store.pending_observed_reports(limit=1) == [
        materialization_report
    ]

    postgres_store.apply_observed_report_results(
        [
            {
                "report_uuid": materialization_report["report_uuid"],
                "status": "applied",
            }
        ]
    )

    assert not postgres_store.has_pending_chat_materializations()


def test_deferred_chat_materialization_remains_in_delivery_gate(postgres_store):
    materialization_report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_chat_catalog",
        "resource_uuid": str(uuid.uuid4()),
        "observed_generation": 1,
        "status": "ready",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "catalog": {
            "operation": "upsert",
            "external_account_uuid": str(uuid.uuid4()),
            "source": {"provider_chat_key": "direct:9,15"},
        },
    }
    assert postgres_store.enqueue_observed_report(materialization_report)

    postgres_store.apply_observed_report_results(
        [
            {
                "report_uuid": materialization_report["report_uuid"],
                "status": "rejected",
                "safe_error": {"retryable": True},
            }
        ]
    )

    with postgres_store.session() as session:
        deferred = session.execute(
            """
            SELECT completed_at IS NULL AS incomplete,
                   available_at > now() AS deferred,
                   attempts
            FROM observed_report_outbox
            WHERE report_uuid = %s
            """,
            (materialization_report["report_uuid"],),
        ).fetchone()

    assert deferred == {"incomplete": True, "deferred": True, "attempts": 1}
    assert postgres_store.pending_observed_reports() == []
    assert postgres_store.has_pending_chat_materializations()

    postgres_store.apply_observed_report_results(
        [
            {
                "report_uuid": materialization_report["report_uuid"],
                "status": "applied",
            }
        ]
    )
    assert not postgres_store.has_pending_chat_materializations()


def test_chat_materialization_blocks_only_its_matching_delivery(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    blocked_delivery = _provider_record(account_uuid, project_uuid)
    ready_delivery = _provider_record(
        account_uuid,
        project_uuid,
        chat_id="channel:43",
    )
    ready_assignment_uuid = str(uuid.uuid4())
    ready_assignment = {
        "uuid": ready_assignment_uuid,
        "generation": 1,
        "external_account_uuid": account_uuid,
        "project_id": project_uuid,
        "selected": True,
        "history_depth": "30_days",
        "provider_chat": {
            "provider_chat_key": "channel:43",
            "chat_type": "channel",
        },
    }
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO desired_resources (
                resource_type, resource_uuid, generation, body, deleted
            ) VALUES ('external_chat_assignment', %s, 1, %s, false)
            """,
            (ready_assignment_uuid, json.dumps(ready_assignment)),
        )
        blocked_assignment = session.execute(
            """
            SELECT resource_uuid
            FROM desired_resources
            WHERE resource_type = 'external_chat_assignment'
              AND body->>'external_account_uuid' = %s
              AND body->'provider_chat'->>'provider_chat_key' = 'channel:42'
              AND NOT deleted
            """,
            (account_uuid,),
        ).fetchone()
    assert blocked_assignment is not None
    materialization_report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_chat_catalog",
        "resource_uuid": str(blocked_assignment["resource_uuid"]),
        "observed_generation": 1,
        "status": "ready",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "catalog": {
            "operation": "upsert",
            "external_account_uuid": account_uuid,
            "source": {"provider_chat_key": "channel:42"},
        },
    }
    assert postgres_store.enqueue_observed_report(materialization_report)
    assert postgres_store.enqueue_workspace_delivery(blocked_delivery, 0)
    assert postgres_store.enqueue_workspace_delivery(ready_delivery, 0)

    assert postgres_store.has_pending_chat_materializations()
    assert postgres_store.has_pending_workspace_deliveries(0, 0)
    assert postgres_store.pending_workspace_deliveries(0, 0) == [ready_delivery]

    postgres_store.apply_observed_report_results(
        [
            {
                "report_uuid": materialization_report["report_uuid"],
                "status": "applied",
            }
        ]
    )
    selected = postgres_store.pending_workspace_deliveries(0, 0)
    assert {record["record_uuid"] for record in selected} == {
        blocked_delivery["record_uuid"],
        ready_delivery["record_uuid"],
    }


def test_concurrent_chat_materialization_commit_blocks_matching_delivery_selection(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    delivery = _provider_record(account_uuid, project_uuid)
    with postgres_store.session() as session:
        assignment = session.execute(
            """
            SELECT resource_uuid
            FROM desired_resources
            WHERE resource_type = 'external_chat_assignment'
              AND body->>'external_account_uuid' = %s
              AND NOT deleted
            """,
            (account_uuid,),
        ).fetchone()
    assert assignment is not None
    materialization_report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_chat_catalog",
        "resource_uuid": str(assignment["resource_uuid"]),
        "observed_generation": 1,
        "status": "ready",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "catalog": {
            "operation": "upsert",
            "external_account_uuid": account_uuid,
            "source": {"provider_chat_key": "channel:42"},
        },
    }
    gate_checked = threading.Event()
    producer_committed = threading.Event()
    selected: list[dict[str, object]] = []
    failures: list[BaseException] = []

    def select_after_stale_gate_check() -> None:
        try:
            assert not postgres_store.has_pending_chat_materializations()
            gate_checked.set()
            assert producer_committed.wait(timeout=2.0)
            selected.extend(
                postgres_store.pending_workspace_deliveries(
                    minimum_priority=0,
                    maximum_priority=0,
                )
            )
        except BaseException as error:
            failures.append(error)

    def produce_materialization_and_delivery() -> None:
        try:
            assert gate_checked.wait(timeout=2.0)
            assert postgres_store.enqueue_observed_report(materialization_report)
            assert postgres_store.enqueue_workspace_delivery(delivery, 0)
        except BaseException as error:
            failures.append(error)
        finally:
            producer_committed.set()

    selector = threading.Thread(target=select_after_stale_gate_check)
    producer = threading.Thread(target=produce_materialization_and_delivery)
    selector.start()
    producer.start()
    selector.join(timeout=2.0)
    producer.join(timeout=2.0)

    assert not selector.is_alive()
    assert not producer.is_alive()
    assert failures == []
    assert postgres_store.has_pending_chat_materializations()
    assert not postgres_store.has_pending_workspace_deliveries(0, 0)
    assert selected == []

    postgres_store.apply_observed_report_results(
        [
            {
                "report_uuid": materialization_report["report_uuid"],
                "status": "applied",
            }
        ]
    )
    assert postgres_store.pending_workspace_deliveries(0, 0) == [delivery]


def test_provider_chat_catalog_lookup_tracks_topology(postgres_store):
    account_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    chat_key = "channel:42"
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
    event = {
        "id": -1,
        "type": "user_topic",
        "stream_id": 42,
        "topic_name": "retired",
        "visibility_policy": 1,
    }
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO desired_resources (
                resource_type, resource_uuid, generation, body, deleted
            ) VALUES ('external_account', %s, 1, %s, false)
            """,
            (account_uuid, json.dumps(account)),
        )

    assert not postgres_store.provider_chat_is_cataloged(account_uuid, chat_key)
    with pytest.raises(ValueError, match="provider_chat_not_selected"):
        converter.event_records(postgres_store, account_uuid, "queue", event)

    postgres_store.merge_catalog_topology(account_uuid, chat_key, [], [])
    assert postgres_store.provider_chat_is_cataloged(account_uuid, chat_key)
    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        converter.event_records(postgres_store, account_uuid, "queue", event)

    postgres_store.delete_catalog_topology(account_uuid, chat_key)
    assert not postgres_store.provider_chat_is_cataloged(account_uuid, chat_key)


def test_registration_membership_finds_omitted_channel_topology(postgres_store):
    account_uuid = str(uuid.uuid4())
    stale_chat_key = "channel:12"
    retained_chat_key = "channel:42"
    direct_chat_key = "direct:1,2"
    postgres_store.merge_catalog_topology(account_uuid, stale_chat_key, [], [])
    postgres_store.merge_catalog_topology(account_uuid, retained_chat_key, [], [])
    postgres_store.merge_catalog_topology(account_uuid, direct_chat_key, [], [])

    retired = postgres_store.omitted_cataloged_channels(
        account_uuid, {retained_chat_key}
    )

    assert retired == [stale_chat_key]
    assert postgres_store.provider_chat_is_cataloged(account_uuid, stale_chat_key)
    assert postgres_store.provider_chat_is_cataloged(account_uuid, retained_chat_key)
    assert postgres_store.provider_chat_is_cataloged(account_uuid, direct_chat_key)

    assert postgres_store.record_provider_event(
        account_uuid,
        "replacement-queue",
        {
            "id": -1,
            "type": "user_topic",
            "stream_id": 12,
            "topic_name": "retired",
            "visibility_policy": 1,
        },
    )
    postgres_store.retry_provider_event(
        account_uuid,
        "replacement-queue",
        -1,
        "provider_chat_assignment_pending",
    )
    with postgres_store.session() as session:
        assert session.execute(
            """
            SELECT available_at > now() AS delayed
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = -1
            """,
            (account_uuid, "replacement-queue"),
        ).fetchone()["delayed"]

    report_uuid = str(uuid.uuid4())
    assert postgres_store.ensure_catalog_deletion(
        {
            "report_uuid": report_uuid,
            "resource_type": "external_chat_catalog",
            "resource_uuid": str(uuid.uuid4()),
            "observed_generation": 1,
            "status": "deleted",
        },
        account_uuid,
        stale_chat_key,
    )

    assert not postgres_store.provider_chat_is_cataloged(
        account_uuid, stale_chat_key
    )
    with postgres_store.session() as session:
        assert session.execute(
            """
            SELECT count(*) AS total
            FROM observed_report_outbox
            WHERE report_uuid = %s
            """,
            (report_uuid,),
        ).fetchone()["total"] == 1
        assert not session.execute(
            """
            SELECT available_at > now() AS delayed
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = -1
            """,
            (account_uuid, "replacement-queue"),
        ).fetchone()["delayed"]


def test_pending_observed_reports_prioritize_live_message_dependency(postgres_store):
    account_uuid = str(uuid.uuid4())
    queue_id = "live-priority"
    for chat_id in (2, 1):
        report = {
            "report_uuid": str(uuid.uuid4()),
            "resource_type": "external_chat_catalog",
            "resource_uuid": str(uuid.uuid4()),
            "observed_generation": 1,
            "status": "ready",
            "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "catalog": {
                "external_account_uuid": account_uuid,
                "source": {"provider_chat_key": f"channel:{chat_id}"},
            },
        }
        assert postgres_store.enqueue_observed_report(report)

    assert postgres_store.record_provider_event(
        account_uuid,
        queue_id,
        {
            "id": 1,
            "type": "message",
            "message": {"id": 1, "stream_id": 1},
        },
    )

    pending = postgres_store.pending_observed_reports(limit=1)

    assert pending[0]["catalog"]["source"]["provider_chat_key"] == "channel:1"


def test_pending_observed_reports_prioritize_live_user_topic_dependency(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    reports = {}
    for chat_id in (2, 1):
        report = {
            "report_uuid": str(uuid.uuid4()),
            "resource_type": "external_chat_catalog",
            "resource_uuid": str(uuid.uuid4()),
            "observed_generation": 1,
            "status": "ready",
            "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "catalog": {
                "external_account_uuid": account_uuid,
                "source": {"provider_chat_key": f"channel:{chat_id}"},
            },
        }
        reports[chat_id] = report
        assert postgres_store.enqueue_observed_report(report)

    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "user_topic",
            "stream_id": 1,
            "topic_name": "New dependency",
            "visibility_policy": 3,
            "last_updated": 1_800_000_000,
        },
    )

    assert postgres_store.pending_observed_reports(limit=1) == [reports[1]]


def test_pending_observed_reports_prioritize_direct_fifo_head(postgres_store):
    account_uuid = str(uuid.uuid4())
    reports = {}
    for chat_key in ("channel:42", "direct:9,11"):
        report = {
            "report_uuid": str(uuid.uuid4()),
            "resource_type": "external_chat_catalog",
            "resource_uuid": str(uuid.uuid4()),
            "observed_generation": 1,
            "status": "ready",
            "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "catalog": {
                "external_account_uuid": account_uuid,
                "source": {"provider_chat_key": chat_key},
            },
        }
        reports[chat_key] = report
        assert postgres_store.enqueue_observed_report(report)

    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "message",
            "message": {
                "id": 1,
                "type": "private",
                "display_recipient": [{"id": 11}, {"id": 9}],
            },
        },
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 2,
            "type": "message",
            "message": {"id": 2, "type": "stream", "stream_id": 42},
        },
    )

    assert postgres_store.pending_observed_reports(limit=1) == [reports["direct:9,11"]]


def test_observed_report_hot_queries_use_migration_indexes(postgres_store):
    resource_uuid = str(uuid.uuid4())
    catalog_account_uuid = str(uuid.uuid4())
    report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_account",
        "resource_uuid": resource_uuid,
        "observed_generation": 1,
        "status": "backfill",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    assert postgres_store.enqueue_observed_report(report)
    materialization_report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_chat_catalog",
        "resource_uuid": str(uuid.uuid4()),
        "observed_generation": 1,
        "status": "ready",
        "observed_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "catalog": {
            "operation": "upsert",
            "external_account_uuid": catalog_account_uuid,
            "source": {"provider_chat_key": "direct:9,15"},
        },
    }
    assert postgres_store.enqueue_observed_report(materialization_report)

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
        session.execute(
            """
            INSERT INTO observed_report_outbox (
                report_uuid, body, result_status, completed_at
            )
            SELECT
                md5('catalog-readiness-report-' || sample::text)::uuid,
                jsonb_build_object(
                    'resource_type', 'external_chat_catalog',
                    'resource_uuid',
                    md5('catalog-resource-' || sample::text)::uuid::text,
                    'observed_generation', 3,
                    'catalog', jsonb_build_object(
                        'external_account_uuid', CASE
                            WHEN sample %% 20 = 0 THEN %s::text
                            ELSE md5(
                                'catalog-account-' || (sample %% 20)::text
                            )::uuid::text
                        END,
                        'operation', 'upsert'
                    )
                ),
                'applied', now()
            FROM generate_series(1, 100000) AS sample
            ON CONFLICT (report_uuid) DO NOTHING
            """,
            (catalog_account_uuid,),
        )
        session.execute("ANALYZE observed_report_outbox")
        dedup_plan = _explain_text(
            session,
            """
            SELECT body FROM observed_report_outbox
            WHERE body->>'resource_type' = %s
              AND (body->>'resource_uuid')::uuid = %s::uuid
            ORDER BY (body->>'observed_generation')::bigint DESC,
                     body->>'observed_at' DESC NULLS LAST,
                     created_at DESC, report_uuid DESC
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
                                    (body->>'resource_uuid')::uuid
                       ORDER BY (body->>'observed_generation')::bigint DESC,
                                body->>'observed_at' DESC NULLS LAST,
                                created_at DESC, report_uuid DESC
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
        materialization_plan = _explain_text(
            session,
            """
            SELECT 1 FROM observed_report_outbox AS report
            WHERE report.completed_at IS NULL
              AND report.body->>'resource_type' = 'external_chat_catalog'
              AND report.body->>'status' = 'ready'
              AND COALESCE(
                  report.body->'catalog'->>'operation',
                  'upsert'
              ) = 'upsert'
            LIMIT 1
            """,
        )
        catalog_readiness_plan = _explain_text(
            session,
            """
            SELECT DISTINCT ON ((body->>'resource_uuid')::uuid) result_status
            FROM observed_report_outbox
            WHERE body->>'resource_type' = 'external_chat_catalog'
              AND (body->'catalog'->>'external_account_uuid')::uuid = %s
              AND (body->>'observed_generation')::bigint = 3
            ORDER BY (body->>'resource_uuid')::uuid,
                     (body->>'observed_generation')::bigint DESC,
                     body->>'observed_at' DESC NULLS LAST,
                     created_at DESC, report_uuid DESC
            """,
            (catalog_account_uuid,),
        )
        catalog_readiness_actual_plan = _explain_json(
            session,
            """
            SELECT DISTINCT ON ((body->>'resource_uuid')::uuid) result_status
            FROM observed_report_outbox
            WHERE body->>'resource_type' = 'external_chat_catalog'
              AND (body->'catalog'->>'external_account_uuid')::uuid = %s
              AND (body->>'observed_generation')::bigint = 3
            ORDER BY (body->>'resource_uuid')::uuid,
                     (body->>'observed_generation')::bigint DESC,
                     body->>'observed_at' DESC NULLS LAST,
                     created_at DESC, report_uuid DESC
            """,
            (catalog_account_uuid,),
        )
        prune_query = """
            WITH probe AS MATERIALIZED (
                SELECT report.report_uuid, report.body, report.completed_at
                FROM observed_report_outbox AS report
                WHERE report.completed_at IS NOT NULL
                ORDER BY report.completed_at, report.report_uuid
                LIMIT 100
            )
            SELECT count(*)
            FROM probe
            JOIN LATERAL (
                SELECT candidate.report_uuid
                FROM observed_report_outbox AS candidate
                WHERE candidate.body->>'resource_type' =
                          probe.body->>'resource_type'
                  AND (candidate.body->>'resource_uuid')::uuid =
                      (probe.body->>'resource_uuid')::uuid
                ORDER BY
                    (candidate.body->>'observed_generation')::bigint DESC,
                    candidate.body->>'observed_at' DESC NULLS LAST,
                    candidate.created_at DESC, candidate.report_uuid DESC
                LIMIT 1
            ) AS semantic_head ON true
        """
        prune_plan = _explain_text(session, prune_query)
        prune_actual_plan = _explain_json(session, prune_query)

    assert "observed_report_outbox_resource_observed_idx" in dedup_plan
    assert "observed_report_outbox_pending_order_idx" in pending_rank_plan
    assert "observed_report_outbox_pending_order_idx" in pending_order_plan
    assert (
        "observed_report_outbox_chat_materialization_idx"
        in materialization_plan
    )
    assert "observed_report_outbox_catalog_readiness_idx" in catalog_readiness_plan
    assert _max_plan_actual_rows(catalog_readiness_actual_plan) <= 5_000
    assert "observed_report_outbox_terminal_history_idx" in prune_plan
    assert "observed_report_outbox_resource_observed_idx" in prune_plan
    assert _max_plan_actual_rows(prune_actual_plan) <= 100


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


def _bind_provider_record_projection(
    record: dict[str, object],
    stream_uuid: str,
    topic_uuid: str,
    author_uuid: str,
) -> dict[str, object]:
    operation = record["operation"]
    assert isinstance(operation, dict)
    payload = operation["payload"]
    assert isinstance(payload, dict)
    payload.update(
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
        }
    )
    record["causal_lane"] = f"chat:{record['account_uuid']}:{stream_uuid}"
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


def test_workspace_delivery_probe_ignores_inactive_accounts_and_provider(
    postgres_store,
):
    policy_uuid = str(uuid.uuid4())
    active_account_uuid = str(uuid.uuid4())
    paused_account_uuid = str(uuid.uuid4())
    active_record = {
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": str(uuid.uuid4()),
        "account_uuid": active_account_uuid,
        "operation": {
            "kind": "identity.upsert",
            "entity_uuid": str(uuid.uuid4()),
        },
    }
    paused_record = {
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": str(uuid.uuid4()),
        "account_uuid": paused_account_uuid,
        "operation": {
            "kind": "identity.upsert",
            "entity_uuid": str(uuid.uuid4()),
        },
    }
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO desired_resources (
                resource_type, resource_uuid, generation, body, deleted
            ) VALUES (
                'external_provider_policy', %s, 1,
                jsonb_build_object(
                    'uuid', %s::text,
                    'generation', 1,
                    'provider_kind', 'zulip',
                    'enabled', true,
                    'emergency_suspended', false
                ),
                false
            ), (
                'external_account', %s, 1,
                jsonb_build_object(
                    'uuid', %s::text,
                    'generation', 1,
                    'synchronization_enabled', true
                ),
                false
            ), (
                'external_account', %s, 1,
                jsonb_build_object(
                    'uuid', %s::text,
                    'generation', 1,
                    'synchronization_enabled', false
                ),
                false
            )
            """,
            (
                policy_uuid,
                policy_uuid,
                active_account_uuid,
                active_account_uuid,
                paused_account_uuid,
                paused_account_uuid,
            ),
        )
        for account_uuid, record in (
            (active_account_uuid, active_record),
            (paused_account_uuid, paused_record),
        ):
            session.execute(
                """
                INSERT INTO workspace_delivery_outbox (
                    record_uuid, operation_uuid, account_uuid,
                    account_generation, priority, record
                ) VALUES (%s, %s, %s, 1, 0, %s)
                """,
                (
                    record["record_uuid"],
                    record["operation_uuid"],
                    account_uuid,
                    json.dumps(record),
                ),
            )

    assert postgres_store.has_pending_workspace_deliveries(0, 0)
    assert postgres_store.pending_workspace_deliveries(0, 0) == [active_record]

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET body = jsonb_set(
                    body,
                    '{synchronization_enabled}',
                    'false'::jsonb
                )
            WHERE resource_type = 'external_account' AND resource_uuid = %s
            """,
            (active_account_uuid,),
        )
    assert not postgres_store.has_pending_workspace_deliveries(0, 0)
    assert postgres_store.pending_workspace_deliveries(0, 0) == []

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET body = CASE
                    WHEN resource_type = 'external_account'
                    THEN jsonb_set(
                        body,
                        '{synchronization_enabled}',
                        'true'::jsonb
                    )
                    ELSE jsonb_set(
                        body,
                        '{emergency_suspended}',
                        'true'::jsonb
                    )
                END
            WHERE resource_uuid IN (%s, %s)
            """,
            (active_account_uuid, policy_uuid),
        )
    assert not postgres_store.has_pending_workspace_deliveries(0, 0)
    assert postgres_store.pending_workspace_deliveries(0, 2) == []


def test_message_delivery_waits_for_one_durable_topic_projection(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
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
        record for record in records if record["operation"]["kind"] == "topic.upsert"
    ]
    message_records = [
        record for record in records if record["operation"]["kind"] == "message.create"
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

    assert (
        postgres_store.pending_workspace_deliveries(
            minimum_priority=0, maximum_priority=0
        )
        == message_records
    )

    postgres_store.accept_result(
        _committed_result(message_records[0], provider_entity_id="101")
    )

    assert postgres_store.pending_workspace_deliveries(
        minimum_priority=0, maximum_priority=0
    ) == [message_records[1], update_message]


def test_rejected_topic_projection_does_not_suppress_fresh_retry(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, _author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "42:Topic",
        topic_uuid,
        {
            "project_uuid": project_uuid,
            "stream_uuid": stream_uuid,
            "chat_key": "channel:42",
            "name": "Topic",
            "workspace_delivery_state": "committed",
        },
    )

    def topic_record() -> dict[str, object]:
        record = _provider_record(
            account_uuid,
            project_uuid,
            kind="topic.upsert",
        )
        operation = record["operation"]
        operation["entity_uuid"] = topic_uuid
        operation["provider"]["entity_id"] = "42:Topic"
        operation["payload"] = {
            "stream_uuid": stream_uuid,
            "name": "Topic",
        }
        record["operation_sha256"] = canonical.operation_digest(record)
        return record

    rejected = topic_record()
    assert postgres_store.enqueue_workspace_delivery(rejected, 0, "queue", 1)
    assert postgres_store.mark_workspace_delivery_submitting(
        rejected["record_uuid"]
    )
    assert postgres_store.reject_provider_event_submission(
        rejected["record_uuid"],
        "provider_api_http_422",
    )
    topic_mapping = postgres_store.provider_mapping(
        account_uuid,
        "topic",
        "42:Topic",
    )
    assert topic_mapping["metadata"]["workspace_delivery_state"] == "pending"
    later_records = converter.event_records(
        postgres_store,
        account_uuid,
        "queue",
        {
            "id": 102,
            "type": "message",
            "message": _provider_history_message(102),
        },
    )
    assert any(
        record["operation"]["kind"] == "topic.upsert"
        for record in later_records
    )

    retry = topic_record()
    assert postgres_store.enqueue_workspace_delivery(retry, 0, "queue", 2)
    with postgres_store.session() as session:
        rows = session.execute(
            """
            SELECT operation_uuid, submission_state
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s
              AND record->'operation'->>'kind' = 'topic.upsert'
            ORDER BY created_at
            """,
            (account_uuid,),
        ).fetchall()
    assert rows == [
        {
            "operation_uuid": uuid.UUID(str(rejected["operation_uuid"])),
            "submission_state": "rejected",
        },
        {
            "operation_uuid": uuid.UUID(str(retry["operation_uuid"])),
            "submission_state": "pending",
        },
    ]
    assert postgres_store.pending_workspace_deliveries(0, 0) == [retry]


def test_same_event_cross_lane_topic_dependency_remains_ordered(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    topic = _provider_record(account_uuid, project_uuid, kind="topic.upsert")
    topic["causal_lane"] = f"chat:{account_uuid}:destination"
    topic["operation"]["entity_uuid"] = topic_uuid
    topic["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "name": "Destination",
    }
    topic["operation_sha256"] = canonical.operation_digest(topic)
    move = _provider_record(account_uuid, project_uuid, kind="message.update")
    move["causal_lane"] = f"chat:{account_uuid}:source"
    move["operation"]["payload"].update(
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
        }
    )
    move["operation_sha256"] = canonical.operation_digest(move)

    assert postgres_store.enqueue_workspace_delivery(topic, 2, "queue", 7)
    assert postgres_store.enqueue_workspace_delivery(move, 0, "queue", 7)
    assert topic["sequence"] == move["sequence"] == 1
    assert postgres_store.pending_workspace_deliveries(0, 0) == [topic]

    postgres_store.accept_result(_committed_result(topic))
    assert postgres_store.pending_workspace_deliveries(0, 0) == [move]


def test_read_state_waits_until_every_message_projection_is_committed(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
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
    _enable_zulip_provider(postgres_store)
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


def test_subscription_event_immediately_invalidates_ready_participants(postgres_store):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)

    postgres_store.invalidate_participant_sync(account_uuid, ["channel:42"])

    claimed = postgres_store.claim_participant_sync()
    assert claimed is not None
    assert str(claimed["account_uuid"]) == account_uuid
    assert claimed["provider_chat_key"] == "channel:42"


def test_participant_and_backfill_claims_rotate_between_accounts(postgres_store):
    first_account_uuid, first_project_uuid = _insert_account_and_assignment(
        postgres_store, "all"
    )
    second_account_uuid, second_project_uuid = _insert_account_and_assignment(
        postgres_store, "all"
    )
    _insert_channel_assignment(
        postgres_store, first_account_uuid, first_project_uuid, 43
    )
    _insert_channel_assignment(
        postgres_store, first_account_uuid, first_project_uuid, 44
    )
    _insert_channel_assignment(
        postgres_store, second_account_uuid, second_project_uuid, 43
    )
    postgres_store.reconcile_participant_sync()
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_participant_sync
            SET updated_at = CASE
                WHEN account_uuid = %s THEN now() - interval '2 minutes'
                ELSE now() - interval '1 minute'
            END
            WHERE state = 'pending'
            """,
            (first_account_uuid,),
        )

    first_participant = postgres_store.claim_participant_sync()
    assert str(first_participant["account_uuid"]) == first_account_uuid
    postgres_store.complete_participant_sync(
        first_account_uuid,
        str(first_participant["provider_chat_key"]),
        1,
        [],
        True,
    )
    second_participant = postgres_store.claim_participant_sync()
    assert str(second_participant["account_uuid"]) == second_account_uuid

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_participant_sync
            SET state = 'ready', lease_until = NULL
            WHERE state IN ('pending', 'running')
            """
        )
    postgres_store.reconcile_backfill_jobs()
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET updated_at = CASE
                WHEN account_uuid = %s THEN now() - interval '2 minutes'
                ELSE now() - interval '1 minute'
            END
            WHERE state = 'pending'
            """,
            (first_account_uuid,),
        )

    first_backfill = postgres_store.claim_backfill_job()
    assert str(first_backfill["account_uuid"]) == first_account_uuid
    postgres_store.advance_backfill_job(
        first_account_uuid,
        str(first_backfill["provider_chat_key"]),
        100,
        False,
    )
    second_backfill = postgres_store.claim_backfill_job()
    assert str(second_backfill["account_uuid"]) == second_account_uuid


def test_participant_batch_reuses_one_fair_account_claim(postgres_store):
    first_account_uuid, first_project_uuid = _insert_account_and_assignment(
        postgres_store, "all"
    )
    second_account_uuid, second_project_uuid = _insert_account_and_assignment(
        postgres_store, "all"
    )
    _insert_channel_assignment(
        postgres_store, first_account_uuid, first_project_uuid, 43
    )
    _insert_channel_assignment(
        postgres_store, first_account_uuid, first_project_uuid, 44
    )
    _insert_channel_assignment(
        postgres_store, second_account_uuid, second_project_uuid, 43
    )
    postgres_store.reconcile_participant_sync()
    with postgres_store.session() as session:
        session.execute("UPDATE scheduler_accounts SET last_participant_sync_at = NULL")
        session.execute(
            """
            UPDATE zulip_participant_sync
            SET updated_at = CASE
                WHEN account_uuid = %s THEN now() - interval '2 minutes'
                ELSE now() - interval '1 minute'
            END
            WHERE state = 'pending'
            """,
            (first_account_uuid,),
        )

    first_batch = postgres_store.claim_participant_sync_batch(10)

    assert len(first_batch) == 2
    assert {str(job["account_uuid"]) for job in first_batch} == {first_account_uuid}
    assert {job["provider_chat_key"] for job in first_batch} == {
        "channel:43",
        "channel:44",
    }
    assert all(isinstance(job["assignment"], dict) for job in first_batch)
    postgres_store.complete_participant_sync_batch(
        [
            {
                **job,
                "provider_user_ids": [],
                "ready": True,
            }
            for job in first_batch
        ]
    )

    second_batch = postgres_store.claim_participant_sync_batch(10)

    assert len(second_batch) == 1
    assert str(second_batch[0]["account_uuid"]) == second_account_uuid
    assert second_batch[0]["provider_chat_key"] == "channel:43"


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


def test_provider_result_acknowledgements_are_applied_as_one_batch(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    records = [
        _provider_record(account_uuid, project_uuid),
        _provider_record(account_uuid, project_uuid),
    ]
    result_uuids = [str(uuid.uuid4()), str(uuid.uuid4())]
    with postgres_store.session() as session:
        for sequence, (record, result_uuid) in enumerate(
            zip(records, result_uuids, strict=True), start=1
        ):
            session.execute(
                """
                INSERT INTO bridge_operations (
                    record_uuid, operation_uuid, attempt, operation_sha256,
                    account_uuid, project_uuid, origin, causal_lane,
                    lane_sequence, priority, state, record, result_record
                ) VALUES (
                    %s, %s, 1, %s, %s, %s, 'zulip', %s, %s, 0,
                    'committed', %s::jsonb, %s::jsonb
                )
                """,
                (
                    record["record_uuid"],
                    record["operation_uuid"],
                    record["operation_sha256"],
                    account_uuid,
                    project_uuid,
                    record["causal_lane"],
                    sequence,
                    json.dumps(record),
                    json.dumps({"record_uuid": result_uuid}),
                ),
            )

    postgres_store.finalize_provider_result_responses(
        [
            (result_uuids[0], "applied", None),
            (result_uuids[1], "rejected", None),
        ]
    )

    with postgres_store.session() as session:
        rows = session.execute(
            """
            SELECT result_record->>'record_uuid' AS result_record_uuid,
                   result_sent_at IS NOT NULL AS sent,
                   manual_reconciliation_required, last_error_code
            FROM bridge_operations
            ORDER BY lane_sequence
            """
        ).fetchall()
    assert rows == [
        {
            "result_record_uuid": result_uuids[0],
            "sent": True,
            "manual_reconciliation_required": False,
            "last_error_code": None,
        },
        {
            "result_record_uuid": result_uuids[1],
            "sent": True,
            "manual_reconciliation_required": True,
            "last_error_code": "provider_result_rejected",
        },
    ]


def test_provider_result_queue_and_retention_queries_stay_index_bounded(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO bridge_operations (
                record_uuid, operation_uuid, attempt, operation_sha256,
                account_uuid, project_uuid, origin, causal_lane,
                lane_sequence, priority, state, record, result_record,
                result_sent_at, created_at, updated_at
            )
            SELECT md5('result-record-' || sample::text)::uuid,
                   md5('result-operation-' || sample::text)::uuid,
                   1, repeat('a', 64), %s, %s, 'workspace',
                   'read:' || sample::text, 1, 0, 'committed',
                   jsonb_build_object(
                       'operation', jsonb_build_object(
                           'kind', 'read_state.set'
                       )
                   ),
                   jsonb_build_object(
                       'record_uuid',
                       md5('provider-result-' || sample::text)::uuid::text
                   ),
                   CASE WHEN sample %% 2 = 0 THEN now() ELSE NULL END,
                   now() - interval '20 minutes',
                   now() - interval '20 minutes'
            FROM generate_series(1, 20000) AS sample
            """,
            (account_uuid, project_uuid),
        )
        session.execute("ANALYZE bridge_operations")
        pending_query = """
            SELECT result_record FROM bridge_operations
            WHERE result_record IS NOT NULL AND result_sent_at IS NULL
            ORDER BY updated_at, record_uuid LIMIT 100
        """
        retention_query = """
            SELECT record_uuid FROM bridge_operations
            WHERE state = 'committed'
              AND result_sent_at IS NOT NULL
              AND NOT manual_reconciliation_required
              AND last_error_code IS DISTINCT FROM
                  'provider_result_stale_lease'
              AND updated_at < now() - interval '10 minutes'
              AND record->'operation'->>'kind' = 'read_state.set'
            ORDER BY updated_at, record_uuid LIMIT 100
        """
        pending_plan = _explain_text(session, pending_query)
        pending_actual_plan = _explain_json(session, pending_query)
        retention_plan = _explain_text(session, retention_query)
        retention_actual_plan = _explain_json(session, retention_query)

    assert "bridge_operations_pending_result_idx" in pending_plan
    assert "bridge_operations_terminal_read_retention_idx" in retention_plan
    assert _max_plan_actual_rows(pending_actual_plan) <= 100
    assert _max_plan_actual_rows(retention_actual_plan) <= 100


@pytest.mark.parametrize(
    ("status", "expected_message_count"),
    [
        ("applied", 0),
        ("duplicate", 0),
        ("rejected", 2),
        ("stale_lease", 2),
    ],
)
def test_provider_result_acknowledgement_scrubs_terminal_read_payload(
    postgres_store,
    status,
    expected_message_count,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(account_uuid, project_uuid, kind="read_state.set")
    operation = record["operation"]
    assert isinstance(operation, dict)
    payload = operation["payload"]
    assert isinstance(payload, dict)
    payload["message_uuids"] = [str(uuid.uuid4()), str(uuid.uuid4())]
    payload["reader_uuid"] = str(uuid.uuid4())
    payload["read"] = True
    record["transport"] = {
        "provider_operation_uuid": record["record_uuid"],
        "lease_uuid": str(uuid.uuid4()),
    }
    record["operation_sha256"] = canonical.operation_digest(record)
    result_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO bridge_operations (
                record_uuid, operation_uuid, attempt, operation_sha256,
                account_uuid, project_uuid, origin, causal_lane, lane_sequence,
                priority, state, record, result_record
            ) VALUES (%s, %s, 1, %s, %s, %s, 'workspace', %s, 1, 0,
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
            SELECT record #> '{operation,payload,message_uuids}' AS message_uuids,
                   operation_sha256,
                   record->>'operation_sha256' AS record_operation_sha256,
                   result_sent_at, last_error_code
            FROM bridge_operations
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
    assert len(row["message_uuids"]) == expected_message_count
    assert row["operation_sha256"] == record["operation_sha256"]
    assert row["record_operation_sha256"] == record["operation_sha256"]
    if status in {"rejected", "stale_lease"}:
        assert row["result_sent_at"] is not None
        assert postgres_store.pending_results() == []
        assert row["last_error_code"] == f"provider_result_{status}"
        with postgres_store.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET updated_at = now() - interval '20 minutes'
                WHERE record_uuid = %s
                """,
                (record["record_uuid"],),
            )
        postgres_store.prune_terminal_delivery_state()
        with postgres_store.session() as session:
            retained = session.execute(
                "SELECT count(*) AS count FROM bridge_operations "
                "WHERE record_uuid = %s",
                (record["record_uuid"],),
            ).fetchone()
        assert retained["count"] == 1
    if status in {"applied", "duplicate", "stale_lease"}:
        renewed_lease_uuid = str(uuid.uuid4())
        transport = record["transport"]
        assert isinstance(transport, dict)
        transport["lease_uuid"] = renewed_lease_uuid
        assert postgres_store.bind_provider_lease(record) is True
        pending = postgres_store.pending_results()
        assert len(pending) == 1
        assert pending[0]["transport"]["lease_uuid"] == renewed_lease_uuid
        with postgres_store.session() as session:
            rebound = session.execute(
                """
                SELECT record #> '{operation,payload,message_uuids}'
                           AS message_uuids,
                       result_sent_at
                FROM bridge_operations
                WHERE record_uuid = %s
                """,
                (record["record_uuid"],),
            ).fetchone()
        assert rebound["message_uuids"] == (
            [] if status in {"applied", "duplicate"} else payload["message_uuids"]
        )
        assert rebound["result_sent_at"] is None
        if status == "stale_lease":
            postgres_store.finalize_provider_result_response(
                result_uuid,
                "applied",
                renewed_lease_uuid,
            )
            assert postgres_store.pending_results() == []


def test_provider_read_rebind_preserves_persisted_revision_identity(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    provider_operation_uuid = str(uuid.uuid4())
    public_operation_uuid = str(uuid.uuid4())
    old_lease_uuid = str(uuid.uuid4())
    renewed_lease_uuid = str(uuid.uuid4())
    persisted = _provider_record(
        account_uuid,
        project_uuid,
        kind="read_state.set",
    )
    persisted.update(
        {
            "record_uuid": provider_operation_uuid,
            "operation_uuid": provider_operation_uuid,
            "origin": "workspace",
            "sequence": 7,
            "predecessor_operation_uuid": str(uuid.uuid4()),
            "expires_at": "2026-08-27T12:05:00Z",
            "transport": {
                "provider_operation_uuid": provider_operation_uuid,
                "lease_uuid": old_lease_uuid,
            },
        }
    )
    operation = persisted["operation"]
    assert isinstance(operation, dict)
    operation["occurred_at"] = "2026-08-27T12:00:00Z"
    operation["payload"] = {
        "message_uuids": [str(uuid.uuid4()), str(uuid.uuid4())],
        "reader_uuid": str(uuid.uuid4()),
        "read": True,
    }
    persisted["operation_sha256"] = canonical.operation_digest(persisted)
    result_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO bridge_operations (
                record_uuid, operation_uuid, attempt, operation_sha256,
                account_uuid, project_uuid, origin, causal_lane, lane_sequence,
                predecessor_operation_uuid, priority, state, expires_at,
                record, result_record, result_sent_at
            ) VALUES (
                %s, %s, 1, %s, %s, %s, 'workspace', %s, 7, %s, 0,
                'committed', %s, %s::jsonb, %s::jsonb, now()
            )
            """,
            (
                provider_operation_uuid,
                provider_operation_uuid,
                persisted["operation_sha256"],
                account_uuid,
                project_uuid,
                persisted["causal_lane"],
                persisted["predecessor_operation_uuid"],
                persisted["expires_at"],
                json.dumps(persisted),
                json.dumps(
                    {
                        "record_uuid": result_uuid,
                        "transport": persisted["transport"],
                    }
                ),
            ),
        )

    postgres_store.finalize_provider_result_response(result_uuid, "applied")
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE bridge_operations
            SET record = record - '_workspace_read_semantic_sha256'
            WHERE record_uuid = %s
            """,
            (provider_operation_uuid,),
        )

    renewed = copy.deepcopy(persisted)
    renewed["operation_uuid"] = public_operation_uuid
    renewed["sequence"] = 0
    renewed["predecessor_operation_uuid"] = None
    renewed["created_at"] = "2026-08-27T12:01:00Z"
    renewed["expires_at"] = "2026-08-27T12:06:00Z"
    renewed_operation = renewed["operation"]
    assert isinstance(renewed_operation, dict)
    renewed_operation["occurred_at"] = "2026-08-27T12:01:00Z"
    renewed["transport"] = {
        "provider_operation_uuid": provider_operation_uuid,
        "lease_uuid": renewed_lease_uuid,
    }
    renewed["operation_sha256"] = canonical.operation_digest(renewed)

    legacy_mismatch = copy.deepcopy(renewed)
    legacy_mismatch_operation = legacy_mismatch["operation"]
    assert isinstance(legacy_mismatch_operation, dict)
    legacy_mismatch_payload = legacy_mismatch_operation["payload"]
    assert isinstance(legacy_mismatch_payload, dict)
    legacy_mismatch_payload["reader_uuid"] = str(uuid.uuid4())
    legacy_mismatch["operation_sha256"] = canonical.operation_digest(legacy_mismatch)
    assert postgres_store.bind_provider_lease(legacy_mismatch) is False
    assert postgres_store.pending_results() == []

    assert postgres_store.bind_provider_lease(renewed) is True

    with postgres_store.session() as session:
        rebound = session.execute(
            """
            SELECT operation_uuid, operation_sha256,
                   record #>> '{transport,lease_uuid}' AS record_lease_uuid,
                   record #> '{operation,payload,message_uuids}'
                       AS message_uuids,
                   record->>'_workspace_read_semantic_sha256'
                       AS read_semantic_sha256,
                   result_record #>> '{transport,lease_uuid}'
                       AS result_lease_uuid,
                   result_sent_at, expires_at
            FROM bridge_operations
            WHERE record_uuid = %s
            """,
            (provider_operation_uuid,),
        ).fetchone()
    assert str(rebound["operation_uuid"]) == provider_operation_uuid
    assert rebound["operation_sha256"] == persisted["operation_sha256"]
    assert rebound["record_lease_uuid"] == renewed_lease_uuid
    assert rebound["message_uuids"] == []
    assert len(rebound["read_semantic_sha256"]) == 64
    assert rebound["result_lease_uuid"] == renewed_lease_uuid
    assert rebound["result_sent_at"] is None
    assert (
        rebound["expires_at"]
        .astimezone(datetime.UTC)
        .isoformat()
        .replace("+00:00", "Z")
        == renewed["expires_at"]
    )

    postgres_store.finalize_provider_result_response(
        result_uuid,
        "duplicate",
        old_lease_uuid,
    )
    assert postgres_store.pending_results() != []
    postgres_store.finalize_provider_result_response(
        result_uuid,
        "duplicate",
        renewed_lease_uuid,
    )
    assert postgres_store.pending_results() == []
    mismatched = copy.deepcopy(renewed)
    mismatched_operation = mismatched["operation"]
    assert isinstance(mismatched_operation, dict)
    mismatched_payload = mismatched_operation["payload"]
    assert isinstance(mismatched_payload, dict)
    mismatched_payload["message_uuids"] = [str(uuid.uuid4())]
    mismatched["operation_sha256"] = canonical.operation_digest(mismatched)
    mismatched_transport = mismatched["transport"]
    assert isinstance(mismatched_transport, dict)
    mismatched_transport["lease_uuid"] = str(uuid.uuid4())

    assert postgres_store.bind_provider_lease(mismatched) is False
    with postgres_store.session() as session:
        result_sent_at = session.execute(
            """
            SELECT result_sent_at
            FROM bridge_operations
            WHERE record_uuid = %s
            """,
            (provider_operation_uuid,),
        ).fetchone()["result_sent_at"]
    assert result_sent_at is not None


def test_initial_backfill_gate_uses_account_priority_index(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO operation_idempotency (
                operation_uuid, operation_sha256, terminal_outcome
            )
            SELECT md5('readiness-operation-' || sample::text)::uuid,
                   repeat('a', 64), 'committed'
            FROM generate_series(1, 5000) AS sample
            """
        )
        session.execute(
            """
            INSERT INTO workspace_delivery_outbox (
                record_uuid, operation_uuid, account_uuid,
                account_generation, priority, record
            )
            SELECT md5('readiness-record-' || sample::text)::uuid,
                   md5('readiness-operation-' || sample::text)::uuid,
                   CASE
                       WHEN sample %% 20 = 0 THEN %s::uuid
                       ELSE md5(
                           'readiness-account-' || (sample %% 20)::text
                       )::uuid
                   END,
                   1, 2, '{}'::jsonb
            FROM generate_series(1, 5000) AS sample
            """,
            (account_uuid,),
        )
        session.execute("ANALYZE workspace_delivery_outbox")
        plan = _explain_text(
            session,
            """
            SELECT 1
            FROM workspace_delivery_outbox AS delivery
            JOIN desired_resources AS account
              ON account.resource_type = 'external_account'
             AND account.resource_uuid = delivery.account_uuid
             AND NOT account.deleted
            LEFT JOIN operation_idempotency AS operation
              ON operation.operation_uuid = delivery.operation_uuid
            WHERE delivery.account_uuid = %s
              AND delivery.priority = 2
              AND delivery.account_generation = account.generation
              AND operation.terminal_outcome IS DISTINCT FROM 'committed'
            """,
            (account_uuid,),
        )

    assert "workspace_delivery_outbox_initial_backfill_idx" in plan


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


def test_queue_loss_catchup_accepts_an_empty_provider_page(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    postgres_store.begin_provider_queue_catchup(account_uuid)

    postgres_store.advance_provider_catchup(
        account_uuid,
        "channel:42",
        [],
        None,
        True,
    )

    with postgres_store.session() as session:
        row = session.execute(
            """
            SELECT seen_provider_message_ids, page_count, state
            FROM zulip_queue_catchup_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
    assert row == {
        "seen_provider_message_ids": [],
        "page_count": 1,
        "state": "complete",
    }


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
        f"identity:{account_uuid}:{canonical_replay['operation']['entity_uuid']}"
    )
    canonical_replay["operation_sha256"] = canonical.operation_digest(canonical_replay)

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


def test_prepared_replay_reuses_a_concurrently_allocated_lane(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(account_uuid, project_uuid)
    prepared_replay = json.loads(json.dumps(record))

    assert postgres_store.enqueue_workspace_delivery(record, 2)
    assert not postgres_store.enqueue_workspace_delivery(prepared_replay, 2)

    assert prepared_replay["sequence"] == record["sequence"]
    assert (
        prepared_replay["predecessor_operation_uuid"]
        == record["predecessor_operation_uuid"]
    )
    assert prepared_replay["operation_sha256"] == record["operation_sha256"]


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


def test_content_only_update_preserves_workspace_origin_topic_mapping(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store, account_uuid, project_uuid
    )
    message_uuid = str(uuid.uuid4())
    outbound = _provider_record(account_uuid, project_uuid)
    outbound["origin"] = "workspace"
    outbound["operation"]["entity_uuid"] = message_uuid
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

    mapping = postgres_store.provider_mapping(account_uuid, "message", "601")
    assert "subject" not in mapping["metadata"]

    records = converter.event_records(
        postgres_store,
        account_uuid,
        "provider-message-update:601",
        {
            "id": 700,
            "type": "update_message",
            "message_id": 601,
            "message_ids": [601],
            "stream_id": 42,
            "content": "edited",
            "edit_timestamp": 1_700_000_010,
        },
    )

    assert [record["operation"]["kind"] for record in records] == ["message.update"]
    update = records[0]
    assert update["operation"]["entity_uuid"] == message_uuid
    assert update["operation"]["payload"]["topic_uuid"] == topic_uuid
    assert "subject" not in update["operation"]["extensions"]

    with postgres_store.session() as session:
        postgres_store._persist_committed_mapping(session, update, None, "1")

    persisted = postgres_store.provider_mapping(account_uuid, "message", "601")
    assert "subject" not in persisted["metadata"]
    assert (
        str(
            postgres_store.provider_mapping(account_uuid, "topic", "42:Topic")[
                "workspace_uuid"
            ]
        )
        == topic_uuid
    )
    assert (
        postgres_store.provider_mapping(account_uuid, "topic", "42:general chat")
        is None
    )


def test_single_message_topic_move_updates_committed_message_mapping(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, source_topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store, account_uuid, project_uuid
    )
    target_topic_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "42:TopicB",
        target_topic_uuid,
        {"stream_uuid": stream_uuid, "chat_key": "channel:42"},
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
            "topic_uuid": source_topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": "channel:42",
            "workspace_delivery_state": "committed",
        },
    )

    records = converter.event_records(
        postgres_store,
        account_uuid,
        "provider-message-move:601",
        {
            "id": 701,
            "type": "update_message",
            "message_id": 601,
            "message_ids": [601],
            "stream_id": 42,
            "orig_subject": "Topic",
            "subject": "TopicB",
            "propagate_mode": "change_one",
            "edit_timestamp": 1_700_000_011,
        },
    )

    assert [record["operation"]["kind"] for record in records] == [
        "topic.upsert",
        "message.update",
    ]
    move = records[1]
    assert move["operation"]["payload"]["topic_uuid"] == target_topic_uuid
    assert "payload" not in move["operation"]["payload"]
    with postgres_store.session() as session:
        postgres_store._persist_committed_mapping(session, move, None, "2")

    persisted = postgres_store.provider_mapping(account_uuid, "message", "601")
    assert persisted["provider_revision"] == "2"
    assert persisted["metadata"]["topic_uuid"] == target_topic_uuid
    assert persisted["metadata"]["subject"] == "TopicB"


def test_channel_move_updates_primary_and_alias_message_context(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    source_stream_uuid, source_topic_uuid, author_uuid = (
        _materialize_channel_projection(postgres_store, account_uuid, project_uuid)
    )
    _insert_channel_assignment(postgres_store, account_uuid, project_uuid, 43)
    destination_stream_uuid = str(uuid.uuid4())
    destination_topic_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "stream",
        "channel:43",
        destination_stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": project_uuid,
            "participants": [],
            "name": "Operations",
            "description": "",
            "private": True,
            "default_topic_uuid": None,
        },
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "43:Topic",
        destination_topic_uuid,
        {
            "stream_uuid": destination_stream_uuid,
            "chat_key": "channel:43",
        },
    )
    message_uuid = str(uuid.uuid4())
    outbound = _provider_record(account_uuid, project_uuid)
    outbound["origin"] = "workspace"
    outbound["operation"]["entity_uuid"] = message_uuid
    outbound["operation"]["payload"].update(
        {
            "stream_uuid": source_stream_uuid,
            "topic_uuid": source_topic_uuid,
            "author_uuid": author_uuid,
        }
    )
    outbound["operation_sha256"] = canonical.operation_digest(outbound)
    with postgres_store.session() as session:
        postgres_store._persist_committed_mapping(session, outbound, "601", None)

    records = converter.event_records(
        postgres_store,
        account_uuid,
        "provider-message-channel-move:601",
        {
            "id": 702,
            "type": "update_message",
            "message_id": 601,
            "message_ids": [601],
            "stream_id": 42,
            "new_stream_id": 43,
            "orig_subject": "Topic",
            "subject": "Topic",
            "propagate_mode": "change_one",
            "edit_timestamp": 1_700_000_012,
        },
    )
    move = next(
        record for record in records if record["operation"]["kind"] == "message.update"
    )
    with postgres_store.session() as session:
        postgres_store._persist_committed_mapping(session, move, None, "3")

    primary = postgres_store.provider_mapping(account_uuid, "message", "601")
    alias = postgres_store.workspace_mapping(account_uuid, "message", message_uuid)
    for mapping in (primary, alias):
        assert mapping["metadata"]["project_uuid"] == project_uuid
        assert mapping["metadata"]["stream_uuid"] == destination_stream_uuid
        assert mapping["metadata"]["topic_uuid"] == destination_topic_uuid
        assert mapping["metadata"]["chat_key"] == "channel:43"


def test_pending_channel_move_routes_followup_edit_before_result_commit(
    postgres_store,
):
    account_uuid, source_project_uuid = _insert_account_and_assignment(postgres_store)
    source_stream_uuid, source_topic_uuid, author_uuid = (
        _materialize_channel_projection(
            postgres_store, account_uuid, source_project_uuid
        )
    )
    destination_project_uuid = str(uuid.uuid4())
    _insert_channel_assignment(
        postgres_store,
        account_uuid,
        destination_project_uuid,
        43,
    )
    destination_stream_uuid = str(uuid.uuid4())
    destination_topic_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "stream",
        "channel:43",
        destination_stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": destination_project_uuid,
            "participants": [],
            "name": "Operations",
            "description": "",
            "private": True,
            "default_topic_uuid": None,
        },
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "43:Topic",
        destination_topic_uuid,
        {
            "stream_uuid": destination_stream_uuid,
            "chat_key": "channel:43",
        },
    )
    message_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "601",
        message_uuid,
        {
            "project_uuid": source_project_uuid,
            "stream_uuid": source_stream_uuid,
            "topic_uuid": source_topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": "channel:42",
            "causal_lane": f"chat:{account_uuid}:{source_stream_uuid}",
            "workspace_delivery_state": "committed",
        },
    )
    queue_id = "provider-message-channel-move:601"
    move_event = {
        "id": 703,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 42,
        "new_stream_id": 43,
        "orig_subject": "Topic",
        "subject": "Topic",
        "propagate_mode": "change_one",
        "edit_timestamp": 1_700_000_013,
    }
    assert postgres_store.record_provider_event(account_uuid, queue_id, move_event)
    move_records = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        move_event,
    )
    move_records = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        int(move_event["id"]),
        move_records,
    )
    postgres_store.enqueue_provider_event_records(
        move_records,
        0,
        account_uuid,
        queue_id,
        int(move_event["id"]),
    )
    move = next(
        record
        for record in move_records
        if record["operation"]["kind"] == "message.update"
    )

    pending = postgres_store.pending_provider_message_context(
        account_uuid, message_uuid
    )
    assert pending == {
        "project_uuid": destination_project_uuid,
        "stream_uuid": destination_stream_uuid,
        "topic_uuid": destination_topic_uuid,
        "chat_key": "channel:43",
        "causal_lane": f"chat:{account_uuid}:{source_stream_uuid}",
        "subject": "Topic",
    }

    edit_records = converter.event_records(
        postgres_store,
        account_uuid,
        "provider-message-update:601",
        {
            "id": 704,
            "type": "update_message",
            "message_id": 601,
            "stream_id": 43,
            "content": "edited after move",
            "edit_timestamp": 1_700_000_014,
        },
    )
    edit = next(
        record
        for record in edit_records
        if record["operation"]["kind"] == "message.update"
    )
    assert edit["project_uuid"] == destination_project_uuid
    assert edit["causal_lane"] == move["causal_lane"]
    assert edit["operation"]["provider"]["chat_id"] == "channel:43"
    assert edit["operation"]["payload"]["stream_uuid"] == destination_stream_uuid
    assert edit["operation"]["payload"]["topic_uuid"] == destination_topic_uuid

    postgres_store.accept_result(_committed_result(move))
    persisted = postgres_store.provider_mapping(account_uuid, "message", "601")
    assert persisted["metadata"]["project_uuid"] == destination_project_uuid
    assert persisted["metadata"]["stream_uuid"] == destination_stream_uuid
    assert persisted["metadata"]["topic_uuid"] == destination_topic_uuid
    assert persisted["metadata"]["chat_key"] == "channel:43"
    assert persisted["metadata"]["causal_lane"] == move["causal_lane"]
    assert (
        postgres_store.pending_provider_message_context(account_uuid, message_uuid)
        is None
    )


@pytest.mark.parametrize("kind", ["message.update", "message.delete"])
def test_rejected_message_mutation_is_not_pending_message_context(
    postgres_store,
    kind,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
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
            "causal_lane": f"chat:{account_uuid}:{stream_uuid}",
            "workspace_delivery_state": "committed",
        },
    )
    event_id = 705 if kind == "message.update" else 706
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state
            ) VALUES (%s, 'queue', %s, %s, '{}'::jsonb, 'delivering')
            """,
            (account_uuid, event_id, kind.removeprefix("message.")),
        )
    record = _provider_record(account_uuid, project_uuid, kind=kind)
    record["operation"]["entity_uuid"] = message_uuid
    record["operation"]["provider"].update(
        {"chat_id": "channel:42", "entity_id": "601"}
    )
    record["operation"]["payload"].update(
        {
            "stream_uuid": str(uuid.uuid4()),
            "topic_uuid": str(uuid.uuid4()),
            "author_uuid": author_uuid,
        }
    )
    record["operation_sha256"] = canonical.operation_digest(record)
    assert postgres_store.enqueue_workspace_delivery(record, 0, "queue", event_id)
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    assert postgres_store.reject_provider_event_submission(
        record["record_uuid"],
        "provider_api_http_422",
    )

    assert (
        postgres_store.pending_provider_message_context(account_uuid, message_uuid)
        is None
    )
    with postgres_store.session() as session:
        outcome = session.execute(
            """
            SELECT terminal_outcome FROM operation_idempotency
            WHERE operation_uuid = %s
            """,
            (record["operation_uuid"],),
        ).fetchone()["terminal_outcome"]
    assert outcome == "rejected"


def test_pending_message_context_uses_lane_sequence_across_provider_queues(
    postgres_store,
):
    account_uuid, source_project_uuid = _insert_account_and_assignment(postgres_store)
    source_stream_uuid, source_topic_uuid, author_uuid = (
        _materialize_channel_projection(
            postgres_store, account_uuid, source_project_uuid
        )
    )
    first_project_uuid = str(uuid.uuid4())
    first_stream_uuid, first_topic_uuid = _materialize_destination_channel(
        postgres_store,
        account_uuid,
        first_project_uuid,
        43,
    )
    second_project_uuid = str(uuid.uuid4())
    second_stream_uuid, second_topic_uuid = _materialize_destination_channel(
        postgres_store,
        account_uuid,
        second_project_uuid,
        44,
    )
    message_uuid = str(uuid.uuid4())
    causal_lane = f"chat:{account_uuid}:{source_stream_uuid}"
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "601",
        message_uuid,
        {
            "project_uuid": source_project_uuid,
            "stream_uuid": source_stream_uuid,
            "topic_uuid": source_topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": "channel:42",
            "causal_lane": causal_lane,
            "workspace_delivery_state": "committed",
        },
    )

    moves = (
        (
            "old-provider-queue",
            {
                "id": 900,
                "type": "update_message",
                "message_id": 601,
                "message_ids": [601],
                "stream_id": 42,
                "new_stream_id": 43,
                "orig_subject": "Topic",
                "subject": "Topic",
                "propagate_mode": "change_one",
                "edit_timestamp": 1_700_000_020,
            },
        ),
        (
            "replacement-provider-queue",
            {
                "id": 1,
                "type": "update_message",
                "message_id": 601,
                "message_ids": [601],
                "stream_id": 43,
                "new_stream_id": 44,
                "orig_subject": "Topic",
                "subject": "Topic",
                "propagate_mode": "change_one",
                "edit_timestamp": 1_700_000_021,
            },
        ),
    )
    for queue_id, event in moves:
        assert postgres_store.record_provider_event(account_uuid, queue_id, event)
        records = converter.event_records(
            postgres_store,
            account_uuid,
            queue_id,
            event,
        )
        prepared = postgres_store.prepare_provider_event_records(
            account_uuid,
            queue_id,
            int(event["id"]),
            records,
        )
        postgres_store.enqueue_provider_event_records(
            prepared,
            0,
            account_uuid,
            queue_id,
            int(event["id"]),
        )

    assert postgres_store.pending_provider_message_context(
        account_uuid, message_uuid
    ) == {
        "project_uuid": second_project_uuid,
        "stream_uuid": second_stream_uuid,
        "topic_uuid": second_topic_uuid,
        "chat_key": "channel:44",
        "causal_lane": causal_lane,
        "subject": "Topic",
    }
    edit = next(
        record
        for record in converter.event_records(
            postgres_store,
            account_uuid,
            "replacement-provider-queue",
            {
                "id": 2,
                "type": "update_message",
                "message_id": 601,
                "stream_id": 44,
                "content": "edited after queue replacement",
                "edit_timestamp": 1_700_000_022,
            },
        )
        if record["operation"]["kind"] == "message.update"
    )
    assert edit["project_uuid"] == second_project_uuid
    assert edit["operation"]["provider"]["chat_id"] == "channel:44"
    assert edit["operation"]["payload"]["stream_uuid"] == second_stream_uuid
    assert edit["operation"]["payload"]["topic_uuid"] == second_topic_uuid
    assert first_stream_uuid != second_stream_uuid
    assert first_topic_uuid != second_topic_uuid


def test_selected_move_then_unselected_move_deletes_pending_destination(
    postgres_store,
):
    account_uuid, source_project_uuid = _insert_account_and_assignment(postgres_store)
    source_stream_uuid, source_topic_uuid, author_uuid = (
        _materialize_channel_projection(
            postgres_store, account_uuid, source_project_uuid
        )
    )
    destination_project_uuid = str(uuid.uuid4())
    destination_stream_uuid, destination_topic_uuid = _materialize_destination_channel(
        postgres_store,
        account_uuid,
        destination_project_uuid,
        43,
    )
    _insert_channel_assignment(
        postgres_store,
        account_uuid,
        destination_project_uuid,
        44,
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET body = jsonb_set(body, '{selected}', 'false'::jsonb)
            WHERE resource_type = 'external_chat_assignment'
              AND body->'provider_chat'->>'provider_chat_key' = 'channel:44'
            """
        )
    message_uuid = str(uuid.uuid4())
    causal_lane = f"chat:{account_uuid}:{source_stream_uuid}"
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "601",
        message_uuid,
        {
            "project_uuid": source_project_uuid,
            "stream_uuid": source_stream_uuid,
            "topic_uuid": source_topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": "channel:42",
            "causal_lane": causal_lane,
            "workspace_delivery_state": "committed",
        },
    )
    move_event = {
        "id": 900,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 42,
        "new_stream_id": 43,
        "orig_subject": "Topic",
        "subject": "Topic",
        "propagate_mode": "change_one",
        "edit_timestamp": 1_700_000_023,
    }
    assert postgres_store.record_provider_event(
        account_uuid, "old-provider-queue", move_event
    )
    move_records = converter.event_records(
        postgres_store,
        account_uuid,
        "old-provider-queue",
        move_event,
    )
    move_records = postgres_store.prepare_provider_event_records(
        account_uuid,
        "old-provider-queue",
        int(move_event["id"]),
        move_records,
    )
    postgres_store.enqueue_provider_event_records(
        move_records,
        0,
        account_uuid,
        "old-provider-queue",
        int(move_event["id"]),
    )

    leave_event = {
        "id": 1,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 43,
        "new_stream_id": 44,
        "orig_subject": "Topic",
        "subject": "Topic",
        "propagate_mode": "change_one",
        "edit_timestamp": 1_700_000_024,
    }
    delete = converter.event_records(
        postgres_store,
        account_uuid,
        "replacement-provider-queue",
        leave_event,
    )
    assert [record["operation"]["kind"] for record in delete] == ["message.delete"]
    assert delete[0]["project_uuid"] == destination_project_uuid
    assert delete[0]["causal_lane"] == causal_lane
    assert delete[0]["operation"]["provider"]["chat_id"] == "channel:43"
    assert delete[0]["operation"]["payload"] == {
        "stream_uuid": destination_stream_uuid,
        "topic_uuid": destination_topic_uuid,
        "author_uuid": author_uuid,
    }


def test_channel_move_to_unselected_destination_retires_message_mapping(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store, account_uuid, project_uuid
    )
    _insert_channel_assignment(postgres_store, account_uuid, project_uuid, 43)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET body = jsonb_set(body, '{selected}', 'false'::jsonb)
            WHERE resource_type = 'external_chat_assignment'
              AND body->'provider_chat'->>'provider_chat_key' = 'channel:43'
            """
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
            "causal_lane": f"chat:{account_uuid}:{stream_uuid}",
            "workspace_delivery_state": "committed",
        },
    )
    queue_id = "provider-message-unselected-move:601"
    event = {
        "id": 705,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 42,
        "new_stream_id": 43,
        "orig_subject": "Topic",
        "subject": "Topic",
        "propagate_mode": "change_one",
        "edit_timestamp": 1_700_000_015,
    }
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    records = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        event,
    )
    assert [record["operation"]["kind"] for record in records] == ["message.delete"]
    records = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        int(event["id"]),
        records,
    )
    postgres_store.enqueue_provider_event_records(
        records,
        0,
        account_uuid,
        queue_id,
        int(event["id"]),
    )
    assert postgres_store.pending_provider_message_context(
        account_uuid, message_uuid
    ) == {
        "deleted": True,
        "causal_lane": f"chat:{account_uuid}:{stream_uuid}",
    }
    assert (
        converter.event_records(
            postgres_store,
            account_uuid,
            "provider-message-update:601",
            {
                "id": 706,
                "type": "update_message",
                "message_id": 601,
                "stream_id": 43,
                "content": "must not resurrect",
                "edit_timestamp": 1_700_000_016,
            },
        )
        == []
    )

    postgres_store.accept_result(_committed_result(records[0]))

    assert postgres_store.provider_mapping(account_uuid, "message", "601") is None
    with postgres_store.session() as session:
        deleted = session.execute(
            """
            SELECT deleted FROM provider_mappings
            WHERE account_uuid = %s AND entity_kind = 'message'
              AND workspace_uuid = %s
            """,
            (account_uuid, message_uuid),
        ).fetchone()
    assert deleted == {"deleted": True}


@pytest.mark.parametrize(
    "delete_committed_before_return_conversion",
    [False, True],
    ids=["pending-delete", "committed-delete"],
)
def test_unselected_delete_is_followed_by_full_selected_recreation(
    postgres_store,
    delete_committed_before_return_conversion,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store, account_uuid, project_uuid
    )
    _insert_channel_assignment(postgres_store, account_uuid, project_uuid, 43)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET body = jsonb_set(body, '{selected}', 'false'::jsonb)
            WHERE resource_type = 'external_chat_assignment'
              AND body->'provider_chat'->>'provider_chat_key' = 'channel:43'
            """
        )
    destination_stream_uuid, destination_topic_uuid = _materialize_destination_channel(
        postgres_store,
        account_uuid,
        project_uuid,
        44,
    )
    destination_stream = postgres_store.provider_mapping(
        account_uuid,
        "stream",
        "channel:44",
    )
    assert destination_stream is not None
    destination_stream_metadata = dict(destination_stream["metadata"])
    destination_stream_metadata["participants"] = sorted(
        [
            str(postgres_store.account_resource(account_uuid)["owner_user_uuid"]),
            author_uuid,
        ]
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "stream",
        "channel:44",
        destination_stream_uuid,
        destination_stream_metadata,
    )
    message_uuid = str(uuid.uuid4())
    causal_lane = f"chat:{account_uuid}:{stream_uuid}"
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
            "causal_lane": causal_lane,
            "workspace_delivery_state": "committed",
        },
    )
    delete_queue_id = "provider-message-unselected-move:601"
    delete_event = {
        "id": 705,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 42,
        "new_stream_id": 43,
        "orig_subject": "Topic",
        "subject": "Topic",
        "propagate_mode": "change_one",
        "edit_timestamp": 1_700_000_015,
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        delete_queue_id,
        delete_event,
    )
    delete_records = converter.event_records(
        postgres_store,
        account_uuid,
        delete_queue_id,
        delete_event,
    )
    delete_records = postgres_store.prepare_provider_event_records(
        account_uuid,
        delete_queue_id,
        int(delete_event["id"]),
        delete_records,
    )
    postgres_store.enqueue_provider_event_records(
        delete_records,
        0,
        account_uuid,
        delete_queue_id,
        int(delete_event["id"]),
    )

    return_queue_id = "provider-message-selected-return:601"
    return_event = {
        "id": 706,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 43,
        "new_stream_id": 44,
        "orig_subject": "Topic",
        "subject": "Topic",
        "propagate_mode": "change_one",
        "edit_timestamp": 1_700_000_016,
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        return_queue_id,
        return_event,
    )
    provider_snapshot = _provider_history_message(601)
    provider_snapshot.update(
        {
            "stream_id": 44,
            "display_recipient": "Channel 44",
            "timestamp": 1_700_000_016,
            "content": "restored with the current provider content",
        }
    )

    class Adapter:
        server_url = "https://zulip.example.invalid"

        def __init__(self):
            self.fetches = []

        def message_by_id(self, provider_message_id):
            self.fetches.append(provider_message_id)
            return dict(provider_snapshot)

    adapter = Adapter()
    bridge_service = object.__new__(service.BridgeService)
    bridge_service.store = postgres_store
    bridge_service.file_client = None
    row = {
        "account_uuid": account_uuid,
        "queue_id": return_queue_id,
        "event_id": int(return_event["id"]),
        "provider_message_context": None,
    }
    if delete_committed_before_return_conversion:
        postgres_store.accept_result(_committed_result(delete_records[0]))
        assert postgres_store.provider_mapping(account_uuid, "message", "601") is None
    converted_records = bridge_service._event_records_with_pending_delete_recreations(
        adapter,
        row,
        str(uuid.UUID(int=0)),
        return_event,
        "live",
    )
    with postgres_store.session() as session:
        cached = session.execute(
            """
            SELECT provider_message_context
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, return_queue_id, int(return_event["id"])),
        ).fetchone()
    assert cached is not None
    row["provider_message_context"] = cached["provider_message_context"]
    assert adapter.fetches == [601]
    assert [record["operation"]["kind"] for record in converted_records] == [
        "topic.upsert",
        "message.create",
    ]
    message_create = converted_records[-1]
    assert message_create["causal_lane"] == causal_lane
    assert message_create["operation"]["payload"]["payload"] == {
        "kind": "markdown",
        "content": "restored with the current provider content",
    }
    if not delete_committed_before_return_conversion:
        postgres_store.accept_result(_committed_result(delete_records[0]))
    assert postgres_store.provider_mapping(account_uuid, "message", "601") is None
    assert (
        bridge_service._event_records_with_pending_delete_recreations(
            adapter,
            row,
            str(uuid.UUID(int=0)),
            return_event,
            "live",
        )
        == converted_records
    )
    assert adapter.fetches == [601]
    recreation_records = postgres_store.prepare_provider_event_records(
        account_uuid,
        return_queue_id,
        int(return_event["id"]),
        converted_records,
    )
    message_create = next(
        record
        for record in recreation_records
        if record["operation"]["kind"] == "message.create"
    )
    postgres_store.enqueue_provider_event_records(
        recreation_records,
        0,
        account_uuid,
        return_queue_id,
        int(return_event["id"]),
    )
    mapping = postgres_store.provider_message_mapping(account_uuid, "601")
    assert mapping is not None
    assert mapping["metadata"]["workspace_delivery_state"] == "pending"
    with postgres_store.session() as session:
        stored_mapping = session.execute(
            """
            SELECT deleted FROM provider_mappings
            WHERE account_uuid = %s AND entity_kind = 'message'
              AND provider_id = '601'
            """,
            (account_uuid,),
        ).fetchone()
    assert stored_mapping == {"deleted": True}
    assert postgres_store.pending_provider_message_context(
        account_uuid,
        message_uuid,
    ) == {
        "project_uuid": project_uuid,
        "stream_uuid": destination_stream_uuid,
        "topic_uuid": destination_topic_uuid,
        "chat_key": "channel:44",
        "causal_lane": causal_lane,
    }
    follow_up = converter.event_records(
        postgres_store,
        account_uuid,
        "provider-message-update:601",
        {
            "id": 707,
            "type": "update_message",
            "message_id": 601,
            "stream_id": 44,
            "content": "newer edit",
            "edit_timestamp": 1_700_000_017,
        },
    )
    assert follow_up[-1]["operation"]["kind"] == "message.update"
    assert follow_up[-1]["project_uuid"] == project_uuid
    assert follow_up[-1]["operation"]["payload"]["stream_uuid"] == (
        destination_stream_uuid
    )
    with postgres_store.session() as session:
        stored_mapping = session.execute(
            """
            SELECT deleted FROM provider_mappings
            WHERE account_uuid = %s AND entity_kind = 'message'
              AND provider_id = '601'
            """,
            (account_uuid,),
        ).fetchone()
    assert stored_mapping == {"deleted": True}
    postgres_store.accept_result(_committed_result(message_create, "601"))
    mapping = postgres_store.provider_mapping(account_uuid, "message", "601")
    assert mapping is not None
    assert mapping["metadata"]["workspace_delivery_state"] == "committed"
    assert mapping["metadata"]["stream_uuid"] == destination_stream_uuid
    assert mapping["metadata"]["topic_uuid"] == destination_topic_uuid


@pytest.mark.parametrize(
    "delete_committed_before_grouped_return",
    [False, True],
    ids=["pending-delete", "committed-delete"],
)
def test_grouped_selected_return_skips_missing_tombstone_and_moves_remaining_message(
    postgres_store,
    delete_committed_before_grouped_return,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store, account_uuid, project_uuid
    )
    _insert_channel_assignment(postgres_store, account_uuid, project_uuid, 43)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET body = jsonb_set(body, '{selected}', 'false'::jsonb)
            WHERE resource_type = 'external_chat_assignment'
              AND body->'provider_chat'->>'provider_chat_key' = 'channel:43'
            """
        )
    destination_stream_uuid, destination_topic_uuid = _materialize_destination_channel(
        postgres_store,
        account_uuid,
        project_uuid,
        44,
    )
    destination_stream = postgres_store.provider_mapping(
        account_uuid,
        "stream",
        "channel:44",
    )
    assert destination_stream is not None
    destination_stream_metadata = dict(destination_stream["metadata"])
    destination_stream_metadata["participants"] = sorted(
        [
            str(postgres_store.account_resource(account_uuid)["owner_user_uuid"]),
            author_uuid,
        ]
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "stream",
        "channel:44",
        destination_stream_uuid,
        destination_stream_metadata,
    )
    causal_lane = f"chat:{account_uuid}:{stream_uuid}"
    message_uuids = {}
    for provider_message_id in (601, 602):
        message_uuid = str(uuid.uuid4())
        message_uuids[provider_message_id] = message_uuid
        postgres_store.remember_provider_mapping(
            account_uuid,
            "message",
            str(provider_message_id),
            message_uuid,
            {
                "project_uuid": project_uuid,
                "stream_uuid": stream_uuid,
                "topic_uuid": topic_uuid,
                "author_uuid": author_uuid,
                "chat_key": "channel:42",
                "causal_lane": causal_lane,
                "workspace_delivery_state": "committed",
            },
        )

    delete_queue_id = "provider-message-unselected-move:601"
    delete_event = {
        "id": 705,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 42,
        "new_stream_id": 43,
        "orig_subject": "Topic",
        "subject": "Topic",
        "propagate_mode": "change_one",
        "edit_timestamp": 1_700_000_015,
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        delete_queue_id,
        delete_event,
    )
    delete_records = converter.event_records(
        postgres_store,
        account_uuid,
        delete_queue_id,
        delete_event,
    )
    delete_records = postgres_store.prepare_provider_event_records(
        account_uuid,
        delete_queue_id,
        int(delete_event["id"]),
        delete_records,
    )
    postgres_store.enqueue_provider_event_records(
        delete_records,
        0,
        account_uuid,
        delete_queue_id,
        int(delete_event["id"]),
    )
    if delete_committed_before_grouped_return:
        postgres_store.accept_result(_committed_result(delete_records[0]))
        assert postgres_store.provider_mapping(account_uuid, "message", "601") is None
    else:
        assert postgres_store.pending_provider_message_context(
            account_uuid,
            message_uuids[601],
        ) == {
            "deleted": True,
            "causal_lane": causal_lane,
        }
    assert postgres_store.provider_mapping(account_uuid, "message", "602") is not None

    return_queue_id = "provider-message-grouped-selected-return"
    return_event = {
        "id": 706,
        "type": "update_message",
        "message_id": 602,
        "message_ids": [601, 602],
        "stream_id": 43,
        "new_stream_id": 44,
        "orig_subject": "Topic",
        "subject": "Topic",
        "propagate_mode": "change_all",
        "edit_timestamp": 1_700_000_016,
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        return_queue_id,
        return_event,
    )

    class Adapter:
        server_url = "https://zulip.example.invalid"

        def __init__(self):
            self.fetches = []

        def message_by_id(self, provider_message_id):
            self.fetches.append(provider_message_id)
            return None

    adapter = Adapter()
    bridge_service = object.__new__(service.BridgeService)
    bridge_service.store = postgres_store
    bridge_service.file_client = None
    row = {
        "account_uuid": account_uuid,
        "queue_id": return_queue_id,
        "event_id": int(return_event["id"]),
        "provider_message_context": None,
    }
    converted_records = bridge_service._event_records_with_pending_delete_recreations(
        adapter,
        row,
        str(uuid.UUID(int=0)),
        return_event,
        "live",
    )
    assert adapter.fetches == [601]
    message_updates = [
        record
        for record in converted_records
        if record["operation"]["kind"] == "message.update"
    ]
    assert len(message_updates) == 1
    assert message_updates[0]["operation"]["provider"]["entity_id"] == "602"
    assert message_updates[0]["operation"]["entity_uuid"] == message_uuids[602]
    assert message_updates[0]["operation"]["payload"]["stream_uuid"] == (
        destination_stream_uuid
    )
    assert message_updates[0]["operation"]["payload"]["topic_uuid"] == (
        destination_topic_uuid
    )
    assert not any(
        record["operation"]["kind"] == "message.create" for record in converted_records
    )

    with postgres_store.session() as session:
        cached = session.execute(
            """
            SELECT provider_message_context
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, return_queue_id, int(return_event["id"])),
        ).fetchone()
    assert cached == {
        "provider_message_context": {
            "context_kind": "pending_delete_recreations",
            "messages": {},
            "missing_message_ids": ["601"],
        }
    }
    row["provider_message_context"] = cached["provider_message_context"]
    assert (
        bridge_service._event_records_with_pending_delete_recreations(
            adapter,
            row,
            str(uuid.UUID(int=0)),
            return_event,
            "live",
        )
        == converted_records
    )
    assert adapter.fetches == [601]


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
        record for record in records if record["operation"]["kind"] == "reaction.upsert"
    )
    assert reaction["operation"]["entity_uuid"] == workspace_reaction_uuid
    assert reaction["operation"]["payload"]["emoji_name"] == "👍"
    assert postgres_store.enqueue_workspace_delivery(reaction, 0)
    postgres_store.accept_result(_committed_result(reaction))
    assert (
        str(
            postgres_store.provider_mapping(
                account_uuid,
                "reaction",
                "601:2:unicode_emoji:1f44d",
            )["workspace_uuid"]
        )
        == workspace_reaction_uuid
    )
    assert (
        postgres_store.provider_mapping(
            account_uuid,
            "reaction",
            "601:2:thumbs_up",
        )
        is None
    )


def test_reaction_mapping_convergence_keeps_displaced_mapping_until_cleanup(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    canonical_uuid = str(uuid.uuid4())
    stale_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "reaction",
        "601:2:unicode_emoji:270d",
        canonical_uuid,
        {
            "emoji_name": "✍",
            "provider_emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        },
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "reaction",
        "601:2:writing_hand",
        stale_uuid,
        {
            "emoji_name": "writing_hand",
            "emoji_code": "270D-FE0F",
            "reaction_type": "unicode_emoji",
        },
    )

    mapping, displaced = postgres_store.converge_reaction_mapping(
        account_uuid,
        "601",
        "2",
        "601:2:unicode_emoji:270d",
        "601:2:writing",
        str(uuid.uuid4()),
        {
            "emoji_name": "✍",
            "provider_emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        },
    )

    assert str(mapping["workspace_uuid"]) == canonical_uuid
    assert [str(row["workspace_uuid"]) for row in displaced] == [stale_uuid]
    stale = postgres_store.provider_mapping(
        account_uuid, "reaction", "601:2:writing_hand"
    )
    assert str(stale["workspace_uuid"]) == stale_uuid
    canonical = postgres_store.provider_mapping(
        account_uuid, "reaction", "601:2:unicode_emoji:270d"
    )
    assert canonical is not None
    assert str(canonical["workspace_uuid"]) == canonical_uuid


def test_reaction_mapping_convergence_replaces_same_uuid_deleted_canonical(
    postgres_store,
):
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
    with pytest.raises(RuntimeError, match="rollback test schema"):
        with postgres_store.transaction() as session:
            # Recreate the legacy state called out by the review. The current
            # schema's workspace-UUID primary key prevents new duplicates, so
            # the constraint change and fixture rows are rolled back together.
            session.execute(
                """
                ALTER TABLE provider_mappings
                DROP CONSTRAINT provider_mappings_pkey
                """
            )
            session.execute(
                """
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    metadata, deleted
                ) VALUES
                    (%s, 'reaction', %s, %s, %s, true),
                    (%s, 'reaction', %s, %s, %s, false)
                """,
                (
                    account_uuid,
                    reaction_uuid,
                    canonical_provider_id,
                    json.dumps(metadata),
                    account_uuid,
                    reaction_uuid,
                    legacy_provider_id,
                    json.dumps(metadata),
                ),
            )

            mapping, displaced = postgres_store.converge_reaction_mapping(
                account_uuid,
                "601",
                "2",
                canonical_provider_id,
                legacy_provider_id,
                reaction_uuid,
                metadata,
            )

            assert str(mapping["workspace_uuid"]) == reaction_uuid
            assert displaced == []
            rows = session.execute(
                """
                SELECT workspace_uuid, provider_id, deleted
                FROM provider_mappings
                WHERE account_uuid = %s AND entity_kind = 'reaction'
                """,
                (account_uuid,),
            ).fetchall()
            assert rows == [
                {
                    "workspace_uuid": uuid.UUID(reaction_uuid),
                    "provider_id": canonical_provider_id,
                    "deleted": False,
                }
            ]
            raise RuntimeError("rollback test schema")


def _reaction_mapping_recovery_case(postgres_store):
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
    canonical_uuid = str(uuid.uuid4())
    stale_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "reaction",
        "601:2:unicode_emoji:270d",
        canonical_uuid,
        {
            "emoji_name": "✍",
            "provider_emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        },
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "reaction",
        "601:2:writing_hand",
        stale_uuid,
        {
            "emoji_name": "writing_hand",
            "emoji_code": "270D-FE0F",
            "reaction_type": "unicode_emoji",
        },
    )
    queue_id = "reaction-recovery"
    event = {
        "id": 700,
        "type": "reaction",
        "op": "add",
        "message_id": 601,
        "user_id": 2,
        "emoji_name": "writing",
        "emoji_code": "270d",
        "reaction_type": "unicode_emoji",
    }
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    records = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        event,
    )
    return account_uuid, queue_id, canonical_uuid, stale_uuid, records


def test_reaction_mapping_cleanup_rolls_back_with_uncommitted_journal(
    postgres_store,
):
    account_uuid, queue_id, canonical_uuid, stale_uuid, records = (
        _reaction_mapping_recovery_case(postgres_store)
    )
    assert [record["operation"]["kind"] for record in records] == [
        "reaction.delete",
        "reaction.upsert",
    ]

    with pytest.raises(RuntimeError, match="simulated crash"):
        with postgres_store.transaction():
            postgres_store.prepare_provider_event_records(
                account_uuid, queue_id, 700, records
            )
            raise RuntimeError("simulated crash")

    assert (
        postgres_store.provider_mapping(account_uuid, "reaction", "601:2:writing_hand")
        is not None
    )
    canonical = postgres_store.provider_mapping(
        account_uuid, "reaction", "601:2:unicode_emoji:270d"
    )
    assert str(canonical["workspace_uuid"]) == canonical_uuid
    with postgres_store.session() as session:
        event = session.execute(
            """
            SELECT prepared_records FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = 700
            """,
            (account_uuid, queue_id),
        ).fetchone()
    assert event["prepared_records"] is None

    prepared = postgres_store.prepare_provider_event_records(
        account_uuid, queue_id, 700, records
    )
    assert [record["operation"] for record in prepared] == [
        record["operation"] for record in records
    ]
    assert prepared[-1]["transport"] == records[-1]["transport"]
    assert prepared[0]["transport"]["reaction_mapping_delete"] == {
        "workspace_uuid": stale_uuid,
        "provider_id": "601:2:writing_hand",
    }
    assert (
        postgres_store.provider_mapping(account_uuid, "reaction", "601:2:writing_hand")
        is not None
    )
    canonical = postgres_store.provider_mapping(
        account_uuid, "reaction", "601:2:unicode_emoji:270d"
    )
    assert str(canonical["workspace_uuid"]) == canonical_uuid
    assert any(
        record["operation"]["entity_uuid"] == stale_uuid
        and record["operation"]["kind"] == "reaction.delete"
        for record in prepared
    )

    for record in prepared:
        assert postgres_store.enqueue_workspace_delivery(
            record,
            0,
            queue_id,
            700,
        )
    postgres_store.accept_result(_committed_result(prepared[0]))
    assert (
        postgres_store.provider_mapping(account_uuid, "reaction", "601:2:writing_hand")
        is None
    )
    postgres_store.accept_result(_committed_result(prepared[-1]))
    canonical = postgres_store.provider_mapping(
        account_uuid, "reaction", "601:2:unicode_emoji:270d"
    )
    assert str(canonical["workspace_uuid"]) == canonical_uuid


def test_rejected_backfill_reaction_cleanup_is_requeued_on_replay(
    postgres_store,
):
    account_uuid, _queue_id, canonical_uuid, stale_uuid, _records = (
        _reaction_mapping_recovery_case(postgres_store)
    )
    _enable_zulip_provider(postgres_store)
    message = _provider_history_message(601)
    message["reactions"] = [
        {
            "user_id": 2,
            "emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        }
    ]
    event = {"id": 601, "type": "message", "message": message}
    records = converter.event_records(
        postgres_store,
        account_uuid,
        "history",
        event,
        "backfill",
    )
    cleanup = next(
        record
        for record in records
        if record.get("transport", {}).get("reaction_mapping_delete")
    )
    assert postgres_store.enqueue_workspace_delivery(cleanup, 2)
    assert postgres_store.mark_workspace_delivery_submitting(cleanup["record_uuid"])
    assert postgres_store.reject_provider_event_submission(
        cleanup["record_uuid"],
        "provider_api_http_422",
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = 2,
                body = jsonb_set(body, '{generation}', '2'::jsonb)
            WHERE resource_type = 'external_chat_assignment'
              AND body->>'external_account_uuid' = %s
            """,
            (account_uuid,),
        )

    stale = postgres_store.provider_mapping(
        account_uuid, "reaction", "601:2:writing_hand"
    )
    canonical = postgres_store.provider_mapping(
        account_uuid, "reaction", "601:2:unicode_emoji:270d"
    )
    assert str(stale["workspace_uuid"]) == stale_uuid
    assert str(canonical["workspace_uuid"]) == canonical_uuid
    replay_records = converter.event_records(
        postgres_store,
        account_uuid,
        "history-replay",
        event,
        "backfill",
    )
    replay_cleanup = next(
        record
        for record in replay_records
        if record.get("transport", {}).get("reaction_mapping_delete")
    )
    assert replay_cleanup["operation_uuid"] == cleanup["operation_uuid"]
    assert replay_cleanup["operation_sha256"] == cleanup["operation_sha256"]
    assert postgres_store.enqueue_workspace_delivery(replay_cleanup, 2)
    assert postgres_store.pending_workspace_deliveries(2, 2) == [cleanup]
    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT assignment_generation, submission_state,
                   submission_error_code
            FROM workspace_delivery_outbox
            WHERE operation_uuid = %s
            """,
            (cleanup["operation_uuid"],),
        ).fetchone()
    assert delivery == {
        "assignment_generation": 2,
        "submission_state": "pending",
        "submission_error_code": None,
    }
    assert postgres_store.mark_workspace_delivery_submitting(cleanup["record_uuid"])
    postgres_store.accept_result(_committed_result(cleanup))
    assert (
        postgres_store.provider_mapping(account_uuid, "reaction", "601:2:writing_hand")
        is None
    )


def test_remove_reaction_tombstones_existing_mapping_only_after_acceptance(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store, account_uuid, project_uuid
    )
    message_uuid = str(uuid.uuid4())
    reaction_uuid = str(uuid.uuid4())
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
        },
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "reaction",
        "601:2:writing",
        reaction_uuid,
        {
            "emoji_name": "writing",
            "emoji_code": "270d",
            "reaction_type": "unicode_emoji",
        },
    )
    queue_id = "reaction-remove"
    event = {
        "id": 701,
        "type": "reaction",
        "op": "remove",
        "message_id": 601,
        "user_id": 2,
        "emoji_name": "writing",
        "emoji_code": "270d",
        "reaction_type": "unicode_emoji",
    }
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    records = converter.event_records(postgres_store, account_uuid, queue_id, event)
    assert [record["operation"]["kind"] for record in records] == ["reaction.delete"]
    assert records[0]["transport"]["reaction_mapping"]["create_if_missing"] is False
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid, queue_id, 701, records
    )
    assert (
        postgres_store.provider_mapping(account_uuid, "reaction", "601:2:writing")
        is not None
    )
    assert postgres_store.enqueue_workspace_delivery(prepared[0], 0, queue_id, 701)
    postgres_store.accept_result(_committed_result(prepared[0]))

    assert (
        postgres_store.provider_mapping(account_uuid, "reaction", "601:2:writing")
        is None
    )
    assert (
        postgres_store.provider_mapping(
            account_uuid, "reaction", "601:2:unicode_emoji:270d"
        )
        is None
    )
    with postgres_store.session() as session:
        tombstone = session.execute(
            """
            SELECT workspace_uuid, deleted FROM provider_mappings
            WHERE account_uuid = %s AND entity_kind = 'reaction'
              AND provider_id = '601:2:unicode_emoji:270d'
            """,
            (account_uuid,),
        ).fetchone()
    assert str(tombstone["workspace_uuid"]) == reaction_uuid
    assert tombstone["deleted"] is True


def test_existing_prepared_reaction_event_does_not_apply_new_mapping_plan(
    postgres_store,
):
    account_uuid, queue_id, canonical_uuid, _stale_uuid, records = (
        _reaction_mapping_recovery_case(postgres_store)
    )
    old_prepared = json.loads(json.dumps([records[-1]]))
    old_prepared[0].pop("transport", None)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET prepared_records = %s
            WHERE account_uuid = %s AND queue_id = %s AND event_id = 700
            """,
            (json.dumps(old_prepared), account_uuid, queue_id),
        )

    replay = postgres_store.prepare_provider_event_records(
        account_uuid, queue_id, 700, records
    )

    assert replay == old_prepared
    assert (
        postgres_store.provider_mapping(account_uuid, "reaction", "601:2:writing_hand")
        is not None
    )
    canonical = postgres_store.provider_mapping(
        account_uuid, "reaction", "601:2:unicode_emoji:270d"
    )
    assert str(canonical["workspace_uuid"]) == canonical_uuid


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
    message["content"] = "#**Engineering>✔ resolved topic** @**Mentioned user|99**"
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
    assert accepted[-1]["operation"]["kind"] == "message.create"
    for record in accepted[:-1]:
        assert postgres_store.enqueue_workspace_delivery(record, 0, queue_id, event_id)
    accepted_message = next(
        record for record in accepted if record["operation"]["kind"] == "message.create"
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
    assert stored_kinds == [record["operation"]["kind"] for record in accepted]

    # A sent prefix can survive an assignment replacement while an unsent tail
    # is discarded as stale. Keep the prepared snapshot through delivering so
    # the event can requeue the missing tail under the new generation.
    postgres_store.mark_provider_event_delivering(account_uuid, queue_id, event_id)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE workspace_delivery_outbox
            SET submission_state = 'sent', sent_at = now()
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
              AND record->'operation'->>'kind' != 'message.create'
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
        "read_states": 0,
    }
    postgres_store.mark_provider_event_delivering(account_uuid, queue_id, event_id)
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
        assert postgres_store.enqueue_workspace_delivery(record, 0, queue_id, event_id)
    postgres_store.mark_provider_event_delivering(account_uuid, queue_id, event_id)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE workspace_delivery_outbox
            SET submission_state = 'sent', sent_at = now()
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
              AND record->'operation'->>'kind' != 'message.create'
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
    expected_state = "pending" if assignment_change == "project" else "processed"
    assert provider_event == {
        "processing_state": expected_state,
        "processing_reason": "assignment_changed",
        "prepared_records": None,
    }
    assert deliveries == {
        "total": len(prepared) - 1,
        "unsent": 0,
        "messages": 0,
        "read_states": 0,
    }
    pending = postgres_store.pending_provider_events()
    if assignment_change == "project":
        assert [row["event_id"] for row in pending] == [event_id]
    else:
        assert pending == []


def test_terminal_delivery_state_is_pruned_by_priority_and_age(postgres_store):
    account_uuid = str(uuid.uuid4())
    history_record_uuid = str(uuid.uuid4())
    live_record_uuid = str(uuid.uuid4())
    report_resource_uuid = str(uuid.uuid4())
    old_report_uuid = str(uuid.uuid4())
    latest_report_uuid = str(uuid.uuid4())
    singleton_report_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state, created_at
            ) VALUES
                (%s, 'history', 1, 'message', '{}'::jsonb, 'processed',
                 now() - interval '20 minutes'),
                (%s, 'live', 2, 'message', '{}'::jsonb, 'processed',
                 now() - interval '20 minutes'),
                (%s, 'invalid', 3, 'message', '{}'::jsonb, 'invalid',
                 now() - interval '20 minutes'),
                (%s, 'pending', 4, 'message', '{}'::jsonb, 'pending',
                 now() - interval '20 minutes')
            """,
            (account_uuid, account_uuid, account_uuid, account_uuid),
        )
        session.execute(
            """
            INSERT INTO workspace_delivery_outbox (
                record_uuid, operation_uuid, account_uuid, provider_queue_id,
                provider_event_id, submission_state,
                priority, record, sent_at, created_at
            ) VALUES
                (%s, %s, %s, 'history', 1, 'sent', 2, '{}'::jsonb,
                 now() - interval '2 minutes', now() - interval '20 minutes'),
                (%s, %s, %s, 'live', 2, 'sent', 0, '{}'::jsonb,
                 now() - interval '2 minutes', now() - interval '20 minutes')
            """,
            (
                history_record_uuid,
                str(uuid.uuid4()),
                account_uuid,
                live_record_uuid,
                str(uuid.uuid4()),
                account_uuid,
            ),
        )
        session.execute(
            """
            INSERT INTO observed_report_outbox (
                report_uuid, body, result_status, completed_at, created_at
            ) VALUES
                (%s, jsonb_build_object(
                    'resource_type', 'external_account',
                    'resource_uuid', %s::text,
                    'observed_generation', 1,
                    'observed_at', '2026-08-27T06:00:00Z'
                ), 'applied', now() - interval '20 minutes',
                    now() - interval '20 minutes'),
                (%s, jsonb_build_object(
                    'resource_type', 'external_account',
                    'resource_uuid', %s::text,
                    'observed_generation', 2,
                    'observed_at', '2026-08-27T05:00:00Z'
                ), 'applied', now() - interval '20 minutes',
                    now() - interval '20 minutes'),
                (%s, jsonb_build_object(
                    'resource_type', 'external_account',
                    'resource_uuid', %s::text,
                    'observed_generation', 1,
                    'observed_at', '2026-08-27T06:00:00Z'
                ), 'applied', now() - interval '20 minutes',
                    now() - interval '20 minutes')
            """,
            (
                old_report_uuid,
                report_resource_uuid,
                latest_report_uuid,
                report_resource_uuid,
                singleton_report_uuid,
                str(uuid.uuid4()),
            ),
        )

    assert postgres_store.prune_terminal_delivery_state() == (1, 2)

    with postgres_store.session() as session:
        deliveries = session.execute(
            "SELECT provider_queue_id FROM workspace_delivery_outbox"
        ).fetchall()
        events = session.execute(
            "SELECT queue_id FROM zulip_provider_events ORDER BY queue_id"
        ).fetchall()
        reports = session.execute(
            """
            SELECT report_uuid::text AS report_uuid
            FROM observed_report_outbox
            WHERE report_uuid = ANY(%s::uuid[])
            ORDER BY report_uuid
            """,
            ([old_report_uuid, latest_report_uuid, singleton_report_uuid],),
        ).fetchall()
    assert deliveries == [{"provider_queue_id": "live"}]
    assert events == [{"queue_id": "live"}, {"queue_id": "pending"}]
    assert [row["report_uuid"] for row in reports] == sorted(
        [latest_report_uuid, singleton_report_uuid]
    )

    # The next bounded pass reaches the end and rewinds without a table scan.
    assert postgres_store.prune_terminal_delivery_state() == (0, 0)
    with postgres_store.session() as session:
        cursor = session.execute(
            """
            SELECT last_completed_at, last_report_uuid
            FROM observed_report_prune_state
            WHERE singleton
            """
        ).fetchone()
    assert cursor == {"last_completed_at": None, "last_report_uuid": None}


def test_acknowledged_read_operations_are_pruned_without_losing_idempotency(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    read_record_uuid = str(uuid.uuid4())
    read_operation_uuid = str(uuid.uuid4())
    message_record_uuid = str(uuid.uuid4())
    message_operation_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO operation_idempotency (
                operation_uuid, operation_sha256, terminal_outcome
            ) VALUES (%s, %s, 'committed')
            """,
            (read_operation_uuid, "a" * 64),
        )
        session.execute(
            """
            INSERT INTO bridge_operations (
                record_uuid, operation_uuid, attempt, operation_sha256,
                account_uuid, project_uuid, origin, causal_lane,
                lane_sequence, priority, state, record, result_record,
                result_sent_at, created_at, updated_at
            ) VALUES
                (%s, %s, 1, %s, %s, %s, 'workspace', 'read:old', 1, 0,
                 'committed',
                 '{"operation":{"kind":"read_state.set"}}'::jsonb,
                 jsonb_build_object('record_uuid', %s::text), now(),
                 now() - interval '20 minutes', now() - interval '20 minutes'),
                (%s, %s, 1, %s, %s, %s, 'workspace', 'message:old', 1, 0,
                 'committed',
                 '{"operation":{"kind":"message.create"}}'::jsonb,
                 jsonb_build_object('record_uuid', %s::text), now(),
                 now() - interval '20 minutes', now() - interval '20 minutes')
            """,
            (
                read_record_uuid,
                read_operation_uuid,
                "a" * 64,
                account_uuid,
                project_uuid,
                str(uuid.uuid4()),
                message_record_uuid,
                message_operation_uuid,
                "b" * 64,
                account_uuid,
                project_uuid,
                str(uuid.uuid4()),
            ),
        )

    assert postgres_store.prune_terminal_delivery_state() == (0, 0)

    with postgres_store.session() as session:
        operations = session.execute(
            """
            SELECT record_uuid::text AS record_uuid
            FROM bridge_operations ORDER BY record_uuid
            """
        ).fetchall()
        idempotency = session.execute(
            """
            SELECT operation_uuid::text AS operation_uuid, terminal_outcome
            FROM operation_idempotency
            WHERE operation_uuid = ANY(%s::uuid[])
            ORDER BY operation_uuid
            """,
            ([read_operation_uuid, read_record_uuid],),
        ).fetchall()
    assert operations == [{"record_uuid": message_record_uuid}]
    assert idempotency == sorted(
        [
            {
                "operation_uuid": read_operation_uuid,
                "terminal_outcome": "committed",
            },
            {
                "operation_uuid": read_record_uuid,
                "terminal_outcome": "committed",
            },
        ],
        key=lambda row: row["operation_uuid"],
    )

    replay = _provider_record(
        account_uuid,
        project_uuid,
        kind="read_state.set",
    )
    replay["record_uuid"] = read_record_uuid
    replay["operation_uuid"] = read_record_uuid
    replay["sequence"] = 1
    replay["operation_sha256"] = canonical.operation_digest(replay)
    assert postgres_store.enqueue(replay, 0) is False


def test_pruned_terminal_operation_cannot_be_enqueued_again(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(
        account_uuid, project_uuid, kind="read_state.set"
    )
    record["sequence"] = 1
    record["operation_sha256"] = canonical.operation_digest(record)
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO operation_idempotency (
                operation_uuid, operation_sha256, terminal_outcome
            ) VALUES (%s, %s, 'committed')
            """,
            (record["operation_uuid"], record["operation_sha256"]),
        )

    replay = json.loads(json.dumps(record))
    replay["operation"]["occurred_at"] = "2026-07-18T12:00:01Z"
    replay["operation_sha256"] = canonical.operation_digest(replay)
    assert replay["operation_sha256"] != record["operation_sha256"]
    assert postgres_store.enqueue(replay, 0) is False
    with postgres_store.session() as session:
        persisted = session.execute(
            """
            SELECT count(*) AS count FROM bridge_operations
            WHERE operation_uuid = %s
            """,
            (record["operation_uuid"],),
        ).fetchone()
    assert persisted["count"] == 0


def test_nonterminal_read_operation_still_rejects_changed_digest(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(
        account_uuid, project_uuid, kind="read_state.set"
    )
    record["sequence"] = 1
    record["operation_sha256"] = canonical.operation_digest(record)
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO operation_idempotency (
                operation_uuid, operation_sha256
            ) VALUES (%s, %s)
            """,
            (record["operation_uuid"], record["operation_sha256"]),
        )

    replay = json.loads(json.dumps(record))
    replay["operation"]["occurred_at"] = "2026-07-18T12:00:01Z"
    replay["operation_sha256"] = canonical.operation_digest(replay)
    with pytest.raises(
        ValueError, match="Operation UUID reused with a different digest"
    ):
        postgres_store.enqueue(replay, 0)


def test_committed_message_fast_path_keeps_flagged_snapshots_pending(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    message_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "42",
        message_uuid,
        {"workspace_delivery_state": "committed"},
    )
    events = [
        {
            "id": 1,
            "type": "message",
            "flags": ["read"],
            "message": {"id": 42},
        },
        {
            "id": 2,
            "type": "message",
            "message": {"id": 42, "flags": []},
        },
        {"id": 3, "type": "message", "message": {"id": 42}},
    ]
    for event in events:
        assert postgres_store.record_provider_event(account_uuid, "queue", event)

    assert not postgres_store.finalize_redundant_provider_message_event(
        account_uuid, "queue", 1
    )
    assert postgres_store.finalize_redundant_provider_message_event(
        account_uuid, "queue", 3
    )
    assert postgres_store.finalize_redundant_provider_message_events() == 0

    with postgres_store.session() as session:
        states = session.execute(
            """
            SELECT event_id, processing_state
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue'
            ORDER BY event_id
            """,
            (account_uuid,),
        ).fetchall()
    assert states == [
        {"event_id": 1, "processing_state": "pending"},
        {"event_id": 2, "processing_state": "pending"},
        {"event_id": 3, "processing_state": "processed"},
    ]


def test_pending_provider_probe_waits_for_each_accounts_head(postgres_store):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid, "queue", {"id": 1, "type": "message"}
    )
    assert postgres_store.record_provider_event(
        account_uuid, "queue", {"id": 2, "type": "message"}
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 1 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = CASE event_id
                    WHEN 1 THEN now() - interval '1 minute'
                    ELSE now()
                END
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )

    assert not postgres_store.has_pending_provider_events()

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = now()
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        )

    assert postgres_store.has_pending_provider_events()


def test_provider_journal_backoff_blocks_only_its_causal_lane(postgres_store):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "user_topic",
            "stream_id": 12,
            "topic_name": "Unavailable",
            "visibility_policy": 1,
            "last_updated": 1_800_000_001,
        },
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 2,
            "type": "user_topic",
            "stream_id": 12,
            "topic_name": "Still blocked",
            "visibility_policy": 1,
            "last_updated": 1_800_000_002,
        },
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 3,
            "type": "message",
            "message": {
                "id": 603,
                "type": "stream",
                "stream_id": 42,
            },
        },
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 1 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = now() - (4 - event_id) * interval '1 minute'
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )

    assert postgres_store.has_pending_provider_events()
    pending = postgres_store.pending_provider_events()
    assert [event["event_id"] for event in pending] == [3]
    assert pending[0]["causal_lane"] == "channel:42"

    postgres_store.mark_provider_event_processed(
        account_uuid, "queue", 3, supported=True
    )

    assert not postgres_store.has_pending_provider_events()
    assert postgres_store.pending_provider_events() == []


def test_provider_journal_lane_expansion_stops_at_deferred_predecessor(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    for event_id in range(1, 4):
        assert postgres_store.record_provider_event(
            account_uuid,
            "queue",
            {
                "id": event_id,
                "type": "user_topic",
                "stream_id": 12,
                "topic_name": f"Ordered {event_id}",
                "visibility_policy": 1,
                "last_updated": 1_800_000_000 + event_id,
            },
        )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 2 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = now() - (4 - event_id) * interval '1 minute'
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )

    expanded = postgres_store.pending_provider_event_lane_batch(
        account_uuid,
        "queue",
        1,
        "channel:12",
        10,
    )

    assert [event["event_id"] for event in expanded] == [1]


def test_provider_journal_missing_lane_metadata_does_not_hide_event(postgres_store):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "message",
            "message": {
                "id": 603,
                "type": "stream",
                "stream_id": 42,
            },
        },
    )
    with postgres_store.session() as session:
        session.execute(
            """
            DELETE FROM scheduler_provider_event_lanes
            WHERE account_uuid = %s AND causal_lane = 'channel:42'
            """,
            (account_uuid,),
        )

    assert postgres_store.has_pending_provider_events()
    pending = postgres_store.pending_provider_events()
    assert [event["event_id"] for event in pending] == [1]

    with postgres_store.session() as session:
        lane = session.execute(
            """
            SELECT last_provider_event_dispatched_at IS NOT NULL AS dispatched
            FROM scheduler_provider_event_lanes
            WHERE account_uuid = %s AND causal_lane = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
    assert lane == {"dispatched": True}


def test_provider_journal_global_event_remains_an_account_barrier(postgres_store):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {"id": 1, "type": "future_account_event"},
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 2,
            "type": "message",
            "message": {
                "id": 602,
                "type": "stream",
                "stream_id": 42,
            },
        },
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 1 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = now() - (3 - event_id) * interval '1 minute'
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )

    assert not postgres_store.has_pending_provider_events()
    assert postgres_store.pending_provider_events() == []


def test_provider_journal_multistream_subscription_is_a_global_barrier(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "subscription",
            "op": "peer_add",
            "stream_ids": [42, 43],
        },
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 2,
            "type": "message",
            "message": {
                "id": 602,
                "type": "stream",
                "stream_id": 42,
            },
        },
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 1 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = now() - (3 - event_id) * interval '1 minute'
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )
        subscription = session.execute(
            """
            SELECT causal_lane
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        ).fetchone()

    assert subscription["causal_lane"] is None
    assert not postgres_store.has_pending_provider_events()
    assert postgres_store.pending_provider_events() == []


def test_provider_journal_global_event_waits_for_every_earlier_lane(postgres_store):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "message",
            "message": {
                "id": 601,
                "type": "stream",
                "stream_id": 42,
            },
        },
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {"id": 2, "type": "future_account_event"},
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 1 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = now() - (3 - event_id) * interval '1 minute'
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )

    assert not postgres_store.has_pending_provider_events()
    assert postgres_store.pending_provider_events() == []


def test_provider_journal_cross_stream_move_remains_a_global_barrier(postgres_store):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "message",
            "message": {
                "id": 601,
                "type": "stream",
                "stream_id": 42,
            },
        },
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 2,
            "type": "update_message",
            "message_id": 601,
            "stream_id": 42,
            "new_stream_id": 43,
        },
    )
    with postgres_store.session() as session:
        event = session.execute(
            """
            SELECT causal_lane
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()

    assert event["causal_lane"] is None


def test_pending_provider_events_include_retry_count(postgres_store):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {"id": 1, "type": "reaction"},
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET retry_count = 37,
                processing_reason = 'provider_unavailable'
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        )

    pending = postgres_store.pending_provider_events()
    assert len(pending) == 1
    assert pending[0]["retry_count"] == 37
    assert pending[0]["processing_reason"] == "provider_unavailable"


def test_pending_provider_events_rotate_beyond_one_quantum(postgres_store):
    account_uuids = [str(uuid.uuid4()) for _ in range(25)]
    for account_uuid in account_uuids:
        assert postgres_store.record_provider_event(
            account_uuid, "queue", {"id": 1, "type": "realm_user"}
        )
        assert postgres_store.record_provider_event(
            account_uuid, "queue", {"id": 2, "type": "realm_user"}
        )

    first = postgres_store.pending_provider_events(limit=20)
    second = postgres_store.pending_provider_events(limit=20)
    first_accounts = {str(row["account_uuid"]) for row in first}
    second_accounts = {str(row["account_uuid"]) for row in second}

    assert len(first) == 20
    assert len(second) == 20
    assert all(row["event_id"] == 1 for row in first + second)
    assert len(first_accounts) == 20
    assert len(second_accounts) == 20
    assert set(account_uuids) == first_accounts | second_accounts


def test_full_provider_journal_selector_disables_jit_on_fresh_statistics(
    postgres_store,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                causal_lane, created_at
            )
            SELECT
                %s,
                'selector-load',
                sample,
                'update_message',
                jsonb_build_object(
                    'id', sample,
                    'type', 'update_message',
                    'message_id', 100000 + sample
                ),
                'channel:' || (sample %% 180)::text,
                now() + sample * interval '1 microsecond'
            FROM generate_series(1, 22500) AS sample
            """,
            (account_uuid,),
        )
        session.execute(
            """
            INSERT INTO scheduler_provider_event_lanes (
                account_uuid, causal_lane
            )
            SELECT DISTINCT account_uuid, causal_lane
            FROM zulip_provider_events
            WHERE account_uuid = %s
            ON CONFLICT (account_uuid, causal_lane) DO NOTHING
            """,
            (account_uuid,),
        )

    with postgres_store.transaction() as session:
        session.execute("SET LOCAL jit = on")
        session.execute("SET LOCAL jit_above_cost = 0")
        selected = postgres_store.pending_provider_events(limit=20)
        jit = session.execute("SHOW jit").fetchone()
    assert len(selected) == 4
    assert {str(row["account_uuid"]) for row in selected} == {account_uuid}
    assert len({str(row["causal_lane"]) for row in selected}) == 4
    assert jit == {"jit": "off"}

    with postgres_store.transaction() as session:
        session.execute("SET LOCAL jit = on")
        session.execute("SET LOCAL jit_above_cost = 0")
        assert postgres_store.has_pending_provider_events()
        jit = session.execute("SHOW jit").fetchone()
    assert jit == {"jit": "off"}


def test_provider_assignment_context_survives_intervening_retry_reason(postgres_store):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {"id": 1, "type": "reaction", "message_id": 601},
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {"id": 2, "type": "reaction", "message_id": 601},
    )
    with postgres_store.session() as session:
        initial_events = session.execute(
            """
            SELECT event_id, causal_lane
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue'
            ORDER BY event_id
            """,
            (account_uuid,),
        ).fetchall()
    assert [(row["event_id"], row["causal_lane"]) for row in initial_events] == [
        (1, "message:601"),
        (2, "message:601"),
    ]
    message_context = {
        "id": 601,
        "type": "stream",
        "stream_id": 42,
        "display_recipient": "Engineering",
        "timestamp": 1_800_000_000,
    }
    assert (
        postgres_store.cache_provider_event_message_context(
            account_uuid,
            "queue",
            1,
            message_context,
        )
        == message_context
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {"id": 3, "type": "reaction", "message_id": 601},
    )

    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        1,
        "provider_chat_assignment_pending",
    )
    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        1,
        "rate_limit_hit",
    )
    assert postgres_store.mark_provider_event_catalog_reported(
        account_uuid,
        "queue",
        1,
    )

    with postgres_store.session() as session:
        causal_lanes = session.execute(
            """
            SELECT event_id, causal_lane
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue'
            ORDER BY event_id
            """,
            (account_uuid,),
        ).fetchall()
        event = session.execute(
            """
            SELECT retry_count, processing_reason, causal_lane,
                   assignment_pending_since, assignment_catalog_reported_at,
                   provider_message_context
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        ).fetchone()
    assert [(row["event_id"], row["causal_lane"]) for row in causal_lanes] == [
        (1, "channel:42"),
        (2, "channel:42"),
        (3, "channel:42"),
    ]
    assert event["retry_count"] == 2
    assert event["processing_reason"] == "rate_limit_hit"
    assert event["causal_lane"] == "channel:42"
    assert event["assignment_pending_since"] is not None
    assert event["assignment_catalog_reported_at"] is not None
    assert event["provider_message_context"] == message_context

    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        1,
        "provider_event_processing_failed",
    )
    with postgres_store.session() as session:
        first_unexpected_failure = session.execute(
            """
            SELECT retry_count, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        ).fetchone()
    assert first_unexpected_failure == {
        "retry_count": 1,
        "processing_reason": "provider_event_processing_failed",
    }

    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        1,
        "provider_event_processing_failed",
    )
    with postgres_store.session() as session:
        consecutive_failure = session.execute(
            """
            SELECT retry_count, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        ).fetchone()
    assert consecutive_failure == {
        "retry_count": 2,
        "processing_reason": "provider_event_processing_failed",
    }


@pytest.mark.parametrize("resolution_source", ["message_context", "mapping"])
def test_resolved_provisional_lane_is_reselected_behind_older_head(
    postgres_store,
    resolution_source,
):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "message",
            "message": {
                "id": 600,
                "type": "stream",
                "stream_id": 42,
            },
        },
    )
    reaction = {
        "id": 2,
        "type": "reaction",
        "message_id": 601,
        "user_id": 3,
        "emoji_name": "thumbs_up",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
        "op": "add",
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        reaction,
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 1 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = now() - (3 - event_id) * interval '1 minute'
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]
    assert selected[0]["causal_lane"] == "message:601"

    if resolution_source == "message_context":
        postgres_store.cache_provider_event_message_context(
            account_uuid,
            "queue",
            2,
            {
                "id": 601,
                "type": "stream",
                "stream_id": 42,
                "display_recipient": "Engineering",
                "timestamp": 1_800_000_000,
            },
        )
    else:
        postgres_store.remember_provider_mapping(
            account_uuid,
            "message",
            "601",
            str(uuid.uuid4()),
            {"chat_key": "channel:42"},
        )
    assert (
        postgres_store.refresh_provider_event_causal_lane(
            account_uuid,
            "queue",
            2,
            reaction,
        )
        == "channel:42"
    )

    assert not postgres_store.has_pending_provider_events()
    assert postgres_store.pending_provider_events() == []


def test_provider_journal_rechecks_lane_after_conversion_mapping_race(
    postgres_store,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "message",
            "message": {
                "id": 600,
                "type": "stream",
                "stream_id": 42,
            },
        },
    )
    update_event = {
        "id": 2,
        "type": "update_message",
        "message_id": 601,
        "content": "edited",
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        update_event,
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 1 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = now() - (3 - event_id) * interval '1 minute'
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]
    assert selected[0]["causal_lane"] == "message:601"

    class Adapter:
        server_url = "https://zulip.example.invalid"

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.file_client = None
    instance.provider_adapters = lambda _account_uuid: Adapter()
    instance._queue_event_catalog = lambda *_args, **_kwargs: False

    def convert_after_mapping_commit(*_args, **_kwargs):
        postgres_store.remember_provider_mapping(
            account_uuid,
            "message",
            "601",
            str(uuid.uuid4()),
            {"chat_key": "channel:42"},
        )
        return [{"record_uuid": str(uuid.uuid4())}]

    instance._event_records_with_pending_delete_recreations = (
        convert_after_mapping_commit
    )

    assert instance.process_provider_journal(selected) == 0

    with postgres_store.session() as session:
        provider_event = session.execute(
            """
            SELECT processing_state, causal_lane, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
        delivery_count = session.execute(
            """
            SELECT count(*) AS count
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = 'queue'
              AND provider_event_id = 2
            """,
            (account_uuid,),
        ).fetchone()["count"]
    assert provider_event == {
        "processing_state": "pending",
        "causal_lane": "channel:42",
        "prepared_records": None,
    }
    assert delivery_count == 0
    assert not postgres_store.has_pending_provider_events()


def test_provider_journal_terminalization_rechecks_lane_after_refresh_race(
    postgres_store,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "message",
            "message": {
                "id": 600,
                "type": "stream",
                "stream_id": 42,
            },
        },
    )
    update_event = {
        "id": 2,
        "type": "update_message",
        "message_id": 601,
        "content": "edited",
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        update_event,
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 1 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = now() - (3 - event_id) * interval '1 minute'
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]
    assert selected[0]["causal_lane"] == "message:601"

    class Adapter:
        server_url = "https://zulip.example.invalid"

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.file_client = None
    instance.provider_adapters = lambda _account_uuid: Adapter()
    instance._queue_event_catalog = lambda *_args, **_kwargs: False
    instance._event_records_with_pending_delete_recreations = (
        lambda *_args, **_kwargs: []
    )

    refresh = postgres_store.refresh_provider_event_causal_lane
    refresh_count = 0

    def refresh_then_commit_mapping(*args):
        nonlocal refresh_count
        lane = refresh(*args)
        refresh_count += 1
        if refresh_count == 2:
            postgres_store.remember_provider_mapping(
                account_uuid,
                "message",
                "601",
                str(uuid.uuid4()),
                {"chat_key": "channel:42"},
            )
        return lane

    postgres_store.refresh_provider_event_causal_lane = refresh_then_commit_mapping

    assert instance.process_provider_journal(selected) == 0
    assert refresh_count == 2

    with postgres_store.session() as session:
        provider_event = session.execute(
            """
            SELECT processing_state, causal_lane, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
        delivery_count = session.execute(
            """
            SELECT count(*) AS count
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = 'queue'
              AND provider_event_id = 2
            """,
            (account_uuid,),
        ).fetchone()["count"]
    assert provider_event == {
        "processing_state": "pending",
        "causal_lane": "channel:42",
        "prepared_records": None,
    }
    assert delivery_count == 0
    assert not postgres_store.has_pending_provider_events()


def test_provider_journal_guard_rechecks_resolved_lane_after_mapping_move(
    postgres_store,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "601",
        str(uuid.uuid4()),
        {"chat_key": "channel:42"},
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {
            "id": 1,
            "type": "message",
            "message": {
                "id": 600,
                "type": "stream",
                "stream_id": 43,
            },
        },
    )
    update_event = {
        "id": 2,
        "type": "update_message",
        "message_id": 601,
        "content": "edited after move",
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        update_event,
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = CASE event_id
                    WHEN 1 THEN now() + interval '5 minutes'
                    ELSE now()
                END,
                created_at = now() - (3 - event_id) * interval '1 minute'
            WHERE account_uuid = %s AND queue_id = 'queue'
            """,
            (account_uuid,),
        )

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]
    assert selected[0]["causal_lane"] == "channel:42"

    class Adapter:
        server_url = "https://zulip.example.invalid"

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.file_client = None
    instance.provider_adapters = lambda _account_uuid: Adapter()
    instance._queue_event_catalog = lambda *_args, **_kwargs: False
    instance._event_records_with_pending_delete_recreations = (
        lambda *_args, **_kwargs: [{"record_uuid": str(uuid.uuid4())}]
    )

    refresh = postgres_store.refresh_provider_event_causal_lane
    refresh_count = 0

    def refresh_then_move_mapping(*args):
        nonlocal refresh_count
        lane = refresh(*args)
        refresh_count += 1
        if refresh_count == 2:
            postgres_store.remember_provider_mapping(
                account_uuid,
                "message",
                "601",
                str(uuid.uuid4()),
                {"chat_key": "channel:43"},
            )
        return lane

    postgres_store.refresh_provider_event_causal_lane = refresh_then_move_mapping

    assert instance.process_provider_journal(selected) == 0
    assert refresh_count == 2

    with postgres_store.session() as session:
        provider_event = session.execute(
            """
            SELECT processing_state, causal_lane, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
        delivery_count = session.execute(
            """
            SELECT count(*) AS count
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = 'queue'
              AND provider_event_id = 2
            """,
            (account_uuid,),
        ).fetchone()["count"]
    assert provider_event == {
        "processing_state": "pending",
        "causal_lane": "channel:43",
        "prepared_records": None,
    }
    assert delivery_count == 0
    assert not postgres_store.has_pending_provider_events()


def test_provider_journal_guard_resolves_grouped_global_lane_before_finalize(
    postgres_store,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    flags_event = {
        "id": 1,
        "type": "update_message_flags",
        "message_ids": [601, 602],
        "flag": "read",
        "op": "add",
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        flags_event,
    )
    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [1]
    assert selected[0]["causal_lane"] is None

    class Adapter:
        server_url = "https://zulip.example.invalid"

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.file_client = None
    instance.provider_adapters = lambda _account_uuid: Adapter()
    instance._queue_event_catalog = lambda *_args, **_kwargs: False
    instance._event_records_with_pending_delete_recreations = (
        lambda *_args, **_kwargs: []
    )

    refresh = postgres_store.refresh_provider_event_causal_lane
    refresh_count = 0

    def refresh_then_commit_mappings(*args):
        nonlocal refresh_count
        lane = refresh(*args)
        refresh_count += 1
        if refresh_count == 2:
            for provider_message_id in ("601", "602"):
                postgres_store.remember_provider_mapping(
                    account_uuid,
                    "message",
                    provider_message_id,
                    str(uuid.uuid4()),
                    {"chat_key": "channel:42"},
                )
        return lane

    postgres_store.refresh_provider_event_causal_lane = (
        refresh_then_commit_mappings
    )

    assert instance.process_provider_journal(selected) == 0
    assert refresh_count == 2

    with postgres_store.session() as session:
        provider_event = session.execute(
            """
            SELECT processing_state, causal_lane, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        ).fetchone()
        delivery_count = session.execute(
            """
            SELECT count(*) AS count
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = 'queue'
              AND provider_event_id = 1
            """,
            (account_uuid,),
        ).fetchone()["count"]
    assert provider_event == {
        "processing_state": "pending",
        "causal_lane": "channel:42",
        "prepared_records": None,
    }
    assert delivery_count == 0


def test_assignment_retry_uses_bounded_exponential_backoff(postgres_store):
    account_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        {"id": 1, "type": "message"},
    )

    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        1,
        "provider_chat_assignment_pending",
    )
    with postgres_store.session() as session:
        first = session.execute(
            """
            SELECT retry_count,
                   available_at > now() + interval '500 milliseconds'
                       AS backed_off
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        ).fetchone()
        session.execute(
            """
            UPDATE zulip_provider_events
            SET retry_count = 20, available_at = now()
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        )

    assert first == {"retry_count": 1, "backed_off": True}

    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        1,
        "provider_chat_assignment_pending",
    )
    with postgres_store.session() as session:
        capped = session.execute(
            """
            SELECT retry_count,
                   available_at > now() + interval '55 seconds' AS capped_low,
                   available_at <= now() + interval '60 seconds' AS capped_high
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        ).fetchone()

    assert capped == {
        "retry_count": 21,
        "capped_low": True,
        "capped_high": True,
    }


def test_pending_provider_probe_includes_only_nonterminal_delivering_head(
    postgres_store,
):
    account_uuid = str(uuid.uuid4())
    delivery_uuid = str(uuid.uuid4())
    assert postgres_store.record_provider_event(
        account_uuid, "queue", {"id": 1, "type": "message"}
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_provider_events
            SET processing_state = 'delivering',
                available_at = now() + interval '5 minutes'
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        )
        session.execute(
            """
            INSERT INTO workspace_delivery_outbox (
                record_uuid, operation_uuid, account_uuid,
                provider_queue_id, provider_event_id,
                submission_state, priority, record
            ) VALUES (%s, %s, %s, 'queue', 1, 'pending', 0, '{}'::jsonb)
            """,
            (delivery_uuid, str(uuid.uuid4()), account_uuid),
        )

    assert postgres_store.has_pending_provider_events()

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE workspace_delivery_outbox
            SET submission_state = 'rejected'
            WHERE record_uuid = %s
            """,
            (delivery_uuid,),
        )

    assert not postgres_store.has_pending_provider_events()


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
    assert postgres_store.enqueue_workspace_delivery(records[0], 0, queue_id, event_id)
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
        postgres_store.enqueue_workspace_delivery(records[1], 0, queue_id, event_id)

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
        postgres_store.mark_workspace_delivery_submitted(topic_record["record_uuid"])

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
    assert {record["operation_uuid"] for record in replay}.isdisjoint(
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
    assert {record["project_uuid"] for record in update_records} == {new_project_uuid}
    assert any(
        record["operation"]["kind"] == "message.update" for record in update_records
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


def test_stale_reset_replays_message_after_permanent_setup_rejection(
    postgres_store,
):
    account_uuid, old_project_uuid = _insert_account_and_assignment(postgres_store)
    _materialize_channel_projection(
        postgres_store,
        account_uuid,
        old_project_uuid,
    )
    message = _provider_history_message(607)
    queue_id = "provider-message:607"
    event_id = 23
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
    rejected = next(
        record for record in prepared if record["operation"]["kind"] != "message.create"
    )
    assert postgres_store.mark_workspace_delivery_submitting(rejected["record_uuid"])
    assert postgres_store.reject_provider_event_submission(
        rejected["record_uuid"],
        "provider_api_http_422",
    )
    assert postgres_store.finalize_ready_provider_events() == 1

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

    replay = converter.event_records(
        postgres_store,
        account_uuid,
        queue_id,
        event,
    )
    assert {record["project_uuid"] for record in replay} == {new_project_uuid}


def test_assignment_change_replays_quarantined_rejected_message(
    postgres_store,
):
    account_uuid, old_project_uuid = _insert_account_and_assignment(postgres_store)
    _materialize_channel_projection(
        postgres_store,
        account_uuid,
        old_project_uuid,
    )
    message = _provider_history_message(608)
    queue_id = "provider-message:608"
    event_id = 24
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
    message_record = next(
        record
        for record in prepared
        if record["operation"]["kind"] == "message.create"
    )
    assert postgres_store.enqueue_workspace_delivery(
        message_record,
        0,
        queue_id,
        event_id,
    )
    postgres_store.mark_provider_event_delivering(
        account_uuid,
        queue_id,
        event_id,
    )
    assert postgres_store.mark_workspace_delivery_submitting(
        message_record["record_uuid"]
    )
    assert postgres_store.reject_provider_event_submission(
        message_record["record_uuid"],
        "provider_api_http_422",
    )
    assert postgres_store.finalize_ready_provider_events() == 1

    with postgres_store.session() as session:
        quarantined = session.execute(
            """
            SELECT processing_state, processing_reason,
                   prepared_records IS NOT NULL AS prepared
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
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
    assert quarantined == {
        "processing_state": "invalid",
        "processing_reason": "workspace_delivery_rejected",
        "prepared": True,
    }

    assert postgres_store.reset_stale_workspace_deliveries() == 1
    with postgres_store.session() as session:
        replay = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert replay == {
        "processing_state": "pending",
        "processing_reason": "assignment_changed",
        "prepared_records": None,
    }


def test_assignment_generation_replays_quarantined_rejected_message(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    queue_id = "provider-message:609"
    event_id = 25
    event = {
        "id": event_id,
        "type": "message",
        "message": {"type": "stream", "stream_id": 42},
    }
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        event_id,
        [_provider_record(account_uuid, project_uuid)],
    )
    record = prepared[0]
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
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    assert postgres_store.reject_provider_event_submission(
        record["record_uuid"],
        "provider_api_http_422",
    )
    assert postgres_store.finalize_ready_provider_events() == 1

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
        replay = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        remaining_deliveries = session.execute(
            """
            SELECT count(*) AS total
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()["total"]
    assert replay == {
        "processing_state": "pending",
        "processing_reason": "assignment_changed",
        "prepared_records": prepared,
    }
    assert remaining_deliveries == 0

    replayed = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        event_id,
        [_provider_record(account_uuid, project_uuid)],
    )
    assert replayed == prepared
    assert postgres_store.enqueue_workspace_delivery(
        replayed[0],
        0,
        queue_id,
        event_id,
    )
    with postgres_store.session() as session:
        assignment_generation = session.execute(
            """
            SELECT assignment_generation
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (replayed[0]["record_uuid"],),
        ).fetchone()["assignment_generation"]
    assert assignment_generation == 2


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


def test_assignment_reset_replays_event_with_rebound_ambiguous_sibling(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    queue_id = "queue"
    event_id = 7
    event = {
        "id": event_id,
        "type": "message",
        "message": {"type": "stream", "stream_id": 42},
    }
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    rejected = _bind_provider_record_projection(
        _provider_record(account_uuid, project_uuid),
        stream_uuid,
        topic_uuid,
        author_uuid,
    )
    ambiguous = _bind_provider_record_projection(
        _provider_record(account_uuid, project_uuid),
        stream_uuid,
        topic_uuid,
        author_uuid,
    )
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        event_id,
        [rejected, ambiguous],
    )
    rejected, ambiguous = prepared
    postgres_store.enqueue_provider_event_records(
        prepared,
        0,
        account_uuid,
        queue_id,
        event_id,
    )
    assert postgres_store.mark_workspace_delivery_submitting(
        rejected["record_uuid"]
    )
    assert postgres_store.reject_provider_event_submission(
        rejected["record_uuid"],
        "provider_api_http_422",
    )
    assert postgres_store.mark_workspace_delivery_submitting(
        ambiguous["record_uuid"]
    )
    assert postgres_store.mark_interrupted_workspace_deliveries_ambiguous() == 1
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
        source = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        deliveries = session.execute(
            """
            SELECT operation_uuid, submission_state, submission_attempts
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = %s
              AND provider_event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchall()
    assert source == {
        "processing_state": "pending",
        "processing_reason": "assignment_changed",
        "prepared_records": prepared,
    }
    assert deliveries == [
        {
            "operation_uuid": uuid.UUID(str(ambiguous["operation_uuid"])),
            "submission_state": "ambiguous",
            "submission_attempts": 1,
        }
    ]
    postgres_store.accept_result(_committed_result(ambiguous))
    replayed = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        event_id,
        prepared,
    )
    postgres_store.enqueue_provider_event_records(
        replayed,
        0,
        account_uuid,
        queue_id,
        event_id,
    )
    postgres_store.accept_result(_committed_result(rejected))
    assert postgres_store.finalize_ready_provider_events() == 1
    with postgres_store.session() as session:
        resolved = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert resolved == {"processing_state": "processed", "processing_reason": None}


def test_assignment_reset_preserves_released_submission_attempt(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    queue_id = "queue"
    event_id = 8
    event = {
        "id": event_id,
        "type": "message",
        "message": {"type": "stream", "stream_id": 42},
    }
    assert postgres_store.record_provider_event(account_uuid, queue_id, event)
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        event_id,
        [
            _bind_provider_record_projection(
                _provider_record(account_uuid, project_uuid),
                stream_uuid,
                topic_uuid,
                author_uuid,
            )
        ],
    )
    record = prepared[0]
    postgres_store.enqueue_provider_event_records(
        prepared,
        0,
        account_uuid,
        queue_id,
        event_id,
    )
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    postgres_store.release_provider_event_submissions([record["record_uuid"]])
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1
            WHERE resource_type = 'external_chat_assignment'
            """
        )

    assert postgres_store.reset_stale_workspace_deliveries() == 0
    with postgres_store.session() as session:
        source = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        delivery = session.execute(
            """
            SELECT submission_state, submission_attempts
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
    assert source == {"processing_state": "delivering", "processing_reason": None}
    assert delivery == {"submission_state": "pending", "submission_attempts": 1}
    assert postgres_store.pending_workspace_deliveries() == [record]


def test_assignment_reset_keeps_active_submission_rejectable(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    queue_id = "queue"
    event_id = 9
    assert postgres_store.record_provider_event(
        account_uuid,
        queue_id,
        {
            "id": event_id,
            "type": "message",
            "message": {"type": "stream", "stream_id": 42},
        },
    )
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        event_id,
        [
            _bind_provider_record_projection(
                _provider_record(account_uuid, project_uuid),
                stream_uuid,
                topic_uuid,
                author_uuid,
            )
        ],
    )
    record = prepared[0]
    postgres_store.enqueue_provider_event_records(
        prepared,
        0,
        account_uuid,
        queue_id,
        event_id,
    )
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1
            WHERE resource_type = 'external_chat_assignment'
            """
        )

    assert postgres_store.reset_stale_workspace_deliveries() == 0
    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, assignment_generation
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
    assert delivery == {"submission_state": "submitting", "assignment_generation": 2}
    assert postgres_store.reject_provider_event_submission(
        record["record_uuid"],
        "provider_api_http_422",
    )


def test_assignment_reset_and_rejection_share_outbox_first_lock_order(
    postgres_store,
    migrated_postgres_dsn,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    record = _bind_provider_record_projection(
        _provider_record(account_uuid, project_uuid),
        stream_uuid,
        topic_uuid,
        author_uuid,
    )
    operation = record["operation"]
    assert isinstance(operation, dict)
    provider = operation["provider"]
    assert isinstance(provider, dict)
    provider["entity_id"] = "777"
    record["operation_sha256"] = canonical.operation_digest(record)
    assert postgres_store.enqueue_workspace_delivery(record, 0, "queue", 11)
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1
            WHERE resource_type = 'external_chat_assignment'
            """
        )

    rejection_results = []
    reset_results = []
    failures = []
    reset_started = threading.Event()
    reset_finished = threading.Event()

    def reject_submission():
        try:
            rejection_results.append(
                postgres_store.reject_provider_event_submission(
                    record["record_uuid"],
                    "provider_api_http_422",
                )
            )
        except Exception as exc:  # pragma: no cover - reported below
            failures.append(exc)

    def reset_assignment():
        reset_started.set()
        try:
            reset_results.append(postgres_store.reset_stale_workspace_deliveries())
        except Exception as exc:  # pragma: no cover - reported below
            failures.append(exc)
        finally:
            reset_finished.set()

    rejection_thread = threading.Thread(target=reject_submission)
    reset_thread = threading.Thread(target=reset_assignment)
    probe_store = storage.RestAlchemyStore(migrated_postgres_dsn)
    mapping_lock_key = storage._provider_mapping_lock_key(
        account_uuid,
        "message",
        "777",
    )
    rejection_waiting = False
    with postgres_store.session() as blocker:
        blocker.execute(
            """
            SELECT record_uuid
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            FOR UPDATE
            """,
            (record["record_uuid"],),
        ).fetchone()
        rejection_thread.start()
        for _attempt in range(200):
            with probe_store.session() as probe:
                acquired = probe.execute(
                    """
                    SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))
                        AS acquired
                    """,
                    (mapping_lock_key,),
                ).fetchone()["acquired"]
            if not acquired:
                rejection_waiting = True
                break
            threading.Event().wait(0.01)
        assert rejection_waiting
        reset_thread.start()
        assert reset_started.wait(timeout=5)
        assert not reset_finished.wait(timeout=0.1)

    rejection_thread.join(timeout=5)
    reset_thread.join(timeout=5)
    assert not rejection_thread.is_alive()
    assert not reset_thread.is_alive()
    assert failures == []
    assert rejection_results == [True]
    assert reset_results in ([0], [1])


def test_assignment_reset_quarantines_changed_projection_payload(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    queue_id = "queue"
    event_id = 10
    assert postgres_store.record_provider_event(
        account_uuid,
        queue_id,
        {
            "id": event_id,
            "type": "message",
            "message": {"type": "stream", "stream_id": 42},
        },
    )
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        event_id,
        [
            _bind_provider_record_projection(
                _provider_record(account_uuid, project_uuid),
                stream_uuid,
                topic_uuid,
                author_uuid,
            )
        ],
    )
    record = prepared[0]
    postgres_store.enqueue_provider_event_records(
        prepared,
        0,
        account_uuid,
        queue_id,
        event_id,
    )
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    postgres_store.release_provider_event_submissions([record["record_uuid"]])
    replacement_stream_uuid = str(uuid.uuid4())
    replacement_topic_uuid = str(uuid.uuid4())
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1
            WHERE resource_type = 'external_chat_assignment'
            """
        )
        session.execute(
            """
            UPDATE provider_mappings
            SET workspace_uuid = %s, updated_at = now()
            WHERE account_uuid = %s AND entity_kind = 'stream'
              AND provider_id = 'channel:42'
            """,
            (replacement_stream_uuid, account_uuid),
        )
        session.execute(
            """
            UPDATE provider_mappings
            SET workspace_uuid = %s,
                metadata = jsonb_set(
                    metadata,
                    '{stream_uuid}',
                    to_jsonb(%s::text)
                ),
                updated_at = now()
            WHERE account_uuid = %s AND entity_kind = 'topic'
              AND provider_id = '42:Topic'
            """,
            (replacement_topic_uuid, replacement_stream_uuid, account_uuid),
        )

    assert postgres_store.reset_stale_workspace_deliveries() == 0
    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, submission_attempts,
                   assignment_generation,
                   record->'operation'->'payload'->>'stream_uuid' AS stream_uuid,
                   record->'operation'->'payload'->>'topic_uuid' AS topic_uuid
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
        source = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert delivery == {
        "submission_state": "pending",
        "submission_attempts": 1,
        "assignment_generation": 1,
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
    }
    assert source == {
        "processing_state": "invalid",
        "processing_reason": "workspace_delivery_assignment_ambiguous",
    }
    assert postgres_store.pending_workspace_deliveries() == []


@pytest.mark.parametrize("setup_kind", ["stream.upsert", "topic.upsert"])
def test_assignment_reset_quarantines_changed_setup_projection(
    postgres_store,
    setup_kind,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "topic",
        "42:Topic",
        topic_uuid,
        {
            "stream_uuid": stream_uuid,
            "chat_key": "channel:42",
            "name": "Topic",
        },
    )
    queue_id = f"queue-{setup_kind}"
    event_id = 12 if setup_kind == "stream.upsert" else 13
    assert postgres_store.record_provider_event(
        account_uuid,
        queue_id,
        {
            "id": event_id,
            "type": "message",
            "message": {"type": "stream", "stream_id": 42},
        },
    )
    record = _provider_record(account_uuid, project_uuid, kind=setup_kind)
    operation = record["operation"]
    assert isinstance(operation, dict)
    provider = operation["provider"]
    assert isinstance(provider, dict)
    if setup_kind == "stream.upsert":
        operation["entity_uuid"] = stream_uuid
        provider["entity_id"] = "channel:42"
        operation["payload"] = {
            "name": "Engineering",
            "description": "",
            "private": True,
            "chat_kind": "channel",
            "participant_uuids": sorted(
                [
                    str(postgres_store.account_resource(account_uuid)["owner_user_uuid"]),
                    author_uuid,
                ]
            ),
            "default_topic_uuid": None,
        }
    else:
        operation["entity_uuid"] = topic_uuid
        provider["entity_id"] = "42:Topic"
        operation["payload"] = {"stream_uuid": stream_uuid, "name": "Topic"}
    record["causal_lane"] = f"chat:{account_uuid}:{stream_uuid}"
    record["operation_sha256"] = canonical.operation_digest(record)
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        queue_id,
        event_id,
        [record],
    )
    record = prepared[0]
    postgres_store.enqueue_provider_event_records(
        prepared,
        0,
        account_uuid,
        queue_id,
        event_id,
    )
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    postgres_store.release_provider_event_submissions([record["record_uuid"]])
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1
            WHERE resource_type = 'external_chat_assignment'
            """
        )
        if setup_kind == "stream.upsert":
            session.execute(
                """
                UPDATE provider_mappings
                SET metadata = jsonb_set(
                        metadata,
                        '{name}',
                        '"Platform"'::jsonb
                    )
                WHERE account_uuid = %s AND entity_kind = 'stream'
                  AND provider_id = 'channel:42'
                """,
                (account_uuid,),
            )
        else:
            session.execute(
                """
                UPDATE provider_mappings
                SET provider_id = '42:Renamed',
                    metadata = jsonb_set(
                        metadata,
                        '{name}',
                        '"Renamed"'::jsonb
                    )
                WHERE account_uuid = %s AND entity_kind = 'topic'
                  AND provider_id = '42:Topic'
                """,
                (account_uuid,),
            )

    assert postgres_store.reset_stale_workspace_deliveries() == 0
    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, assignment_generation
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
        source = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
    assert delivery == {"submission_state": "pending", "assignment_generation": 1}
    assert source == {
        "processing_state": "invalid",
        "processing_reason": "workspace_delivery_assignment_ambiguous",
    }


def test_assignment_reset_quarantines_unsafe_backfill_without_target(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    record = _provider_record(account_uuid, project_uuid)
    assert postgres_store.enqueue_workspace_delivery(record, 2)
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    postgres_store.release_provider_event_submissions([record["record_uuid"]])
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources SET deleted = true
            WHERE resource_type = 'external_chat_assignment'
            """
        )

    assert postgres_store.reset_stale_workspace_deliveries() == 0
    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, submission_attempts, submission_error_code
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
        job = session.execute(
            """
            SELECT state, last_error_code
            FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
    assert delivery == {
        "submission_state": "ambiguous",
        "submission_attempts": 1,
        "submission_error_code": "workspace_delivery_assignment_ambiguous",
    }
    assert job == {
        "state": "failed",
        "last_error_code": "workspace_delivery_assignment_ambiguous",
    }


def test_assignment_reset_keeps_inflight_backfill_rejectable(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    record = _provider_record(account_uuid, project_uuid)
    operation = record["operation"]
    assert isinstance(operation, dict)
    provider = operation["provider"]
    assert isinstance(provider, dict)
    provider["entity_id"] = "778"
    record["operation_sha256"] = canonical.operation_digest(record)
    assert postgres_store.enqueue_workspace_delivery(record, 2)
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources SET deleted = true
            WHERE resource_type = 'external_chat_assignment'
            """
        )

    assert postgres_store.reset_stale_workspace_deliveries() == 0
    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, submission_attempts, submission_error_code
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
        job = session.execute(
            """
            SELECT state, last_error_code
            FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
    assert delivery == {
        "submission_state": "submitting",
        "submission_attempts": 1,
        "submission_error_code": "workspace_delivery_assignment_ambiguous",
    }
    assert job == {
        "state": "failed",
        "last_error_code": "workspace_delivery_assignment_ambiguous",
    }
    assert postgres_store.reject_provider_event_submission(
        record["record_uuid"],
        "provider_api_http_422",
    )


def test_replayed_committed_result_repairs_dangling_delivery(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    record = _provider_record(account_uuid, project_uuid)
    assert postgres_store.enqueue_workspace_delivery(record, 2)
    postgres_store.accept_result(_committed_result(record))

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE workspace_delivery_outbox
            SET sent_at = NULL, submission_state = 'pending'
            WHERE operation_uuid = %s
            """,
            (record["operation_uuid"],),
        )

    postgres_store.accept_result(_committed_result(record))

    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT sent_at, submission_state FROM workspace_delivery_outbox
            WHERE operation_uuid = %s
            """,
            (record["operation_uuid"],),
        ).fetchone()
        session.execute(
            "DELETE FROM workspace_delivery_outbox WHERE operation_uuid = %s",
            (record["operation_uuid"],),
        )
    assert delivery["sent_at"] is not None
    assert delivery["submission_state"] == "sent"
    assert not postgres_store.enqueue_workspace_delivery(record, 2)


def test_inactive_account_event_and_delivery_are_terminalized_without_deletion(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    queue_id = "retired-queue"
    event_id = 11
    record = _provider_record(account_uuid, project_uuid)
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body
            ) VALUES (%s, %s, %s, 'message', '{}'::jsonb)
            """,
            (account_uuid, queue_id, event_id),
        )
    assert postgres_store.enqueue_workspace_delivery(record, 0, queue_id, event_id)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET body = jsonb_set(
                body, '{synchronization_enabled}', 'false'::jsonb
            )
            WHERE resource_type = 'external_account'
              AND resource_uuid = %s
            """,
            (account_uuid,),
        )

    assert postgres_store.ignore_provider_event_for_inactive_account(
        account_uuid, queue_id, event_id
    )
    assert not postgres_store.ignore_provider_event_for_inactive_account(
        account_uuid, queue_id, event_id
    )

    with postgres_store.session() as session:
        event = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        delivery = session.execute(
            """
            SELECT submission_state, submission_error_code, sent_at
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
    assert event == {
        "processing_state": "ignored",
        "processing_reason": "account_inactive",
        "prepared_records": None,
    }
    assert delivery == {
        "submission_state": "cancelled",
        "submission_error_code": "account_inactive",
        "sent_at": None,
    }


def test_reaction_is_ignored_only_outside_completed_history_window(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    cutoff = datetime.datetime(2026, 7, 1, tzinfo=datetime.UTC)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET state = 'pending', cutoff_at = %s
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (cutoff, account_uuid),
        )
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body
            )
            SELECT %s, 'queue', event_id, 'reaction', '{}'::jsonb
            FROM generate_series(1, 5) AS event_id
            """,
            (account_uuid,),
        )

    old_message_time = datetime.datetime(2026, 6, 1, tzinfo=datetime.UTC)
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "601",
        old_message_time,
        "queue",
        1,
    )

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET state = 'complete'
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        )
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "602",
        datetime.datetime(2026, 7, 2, tzinfo=datetime.UTC),
        "queue",
        2,
    )

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET cutoff_at = NULL
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        )
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "603",
        old_message_time,
        "queue",
        3,
    )

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET cutoff_at = %s
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (cutoff, account_uuid),
        )
        session.execute(
            """
            INSERT INTO provider_mappings (
                account_uuid, entity_kind, workspace_uuid, provider_id, metadata
            ) VALUES (%s, 'message', %s, '604', %s)
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
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "604",
        old_message_time,
        "queue",
        4,
    )
    assert postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "605",
        old_message_time,
        "queue",
        5,
    )
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "605",
        old_message_time,
        "queue",
        5,
    )

    with postgres_store.session() as session:
        events = session.execute(
            """
            SELECT event_id, processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue'
            ORDER BY event_id
            """,
            (account_uuid,),
        ).fetchall()
    assert events == [
        {"event_id": 1, "processing_state": "pending", "processing_reason": None},
        {"event_id": 2, "processing_state": "pending", "processing_reason": None},
        {"event_id": 3, "processing_state": "pending", "processing_reason": None},
        {"event_id": 4, "processing_state": "pending", "processing_reason": None},
        {
            "event_id": 5,
            "processing_state": "ignored",
            "processing_reason": "provider_message_outside_history",
        },
    ]


def test_reaction_waits_for_other_recoverable_mapping_sources(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    cutoff = datetime.datetime(2026, 7, 1, tzinfo=datetime.UTC)
    message_time = datetime.datetime(2026, 6, 1, tzinfo=datetime.UTC)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET state = 'complete', cutoff_at = %s
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (cutoff, account_uuid),
        )
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body
            ) VALUES
                (%s, 'reaction-queue', 1, 'reaction', '{}'::jsonb),
                (%s, 'reaction-queue', 2, 'reaction', '{}'::jsonb),
                (%s, 'reaction-queue', 3, 'reaction', '{}'::jsonb),
                (
                    %s, 'message-queue', 99, 'message',
                    '{"type":"message","message":{"id":606}}'::jsonb
                )
            """,
            (account_uuid, account_uuid, account_uuid, account_uuid),
        )
        session.execute(
            """
            INSERT INTO zulip_queue_catchup_jobs (
                account_uuid, provider_chat_key, state
            ) VALUES (%s, 'channel:42', 'manual')
            """,
            (account_uuid,),
        )

    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "606",
        message_time,
        "reaction-queue",
        1,
    )
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "607",
        message_time,
        "reaction-queue",
        2,
    )
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "608",
        message_time,
        "reaction-queue",
        3,
    )

    postgres_store.begin_provider_queue_catchup(account_uuid)
    with postgres_store.session() as session:
        catchup = session.execute(
            """
            SELECT state FROM zulip_queue_catchup_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET state = 'complete', cutoff_at = %s
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (cutoff, account_uuid),
        )
    assert catchup["state"] == "pending"
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "608",
        message_time,
        "reaction-queue",
        3,
    )

    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "606",
        str(uuid.uuid4()),
        {"chat_key": "channel:42", "project_uuid": project_uuid},
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "608",
        str(uuid.uuid4()),
        {"chat_key": "channel:42", "project_uuid": project_uuid},
    )
    postgres_store.mark_provider_event_processed(
        account_uuid,
        "message-queue",
        99,
        True,
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_queue_catchup_jobs
            SET state = 'complete'
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        )

    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "606",
        message_time,
        "reaction-queue",
        1,
    )
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "608",
        message_time,
        "reaction-queue",
        3,
    )
    assert postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "607",
        message_time,
        "reaction-queue",
        2,
    )
    with postgres_store.session() as session:
        reactions = session.execute(
            """
            SELECT event_id, processing_state
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'reaction-queue'
            ORDER BY event_id
            """,
            (account_uuid,),
        ).fetchall()
    assert reactions == [
        {"event_id": 1, "processing_state": "pending"},
        {"event_id": 2, "processing_state": "ignored"},
        {"event_id": 3, "processing_state": "pending"},
    ]


def test_reaction_waits_for_processed_local_echo_operation(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    cutoff = datetime.datetime(2026, 7, 1, tzinfo=datetime.UTC)
    message_time = datetime.datetime(2026, 6, 1, tzinfo=datetime.UTC)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET state = 'complete', cutoff_at = %s
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (cutoff, account_uuid),
        )
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state
            ) VALUES
                (%s, 'reaction-queue', 1, 'reaction', '{}'::jsonb, 'pending'),
                (
                    %s, 'message-queue', 99, 'message',
                    '{
                        "type":"message",
                        "local_message_id":"local-606",
                        "message":{"id":606}
                    }'::jsonb,
                    'processed'
                )
            """,
            (account_uuid, account_uuid),
        )

    record = _provider_record(account_uuid, project_uuid)
    assert postgres_store.enqueue(record, 0)
    item = postgres_store.claim("worker")
    assert item is not None
    postgres_store.record_provider_attempt(
        item,
        "message-queue",
        "local-606",
        99,
        "hello",
    )
    postgres_store.mark_uncertain(item, "ambiguous_provider_outcome")

    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state
            )
            SELECT
                %s, 'retained-message-journal', sample, 'message',
                jsonb_build_object(
                    'type', 'message',
                    'message', jsonb_build_object('id', 100000 + sample)
                ),
                'processed'
            FROM generate_series(1, 10000) AS sample
            """,
            (account_uuid,),
        )
        session.execute("ANALYZE zulip_provider_events")
        session.execute("ANALYZE bridge_operations")
        local_echo_plan = _explain_text(
            session,
            """
            SELECT 1
            FROM zulip_provider_events AS echo_event
            JOIN bridge_operations AS operation
              ON operation.account_uuid = echo_event.account_uuid
             AND operation.provider_queue_id = echo_event.queue_id
             AND operation.provider_local_id =
                 echo_event.body->>'local_message_id'
             AND operation.provider_local_id IS NOT NULL
             AND operation.state IN ('pending', 'running', 'uncertain')
             AND operation.record->'operation'->>'kind' = 'message.create'
            WHERE echo_event.account_uuid = %s
              AND echo_event.event_type = 'message'
              AND echo_event.body ? 'local_message_id'
              AND echo_event.body->'message'->>'id' = %s
            """,
            (account_uuid, "606"),
        )
    assert "zulip_provider_message_events_local_echo_idx" in local_echo_plan
    assert "Seq Scan on zulip_provider_events" not in local_echo_plan

    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "606",
        message_time,
        "reaction-queue",
        1,
    )

    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE bridge_operations
            SET reconciliation_after = now()
            WHERE record_uuid = %s
            """,
            (str(item.record_uuid),),
        )

    class MatchingAdapter:
        def reconcile_message(
            self,
            operation,
            attempted_at,
            provider_rendered_content=None,
        ):
            return zulip_adapter.ReconciliationEvidence(
                "now",
                ("606",),
                1,
                "606",
            )

    reconciler = scheduler.Scheduler(
        postgres_store,
        lambda _: MatchingAdapter(),
        "reconciler",
    )
    assert reconciler.reconcile_once()
    mapping = postgres_store.provider_mapping(account_uuid, "message", "606")
    assert mapping is not None
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "606",
        message_time,
        "reaction-queue",
        1,
    )
    with postgres_store.session() as session:
        reaction = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s
              AND queue_id = 'reaction-queue' AND event_id = 1
            """,
            (account_uuid,),
        ).fetchone()
    assert reaction == {
        "processing_state": "pending",
        "processing_reason": None,
    }


def test_terminal_local_echo_operation_no_longer_blocks_reaction(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    cutoff = datetime.datetime(2026, 7, 1, tzinfo=datetime.UTC)
    message_time = datetime.datetime(2026, 6, 1, tzinfo=datetime.UTC)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET state = 'complete', cutoff_at = %s
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (cutoff, account_uuid),
        )
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state
            ) VALUES
                (%s, 'reaction-queue', 1, 'reaction', '{}'::jsonb, 'pending'),
                (
                    %s, 'message-queue', 99, 'message',
                    '{
                        "type":"message",
                        "local_message_id":"local-607",
                        "message":{"id":607}
                    }'::jsonb,
                    'processed'
                )
            """,
            (account_uuid, account_uuid),
        )

    record = _provider_record(account_uuid, project_uuid)
    assert postgres_store.enqueue(record, 0)
    item = postgres_store.claim("worker")
    assert item is not None
    postgres_store.record_provider_attempt(
        item,
        "message-queue",
        "local-607",
        99,
        "hello",
    )
    postgres_store.mark_uncertain(item, "ambiguous_provider_outcome")

    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "607",
        message_time,
        "reaction-queue",
        1,
    )
    postgres_store.require_operation_manual_reconciliation(
        item,
        "unsafe_provider_state",
        {"match_count": None},
    )
    assert postgres_store.ignore_provider_reaction_outside_history_window(
        account_uuid,
        "channel:42",
        "607",
        message_time,
        "reaction-queue",
        1,
    )


def test_new_history_persists_cutoff_without_bounding_all(postgres_store):
    new_account_uuid, _ = _insert_account_and_assignment(postgres_store, "new")
    all_account_uuid, _ = _insert_account_and_assignment(postgres_store, "all")
    selection_time = datetime.datetime(2026, 6, 1, tzinfo=datetime.UTC)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET updated_at = %s
            WHERE resource_type = 'external_chat_assignment'
              AND body->>'external_account_uuid' = %s
            """,
            (selection_time, new_account_uuid),
        )

    postgres_store.reconcile_backfill_jobs()

    with postgres_store.session() as session:
        initial = session.execute(
            """
            SELECT account_uuid, history_depth, state, cutoff_at
            FROM zulip_backfill_jobs
            ORDER BY history_depth
            """
        ).fetchall()
    assert initial == [
        {
            "account_uuid": uuid.UUID(all_account_uuid),
            "history_depth": "all",
            "state": "pending",
            "cutoff_at": None,
        },
        {
            "account_uuid": uuid.UUID(new_account_uuid),
            "history_depth": "new",
            "state": "complete",
            "cutoff_at": selection_time,
        },
    ]

    repaired_cutoff = datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC)
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET cutoff_at = NULL, updated_at = %s
            WHERE account_uuid = %s AND history_depth = 'new'
            """,
            (repaired_cutoff, new_account_uuid),
        )
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET state = 'complete'
            WHERE account_uuid = %s AND history_depth = 'all'
            """,
            (all_account_uuid,),
        )
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body
            ) VALUES (%s, 'queue', 1, 'reaction', '{}'::jsonb),
                     (%s, 'queue', 1, 'reaction', '{}'::jsonb)
            """,
            (new_account_uuid, all_account_uuid),
        )

    postgres_store.reconcile_backfill_jobs()

    with postgres_store.session() as session:
        repaired = session.execute(
            """
            SELECT cutoff_at FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND history_depth = 'new'
            """,
            (new_account_uuid,),
        ).fetchone()
    assert repaired["cutoff_at"] == repaired_cutoff

    old_message_time = datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC)
    assert postgres_store.ignore_provider_reaction_outside_history_window(
        new_account_uuid,
        "channel:42",
        "608",
        old_message_time,
        "queue",
        1,
    )
    assert not postgres_store.ignore_provider_reaction_outside_history_window(
        all_account_uuid,
        "channel:42",
        "608",
        old_message_time,
        "queue",
        1,
    )


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
    assert postgres_store.enqueue_workspace_delivery(record, 0, queue_id, event_id)
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])

    assert postgres_store.reject_provider_event_submission(
        record["record_uuid"],
        "provider_api_http_422",
    )
    assert postgres_store.reject_provider_event_submission(
        record["record_uuid"],
        "provider_api_http_422",
    )
    assert not postgres_store.enqueue_workspace_delivery(record, 0)
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

    assert postgres_store.finalize_ready_provider_events() == 1
    with postgres_store.session() as session:
        event = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        delivery = session.execute(
            """
            SELECT submission_state, submission_error_code, sent_at
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (record["record_uuid"],),
        ).fetchone()
    assert event == {
        "processing_state": "invalid",
        "processing_reason": "workspace_delivery_rejected",
    }
    assert delivery == {
        "submission_state": "rejected",
        "submission_error_code": "provider_api_http_422",
        "sent_at": None,
    }


def test_rejected_delivery_no_longer_blocks_later_global_provider_event(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    first_event = {
        "id": 1,
        "type": "message",
        "message": {"type": "stream", "stream_id": 42},
    }
    barrier_event = {
        "id": 2,
        "type": "update_message",
        "message_ids": [601],
        "stream_id": 42,
        "new_stream_id": 43,
    }
    assert postgres_store.record_provider_event(account_uuid, "queue", first_event)
    assert postgres_store.record_provider_event(account_uuid, "queue", barrier_event)

    record = _provider_record(account_uuid, project_uuid)
    assert postgres_store.enqueue_workspace_delivery(record, 0, "queue", 1)
    postgres_store.mark_provider_event_delivering(account_uuid, "queue", 1)
    assert postgres_store.mark_workspace_delivery_submitting(record["record_uuid"])
    assert postgres_store.reject_provider_event_submission(
        record["record_uuid"],
        "provider_api_http_422",
    )

    assert postgres_store.pending_provider_events() == []
    assert not postgres_store.has_pending_provider_events()

    assert postgres_store.finalize_ready_provider_events() == 1
    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]
    assert selected[0]["causal_lane"] is None
    assert postgres_store.has_pending_provider_events()

    with postgres_store.session() as session:
        rejected_event = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            """,
            (account_uuid,),
        ).fetchone()
    assert rejected_event == {
        "processing_state": "invalid",
        "processing_reason": "workspace_delivery_rejected",
    }


def test_grouped_flags_can_convert_after_older_event_reaches_durable_outbox(
    postgres_store,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    first_event = {
        "id": 1,
        "type": "message",
        "message": {"type": "stream", "stream_id": 42},
    }
    flags_event = {
        "id": 2,
        "type": "update_message_flags",
        "messages": [601, 602],
        "flag": "read",
        "op": "add",
    }
    assert postgres_store.record_provider_event(account_uuid, "queue", first_event)
    assert postgres_store.record_provider_event(account_uuid, "queue", flags_event)
    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [1]

    postgres_store.mark_provider_event_delivering(account_uuid, "queue", 1)

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]
    assert selected[0]["causal_lane"] is None
    assert postgres_store.has_pending_provider_events()


@pytest.mark.parametrize("predecessor_type", ["update_message", "delete_message"])
def test_grouped_flags_wait_for_delivering_message_materialization_change(
    postgres_store,
    predecessor_type,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    predecessor = {
        "id": 1,
        "type": predecessor_type,
        "message_id": 601,
        "message_ids": [601],
    }
    if predecessor_type == "update_message":
        predecessor.update(
            {
                "stream_id": 42,
                "new_stream_id": 42,
                "orig_subject": "Old topic",
                "subject": "New topic",
            }
        )
    flags_event = {
        "id": 2,
        "type": "update_message_flags",
        "messages": [601, 602],
        "flag": "read",
        "op": "add",
    }
    assert postgres_store.record_provider_event(account_uuid, "queue", predecessor)
    assert postgres_store.record_provider_event(account_uuid, "queue", flags_event)
    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [1]

    postgres_store.mark_provider_event_delivering(account_uuid, "queue", 1)

    assert postgres_store.pending_provider_events() == []
    assert not postgres_store.has_pending_provider_events()


def test_grouped_flags_can_pass_delivering_content_only_update(postgres_store):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    edit_event = {
        "id": 1,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 42,
        "content": "edited",
    }
    flags_event = {
        "id": 2,
        "type": "update_message_flags",
        "messages": [601, 602],
        "flag": "read",
        "op": "add",
    }
    assert postgres_store.record_provider_event(account_uuid, "queue", edit_event)
    assert postgres_store.record_provider_event(account_uuid, "queue", flags_event)
    postgres_store.mark_provider_event_delivering(account_uuid, "queue", 1)

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]
    assert postgres_store.has_pending_provider_events()


def test_grouped_flags_finish_without_unblocking_older_chat_delivery(
    postgres_store,
):
    account_uuid, _project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    first_event = {
        "id": 1,
        "type": "message",
        "message": {"type": "stream", "stream_id": 42},
    }
    flags_event = {
        "id": 2,
        "type": "update_message_flags",
        "messages": [601, 602],
        "flag": "read",
        "op": "add",
    }
    later_event = {
        "id": 3,
        "type": "user_topic",
        "stream_id": 43,
    }
    assert postgres_store.record_provider_event(account_uuid, "queue", first_event)
    assert postgres_store.record_provider_event(account_uuid, "queue", flags_event)
    assert postgres_store.record_provider_event(account_uuid, "queue", later_event)
    postgres_store.mark_provider_event_delivering(account_uuid, "queue", 1)
    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]

    class Adapter:
        server_url = "https://zulip.example.invalid"

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.file_client = None
    instance.provider_adapters = lambda _account_uuid: Adapter()
    instance._queue_event_catalog = lambda *_args, **_kwargs: False
    instance._event_records_with_pending_delete_recreations = (
        lambda *_args, **_kwargs: []
    )
    instance._workspace_delivery_recovery_done = True

    processed = instance.process_provider_journal(selected)
    with postgres_store.session() as session:
        states = session.execute(
            """
            SELECT event_id, processing_state, processing_reason, causal_lane
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue'
              AND event_id IN (1, 2)
            ORDER BY event_id
            """,
            (account_uuid,),
        ).fetchall()
    assert (processed, states) == (1, [
        {
            "event_id": 1,
            "processing_state": "delivering",
            "processing_reason": None,
            "causal_lane": "channel:42",
        },
        {
            "event_id": 2,
            "processing_state": "processed",
            "processing_reason": None,
            "causal_lane": None,
        },
    ])

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [3]


def test_rejected_message_dependency_does_not_stall_grouped_flags(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    first_stream_uuid, first_topic_uuid, author_uuid = (
        _materialize_channel_projection(
            postgres_store,
            account_uuid,
            project_uuid,
        )
    )
    second_stream_uuid, second_topic_uuid = _materialize_destination_channel(
        postgres_store,
        account_uuid,
        project_uuid,
        43,
    )
    rejected_message_uuid = str(uuid.uuid4())
    committed_message_uuid = str(uuid.uuid4())
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "601",
        rejected_message_uuid,
        {
            "project_uuid": project_uuid,
            "stream_uuid": first_stream_uuid,
            "topic_uuid": first_topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": "channel:42",
            "causal_lane": f"chat:{account_uuid}:{first_stream_uuid}",
            "workspace_delivery_state": "pending",
        },
    )
    postgres_store.remember_provider_mapping(
        account_uuid,
        "message",
        "602",
        committed_message_uuid,
        {
            "project_uuid": project_uuid,
            "stream_uuid": second_stream_uuid,
            "topic_uuid": second_topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": "channel:43",
            "causal_lane": f"chat:{account_uuid}:{second_stream_uuid}",
            "workspace_delivery_state": "committed",
        },
    )

    message_event = {
        "id": 1,
        "type": "message",
        "message": {"type": "stream", "stream_id": 42},
    }
    flags_event = {
        "id": 2,
        "type": "update_message_flags",
        "messages": [601, 602],
        "flag": "read",
        "op": "add",
    }
    later_event = {
        "id": 3,
        "type": "user_topic",
        "stream_id": 44,
    }
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        message_event,
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        flags_event,
    )
    assert postgres_store.record_provider_event(
        account_uuid,
        "queue",
        later_event,
    )

    message_record = _provider_record(account_uuid, project_uuid)
    message_record["causal_lane"] = f"chat:{account_uuid}:{first_stream_uuid}"
    message_record["operation"]["entity_uuid"] = rejected_message_uuid
    message_record["operation"]["provider"]["entity_id"] = "601"
    message_record["operation"]["payload"].update(
        {
            "stream_uuid": first_stream_uuid,
            "topic_uuid": first_topic_uuid,
            "author_uuid": author_uuid,
        }
    )
    message_record["operation_sha256"] = canonical.operation_digest(message_record)
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        "queue",
        1,
        [message_record],
    )
    postgres_store.enqueue_provider_event_records(
        prepared,
        0,
        account_uuid,
        "queue",
        1,
    )

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]

    class Adapter:
        server_url = "https://zulip.example.invalid"

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.file_client = None
    instance.provider_adapters = lambda _account_uuid: Adapter()
    instance._queue_event_catalog = lambda *_args, **_kwargs: False
    instance._workspace_delivery_recovery_done = True
    assert instance.process_provider_journal(selected) == 1

    with postgres_store.session() as session:
        reads = session.execute(
            """
            SELECT record_uuid, submission_state, submission_error_code, record
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = 'queue'
              AND provider_event_id = 2
            ORDER BY record->'operation'->'provider'->>'chat_id'
            """,
            (account_uuid,),
        ).fetchall()
    assert len(reads) == 2
    rejected_read = next(
        row
        for row in reads
        if rejected_message_uuid
        in row["record"]["operation"]["payload"]["message_uuids"]
    )
    independent_read = next(
        row
        for row in reads
        if committed_message_uuid
        in row["record"]["operation"]["payload"]["message_uuids"]
    )

    assert postgres_store.mark_workspace_delivery_submitting(
        message_record["record_uuid"]
    )
    assert postgres_store.reject_provider_event_submission(
        message_record["record_uuid"],
        "provider_api_http_422",
    )
    assert postgres_store.finalize_ready_provider_events() == 1

    with postgres_store.session() as session:
        states = session.execute(
            """
            SELECT event_id, processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue'
              AND event_id IN (1, 2)
            ORDER BY event_id
            """,
            (account_uuid,),
        ).fetchall()
        read_states = session.execute(
            """
            SELECT record_uuid, submission_state, submission_error_code
            FROM workspace_delivery_outbox
            WHERE record_uuid = ANY(%s::uuid[])
            ORDER BY record_uuid
            """,
            ([rejected_read["record_uuid"], independent_read["record_uuid"]],),
        ).fetchall()
    assert states == [
        {
            "event_id": 1,
            "processing_state": "invalid",
            "processing_reason": "workspace_delivery_rejected",
        },
        {
            "event_id": 2,
            "processing_state": "delivering",
            "processing_reason": None,
        },
    ]
    assert {
        str(row["record_uuid"]): (
            row["submission_state"],
            row["submission_error_code"],
        )
        for row in read_states
    } == {str(independent_read["record_uuid"]): ("pending", None)}
    assert postgres_store.pending_provider_events() == []

    independent_record = independent_read["record"]
    assert postgres_store.mark_workspace_delivery_submitting(
        independent_record["record_uuid"]
    )
    postgres_store.accept_result(_committed_result(independent_record))
    assert postgres_store.finalize_ready_provider_events() == 1

    with postgres_store.session() as session:
        grouped = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
    assert grouped == {
        "processing_state": "processed",
        "processing_reason": None,
    }
    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [3]


def test_finalized_rejection_does_not_quarantine_later_grouped_read(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    rejected_message_uuid = str(uuid.uuid4())
    independent_message_uuid = str(uuid.uuid4())
    for provider_id, message_uuid in (
        ("601", rejected_message_uuid),
        ("602", independent_message_uuid),
    ):
        postgres_store.remember_provider_mapping(
            account_uuid,
            "message",
            provider_id,
            message_uuid,
            {
                "project_uuid": project_uuid,
                "stream_uuid": stream_uuid,
                "topic_uuid": topic_uuid,
                "author_uuid": author_uuid,
                "chat_key": "channel:42",
                "causal_lane": f"chat:{account_uuid}:{stream_uuid}",
                "workspace_delivery_state": "committed",
            },
        )
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state
            ) VALUES (%s, 'queue', 1, 'update_message', '{}'::jsonb, 'delivering')
            """,
            (account_uuid,),
        )
    rejected = _provider_record(account_uuid, project_uuid, kind="message.update")
    rejected["operation"]["entity_uuid"] = rejected_message_uuid
    rejected["operation"]["provider"]["entity_id"] = "601"
    rejected["operation_sha256"] = canonical.operation_digest(rejected)
    assert postgres_store.enqueue_workspace_delivery(rejected, 0, "queue", 1)
    assert postgres_store.mark_workspace_delivery_submitting(
        rejected["record_uuid"]
    )
    assert postgres_store.reject_provider_event_submission(
        rejected["record_uuid"],
        "provider_api_http_422",
    )
    assert postgres_store.finalize_ready_provider_events() == 1

    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state
            ) VALUES (
                %s, 'queue', 2, 'update_message_flags', '{}'::jsonb, 'delivering'
            )
            """,
            (account_uuid,),
        )
    later_read = _provider_record(account_uuid, project_uuid, kind="read_state.set")
    later_read["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "reader_uuid": str(
            postgres_store.account_resource(account_uuid)["owner_user_uuid"]
        ),
        "message_uuids": [rejected_message_uuid, independent_message_uuid],
        "read": True,
    }
    later_read["operation_sha256"] = canonical.operation_digest(later_read)
    assert postgres_store.enqueue_workspace_delivery(later_read, 0, "queue", 2)

    assert postgres_store.finalize_ready_provider_events() == 0
    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, submission_error_code
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (later_read["record_uuid"],),
        ).fetchone()
    assert delivery == {
        "submission_state": "pending",
        "submission_error_code": None,
    }
    assert postgres_store.pending_workspace_deliveries() == [later_read]


def test_rejected_content_update_does_not_discard_grouped_read(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
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
            "causal_lane": f"chat:{account_uuid}:{stream_uuid}",
            "workspace_delivery_state": "committed",
        },
    )
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state
            ) VALUES
                (%s, 'queue', 1, 'update_message', '{}'::jsonb, 'delivering'),
                (%s, 'queue', 2, 'update_message_flags', '{}'::jsonb, 'delivering')
            """,
            (account_uuid, account_uuid),
        )
    edit = _provider_record(account_uuid, project_uuid, kind="message.update")
    edit["operation"]["entity_uuid"] = message_uuid
    edit["operation"]["provider"]["entity_id"] = "601"
    edit["operation"]["payload"].update(
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
        }
    )
    edit["operation_sha256"] = canonical.operation_digest(edit)
    read = _provider_record(account_uuid, project_uuid, kind="read_state.set")
    read["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "reader_uuid": str(
            postgres_store.account_resource(account_uuid)["owner_user_uuid"]
        ),
        "message_uuids": [message_uuid],
        "read": True,
    }
    read["operation_sha256"] = canonical.operation_digest(read)
    assert postgres_store.enqueue_workspace_delivery(edit, 0, "queue", 1)
    assert postgres_store.enqueue_workspace_delivery(read, 0, "queue", 2)
    assert postgres_store.mark_workspace_delivery_submitting(edit["record_uuid"])
    assert postgres_store.reject_provider_event_submission(
        edit["record_uuid"],
        "provider_api_http_422",
    )

    assert postgres_store.finalize_ready_provider_events() == 1
    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, submission_error_code
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (read["record_uuid"],),
        ).fetchone()
    assert delivery == {
        "submission_state": "pending",
        "submission_error_code": None,
    }
    assert postgres_store.pending_workspace_deliveries() == [read]


def test_later_rejected_move_does_not_discard_earlier_grouped_read(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
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
            "causal_lane": f"chat:{account_uuid}:{stream_uuid}",
            "workspace_delivery_state": "committed",
        },
    )
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_provider_events (
                account_uuid, queue_id, event_id, event_type, body,
                processing_state
            ) VALUES
                (%s, 'queue', 1, 'update_message_flags', '{}'::jsonb, 'delivering'),
                (%s, 'queue', 2, 'update_message', '{}'::jsonb, 'delivering')
            """,
            (account_uuid, account_uuid),
        )
    lane = f"chat:{account_uuid}:{stream_uuid}"
    read = _provider_record(account_uuid, project_uuid, kind="read_state.set")
    read["causal_lane"] = lane
    read["operation"]["entity_uuid"] = stream_uuid
    read["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "reader_uuid": str(
            postgres_store.account_resource(account_uuid)["owner_user_uuid"]
        ),
        "message_uuids": [message_uuid],
        "read": True,
    }
    read["operation_sha256"] = canonical.operation_digest(read)
    move = _provider_record(account_uuid, project_uuid, kind="message.update")
    move["causal_lane"] = lane
    move["operation"]["entity_uuid"] = message_uuid
    move["operation"]["provider"]["entity_id"] = "601"
    move["operation"]["payload"].update(
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": str(uuid.uuid4()),
            "author_uuid": author_uuid,
        }
    )
    move["operation_sha256"] = canonical.operation_digest(move)
    assert postgres_store.enqueue_workspace_delivery(read, 0, "queue", 1)
    assert postgres_store.enqueue_workspace_delivery(move, 0, "queue", 2)
    assert int(read["sequence"]) < int(move["sequence"])

    assert postgres_store.pending_workspace_deliveries() == [read, move]
    assert postgres_store.mark_workspace_delivery_submitting(move["record_uuid"])
    assert postgres_store.reject_provider_event_submission(
        move["record_uuid"],
        "provider_api_http_422",
    )
    assert postgres_store.finalize_ready_provider_events() == 1

    with postgres_store.session() as session:
        delivery = session.execute(
            """
            SELECT submission_state, submission_error_code
            FROM workspace_delivery_outbox
            WHERE record_uuid = %s
            """,
            (read["record_uuid"],),
        ).fetchone()
        events = session.execute(
            """
            SELECT event_id, processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue'
              AND event_id IN (1, 2)
            ORDER BY event_id
            """,
            (account_uuid,),
        ).fetchall()
    assert delivery == {
        "submission_state": "pending",
        "submission_error_code": None,
    }
    assert events == [
        {
            "event_id": 1,
            "processing_state": "delivering",
            "processing_reason": None,
        },
        {
            "event_id": 2,
            "processing_state": "invalid",
            "processing_reason": "workspace_delivery_rejected",
        },
    ]
    assert postgres_store.pending_workspace_deliveries() == [read]


def test_rejection_serializes_stale_grouped_read_preparation(
    postgres_store,
    migrated_postgres_dsn,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    message_uuid = str(uuid.uuid4())
    mapping_lock_key = storage._provider_mapping_lock_key(
        account_uuid,
        "message",
        "601",
    )
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
            "causal_lane": f"chat:{account_uuid}:{stream_uuid}",
            "workspace_delivery_state": "pending",
        },
    )
    create_event = {
        "id": 1,
        "type": "message",
        "message": {"id": 601, "type": "stream", "stream_id": 42},
    }
    flags_event = {
        "id": 2,
        "type": "update_message_flags",
        "messages": [601],
        "flag": "read",
        "op": "add",
    }
    later_event = {
        "id": 3,
        "type": "user_topic",
        "stream_id": 44,
    }
    for event in (create_event, flags_event, later_event):
        assert postgres_store.record_provider_event(
            account_uuid,
            "queue",
            event,
        )

    create = _provider_record(account_uuid, project_uuid)
    create["causal_lane"] = f"chat:{account_uuid}:{stream_uuid}"
    create["operation"]["entity_uuid"] = message_uuid
    create["operation"]["provider"]["entity_id"] = "601"
    create["operation"]["payload"].update(
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
        }
    )
    create["operation_sha256"] = canonical.operation_digest(create)
    prepared_create = postgres_store.prepare_provider_event_records(
        account_uuid,
        "queue",
        1,
        [create],
    )
    postgres_store.enqueue_provider_event_records(
        prepared_create,
        0,
        account_uuid,
        "queue",
        1,
    )
    assert postgres_store.mark_workspace_delivery_submitting(create["record_uuid"])

    stale_read = _provider_record(
        account_uuid,
        project_uuid,
        kind="read_state.set",
    )
    stale_read["causal_lane"] = f"chat:{account_uuid}:{stream_uuid}"
    stale_read["operation"]["entity_uuid"] = stream_uuid
    stale_read["operation"]["provider"]["entity_id"] = None
    stale_read["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "reader_uuid": str(
            postgres_store.account_resource(account_uuid)["owner_user_uuid"]
        ),
        "message_uuids": [message_uuid],
        "read": True,
    }
    stale_read["operation_sha256"] = canonical.operation_digest(stale_read)

    rejection_results = []
    preparation_errors = []
    rejection_failures = []
    preparation_failures = []
    preparation_finished = threading.Event()

    def reject_create():
        try:
            rejection_results.append(
                postgres_store.reject_provider_event_submission(
                    create["record_uuid"],
                    "provider_api_http_422",
                )
            )
        except Exception as exc:  # pragma: no cover - reported below
            rejection_failures.append(exc)

    def prepare_stale_read():
        try:
            with postgres_store.provider_event_lane_guard(
                account_uuid,
                "queue",
                2,
                flags_event,
                "channel:42",
            ) as lane_current:
                assert lane_current
                postgres_store.prepare_provider_event_records(
                    account_uuid,
                    "queue",
                    2,
                    [stale_read],
                )
        except ValueError as exc:
            preparation_errors.append(str(exc))
        except Exception as exc:  # pragma: no cover - reported below
            preparation_failures.append(exc)
        finally:
            preparation_finished.set()

    rejection_thread = threading.Thread(target=reject_create)
    preparation_thread = threading.Thread(target=prepare_stale_read)
    probe_store = storage.RestAlchemyStore(migrated_postgres_dsn)
    mapping_lock_observed = False
    with postgres_store.session() as blocker:
        blocker.execute(
            """
            SELECT event_id
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 1
            FOR UPDATE
            """,
            (account_uuid,),
        ).fetchone()
        rejection_thread.start()
        for _attempt in range(200):
            with probe_store.session() as probe:
                acquired = probe.execute(
                    """
                    SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))
                        AS acquired
                    """,
                    (mapping_lock_key,),
                ).fetchone()["acquired"]
            if not acquired:
                mapping_lock_observed = True
                break
            threading.Event().wait(0.01)
        assert mapping_lock_observed
        preparation_thread.start()
        assert not preparation_finished.wait(timeout=0.1)

    rejection_thread.join(timeout=5)
    preparation_thread.join(timeout=5)
    assert not rejection_thread.is_alive()
    assert not preparation_thread.is_alive()
    assert rejection_failures == []
    assert preparation_failures == []
    assert rejection_results == [True]
    assert preparation_errors == ["provider_message_mapping_changed"]

    assert postgres_store.finalize_ready_provider_events() == 1
    with postgres_store.session() as session:
        state = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
        queued = session.execute(
            """
            SELECT count(*) AS count
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = 'queue'
              AND provider_event_id = 2
            """,
            (account_uuid,),
        ).fetchone()["count"]
    assert state == {
        "processing_state": "pending",
        "processing_reason": None,
        "prepared_records": None,
    }
    assert queued == 0
    assert postgres_store.provider_mapping(account_uuid, "message", "601") is None

    class Adapter:
        server_url = "https://zulip.example.invalid"

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.file_client = None
    instance.provider_adapters = lambda _account_uuid: Adapter()
    instance._queue_event_catalog = lambda *_args, **_kwargs: False
    instance._workspace_delivery_recovery_done = True
    selected = postgres_store.pending_provider_events()
    assert {row["event_id"] for row in selected} == {2, 3}
    assert instance.process_provider_journal(
        [row for row in selected if row["event_id"] == 2]
    ) == 1

    with postgres_store.session() as session:
        grouped = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
    assert grouped == {
        "processing_state": "processed",
        "processing_reason": None,
    }
    assert [
        row["event_id"] for row in postgres_store.pending_provider_events()
    ] == [3]


def test_rejected_create_filters_only_affected_same_topic_grouped_read(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    rejected_message_uuid = str(uuid.uuid4())
    committed_message_uuid = str(uuid.uuid4())
    for provider_id, message_uuid, delivery_state in (
        ("601", rejected_message_uuid, "pending"),
        ("602", committed_message_uuid, "committed"),
    ):
        postgres_store.remember_provider_mapping(
            account_uuid,
            "message",
            provider_id,
            message_uuid,
            {
                "project_uuid": project_uuid,
                "stream_uuid": stream_uuid,
                "topic_uuid": topic_uuid,
                "author_uuid": author_uuid,
                "chat_key": "channel:42",
                "causal_lane": f"chat:{account_uuid}:{stream_uuid}",
                "workspace_delivery_state": delivery_state,
            },
        )
    message_event = {
        "id": 1,
        "type": "message",
        "message": {"type": "stream", "stream_id": 42},
    }
    flags_event = {
        "id": 2,
        "type": "update_message_flags",
        "messages": [601, 602],
        "flag": "read",
        "op": "add",
    }
    assert postgres_store.record_provider_event(account_uuid, "queue", message_event)
    assert postgres_store.record_provider_event(account_uuid, "queue", flags_event)
    create = _provider_record(account_uuid, project_uuid)
    create["causal_lane"] = f"chat:{account_uuid}:{stream_uuid}"
    create["operation"]["entity_uuid"] = rejected_message_uuid
    create["operation"]["provider"]["entity_id"] = "601"
    create["operation"]["payload"].update(
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
        }
    )
    create["operation_sha256"] = canonical.operation_digest(create)
    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        "queue",
        1,
        [create],
    )
    postgres_store.enqueue_provider_event_records(
        prepared,
        0,
        account_uuid,
        "queue",
        1,
    )

    read = _provider_record(account_uuid, project_uuid, kind="read_state.set")
    read["causal_lane"] = f"chat:{account_uuid}:{stream_uuid}"
    read["operation"]["entity_uuid"] = stream_uuid
    read["operation"]["provider"]["entity_id"] = None
    read["operation"]["payload"] = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "reader_uuid": str(
            postgres_store.account_resource(account_uuid)["owner_user_uuid"]
        ),
        "message_uuids": sorted(
            [rejected_message_uuid, committed_message_uuid]
        ),
        "read": True,
    }
    read["operation_sha256"] = canonical.operation_digest(read)
    prepared_read = postgres_store.prepare_provider_event_records(
        account_uuid,
        "queue",
        2,
        [read],
    )
    postgres_store.enqueue_provider_event_records(
        prepared_read,
        0,
        account_uuid,
        "queue",
        2,
    )

    with postgres_store.session() as session:
        original_read = session.execute(
            """
            SELECT record_uuid, operation_uuid, record
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = 'queue'
              AND provider_event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
    assert original_read["record"]["operation"]["payload"]["message_uuids"] == sorted(
        [rejected_message_uuid, committed_message_uuid]
    )
    assert postgres_store.mark_workspace_delivery_submitting(
        create["record_uuid"]
    )
    assert postgres_store.reject_provider_event_submission(
        create["record_uuid"],
        "provider_api_http_422",
    )

    assert postgres_store.finalize_ready_provider_events() == 1
    with postgres_store.session() as session:
        filtered = session.execute(
            """
            SELECT delivery.record_uuid, delivery.operation_uuid,
                   delivery.submission_state, delivery.submission_attempts,
                   delivery.record, operation.operation_sha256
            FROM workspace_delivery_outbox AS delivery
            JOIN operation_idempotency AS operation
              ON operation.operation_uuid = delivery.operation_uuid
            WHERE delivery.account_uuid = %s
              AND delivery.provider_queue_id = 'queue'
              AND delivery.provider_event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
        grouped_event = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
    assert filtered["record_uuid"] == original_read["record_uuid"]
    assert filtered["operation_uuid"] == original_read["operation_uuid"]
    assert filtered["submission_state"] == "pending"
    assert filtered["submission_attempts"] == 0
    assert filtered["record"]["operation"]["payload"]["message_uuids"] == [
        committed_message_uuid
    ]
    assert filtered["operation_sha256"] == filtered["record"]["operation_sha256"]
    assert grouped_event["processing_state"] == "delivering"
    assert grouped_event["processing_reason"] is None
    assert grouped_event["prepared_records"][0] == filtered["record"]
    assert postgres_store.provider_mapping(account_uuid, "message", "601") is None
    assert postgres_store.pending_workspace_deliveries() == [filtered["record"]]

    assert postgres_store.mark_workspace_delivery_submitting(
        filtered["record_uuid"]
    )
    postgres_store.accept_result(_committed_result(filtered["record"]))
    assert postgres_store.finalize_ready_provider_events() == 1
    with postgres_store.session() as session:
        state = session.execute(
            """
            SELECT processing_state, processing_reason
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
    assert state == {"processing_state": "processed", "processing_reason": None}


def test_assignment_reset_grouped_read_snapshot_rebuilds_after_mapping_rejection(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
    )
    rejected_message_uuid = str(uuid.uuid4())
    committed_message_uuid = str(uuid.uuid4())
    for provider_id, message_uuid, delivery_state in (
        ("601", rejected_message_uuid, "pending"),
        ("602", committed_message_uuid, "committed"),
    ):
        postgres_store.remember_provider_mapping(
            account_uuid,
            "message",
            provider_id,
            message_uuid,
            {
                "project_uuid": project_uuid,
                "stream_uuid": stream_uuid,
                "topic_uuid": topic_uuid,
                "author_uuid": author_uuid,
                "chat_key": "channel:42",
                "causal_lane": f"chat:{account_uuid}:{stream_uuid}",
                "workspace_delivery_state": delivery_state,
            },
        )
    create_event = {
        "id": 1,
        "type": "message",
        "message": {"id": 601, "type": "stream", "stream_id": 42},
    }
    flags_event = {
        "id": 2,
        "type": "update_message_flags",
        "messages": [601, 602],
        "flag": "read",
        "op": "add",
    }
    for event in (create_event, flags_event):
        assert postgres_store.record_provider_event(account_uuid, "queue", event)

    create = _provider_record(account_uuid, project_uuid)
    create["causal_lane"] = f"chat:{account_uuid}:{stream_uuid}"
    create["operation"]["entity_uuid"] = rejected_message_uuid
    create["operation"]["provider"]["entity_id"] = "601"
    create["operation"]["payload"].update(
        {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
        }
    )
    create["operation_sha256"] = canonical.operation_digest(create)
    prepared_create = postgres_store.prepare_provider_event_records(
        account_uuid,
        "queue",
        1,
        [create],
    )
    postgres_store.enqueue_provider_event_records(
        prepared_create,
        0,
        account_uuid,
        "queue",
        1,
    )

    stale_read = converter.event_records(
        postgres_store,
        account_uuid,
        "queue",
        flags_event,
    )
    assert len(stale_read) == 1
    assert stale_read[0]["operation"]["payload"]["message_uuids"] == sorted(
        [rejected_message_uuid, committed_message_uuid]
    )
    prepared_read = postgres_store.prepare_provider_event_records(
        account_uuid,
        "queue",
        2,
        stale_read,
    )
    assert prepared_read[0]["sequence"] > 0
    postgres_store.enqueue_provider_event_records(
        prepared_read,
        0,
        account_uuid,
        "queue",
        2,
    )
    assert postgres_store.mark_workspace_delivery_submitting(
        create["record_uuid"]
    )
    assert postgres_store.reject_provider_event_submission(
        create["record_uuid"],
        "provider_api_http_422",
    )
    # A same-project generation change can delete both stale outbox rows before
    # rejected-delivery finalization narrows the grouped read. The source event
    # deliberately keeps its immutable snapshot for an exact replay.
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources
            SET generation = generation + 1
            WHERE resource_type = 'external_chat_assignment'
            """
        )
    assert postgres_store.reset_stale_workspace_deliveries() == 2
    postgres_store.mark_provider_event_invalid(
        account_uuid,
        "queue",
        1,
        "workspace_delivery_rejected",
    )

    class Adapter:
        server_url = "https://zulip.example.invalid"

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.file_client = None
    instance.provider_adapters = lambda _account_uuid: Adapter()
    instance._queue_event_catalog = lambda *_args, **_kwargs: False
    instance._workspace_delivery_recovery_done = True

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]
    assert instance.process_provider_journal(selected) == 0
    with postgres_store.session() as session:
        retry = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records,
                   retry_count
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
        queued = session.execute(
            """
            SELECT count(*) AS count
            FROM workspace_delivery_outbox
            WHERE account_uuid = %s AND provider_queue_id = 'queue'
              AND provider_event_id = 2
            """,
            (account_uuid,),
        ).fetchone()["count"]
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = now() - interval '1 second'
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 2
            """,
            (account_uuid,),
        )
    assert retry["processing_state"] == "pending"
    assert retry["processing_reason"] == "provider_message_mapping_changed"
    assert retry["retry_count"] == 1
    assert len(retry["prepared_records"]) == 1
    assert retry["prepared_records"][0]["operation"]["payload"][
        "message_uuids"
    ] == [committed_message_uuid]
    assert queued == 0

    selected = postgres_store.pending_provider_events()
    assert [row["event_id"] for row in selected] == [2]
    assert instance.process_provider_journal(selected) == 1
    with postgres_store.session() as session:
        rebuilt = session.execute(
            """
            SELECT delivery.record, event.processing_state,
                   event.processing_reason, event.prepared_records
            FROM workspace_delivery_outbox AS delivery
            JOIN zulip_provider_events AS event
              ON event.account_uuid = delivery.account_uuid
             AND event.queue_id = delivery.provider_queue_id
             AND event.event_id = delivery.provider_event_id
            WHERE delivery.account_uuid = %s
              AND delivery.provider_queue_id = 'queue'
              AND delivery.provider_event_id = 2
            """,
            (account_uuid,),
        ).fetchone()
    assert rebuilt["record"]["operation"]["payload"]["message_uuids"] == [
        committed_message_uuid
    ]
    assert rebuilt["processing_state"] == "delivering"
    assert rebuilt["processing_reason"] is None
    assert rebuilt["prepared_records"] == [rebuilt["record"]]

    assert postgres_store.mark_workspace_delivery_submitting(
        rebuilt["record"]["record_uuid"]
    )
    postgres_store.accept_result(_committed_result(rebuilt["record"]))
    assert postgres_store.finalize_ready_provider_events() == 1
    later_event = {
        "id": 3,
        "type": "user_topic",
        "stream_id": 44,
    }
    assert postgres_store.record_provider_event(account_uuid, "queue", later_event)
    assert [
        row["event_id"] for row in postgres_store.pending_provider_events()
    ] == [3]


def test_mapping_retry_preserves_surviving_group_operation_identity(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    first_message_uuid = str(uuid.uuid4())
    second_message_uuid = str(uuid.uuid4())
    first_stream_uuid = str(uuid.uuid4())
    second_stream_uuid = str(uuid.uuid4())
    first_topic_uuid = str(uuid.uuid4())
    second_topic_uuid = str(uuid.uuid4())
    for provider_id, message_uuid, stream_uuid, topic_uuid in (
        ("601", first_message_uuid, first_stream_uuid, first_topic_uuid),
        ("602", second_message_uuid, second_stream_uuid, second_topic_uuid),
    ):
        chat_key = f"channel:{provider_id}"
        _insert_channel_assignment(
            postgres_store,
            account_uuid,
            project_uuid,
            int(provider_id),
        )
        postgres_store.remember_provider_mapping(
            account_uuid,
            "stream",
            chat_key,
            stream_uuid,
            {
                "chat_type": "channel",
                "project_uuid": project_uuid,
                "participants": [],
                "name": f"Channel {provider_id}",
                "description": "",
                "private": True,
                "default_topic_uuid": None,
            },
        )
        postgres_store.remember_provider_mapping(
            account_uuid,
            "message",
            provider_id,
            message_uuid,
            {
                "project_uuid": project_uuid,
                "stream_uuid": stream_uuid,
                "topic_uuid": topic_uuid,
                "chat_key": chat_key,
                "workspace_delivery_state": "committed",
            },
        )
    event = {
        "id": 17,
        "type": "update_message_flags",
        "messages": [601, 602],
        "flag": "read",
        "op": "add",
    }
    assert postgres_store.record_provider_event(account_uuid, "queue", event)

    def read_record(
        message_uuid: str,
        stream_uuid: str,
        topic_uuid: str,
    ) -> dict[str, object]:
        record = _provider_record(
            account_uuid,
            project_uuid,
            kind="read_state.set",
        )
        record["causal_lane"] = f"chat:{account_uuid}:{stream_uuid}"
        operation = record["operation"]
        operation["entity_uuid"] = stream_uuid
        operation["provider"]["entity_id"] = None
        operation["payload"] = {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "reader_uuid": str(
                postgres_store.account_resource(account_uuid)["owner_user_uuid"]
            ),
            "message_uuids": [message_uuid],
            "read": True,
        }
        record["operation_sha256"] = canonical.operation_digest(record)
        return record

    prepared = postgres_store.prepare_provider_event_records(
        account_uuid,
        "queue",
        17,
        [
            read_record(first_message_uuid, first_stream_uuid, first_topic_uuid),
            read_record(second_message_uuid, second_stream_uuid, second_topic_uuid),
        ],
    )
    surviving_record = prepared[1]
    postgres_store.mark_provider_mapping_deleted(account_uuid, "message", "601")
    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        17,
        "provider_message_mapping_changed",
    )
    with postgres_store.session() as session:
        narrowed = session.execute(
            """
            SELECT prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 17
            """,
            (account_uuid,),
        ).fetchone()["prepared_records"]
    assert narrowed == [surviving_record]

    postgres_store.mark_provider_mapping_deleted(account_uuid, "message", "602")
    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        17,
        "provider_message_mapping_changed",
    )
    with postgres_store.session() as session:
        empty = session.execute(
            """
            SELECT prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 17
            """,
            (account_uuid,),
        ).fetchone()["prepared_records"]
    assert empty == []


def test_mapping_retry_quarantines_retired_stream_projection(postgres_store):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    stream_uuid, topic_uuid, author_uuid = _materialize_channel_projection(
        postgres_store,
        account_uuid,
        project_uuid,
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
    event = {
        "id": 18,
        "type": "update_message_flags",
        "messages": [601],
        "flag": "read",
        "op": "add",
    }
    assert postgres_store.record_provider_event(account_uuid, "queue", event)
    prepared = converter.event_records(
        postgres_store,
        account_uuid,
        "queue",
        event,
    )
    assert len(prepared) == 1
    postgres_store.prepare_provider_event_records(
        account_uuid,
        "queue",
        18,
        prepared,
    )
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE provider_mappings
            SET workspace_uuid = %s, updated_at = now()
            WHERE account_uuid = %s AND entity_kind = 'stream'
              AND provider_id = 'channel:42'
            """,
            (str(uuid.uuid4()), account_uuid),
        )

    with pytest.raises(ValueError, match="provider_message_mapping_changed"):
        postgres_store.prepare_provider_event_records(
            account_uuid,
            "queue",
            18,
            [],
        )
    postgres_store.retry_provider_event(
        account_uuid,
        "queue",
        18,
        "provider_message_mapping_changed",
    )
    with postgres_store.session() as session:
        quarantined = session.execute(
            """
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = 'queue' AND event_id = 18
            """,
            (account_uuid,),
        ).fetchone()
    assert quarantined == {
        "processing_state": "invalid",
        "processing_reason": "provider_message_projection_changed",
        "prepared_records": None,
    }


def test_pre_provider_result_crash_retries_same_immutable_record_until_result(
    postgres_store,
):
    account_uuid, project_uuid = _insert_account_and_assignment(postgres_store)
    _enable_zulip_provider(postgres_store)
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
    component = storage.backfill_health_component(account_uuid, "channel:42")
    with postgres_store.session() as session:
        session.execute(
            """
            INSERT INTO zulip_backfill_jobs (
                account_uuid, provider_chat_key, history_depth, state
            ) VALUES (%s, 'channel:42', '30_days', 'cancelled')
            """,
            (account_uuid,),
        )
    postgres_store.mark_health(component, "degraded", "provider_forbidden")

    postgres_store.reconcile_backfill_jobs()

    with postgres_store.session() as session:
        state = session.execute("SELECT state FROM zulip_backfill_jobs").fetchone()[
            "state"
        ]
        health = session.execute(
            "SELECT component FROM bridge_health WHERE component = %s",
            (component,),
        ).fetchone()
    assert state == "pending"
    assert health is None


def test_inactive_account_cancels_backfill_and_clears_only_its_health(
    postgres_store,
):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    component = storage.backfill_health_component(account_uuid, "channel:42")
    unrelated = storage.backfill_health_component(account_uuid, "channel:99")
    postgres_store.mark_health(component, "degraded", "provider_forbidden")
    postgres_store.mark_health(unrelated, "degraded", "provider_forbidden")
    postgres_store.mark_health("provider", "healthy")
    with postgres_store.session() as session:
        session.execute(
            """
            UPDATE desired_resources SET deleted = true
            WHERE resource_type = 'external_account'
              AND resource_uuid = %s
            """,
            (account_uuid,),
        )

    postgres_store.reconcile_backfill_jobs()

    with postgres_store.session() as session:
        state = session.execute(
            """
            SELECT state FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()["state"]
        components = {
            row["component"]
            for row in session.execute("SELECT component FROM bridge_health").fetchall()
        }
    assert state == "cancelled"
    assert components == {"provider", unrelated}


def test_successful_backfill_progress_clears_prior_chat_health(postgres_store):
    account_uuid, _ = _insert_account_and_assignment(postgres_store)
    postgres_store.reconcile_backfill_jobs()
    component = storage.backfill_health_component(account_uuid, "channel:42")
    postgres_store.mark_health(component, "degraded", "provider_forbidden")

    postgres_store.advance_backfill_job(account_uuid, "channel:42", 41, False)

    with postgres_store.session() as session:
        job = session.execute(
            """
            SELECT state, next_anchor FROM zulip_backfill_jobs
            WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
            """,
            (account_uuid,),
        ).fetchone()
        health = session.execute(
            "SELECT component FROM bridge_health WHERE component = %s",
            (component,),
        ).fetchone()
    assert job == {"state": "pending", "next_anchor": 41}
    assert health is None


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
    component = storage.backfill_health_component(account_uuid, "channel:42")
    postgres_store.reconcile_backfill_jobs()
    assert postgres_store.claim_backfill_job() is not None

    postgres_store.fail_backfill_job(
        account_uuid,
        "channel:42",
        "provider_forbidden",
    )
    postgres_store.mark_health(component, "degraded", "provider_forbidden")
    postgres_store.reconcile_backfill_jobs()

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
        health = session.execute(
            """
            SELECT status, safe_error_code FROM bridge_health
            WHERE component = %s
            """,
            (component,),
        ).fetchone()
    assert failed["state"] == "failed"
    assert failed["last_error_code"] == "provider_forbidden"
    assert failed["lease_until"] is None
    assert health == {
        "status": "degraded",
        "safe_error_code": "provider_forbidden",
    }


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
