import importlib.util
import os
import pathlib
import subprocess
import sys
import urllib.parse
import uuid

import pytest
from restalchemy.storage.sql import migrations

from workspace_zulip_bridge import storage

ROOT = pathlib.Path(__file__).parents[2]
MIGRATIONS = ROOT / "migrations"


def _schema_connection_url(connection_url: str, schema: str) -> str:
    base_url, _, raw_query = connection_url.partition("?")
    query = [
        (key, value)
        for key, value in urllib.parse.parse_qsl(
            raw_query, keep_blank_values=True
        )
        if key != "options"
    ]
    query.append(("options", f"-csearch_path={schema}"))
    return f"{base_url}?{urllib.parse.urlencode(query)}"


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


def test_migrations_have_one_versioned_dependency_chain():
    engine = migrations.MigrationEngine(migrations_path=str(MIGRATIONS))

    all_migrations = engine.get_all_migrations()

    assert list(sorted(all_migrations)) == [
        "0000-initialize-bridge-operational-state-18f707.py",
        "0001-add-Zulip-provider-scheduler-state-143113.py",
        "0002-remove-legacy-message-projection-deliveries-e1636f.py",
        "0003-requeue-message-missing-topic-projection-ed8a5e.py",
        "0004-gate-selected-chat-messages-on-participants-23f11f.py",
        "0005-rebuild-message-topic-dependencies-7c52a1.py",
        "0006-index-pending-Workspace-deliveries-c143b4.py",
    ]
    assert engine.get_latest_migration() == (
        "0006-index-pending-Workspace-deliveries-c143b4.py"
    )
    assert len({step["uuid"] for step in all_migrations.values()}) == 7
    assert all_migrations[
        "0001-add-Zulip-provider-scheduler-state-143113.py"
    ]["depends"] == ["0000-initialize-bridge-operational-state-18f707.py"]
    assert all_migrations[
        "0002-remove-legacy-message-projection-deliveries-e1636f.py"
    ]["depends"] == ["0001-add-Zulip-provider-scheduler-state-143113.py"]
    assert all_migrations[
        "0003-requeue-message-missing-topic-projection-ed8a5e.py"
    ]["depends"] == [
        "0002-remove-legacy-message-projection-deliveries-e1636f.py"
    ]
    assert all_migrations[
        "0004-gate-selected-chat-messages-on-participants-23f11f.py"
    ]["depends"] == [
        "0003-requeue-message-missing-topic-projection-ed8a5e.py"
    ]
    assert all_migrations[
        "0005-rebuild-message-topic-dependencies-7c52a1.py"
    ]["depends"] == [
        "0004-gate-selected-chat-messages-on-participants-23f11f.py"
    ]
    assert all_migrations[
        "0006-index-pending-Workspace-deliveries-c143b4.py"
    ]["depends"] == [
        "0005-rebuild-message-topic-dependencies-7c52a1.py"
    ]


def test_restalchemy_migrations_adopt_existing_schema_and_repeat(tmp_path):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_migration_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        with scoped_store.session() as session:
            applied = session.execute(
                "SELECT count(*) AS count FROM ra_migrations WHERE applied"
            ).fetchone()
            indexes = session.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname IN (
                      'workspace_delivery_outbox_pending_order_idx',
                      'workspace_delivery_outbox_pending_dependency_idx'
                  )
                ORDER BY indexname
                """
            ).fetchall()
            assert applied["count"] == 7
            assert [row["indexname"] for row in indexes] == [
                "workspace_delivery_outbox_pending_dependency_idx",
                "workspace_delivery_outbox_pending_order_idx",
            ]
            session.execute("UPDATE bridge_metadata SET control_cursor = 'preserved'")
            session.execute("DROP TABLE ra_migrations")

        _apply_migrations(scoped_url, config_path)
        _apply_migrations(scoped_url, config_path)

        with scoped_store.session() as session:
            applied = session.execute(
                "SELECT count(*) AS count FROM ra_migrations WHERE applied"
            ).fetchone()
            cursor = session.execute(
                "SELECT control_cursor FROM bridge_metadata WHERE singleton"
            ).fetchone()
            assert applied["count"] == 7
            assert cursor["control_cursor"] == "preserved"
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_legacy_message_projection_migration_preserves_real_renames(tmp_path):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_projection_cleanup_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    migration_path = (
        MIGRATIONS / "0002-remove-legacy-message-projection-deliveries-e1636f.py"
    )
    spec = importlib.util.spec_from_file_location("projection_cleanup", migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        account_uuid = str(uuid.uuid4())
        legacy_event_id = 10
        rows = [
            ("stream.upsert", "queue", legacy_event_id),
            ("topic.upsert", "queue", legacy_event_id),
            ("message.create", "queue", legacy_event_id),
            ("topic.upsert", "queue", legacy_event_id + 1),
        ]
        operation_uuids = []
        with scoped_store.session() as session:
            for sequence, (kind, queue_id, event_id) in enumerate(rows, start=1):
                operation_uuid = str(uuid.uuid4())
                operation_uuids.append(operation_uuid)
                session.execute(
                    """
                    INSERT INTO operation_idempotency (
                        operation_uuid, operation_sha256
                    ) VALUES (%s, %s)
                    """,
                    (operation_uuid, "0" * 64),
                )
                session.execute(
                    """
                    INSERT INTO producer_operations (
                        operation_uuid, origin, causal_lane, lane_sequence
                    ) VALUES (%s, 'zulip', 'test', %s)
                    """,
                    (operation_uuid, sequence),
                )
                session.execute(
                    """
                    INSERT INTO workspace_delivery_outbox (
                        record_uuid, operation_uuid, account_uuid,
                        provider_queue_id, provider_event_id, priority, record
                    ) VALUES (%s, %s, %s, %s, %s, 0, %s)
                    """,
                    (
                        str(uuid.uuid4()),
                        operation_uuid,
                        account_uuid,
                        queue_id,
                        event_id,
                        {"operation": {"kind": kind}},
                    ),
                )

            module.migration_step.upgrade(session)
            remaining = session.execute(
                """
                SELECT record->'operation'->>'kind' AS kind, provider_event_id
                FROM workspace_delivery_outbox
                ORDER BY provider_event_id, kind
                """
            ).fetchall()
            retained_operations = session.execute(
                """
                SELECT operation_uuid FROM operation_idempotency
                ORDER BY operation_uuid
                """
            ).fetchall()
            retained_producers = session.execute(
                """
                SELECT operation_uuid FROM producer_operations
                ORDER BY operation_uuid
                """
            ).fetchall()

        assert [(row["kind"], row["provider_event_id"]) for row in remaining] == [
            ("message.create", legacy_event_id),
            ("topic.upsert", legacy_event_id),
            ("topic.upsert", legacy_event_id + 1),
        ]
        expected = {uuid.UUID(value) for value in operation_uuids[1:]}
        assert {row["operation_uuid"] for row in retained_operations} == expected
        assert {row["operation_uuid"] for row in retained_producers} == expected
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_missing_topic_projection_migration_requeues_provider_event(tmp_path):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_topic_requeue_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    migration_path = (
        MIGRATIONS / "0003-requeue-message-missing-topic-projection-ed8a5e.py"
    )
    spec = importlib.util.spec_from_file_location("topic_requeue", migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        account_uuid = str(uuid.uuid4())
        queue_id = "queue"
        missing_topic_event_id = 20
        complete_event_id = 21
        delivery_rows = [
            ("message.create", missing_topic_event_id),
            ("topic.upsert", complete_event_id),
            ("message.create", complete_event_id),
        ]
        operation_uuids = []
        with scoped_store.session() as session:
            for event_id in (missing_topic_event_id, complete_event_id):
                session.execute(
                    """
                    INSERT INTO zulip_provider_events (
                        account_uuid, queue_id, event_id, event_type, body,
                        processing_state
                    ) VALUES (%s, %s, %s, 'message', %s, 'delivering')
                    """,
                    (account_uuid, queue_id, event_id, {}),
                )
            for sequence, (kind, event_id) in enumerate(delivery_rows, start=1):
                operation_uuid = str(uuid.uuid4())
                operation_uuids.append(operation_uuid)
                session.execute(
                    """
                    INSERT INTO operation_idempotency (
                        operation_uuid, operation_sha256
                    ) VALUES (%s, %s)
                    """,
                    (operation_uuid, "0" * 64),
                )
                session.execute(
                    """
                    INSERT INTO producer_operations (
                        operation_uuid, origin, causal_lane, lane_sequence
                    ) VALUES (%s, 'zulip', 'test', %s)
                    """,
                    (operation_uuid, sequence),
                )
                session.execute(
                    """
                    INSERT INTO workspace_delivery_outbox (
                        record_uuid, operation_uuid, account_uuid,
                        provider_queue_id, provider_event_id, priority, record
                    ) VALUES (%s, %s, %s, %s, %s, 0, %s)
                    """,
                    (
                        str(uuid.uuid4()),
                        operation_uuid,
                        account_uuid,
                        queue_id,
                        event_id,
                        {"operation": {"kind": kind}},
                    ),
                )

            module.migration_step.upgrade(session)
            events = session.execute(
                """
                SELECT event_id, processing_state, processing_reason
                FROM zulip_provider_events ORDER BY event_id
                """
            ).fetchall()
            remaining = session.execute(
                """
                SELECT operation_uuid, provider_event_id,
                       record->'operation'->>'kind' AS kind
                FROM workspace_delivery_outbox
                ORDER BY provider_event_id, kind
                """
            ).fetchall()
            retained_operations = session.execute(
                "SELECT operation_uuid FROM operation_idempotency"
            ).fetchall()
            retained_producers = session.execute(
                "SELECT operation_uuid FROM producer_operations"
            ).fetchall()

        assert [
            (row["event_id"], row["processing_state"], row["processing_reason"])
            for row in events
        ] == [
            (
                missing_topic_event_id,
                "pending",
                "missing_topic_projection_requeued",
            ),
            (complete_event_id, "delivering", None),
        ]
        assert [(row["provider_event_id"], row["kind"]) for row in remaining] == [
            (complete_event_id, "message.create"),
            (complete_event_id, "topic.upsert"),
        ]
        expected = {uuid.UUID(value) for value in operation_uuids[1:]}
        assert {row["operation_uuid"] for row in retained_operations} == expected
        assert {row["operation_uuid"] for row in retained_producers} == expected
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
