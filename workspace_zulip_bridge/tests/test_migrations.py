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
        for key, value in urllib.parse.parse_qsl(raw_query, keep_blank_values=True)
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
        "0007-persist-Zulip-provider-identity-c721d9.py",
        "0008-refresh-Zulip-reaction-queues-c511aa.py",
        "0009-index-observed-reports-d6d013.py",
        "0010-prepare-provider-event-records-f970c8.py",
        "0011-quarantine-rejected-provider-events-f1169c.py",
        "0012-optimize-bridge-load-and-reconcile-stale-queues-6c9ddc.py",
        "0013-bound-reaction-history-window-5edf75.py",
        "0014-bound-terminal-delivery-retention-4c61bd.py",
        "0015-scale-large-synchronizations-ad12e8.py",
        "0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py",
        "0017-persist-provider-account-circuit-breaker-e875bc.py",
        "0018-persist-reaction-assignment-context-372258.py",
    ]
    assert engine.get_latest_migration() == (
        "0018-persist-reaction-assignment-context-372258.py"
    )
    assert len({step["uuid"] for step in all_migrations.values()}) == 19
    assert all_migrations["0001-add-Zulip-provider-scheduler-state-143113.py"][
        "depends"
    ] == ["0000-initialize-bridge-operational-state-18f707.py"]
    assert all_migrations["0002-remove-legacy-message-projection-deliveries-e1636f.py"][
        "depends"
    ] == ["0001-add-Zulip-provider-scheduler-state-143113.py"]
    assert all_migrations["0003-requeue-message-missing-topic-projection-ed8a5e.py"][
        "depends"
    ] == ["0002-remove-legacy-message-projection-deliveries-e1636f.py"]
    assert all_migrations["0004-gate-selected-chat-messages-on-participants-23f11f.py"][
        "depends"
    ] == ["0003-requeue-message-missing-topic-projection-ed8a5e.py"]
    assert all_migrations["0005-rebuild-message-topic-dependencies-7c52a1.py"][
        "depends"
    ] == ["0004-gate-selected-chat-messages-on-participants-23f11f.py"]
    assert all_migrations["0006-index-pending-Workspace-deliveries-c143b4.py"][
        "depends"
    ] == ["0005-rebuild-message-topic-dependencies-7c52a1.py"]
    assert all_migrations["0007-persist-Zulip-provider-identity-c721d9.py"][
        "depends"
    ] == ["0006-index-pending-Workspace-deliveries-c143b4.py"]
    assert all_migrations["0008-refresh-Zulip-reaction-queues-c511aa.py"][
        "depends"
    ] == ["0007-persist-Zulip-provider-identity-c721d9.py"]
    assert all_migrations["0009-index-observed-reports-d6d013.py"]["depends"] == [
        "0008-refresh-Zulip-reaction-queues-c511aa.py"
    ]
    assert all_migrations["0010-prepare-provider-event-records-f970c8.py"][
        "depends"
    ] == ["0009-index-observed-reports-d6d013.py"]
    assert all_migrations["0011-quarantine-rejected-provider-events-f1169c.py"][
        "depends"
    ] == ["0010-prepare-provider-event-records-f970c8.py"]
    assert all_migrations[
        "0012-optimize-bridge-load-and-reconcile-stale-queues-6c9ddc.py"
    ]["depends"] == ["0011-quarantine-rejected-provider-events-f1169c.py"]
    assert all_migrations["0013-bound-reaction-history-window-5edf75.py"][
        "depends"
    ] == ["0012-optimize-bridge-load-and-reconcile-stale-queues-6c9ddc.py"]
    assert all_migrations["0014-bound-terminal-delivery-retention-4c61bd.py"][
        "depends"
    ] == ["0013-bound-reaction-history-window-5edf75.py"]
    assert all_migrations["0015-scale-large-synchronizations-ad12e8.py"][
        "depends"
    ] == ["0014-bound-terminal-delivery-retention-4c61bd.py"]
    assert all_migrations[
        "0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py"
    ]["depends"] == ["0015-scale-large-synchronizations-ad12e8.py"]
    assert all_migrations[
        "0017-persist-provider-account-circuit-breaker-e875bc.py"
    ]["depends"] == ["0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py"]
    assert all_migrations[
        "0018-persist-reaction-assignment-context-372258.py"
    ]["depends"] == ["0017-persist-provider-account-circuit-breaker-e875bc.py"]


def test_reaction_assignment_context_migration_is_persisted():
    migration_path = (
        MIGRATIONS / "0018-persist-reaction-assignment-context-372258.py"
    )
    spec = importlib.util.spec_from_file_location("reaction_context", migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Session:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(statement)

    session = Session()
    module.migration_step.upgrade(session)

    assert len(session.statements) == 1
    statement = session.statements[0]
    assert "assignment_pending_since timestamptz" in statement
    assert "assignment_catalog_reported_at timestamptz" in statement
    assert "provider_message_context jsonb" in statement
    assert "jsonb_typeof(provider_message_context) = 'object'" in statement


def test_provider_account_breaker_migration_is_persisted_and_generation_bound():
    migration_path = (
        MIGRATIONS / "0017-persist-provider-account-circuit-breaker-e875bc.py"
    )
    spec = importlib.util.spec_from_file_location("provider_breaker", migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Session:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(statement)

    session = Session()
    module.migration_step.upgrade(session)

    assert len(session.statements) == 1
    statement = session.statements[0]
    assert "provider_generation bigint" in statement
    assert "provider_state IN ('ready', 'backoff', 'auth_required')" in statement
    assert "provider_retry_after timestamptz" in statement
    assert "scheduler_accounts_provider_ready_idx" in statement
    assert "last_error_code IN ('unauthorized', 'unauthorized_account')" in statement


def test_reaction_emoji_migration_requeues_configured_history():
    migration_path = (
        MIGRATIONS / "0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py"
    )
    spec = importlib.util.spec_from_file_location("reaction_emoji", migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Session:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(statement)

    session = Session()
    module.migration_step.upgrade(session)

    assert len(session.statements) == 1
    statement = session.statements[0]
    assert "UPDATE zulip_backfill_jobs" in statement
    assert "state = 'pending'" in statement
    assert "THEN COALESCE(job.cutoff_at, assignment.updated_at)" in statement
    assert "retry_count = 0" in statement


def test_reaction_emoji_migration_replays_new_history_to_original_cutoff(tmp_path):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_reaction_emoji_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    migration_path = (
        MIGRATIONS / "0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py"
    )
    spec = importlib.util.spec_from_file_location("reaction_emoji", migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    account_uuid = str(uuid.uuid4())
    assignment_uuid = str(uuid.uuid4())

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        with scoped_store.session() as session:
            session.execute(
                """
                INSERT INTO desired_resources (
                    resource_type, resource_uuid, generation, body, deleted
                ) VALUES (
                    'external_account', %s, 1,
                    jsonb_build_object('uuid', %s::text), false
                ), (
                    'external_chat_assignment', %s, 1,
                    jsonb_build_object(
                        'external_account_uuid', %s::text,
                        'selected', true,
                        'provider_chat', jsonb_build_object(
                            'provider_chat_key', 'channel:42'
                        )
                    ), false
                )
                """,
                (account_uuid, account_uuid, assignment_uuid, account_uuid),
            )
            assignment = session.execute(
                """
                SELECT updated_at FROM desired_resources
                WHERE resource_uuid = %s
                """,
                (assignment_uuid,),
            ).fetchone()
            session.execute(
                """
                INSERT INTO zulip_backfill_jobs (
                    account_uuid, provider_chat_key, history_depth, state,
                    next_anchor, cutoff_at
                ) VALUES (%s, 'channel:42', 'new', 'complete', 99, NULL)
                """,
                (account_uuid,),
            )
            module.migration_step.upgrade(session)
            job = session.execute(
                """
                SELECT state, next_anchor, cutoff_at
                FROM zulip_backfill_jobs
                WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
                """,
                (account_uuid,),
            ).fetchone()

        assert job["state"] == "pending"
        assert job["next_anchor"] is None
        assert job["cutoff_at"] == assignment["updated_at"]
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


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
                      'workspace_delivery_outbox_pending_dependency_idx',
                      'observed_report_outbox_resource_latest_idx',
                      'observed_report_outbox_pending_order_idx',
                      'observed_report_outbox_catalog_readiness_idx',
                      'desired_resources_selected_assignment_account_idx',
                      'workspace_delivery_outbox_initial_backfill_idx',
                      'zulip_provider_events_pending_order_idx',
                      'workspace_delivery_outbox_provider_event_pending_idx',
                      'zulip_provider_message_events_inflight_idx',
                      'zulip_provider_message_events_local_echo_idx',
                      'bridge_operations_active_local_echo_idx',
                      'workspace_delivery_outbox_sent_at_idx',
                      'zulip_provider_events_terminal_created_idx',
                      'desired_resources_assignment_chat_idx',
                      'zulip_participant_sync_account_claim_idx',
                      'zulip_backfill_jobs_account_claim_idx',
                      'scheduler_accounts_provider_ready_idx'
                  )
                ORDER BY indexname
                """
            ).fetchall()
            assert applied["count"] == 19
            assert [row["indexname"] for row in indexes] == [
                "bridge_operations_active_local_echo_idx",
                "desired_resources_assignment_chat_idx",
                "desired_resources_selected_assignment_account_idx",
                "observed_report_outbox_catalog_readiness_idx",
                "observed_report_outbox_pending_order_idx",
                "observed_report_outbox_resource_latest_idx",
                "scheduler_accounts_provider_ready_idx",
                "workspace_delivery_outbox_initial_backfill_idx",
                "workspace_delivery_outbox_pending_dependency_idx",
                "workspace_delivery_outbox_pending_order_idx",
                "workspace_delivery_outbox_provider_event_pending_idx",
                "workspace_delivery_outbox_sent_at_idx",
                "zulip_backfill_jobs_account_claim_idx",
                "zulip_participant_sync_account_claim_idx",
                "zulip_provider_events_pending_order_idx",
                "zulip_provider_events_terminal_created_idx",
                "zulip_provider_message_events_inflight_idx",
                "zulip_provider_message_events_local_echo_idx",
            ]
            session.execute("UPDATE bridge_metadata SET control_cursor = 'preserved'")
            session.execute(
                """
                INSERT INTO zulip_event_cursors (
                    account_uuid, queue_id, last_event_id
                ) VALUES (%s, 'legacy-reaction-queue', 42)
                """,
                (str(uuid.uuid4()),),
            )
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
            provider_cursor_count = session.execute(
                "SELECT count(*) AS count FROM zulip_event_cursors"
            ).fetchone()
            assert applied["count"] == 19
            assert cursor["control_cursor"] == "preserved"
            assert provider_cursor_count["count"] == 0
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_reaction_history_migration_repairs_new_cutoff_idempotently(tmp_path):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_reaction_history_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    migration_path = MIGRATIONS / "0013-bound-reaction-history-window-5edf75.py"
    spec = importlib.util.spec_from_file_location("reaction_history", migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    account_uuid = str(uuid.uuid4())
    legacy_boundary = "2026-05-01 00:00:00+00"

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        with scoped_store.session() as session:
            session.execute("DROP INDEX bridge_operations_active_local_echo_idx")
            session.execute("DROP INDEX zulip_provider_message_events_local_echo_idx")
            session.execute("DROP INDEX zulip_provider_message_events_inflight_idx")
            session.execute(
                """
                INSERT INTO zulip_backfill_jobs (
                    account_uuid, provider_chat_key, history_depth,
                    cutoff_at, state, updated_at
                ) VALUES (
                    %s, 'channel:42', 'new', NULL, 'complete', %s
                )
                """,
                (account_uuid, legacy_boundary),
            )

            module.migration_step.upgrade(session)
            module.migration_step.upgrade(session)

            repaired = session.execute(
                """
                SELECT cutoff_at, updated_at FROM zulip_backfill_jobs
                WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
                """,
                (account_uuid,),
            ).fetchone()
            indexes = session.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname IN (
                      'zulip_provider_message_events_inflight_idx',
                      'zulip_provider_message_events_local_echo_idx',
                      'bridge_operations_active_local_echo_idx'
                  )
                ORDER BY indexname
                """
            ).fetchall()
            assert repaired["cutoff_at"] == repaired["updated_at"]
            assert [row["indexname"] for row in indexes] == [
                "bridge_operations_active_local_echo_idx",
                "zulip_provider_message_events_inflight_idx",
                "zulip_provider_message_events_local_echo_idx",
            ]

            module.migration_step.downgrade(session)
            downgraded = session.execute(
                """
                SELECT cutoff_at FROM zulip_backfill_jobs
                WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
                """,
                (account_uuid,),
            ).fetchone()
            dropped = session.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname IN (
                      'zulip_provider_message_events_inflight_idx',
                      'zulip_provider_message_events_local_echo_idx',
                      'bridge_operations_active_local_echo_idx'
                  )
                """
            ).fetchall()
            assert downgraded["cutoff_at"] is None
            assert dropped == []
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_load_optimization_migration_reconciles_stale_queues_idempotently(tmp_path):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_load_optimization_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    migration_path = (
        MIGRATIONS / "0012-optimize-bridge-load-and-reconcile-stale-queues-6c9ddc.py"
    )
    spec = importlib.util.spec_from_file_location("load_optimization", migration_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        active_account_uuid = str(uuid.uuid4())
        inactive_account_uuid = str(uuid.uuid4())
        active_operation_uuid = str(uuid.uuid4())
        inactive_operation_uuid = str(uuid.uuid4())
        with scoped_store.session() as session:
            session.execute(
                """
                INSERT INTO desired_resources (
                    resource_type, resource_uuid, generation, body
                ) VALUES (
                    'external_account', %s, 1,
                    jsonb_build_object('synchronization_enabled', true)
                )
                """,
                (active_account_uuid,),
            )
            session.execute(
                """
                INSERT INTO workspace_delivery_outbox (
                    record_uuid, operation_uuid, account_uuid,
                    account_generation, submission_state,
                    next_submission_at, priority, record
                ) VALUES (
                    %s, %s, %s, 1, 'awaiting_result',
                    TIMESTAMPTZ '2126-01-01 00:00:00+00', 0, '{}'::jsonb
                )
                """,
                (str(uuid.uuid4()), active_operation_uuid, active_account_uuid),
            )
            session.execute(
                """
                INSERT INTO zulip_provider_events (
                    account_uuid, queue_id, event_id, event_type, body,
                    processing_state, prepared_records
                ) VALUES (
                    %s, 'retired-queue', 7, 'message', '{}'::jsonb,
                    'delivering', '[]'::jsonb
                )
                """,
                (inactive_account_uuid,),
            )
            session.execute(
                """
                INSERT INTO workspace_delivery_outbox (
                    record_uuid, operation_uuid, account_uuid,
                    account_generation, provider_queue_id, provider_event_id,
                    submission_state, priority, record
                ) VALUES (
                    %s, %s, %s, 1, 'retired-queue', 7,
                    'ambiguous', 0, '{}'::jsonb
                )
                """,
                (
                    str(uuid.uuid4()),
                    inactive_operation_uuid,
                    inactive_account_uuid,
                ),
            )

            module.migration_step.upgrade(session)
            module.migration_step.upgrade(session)

            active_delivery = session.execute(
                """
                SELECT submission_state, next_submission_at <= now() AS due
                FROM workspace_delivery_outbox
                WHERE operation_uuid = %s
                """,
                (active_operation_uuid,),
            ).fetchone()
            inactive_delivery = session.execute(
                """
                SELECT submission_state, submission_error_code
                FROM workspace_delivery_outbox
                WHERE operation_uuid = %s
                """,
                (inactive_operation_uuid,),
            ).fetchone()
            inactive_event = session.execute(
                """
                SELECT processing_state, processing_reason, prepared_records
                FROM zulip_provider_events
                WHERE account_uuid = %s AND queue_id = 'retired-queue'
                  AND event_id = 7
                """,
                (inactive_account_uuid,),
            ).fetchone()

        assert active_delivery == {"submission_state": "ambiguous", "due": True}
        assert inactive_delivery == {
            "submission_state": "cancelled",
            "submission_error_code": "account_inactive",
        }
        assert inactive_event == {
            "processing_state": "ignored",
            "processing_reason": "account_inactive",
            "prepared_records": None,
        }
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
