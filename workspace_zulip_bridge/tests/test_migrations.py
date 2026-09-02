import importlib.util
import json
import os
import pathlib
import shutil
import subprocess
import sys
import urllib.parse
import uuid

import pytest
from restalchemy.storage.sql import migrations

from workspace_zulip_bridge import canonical, storage

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


def _apply_migrations(
    connection_url: str,
    config_path: pathlib.Path,
    migrations_path: pathlib.Path = MIGRATIONS,
) -> None:
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
            str(migrations_path),
        ],
        check=False,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr


def test_semantic_report_indexes_upgrade_an_applied_archive_chain(tmp_path):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_archive_upgrade_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    archive_migrations = tmp_path / "archive-migrations"
    archive_migrations.mkdir()
    for migration_path in MIGRATIONS.glob("*.py"):
        if migration_path.name not in {
            "0024-index-observed-report-semantic-order-6ecddb.py",
            "0025-bound-provider-result-delivery-state-3cd8cf.py",
            "0026-compare-observed-report-timestamps-chronologically-00f58f.py",
            "0027-track-Workspace-projection-resets-a627d4.py",
            "0028-track-private-catalog-scan-generation-670844.py",
            "0029-replay-exact-provider-read-snapshots-72f1cf.py",
            "0030-converge-shared-realm-message-mappings-75632b.py",
            "0031-replay-versioned-provider-read-snapshots-724065.py",
            "0032-replay-exact-provider-read-snapshots-v3-797e62.py",
            "0033-replay-versioned-provider-snapshots-v4-d87fa7.py",
            "0034-rekey-pending-authoritative-message-dependents-dc6abe.py",
            "0035-Replay-provider-read-snapshots-after-owner-state-repair-e4b510.py",
            "0036-replay-canonical-provider-quotes-6ea4c2.py",
            "0037-replay-independent-provider-read-snapshots-ae38ad.py",
            "0038-replay-history-with-final-unread-snapshot-05224a.py",
        }:
            shutil.copy2(migration_path, archive_migrations / migration_path.name)
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path, archive_migrations)
        with scoped_store.session() as session:
            applied = session.execute(
                "SELECT count(*) AS count FROM ra_migrations WHERE applied"
            ).fetchone()
            archive_indexes = session.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname IN (
                      'provider_mappings_reaction_provider_prefix_idx',
                      'observed_report_outbox_resource_latest_idx'
                  )
                ORDER BY indexname
                """
            ).fetchall()
        assert applied["count"] == 24
        assert [row["indexname"] for row in archive_indexes] == [
            "observed_report_outbox_resource_latest_idx",
            "provider_mappings_reaction_provider_prefix_idx",
        ]

        _apply_migrations(scoped_url, config_path)
        with scoped_store.session() as session:
            applied = session.execute(
                "SELECT count(*) AS count FROM ra_migrations WHERE applied"
            ).fetchone()
            indexes = session.execute(
                """
                SELECT indexname, indexdef FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname IN (
                      'observed_report_outbox_resource_latest_idx',
                      'observed_report_outbox_resource_observed_idx',
                      'observed_report_outbox_catalog_readiness_idx',
                      'observed_report_outbox_terminal_history_idx',
                      'provider_mappings_reaction_provider_prefix_idx',
                      'bridge_operations_result_record_uuid_idx',
                      'bridge_operations_pending_result_idx',
                      'bridge_operations_terminal_read_retention_idx'
                  )
                ORDER BY indexname
                """
            ).fetchall()
            result_record_uuid = session.execute(
                """
                SELECT is_generated, generation_expression
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'bridge_operations'
                  AND column_name = 'result_record_uuid'
                """
            ).fetchone()
        assert applied["count"] == 39
        assert [row["indexname"] for row in indexes] == [
            "bridge_operations_pending_result_idx",
            "bridge_operations_result_record_uuid_idx",
            "bridge_operations_terminal_read_retention_idx",
            "observed_report_outbox_catalog_readiness_idx",
            "observed_report_outbox_resource_observed_idx",
            "observed_report_outbox_terminal_history_idx",
            "provider_mappings_reaction_provider_prefix_idx",
        ]
        catalog_index = indexes[3]["indexdef"]
        assert "workspace_bridge_observed_at" in catalog_index
        assert "body ->> 'observed_at'" in catalog_index
        assert "COALESCE" in catalog_index
        assert "report_uuid DESC" in catalog_index
        assert result_record_uuid is None
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_projection_reset_upgrade_forces_snapshot_after_old_bridge_consumed_change(
    tmp_path,
):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_projection_reset_upgrade_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    old_bridge_migrations = tmp_path / "old-bridge-migrations"
    old_bridge_migrations.mkdir()
    for migration_path in MIGRATIONS.glob("*.py"):
        if migration_path.name not in {
            "0027-track-Workspace-projection-resets-a627d4.py",
            "0028-track-private-catalog-scan-generation-670844.py",
            "0029-replay-exact-provider-read-snapshots-72f1cf.py",
            "0030-converge-shared-realm-message-mappings-75632b.py",
            "0031-replay-versioned-provider-read-snapshots-724065.py",
            "0032-replay-exact-provider-read-snapshots-v3-797e62.py",
            "0033-replay-versioned-provider-snapshots-v4-d87fa7.py",
            "0034-rekey-pending-authoritative-message-dependents-dc6abe.py",
            "0035-Replay-provider-read-snapshots-after-owner-state-repair-e4b510.py",
            "0036-replay-canonical-provider-quotes-6ea4c2.py",
            "0037-replay-independent-provider-read-snapshots-ae38ad.py",
            "0038-replay-history-with-final-unread-snapshot-05224a.py",
        }:
            shutil.copy2(migration_path, old_bridge_migrations / migration_path.name)
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    account_uuid = str(uuid.uuid4())
    message_uuid = str(uuid.uuid4())

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path, old_bridge_migrations)
        with scoped_store.session() as session:
            session.execute(
                """
                UPDATE bridge_metadata
                SET control_cursor = 'cursor-after-reset-change'
                WHERE singleton
                """
            )
            session.execute(
                """
                INSERT INTO desired_resources (
                    resource_type, resource_uuid, generation, body, deleted
                ) VALUES (
                    'external_account', %s, 2,
                    jsonb_build_object(
                        'resource_type', 'external_account',
                        'uuid', %s::text,
                        'generation', 2,
                        'projection_reset_generation', 1
                    ),
                    false
                )
                """,
                (account_uuid, account_uuid),
            )
            session.execute(
                """
                INSERT INTO scheduler_accounts (
                    account_uuid, provider_generation
                ) VALUES (%s, 2)
                """,
                (account_uuid,),
            )
            session.execute(
                """
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id
                ) VALUES (%s, 'message', %s, '42')
                """,
                (account_uuid, message_uuid),
            )

        _apply_migrations(scoped_url, config_path)
        with scoped_store.session() as session:
            migrated = session.execute(
                """
                SELECT metadata.control_cursor,
                       account.projection_reset_generation,
                       desired.body->>'projection_reset_generation'
                           AS desired_reset_generation
                FROM bridge_metadata AS metadata
                JOIN scheduler_accounts AS account
                  ON account.account_uuid = %s
                JOIN desired_resources AS desired
                  ON desired.resource_type = 'external_account'
                 AND desired.resource_uuid = account.account_uuid
                WHERE metadata.singleton
                """,
                (account_uuid,),
            ).fetchone()
        assert migrated == {
            "control_cursor": "",
            "projection_reset_generation": 0,
            "desired_reset_generation": "1",
        }

        scoped_store.install_snapshot(
            [
                {
                    "resource_type": "external_account",
                    "uuid": account_uuid,
                    "generation": 2,
                    "projection_reset_generation": 1,
                    "required_capabilities": {},
                }
            ],
            "snapshot-after-upgrade",
        )
        with scoped_store.session() as session:
            reconciled = session.execute(
                """
                SELECT metadata.control_cursor,
                       account.projection_reset_generation,
                       (
                           SELECT count(*)
                           FROM provider_mappings
                           WHERE account_uuid = account.account_uuid
                             AND entity_kind = 'message'
                       ) AS stale_message_mappings
                FROM bridge_metadata AS metadata
                JOIN scheduler_accounts AS account
                  ON account.account_uuid = %s
                WHERE metadata.singleton
                """,
                (account_uuid,),
            ).fetchone()
        assert reconciled == {
            "control_cursor": "snapshot-after-upgrade",
            "projection_reset_generation": 1,
            "stale_message_mappings": 0,
        }
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


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
        "0019-fair-provider-journal-scheduling-5e3926.py",
        "0020-refresh-Zulip-notification-queues-93df4e.py",
        "0021-index-pending-chat-materializations-dcdd12.py",
        "0022-isolate-provider-journal-causal-lanes-3ae83f.py",
        "0023-index-reaction-provider-prefixes-dbc736.py",
        "0024-index-observed-report-semantic-order-6ecddb.py",
        "0025-bound-provider-result-delivery-state-3cd8cf.py",
        "0026-compare-observed-report-timestamps-chronologically-00f58f.py",
        "0027-track-Workspace-projection-resets-a627d4.py",
        "0028-track-private-catalog-scan-generation-670844.py",
        "0029-replay-exact-provider-read-snapshots-72f1cf.py",
        "0030-converge-shared-realm-message-mappings-75632b.py",
        "0031-replay-versioned-provider-read-snapshots-724065.py",
        "0032-replay-exact-provider-read-snapshots-v3-797e62.py",
        "0033-replay-versioned-provider-snapshots-v4-d87fa7.py",
        "0034-rekey-pending-authoritative-message-dependents-dc6abe.py",
        "0035-Replay-provider-read-snapshots-after-owner-state-repair-e4b510.py",
        "0036-replay-canonical-provider-quotes-6ea4c2.py",
        "0037-replay-independent-provider-read-snapshots-ae38ad.py",
        "0038-replay-history-with-final-unread-snapshot-05224a.py",
    ]
    assert engine.get_latest_migration() == (
        "0038-replay-history-with-final-unread-snapshot-05224a.py"
    )
    assert len({step["uuid"] for step in all_migrations.values()}) == 39
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
    assert all_migrations["0015-scale-large-synchronizations-ad12e8.py"]["depends"] == [
        "0014-bound-terminal-delivery-retention-4c61bd.py"
    ]
    assert all_migrations["0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py"][
        "depends"
    ] == ["0015-scale-large-synchronizations-ad12e8.py"]
    assert all_migrations["0017-persist-provider-account-circuit-breaker-e875bc.py"][
        "depends"
    ] == ["0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py"]
    assert all_migrations["0018-persist-reaction-assignment-context-372258.py"][
        "depends"
    ] == ["0017-persist-provider-account-circuit-breaker-e875bc.py"]
    assert all_migrations["0019-fair-provider-journal-scheduling-5e3926.py"][
        "depends"
    ] == ["0018-persist-reaction-assignment-context-372258.py"]
    assert all_migrations["0020-refresh-Zulip-notification-queues-93df4e.py"][
        "depends"
    ] == ["0019-fair-provider-journal-scheduling-5e3926.py"]
    assert all_migrations["0021-index-pending-chat-materializations-dcdd12.py"][
        "depends"
    ] == ["0020-refresh-Zulip-notification-queues-93df4e.py"]
    assert all_migrations["0022-isolate-provider-journal-causal-lanes-3ae83f.py"][
        "depends"
    ] == ["0021-index-pending-chat-materializations-dcdd12.py"]
    assert all_migrations["0023-index-reaction-provider-prefixes-dbc736.py"][
        "depends"
    ] == ["0022-isolate-provider-journal-causal-lanes-3ae83f.py"]
    assert all_migrations["0024-index-observed-report-semantic-order-6ecddb.py"][
        "depends"
    ] == ["0023-index-reaction-provider-prefixes-dbc736.py"]
    assert all_migrations["0025-bound-provider-result-delivery-state-3cd8cf.py"][
        "depends"
    ] == ["0024-index-observed-report-semantic-order-6ecddb.py"]
    assert all_migrations[
        "0026-compare-observed-report-timestamps-chronologically-00f58f.py"
    ]["depends"] == ["0025-bound-provider-result-delivery-state-3cd8cf.py"]
    assert all_migrations["0027-track-Workspace-projection-resets-a627d4.py"][
        "depends"
    ] == ["0026-compare-observed-report-timestamps-chronologically-00f58f.py"]
    assert all_migrations["0028-track-private-catalog-scan-generation-670844.py"][
        "depends"
    ] == ["0027-track-Workspace-projection-resets-a627d4.py"]
    assert all_migrations["0029-replay-exact-provider-read-snapshots-72f1cf.py"][
        "depends"
    ] == ["0028-track-private-catalog-scan-generation-670844.py"]
    assert all_migrations["0030-converge-shared-realm-message-mappings-75632b.py"][
        "depends"
    ] == ["0029-replay-exact-provider-read-snapshots-72f1cf.py"]
    assert all_migrations["0031-replay-versioned-provider-read-snapshots-724065.py"][
        "depends"
    ] == ["0030-converge-shared-realm-message-mappings-75632b.py"]
    assert all_migrations["0032-replay-exact-provider-read-snapshots-v3-797e62.py"][
        "depends"
    ] == ["0031-replay-versioned-provider-read-snapshots-724065.py"]
    assert all_migrations["0033-replay-versioned-provider-snapshots-v4-d87fa7.py"][
        "depends"
    ] == ["0032-replay-exact-provider-read-snapshots-v3-797e62.py"]
    assert all_migrations[
        "0034-rekey-pending-authoritative-message-dependents-dc6abe.py"
    ]["depends"] == ["0033-replay-versioned-provider-snapshots-v4-d87fa7.py"]
    assert all_migrations[
        "0035-Replay-provider-read-snapshots-after-owner-state-repair-e4b510.py"
    ]["depends"] == ["0034-rekey-pending-authoritative-message-dependents-dc6abe.py"]
    assert all_migrations["0036-replay-canonical-provider-quotes-6ea4c2.py"][
        "depends"
    ] == ["0035-Replay-provider-read-snapshots-after-owner-state-repair-e4b510.py"]
    assert all_migrations["0037-replay-independent-provider-read-snapshots-ae38ad.py"][
        "depends"
    ] == ["0036-replay-canonical-provider-quotes-6ea4c2.py"]
    assert all_migrations["0038-replay-history-with-final-unread-snapshot-05224a.py"][
        "depends"
    ] == ["0037-replay-independent-provider-read-snapshots-ae38ad.py"]


def test_pending_authoritative_message_dependency_migration_rekeys_alias(
    tmp_path,
):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_authoritative_dependency_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    migration_path = (
        MIGRATIONS / "0034-rekey-pending-authoritative-message-dependents-dc6abe.py"
    )
    spec = importlib.util.spec_from_file_location(
        "pending_authoritative_message_dependents",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    account_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    provisional_message_uuid = str(uuid.uuid4())
    canonical_message_uuid = str(uuid.uuid4())
    operation_uuid = str(uuid.uuid4())
    actor_uuid = str(uuid.uuid4())
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
        "causal_lane": f"chat:{account_uuid}:channel:42",
        "sequence": 1,
        "predecessor_operation_uuid": None,
        "created_at": "2026-09-01T00:00:00Z",
        "expires_at": None,
        "operation": {
            "kind": "reaction.upsert",
            "entity_uuid": str(uuid.uuid4()),
            "actor_uuid": actor_uuid,
            "occurred_at": "2026-09-01T00:00:00Z",
            "provider": {
                "kind": "zulip",
                "chat_id": "channel:42",
                "entity_id": "101:2:unicode_emoji:1f44d",
                "revision": None,
            },
            "payload": {
                "stream_uuid": stream_uuid,
                "topic_uuid": topic_uuid,
                "message_uuid": provisional_message_uuid,
                "user_uuid": actor_uuid,
                "emoji_name": "thumbs_up",
            },
            "extensions": {"provider_badge": "zulip"},
        },
    }
    record["operation_sha256"] = canonical.operation_digest(record)
    original_digest = record["operation_sha256"]

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        with scoped_store.session() as session:
            session.execute(
                """
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    metadata, deleted
                ) VALUES (
                    %s, 'message', %s, '101',
                    jsonb_build_object(
                        'workspace_delivery_state', 'committed'
                    ), false
                )
                """,
                (account_uuid, canonical_message_uuid),
            )
            session.execute(
                """
                INSERT INTO provider_mapping_aliases (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    metadata, deleted
                ) VALUES (
                    %s, 'message', %s, '101',
                    jsonb_build_object(
                        'workspace_delivery_state', 'committed'
                    ), false
                )
                """,
                (account_uuid, provisional_message_uuid),
            )
            session.execute(
                """
                INSERT INTO operation_idempotency (
                    operation_uuid, operation_sha256
                ) VALUES (%s, %s)
                """,
                (operation_uuid, original_digest),
            )
            session.execute(
                """
                INSERT INTO workspace_delivery_outbox (
                    record_uuid, operation_uuid, account_uuid, priority, record
                ) VALUES (%s, %s, %s, 2, %s)
                """,
                (
                    record["record_uuid"],
                    operation_uuid,
                    account_uuid,
                    json.dumps(record),
                ),
            )

            module.migration_step.upgrade(session)
            module.migration_step.upgrade(session)
            repaired = session.execute(
                """
                SELECT delivery.record, idempotency.operation_sha256
                FROM workspace_delivery_outbox AS delivery
                JOIN operation_idempotency AS idempotency
                  ON idempotency.operation_uuid = delivery.operation_uuid
                WHERE delivery.operation_uuid = %s
                """,
                (operation_uuid,),
            ).fetchone()

        assert repaired["record"]["operation"]["payload"]["message_uuid"] == (
            canonical_message_uuid
        )
        assert repaired["record"]["operation_sha256"] != original_digest
        assert repaired["record"]["operation_sha256"] == (
            canonical.operation_digest(repaired["record"])
        )
        assert repaired["operation_sha256"] == repaired["record"]["operation_sha256"]
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_reaction_provider_prefix_migration_adds_pattern_index():
    migration_path = MIGRATIONS / "0023-index-reaction-provider-prefixes-dbc736.py"
    spec = importlib.util.spec_from_file_location(
        "reaction_provider_prefix", migration_path
    )
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
    assert "provider_mappings_reaction_provider_prefix_idx" in statement
    assert "provider_id text_pattern_ops" in statement
    assert "WHERE entity_kind = 'reaction'" in statement

    module.migration_step.downgrade(session)
    assert len(session.statements) == 2
    assert "DROP INDEX IF EXISTS" in session.statements[1]


def test_observed_report_order_migration_adds_semantic_latest_index():
    migration_path = MIGRATIONS / "0024-index-observed-report-semantic-order-6ecddb.py"
    spec = importlib.util.spec_from_file_location(
        "observed_report_order", migration_path
    )
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

    statement = session.statements[0]
    assert "observed_report_outbox_resource_observed_idx" in statement
    assert "observed_generation" in statement
    assert "body->>'observed_at'" in statement
    assert "DESC NULLS LAST" in statement
    assert "created_at DESC" in statement
    assert "report_uuid DESC" in statement

    module.migration_step.downgrade(session)
    assert "DROP INDEX IF EXISTS" in session.statements[1]


def test_observed_report_timestamp_migration_adds_chronological_indexes():
    migration_path = (
        MIGRATIONS / "0026-compare-observed-report-timestamps-chronologically-00f58f.py"
    )
    spec = importlib.util.spec_from_file_location(
        "observed_report_timestamp", migration_path
    )
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

    statement = "\n".join(session.statements)
    assert "workspace_bridge_observed_at(value text)" in statement
    assert "RETURNS timestamptz" in statement
    assert "CREATE INDEX CONCURRENTLY" in statement
    assert "COALESCE" in statement
    assert "body->>'observed_at'" in statement
    assert statement.count("created_at DESC") >= 2
    assert statement.index(
        "DROP INDEX CONCURRENTLY IF EXISTS "
        "observed_report_outbox_resource_chronological_idx"
    ) < statement.index(
        "CREATE INDEX CONCURRENTLY\n"
        "                observed_report_outbox_resource_chronological_idx"
    )
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS" not in statement

    module.migration_step.downgrade(session)
    downgrade = "\n".join(session.statements)
    assert "DROP INDEX CONCURRENTLY" in downgrade
    assert "DROP FUNCTION IF EXISTS workspace_bridge_observed_at" in downgrade


def test_provider_result_indexes_rebuild_interrupted_concurrent_remnants():
    migration_path = MIGRATIONS / "0025-bound-provider-result-delivery-state-3cd8cf.py"
    spec = importlib.util.spec_from_file_location(
        "provider_result_delivery_state", migration_path
    )
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

    statement = "\n".join(session.statements)
    for index_name in (
        "bridge_operations_result_record_uuid_idx",
        "bridge_operations_pending_result_idx",
        "bridge_operations_terminal_read_retention_idx",
    ):
        assert statement.index(
            f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}"
        ) < statement.index(f"CREATE INDEX CONCURRENTLY\n                {index_name}")
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS" not in statement
    assert "NOT manual_reconciliation_required" in statement
    assert "provider_result_stale_lease" in statement


def test_provider_journal_lane_migration_backfills_and_indexes_scheduler():
    migration_path = MIGRATIONS / "0022-isolate-provider-journal-causal-lanes-3ae83f.py"
    spec = importlib.util.spec_from_file_location(
        "provider_journal_causal_lanes", migration_path
    )
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
    assert "ADD COLUMN IF NOT EXISTS causal_lane text" in statement
    assert "event_type = 'user_topic'" in statement
    assert "scoped_stream_events" in statement
    assert "CREATE TABLE IF NOT EXISTS scheduler_provider_event_lanes" in statement
    assert "zulip_provider_events_lane_head_idx" in statement
    assert "zulip_provider_events_global_head_idx" in statement
    assert "zulip_provider_message_context_inflight_idx" in statement

    module.migration_step.downgrade(session)
    assert len(session.statements) == 2


def test_notification_queue_refresh_preserves_live_message_gap():
    migration_path = MIGRATIONS / "0020-refresh-Zulip-notification-queues-93df4e.py"
    spec = importlib.util.spec_from_file_location(
        "notification_queue_refresh", migration_path
    )
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

    assert len(session.statements) == 2
    catchup, queue_reset = session.statements
    assert "INSERT INTO zulip_queue_catchup_jobs" in catchup
    assert "checkpoint_provider_message_id" in catchup
    assert "ON CONFLICT (account_uuid, provider_chat_key) DO UPDATE" in catchup
    assert queue_reset == "DELETE FROM zulip_event_cursors"

    module.migration_step.downgrade(session)
    assert len(session.statements) == 2


def test_notification_queue_refresh_seeds_catchup_before_cursor_reset(tmp_path):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_notification_queue_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    migration_path = MIGRATIONS / "0020-refresh-Zulip-notification-queues-93df4e.py"
    spec = importlib.util.spec_from_file_location(
        "notification_queue_refresh_integration", migration_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    account_uuid = str(uuid.uuid4())

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
                    jsonb_build_object(
                        'settings', jsonb_build_object('selection_mode', 'all')
                    ), false
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
                (account_uuid, str(uuid.uuid4()), account_uuid),
            )
            session.execute(
                """
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid,
                    provider_id, metadata
                ) VALUES
                    (%s, 'message', %s, '100', '{"chat_key":"channel:42"}'),
                    (%s, 'message', %s, '125', '{"chat_key":"channel:42"}')
                """,
                (account_uuid, str(uuid.uuid4()), account_uuid, str(uuid.uuid4())),
            )
            session.execute(
                """
                INSERT INTO zulip_event_cursors (
                    account_uuid, queue_id, last_event_id
                ) VALUES (%s, 'old-notification-queue', 77)
                """,
                (account_uuid,),
            )
            module.migration_step.upgrade(session)
            catchup = session.execute(
                """
                SELECT checkpoint_provider_message_id, state
                FROM zulip_queue_catchup_jobs
                WHERE account_uuid = %s AND provider_chat_key = 'channel:42'
                """,
                (account_uuid,),
            ).fetchone()
            cursors = session.execute(
                "SELECT count(*) AS count FROM zulip_event_cursors"
            ).fetchone()

        assert catchup == {
            "checkpoint_provider_message_id": 125,
            "state": "pending",
        }
        assert cursors["count"] == 0
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_fair_provider_journal_migration_persists_account_dispatch_state():
    migration_path = MIGRATIONS / "0019-fair-provider-journal-scheduling-5e3926.py"
    spec = importlib.util.spec_from_file_location(
        "fair_provider_journal", migration_path
    )
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
    assert "last_provider_event_dispatched_at timestamptz" in statement
    assert "SELECT DISTINCT account_uuid FROM zulip_provider_events" in statement
    assert "zulip_provider_events_account_head_idx" in statement
    assert "INCLUDE (processing_state, available_at)" in statement
    assert "processing_state IN ('pending', 'delivering')" in statement

    module.migration_step.downgrade(session)

    assert len(session.statements) == 2
    downgrade = session.statements[1]
    assert "zulip_provider_events_account_head_idx" in downgrade
    assert "INCLUDE (available_at)" in downgrade
    assert "WHERE processing_state = 'pending'" in downgrade
    assert "DROP COLUMN IF EXISTS last_provider_event_dispatched_at" in downgrade


def test_fair_provider_journal_migration_backfills_existing_accounts(tmp_path):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_fair_journal_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    account_uuid = str(uuid.uuid4())
    migration_path = MIGRATIONS / "0019-fair-provider-journal-scheduling-5e3926.py"
    spec = importlib.util.spec_from_file_location(
        "fair_provider_journal_backfill", migration_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        with scoped_store.session() as session:
            session.execute(
                """
                INSERT INTO zulip_provider_events (
                    account_uuid, queue_id, event_id, event_type, body
                ) VALUES (%s, 'legacy-queue', 1, 'realm_user', '{}'::jsonb)
                """,
                (account_uuid,),
            )
            module.migration_step.upgrade(session)
            journal = session.execute(
                """
                SELECT last_provider_event_dispatched_at
                FROM scheduler_accounts WHERE account_uuid = %s
                """,
                (account_uuid,),
            ).fetchone()

        assert journal == {"last_provider_event_dispatched_at": None}
        pending = scoped_store.pending_provider_events(limit=20)
        assert [str(row["account_uuid"]) for row in pending] == [account_uuid]
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_reaction_assignment_context_migration_is_persisted():
    migration_path = MIGRATIONS / "0018-persist-reaction-assignment-context-372258.py"
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
    migration_path = MIGRATIONS / "0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py"
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
    migration_path = MIGRATIONS / "0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py"
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


@pytest.mark.parametrize(
    "migration_name",
    [
        "0029-replay-exact-provider-read-snapshots-72f1cf.py",
        "0031-replay-versioned-provider-read-snapshots-724065.py",
        "0032-replay-exact-provider-read-snapshots-v3-797e62.py",
        "0033-replay-versioned-provider-snapshots-v4-d87fa7.py",
        "0035-Replay-provider-read-snapshots-after-owner-state-repair-e4b510.py",
        "0036-replay-canonical-provider-quotes-6ea4c2.py",
        "0037-replay-independent-provider-read-snapshots-ae38ad.py",
        "0038-replay-history-with-final-unread-snapshot-05224a.py",
    ],
)
def test_exact_read_snapshot_migration_requeues_only_selected_live_history(
    tmp_path, migration_name
):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_exact_read_replay_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    migration_path = MIGRATIONS / migration_name
    spec = importlib.util.spec_from_file_location(
        f"exact_read_replay_{migration_name[:4]}", migration_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    account_uuid = str(uuid.uuid4())
    disabled_account_uuid = str(uuid.uuid4())

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        with scoped_store.session() as session:
            session.execute(
                """
                INSERT INTO desired_resources (
                    resource_type, resource_uuid, generation, body, deleted
                ) VALUES
                ('external_account', %s, 1, jsonb_build_object(
                    'uuid', %s::text, 'synchronization_enabled', true
                ), false),
                ('external_account', %s, 1, jsonb_build_object(
                    'uuid', %s::text, 'synchronization_enabled', false
                ), false),
                ('external_chat_assignment', %s, 1, jsonb_build_object(
                    'external_account_uuid', %s::text, 'selected', true,
                    'provider_chat', jsonb_build_object(
                        'provider_chat_key', 'channel:42'
                    )
                ), false),
                ('external_chat_assignment', %s, 1, jsonb_build_object(
                    'external_account_uuid', %s::text, 'selected', false,
                    'provider_chat', jsonb_build_object(
                        'provider_chat_key', 'channel:43'
                    )
                ), false),
                ('external_chat_assignment', %s, 1, jsonb_build_object(
                    'external_account_uuid', %s::text, 'selected', true,
                    'provider_chat', jsonb_build_object(
                        'provider_chat_key', 'channel:44'
                    )
                ), false)
                """,
                (
                    account_uuid,
                    account_uuid,
                    disabled_account_uuid,
                    disabled_account_uuid,
                    str(uuid.uuid4()),
                    account_uuid,
                    str(uuid.uuid4()),
                    account_uuid,
                    str(uuid.uuid4()),
                    disabled_account_uuid,
                ),
            )
            session.execute(
                """
                INSERT INTO zulip_backfill_jobs (
                    account_uuid, provider_chat_key, history_depth, state,
                    next_anchor, retry_count, last_error_code
                ) VALUES
                (%s, 'channel:42', 'all', 'complete', 99, 3, 'old_error'),
                (%s, 'channel:43', 'all', 'complete', 99, 3, 'old_error'),
                (%s, 'channel:44', 'all', 'complete', 99, 3, 'old_error')
                """,
                (account_uuid, account_uuid, disabled_account_uuid),
            )
            module.migration_step.upgrade(session)
            jobs = session.execute(
                """
                SELECT provider_chat_key, state, next_anchor, retry_count,
                       last_error_code
                FROM zulip_backfill_jobs ORDER BY provider_chat_key
                """
            ).fetchall()

        assert jobs == [
            {
                "provider_chat_key": "channel:42",
                "state": "pending",
                "next_anchor": None,
                "retry_count": 0,
                "last_error_code": None,
            },
            {
                "provider_chat_key": "channel:43",
                "state": "complete",
                "next_anchor": 99,
                "retry_count": 3,
                "last_error_code": "old_error",
            },
            {
                "provider_chat_key": "channel:44",
                "state": "complete",
                "next_anchor": 99,
                "retry_count": 3,
                "last_error_code": "old_error",
            },
        ]
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_shared_realm_convergence_requeues_and_removes_only_unattempted_reactions(
    tmp_path,
):
    connection_url = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection_url:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = f"bridge_shared_realm_convergence_{uuid.uuid4().hex}"
    scoped_url = _schema_connection_url(connection_url, schema)
    config_path = tmp_path / "bridge.conf"
    admin_store = storage.RestAlchemyStore(connection_url)
    scoped_store = storage.RestAlchemyStore(scoped_url)
    migration_path = (
        MIGRATIONS / "0030-converge-shared-realm-message-mappings-75632b.py"
    )
    spec = importlib.util.spec_from_file_location(
        "shared_realm_convergence", migration_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    source_account_uuid = str(uuid.uuid4())
    target_account_uuid = str(uuid.uuid4())
    project_uuid = str(uuid.uuid4())
    realm_uuid = str(uuid.uuid4())
    canonical_message_uuid = str(uuid.uuid4())
    provisional_message_uuid = str(uuid.uuid4())
    pending_operation_uuid = str(uuid.uuid4())
    attempted_operation_uuid = str(uuid.uuid4())

    with admin_store.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        _apply_migrations(scoped_url, config_path)
        with scoped_store.session() as session:
            for account_uuid in (source_account_uuid, target_account_uuid):
                session.execute(
                    """
                    INSERT INTO desired_resources (
                        resource_type, resource_uuid, generation, body
                    ) VALUES (
                        'external_account', %s, 1,
                        jsonb_build_object(
                            'uuid', %s::text,
                            'synchronization_enabled', true
                        )
                    )
                    """,
                    (account_uuid, account_uuid),
                )
                session.execute(
                    """
                    INSERT INTO desired_resources (
                        resource_type, resource_uuid, generation, body
                    ) VALUES (
                        'external_chat_assignment', %s, 1,
                        jsonb_build_object(
                            'external_account_uuid', %s::text,
                            'project_id', %s::text,
                            'selected', true,
                            'provider_chat', jsonb_build_object(
                                'provider_chat_key', 'channel:42'
                            )
                        )
                    )
                    """,
                    (str(uuid.uuid4()), account_uuid, project_uuid),
                )
                session.execute(
                    """
                    INSERT INTO zulip_event_cursors (
                        account_uuid, queue_id, last_event_id,
                        provider_realm_uuid, provider_owner_user_id,
                        provider_account_generation
                    ) VALUES (%s, %s, 1, %s, %s, 1)
                    """,
                    (account_uuid, f"queue-{account_uuid}", realm_uuid, "1"),
                )
                session.execute(
                    """
                    INSERT INTO zulip_backfill_jobs (
                        account_uuid, provider_chat_key, history_depth,
                        state, next_anchor, retry_count, last_error_code
                    ) VALUES (
                        %s, 'channel:42', 'all', 'complete', 99, 3, 'old_error'
                    )
                    """,
                    (account_uuid,),
                )

            session.execute(
                """
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    metadata
                ) VALUES
                (%s, 'message', %s, '601', jsonb_build_object(
                    'project_uuid', %s::text,
                    'chat_key', 'channel:42',
                    'workspace_delivery_state', 'committed'
                )),
                (%s, 'message', %s, '601', jsonb_build_object(
                    'project_uuid', %s::text,
                    'chat_key', 'channel:42',
                    'workspace_delivery_state', 'pending'
                ))
                """,
                (
                    source_account_uuid,
                    canonical_message_uuid,
                    project_uuid,
                    target_account_uuid,
                    provisional_message_uuid,
                    project_uuid,
                ),
            )
            for operation_uuid, submission_attempts in (
                (pending_operation_uuid, 0),
                (attempted_operation_uuid, 1),
            ):
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
                    INSERT INTO workspace_delivery_outbox (
                        record_uuid, operation_uuid, account_uuid,
                        submission_state, submission_attempts, priority, record
                    ) VALUES (
                        %s, %s, %s, 'pending', %s, 0,
                        jsonb_build_object(
                            'operation', jsonb_build_object(
                                'kind', 'reaction.upsert',
                                'payload', jsonb_build_object(
                                    'message_uuid', %s::text
                                )
                            )
                        )
                    )
                    """,
                    (
                        str(uuid.uuid4()),
                        operation_uuid,
                        target_account_uuid,
                        submission_attempts,
                        provisional_message_uuid,
                    ),
                )

            module.migration_step.upgrade(session)
            deliveries = session.execute(
                """
                SELECT operation_uuid, submission_attempts
                FROM workspace_delivery_outbox ORDER BY operation_uuid
                """
            ).fetchall()
            idempotency = session.execute(
                """
                SELECT operation_uuid FROM operation_idempotency
                ORDER BY operation_uuid
                """
            ).fetchall()
            jobs = session.execute(
                """
                SELECT account_uuid, state, next_anchor, retry_count,
                       last_error_code
                FROM zulip_backfill_jobs ORDER BY account_uuid
                """
            ).fetchall()
            mapping_index = session.execute(
                """
                SELECT indexdef FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'provider_mappings_message_provider_id_idx'
                """
            ).fetchone()

        assert deliveries == [
            {
                "operation_uuid": uuid.UUID(attempted_operation_uuid),
                "submission_attempts": 1,
            }
        ]
        assert idempotency == [{"operation_uuid": uuid.UUID(attempted_operation_uuid)}]
        assert jobs == [
            {
                "account_uuid": uuid.UUID(account_uuid),
                "state": "pending",
                "next_anchor": None,
                "retry_count": 0,
                "last_error_code": None,
            }
            for account_uuid in sorted((source_account_uuid, target_account_uuid))
        ]
        assert mapping_index is not None
        assert "(provider_id, account_uuid)" in mapping_index["indexdef"]
        assert "entity_kind = 'message'" in mapping_index["indexdef"]
        assert "NOT deleted" in mapping_index["indexdef"]
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
                      'observed_report_outbox_resource_observed_idx',
                      'observed_report_outbox_terminal_history_idx',
                      'observed_report_outbox_pending_order_idx',
                      'observed_report_outbox_chat_materialization_idx',
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
                      'scheduler_accounts_provider_ready_idx',
                      'zulip_provider_events_account_head_idx',
                      'zulip_provider_events_lane_head_idx',
                      'zulip_provider_events_global_head_idx',
                      'zulip_provider_message_context_inflight_idx',
                      'provider_mappings_reaction_provider_prefix_idx'
                  )
                ORDER BY indexname
                """
            ).fetchall()
            account_head_index = session.execute(
                """
                SELECT indexdef FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname = 'zulip_provider_events_account_head_idx'
                """
            ).fetchone()
            private_catalog_marker = session.execute(
                """
                SELECT data_type FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'zulip_event_cursors'
                  AND column_name = 'private_catalog_scanned_generation'
                """
            ).fetchone()
            assert applied["count"] == 39
            assert private_catalog_marker == {"data_type": "bigint"}
            assert [row["indexname"] for row in indexes] == [
                "bridge_operations_active_local_echo_idx",
                "desired_resources_assignment_chat_idx",
                "desired_resources_selected_assignment_account_idx",
                "observed_report_outbox_catalog_readiness_idx",
                "observed_report_outbox_chat_materialization_idx",
                "observed_report_outbox_pending_order_idx",
                "observed_report_outbox_resource_observed_idx",
                "observed_report_outbox_terminal_history_idx",
                "provider_mappings_reaction_provider_prefix_idx",
                "scheduler_accounts_provider_ready_idx",
                "workspace_delivery_outbox_initial_backfill_idx",
                "workspace_delivery_outbox_pending_dependency_idx",
                "workspace_delivery_outbox_pending_order_idx",
                "workspace_delivery_outbox_provider_event_pending_idx",
                "workspace_delivery_outbox_sent_at_idx",
                "zulip_backfill_jobs_account_claim_idx",
                "zulip_participant_sync_account_claim_idx",
                "zulip_provider_events_account_head_idx",
                "zulip_provider_events_global_head_idx",
                "zulip_provider_events_lane_head_idx",
                "zulip_provider_events_pending_order_idx",
                "zulip_provider_events_terminal_created_idx",
                "zulip_provider_message_context_inflight_idx",
                "zulip_provider_message_events_inflight_idx",
                "zulip_provider_message_events_local_echo_idx",
            ]
            assert account_head_index is not None
            assert (
                "INCLUDE (processing_state, available_at)"
                in account_head_index["indexdef"]
            )
            assert "processing_state = ANY" in account_head_index["indexdef"]
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
            assert applied["count"] == 39
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
