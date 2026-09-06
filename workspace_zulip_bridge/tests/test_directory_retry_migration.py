"""Adopt directory retry state without rebuilding captures or resetting retries."""

import importlib.util
import os
import shutil
import uuid

import pytest

from workspace_zulip_bridge import storage
from workspace_zulip_bridge.tests import test_migrations as helpers


def test_0047_preserves_existing_directory_and_retry_state_on_repeat(tmp_path):
    connection = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = "cassi_directory_retries_" + uuid.uuid4().hex
    scoped = helpers._schema_connection_url(connection, schema)
    previous = tmp_path / "previous"
    previous.mkdir()
    for path in helpers.MIGRATIONS.glob("*.py"):
        if path.name < "0047-":
            shutil.copy2(path, previous / path.name)
    admin, store = (
        storage.RestAlchemyStore(connection),
        storage.RestAlchemyStore(scoped),
    )
    config = tmp_path / "bridge.conf"
    with admin.session() as session:
        session.execute(f'CREATE SCHEMA "{schema}"')
    try:
        helpers._apply_migrations(scoped, config, previous)
        with store.session() as session:
            project, realm = uuid.uuid4(), uuid.uuid4()
            session.execute(
                "INSERT INTO zulip_history_scopes(project_uuid,provider_realm_uuid,generation,fingerprint,sources,directory_pending,directory_revision,directory_ack_revision,directory_cursor) VALUES (%s,%s,7,%s,'[]'::jsonb,true,5,4,5001)",
                (project, realm, "a" * 64),
            )
            session.execute(
                "INSERT INTO zulip_history_batches(project_uuid,provider_realm_uuid,from_id,to_id,body,import_status) VALUES (%s,%s,5001,10000,'{}'::jsonb,'pending')",
                (project, realm),
            )
            before = dict(
                session.execute("SELECT * FROM zulip_history_scopes").fetchone()
            )
            batch = dict(
                session.execute("SELECT * FROM zulip_history_batches").fetchone()
            )
        helpers._apply_migrations(scoped, config)
        with store.session() as session:
            assert dict(
                session.execute("SELECT * FROM zulip_history_scopes").fetchone()
            ) == {**before, "directory_write_attempts": 0}
            assert (
                dict(session.execute("SELECT * FROM zulip_history_batches").fetchone())
                == batch
            )
            session.execute(
                "UPDATE zulip_history_scopes SET directory_write_attempts=8"
            )
        helpers._apply_migrations(scoped, config)
        path = next(helpers.MIGRATIONS.glob("0047-*.py"))
        spec = importlib.util.spec_from_file_location(
            "cassi_directory_retry_migration", path
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with store.session() as session:
            module.migration_step.upgrade(session)
            module.migration_step.downgrade(session)
            assert dict(
                session.execute("SELECT * FROM zulip_history_scopes").fetchone()
            ) == {**before, "directory_write_attempts": 8}
    finally:
        with admin.session() as session:
            session.execute(f'DROP SCHEMA "{schema}" CASCADE')
