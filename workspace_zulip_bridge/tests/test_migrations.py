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
    ]
    assert engine.get_latest_migration() == (
        "0001-add-Zulip-provider-scheduler-state-143113.py"
    )
    assert len({step["uuid"] for step in all_migrations.values()}) == 2
    assert all_migrations[
        "0001-add-Zulip-provider-scheduler-state-143113.py"
    ]["depends"] == ["0000-initialize-bridge-operational-state-18f707.py"]


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
            assert applied["count"] == 2
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
            assert applied["count"] == 2
            assert cursor["control_cursor"] == "preserved"
    finally:
        with admin_store.session() as session:
            session.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
