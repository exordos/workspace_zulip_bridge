"""Upgrade legacy capture timeouts without reviving completed or rebuilt work."""

import importlib.util
import json
import os
import shutil
import uuid

import pytest

from workspace_zulip_bridge import storage
from workspace_zulip_bridge.tests import test_migrations as helpers


def test_0046_seeds_outstanding_timeouts_and_preserves_completed_capture(tmp_path):
    connection = os.environ.get("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN")
    if not connection:
        pytest.skip("WORKSPACE_BRIDGE_TEST_POSTGRES_DSN is not configured")
    schema = "cassi_capture_timeout_" + uuid.uuid4().hex
    scoped = helpers._schema_connection_url(connection, schema)
    previous = tmp_path / "previous"
    previous.mkdir()
    for path in helpers.MIGRATIONS.glob("*.py"):
        if path.name < "0046-":
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
            for kind in ("job", "health", "work", "complete", "cancelled", "rebuilt"):
                account, project, realm = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
                session.execute(
                    "INSERT INTO zulip_backfill_jobs(account_uuid,provider_chat_key,history_depth,state,retry_count,last_error_code) VALUES (%s,%s,'all',%s,8,%s)",
                    (
                        account,
                        kind,
                        kind
                        if kind in {"complete", "cancelled"}
                        else "failed"
                        if kind == "job"
                        else "pending",
                        "history_capture_write_timeout"
                        if kind in {"job", "complete", "cancelled"}
                        else "provider_unavailable",
                    ),
                )
                session.execute(
                    "INSERT INTO zulip_history_scopes(project_uuid,provider_realm_uuid,generation,fingerprint,sources) VALUES (%s,%s,10,%s,%s::jsonb)",
                    (
                        project,
                        realm,
                        "a" * 64,
                        json.dumps(
                            [{"account_uuid": str(account), "provider_chat_key": kind}]
                        ),
                    ),
                )
                if kind in {"job", "work", "rebuilt"}:
                    session.execute(
                        "INSERT INTO zulip_history_failure_reports(project_uuid,provider_realm_uuid,generation,safe_error_code) VALUES (%s,%s,%s,'history_capture_write_timeout')",
                        (project, realm, 10 if kind == "work" else 9),
                    )
                if kind == "health":
                    store.mark_health(
                        storage.backfill_health_component(str(account), kind),
                        "degraded",
                        "history_capture_write_timeout",
                    )
            before = session.execute(
                "SELECT * FROM zulip_backfill_jobs ORDER BY provider_chat_key"
            ).fetchall()
        helpers._apply_migrations(scoped, config)
        helpers._apply_migrations(scoped, config)
        with store.session() as session:
            jobs = session.execute(
                "SELECT * FROM zulip_backfill_jobs ORDER BY provider_chat_key"
            ).fetchall()
            assert [
                dict(
                    row,
                    capture_timeout_pending=row["provider_chat_key"]
                    in {"job", "health", "work"},
                )
                for row in before
            ] == jobs
            work = session.execute(
                "SELECT source.provider_chat_key,report.generation FROM zulip_history_failure_reports AS report JOIN zulip_history_scopes AS scope USING(project_uuid,provider_realm_uuid) CROSS JOIN LATERAL jsonb_to_recordset(scope.sources) AS source(provider_chat_key text)"
            ).fetchall()
            assert {row["provider_chat_key"]: row["generation"] for row in work} == {
                "job": 10,
                "health": 10,
                "work": 10,
                "rebuilt": 9,
            }
            # Re-adopting an existing column must not re-seed a marker cleared
            # by a successful partial save before its report cursor is retired.
            session.execute(
                "UPDATE zulip_backfill_jobs SET capture_timeout_pending=false WHERE provider_chat_key='health'"
            )
            path = next(helpers.MIGRATIONS.glob("0046-*.py"))
            spec = importlib.util.spec_from_file_location(
                "cassi_timeout_migration", path
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.migration_step.upgrade(session)
            assert not session.execute(
                "SELECT capture_timeout_pending FROM zulip_backfill_jobs WHERE provider_chat_key='health'"
            ).fetchone()["capture_timeout_pending"]
    finally:
        with admin.session() as session:
            session.execute(f'DROP SCHEMA "{schema}" CASCADE')
