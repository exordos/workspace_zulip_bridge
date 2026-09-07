"""Directory SQL budgets and terminal file observer authorization on PostgreSQL."""

import base64
import concurrent.futures
import contextlib
import random
import types
import uuid

import httpx
import pytest

from workspace_zulip_bridge import (
    history,
    history_configuration,
    history_delivery,
    service,
    storage,
    zulip_adapter,
)
from workspace_zulip_bridge.tests import test_history as unit
from workspace_zulip_bridge.tests import test_postgres_integration as pg

migrated_postgres_dsn = pg.migrated_postgres_dsn
postgres_store = pg.postgres_store


def captured(store, batch_factory=None):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    account = pg._history_source(store, 17, realm, project)
    job = store.claim_backfill_job()
    batch = batch_factory() if batch_factory else pg._history_job_batch(job, 17)
    assert store.save_history_batch(job, batch, None, True)
    return account


def directory_service(store, account):
    with store.session() as session:
        history_configuration.invalidate_directory(session, account)
    instance = object.__new__(service.BridgeService)
    instance.store = store
    instance.provider_adapters = lambda _: types.SimpleNamespace(
        history_users=lambda **_: [{"user_id": 17, "full_name": "Updated user"}]
    )
    return instance


def snapshot(store):
    with store.session() as session:
        return dict(
            session.execute(
                "SELECT directory_account_cursor,directory_cursor,directory_pending,directory_retry_at FROM zulip_history_scopes"
            ).fetchone()
        )


@pytest.mark.parametrize("blocked", ["control", "scope", "batch", "provider_error"])
def test_directory_lock_contention_yields_without_advancing_checkpoint(
    postgres_store, blocked
):
    instance = directory_service(postgres_store, captured(postgres_store))
    if blocked == "batch":
        assert instance._refresh_history_directory_once()
    if blocked == "provider_error":

        def fail(**_):
            raise zulip_adapter.ZulipOperationError("provider_unavailable", True)

        instance.provider_adapters = lambda _: types.SimpleNamespace(history_users=fail)
    before = snapshot(postgres_store)
    query = {
        "control": "LOCK TABLE desired_resources IN ROW EXCLUSIVE MODE",
        "scope": "SELECT 1 FROM zulip_history_scopes FOR SHARE",
        "batch": "SELECT 1 FROM zulip_history_batches FOR UPDATE",
        "provider_error": "SELECT 1 FROM zulip_history_scopes FOR UPDATE",
    }[blocked]
    with concurrent.futures.ThreadPoolExecutor() as pool:
        with postgres_store.session() as blocker:
            blocker.execute(query)
            pending = pool.submit(instance._refresh_history_directory_once)
            # The lock stays held until this result: a missing lock budget hangs.
            assert pending.result(timeout=3) is False
    after = snapshot(postgres_store)
    if blocked == "batch":
        assert after.pop("directory_retry_at") > before.pop("directory_retry_at")
        assert (
            postgres_store.health()[0]["safe_error_code"]
            == "history_directory_database_contention"
        )
    assert after == before
    with postgres_store.session() as session:
        session.execute("SET LOCAL lock_timeout='100ms'")
        session.execute("UPDATE desired_resources SET generation=generation")


@contextlib.contextmanager
def write_trigger(store, table, column, action):
    name = "cassi_directory_budget_" + uuid.uuid4().hex
    with store.session() as session:
        session.execute(
            f"CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {action}; RETURN NEW; END; $$"
        )
        session.execute(
            f"CREATE TRIGGER {name} BEFORE UPDATE OF {column} ON {table} FOR EACH ROW EXECUTE FUNCTION {name}()"
        )
    try:
        yield
    finally:
        with store.session() as session:
            session.execute(f"DROP TRIGGER {name} ON {table}")
            session.execute(f"DROP FUNCTION {name}()")


@pytest.mark.parametrize("phase", ["catalog", "body"])
def test_directory_statement_timeout_rolls_back_and_can_retry(
    postgres_store, monkeypatch, phase
):
    instance = directory_service(postgres_store, captured(postgres_store))
    if phase == "body":
        assert instance._refresh_history_directory_once()
    before = snapshot(postgres_store)
    with postgres_store.session() as session:
        body = session.execute("SELECT body FROM zulip_history_batches").fetchone()[
            "body"
        ]
    original = history.capture_write_budget_ms
    monkeypatch.setattr(history, "capture_write_budget_ms", lambda _: 20)
    table, column = (
        ("zulip_history_scopes", "directory_users")
        if phase == "catalog"
        else ("zulip_history_batches", "body")
    )
    with write_trigger(postgres_store, table, column, "PERFORM pg_sleep(0.1)"):
        assert instance._refresh_history_directory_once() is False
    after = snapshot(postgres_store)
    assert after.pop("directory_retry_at") > before.pop("directory_retry_at")
    assert after == before
    with postgres_store.session() as session:
        assert (
            session.execute("SELECT body FROM zulip_history_batches").fetchone()["body"]
            == body
        )
    monkeypatch.setattr(history, "capture_write_budget_ms", original)
    with postgres_store.session() as session:
        session.execute("UPDATE zulip_history_scopes SET directory_retry_at=now()")
    assert instance._refresh_history_directory_once()
    assert snapshot(postgres_store) != before


def test_large_directory_body_write_uses_size_budget_and_restores_checkpoint_budget(
    postgres_store,
):
    def large_batch():
        rng = random.Random(18)
        messages = [
            {
                **unit.source_message(message_id=i),
                "content": base64.b64encode(rng.randbytes(7500)).decode(),
            }
            for i in range(1, 5001)
        ]
        return history.make_batch(
            history.HistoryRange(1, 5000, messages), unit.source_users(), 17
        )

    instance = directory_service(postgres_store, captured(postgres_store, large_batch))
    assert instance._refresh_history_directory_once()
    body_check = """IF current_setting('statement_timeout')::interval < interval '3500 milliseconds'
        OR current_setting('statement_timeout')::interval > interval '5 seconds'
        OR current_setting('lock_timeout') <> '50ms' THEN
        RAISE EXCEPTION 'Unexpected directory body budget'; END IF"""
    checkpoint_check = """IF current_setting('statement_timeout') <> '500ms'
        OR current_setting('lock_timeout') <> '50ms' THEN
        RAISE EXCEPTION 'Unexpected directory checkpoint budget'; END IF"""
    with write_trigger(postgres_store, "zulip_history_batches", "body", body_check):
        with write_trigger(
            postgres_store, "zulip_history_scopes", "directory_cursor", checkpoint_check
        ):
            assert instance._refresh_history_directory_once()
    with postgres_store.session() as session:
        row = session.execute(
            "SELECT jsonb_array_length(body->'messages') AS n FROM zulip_history_batches"
        ).fetchone()
        assert row["n"] == 5000
        assert (
            session.execute("SELECT state FROM zulip_backfill_jobs").fetchone()["state"]
            == "complete"
        )
    assert instance._refresh_history_directory_once()
    assert not snapshot(postgres_store)["directory_pending"]


@pytest.mark.parametrize(
    "requested",
    ["foreign", "mixed", "empty", "invalid", "scalar", "object", "null", "null_member"],
)
def test_bad_file_observer_request_fails_durably_without_provider_io(
    postgres_store, requested
):
    account = captured(postgres_store)
    values = {
        "foreign": [str(uuid.uuid4())],
        "mixed": [account, str(uuid.uuid4())],
        "empty": [],
        "invalid": ["invalid"],
        "scalar": account,
        "object": {account: True},
        "null": None,
        "null_member": [None],
    }[requested]
    requests = []
    receipt = str(uuid.uuid4())

    def respond(request):
        requests.append(request)
        return httpx.Response(
            202,
            json={
                "uuid": receipt,
                "status": "waiting_files",
                "safe_error": None,
                "files": [
                    {
                        "uuid": str(uuid.uuid4()),
                        "source_path": "/user_uploads/1/file",
                        "account_uuids": values,
                    }
                ],
            },
        )

    with httpx.Client(
        transport=httpx.MockTransport(respond),
        base_url="https://workspace.example.test",
    ) as client:
        publisher = history_delivery.HistoryPublisher(
            postgres_store,
            types.SimpleNamespace(client=client),
            lambda _: pytest.fail("Invalid observer reached provider adapter"),
        )
        publisher.supported = True
        assert not publisher.run_once()
    assert len(requests) == 1 and requests[0].url.path == history_delivery.PATH
    code = "history_file_account_not_assigned"
    with postgres_store.session() as session:
        row = session.execute(
            "SELECT import_status,import_error,import_lease FROM zulip_history_batches"
        ).fetchone()
        assert row["import_status"] == "failed" and row["import_error"] == code
        assert row["import_lease"] is None
        assert (
            session.execute(
                "SELECT safe_error_code FROM zulip_history_failure_reports"
            ).fetchone()["safe_error_code"]
            == code
        )
    assert any(row["safe_error_code"] == code for row in postgres_store.health())
    restarted = storage.RestAlchemyStore(postgres_store.connection_url)
    instance = object.__new__(service.BridgeService)
    instance.store = restarted
    pg._drain_history_failure_reports(restarted, instance._queue_history_failure_report)
    with restarted.session() as session:
        rows = session.execute("SELECT body FROM observed_report_outbox").fetchall()
        assert len(rows) == 1 and rows[0]["body"]["resource_uuid"] == account
        assert rows[0]["body"]["safe_error"]["code"] == code
        assert rows[0]["body"]["status"] == "degraded"
        assert rows[0]["body"]["observed_generation"] == 1
    assert history_delivery.claim(restarted) is None


@pytest.mark.parametrize("spelling", ["uppercase", "braces", "duplicates"])
def test_equivalent_file_observer_uuid_uses_canonical_adapter(postgres_store, spelling):
    account = captured(postgres_store)
    with postgres_store.session() as session:
        session.execute(
            "UPDATE desired_resources SET body=body || '{\"limits\":{\"max_file_bytes\":1024}}'::jsonb WHERE resource_type='external_provider_policy'"
        )
    values = {
        "uppercase": [account.upper()],
        "braces": ["{" + account + "}"],
        "duplicates": [account.upper(), "{" + account + "}", account],
    }[spelling]
    requests, observers = [], []

    def respond(request):
        requests.append(request)
        if request.method == "PUT":
            assert request.content == b"synthetic file"
            return httpx.Response(200)
        return httpx.Response(
            202,
            json={
                "uuid": str(uuid.uuid4()),
                "status": "waiting_files",
                "safe_error": None,
                "files": [
                    {
                        "uuid": str(uuid.uuid4()),
                        "source_path": "/user_uploads/1/file",
                        "account_uuids": values,
                    }
                ],
            },
        )

    def adapters(value):
        observers.append(value)
        return types.SimpleNamespace(
            download_file=lambda *_, **__: types.SimpleNamespace(
                content=b"synthetic file", content_type="text/plain", name="file.txt"
            )
        )

    with httpx.Client(
        transport=httpx.MockTransport(respond),
        base_url="https://workspace.example.test",
    ) as client:
        publisher = history_delivery.HistoryPublisher(
            postgres_store, types.SimpleNamespace(client=client), adapters
        )
        publisher.supported = True
        assert publisher.run_once()
    assert observers == [account]
    assert [request.method for request in requests] == ["POST", "PUT"]
    assert not postgres_store.health()
