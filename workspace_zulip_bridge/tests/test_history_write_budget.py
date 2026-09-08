"""Size-bounded capture writes and durable timeout recovery."""

import base64
import contextlib
import random
import uuid

import pytest

from workspace_zulip_bridge import history, history_failure_reports, service, storage
from workspace_zulip_bridge.tests import test_history as unit
from workspace_zulip_bridge.tests import test_postgres_integration as pg

migrated_postgres_dsn = pg.migrated_postgres_dsn
postgres_store = pg.postgres_store


@pytest.mark.parametrize(
    "size,expected",
    [
        (0, 500),
        (1, 501),
        (16384, 501),
        (16385, 502),
        (42269062, 3080),
        (51309062, 3632),
        (1024**3, 5000),
    ],
)
def test_body_write_budget_has_size_floor_and_hard_cap(size, expected):
    assert history.capture_write_budget_ms(size) == expected


def source(store):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    account = pg._history_source(store, 17, realm, project)
    job = store.claim_backfill_job()
    with store.session() as session:
        generation = session.execute(
            "SELECT generation FROM zulip_history_scopes WHERE project_uuid=%s AND provider_realm_uuid=%s",
            (project, realm),
        ).fetchone()["generation"]
    return job, history.CaptureWriteTimeout((project, realm, 1), generation), account


def report_service(store):
    instance = object.__new__(service.BridgeService)
    instance.store = store
    return instance


def job_state(store):
    with store.session() as session:
        return session.execute(
            "SELECT *,extract(epoch FROM available_at-now()) AS delay FROM zulip_backfill_jobs"
        ).fetchone()


@contextlib.contextmanager
def slow_body_trigger(store):
    name = "cassi_history_write_" + uuid.uuid4().hex
    with store.session() as session:
        session.execute(
            f"CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.1); RETURN NEW; END; $$"
        )
        session.execute(
            f"CREATE TRIGGER {name} BEFORE INSERT OR UPDATE OF body ON zulip_history_batches FOR EACH ROW EXECUTE FUNCTION {name}()"
        )
    try:
        yield
    finally:
        with store.session() as session:
            session.execute(f"DROP TRIGGER {name} ON zulip_history_batches")
            session.execute(f"DROP FUNCTION {name}()")


def test_actual_write_timeouts_back_off_across_restart_and_stop_after_eight(
    postgres_store, monkeypatch
):
    job, _, account = source(postgres_store)
    batch = pg._history_job_batch(job, 17)
    monkeypatch.setattr(history, "capture_write_budget_ms", lambda _: 20)
    with slow_body_trigger(postgres_store):
        for attempt in range(1, 9):
            with pytest.raises(history.CaptureWriteTimeout) as failed:
                postgres_store.save_history_batch(job, batch, None, True)
            assert str(failed.value) == "history_capture_write_timeout"
            assert history.is_statement_timeout(failed.value.__cause__)
            # Restart before bookkeeping to exercise durable authority/context.
            postgres_store = storage.RestAlchemyStore(postgres_store.connection_url)
            instance = report_service(postgres_store)
            assert postgres_store.defer_history_capture_write(job, failed.value)
            pg._drain_history_failure_reports(
                postgres_store, instance._queue_history_failure_report
            )
            current = job_state(postgres_store)
            assert current["retry_count"] == attempt
            assert current["last_error_code"] == "history_capture_write_timeout"
            assert current["next_anchor"] is None and current["lease_until"] is None
            assert current["state"] == ("failed" if attempt == 8 else "pending")
            assert (
                min(300, 5 * 2 ** (attempt - 1)) - 2
                < current["delay"]
                <= min(300, 5 * 2 ** (attempt - 1))
            )
            with postgres_store.session() as session:
                assert (
                    session.execute(
                        "SELECT count(*) AS n FROM zulip_history_batches"
                    ).fetchone()["n"]
                    == 0
                )
                assert (
                    session.execute(
                        "SELECT count(*) AS n FROM bridge_health WHERE safe_error_code='history_capture_write_timeout'"
                    ).fetchone()["n"]
                    == 1
                )
                assert (
                    session.execute(
                        "SELECT count(*) AS n FROM observed_report_outbox WHERE body->>'resource_uuid'=%s",
                        (account,),
                    ).fetchone()["n"]
                    == 1
                )
                session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")
            job = postgres_store.claim_backfill_job()
            assert (job is None) is (attempt == 8)


def test_success_resets_timeout_streak_and_health(postgres_store):
    job, timeout, _ = source(postgres_store)
    assert postgres_store.defer_history_capture_write(job, timeout)
    with postgres_store.session() as session:
        session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")
    resumed = postgres_store.claim_backfill_job()
    assert postgres_store.save_history_batch(
        resumed, pg._history_job_batch(resumed, 17), None, True
    )
    current = job_state(postgres_store)
    assert (
        current["state"] == "complete"
        and current["retry_count"] == 0
        and current["last_error_code"] is None
        and not current["capture_timeout_pending"]
    )
    assert not postgres_store.health()


def test_timeout_streak_does_not_count_prior_provider_failures(postgres_store):
    job, timeout, _ = source(postgres_store)
    with postgres_store.session() as session:
        session.execute(
            "UPDATE zulip_backfill_jobs SET retry_count=100,last_error_code='provider_unavailable'"
        )
    assert postgres_store.defer_history_capture_write(job, timeout)
    current = job_state(postgres_store)
    assert current["retry_count"] == 1 and current["state"] == "pending"


@pytest.mark.parametrize("change", ["lease", "account", "scope"])
def test_obsolete_write_timeout_cannot_report_or_retire_new_work(
    postgres_store, change
):
    job, timeout, _ = source(postgres_store)
    with postgres_store.session() as session:
        if change == "lease":
            session.execute(
                "UPDATE zulip_backfill_jobs SET lease_until=lease_until+interval '1 minute'"
            )
        elif change == "account":
            session.execute(
                "UPDATE desired_resources SET generation=generation+1 WHERE resource_type='external_account'"
            )
        else:
            session.execute("UPDATE zulip_history_scopes SET generation=generation+1")
    assert not postgres_store.defer_history_capture_write(job, timeout)
    current = job_state(postgres_store)
    assert current["retry_count"] == 0 and current["last_error_code"] is None
    if change == "lease":
        assert (
            current["state"] == "running"
            and current["lease_until"] != job["lease_until"]
        )
    assert not postgres_store.health()


def test_timeout_bookkeeping_and_reporting_work_roll_back_together(
    postgres_store, monkeypatch
):
    job, timeout, _ = source(postgres_store)
    original = history_failure_reports.record

    def record_then_crash(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("synthetic report failure")

    monkeypatch.setattr(history_failure_reports, "record", record_then_crash)
    with pytest.raises(RuntimeError, match="synthetic report failure"):
        postgres_store.defer_history_capture_write(job, timeout)
    current = job_state(postgres_store)
    assert (
        current["state"] == "running"
        and current["lease_until"] == job["lease_until"]
        and current["retry_count"] == 0
        and not current["capture_timeout_pending"]
    )
    assert not postgres_store.health()
    with postgres_store.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM zulip_history_failure_reports"
            ).fetchone()["n"]
            == 0
        )
    monkeypatch.setattr(history_failure_reports, "record", original)
    assert postgres_store.defer_history_capture_write(job, timeout)


def test_large_actual_5000_message_insert_and_update_use_bounded_body_budget(
    postgres_store, monkeypatch
):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    accounts = {
        pg._history_source(postgres_store, user, realm, project): user
        for user in (17, 28)
    }
    rng = random.Random(7)
    messages = [
        {
            **unit.source_message(message_id=i),
            "content": base64.b64encode(rng.randbytes(7500)).decode(),
        }
        for i in range(1, 5001)
    ]
    budgets = []
    original = postgres_store._write_history_body

    def write(session, query, parameters, budget, scope, generation):
        budgets.append(int(budget.removesuffix("ms")))
        result = original(session, query, parameters, budget, scope, generation)
        assert (
            session.execute("SHOW statement_timeout").fetchone()["statement_timeout"]
            == "500ms"
        )
        assert session.execute("SHOW lock_timeout").fetchone()["lock_timeout"] == "2s"
        return result

    monkeypatch.setattr(postgres_store, "_write_history_body", write)
    for _ in accounts:
        job = postgres_store.claim_backfill_job()
        batch = history.make_batch(
            history.HistoryRange(1, 5000, messages),
            unit.source_users(),
            accounts[str(job["account_uuid"])],
        )
        assert postgres_store.save_history_batch(job, batch, None, True)
    assert len(budgets) == 2 and all(3500 < budget <= 5000 for budget in budgets)
    with postgres_store.session() as session:
        row = session.execute(
            "SELECT jsonb_array_length(body->'messages') AS count,jsonb_array_length(body->'messages'->0->'access') AS observers FROM zulip_history_batches"
        ).fetchone()
        assert row["count"] == 5000 and row["observers"] == 2
        assert all(
            row["state"] == "complete"
            for row in session.execute(
                "SELECT state FROM zulip_backfill_jobs"
            ).fetchall()
        )


def test_service_uses_durable_timeout_path_without_bare_release():
    instance = unit.capture_service(
        history.HistoryRange(1, 5000, [unit.source_message()])
    )
    timeout = history.CaptureWriteTimeout(("project", "realm", 1), 2)
    instance.store.save_history_batch = lambda *_: (_ for _ in ()).throw(timeout)
    calls = []
    instance.store.defer_history_capture_write = lambda *args: (
        calls.append(args) or True
    )
    assert instance.run_backfill_once()
    assert len(calls) == 1 and calls[0][1] is timeout
    assert not instance.store.released and not instance.store.failed


def test_service_timeout_bookkeeping_contention_waits_for_lease_expiry():
    class Conflict(Exception):
        sqlstate = "55P03"

    instance = unit.capture_service(
        history.HistoryRange(1, 5000, [unit.source_message()])
    )
    instance.store.save_history_batch = lambda *_: (_ for _ in ()).throw(
        history.CaptureWriteTimeout(("project", "realm", 1), 2)
    )
    instance.store.defer_history_capture_write = lambda *_: (_ for _ in ()).throw(
        Conflict()
    )
    assert instance.run_backfill_once()
    assert not instance.store.released and not instance.store.failed


def test_actual_update_timeout_preserves_prior_body_and_checkpoint(
    postgres_store, monkeypatch
):
    job, _, _ = source(postgres_store)
    before = pg._history_job_batch(job, 17)
    assert postgres_store.save_history_batch(job, before, 1, False)
    job = postgres_store.claim_backfill_job()
    after = pg._history_job_batch(job, 17, read=False)
    monkeypatch.setattr(history, "capture_write_budget_ms", lambda _: 20)
    with slow_body_trigger(postgres_store):
        with pytest.raises(history.CaptureWriteTimeout) as failed:
            postgres_store.save_history_batch(job, after, None, True)
    assert postgres_store.defer_history_capture_write(job, failed.value)
    with postgres_store.session() as session:
        assert (
            session.execute(
                "SELECT body->>'hash' AS hash FROM zulip_history_batches"
            ).fetchone()["hash"]
            == before["hash"]
        )
    current = job_state(postgres_store)
    assert (
        current["next_anchor"] == 1
        and current["state"] == "pending"
        and current["retry_count"] == 1
    )


def test_body_lock_timeout_is_not_a_body_statement_timeout():
    class LockTimeout(Exception):
        sqlstate = "55P03"

    error = LockTimeout()

    class Session:
        def execute(self, query, parameters=None):
            if query == "body write":
                raise error

    with pytest.raises(LockTimeout) as caught:
        storage.RestAlchemyStore._write_history_body(
            Session(), "body write", (), "500ms", ("project", "realm", 1), 1
        )
    assert caught.value is error
