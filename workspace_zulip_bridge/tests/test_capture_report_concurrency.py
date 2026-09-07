"""Serialize capture recovery and reporting without reversing job/report locks."""

import contextlib
import threading
import types

import pytest

from workspace_zulip_bridge import history_delivery, history_failure_reports, storage
from workspace_zulip_bridge.tests import test_history_capture_reports as reports
from workspace_zulip_bridge.tests import test_history_write_budget as budget
from workspace_zulip_bridge.tests import test_postgres_integration as pg

migrated_postgres_dsn = pg.migrated_postgres_dsn
postgres_store = pg.postgres_store


def pending_retry(store):
    job, timeout, _ = budget.source(store)
    assert store.defer_history_capture_write(job, timeout)
    with store.session() as session:
        session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")
    return store.claim_backfill_job(), timeout


def publisher(store, callback=None):
    instance = history_delivery.HistoryPublisher(
        store,
        types.SimpleNamespace(client=object()),
        None,
        callback or budget.report_service(store)._queue_history_failure_report,
    )
    instance.probe_after = float("inf")
    return instance


def observe_transactions(store, monkeypatch, after_execute):
    original = store.transaction
    statements = []

    class Session:
        def __init__(self, session):
            self.session = session

        def execute(self, query, parameters=None):
            statements.append(query)
            result = self.session.execute(query, parameters)
            after_execute(query)
            return result

    @contextlib.contextmanager
    def transaction():
        with original() as session:
            yield Session(session)

    monkeypatch.setattr(store, "transaction", transaction)
    return statements


@contextlib.contextmanager
def background_call(call, ready, resume):
    results, errors = [], []

    def run():
        try:
            results.append(call())
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert ready.wait(5), errors
        yield results
    finally:
        resume.set()
        thread.join(5)
        assert not thread.is_alive() and not errors, errors


def test_success_after_report_selection_prevents_late_degraded_enqueue(
    postgres_store, monkeypatch
):
    job, _ = pending_retry(postgres_store)
    reader = storage.RestAlchemyStore(postgres_store.connection_url)
    selected, resume = threading.Event(), threading.Event()

    def gate(query):
        if query.startswith("SELECT project_uuid,provider_realm_uuid,safe_error_code"):
            selected.set()
            assert resume.wait(5)

    observe_transactions(reader, monkeypatch, gate)
    with background_call(publisher(reader).run_once, selected, resume):
        assert postgres_store.save_history_batch(
            job, pg._history_job_batch(job, 17), None, True
        )
    assert reports.reporting_work(postgres_store) is None
    with postgres_store.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM observed_report_outbox"
            ).fetchone()["n"]
            == 0
        )


def test_report_marker_lock_prevents_save_between_validation_and_enqueue(
    postgres_store,
):
    job, _ = pending_retry(postgres_store)
    reader = storage.RestAlchemyStore(postgres_store.connection_url)
    selected, resume = threading.Event(), threading.Event()
    callback = budget.report_service(reader)._queue_history_failure_report

    def report(*args, **kwargs):
        selected.set()
        assert resume.wait(5)
        return callback(*args, **kwargs)

    with background_call(publisher(reader, report).run_once, selected, resume):
        with pytest.raises(Exception) as blocked:
            postgres_store.save_history_batch(
                job, pg._history_job_batch(job, 17), None, True
            )
        assert history_delivery.retryable_database_error(blocked.value)
        assert blocked.value.sqlstate == "55P03"
    # Enqueue committed while the timeout was still current. The later capture
    # succeeds and retires its remaining reporting work without another enqueue.
    assert postgres_store.save_history_batch(
        job, pg._history_job_batch(job, 17), None, True
    )
    assert history_failure_reports.flush_once(
        postgres_store, lambda *_, **__: pytest.fail("late report")
    )
    assert reports.reporting_work(postgres_store) is None


def test_timeout_bookkeeping_lock_cannot_invert_report_job_order(
    postgres_store, monkeypatch
):
    job, timeout = pending_retry(postgres_store)
    writer = storage.RestAlchemyStore(postgres_store.connection_url)
    reader = storage.RestAlchemyStore(postgres_store.connection_url)
    selected, resume = threading.Event(), threading.Event()

    def gate(query):
        if query.startswith(
            "SELECT retry_count,last_error_code FROM zulip_backfill_jobs"
        ):
            selected.set()
            assert resume.wait(5)

    observe_transactions(writer, monkeypatch, gate)
    statements = observe_transactions(reader, monkeypatch, lambda _: None)
    with background_call(
        lambda: writer.defer_history_capture_write(job, timeout), selected, resume
    ):
        assert not publisher(reader).run_once()
        assert any("FOR SHARE OF job" in query for query in statements)
        assert not any(
            query.startswith("SELECT * FROM zulip_history_failure_reports")
            for query in statements
        )
    assert budget.job_state(postgres_store)["retry_count"] == 2
    assert publisher(reader).run_once()
    assert reports.reporting_work(postgres_store)["after_account"] is not None


def test_new_timeout_after_marker_scan_yields_before_reporting(
    postgres_store, monkeypatch
):
    job, timeout = pending_retry(postgres_store)
    assert postgres_store.save_history_batch(
        job, pg._history_job_batch(job, 17), 1, False
    )
    resumed = postgres_store.claim_backfill_job()
    reader = storage.RestAlchemyStore(postgres_store.connection_url)
    selected, resume = threading.Event(), threading.Event()

    def gate(query):
        if "FOR SHARE OF job" in query:
            selected.set()
            assert resume.wait(5)

    observe_transactions(reader, monkeypatch, gate)
    with background_call(publisher(reader).run_once, selected, resume) as result:
        assert postgres_store.defer_history_capture_write(resumed, timeout)
    assert result == [False]
    assert reports.reporting_work(postgres_store)["after_account"] is None
    assert publisher(reader).run_once()
    assert reports.reporting_work(postgres_store)["after_account"] is not None
