"""Capture bookkeeping remains durable when the control report cannot enqueue."""

import threading
import types
import uuid

import pytest

from workspace_zulip_bridge import (
    history,
    history_configuration,
    history_delivery,
    history_failure_reports,
    storage,
    zulip_adapter,
)
from workspace_zulip_bridge.tests import test_history as unit
from workspace_zulip_bridge.tests import test_history_write_budget as budget
from workspace_zulip_bridge.tests import test_postgres_integration as pg

migrated_postgres_dsn = pg.migrated_postgres_dsn
postgres_store = pg.postgres_store


def reporting_work(store):
    with store.session() as session:
        return session.execute(
            "SELECT *,retry_at>now() AS deferred FROM zulip_history_failure_reports"
        ).fetchone()


def make_due(store):
    with store.session() as session:
        session.execute("UPDATE zulip_history_failure_reports SET retry_at=now()")


@pytest.mark.parametrize("result_status", ["rejected", "stale", "exception"])
def test_real_capture_lane_survives_report_failure_and_restarts_durable_work(
    postgres_store, monkeypatch, result_status
):
    job, timeout, account = budget.source(postgres_store)
    reporter = budget.report_service(postgres_store)
    assert postgres_store.defer_history_capture_write(job, timeout)
    pg._drain_history_failure_reports(
        postgres_store, reporter._queue_history_failure_report
    )
    assert reporting_work(postgres_store)["complete"]
    with postgres_store.session() as session:
        if result_status != "exception":
            session.execute(
                "UPDATE observed_report_outbox SET result_status=%s,completed_at=now()",
                (result_status,),
            )
        else:
            session.execute("DELETE FROM observed_report_outbox")
        session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")

    instance = unit.capture_service(
        history.HistoryRange(1, 5000, [unit.source_message()]), store=postgres_store
    )

    def write_timeout(*_):
        raise timeout

    monkeypatch.setattr(postgres_store, "save_history_batch", write_timeout)
    callback_results = []

    def report(*args, **kwargs):
        retained = reporter._queue_history_failure_report(*args, **kwargs)
        callback_results.append(retained)
        if result_status == "exception":
            assert retained
            raise RuntimeError("synthetic enqueue crash")
        return retained

    publisher = history_delivery.HistoryPublisher(
        postgres_store, types.SimpleNamespace(client=object()), None, report
    )
    publisher.supported = True
    continued, stop = threading.Event(), threading.Event()
    calls = 0

    class StopLane(BaseException):
        pass

    def quantum():
        nonlocal calls
        calls += 1
        if calls == 1:
            assert instance.run_backfill_once()
            assert not publisher.run_once()
            return True
        continued.set()
        stop.wait(10)
        raise StopLane()

    instance._run_history_lane_once = quantum

    def run():
        try:
            instance._run_background_history_lane()
        except StopLane:
            pass

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert continued.wait(5), getattr(instance, "background_history_error", None)
        assert thread.is_alive()
        assert getattr(instance, "background_history_error", None) is None
    finally:
        stop.set()
        thread.join(5)
    assert callback_results == [result_status == "exception"]
    assert budget.job_state(postgres_store)["retry_count"] == 2
    assert (
        postgres_store.health()[0]["safe_error_code"]
        == history_failure_reports.CAPTURE_TIMEOUT
    )
    work = reporting_work(postgres_store)
    assert not work["complete"] and work["after_account"] is None and work["deferred"]
    with postgres_store.session() as session:
        count = session.execute(
            "SELECT count(*) AS n FROM observed_report_outbox"
        ).fetchone()["n"]
        assert count == (0 if result_status == "exception" else 1)

    restarted = storage.RestAlchemyStore(postgres_store.connection_url)
    fresh = budget.report_service(restarted)
    make_due(restarted)
    if result_status != "exception":
        # Exercise the real rejected/stale acknowledgement again after restart.
        assert not history_failure_reports.flush_once(
            restarted, fresh._queue_history_failure_report
        )
        assert reporting_work(restarted)["after_account"] is None
        with restarted.session() as session:
            if result_status == "rejected":
                session.execute(
                    "UPDATE observed_report_outbox SET completed_at=now()-interval '6 minutes'"
                )
            else:
                # A stale terminal report remains non-durable until retired.
                session.execute(
                    "DELETE FROM observed_report_outbox WHERE result_status='stale'"
                )
        make_due(restarted)
    pg._drain_history_failure_reports(restarted, fresh._queue_history_failure_report)
    assert reporting_work(restarted)["complete"]
    with restarted.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM observed_report_outbox WHERE body->>'resource_uuid'=%s AND completed_at IS NULL",
                (account,),
            ).fetchone()["n"]
            == 1
        )


def test_capture_reporting_waits_for_same_generation_directory_then_recovers(
    postgres_store,
):
    job, timeout, _ = budget.source(postgres_store)
    with postgres_store.session() as session:
        session.execute("UPDATE zulip_history_scopes SET directory_pending=true")
    assert postgres_store.defer_history_capture_write(job, timeout)
    callback = budget.report_service(postgres_store)._queue_history_failure_report
    assert not history_failure_reports.flush_once(postgres_store, callback)
    assert reporting_work(postgres_store)["deferred"]
    restarted = storage.RestAlchemyStore(postgres_store.connection_url)
    with restarted.session() as session:
        session.execute("UPDATE zulip_history_scopes SET directory_pending=false")
    make_due(restarted)
    pg._drain_history_failure_reports(
        restarted, budget.report_service(restarted)._queue_history_failure_report
    )
    assert reporting_work(restarted)["complete"]


@pytest.mark.parametrize("change", ["success", "scope", "account"])
def test_recovered_or_obsolete_capture_does_not_emit_delayed_error(
    postgres_store, change
):
    job, timeout, _ = budget.source(postgres_store)
    assert postgres_store.defer_history_capture_write(job, timeout)
    with postgres_store.session() as session:
        if change == "scope":
            session.execute("UPDATE zulip_history_scopes SET generation=generation+1")
        elif change == "account":
            session.execute(
                "UPDATE desired_resources SET generation=generation+1 WHERE resource_type='external_account'"
            )
        else:
            session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")
    if change == "success":
        resumed = postgres_store.claim_backfill_job()
        assert postgres_store.save_history_batch(
            resumed, pg._history_job_batch(resumed, 17), None, True
        )
    assert history_failure_reports.flush_once(
        postgres_store, lambda *_, **__: pytest.fail("obsolete error reported")
    )
    assert reporting_work(postgres_store) is None


def test_new_capture_timeout_preserves_unfinished_reporting_cursor(postgres_store):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    for user in (17, 28):
        pg._history_source(postgres_store, user, realm, project)
    job = postgres_store.claim_backfill_job()
    with postgres_store.session() as session:
        generation = session.execute(
            "SELECT generation FROM zulip_history_scopes"
        ).fetchone()["generation"]
    timeout = history.CaptureWriteTimeout((project, realm, 1), generation)
    assert postgres_store.defer_history_capture_write(job, timeout)
    callback = budget.report_service(postgres_store)._queue_history_failure_report
    assert history_failure_reports.flush_once(postgres_store, callback)
    cursor = reporting_work(postgres_store)["after_account"]
    assert cursor is not None
    with postgres_store.session() as session:
        session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")
    another = postgres_store.claim_backfill_job()
    assert postgres_store.defer_history_capture_write(another, timeout)
    assert reporting_work(postgres_store)["after_account"] == cursor
    pg._drain_history_failure_reports(postgres_store, callback)
    assert reporting_work(postgres_store)["complete"]


@pytest.mark.parametrize("already_reported", [False, True])
def test_capture_timeout_and_recovery_preserve_terminal_publication_work(
    postgres_store, monkeypatch, already_reported
):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    users = {
        pg._history_source(postgres_store, user, realm, project): user
        for user in (17, 28)
    }
    job = postgres_store.claim_backfill_job()
    assert postgres_store.save_history_batch(
        job, pg._history_job_batch(job, users[str(job["account_uuid"])]), None, True
    )
    monkeypatch.setattr(history_delivery, "MAX_SOURCES", 1)
    item = history_delivery.claim(postgres_store)
    assert item["validation_error"] == "history_source_limit_exceeded"
    publisher = history_delivery.HistoryPublisher(
        postgres_store, types.SimpleNamespace(client=object()), None
    )
    publisher.fail(item, item["validation_error"])
    callback = budget.report_service(postgres_store)._queue_history_failure_report
    if already_reported:
        pg._drain_history_failure_reports(postgres_store, callback)
    previous = reporting_work(postgres_store)
    pending = postgres_store.claim_backfill_job()
    timeout = history.CaptureWriteTimeout(item["scope"], item["generation"])
    assert postgres_store.defer_history_capture_write(pending, timeout)
    retained = reporting_work(postgres_store)
    assert retained["safe_error_code"] == "history_source_limit_exceeded"
    assert retained["after_account"] == previous["after_account"]
    assert retained["complete"] == previous["complete"]
    with postgres_store.session() as session:
        session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")
    resumed = postgres_store.claim_backfill_job()
    assert postgres_store.save_history_batch(
        resumed,
        pg._history_job_batch(resumed, users[str(resumed["account_uuid"])]),
        None,
        True,
    )
    pg._drain_history_failure_reports(postgres_store, callback)
    assert reporting_work(postgres_store)["complete"]
    with postgres_store.session() as session:
        reports = session.execute("SELECT body FROM observed_report_outbox").fetchall()
    assert len(reports) == 2
    assert all(
        row["body"]["safe_error"]["code"] == "history_source_limit_exceeded"
        for row in reports
    )


@pytest.mark.parametrize("failure", ["retryable", "permanent", "lease_delay"])
def test_timeout_report_survives_later_provider_failure_or_lease_delay(
    postgres_store, failure
):
    job, timeout, _ = budget.source(postgres_store)
    assert postgres_store.defer_history_capture_write(job, timeout)
    with postgres_store.session() as session:
        session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")
    resumed = postgres_store.claim_backfill_job()
    if failure == "lease_delay":
        postgres_store.release_history_capture(resumed)
    else:
        instance = unit.capture_service(None, store=postgres_store)
        instance._fail_history_capture(
            resumed,
            None,
            zulip_adapter.ZulipOperationError(
                "provider_unavailable", failure == "retryable"
            ),
        )
        assert (
            budget.job_state(postgres_store)["last_error_code"]
            == "provider_unavailable"
        )
    assert budget.job_state(postgres_store)["capture_timeout_pending"]
    restarted = storage.RestAlchemyStore(postgres_store.connection_url)
    callback = budget.report_service(restarted)._queue_history_failure_report
    assert history_failure_reports.flush_once(restarted, callback)
    assert reporting_work(restarted)["after_account"] is not None


def change_metadata(store, account, kind):
    with store.session() as session:
        if kind == "directory":
            history_configuration.invalidate_directory(session, account)
        else:
            session.execute(
                "UPDATE desired_resources SET generation=generation+1 WHERE resource_type='external_chat_assignment'"
            )
    store.reconcile_backfill_jobs()


def finish_directory(store):
    adapter = types.SimpleNamespace(history_users=lambda **_: unit.source_users())
    for _ in range(10):
        if not history_configuration.refresh_once(store, lambda _: adapter):
            return
    pytest.fail("Directory refresh did not finish")


@pytest.mark.parametrize("kind", ["metadata", "directory"])
@pytest.mark.parametrize("attempts", [1, 8])
def test_capture_report_rebases_across_metadata_only_generations(
    postgres_store, kind, attempts
):
    job, timeout, account = budget.source(postgres_store)
    for attempt in range(attempts):
        assert postgres_store.defer_history_capture_write(job, timeout)
        if attempt < attempts - 1:
            with postgres_store.session() as session:
                session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")
            job = postgres_store.claim_backfill_job()
    before = budget.job_state(postgres_store)
    change_metadata(postgres_store, account, kind)
    after = budget.job_state(postgres_store)
    assert {k: v for k, v in before.items() if k != "delay"} == {
        k: v for k, v in after.items() if k != "delay"
    }
    work = reporting_work(postgres_store)
    assert work["generation"] > timeout.generation
    restarted = storage.RestAlchemyStore(postgres_store.connection_url)
    callback = budget.report_service(restarted)._queue_history_failure_report
    if kind == "directory":
        assert not history_failure_reports.flush_once(restarted, callback)
        assert reporting_work(restarted) is not None
        finish_directory(restarted)
        make_due(restarted)
    pg._drain_history_failure_reports(restarted, callback)
    assert reporting_work(restarted)["complete"]
    assert budget.job_state(restarted)["capture_timeout_pending"]


@pytest.mark.parametrize("kind", ["metadata", "directory"])
@pytest.mark.parametrize("complete", [False, True])
def test_rebased_capture_reporting_keeps_cursor(postgres_store, kind, complete):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    for user in (17, 28):
        pg._history_source(postgres_store, user, realm, project)
    job = postgres_store.claim_backfill_job()
    with postgres_store.session() as session:
        generation = session.execute(
            "SELECT generation FROM zulip_history_scopes"
        ).fetchone()["generation"]
    assert postgres_store.defer_history_capture_write(
        job, history.CaptureWriteTimeout((project, realm, 1), generation)
    )
    callback = budget.report_service(postgres_store)._queue_history_failure_report
    assert history_failure_reports.flush_once(postgres_store, callback)
    if complete:
        pg._drain_history_failure_reports(postgres_store, callback)
    before = reporting_work(postgres_store)
    change_metadata(postgres_store, str(job["account_uuid"]), kind)
    after = reporting_work(postgres_store)
    assert after["generation"] > before["generation"]
    assert after["after_account"] == before["after_account"] and after["complete"] == complete
    if kind == "directory":
        finish_directory(postgres_store)
    pg._drain_history_failure_reports(postgres_store, callback)
    with postgres_store.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM observed_report_outbox"
            ).fetchone()["n"]
            == 2
        )


@pytest.mark.parametrize("change", ["depth", "account", "source"])
def test_real_capture_rebuild_retires_timeout_marker_and_work(postgres_store, change):
    job, timeout, _ = budget.source(postgres_store)
    assert postgres_store.defer_history_capture_write(job, timeout)
    with postgres_store.session() as session:
        if change == "depth":
            session.execute(
                "UPDATE desired_resources SET generation=generation+1,body=jsonb_set(body,'{history_depth}','\"7_days\"') WHERE resource_type='external_chat_assignment'"
            )
        elif change == "account":
            session.execute(
                "UPDATE desired_resources SET generation=generation+1 WHERE resource_type='external_account'"
            )
        else:
            session.execute(
                "UPDATE desired_resources SET deleted=true WHERE resource_type='external_chat_assignment'"
            )
    postgres_store.reconcile_backfill_jobs()
    assert not budget.job_state(postgres_store)["capture_timeout_pending"]
    assert reporting_work(postgres_store) is None


def test_successful_job_does_not_clear_another_jobs_outstanding_timeout(postgres_store):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    users = {
        pg._history_source(postgres_store, user, realm, project): user
        for user in (17, 28)
    }
    with postgres_store.session() as session:
        generation = session.execute(
            "SELECT generation FROM zulip_history_scopes"
        ).fetchone()["generation"]
    timeout = history.CaptureWriteTimeout((project, realm, 1), generation)
    for _ in range(2):
        job = postgres_store.claim_backfill_job()
        assert postgres_store.defer_history_capture_write(job, timeout)
    with postgres_store.session() as session:
        session.execute("UPDATE zulip_backfill_jobs SET available_at=now()")
    first = postgres_store.claim_backfill_job()
    assert postgres_store.save_history_batch(
        first,
        pg._history_job_batch(first, users[str(first["account_uuid"])]),
        None,
        True,
    )
    assert not history_failure_reports.flush_once(
        postgres_store, lambda *_, **__: False
    )
    assert reporting_work(postgres_store) is not None
    second = postgres_store.claim_backfill_job()
    assert postgres_store.save_history_batch(
        second,
        pg._history_job_batch(second, users[str(second["account_uuid"])]),
        None,
        True,
    )
    make_due(postgres_store)
    assert history_failure_reports.flush_once(
        postgres_store, lambda *_, **__: pytest.fail("recovered error reported")
    )
    assert reporting_work(postgres_store) is None
