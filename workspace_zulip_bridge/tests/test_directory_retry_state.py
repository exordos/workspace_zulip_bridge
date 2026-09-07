"""Durable directory write failure, authority and recovery boundaries."""

import pytest

from workspace_zulip_bridge import history, history_delivery, service, storage
from workspace_zulip_bridge import history_configuration as configuration
from workspace_zulip_bridge import history_failure_reports as reports
from workspace_zulip_bridge.tests import test_history_refresh_budgets as helpers
from workspace_zulip_bridge.tests import test_postgres_integration as pg

migrated_postgres_dsn = pg.migrated_postgres_dsn
postgres_store = pg.postgres_store


def state(store):
    with store.session() as session:
        return dict(
            session.execute(
                "SELECT *,extract(epoch FROM directory_retry_at-now()) AS delay FROM zulip_history_scopes"
            ).fetchone()
        )


def due(store):
    with store.session() as session:
        session.execute("UPDATE zulip_history_scopes SET directory_retry_at=now()")


def reporter(store):
    instance = object.__new__(service.BridgeService)
    instance.store = store
    return instance._queue_history_failure_report


def stop(store):
    row = state(store)
    for _ in range(8):
        assert configuration._defer_directory(store, row, reports.DIRECTORY_TIMEOUT)
    return row


@pytest.mark.parametrize("phase", ["catalog", "body"])
def test_actual_directory_timeouts_stop_after_eight_across_restart_and_new_revision_recovers(
    postgres_store, monkeypatch, phase
):
    account = helpers.captured(postgres_store)
    instance = helpers.directory_service(postgres_store, account)
    if phase == "body":
        assert instance._refresh_history_directory_once()
    initial = state(postgres_store)
    original = history.capture_write_budget_ms
    monkeypatch.setattr(history, "capture_write_budget_ms", lambda _: 20)
    table, column = (
        ("zulip_history_scopes", "directory_users")
        if phase == "catalog"
        else ("zulip_history_batches", "body")
    )
    with helpers.write_trigger(postgres_store, table, column, "PERFORM pg_sleep(0.1)"):
        for attempt in range(1, 9):
            instance.store = storage.RestAlchemyStore(postgres_store.connection_url)
            assert not instance._refresh_history_directory_once()
            row = state(postgres_store)
            assert row["directory_write_attempts"] == attempt
            assert row["directory_pending"]
            assert row["directory_cursor"] == initial["directory_cursor"]
            assert (
                row["directory_account_cursor"] == initial["directory_account_cursor"]
            )
            delay = min(300, 5 * 2 ** (attempt - 1))
            assert delay - 2 < row["delay"] <= delay
            assert any(
                h["safe_error_code"] == reports.DIRECTORY_TIMEOUT
                for h in postgres_store.health()
            )
            due(postgres_store)
        instance.provider_adapters = lambda _: pytest.fail(
            "Terminal pass fetched directory again"
        )
        assert not instance._refresh_history_directory_once()
    assert history_delivery.claim(postgres_store) is None
    pg._drain_history_failure_reports(instance.store, reporter(instance.store))
    with postgres_store.session() as session:
        report = session.execute("SELECT body FROM observed_report_outbox").fetchone()[
            "body"
        ]
        assert report["resource_uuid"] == account
        assert report["safe_error"]["code"] == reports.DIRECTORY_TIMEOUT
        assert session.execute(
            "SELECT complete FROM zulip_history_failure_reports"
        ).fetchone()["complete"]
    assert state(postgres_store)["directory_write_attempts"] == 8
    # New realm invalidation restarts the stopped pass, preserving captured messages.
    monkeypatch.setattr(history, "capture_write_budget_ms", original)
    instance = helpers.directory_service(postgres_store, account)
    assert state(postgres_store)["directory_write_attempts"] == 0
    assert not postgres_store.health()
    with postgres_store.session() as session:
        assert not session.execute(
            "SELECT 1 FROM zulip_history_failure_reports"
        ).fetchone()
    for _ in range(3):
        assert instance._refresh_history_directory_once()
    assert not state(postgres_store)["directory_pending"]
    assert history_delivery.claim(postgres_store) is not None
    with postgres_store.session() as session:
        assert (
            session.execute("SELECT state FROM zulip_backfill_jobs").fetchone()["state"]
            == "complete"
        )


def test_metadata_revision_carries_terminal_report_and_partial_account_cursor(
    postgres_store,
):
    account = helpers.captured(postgres_store)
    initial = state(postgres_store)
    second = pg._history_source(
        postgres_store,
        28,
        str(initial["provider_realm_uuid"]),
        str(initial["project_uuid"]),
    )
    helpers.directory_service(postgres_store, account)
    old = stop(postgres_store)
    assert reports.flush_once(postgres_store, reporter(postgres_store))
    with postgres_store.session() as session:
        before = dict(
            session.execute("SELECT * FROM zulip_history_failure_reports").fetchone()
        )
        session.execute(
            "UPDATE desired_resources SET generation=generation+1 WHERE resource_type='external_chat_assignment'"
        )
    postgres_store.reconcile_backfill_jobs()
    row = state(postgres_store)
    assert (
        row["generation"] > old["generation"] and row["directory_write_attempts"] == 8
    )
    with postgres_store.session() as session:
        after = dict(
            session.execute("SELECT * FROM zulip_history_failure_reports").fetchone()
        )
        assert after == {**before, "generation": row["generation"]}
    pg._drain_history_failure_reports(postgres_store, reporter(postgres_store))
    with postgres_store.session() as session:
        values = session.execute("SELECT body FROM observed_report_outbox").fetchall()
        assert sorted(r["body"]["resource_uuid"] for r in values) == sorted(
            [account, second]
        )


@pytest.mark.parametrize("change", ["depth", "account", "remove"])
def test_capture_changing_rebuild_retires_terminal_directory_failure(
    postgres_store, change
):
    account = helpers.captured(postgres_store)
    helpers.directory_service(postgres_store, account)
    old = stop(postgres_store)
    with postgres_store.session() as session:
        query = {
            "depth": "UPDATE desired_resources SET body=jsonb_set(body,'{history_depth}','\"7_days\"'::jsonb) WHERE resource_type='external_chat_assignment'",
            "account": "UPDATE desired_resources SET generation=generation+1 WHERE resource_type='external_account'",
            "remove": "UPDATE desired_resources SET deleted=true WHERE resource_type='external_chat_assignment'",
        }[change]
        session.execute(query)
    postgres_store.reconcile_backfill_jobs()
    assert state(postgres_store)["directory_write_attempts"] == 0
    assert not configuration._defer_directory(
        postgres_store, old, reports.DIRECTORY_TIMEOUT
    )
    assert not postgres_store.health()
    with postgres_store.session() as session:
        assert not session.execute(
            "SELECT 1 FROM zulip_history_failure_reports"
        ).fetchone()


@pytest.mark.parametrize(
    "change", ["success", "new_revision", "source_before_reconcile"]
)
def test_stale_timeout_cannot_degrade_progress_or_changed_authority(
    postgres_store, change
):
    account = helpers.captured(postgres_store)
    instance = helpers.directory_service(postgres_store, account)
    old = state(postgres_store)
    if change == "success":
        assert instance._refresh_history_directory_once()
    elif change == "new_revision":
        helpers.directory_service(postgres_store, account)
    else:
        with postgres_store.session() as session:
            session.execute(
                "UPDATE desired_resources SET deleted=true WHERE resource_type='external_chat_assignment'"
            )
    assert not configuration._defer_directory(
        postgres_store, old, reports.DIRECTORY_TIMEOUT
    )
    assert state(postgres_store)["directory_write_attempts"] == 0
    assert not postgres_store.health()


@pytest.mark.parametrize("sqlstate", ["55P03", "40001", "40P01"])
def test_transient_contention_delays_and_marks_health_without_spending_attempts(
    postgres_store, monkeypatch, sqlstate
):
    account = helpers.captured(postgres_store)
    instance = helpers.directory_service(postgres_store, account)
    real = configuration._apply_refresh

    class Conflict(Exception):
        pass

    def conflict(*_):
        error = Conflict()
        error.sqlstate = sqlstate
        raise error

    monkeypatch.setattr(configuration, "_apply_refresh", conflict)
    for _ in range(10):
        assert not instance._refresh_history_directory_once()
        row = state(postgres_store)
        assert row["directory_write_attempts"] == 0 and 0 < row["delay"] <= 1
        assert (
            postgres_store.health()[0]["safe_error_code"]
            == "history_directory_database_contention"
        )
        due(postgres_store)
    monkeypatch.setattr(configuration, "_apply_refresh", real)
    assert instance._refresh_history_directory_once()
    assert not postgres_store.health()


def test_terminal_bookkeeping_rolls_back_attempts_health_and_work_together(
    postgres_store, monkeypatch
):
    account = helpers.captured(postgres_store)
    helpers.directory_service(postgres_store, account)
    row = state(postgres_store)
    for _ in range(7):
        assert configuration._defer_directory(
            postgres_store, row, reports.DIRECTORY_TIMEOUT
        )
    before = state(postgres_store)

    def broken(*_):
        raise RuntimeError("Synthetic durable enqueue failure")

    monkeypatch.setattr(reports, "record", broken)
    with pytest.raises(RuntimeError, match="Synthetic durable"):
        configuration._defer_directory(postgres_store, row, reports.DIRECTORY_TIMEOUT)
    after = state(postgres_store)
    after.pop("delay")
    before.pop("delay")
    assert after == before


def test_terminal_reporting_pauses_without_losing_work_and_resumes(postgres_store):
    account = helpers.captured(postgres_store)
    helpers.directory_service(postgres_store, account)
    stop(postgres_store)
    with postgres_store.session() as session:
        session.execute(
            "UPDATE desired_resources SET body=body || '{\"emergency_suspended\":true}'::jsonb WHERE resource_type='external_provider_policy'"
        )
    assert not reports.flush_once(postgres_store, reporter(postgres_store))
    with postgres_store.session() as session:
        assert session.execute("SELECT 1 FROM zulip_history_failure_reports").fetchone()
        session.execute(
            "UPDATE desired_resources SET body=body || '{\"emergency_suspended\":false}'::jsonb WHERE resource_type='external_provider_policy'"
        )
        session.execute("UPDATE zulip_history_failure_reports SET retry_at=now()")
    assert reports.flush_once(postgres_store, reporter(postgres_store))
