"""One retry ceiling controls claims, write authority and durable reporting."""

import pytest

from workspace_zulip_bridge import history
from workspace_zulip_bridge import history_configuration as configuration
from workspace_zulip_bridge import history_failure_reports as reports
from workspace_zulip_bridge.tests import test_directory_retry_state as retry
from workspace_zulip_bridge.tests import test_history_refresh_budgets as helpers
from workspace_zulip_bridge.tests import test_postgres_integration as pg

migrated_postgres_dsn = pg.migrated_postgres_dsn
postgres_store = pg.postgres_store


def scope_row(store, project):
    with store.session() as session:
        return dict(
            session.execute(
                "SELECT * FROM zulip_history_scopes WHERE project_uuid=%s", (project,)
            ).fetchone()
        )


def oldest(store, project):
    with store.session() as session:
        session.execute(
            "UPDATE zulip_history_scopes SET directory_retry_at=now()-interval '1 hour' WHERE project_uuid=%s",
            (project,),
        )


@pytest.mark.parametrize("limit", [4, 12])
def test_retry_ceiling_controls_all_gates_without_starving_other_scopes(
    postgres_store, monkeypatch, limit
):
    assert history.DIRECTORY_WRITE_MAX_ATTEMPTS == 8
    account = helpers.captured(postgres_store)
    project = retry.state(postgres_store)["project_uuid"]
    instance = helpers.directory_service(postgres_store, account)
    healthy = helpers.captured(postgres_store)
    helpers.directory_service(postgres_store, healthy)
    with postgres_store.session() as session:
        other = session.execute(
            "SELECT project_uuid FROM zulip_history_scopes WHERE project_uuid<>%s",
            (project,),
        ).fetchone()["project_uuid"]
    with monkeypatch.context() as patched:
        patched.setattr(history, "DIRECTORY_WRITE_MAX_ATTEMPTS", limit)
        patched.setattr(history, "capture_write_budget_ms", lambda _: 20)
        action = (
            f"IF NEW.project_uuid='{project}'::uuid THEN PERFORM pg_sleep(0.1); END IF"
        )
        with helpers.write_trigger(
            postgres_store, "zulip_history_scopes", "directory_users", action
        ):
            for attempt in range(1, limit + 1):
                oldest(postgres_store, project)
                assert not instance._refresh_history_directory_once()
                assert (
                    scope_row(postgres_store, project)["directory_write_attempts"]
                    == attempt
                )
                with postgres_store.session() as session:
                    terminal = session.execute(
                        "SELECT 1 FROM zulip_history_failure_reports WHERE project_uuid=%s",
                        (project,),
                    ).fetchone()
                    assert bool(terminal) is (attempt == limit)
        terminal = scope_row(postgres_store, project)
        # A previously prepared in-flight write cannot revive a terminal pass.
        assert not configuration._apply_refresh(
            postgres_store,
            terminal,
            (project, terminal["provider_realm_uuid"]),
            [account],
            0,
            None,
            "[]",
            500,
        )
        assert not configuration._defer_directory(
            postgres_store, terminal, reports.DIRECTORY_TIMEOUT
        )
        oldest(postgres_store, project)
        assert instance._refresh_history_directory_once()
        assert scope_row(postgres_store, other)["directory_account_cursor"] == 1
        assert scope_row(postgres_store, project)["directory_account_cursor"] == 0
        # Catalog-only generation changes must carry the same terminal work at
        # either threshold, before its account report is delivered.
        with postgres_store.session() as session:
            session.execute(
                "UPDATE desired_resources SET generation=generation+1 WHERE resource_type='external_chat_assignment' AND body->>'external_account_uuid'=%s",
                (account,),
            )
        postgres_store.reconcile_backfill_jobs()
        current = scope_row(postgres_store, project)
        with postgres_store.session() as session:
            work = session.execute(
                "SELECT generation FROM zulip_history_failure_reports WHERE project_uuid=%s",
                (project,),
            ).fetchone()
            assert work["generation"] == current["generation"] > terminal["generation"]
        assert reports.flush_once(postgres_store, retry.reporter(postgres_store))
        with postgres_store.session() as session:
            report = session.execute(
                "SELECT body FROM observed_report_outbox"
            ).fetchone()["body"]
            assert report["resource_uuid"] == account
            assert report["safe_error"]["code"] == reports.DIRECTORY_TIMEOUT
    # Restore the production default and exercise the SQL selector with it too.
    assert history.DIRECTORY_WRITE_MAX_ATTEMPTS == 8
    helpers.directory_service(postgres_store, account)
    with postgres_store.session() as session:
        session.execute(
            "UPDATE zulip_history_scopes SET directory_write_attempts=7 WHERE project_uuid=%s",
            (project,),
        )
    assert configuration._defer_directory(
        postgres_store, scope_row(postgres_store, project), reports.DIRECTORY_TIMEOUT
    )
    assert scope_row(postgres_store, project)["directory_write_attempts"] == 8
    oldest(postgres_store, project)
    assert instance._refresh_history_directory_once()
    assert scope_row(postgres_store, project)["directory_account_cursor"] == 0
    assert scope_row(postgres_store, other)["directory_cursor"] == 1
    assert instance._refresh_history_directory_once()
    assert not scope_row(postgres_store, other)["directory_pending"]
