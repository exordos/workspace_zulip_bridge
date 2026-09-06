"""Real transaction boundaries and durable directory acknowledgement races."""

import concurrent.futures
import contextlib
import threading
import types
import uuid

import pytest

from workspace_zulip_bridge import (
    history,
    history_configuration,
    service,
    storage,
    zulip_adapter,
)
from workspace_zulip_bridge.tests import test_postgres_integration as pg
from workspace_zulip_bridge.tests.test_postgres_integration import (
    _history_job_batch,
    _history_source,
    _refresh_directory,
)

migrated_postgres_dsn = pg.migrated_postgres_dsn
postgres_store = pg.postgres_store


def scope_row(store, project):
    with store.session() as session:
        return session.execute(
            "SELECT * FROM zulip_history_scopes WHERE project_uuid=%s", (project,)
        ).fetchone()


def invalidate(store, account):
    with store.session() as session:
        history_configuration.invalidate_directory(session, account)


def adapter(users=None):
    return types.SimpleNamespace(
        history_users=lambda **_: (
            users or [{"user_id": 17, "full_name": "Updated directory"}]
        )
    )


@pytest.mark.parametrize("phase", ["merge", "directory", "encode"])
def test_capture_preparation_does_not_block_control_writes(
    postgres_store, monkeypatch, phase
):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    account = _history_source(postgres_store, 17, realm, project)
    job = postgres_store.claim_backfill_job()
    batch = _history_job_batch(job, 17)
    if phase == "directory":
        invalidate(postgres_store, account)
        _refresh_directory(postgres_store, batch["users"])
    started, resume = threading.Event(), threading.Event()
    owner = threading.get_ident()
    target, name = {
        "merge": (history, "merge_batch"),
        "directory": (history_configuration, "with_directory"),
        "encode": (storage.json, "dumps"),
    }[phase]
    original = getattr(target, name)
    calls = []

    def prepare(*args, **kwargs):
        if threading.get_ident() != owner:
            assert getattr(postgres_store._transaction_state, "session", None) is None
            calls.append(True)
            started.set()
            assert resume.wait(10)
        return original(*args, **kwargs)

    monkeypatch.setattr(target, name, prepare)
    with concurrent.futures.ThreadPoolExecutor() as pool:
        pending = pool.submit(postgres_store.save_history_batch, job, batch, None, True)
        try:
            assert started.wait(10)
            with postgres_store.session() as session:
                session.execute("SET LOCAL lock_timeout='200ms'")
                session.execute(
                    "UPDATE desired_resources SET generation=generation+1 WHERE resource_type='external_chat_assignment'"
                )
        finally:
            resume.set()
        assert not pending.result(timeout=10)
    assert calls == [True]
    with postgres_store.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM zulip_history_batches"
            ).fetchone()["n"]
            == 0
        )
        checkpoint = session.execute(
            "SELECT state,next_anchor FROM zulip_backfill_jobs"
        ).fetchone()
        assert checkpoint["state"] == "pending" and checkpoint["next_anchor"] is None


def test_capture_refuses_outer_transaction_before_preparing(
    postgres_store, monkeypatch
):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    _history_source(postgres_store, 17, realm, project)
    job = postgres_store.claim_backfill_job()
    batch = _history_job_batch(job, 17)
    monkeypatch.setattr(
        history, "merge_batch", lambda *_: pytest.fail("prepared in outer transaction")
    )
    with postgres_store.transaction():
        with pytest.raises(
            RuntimeError, match="history_capture_requires_transaction_boundary"
        ):
            postgres_store.save_history_batch(job, batch, None, True)


@pytest.mark.parametrize("existing", [False, True])
def test_capture_cas_conflict_keeps_checkpoint_and_new_observations(
    postgres_store, monkeypatch, existing
):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    accounts = {
        _history_source(postgres_store, user, realm, project): user
        for user in (17, 28, 39)
    }
    if existing:
        first = postgres_store.claim_backfill_job()
        assert postgres_store.save_history_batch(
            first,
            _history_job_batch(first, accounts[str(first["account_uuid"])]),
            None,
            True,
        )
    job, competing = (
        postgres_store.claim_backfill_job(),
        postgres_store.claim_backfill_job(),
    )
    incoming = _history_job_batch(job, accounts[str(job["account_uuid"])])
    other = _history_job_batch(competing, accounts[str(competing["account_uuid"])])
    original = history.merge_batch
    once = []

    def merge(prior, batch):
        if batch is incoming and not once:
            once.append(True)
            assert postgres_store.save_history_batch(competing, other, None, True)
        return original(prior, batch)

    monkeypatch.setattr(history, "merge_batch", merge)
    assert not postgres_store.save_history_batch(job, incoming, None, True)
    with postgres_store.session() as session:
        saved = session.execute("SELECT body FROM zulip_history_batches").fetchone()[
            "body"
        ]
        assert accounts[str(competing["account_uuid"])] in {
            a["user_id"] for a in saved["messages"][0]["access"]
        }
        assert accounts[str(job["account_uuid"])] not in {
            a["user_id"] for a in saved["messages"][0]["access"]
        }
        checkpoint = session.execute(
            "SELECT state,next_anchor FROM zulip_backfill_jobs WHERE account_uuid=%s",
            (job["account_uuid"],),
        ).fetchone()
        assert checkpoint["state"] == "pending" and checkpoint["next_anchor"] is None


def test_capture_invalidation_cannot_release_reissued_lease(
    postgres_store, monkeypatch
):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    _history_source(postgres_store, 17, realm, project)
    job = postgres_store.claim_backfill_job()
    original = history.merge_batch
    new_lease = []

    def merge(*args):
        with postgres_store.session() as session:
            session.execute(
                "UPDATE desired_resources SET generation=generation+1 WHERE resource_type='external_chat_assignment'"
            )
            new_lease.append(
                session.execute(
                    "UPDATE zulip_backfill_jobs SET lease_until=lease_until+interval '1 minute' RETURNING lease_until"
                ).fetchone()["lease_until"]
            )
        return original(*args)

    monkeypatch.setattr(history, "merge_batch", merge)
    assert not postgres_store.save_history_batch(
        job, _history_job_batch(job, 17), None, True
    )
    with postgres_store.session() as session:
        current = session.execute(
            "SELECT state,lease_until FROM zulip_backfill_jobs"
        ).fetchone()
        assert current["state"] == "running" and current["lease_until"] == new_lease[0]


@pytest.mark.parametrize("superseded", ["lease", "source"])
def test_capture_error_cannot_mutate_or_report_a_superseded_claim(
    postgres_store, superseded
):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    _history_source(postgres_store, 17, realm, project)
    claimed = []

    class Adapter:
        account_generation = 1

        def history_range(self, *_args, **_kwargs):
            with postgres_store.session() as session:
                if superseded == "lease":
                    session.execute(
                        "UPDATE zulip_backfill_jobs SET lease_until=lease_until+interval '1 minute'"
                    )
                else:
                    session.execute(
                        "UPDATE desired_resources SET generation=generation+1 "
                        "WHERE resource_type='external_chat_assignment'"
                    )
            raise zulip_adapter.ZulipOperationError("provider_forbidden", False)

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.provider_adapters = lambda _account: Adapter()
    instance._handle_provider_account_error = lambda *_args: claimed.append(True)
    instance._queue_account_report = lambda *_args, **_kwargs: claimed.append(True)
    assert instance.run_backfill_once()
    assert claimed == []
    with postgres_store.session() as session:
        job = session.execute(
            "SELECT state,lease_until,last_error_code FROM zulip_backfill_jobs"
        ).fetchone()
        health = session.execute(
            "SELECT 1 FROM bridge_health WHERE component LIKE 'provider:%'"
        ).fetchone()
    assert job["state"] == "running"
    assert job["lease_until"] is not None
    assert job["last_error_code"] is None
    assert health is None


def test_capture_error_transitions_current_claim_before_reporting(postgres_store):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    account = _history_source(postgres_store, 17, realm, project)
    reported = []

    class Adapter:
        account_generation = 1

        def history_range(self, *_args, **_kwargs):
            raise zulip_adapter.ZulipOperationError("provider_forbidden", False)

    instance = object.__new__(service.BridgeService)
    instance.store = postgres_store
    instance.provider_adapters = lambda _account: Adapter()
    instance._handle_provider_account_error = lambda *_args: False
    instance._queue_account_report = lambda *args, **kwargs: reported.append(
        (args, kwargs)
    )
    assert instance.run_backfill_once()
    with postgres_store.session() as session:
        job = session.execute(
            "SELECT state,lease_until,last_error_code FROM zulip_backfill_jobs"
        ).fetchone()
        health = session.execute(
            "SELECT status,safe_error_code FROM bridge_health WHERE component=%s",
            (storage.backfill_health_component(account, "channel:42"),),
        ).fetchone()
    assert job == {
        "state": "failed",
        "lease_until": None,
        "last_error_code": "provider_forbidden",
    }
    assert health == {
        "status": "degraded",
        "safe_error_code": "provider_forbidden",
    }
    assert reported == [
        ((account, "degraded", "provider_forbidden", 1), {})
    ]


def test_acknowledged_scope_rebuild_does_not_repeat_directory_while_other_scope_pending(
    postgres_store,
):
    realm = str(uuid.uuid4())
    projects = sorted(str(uuid.uuid4()) for _ in range(2))
    accounts = [_history_source(postgres_store, 17, realm, p) for p in projects]
    invalidate(postgres_store, accounts[0])
    with postgres_store.session() as session:
        session.execute(
            "UPDATE zulip_history_scopes SET directory_retry_at=now()+interval '1 hour' WHERE project_uuid=%s",
            (projects[1],),
        )
    _refresh_directory(postgres_store, [{"id": 17, "name": "Updated directory"}])
    first, second = (scope_row(postgres_store, p) for p in projects)
    assert first["directory_ack_revision"] == first["directory_revision"] > 0
    assert not first["directory_pending"] and second["directory_pending"]
    with postgres_store.session() as session:
        session.execute(
            "UPDATE desired_resources SET generation=generation+1,body=jsonb_set(body,'{history_depth}','\"30_days\"'::jsonb) WHERE resource_type='external_chat_assignment' AND body->>'project_id'=%s",
            (projects[0],),
        )
    postgres_store.reconcile_backfill_jobs()
    assert not scope_row(postgres_store, projects[0])["directory_pending"]
    with postgres_store.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM zulip_history_directory_revisions"
            ).fetchone()["n"]
            == 1
        )
        session.execute("UPDATE zulip_history_scopes SET directory_retry_at=now()")
    _refresh_directory(postgres_store, [{"id": 17, "name": "Updated directory"}])
    with postgres_store.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM zulip_history_directory_revisions"
            ).fetchone()["n"]
            == 0
        )
        session.execute(
            "UPDATE desired_resources SET generation=generation+1,body=jsonb_set(body,'{history_depth}','\"all\"'::jsonb) WHERE resource_type='external_chat_assignment'"
        )
    postgres_store.reconcile_backfill_jobs()
    assert all(not scope_row(postgres_store, p)["directory_pending"] for p in projects)


@pytest.mark.parametrize("phase", ["get", "rewrite"])
def test_directory_event_during_preparation_is_not_acknowledged(
    postgres_store, monkeypatch, phase
):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    account = _history_source(postgres_store, 17, realm, project)
    job = postgres_store.claim_backfill_job()
    assert postgres_store.save_history_batch(
        job, _history_job_batch(job, 17), None, True
    )
    invalidate(postgres_store, account)
    initial = scope_row(postgres_store, project)["directory_revision"]
    if phase == "rewrite":
        assert history_configuration.refresh_once(postgres_store, lambda _: adapter())
        original = history_configuration.with_directory

        def rewrite(*args):
            invalidate(postgres_store, account)
            return original(*args)

        monkeypatch.setattr(history_configuration, "with_directory", rewrite)
        assert not history_configuration.refresh_once(
            postgres_store, lambda _: adapter()
        )
        monkeypatch.setattr(history_configuration, "with_directory", original)
    else:

        def refresh(**_):
            invalidate(postgres_store, account)
            return [{"user_id": 17, "full_name": "Stale"}]

        assert not history_configuration.refresh_once(
            postgres_store, lambda _: types.SimpleNamespace(history_users=refresh)
        )
    current = scope_row(postgres_store, project)
    assert current["directory_pending"] and current["directory_revision"] > initial
    assert current["directory_ack_revision"] < current["directory_revision"]
    _refresh_directory(postgres_store, [{"id": 17, "name": "Newest"}])
    final = scope_row(postgres_store, project)
    assert final["directory_ack_revision"] == current["directory_revision"]
    assert not final["directory_pending"]


def test_new_scope_waits_for_ack_fence_then_captures_without_stale_revision(
    postgres_store, monkeypatch
):
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    account = _history_source(postgres_store, 17, realm, project)
    invalidate(postgres_store, account)
    assert history_configuration.refresh_once(postgres_store, lambda _: adapter())
    locked, release = threading.Event(), threading.Event()
    original = postgres_store.session

    class SessionProxy:
        def __init__(self, session):
            self.session = session

        def execute(self, query, params=None):
            result = (
                self.session.execute(query, params)
                if params is not None
                else self.session.execute(query)
            )
            if (
                query
                == "SELECT generation FROM zulip_history_configuration WHERE singleton FOR UPDATE"
            ):
                locked.set()
                assert release.wait(10)
            return result

    @contextlib.contextmanager
    def scoped_session():
        with original() as session:
            yield SessionProxy(session)

    monkeypatch.setattr(postgres_store, "session", scoped_session)
    second = storage.RestAlchemyStore(postgres_store.connection_url)
    new_project = str(uuid.uuid4())
    with concurrent.futures.ThreadPoolExecutor() as pool:
        ack = pool.submit(
            history_configuration.refresh_once, postgres_store, lambda _: adapter()
        )
        try:
            assert locked.wait(10)
            added = pool.submit(_history_source, second, 28, realm, new_project)
            # The new source cannot reconcile across the active acknowledgement.
            with pytest.raises(concurrent.futures.TimeoutError):
                added.result(timeout=0.1)
        finally:
            release.set()
        assert ack.result(timeout=10)
        added.result(timeout=10)
    assert not scope_row(postgres_store, new_project)["directory_pending"]
    with postgres_store.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM zulip_history_directory_revisions"
            ).fetchone()["n"]
            == 0
        )


def test_concurrent_final_directory_acks_clear_last_realm_revision(
    postgres_store, monkeypatch
):
    realm = str(uuid.uuid4())
    projects = sorted(str(uuid.uuid4()) for _ in range(2))
    accounts = [_history_source(postgres_store, 17, realm, p) for p in projects]
    invalidate(postgres_store, accounts[0])
    # Both scopes have assembled their directory and need only the final ack.
    with postgres_store.session() as session:
        session.execute("UPDATE zulip_history_scopes SET directory_account_cursor=1")
    barrier = threading.Barrier(2)
    original = postgres_store.session
    local = threading.local()

    class SessionProxy:
        def __init__(self, session):
            self.session = session

        def execute(self, query, params=None):
            if query.startswith(
                "SELECT * FROM zulip_history_scopes WHERE directory_pending"
            ):
                query = "SELECT * FROM zulip_history_scopes WHERE project_uuid=%s"
                params = (local.project,)
            if (
                query
                == "SELECT generation FROM zulip_history_configuration WHERE singleton FOR UPDATE"
            ):
                barrier.wait(timeout=10)
            return (
                self.session.execute(query, params)
                if params is not None
                else self.session.execute(query)
            )

    @contextlib.contextmanager
    def session():
        with original() as current:
            yield SessionProxy(current)

    monkeypatch.setattr(postgres_store, "session", session)

    def finish(project):
        local.project = project
        return history_configuration.refresh_once(postgres_store, lambda _: adapter())

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        assert all(pool.map(finish, projects))
    with original() as session:
        assert not session.execute(
            "SELECT 1 FROM zulip_history_directory_revisions"
        ).fetchall()
        assert all(
            not row["directory_pending"]
            and row["directory_revision"] == row["directory_ack_revision"]
            for row in session.execute("SELECT * FROM zulip_history_scopes").fetchall()
        )
