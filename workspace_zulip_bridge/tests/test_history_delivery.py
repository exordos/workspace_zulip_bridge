import contextlib
import threading
import time
import types
import uuid

import httpx
import pytest

from workspace_zulip_bridge import (
    history_configuration,
    history_delivery,
    history_failure_reports,
    zulip_adapter,
)


def publication_item(*, registered=True):
    return {
        "scope": (uuid.uuid4(), uuid.uuid4(), 1),
        "lease": uuid.uuid4(),
        "generation": 1,
        "import_uuid": uuid.uuid4() if registered else None,
        "mapping_cursor": {},
        "envelope": {
            "sources": [{"account_uuid": str(uuid.uuid4()), "account_generation": 3}],
            "batch": {"from_id": 1},
        },
    }


class TerminalStore:
    """Model the transaction boundary and conditional batch write, not SQL."""

    def __init__(self, health=None, *, scope_exists=True, owns_lease=True):
        self.health = health if health is not None else []
        self.scope_exists = scope_exists
        self.owns_lease = owns_lease
        self.status = "pending"
        self.active = False
        self.commits = 0
        self.rollbacks = 0
        self.sql = []
        self.work = []

    @contextlib.contextmanager
    def transaction(self):
        assert not self.active
        self.active = True
        previous = self.status, self.owns_lease, list(self.health), list(self.work)
        try:
            yield self
        except Exception:
            self.status, self.owns_lease, health, self.work = previous
            self.health[:] = health
            self.rollbacks += 1
            raise
        else:
            self.commits += 1
        finally:
            self.active = False

    @contextlib.contextmanager
    def session(self):
        assert self.active
        yield self

    def execute(self, sql, parameters=None):
        assert self.active
        self.sql.append(sql)
        row = {"result": 1}
        if "SELECT 1 FROM zulip_history_scopes" in sql:
            row = row if self.scope_exists else None
        elif sql.startswith("UPDATE zulip_history_batches"):
            row = row if self.owns_lease else None
            if self.owns_lease:
                self.status = parameters[0]
                self.owns_lease = False
        elif sql.startswith("INSERT INTO zulip_history_failure_reports"):
            assert parameters[4:] == (False, False)
            self.work.append(parameters[:4])
        return types.SimpleNamespace(fetchone=lambda: row)

    def mark_health(self, *args):
        assert self.active
        self.health.append(args)


def ready_publisher(
    monkeypatch, item, respond, *, store=None, adapters=None, account_report=None
):
    requests, releases = [], []

    def transport(request):
        requests.append(request)
        return respond(request)

    client = httpx.Client(
        transport=httpx.MockTransport(transport),
        base_url="https://workspace.example.test",
    )
    publisher = history_delivery.HistoryPublisher(
        store or object(),
        types.SimpleNamespace(client=client),
        adapters,
        account_report,
    )
    publisher.supported = True
    monkeypatch.setattr(history_delivery, "claim", lambda _, **__: item)
    monkeypatch.setattr(history_delivery, "current", lambda *_: True)
    monkeypatch.setattr(history_failure_reports, "flush_once", lambda *_: False)
    monkeypatch.setattr(
        history_delivery,
        "release",
        lambda *_, **kwargs: releases.append(kwargs) or True,
    )
    return publisher, requests, releases


def waiting_file_publisher(monkeypatch, download, *, limit=lambda hard: hard):
    item = publication_item()
    account = item["envelope"]["sources"][0]["account_uuid"]
    file_request = {
        "uuid": str(uuid.uuid4()),
        "source_path": "/user_uploads/1/file",
        "account_uuids": [account],
    }

    def respond(request):
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "uuid": str(item["import_uuid"]),
                    "status": "waiting_files",
                    "safe_error": None,
                    "files": [file_request],
                },
            )
        return httpx.Response(200, json={"status": "ready"})

    return ready_publisher(
        monkeypatch,
        item,
        respond,
        store=types.SimpleNamespace(effective_file_limit=limit),
        adapters=lambda _: types.SimpleNamespace(download_file=download),
    )


def test_old_backend_is_capture_only_and_does_not_claim(monkeypatch):
    client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(404)),
        base_url="https://workspace.example.test",
    )
    publisher = history_delivery.HistoryPublisher(
        object(), types.SimpleNamespace(client=client), None
    )
    monkeypatch.setattr(
        history_delivery,
        "claim",
        lambda _: (_ for _ in ()).throw(AssertionError("old backend claimed work")),
    )
    assert publisher.run_once() is False
    assert publisher.run_once() is False


def test_publisher_uploads_one_file_and_keeps_job_receipt(monkeypatch):
    item = publication_item(registered=False)
    account = item["envelope"]["sources"][0]["account_uuid"]
    job_uuid, file_uuid = str(uuid.uuid4()), str(uuid.uuid4())

    def respond(request):
        if request.url.path == history_delivery.PATH:
            return httpx.Response(
                202,
                json={
                    "uuid": job_uuid,
                    "status": "waiting_files",
                    "safe_error": None,
                    "files": [
                        {
                            "uuid": file_uuid,
                            "source_path": "/user_uploads/1/file",
                            "account_uuids": [account],
                        }
                    ],
                },
            )
        return httpx.Response(200, json={"status": "ready"})

    publisher, requests, releases = ready_publisher(
        monkeypatch,
        item,
        respond,
        store=types.SimpleNamespace(effective_file_limit=lambda hard: hard),
        adapters=lambda _: types.SimpleNamespace(
            download_file=lambda *_, **__: zulip_adapter.ProviderFile(
                "file.txt", "text/plain", b"fixture"
            )
        ),
    )
    assert publisher.run_once()
    assert [request.method for request in requests] == ["POST", "PUT"]
    assert requests[-1].content == b"fixture"
    assert releases[0]["job_uuid"] == uuid.UUID(job_uuid)


def test_busy_admission_sets_bounded_cooldown_without_terminal_failure(monkeypatch):
    item = publication_item(registered=False)
    now = [100.0]
    stats = []
    publisher, requests, releases = ready_publisher(
        monkeypatch,
        item,
        lambda _: httpx.Response(
            429,
            headers={"Retry-After": "12"},
            json={"error": "history_import_busy"},
        ),
    )
    publisher.clock = lambda: now[0]
    publisher.record_stat = stats.append

    assert publisher.run_once() is False
    assert [request.method for request in requests] == ["POST"]
    assert releases == [{"error": "history_import_busy", "delay": 12}]
    assert publisher.admission_probe_after == 112.0
    assert stats == ["history_admission_attempts", "history_admission_busy"]


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_retryable_http_failures_keep_generic_delivery_retry(monkeypatch, status):
    item = publication_item(registered=False)
    publisher, _, releases = ready_publisher(
        monkeypatch,
        item,
        lambda _: httpx.Response(status, json={"error": "temporary_failure"}),
    )

    assert publisher.run_once() is False
    assert releases == [
        {"job_uuid": None, "error": "history_delivery_unavailable", "delay": 5}
    ]
    assert publisher.admission_probe_after == 0.0


@pytest.mark.parametrize(
    "retry_after,expected",
    [(None, 5), ("0", 1), ("invalid", 5), ("120", 60)],
)
def test_admission_retry_after_is_bounded(retry_after, expected):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    response = httpx.Response(429, headers=headers)
    assert history_delivery.admission_retry_after(response) == expected


def test_transport_failure_keeps_generic_delivery_retry(monkeypatch):
    item = publication_item(registered=False)

    def unavailable(_):
        raise httpx.ConnectError("synthetic transport failure")

    publisher, _, releases = ready_publisher(monkeypatch, item, unavailable)
    assert publisher.run_once() is False
    assert releases == [
        {"job_uuid": None, "error": "history_delivery_unavailable", "delay": 5}
    ]
    assert publisher.admission_probe_after == 0.0


def test_cooldown_polls_active_receipt_and_terminal_status_reopens_admission(
    monkeypatch,
):
    active = publication_item()
    unregistered = publication_item(registered=False)
    claims = [active, unregistered]
    claim_options = []
    requests = []
    releases = []
    stats = []

    def claim_next(_, **options):
        claim_options.append(options)
        return claims.pop(0)

    def respond(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"uuid": str(active["import_uuid"]), "status": "complete"},
            )
        return httpx.Response(
            202,
            json={
                "uuid": str(uuid.uuid4()),
                "status": "pending",
                "safe_error": None,
                "files": [],
            },
        )

    client = httpx.Client(
        transport=httpx.MockTransport(respond),
        base_url="https://workspace.example.test",
    )
    publisher = history_delivery.HistoryPublisher(
        object(),
        types.SimpleNamespace(client=client),
        None,
        record_stat=stats.append,
        clock=lambda: 10.0,
    )
    publisher.supported = True
    publisher.admission_probe_after = 20.0
    monkeypatch.setattr(history_delivery, "claim", claim_next)
    monkeypatch.setattr(history_delivery, "current", lambda *_: True)
    monkeypatch.setattr(history_failure_reports, "flush_once", lambda *_: False)
    monkeypatch.setattr(
        history_delivery,
        "release",
        lambda *_, **options: releases.append(options) or True,
    )
    monkeypatch.setattr(publisher, "sync_references", lambda *_: None)

    assert publisher.run_once()
    assert publisher.admission_probe_after == 0.0
    assert publisher.run_once()
    assert claim_options == [
        {"allow_admission": False},
        {"prefer_admission": True},
    ]
    assert [request.method for request in requests] == ["GET", "POST"]
    assert stats == ["history_active_polls", "history_admission_attempts"]
    assert releases[0] == {"job_uuid": active["import_uuid"]}
    assert releases[1]["job_uuid"] == unregistered["import_uuid"]


@pytest.mark.parametrize("waiting", [1, 1495])
def test_busy_admission_rate_is_independent_of_waiting_queue_size(
    monkeypatch, waiting
):
    active = [publication_item(), publication_item()]
    unregistered = [publication_item(registered=False) for _ in range(waiting)]
    selections = [active[0], unregistered[0], active[1]]
    now = [0.0]
    requests = []
    claim_options = []

    def claim_next(_, **options):
        claim_options.append(options)
        if options.get("allow_admission") is False and selections:
            assert selections[0]["import_uuid"] is not None
        return selections.pop(0) if selections else None

    def respond(request):
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(429, json={"error": "history_import_busy"})
        item = active[0] if len(requests) == 1 else active[1]
        return httpx.Response(
            200,
            json={
                "uuid": str(item["import_uuid"]),
                "status": "waiting_files",
                "safe_error": None,
                "files": [],
            },
        )

    client = httpx.Client(
        transport=httpx.MockTransport(respond),
        base_url="https://workspace.example.test",
    )
    publisher = history_delivery.HistoryPublisher(
        object(),
        types.SimpleNamespace(client=client),
        None,
        clock=lambda: now[0],
    )
    publisher.supported = True
    monkeypatch.setattr(history_delivery, "claim", claim_next)
    monkeypatch.setattr(history_delivery, "current", lambda *_: True)
    monkeypatch.setattr(history_failure_reports, "flush_once", lambda *_: False)
    monkeypatch.setattr(history_delivery, "release", lambda *_, **__: True)

    assert publisher.run_once()
    assert publisher.run_once() is False
    assert publisher.run_once()
    for _ in range(100):
        assert publisher.run_once() is False
    assert [request.method for request in requests] == ["GET", "POST", "GET"]
    assert claim_options[:3] == [
        {},
        {"prefer_admission": True},
        {"allow_admission": False},
    ]

    now[0] = history_delivery.ADMISSION_COOLDOWN_SECONDS
    selections.append(unregistered[-1])
    assert publisher.run_once() is False
    assert claim_options[-1] == {"prefer_admission": True}
    assert [request.method for request in requests] == [
        "GET",
        "POST",
        "GET",
        "POST",
    ]


@pytest.mark.parametrize("code", ["55P03", "57014", "40001", "40P01"])
def test_routing_contention_releases_lease_without_stopping_publisher(
    monkeypatch, code
):
    class DatabaseError(Exception):
        sqlstate = code

    item = publication_item()
    publisher, _, releases = ready_publisher(
        monkeypatch,
        item,
        lambda _: httpx.Response(
            200, json={"uuid": str(item["import_uuid"]), "status": "complete"}
        ),
    )

    def contend(*_):
        raise DatabaseError()

    monkeypatch.setattr(publisher, "sync_references", contend)
    assert publisher.run_once() is False
    assert releases[0]["error"] == "history_database_contention"
    # Even release itself may contend; the lease expires and the live loop survives.
    monkeypatch.setattr(history_delivery, "release", lambda *_, **__: contend())
    assert publisher.run_once() is False
    monkeypatch.setattr(publisher, "sync_references", lambda *_: None)
    monkeypatch.setattr(
        history_delivery, "release", lambda *_, **kwargs: releases.append(kwargs)
    )
    assert publisher.run_once() is True
    assert releases[-1] == {"job_uuid": item["import_uuid"]}


def test_superseded_receipt_is_cleared_for_registration_retry(monkeypatch):
    item = publication_item()
    publisher, _, releases = ready_publisher(
        monkeypatch,
        item,
        lambda _: httpx.Response(
            200, json={"uuid": str(item["import_uuid"]), "status": "superseded"}
        ),
    )
    assert publisher.run_once() is False
    assert releases == [{"error": "history_receipt_superseded", "reset": True}]


@pytest.mark.parametrize("limit", [0, 1024])
def test_history_download_uses_current_provider_file_limit(monkeypatch, limit):
    calls = []

    def download(path, *, max_bytes):
        calls.append(max_bytes)
        raise zulip_adapter.ZulipOperationError(
            "provider_file_transfer_disabled"
            if max_bytes == 0
            else "provider_file_too_large",
            False,
        )

    publisher, requests, releases = waiting_file_publisher(
        monkeypatch, download, limit=lambda _: limit
    )
    assert publisher.run_once()
    assert calls == [limit]
    assert [request.method for request in requests] == ["GET", "POST"]
    assert requests[-1].url.path.endswith("/unavailable")
    assert len(releases) == 1


def test_slow_file_transfer_renews_lease_until_publisher_returns(monkeypatch):
    renewed = threading.Event()
    renewals = []

    def renew(*_):
        renewals.append(True)
        renewed.set()
        return True

    def download(*_, **__):
        assert renewed.wait(1)
        return zulip_adapter.ProviderFile("file.txt", "text/plain", b"fixture")

    publisher, requests, releases = waiting_file_publisher(monkeypatch, download)
    monkeypatch.setattr(history_delivery, "LEASE_RENEW_SECONDS", 0.01)
    monkeypatch.setattr(history_delivery, "renew_lease", renew)
    assert publisher.run_once()
    assert renewals
    assert [request.method for request in requests] == ["GET", "PUT"]
    assert requests[-1].content == b"fixture"
    assert len(releases) == 1
    count = len(renewals)
    time.sleep(0.03)
    assert len(renewals) == count


def test_file_limit_reduction_during_download_prevents_upload(monkeypatch):
    limits = iter([1024, 0])
    publisher, requests, releases = waiting_file_publisher(
        monkeypatch,
        lambda *_, **__: zulip_adapter.ProviderFile("file", "text/plain", b"fixture"),
        limit=lambda _: next(limits),
    )
    assert publisher.run_once()
    assert [request.method for request in requests] == ["GET", "POST"]
    assert requests[-1].url.path.endswith("/unavailable")
    assert len(releases) == 1


def test_slow_registration_renews_lease_until_mapping_synchronization(monkeypatch):
    renewed = threading.Event()
    calls = []
    job_uuid = uuid.uuid4()
    item = publication_item(registered=False)

    def renew(*_):
        calls.append(True)
        renewed.set()
        return True

    def respond(request):
        assert request.method == "POST"
        assert renewed.wait(1)
        return httpx.Response(200, json={"uuid": str(job_uuid), "status": "complete"})

    publisher, _, releases = ready_publisher(monkeypatch, item, respond)
    mappings = []
    publisher.sync_references = lambda client, value: mappings.append(
        value["import_uuid"]
    )
    monkeypatch.setattr(history_delivery, "renew_lease", renew)
    monkeypatch.setattr(history_delivery, "LEASE_RENEW_SECONDS", 0.01)
    assert publisher.run_once()
    assert mappings == [job_uuid]
    assert releases == [{"job_uuid": job_uuid}]
    count = len(calls)
    time.sleep(0.02)
    assert len(calls) == count


@pytest.mark.parametrize("status_code", [200, 400, 413, 422])
def test_permanent_history_failures_report_health_and_each_account_once(
    monkeypatch, status_code
):
    item = publication_item(registered=status_code == 200)
    source = item["envelope"]["sources"][0]
    item["envelope"]["sources"].append(dict(source))
    job_uuid = uuid.uuid4()
    health, reports = [], []
    publisher, _, releases = ready_publisher(
        monkeypatch,
        item,
        lambda _: httpx.Response(
            status_code,
            json={
                "uuid": str(job_uuid),
                "status": "failed",
                "safe_error": {
                    "code": "history_invalid_domain_value",
                    "message": "Untrusted provider details",
                },
            },
        ),
        store=TerminalStore(health),
        account_report=lambda *args, **kwargs: reports.append((args, kwargs)),
    )
    assert publisher.run_once() is False
    code = (
        "history_invalid_domain_value"
        if status_code == 200
        else "history_batch_rejected"
    )
    assert releases[0]["status"] == "failed"
    assert releases[0]["error"] == code
    assert health == [
        (history_configuration.health_component(item["scope"]), "degraded", code)
    ]
    assert reports == []
    assert publisher.store.work == [(*item["scope"][:2], item["generation"], code)]


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            {"code": "history_unsupported_scope", "message": "Private details"},
            "history_unsupported_scope",
        ),
        ({"code": "x" * 10000}, "history_import_failed"),
        ({"code": ["wrong", "type"]}, "history_import_failed"),
        ("x" * 10000, "history_import_failed"),
        ("Not a safe code", "history_import_failed"),
        (["unexpected"], "history_import_failed"),
        (42, "history_import_failed"),
        (None, None),
    ],
)
def test_release_persists_only_a_bounded_safe_error_code(value, expected):
    calls = []

    @contextlib.contextmanager
    def session():
        yield types.SimpleNamespace(
            execute=lambda sql, parameters: (
                calls.append(parameters)
                or types.SimpleNamespace(fetchone=lambda: {"result": 1})
            )
        )

    item = publication_item()
    history_delivery.release(types.SimpleNamespace(session=session), item, error=value)
    assert len(calls) == 1
    assert calls[0][4] == expected


def test_oversized_source_validation_fails_locally_before_http_post(monkeypatch):
    item = publication_item(registered=False)
    item["validation_error"] = "history_source_limit_exceeded"
    health, reports = [], []
    publisher, requests, releases = ready_publisher(
        monkeypatch,
        item,
        lambda _: pytest.fail("Invalid scope must not be sent to the backend"),
        store=TerminalStore(health),
        account_report=lambda *args, **kwargs: reports.append((args, kwargs)),
    )
    assert publisher.run_once() is False
    assert requests == []
    assert releases == [
        {"status": "failed", "job_uuid": None, "error": "history_source_limit_exceeded"}
    ]
    assert health and publisher.store.work
    assert reports == []


@pytest.mark.parametrize(
    "lost_at", ["complete_status", "mapping_response", "mapping_write"]
)
def test_complete_receipt_releases_lease_when_authority_disappears(
    monkeypatch, lost_at
):
    item = publication_item()

    def respond(request):
        if "/mappings/" in request.url.path:
            return httpx.Response(200, json={"mappings": [], "next_cursor": None})
        return httpx.Response(
            200, json={"uuid": str(item["import_uuid"]), "status": "complete"}
        )

    publisher, requests, releases = ready_publisher(monkeypatch, item, respond)
    authority = iter(
        {
            "complete_status": [True, False],
            "mapping_response": [True, True, False],
            "mapping_write": [True, True, True],
        }[lost_at]
    )
    monkeypatch.setattr(history_delivery, "current", lambda *_: next(authority))
    writes = []
    # save_references may independently detect changed authority under its lock.
    monkeypatch.setattr(
        history_delivery, "save_references", lambda *args: writes.append(args)
    )
    assert publisher.run_once()
    assert len(requests) == (1 if lost_at == "complete_status" else 2)
    assert bool(writes) is (lost_at == "mapping_write")
    assert releases == [{"job_uuid": item["import_uuid"]}]


def test_terminal_failure_commits_batch_health_and_durable_reporting_work(monkeypatch):
    item = publication_item()
    store = TerminalStore()
    reports = []

    def is_current(*_):
        assert store.active
        assert "LOCK TABLE desired_resources IN SHARE MODE" in store.sql
        assert "FOR SHARE" in store.sql[-1]
        return True

    def report(*args, **kwargs):
        pytest.fail("Terminal commit must not call account reports")

    monkeypatch.setattr(history_delivery, "current", is_current)
    publisher = history_delivery.HistoryPublisher(store, None, None, report)
    assert publisher.fail(item, {"code": "history_invalid_domain_value"})
    assert store.status == "failed"
    assert not store.owns_lease
    assert len(store.health) == len(store.work) == 1
    assert reports == []
    assert store.work[0] == (
        *item["scope"][:2],
        item["generation"],
        "history_invalid_domain_value",
    )


@pytest.mark.parametrize("reason", ["invalidated", "missing_scope", "lost_lease"])
def test_stale_terminal_failure_cannot_degrade_new_scope_or_account(
    monkeypatch, reason
):
    item = publication_item()
    store = TerminalStore(
        scope_exists=reason != "missing_scope", owns_lease=reason != "lost_lease"
    )
    reports = []
    monkeypatch.setattr(history_delivery, "current", lambda *_: reason != "invalidated")
    publisher = history_delivery.HistoryPublisher(
        store, None, None, lambda *args, **kwargs: reports.append((args, kwargs))
    )
    assert publisher.fail(item, "history_invalid_domain_value") is False
    assert store.status == "pending"
    assert not store.owns_lease
    assert store.health == reports == []


def test_health_write_failure_rolls_back_terminal_row_and_suppresses_report(
    monkeypatch,
):
    item = publication_item()
    store = TerminalStore()
    reports = []
    monkeypatch.setattr(history_delivery, "current", lambda *_: True)

    def unavailable_health(*_):
        assert store.active and store.status == "failed"
        raise RuntimeError("health_storage_unavailable")

    store.mark_health = unavailable_health
    publisher = history_delivery.HistoryPublisher(
        store, None, None, lambda *args, **kwargs: reports.append((args, kwargs))
    )
    with pytest.raises(RuntimeError, match="health_storage_unavailable"):
        publisher.fail(item, "history_invalid_domain_value")
    assert store.status == "pending" and store.owns_lease
    assert store.rollbacks == 1 and store.commits == 0
    assert store.health == reports == []


@pytest.mark.parametrize(
    "errors",
    [
        [("provider_unavailable", True), None],
        [("auth_required", False), None],
        [("provider_file_unavailable", False), None],
        [("provider_unavailable", True), ("provider_file_unavailable", False)],
        [("provider_file_unavailable", False), ("auth_required", False)],
        [("provider_file_unavailable", False), ("provider_file_unavailable", False)],
        [("provider_unavailable", True), ("provider_file_transfer_disabled", False)],
    ],
)
def test_file_download_tries_other_observers_without_losing_retryable_errors(
    monkeypatch, errors
):
    item = publication_item()
    accounts = [str(uuid.uuid4()), str(uuid.uuid4())]
    item["envelope"]["sources"] = [
        {"account_uuid": account, "account_generation": 1} for account in accounts
    ]
    request = {
        "uuid": str(uuid.uuid4()),
        "source_path": "/user_uploads/1/file",
        "account_uuids": [accounts[0], accounts[0], accounts[1]],
    }
    calls = []

    def adapter(account):
        def download(*_, **__):
            calls.append(account)
            error = errors[accounts.index(account)]
            if error is not None:
                raise zulip_adapter.ZulipOperationError(*error)
            return zulip_adapter.ProviderFile("file.txt", "text/plain", b"fixture")

        return types.SimpleNamespace(download_file=download)

    def respond(http_request):
        if http_request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "uuid": str(item["import_uuid"]),
                    "status": "waiting_files",
                    "safe_error": None,
                    "files": [request],
                },
            )
        return httpx.Response(200, json={"status": "ready"})

    publisher, requests, releases = ready_publisher(
        monkeypatch,
        item,
        respond,
        store=types.SimpleNamespace(effective_file_limit=lambda limit: limit),
        adapters=adapter,
    )
    should_retry = (
        errors[-1] is not None
        and any(
            error is not None and error[0] in {"provider_unavailable", "auth_required"}
            for error in errors
        )
        and errors[-1][0] != "provider_file_transfer_disabled"
    )
    assert publisher.run_once() is not should_retry
    assert calls == accounts
    if should_retry:
        assert [request.method for request in requests] == ["GET"]
        assert releases[0]["error"] == "history_delivery_unavailable"
    else:
        assert [request.method for request in requests] == [
            "GET",
            "PUT" if errors[-1] is None else "POST",
        ]


def test_failure_report_quantum_does_not_starve_ordinary_publication(monkeypatch):
    item = publication_item()
    publisher, requests, releases = ready_publisher(
        monkeypatch,
        item,
        lambda _: httpx.Response(
            200,
            json={
                "uuid": str(item["import_uuid"]),
                "status": "complete",
            },
        ),
    )
    reports = []
    monkeypatch.setattr(
        history_failure_reports, "flush_once", lambda *_: reports.append(True) or True
    )
    monkeypatch.setattr(publisher, "sync_references", lambda *_: None)
    assert publisher.run_once()
    assert reports == [True] and len(requests) == 1 and len(releases) == 1


@pytest.mark.parametrize("errors,expected", [
    ([("invalid_provider_file_length", False)] * 2, "failed"),
    ([("invalid_provider_file_url", False)] * 2, "failed"),
    ([("invalid_provider_file_length", False), ("provider_file_unavailable", False)], "failed"),
    ([("invalid_provider_file_length", False), None], "uploaded"),
    ([("provider_file_credentials_unavailable", False), None], "uploaded"),
    ([("invalid_provider_file_length", False), ("provider_file_credentials_unavailable", False)], "retry"),
    ([("provider_unavailable", True), ("invalid_provider_file_length", False)], "retry"),
    ([("invalid_provider_file_length", False), ("provider_file_unavailable", True)], "retry"),
])
def test_permanent_file_errors_fail_only_after_all_observers(monkeypatch, errors, expected):
    item = publication_item()
    accounts = [str(uuid.uuid4()), str(uuid.uuid4())]
    item["envelope"]["sources"] = [{"account_uuid": account} for account in accounts]
    request = {"uuid": str(uuid.uuid4()), "source_path": "/user_uploads/1/file", "account_uuids": accounts}
    calls, failures = [], []

    def adapter(account):
        def download(*_, **__):
            calls.append(account)
            error = errors[accounts.index(account)]
            if error is not None:
                raise zulip_adapter.ZulipOperationError(*error)
            return zulip_adapter.ProviderFile("example.txt", "text/plain", b"fixture")
        return types.SimpleNamespace(download_file=download)

    def respond(http_request):
        if http_request.method == "GET":
            return httpx.Response(200, json={"uuid": str(item["import_uuid"]), "status": "waiting_files", "safe_error": None, "files": [request]})
        return httpx.Response(200)

    publisher, requests, releases = ready_publisher(monkeypatch, item, respond,
        store=types.SimpleNamespace(effective_file_limit=lambda limit: limit), adapters=adapter)
    monkeypatch.setattr(publisher, "fail", lambda received, code: failures.append((received, code)))
    assert publisher.run_once() is (expected == "uploaded")
    assert calls == accounts
    if expected == "failed":
        assert failures == [(item, errors[0][0])]
        assert not releases
        assert [r.method for r in requests] == ["GET"]
    else:
        assert not failures
        assert [r.method for r in requests] == (["GET", "PUT"] if expected == "uploaded" else ["GET"])
    if expected == "retry":
        # Replacing credentials/provider recovery resumes the same receipt.
        errors[:] = [None, None]
        assert publisher.run_once()
        assert requests[-1].method == "PUT"


@pytest.mark.parametrize("path", ["https://example.test/file", "/other/path", None,
    "/user_uploads/../../api/v1/users", "/user_uploads/%252e%252e/api/v1/users",
    "/user_uploads/file%2fname", "/user_uploads/file%5cname"])
def test_invalid_history_file_path_is_terminal_before_provider_io(monkeypatch, path):
    item = publication_item()
    request = {"uuid": str(uuid.uuid4()), "source_path": path, "account_uuids": [item["envelope"]["sources"][0]["account_uuid"]]}
    publisher, requests, releases = ready_publisher(monkeypatch, item,
        lambda _: httpx.Response(200, json={"uuid": str(item["import_uuid"]), "status": "waiting_files", "safe_error": None, "files": [request]}),
        adapters=lambda _: pytest.fail("invalid path reached provider"))
    failed = []
    monkeypatch.setattr(publisher, "fail", lambda received, code: failed.append(code))
    assert not publisher.run_once()
    assert failed == ["invalid_provider_file_url"] and not releases
    assert [r.method for r in requests] == ["GET"]
