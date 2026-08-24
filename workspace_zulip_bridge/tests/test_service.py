import contextlib
import datetime
import pathlib
import threading
import time
import uuid

import certifi
import httpx
import pytest

from workspace_zulip_bridge import (
    control,
    converter,
    service,
    zulip_adapter,
)


class SnapshotControl:
    def __init__(self):
        self.pages = []

    def create_snapshot(self):
        return {"snapshot_token": "token", "anchor_cursor": "anchor"}

    def snapshot_page(self, token, cursor):
        self.pages.append(cursor)
        if cursor is None:
            return {
                "resources": [{"resource_type": "external_account", "uuid": "a"}],
                "next_page_cursor": "page-2",
            }
        return {
            "resources": [{"resource_type": "custom_ca_bundle", "uuid": "b"}],
            "next_page_cursor": None,
        }


class SnapshotStore:
    def __init__(self, cursor):
        self.cursor = cursor
        self.installed = []
        self.stale_recoveries = 0

    def control_cursor(self):
        return self.cursor

    def install_snapshot(self, resources, anchor):
        self.installed.append((resources, anchor))

    def reset_stale_workspace_deliveries(self):
        self.stale_recoveries += 1


def _service(store, api):
    instance = object.__new__(service.BridgeService)
    instance.store = store
    instance.control = api
    return instance


def test_adapter_registry_does_not_retain_decrypted_credentials(monkeypatch):
    account_uuid = "00000000-0000-0000-0000-000000000001"

    class Store:
        def provider_is_enabled(self, provider_kind):
            return True

        def custom_ca_bundle(self, provider_kind):
            return None

        def desired_resource(self, resource_type, resource_uuid):
            return {
                "synchronization_enabled": True,
                "generation": 7,
                "owner_user_uuid": "00000000-0000-0000-0000-000000000002",
                "credential_envelope": {
                    "associated_data": {"account_generation": 7},
                    "ciphertext": "opaque",
                },
            }

    class Decryptor:
        def __init__(self):
            self.calls = 0

        def decrypt(self, *args):
            self.calls += 1
            return zulip_adapter.ZulipCredentials("https://zulip.invalid", "e", "k")

    created = []

    class Adapter:
        def __init__(self, credentials, **kwargs):
            created.append(self)
            self.account_generation = kwargs["account_generation"]

    monkeypatch.setattr(zulip_adapter, "OfficialZulipAdapter", Adapter)
    decryptor = Decryptor()
    registry = service.AdapterRegistry(Store(), decryptor)
    first = registry(account_uuid)
    second = registry(account_uuid)
    assert first is not second
    assert decryptor.calls == 2
    assert not hasattr(registry, "cache")
    assert first.account_generation == second.account_generation == 7


def test_adapter_registry_uses_encrypted_credential_generation(monkeypatch):
    account_uuid = "00000000-0000-0000-0000-000000000001"
    owner_uuid = "00000000-0000-0000-0000-000000000002"
    envelope = {
        "associated_data": {"account_generation": 7},
        "ciphertext": "opaque",
    }

    class Store:
        def provider_is_enabled(self, provider_kind):
            return True

        def custom_ca_bundle(self, provider_kind):
            return None

        def desired_resource(self, resource_type, resource_uuid):
            return {
                "synchronization_enabled": True,
                "generation": 8,
                "owner_user_uuid": owner_uuid,
                "credential_envelope": envelope,
            }

    class Decryptor:
        def __init__(self):
            self.calls = []

        def decrypt(self, *args):
            self.calls.append(args)
            return zulip_adapter.ZulipCredentials("https://zulip.invalid", "e", "k")

    class Adapter:
        def __init__(self, credentials, **kwargs):
            pass

    monkeypatch.setattr(zulip_adapter, "OfficialZulipAdapter", Adapter)
    decryptor = Decryptor()
    service.AdapterRegistry(Store(), decryptor)(account_uuid)

    assert decryptor.calls == [(account_uuid, owner_uuid, 7, envelope)]


@pytest.mark.parametrize(
    "credential_generation",
    [0, 8, True, "7"],
)
def test_adapter_registry_rejects_invalid_credential_generation(
    monkeypatch, credential_generation
):
    class Store:
        def provider_is_enabled(self, provider_kind):
            return True

        def desired_resource(self, resource_type, resource_uuid):
            return {
                "synchronization_enabled": True,
                "generation": 7,
                "owner_user_uuid": "00000000-0000-0000-0000-000000000002",
                "credential_envelope": {
                    "associated_data": {
                        "account_generation": credential_generation,
                    }
                },
            }

    class Decryptor:
        def decrypt(self, *args):
            raise AssertionError("invalid credential generation must fail closed")

    registry = service.AdapterRegistry(Store(), Decryptor())
    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        registry("00000000-0000-0000-0000-000000000001")

    assert error.value.code == "unauthorized_account"
    assert not error.value.retryable
    assert error.value.account_generation == 7


def test_adapter_registry_isolates_credential_decryption_failure():
    class Store:
        def provider_is_enabled(self, provider_kind):
            return True

        def desired_resource(self, resource_type, resource_uuid):
            return {
                "synchronization_enabled": True,
                "generation": 7,
                "owner_user_uuid": "00000000-0000-0000-0000-000000000002",
                "credential_envelope": {
                    "associated_data": {"account_generation": 7},
                },
            }

    class Decryptor:
        def decrypt(self, *args):
            raise ValueError("Credential associated data mismatch")

    registry = service.AdapterRegistry(Store(), Decryptor())
    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        registry("00000000-0000-0000-0000-000000000001")

    assert error.value.code == "unauthorized_account"
    assert not error.value.retryable


def test_adapter_registry_combines_system_and_managed_provider_ca(
    tmp_path, monkeypatch
):
    custom_ca = (
        pathlib.Path(certifi.where())
        .read_text(encoding="ascii")
        .partition("-----END CERTIFICATE-----")[0]
        + "-----END CERTIFICATE-----\n"
    )

    class Store:
        def provider_is_enabled(self, provider_kind):
            return True

        def custom_ca_bundle(self, provider_kind):
            return {"certificates_pem": [custom_ca]}

        def desired_resource(self, resource_type, resource_uuid):
            return {
                "synchronization_enabled": True,
                "generation": 1,
                "owner_user_uuid": "00000000-0000-0000-0000-000000000002",
                "credential_envelope": {"associated_data": {"account_generation": 1}},
            }

    class Decryptor:
        def decrypt(self, *args):
            return zulip_adapter.ZulipCredentials("https://zulip.invalid", "e", "k")

    created = []

    class Adapter:
        def __init__(self, adapter_credentials, **kwargs):
            created.append(adapter_credentials)

    monkeypatch.setattr(zulip_adapter, "OfficialZulipAdapter", Adapter)
    registry = service.AdapterRegistry(Store(), Decryptor(), tmp_path / "ca")
    registry("00000000-0000-0000-0000-000000000001")

    bundle = pathlib.Path(created[0].cert_bundle).read_text(encoding="ascii")
    assert bundle.startswith(pathlib.Path(certifi.where()).read_text(encoding="ascii"))
    assert bundle.endswith(custom_ca)


def test_adapter_registry_fails_closed_when_provider_is_suspended():
    class Store:
        def provider_is_enabled(self, provider_kind):
            return False

    class Decryptor:
        def decrypt(self, *args):
            raise AssertionError("credentials must not be decrypted while suspended")

    registry = service.AdapterRegistry(Store(), Decryptor())
    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        registry("00000000-0000-0000-0000-000000000001")
    assert error.value.code == "provider_suspended"
    assert error.value.retryable


def test_zb_control_001_snapshot_pages_are_collected_before_atomic_install():
    store = SnapshotStore("")
    api = SnapshotControl()
    _service(store, api).synchronize_control()
    assert api.pages == [None, "page-2"]
    assert [resource["uuid"] for resource in store.installed[0][0]] == ["a", "b"]
    assert store.installed[0][1] == "anchor"
    assert store.stale_recoveries == 1


def test_zb_control_001_expired_cursor_does_not_install_empty_reset():
    store = SnapshotStore("expired")
    api = SnapshotControl()

    def expired(cursor):
        raise control.ControlCursorExpired

    api.desired_changes = expired
    _service(store, api).synchronize_control()
    assert len(store.installed) == 1
    assert len(store.installed[0][0]) == 2


def test_control_snapshot_repeated_cursor_preserves_installed_state():
    class RepeatingControl(SnapshotControl):
        def snapshot_page(self, token, cursor):
            self.pages.append(cursor)
            return {
                "resources": [{"resource_type": "external_account", "uuid": "new"}],
                "next_page_cursor": "repeat",
            }

    store = SnapshotStore("")
    store.installed = [([{"uuid": "old"}], "old-anchor")]

    with pytest.raises(ValueError, match="cursor repeated"):
        _service(store, RepeatingControl()).synchronize_control()

    assert store.installed == [([{"uuid": "old"}], "old-anchor")]


def test_control_snapshot_page_guard_preserves_installed_state(monkeypatch):
    store = SnapshotStore("")
    store.installed = [([{"uuid": "old"}], "old-anchor")]
    monkeypatch.setattr(service.BridgeService, "MAX_CONTROL_SNAPSHOT_PAGES", 1)

    with pytest.raises(ValueError, match="page limit exceeded"):
        _service(store, SnapshotControl()).synchronize_control()

    assert store.installed == [([{"uuid": "old"}], "old-anchor")]


def test_control_snapshot_resource_guard_preserves_installed_state(monkeypatch):
    store = SnapshotStore("")
    store.installed = [([{"uuid": "old"}], "old-anchor")]
    monkeypatch.setattr(service.BridgeService, "MAX_CONTROL_SNAPSHOT_RESOURCES", 0)

    with pytest.raises(ValueError, match="resource limit exceeded"):
        _service(store, SnapshotControl()).synchronize_control()

    assert store.installed == [([{"uuid": "old"}], "old-anchor")]


def test_retryable_longpoll_error_defers_only_failing_account(monkeypatch):
    accounts = ["account-a", "account-b"]
    healthy_started = threading.Event()
    healthy_release = threading.Event()
    healthy_stop_release = threading.Event()

    class Store:
        def __init__(self):
            self.recorded = []
            self.cursors = []
            self.health = []

        def active_account_uuids(self):
            return accounts

        def provider_event_cursor(self, account_uuid):
            return {"queue_id": f"queue-{account_uuid}", "last_event_id": 4}

        def account_resource(self, account_uuid):
            return None

        def record_provider_event(self, account_uuid, queue_id, event):
            self.recorded.append((account_uuid, queue_id, event["id"]))

        def update_provider_event_cursor(self, account_uuid, queue_id, event_id):
            self.cursors.append((account_uuid, queue_id, event_id))

        def mark_health(self, component, status, code=None):
            self.health.append((component, status, code))

    class Adapter:
        def __init__(self, account_uuid):
            self.account_uuid = account_uuid
            self.calls = 0

        def restore_queue(self, queue_id, last_event_id):
            return None

        def events(self, queue_id, last_event_id):
            if self.account_uuid == "account-a":
                raise zulip_adapter.ZulipOperationError("provider_unavailable", True)
            self.calls += 1
            if self.calls == 1:
                healthy_started.set()
                assert healthy_release.wait(timeout=2)
                return [{"id": 5, "type": "realm_user"}]
            assert healthy_stop_release.wait(timeout=2)
            return [{"id": 6, "type": "heartbeat"}]

    class FixedRandom:
        def uniform(self, lower, upper):
            return upper

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = Adapter
    instance.scheduler = type(
        "Scheduler", (), {"reconcile_local_echo": lambda *args: None}
    )()
    instance.provider_retry_attempts = {}
    instance.provider_retry_after = {}
    instance.provider_random = FixedRandom()
    monkeypatch.setattr(time, "monotonic", lambda: 100.0)

    assert instance.poll_provider_events() == 0
    assert set(instance.provider_poll_threads) == set(accounts)
    assert {thread.name for thread in instance.provider_poll_threads.values()} == {
        "zulip-live-account--nt-a",
        "zulip-live-account--nt-b",
    }
    assert healthy_started.wait(timeout=1)
    healthy_release.set()
    deadline = time.time() + 1
    while len(instance.store.recorded) < 1 and time.time() < deadline:
        time.sleep(0.01)
    deadline = time.time() + 1
    while not instance.provider_poll_results.qsize() and time.time() < deadline:
        time.sleep(0.01)
    assert instance.poll_provider_events() == 1
    assert instance.store.recorded == [("account-b", "queue-account-b", 5)]
    assert instance.provider_retry_attempts == {"account-a": 1}
    assert instance.provider_retry_after["account-a"] > 100.0

    instance.provider_poll_stops["account-b"].set()
    healthy_stop_release.set()
    instance.provider_poll_threads["account-b"].join(timeout=1)
    assert not instance.provider_poll_threads["account-b"].is_alive()


def test_empty_successful_longpoll_recovers_health():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    first_returned = threading.Event()
    stop_release = threading.Event()

    class Store:
        def __init__(self):
            self.health = []

        def active_account_uuids(self):
            return [account_uuid]

        def provider_event_cursor(self, requested):
            assert requested == account_uuid
            return {"queue_id": "queue", "last_event_id": -1}

        def account_resource(self, requested):
            assert requested == account_uuid
            return None

        def mark_health(self, component, status, code=None):
            self.health.append((component, status, code))

    class Adapter:
        def __init__(self):
            self.calls = 0

        def restore_queue(self, queue_id, last_event_id):
            assert (queue_id, last_event_id) == ("queue", -1)

        def events(self, queue_id, last_event_id):
            self.calls += 1
            if self.calls == 1:
                first_returned.set()
                return []
            assert stop_release.wait(timeout=2)
            return []

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.scheduler = type(
        "Scheduler", (), {"reconcile_local_echo": lambda *args: None}
    )()
    instance.provider_retry_attempts = {account_uuid: 1}
    instance.provider_retry_after = {account_uuid: 0.0}
    instance.provider_random = type(
        "Random", (), {"uniform": lambda self, lower, upper: lower}
    )()
    instance.provider_poll_interval_seconds = 0.01

    assert instance.poll_provider_events() == 0
    assert first_returned.wait(timeout=1)
    deadline = time.time() + 1
    while not instance.provider_poll_results.qsize() and time.time() < deadline:
        time.sleep(0.01)
    assert instance.poll_provider_events() == 0
    assert instance.provider_retry_attempts == {}
    assert instance.provider_retry_after == {}
    assert instance.store.health == [("provider", "healthy", None)]
    instance.provider_poll_stops[account_uuid].set()
    stop_release.set()
    instance.provider_poll_threads[account_uuid].join(timeout=1)


def test_account_provider_poll_is_throttled_between_empty_responses():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    first_returned = threading.Event()
    second_started = threading.Event()

    class Store:
        def provider_event_cursor(self, requested):
            assert requested == account_uuid
            return {"queue_id": "queue", "last_event_id": 4}

        def account_resource(self, requested):
            return None

    class Adapter:
        def __init__(self):
            self.calls = 0

        def restore_queue(self, queue_id, last_event_id):
            return None

        def events(self, queue_id, last_event_id):
            self.calls += 1
            if self.calls == 1:
                first_returned.set()
            else:
                second_started.set()
            return []

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.scheduler = type(
        "Scheduler", (), {"reconcile_local_echo": lambda *args: None}
    )()
    instance.provider_poll_results = service.queue.SimpleQueue()
    instance.provider_poll_interval_seconds = 0.1
    stop = threading.Event()
    thread = threading.Thread(
        target=instance._run_provider_account_poll,
        args=(account_uuid, stop),
    )
    thread.start()

    assert first_returned.wait(timeout=1)
    assert not second_started.wait(timeout=0.03)
    assert second_started.wait(timeout=0.2)
    stop.set()
    thread.join(timeout=1)
    assert not thread.is_alive()


def test_live_event_is_persisted_before_incomplete_queue_catchup():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    calls = []

    class Store:
        def account_resource(self, requested):
            assert requested == account_uuid
            return None

        def provider_event_cursor(self, requested):
            assert requested == account_uuid
            return {"queue_id": "queue", "last_event_id": 4}

        def record_provider_event(self, requested, queue_id, event):
            calls.append(("record", requested, queue_id, event["id"]))

        def update_provider_event_cursor(self, requested, queue_id, event_id):
            calls.append(("cursor", requested, queue_id, event_id))

    class Adapter:
        def restore_queue(self, queue_id, last_event_id):
            calls.append(("restore", queue_id, last_event_id))

        def events(self, queue_id, last_event_id):
            calls.append(("events", queue_id, last_event_id))
            return [{"id": 5, "type": "realm_user"}]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.scheduler = type(
        "Scheduler", (), {"reconcile_local_echo": lambda *args: None}
    )()
    instance._run_provider_queue_catchup = lambda requested, adapter: (
        calls.append(("catchup", requested)) or False
    )
    instance._queue_account_report = lambda requested, status: calls.append(
        ("report", requested, status)
    )

    assert instance._poll_provider_account(account_uuid) == (1, None)
    assert calls == [
        ("restore", "queue", 4),
        ("events", "queue", 4),
        ("record", account_uuid, "queue", 5),
        ("cursor", account_uuid, "queue", 5),
        ("report", account_uuid, "backfill"),
    ]


def test_heartbeat_advances_cursor_without_journaling_an_operation():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    calls = []

    class Store:
        def account_resource(self, requested):
            return None

        def provider_event_cursor(self, requested):
            return {"queue_id": "queue", "last_event_id": 4}

        def record_provider_event(self, *args):
            calls.append(("record", args))

        def update_provider_event_cursor(self, requested, queue_id, event_id):
            calls.append(("cursor", requested, queue_id, event_id))

    class Adapter:
        def restore_queue(self, queue_id, last_event_id):
            return None

        def events(self, queue_id, last_event_id):
            return [{"id": 5, "type": "heartbeat"}]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()

    assert instance._poll_provider_account(account_uuid) == (0, None)
    assert calls == [("cursor", account_uuid, "queue", 5)]


def test_account_poll_enables_configured_provider_long_polling():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    calls = []

    class Store:
        def account_resource(self, requested):
            return None

        def provider_event_cursor(self, requested):
            return {"queue_id": "queue", "last_event_id": 4}

        def update_provider_event_cursor(self, requested, queue_id, event_id):
            calls.append(("cursor", requested, queue_id, event_id))

    class Adapter:
        def restore_queue(self, queue_id, last_event_id):
            return None

        def events(self, queue_id, last_event_id, *, long_polling=False):
            calls.append(("events", queue_id, last_event_id, long_polling))
            return [{"id": 5, "type": "heartbeat"}]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.provider_event_long_polling = True

    assert instance._poll_provider_account(account_uuid) == (0, None)
    assert calls == [
        ("events", "queue", 4, True),
        ("cursor", account_uuid, "queue", 5),
    ]


def test_adapter_connection_failure_is_reported_from_account_poll(monkeypatch):
    failed_account = "00000000-0000-4000-8000-000000000001"

    class Store:
        def provider_event_cursor(self, account_uuid):
            return {"queue_id": f"queue-{account_uuid}", "last_event_id": 4}

        def account_resource(self, account_uuid):
            return None

    class UnreachableClient:
        def __init__(self, **kwargs):
            raise zulip_adapter.zulip.UnrecoverableNetworkError("offline")

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda account_uuid: (
        zulip_adapter.OfficialZulipAdapter(
            credentials=zulip_adapter.ZulipCredentials(
                "https://unresolvable.example.invalid",
                "user@example.invalid",
                "opaque-api-key",
            )
        )
    )
    monkeypatch.setattr(zulip_adapter.zulip, "Client", UnreachableClient)

    processed, error = instance._poll_provider_account(failed_account)
    assert processed == 0
    assert error is not None
    assert error.code == "provider_unavailable"


def test_each_active_account_gets_a_dedicated_longpoll_thread():
    accounts = [str(uuid.UUID(int=index + 1)) for index in range(3)]
    started = {account_uuid: threading.Event() for account_uuid in accounts}
    release = threading.Event()

    class Store:
        def active_account_uuids(self):
            return accounts

        def provider_event_cursor(self, account_uuid):
            return {"queue_id": f"queue-{account_uuid}", "last_event_id": 0}

        def account_resource(self, account_uuid):
            return None

        def mark_health(self, *args):
            return None

    adapters = []

    class Adapter:
        def __init__(self, account_uuid):
            self.account_uuid = account_uuid
            adapters.append((account_uuid, self))

        def restore_queue(self, queue_id, last_event_id):
            return None

        def events(self, queue_id, last_event_id):
            started[self.account_uuid].set()
            assert release.wait(timeout=2)
            return [{"id": 1, "type": "heartbeat"}]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = Adapter
    instance.provider_retry_attempts = {}
    instance.provider_retry_after = {}
    instance.provider_random = type(
        "Random", (), {"uniform": lambda self, lower, upper: lower}
    )()

    assert instance.poll_provider_events() == 0
    assert all(event.wait(timeout=1) for event in started.values())
    assert set(instance.provider_poll_threads) == set(accounts)
    assert (
        len({thread.ident for thread in instance.provider_poll_threads.values()}) == 3
    )
    assert all(
        thread.name == f"zulip-live-{account_uuid[:8]}-{account_uuid[-4:]}"
        for account_uuid, thread in instance.provider_poll_threads.items()
    )
    assert len(adapters) == len(accounts)
    assert len({id(adapter) for _, adapter in adapters}) == len(accounts)
    for stop in instance.provider_poll_stops.values():
        stop.set()
    release.set()
    for thread in instance.provider_poll_threads.values():
        thread.join(timeout=1)


class DeliveryStore:
    def __init__(self, events=None):
        self.events = events or []
        self.enqueued = []
        self.processed = []
        self.invalid = []
        self.retried = []

    def account_is_active(self, account_uuid):
        return True

    def account_resource(self, account_uuid):
        return None

    def assignment_for_provider_chat(self, account_uuid, provider_chat_key):
        return {
            "uuid": "00000000-0000-4000-8000-000000000090",
            "generation": 1,
        }

    def pending_provider_events(self, limit):
        assert limit == service.BridgeService.PROVIDER_JOURNAL_QUANTUM
        events, self.events = self.events, []
        return events

    def enqueue_workspace_delivery(self, record, priority):
        self.enqueued.append((record, priority))
        return True

    def retry_provider_event(self, account_uuid, queue_id, event_id, reason):
        self.retried.append((account_uuid, queue_id, event_id, reason))

    def mark_health(self, component, status, code=None):
        pass

    def mark_provider_event_processed(
        self, account_uuid, queue_id, event_id, supported
    ):
        self.processed.append((account_uuid, queue_id, event_id, supported))

    def finalize_provider_event(
        self, account_uuid, queue_id, event_id, supported, deleted_message_ids
    ):
        self.processed.append((account_uuid, queue_id, event_id, supported))

    def mark_provider_event_invalid(self, account_uuid, queue_id, event_id, reason):
        self.invalid.append((account_uuid, queue_id, event_id, reason))


def test_provider_journal_recovers_interrupted_deliveries_only_once():
    class Store(DeliveryStore):
        def __init__(self):
            super().__init__()
            self.ambiguous_recoveries = 0
            self.stale_recoveries = 0

        def mark_interrupted_workspace_deliveries_ambiguous(self):
            self.ambiguous_recoveries += 1

        def reset_stale_workspace_deliveries(self):
            self.stale_recoveries += 1

    store = Store()
    instance = _delivery_service(store)

    assert instance.process_provider_journal() == 0
    assert instance.process_provider_journal() == 0
    assert store.ambiguous_recoveries == 1
    assert store.stale_recoveries == 1


def test_provider_journal_processes_distinct_account_heads_in_parallel():
    instance = _delivery_service(DeliveryStore([]))
    instance.provider_batch_size = 20
    worker_count = instance._provider_journal_worker_count()
    barrier = threading.Barrier(worker_count)
    worker_threads = set()
    worker_threads_lock = threading.Lock()
    events = [
        {
            "account_uuid": f"00000000-0000-4000-8000-{index:012d}",
            "queue_id": f"queue-{index}",
            "event_id": 1,
            "body": {"id": 1, "type": "heartbeat"},
        }
        for index in range(worker_count)
    ]

    class Store(DeliveryStore):
        def account_is_active(self, account_uuid):
            with worker_threads_lock:
                worker_threads.add(threading.get_ident())
            barrier.wait(timeout=2)
            return False

    instance = _delivery_service(Store(events))
    instance.provider_batch_size = 20
    instance.provider_journal_parallel_enabled = True

    assert instance.process_provider_journal() == 0
    assert len(worker_threads) == worker_count
    instance.provider_journal_executor.shutdown(wait=True)


def test_large_profile_scales_live_conversion_and_delivery_batches():
    instance = object.__new__(service.BridgeService)
    instance.provider_batch_size = 100

    assert instance._provider_journal_worker_count() == 16
    assert instance._live_delivery_batch_size() == 100
    assert instance._history_worker_count() == 8
    assert instance._history_delivery_batch_size(live_pending=False) == 100
    assert instance._history_delivery_batch_size(live_pending=True) == 1


def test_run_recovers_interrupted_deliveries_before_starting_workers(monkeypatch):
    class StopRun(Exception):
        pass

    class Store:
        def release_dependency_gated_provider_events(self):
            return None

        def mark_interrupted_workspace_deliveries_ambiguous(self):
            return None

        def reset_stale_workspace_deliveries(self):
            return None

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.tick = lambda: (_ for _ in ()).throw(StopRun())
    started = []

    class Thread:
        def __init__(self, *, target, name, daemon):
            self.name = name

        def start(self):
            assert instance._workspace_delivery_recovery_done
            assert instance.background_live_delivery_enabled
            started.append(self.name)

    monkeypatch.setattr(service.threading, "Thread", Thread)

    with pytest.raises(StopRun):
        instance.run()

    assert len(started) == 5
    assert "workspace-zulip-heartbeat" in started
    assert len([name for name in started if "live-delivery" in name]) == 1


def test_live_delivery_dependency_stall_updates_health_once_and_recovers():
    health = []

    class Store:
        def mark_health(self, component, status, code=None):
            health.append((component, status, code))

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.LIVE_DELIVERY_STALL_THRESHOLD_SECONDS = 5.0

    instance._record_live_delivery_stall(10.0)
    instance._record_live_delivery_stall(14.9)
    assert health == []

    instance._record_live_delivery_stall(15.0)
    instance._record_live_delivery_stall(20.0)
    assert health == [
        (
            "provider_delivery",
            "degraded",
            "workspace_delivery_dependency_stalled",
        )
    ]

    instance._clear_live_delivery_stall()
    instance._clear_live_delivery_stall()
    assert health[-1] == ("provider_delivery", "healthy", None)
    assert len(health) == 2


def test_background_heartbeat_lane_surfaces_unexpected_failure(monkeypatch):
    class StopHeartbeat(Exception):
        pass

    instance = object.__new__(service.BridgeService)
    instance._run_heartbeat = lambda _now: (_ for _ in ()).throw(StopHeartbeat())
    monkeypatch.setattr(
        service.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(
            AssertionError("failed heartbeat must stop its lane")
        ),
    )

    instance._run_background_heartbeat_lane()

    assert isinstance(instance.background_heartbeat_error, StopHeartbeat)


def test_tick_skips_heartbeat_owned_by_background_lane(tmp_path, monkeypatch):
    now = [10.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    instance = object.__new__(service.BridgeService)
    instance.background_heartbeat_enabled = True
    instance.background_heartbeat_error = None
    instance.background_live_enabled = True
    instance.background_live_delivery_enabled = True
    instance.background_history_enabled = True
    instance.last_certificate_check = now[0]
    instance.last_control_state_reconcile = now[0]
    instance.control_state_dirty = False
    instance.last_provider_poll = now[0]
    instance.last_terminal_state_prune = now[0]
    instance.health_file = tmp_path / "progress"
    instance.certificate_renewer = None
    instance._run_heartbeat = lambda _now: (_ for _ in ()).throw(
        AssertionError("main tick must not share the heartbeat client lane")
    )
    instance._run_control_poll = lambda _now: False
    instance.poll_provider_events = lambda: 0
    instance._flush_observed_reports = lambda _now: 0
    instance.refresh_selected_participants_once = lambda: False
    instance.store = object()

    assert not instance.tick()
    assert instance.health_file.is_file()


def test_inactive_account_event_is_terminalized_without_provider_access():
    account_uuid = "00000000-0000-4000-8000-000000000001"

    class Store(DeliveryStore):
        def __init__(self):
            super().__init__(
                [
                    {
                        "account_uuid": account_uuid,
                        "queue_id": "retired-queue",
                        "event_id": 7,
                        "body": {"id": 7, "type": "message"},
                    }
                ]
            )
            self.ignored = []

        def account_is_active(self, requested):
            return False

        def ignore_provider_event_for_inactive_account(
            self, requested, queue_id, event_id
        ):
            self.ignored.append((requested, queue_id, event_id))
            return True

    store = Store()
    instance = _delivery_service(store)
    instance.provider_adapters = lambda requested: pytest.fail(
        "inactive accounts must not instantiate a Zulip client"
    )

    assert instance.process_provider_journal() == 1
    assert store.ignored == [(account_uuid, "retired-queue", 7)]


class ProviderAdapter:
    server_url = "https://zulip.example.invalid"


class CatchupStore(DeliveryStore):
    def __init__(self):
        super().__init__()
        self.job = {
            "provider_chat_key": "channel:42",
            "checkpoint_provider_message_id": 11,
            "next_anchor": None,
            "seen_provider_message_ids": [],
            "page_count": 0,
        }
        common = {
            "project_uuid": "00000000-0000-0000-0000-000000000002",
            "stream_uuid": "00000000-0000-0000-0000-000000000003",
            "topic_uuid": "00000000-0000-0000-0000-000000000004",
            "author_uuid": "00000000-0000-0000-0000-000000000005",
            "chat_key": "channel:42",
            "subject": "Topic",
            "workspace_delivery_state": "committed",
        }
        self.mappings = {
            "10": {
                "workspace_uuid": "00000000-0000-0000-0000-000000000010",
                "provider_id": "10",
                "metadata": {**common, "provider_content_sha256": "old"},
            },
            "11": {
                "workspace_uuid": "00000000-0000-0000-0000-000000000011",
                "provider_id": "11",
                "metadata": {**common, "provider_content_sha256": "deleted"},
            },
        }
        self.created = []
        self.advanced = []

    def pending_provider_catchup(self, account_uuid):
        return self.job

    def provider_catchup_ready(self, account_uuid):
        return self.job is None

    def provider_mapping(self, account_uuid, entity_kind, provider_id):
        return self.mappings.get(provider_id)

    def mapped_provider_messages(self, account_uuid, chat_key, minimum_id):
        return [
            mapping
            for provider_id, mapping in self.mappings.items()
            if int(provider_id) >= minimum_id
        ]

    def advance_provider_catchup(
        self, account_uuid, chat_key, seen_ids, next_anchor, complete, error=None
    ):
        self.advanced.append((seen_ids, next_anchor, complete, error))
        if complete:
            self.job = None

    def enqueue_workspace_delivery(self, record, priority):
        if any(
            existing[0]["operation_uuid"] == record["operation_uuid"]
            for existing in self.enqueued
        ):
            return False
        self.enqueued.append((record, priority))
        return True


class CatchupAdapter(ProviderAdapter):
    def message_history(self, chat_key, anchor="newest"):
        return [
            {"id": 12, "timestamp": 12, "content": "new", "subject": "Topic"},
            {"id": 13, "timestamp": 13, "content": "newer", "subject": "Topic"},
            {
                "id": 10,
                "timestamp": 10,
                "last_edit_timestamp": 13,
                "content": "edited",
                "subject": "Topic",
                "stream_id": 42,
            },
        ]


def _delivery_service(store):
    instance = object.__new__(service.BridgeService)
    instance.store = store
    instance.file_client = None
    instance.provider_adapters = lambda account_uuid: ProviderAdapter()
    return instance


def test_provider_journal_enqueues_live_before_marking_event(monkeypatch):
    store = DeliveryStore(
        [
            {
                "account_uuid": "00000000-0000-0000-0000-000000000001",
                "queue_id": "queue",
                "event_id": 7,
                "body": {"id": 7, "type": "realm_user", "person": {}},
            }
        ]
    )
    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [{"record_uuid": "record"}],
    )
    assert _delivery_service(store).process_provider_journal() == 1
    assert store.enqueued == [({"record_uuid": "record"}, 0)]
    assert store.processed == [
        ("00000000-0000-0000-0000-000000000001", "queue", 7, True)
    ]


def test_provider_journal_retries_unexpected_row_failure(monkeypatch):
    account_uuid = "00000000-0000-4000-8000-000000000001"
    store = DeliveryStore(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "retry_count": 0,
                "body": {"id": 7, "type": "realm_user", "person": {}},
            }
        ]
    )
    instance = _delivery_service(store)
    monkeypatch.setattr(
        instance,
        "_event_records_with_file_fallback",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("unexpected conversion failure")
        ),
    )

    assert instance.process_provider_journal() == 0
    assert store.retried == [
        (account_uuid, "queue", 7, "provider_event_processing_failed")
    ]
    assert store.invalid == []


def test_provider_journal_terminalizes_repeated_unexpected_row_failure(monkeypatch):
    account_uuid = "00000000-0000-4000-8000-000000000001"
    store = DeliveryStore(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "retry_count": (
                    service.BridgeService.PROVIDER_EVENT_FAILURE_MAX_ATTEMPTS - 1
                ),
                "processing_reason": "provider_event_processing_failed",
                "body": {"id": 7, "type": "realm_user", "person": {}},
            }
        ]
    )
    instance = _delivery_service(store)
    monkeypatch.setattr(
        instance,
        "_event_records_with_file_fallback",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("unexpected conversion failure")
        ),
    )

    assert instance.process_provider_journal() == 1
    assert store.retried == []
    assert store.invalid == [
        (account_uuid, "queue", 7, "provider_event_processing_failed")
    ]


def test_dependency_retries_do_not_count_toward_unexpected_failure_limit(
    monkeypatch,
):
    account_uuid = "00000000-0000-4000-8000-000000000001"
    store = DeliveryStore(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "retry_count": (
                    service.BridgeService.PROVIDER_EVENT_FAILURE_MAX_ATTEMPTS - 1
                ),
                "processing_reason": "provider_chat_assignment_pending",
                "body": {"id": 7, "type": "realm_user", "person": {}},
            }
        ]
    )
    instance = _delivery_service(store)
    monkeypatch.setattr(
        instance,
        "_event_records_with_file_fallback",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("first unexpected conversion failure")
        ),
    )

    assert instance.process_provider_journal() == 0
    assert store.retried == [
        (account_uuid, "queue", 7, "provider_event_processing_failed")
    ]
    assert store.invalid == []


def test_adapter_environment_failure_is_not_quarantined():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    store = DeliveryStore(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "retry_count": 0,
                "body": {"id": 7, "type": "realm_user", "person": {}},
            }
        ]
    )
    instance = _delivery_service(store)
    instance.provider_adapters = lambda _account_uuid: (_ for _ in ()).throw(
        OSError("managed CA directory unavailable")
    )

    with pytest.raises(OSError, match="managed CA directory unavailable"):
        instance.process_provider_journal()
    assert store.retried == []
    assert store.invalid == []


def test_provider_journal_contains_unexpected_delivery_enqueue_failure(monkeypatch):
    account_uuid = "00000000-0000-4000-8000-000000000001"

    class Store(DeliveryStore):
        def enqueue_workspace_delivery(self, record, priority):
            raise RuntimeError("unexpected outbox failure")

    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "retry_count": 0,
                "body": {"id": 7, "type": "realm_user", "person": {}},
            }
        ]
    )
    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *_args, **_kwargs: [{"record_uuid": "record"}],
    )

    assert _delivery_service(store).process_provider_journal() == 0
    assert store.retried == [
        (account_uuid, "queue", 7, "provider_event_processing_failed")
    ]
    assert store.invalid == []


def test_unselected_provider_event_is_finalized_without_crashing(monkeypatch):
    account_uuid = "00000000-0000-0000-0000-000000000001"
    store = DeliveryStore(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": {"id": 7, "type": "realm_user", "person": {}},
            }
        ]
    )
    instance = _delivery_service(store)

    def reject_unselected(*_args, **_kwargs):
        raise ValueError("provider_chat_not_selected")

    monkeypatch.setattr(
        instance,
        "_event_records_with_file_fallback",
        reject_unselected,
    )

    assert instance.process_provider_journal() == 1
    assert store.enqueued == []
    assert store.processed == [(account_uuid, "queue", 7, True)]


def test_reaction_event_is_delivered_only_for_account_with_selected_chat(monkeypatch):
    selected_account_uuid = "00000000-0000-0000-0000-000000000001"
    unselected_account_uuid = "00000000-0000-0000-0000-000000000002"
    reaction = {
        "id": 7,
        "type": "reaction",
        "op": "add",
        "message_id": 601,
        "user_id": 3,
        "emoji_name": "thumbs_up",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
    }

    class Store(DeliveryStore):
        def provider_mapping(self, account_uuid, entity_kind, provider_id):
            assert entity_kind == "message"
            assert provider_id == "601"
            if account_uuid == selected_account_uuid:
                return {"workspace_uuid": "00000000-0000-0000-0000-000000000601"}
            return None

        def assignment_for_provider_chat(self, account_uuid, provider_chat_key):
            assert account_uuid == unselected_account_uuid
            assert provider_chat_key == "channel:42"
            return None

        def account_settings(self, account_uuid):
            assert account_uuid == unselected_account_uuid
            return {"selection_mode": "manual"}

    store = Store(
        [
            {
                "account_uuid": selected_account_uuid,
                "queue_id": "selected-queue",
                "event_id": 7,
                "body": reaction,
            },
            {
                "account_uuid": unselected_account_uuid,
                "queue_id": "unselected-queue",
                "event_id": 7,
                "body": reaction,
            },
        ]
    )
    instance = _delivery_service(store)

    class Adapter(ProviderAdapter):
        def __init__(self, account_uuid):
            self.account_uuid = account_uuid

        def message_by_id(self, provider_message_id):
            assert self.account_uuid == unselected_account_uuid
            assert provider_message_id == 601
            return {
                "id": 601,
                "type": "stream",
                "stream_id": 42,
                "display_recipient": "Engineering",
            }

    instance.provider_adapters = Adapter

    def records_for_selected_account(
        _adapter,
        account_uuid,
        _external_chat_uuid,
        _queue_id,
        _event,
        _delivery_class,
    ):
        assert account_uuid == selected_account_uuid
        return [{"record_uuid": "selected-reaction"}]

    monkeypatch.setattr(
        instance,
        "_event_records_with_file_fallback",
        records_for_selected_account,
    )

    assert instance.process_provider_journal() == 2
    assert store.enqueued == [({"record_uuid": "selected-reaction"}, 0)]
    assert store.retried == []
    assert store.processed == [
        (selected_account_uuid, "selected-queue", 7, True),
        (unselected_account_uuid, "unselected-queue", 7, True),
    ]


def test_reaction_event_for_selected_chat_waits_for_message_mapping():
    account_uuid = "00000000-0000-0000-0000-000000000001"
    reaction = {
        "id": 7,
        "type": "reaction",
        "op": "add",
        "message_id": 601,
        "user_id": 3,
        "emoji_name": "thumbs_up",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
    }

    class Store(DeliveryStore):
        def __init__(self, events):
            super().__init__(events)
            self.outside_history_checks = []

        def account_resource(self, requested):
            assert requested == account_uuid
            return {
                "owner_user_uuid": "00000000-0000-0000-0000-000000000010",
                "generation": 1,
                "settings": {
                    "default_project_id": "00000000-0000-0000-0000-000000000020"
                },
            }

        def provider_mapping(self, requested, entity_kind, provider_id):
            assert requested == account_uuid
            assert entity_kind == "message"
            assert provider_id == "601"
            return None

        def assignment_for_provider_chat(self, requested, provider_chat_key):
            assert requested == account_uuid
            assert provider_chat_key == "channel:42"
            return {
                "project_id": "00000000-0000-0000-0000-000000000020",
                "selected": True,
            }

        def ignore_provider_reaction_outside_history_window(
            self,
            requested,
            provider_chat_key,
            provider_message_id,
            provider_message_time,
            queue_id,
            event_id,
        ):
            self.outside_history_checks.append(
                (
                    requested,
                    provider_chat_key,
                    provider_message_id,
                    provider_message_time,
                    queue_id,
                    event_id,
                )
            )
            return False

    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": reaction,
            }
        ]
    )
    instance = _delivery_service(store)
    catalog_events = []
    instance._queue_event_catalog = lambda requested, event, server_url, marker=None: (
        catalog_events.append((requested, event, server_url))
    )

    class Adapter(ProviderAdapter):
        def message_by_id(self, provider_message_id):
            assert provider_message_id == 601
            return {
                "id": 601,
                "type": "stream",
                "stream_id": 42,
                "display_recipient": "Engineering",
                "timestamp": 1_800_000_000,
            }

    instance.provider_adapters = lambda _account_uuid: Adapter()

    assert instance.process_provider_journal() == 0
    assert store.enqueued == []
    assert store.processed == []
    assert store.outside_history_checks == [
        (
            account_uuid,
            "channel:42",
            "601",
            datetime.datetime.fromtimestamp(1_800_000_000, datetime.UTC),
            "queue",
            7,
        )
    ]
    assert store.retried == [
        (
            account_uuid,
            "queue",
            7,
            "provider_chat_assignment_pending",
        )
    ]
    assert catalog_events == [
        (account_uuid, reaction, "https://zulip.example.invalid"),
    ]


@pytest.mark.parametrize(
    ("assignment_pending_since", "processing_reason", "expected_processed"),
    [
        (None, None, 0),
        (
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=1),
            "provider_chat_assignment_pending",
            0,
        ),
        (
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=6),
            "rate_limit_hit",
            1,
        ),
    ],
)
def test_reaction_event_for_uncatalogued_chat_has_bounded_assignment_wait(
    assignment_pending_since,
    processing_reason,
    expected_processed,
):
    account_uuid = "00000000-0000-0000-0000-000000000001"
    reaction = {
        "id": 7,
        "type": "reaction",
        "op": "add",
        "message_id": 601,
        "user_id": 3,
        "emoji_name": "thumbs_up",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
    }

    class Store(DeliveryStore):
        def provider_mapping(self, requested, entity_kind, provider_id):
            assert requested == account_uuid
            assert entity_kind == "message"
            assert provider_id == "601"
            return None

        def assignment_for_provider_chat(self, requested, provider_chat_key):
            assert requested == account_uuid
            assert provider_chat_key == "group_direct:3,4,5"
            return None

        def account_settings(self, requested):
            assert requested == account_uuid
            return {"selection_mode": "all"}

    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "assignment_pending_since": assignment_pending_since,
                "processing_reason": processing_reason,
                "body": reaction,
            }
        ]
    )
    instance = _delivery_service(store)
    catalog_events = []
    instance._queue_event_catalog = lambda requested, event, server_url, marker=None: (
        catalog_events.append((requested, event, server_url))
    )

    class Adapter(ProviderAdapter):
        def message_by_id(self, provider_message_id):
            assert provider_message_id == 601
            return {
                "id": 601,
                "type": "private",
                "display_recipient": [
                    {"id": 3, "full_name": "One"},
                    {"id": 4, "full_name": "Two"},
                    {"id": 5, "full_name": "Three"},
                ],
                "timestamp": 1_800_000_000,
            }

    instance.provider_adapters = lambda _account_uuid: Adapter()

    assert instance.process_provider_journal() == expected_processed
    assert store.enqueued == []
    assert store.processed == []
    if expected_processed:
        assert store.retried == []
        assert store.invalid == [
            (
                account_uuid,
                "queue",
                7,
                "provider_reaction_chat_assignment_timeout",
            )
        ]
    else:
        assert store.retried == [
            (
                account_uuid,
                "queue",
                7,
                "provider_chat_assignment_pending",
            )
        ]
        assert store.invalid == []
    assert catalog_events == [
        (account_uuid, reaction, "https://zulip.example.invalid"),
        (
            account_uuid,
            {
                "type": "message",
                "message": {
                    "id": 601,
                    "type": "private",
                    "display_recipient": [
                        {"id": 3, "full_name": "One"},
                        {"id": 4, "full_name": "Two"},
                        {"id": 5, "full_name": "Three"},
                    ],
                    "timestamp": 1_800_000_000,
                },
            },
            "https://zulip.example.invalid",
        ),
    ]


def test_reaction_assignment_poll_reuses_persisted_message_context():
    account_uuid = "00000000-0000-0000-0000-000000000001"
    reaction = {
        "id": 7,
        "type": "reaction",
        "op": "add",
        "message_id": 601,
        "user_id": 3,
        "emoji_name": "thumbs_up",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
    }

    class Store(DeliveryStore):
        def __init__(self, events):
            super().__init__(events)
            self.message_context = None
            self.catalog_marks = []

        def provider_mapping(self, *_args):
            return None

        def assignment_for_provider_chat(self, _account, provider_chat_key):
            assert provider_chat_key == "direct:3,4"
            return None

        def account_settings(self, _account):
            return {"selection_mode": "all"}

        def cache_provider_event_message_context(
            self, _account, _queue, _event, message_context
        ):
            assert "content" not in message_context
            self.message_context = message_context
            return message_context

        def mark_provider_event_catalog_reported(self, account, queue, event_id):
            self.catalog_marks.append((account, queue, event_id))
            return True

    event_row = {
        "account_uuid": account_uuid,
        "queue_id": "queue",
        "event_id": 7,
        "assignment_pending_since": None,
        "processing_reason": None,
        "body": reaction,
    }
    store = Store([event_row])
    instance = _delivery_service(store)
    fetches = []
    catalog_events = []

    def queue_catalog(account, event, server, marker=None):
        catalog_events.append((account, event, server))
        if event.get("type") == "message" and marker is not None:
            store.mark_provider_event_catalog_reported(*marker)
        return event.get("type") == "message"

    instance._queue_event_catalog = queue_catalog

    class Adapter(ProviderAdapter):
        def message_by_id(self, provider_message_id):
            fetches.append(provider_message_id)
            return {
                "id": provider_message_id,
                "type": "private",
                "display_recipient": [
                    {"id": 3, "full_name": "Owner"},
                    {"id": 4, "full_name": "Peer"},
                ],
                "timestamp": 1_800_000_000,
                "content": "not retained in the journal context",
            }

    instance.provider_adapters = lambda _account_uuid: Adapter()

    assert instance.process_provider_journal() == 0
    assert fetches == [601]
    assert store.message_context is not None
    assert len(catalog_events) == 2
    assert catalog_events[-1][1]["type"] == "message"
    assert store.catalog_marks == [(account_uuid, "queue", 7)]

    store.events = [
        {
            **event_row,
            "assignment_pending_since": datetime.datetime.now(datetime.UTC),
            "assignment_catalog_reported_at": datetime.datetime.now(datetime.UTC),
            "processing_reason": "provider_chat_assignment_pending",
            "provider_message_context": store.message_context,
        }
    ]

    assert instance.process_provider_journal() == 0
    assert fetches == [601]
    assert len(catalog_events) == 2
    assert catalog_events[-1][1]["type"] == "message"
    assert store.catalog_marks == [(account_uuid, "queue", 7)]
    assert [retry[-1] for retry in store.retried] == [
        "provider_chat_assignment_pending",
        "provider_chat_assignment_pending",
    ]


def test_reaction_catalog_publication_retries_row_before_durable_marker():
    account_uuid = "00000000-0000-0000-0000-000000000001"
    reaction = {
        "id": 7,
        "type": "reaction",
        "op": "add",
        "message_id": 601,
        "user_id": 3,
        "emoji_name": "thumbs_up",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
    }
    event_row = {
        "account_uuid": account_uuid,
        "queue_id": "queue",
        "event_id": 7,
        "assignment_pending_since": datetime.datetime.now(datetime.UTC),
        "assignment_catalog_reported_at": None,
        "provider_message_context": {
            "id": 601,
            "type": "private",
            "display_recipient": [
                {"id": 3, "full_name": "Owner"},
                {"id": 4, "full_name": "Peer"},
            ],
            "timestamp": 1_800_000_000,
        },
        "body": reaction,
    }

    class Store(DeliveryStore):
        def __init__(self, events):
            super().__init__(events)
            self.catalog_marks = []

        def provider_mapping(self, *_args):
            return None

        def assignment_for_provider_chat(self, _account, _chat):
            return None

        def account_settings(self, _account):
            return {"selection_mode": "all"}

        def mark_provider_event_catalog_reported(self, account, queue, event_id):
            self.catalog_marks.append((account, queue, event_id))
            return True

    store = Store([event_row])
    instance = _delivery_service(store)
    instance.provider_adapters = lambda _account_uuid: ProviderAdapter()
    catalog_attempts = []
    stop_before_durable = True

    def queue_catalog(account, event, server, marker=None):
        nonlocal stop_before_durable
        if event.get("type") != "message":
            return
        catalog_attempts.append((account, event, server))
        if stop_before_durable:
            raise RuntimeError("process stopped before durable catalog enqueue")
        assert marker is not None
        store.mark_provider_event_catalog_reported(*marker)
        return True

    instance._queue_event_catalog = queue_catalog

    assert instance.process_provider_journal() == 0
    assert len(catalog_attempts) == 1
    assert store.catalog_marks == []
    assert [retry[-1] for retry in store.retried] == [
        "provider_event_processing_failed"
    ]

    stop_before_durable = False
    store.events = [event_row]
    assert instance.process_provider_journal() == 0
    assert len(catalog_attempts) == 2
    assert store.catalog_marks == [(account_uuid, "queue", 7)]
    assert [retry[-1] for retry in store.retried] == [
        "provider_event_processing_failed",
        "provider_chat_assignment_pending",
    ]


def test_expired_reaction_mapping_wait_is_quarantined_after_assignment_exists():
    account_uuid = "00000000-0000-0000-0000-000000000001"
    reaction = {
        "id": 7,
        "type": "reaction",
        "op": "add",
        "message_id": 601,
        "user_id": 3,
        "emoji_name": "thumbs_up",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
    }

    class Store(DeliveryStore):
        def provider_mapping(self, *_args):
            return None

        def assignment_for_provider_chat(self, _account, provider_chat_key):
            assert provider_chat_key == "direct:3,4"
            return {
                "uuid": "00000000-0000-0000-0000-000000000090",
                "generation": 1,
                "project_id": "00000000-0000-0000-0000-000000000091",
                "selected": True,
            }

    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "assignment_pending_since": (
                    datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=6)
                ),
                "assignment_catalog_reported_at": datetime.datetime.now(datetime.UTC),
                "provider_message_context": {
                    "id": 601,
                    "type": "private",
                    "display_recipient": [
                        {"id": 3, "full_name": "Owner"},
                        {"id": 4, "full_name": "Peer"},
                    ],
                    "timestamp": 1_800_000_000,
                },
                "body": reaction,
            }
        ]
    )
    instance = _delivery_service(store)
    instance.provider_adapters = lambda _account_uuid: ProviderAdapter()

    def wait_for_mapping(*_args, **_kwargs):
        raise ValueError("provider_chat_assignment_pending")

    instance._event_records_with_file_fallback = wait_for_mapping

    assert instance.process_provider_journal() == 1
    assert store.retried == []
    assert store.invalid == [
        (
            account_uuid,
            "queue",
            7,
            "provider_reaction_chat_assignment_timeout",
        )
    ]


def test_reaction_event_outside_completed_history_window_is_terminal():
    account_uuid = "00000000-0000-0000-0000-000000000001"
    reaction = {
        "id": 7,
        "type": "reaction",
        "op": "add",
        "message_id": 601,
        "user_id": 3,
        "emoji_name": "thumbs_up",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
    }

    class Store(DeliveryStore):
        def __init__(self, events):
            super().__init__(events)
            self.ignored = []

        def account_resource(self, requested):
            assert requested == account_uuid
            return {
                "owner_user_uuid": "00000000-0000-0000-0000-000000000010",
                "generation": 1,
                "settings": {
                    "default_project_id": "00000000-0000-0000-0000-000000000020"
                },
            }

        def provider_mapping(self, requested, entity_kind, provider_id):
            assert requested == account_uuid
            assert entity_kind == "message"
            assert provider_id == "601"
            return None

        def assignment_for_provider_chat(self, requested, provider_chat_key):
            assert requested == account_uuid
            assert provider_chat_key == "channel:42"
            return {
                "project_id": "00000000-0000-0000-0000-000000000020",
                "selected": True,
            }

        def ignore_provider_reaction_outside_history_window(
            self,
            requested,
            provider_chat_key,
            provider_message_id,
            provider_message_time,
            queue_id,
            event_id,
        ):
            self.ignored.append(
                (
                    requested,
                    provider_chat_key,
                    provider_message_id,
                    provider_message_time,
                    queue_id,
                    event_id,
                )
            )
            return True

    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": reaction,
            }
        ]
    )
    instance = _delivery_service(store)

    class Adapter(ProviderAdapter):
        def message_by_id(self, provider_message_id):
            assert provider_message_id == 601
            return {
                "id": 601,
                "type": "stream",
                "stream_id": 42,
                "display_recipient": "Engineering",
                "timestamp": 1_700_000_000,
            }

    instance.provider_adapters = lambda _account_uuid: Adapter()
    instance._event_records_with_file_fallback = lambda *_args: pytest.fail(
        "ignored reactions must not reach conversion"
    )

    assert instance.process_provider_journal() == 1
    assert store.enqueued == []
    assert store.retried == []
    assert store.processed == []
    assert store.ignored == [
        (
            account_uuid,
            "channel:42",
            "601",
            datetime.datetime.fromtimestamp(1_700_000_000, datetime.UTC),
            "queue",
            7,
        )
    ]


def test_reaction_event_for_missing_provider_message_is_terminal():
    account_uuid = "00000000-0000-0000-0000-000000000001"

    class Store(DeliveryStore):
        def provider_mapping(self, requested, entity_kind, provider_id):
            assert requested == account_uuid
            assert entity_kind == "message"
            assert provider_id == "601"
            return None

    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": {
                    "id": 7,
                    "type": "reaction",
                    "op": "remove",
                    "message_id": 601,
                    "user_id": 3,
                    "emoji_name": "thumbs_up",
                    "emoji_code": "1f44d",
                    "reaction_type": "unicode_emoji",
                },
            }
        ]
    )
    instance = _delivery_service(store)

    class Adapter(ProviderAdapter):
        def message_by_id(self, provider_message_id):
            assert provider_message_id == 601
            return None

    instance.provider_adapters = lambda _account_uuid: Adapter()

    assert instance.process_provider_journal() == 1
    assert store.enqueued == []
    assert store.retried == []
    assert store.processed == [(account_uuid, "queue", 7, True)]


def test_provider_journal_waits_for_assignment_bound_delivery(monkeypatch):
    class Store(DeliveryStore):
        def __init__(self, events):
            super().__init__(events)
            self.delivering = []
            self.prepared = []

        def reset_stale_workspace_deliveries(self):
            return 0

        def mark_provider_event_delivering(self, account_uuid, queue_id, event_id):
            self.delivering.append((account_uuid, queue_id, event_id))

        def prepare_provider_event_records(
            self, account_uuid, queue_id, event_id, records
        ):
            self.prepared.append((account_uuid, queue_id, event_id, records))
            return [{**records[0], "prepared": True}]

        def enqueue_workspace_delivery(
            self, record, priority, provider_queue_id, provider_event_id
        ):
            self.enqueued.append(
                (record, priority, provider_queue_id, provider_event_id)
            )
            return True

    account_uuid = "00000000-0000-0000-0000-000000000001"
    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": {"id": 7, "type": "realm_user", "person": {}},
            }
        ]
    )
    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [{"record_uuid": "record"}],
    )
    assert _delivery_service(store).process_provider_journal() == 1
    assert store.prepared == [
        (
            account_uuid,
            "queue",
            7,
            [{"record_uuid": "record"}],
        )
    ]
    assert store.enqueued == [
        ({"record_uuid": "record", "prepared": True}, 0, "queue", 7)
    ]
    assert store.delivering == [(account_uuid, "queue", 7)]
    assert store.processed == []


def test_provider_journal_retries_changed_reaction_mapping_plan(monkeypatch):
    class Store(DeliveryStore):
        def prepare_provider_event_records(
            self, account_uuid, queue_id, event_id, records
        ):
            raise ValueError("reaction_mapping_plan_changed")

    account_uuid = "00000000-0000-0000-0000-000000000001"
    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": {"id": 7, "type": "realm_user", "person": {}},
            }
        ]
    )
    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [{"record_uuid": "record"}],
    )

    assert _delivery_service(store).process_provider_journal() == 0
    assert store.retried == [
        (account_uuid, "queue", 7, "reaction_mapping_plan_changed")
    ]
    assert store.enqueued == []
    assert store.invalid == []


def test_provider_journal_finishes_assignment_change_during_enqueue(monkeypatch):
    class Store(DeliveryStore):
        def __init__(self, events):
            super().__init__(events)
            self.assignment_changes = []
            self.delivering = []

        def reset_stale_workspace_deliveries(self):
            return 0

        def prepare_provider_event_records(
            self, account_uuid, queue_id, event_id, records
        ):
            return records

        def enqueue_workspace_delivery(
            self, record, priority, provider_queue_id, provider_event_id
        ):
            if record["record_uuid"] == "stale-target":
                raise ValueError("provider_chat_assignment_pending")
            self.enqueued.append(
                (record, priority, provider_queue_id, provider_event_id)
            )
            return True

        def finalize_provider_event_assignment_changed(
            self, account_uuid, queue_id, event_id
        ):
            self.assignment_changes.append((account_uuid, queue_id, event_id))
            return True

        def mark_provider_event_delivering(self, account_uuid, queue_id, event_id):
            self.delivering.append((account_uuid, queue_id, event_id))

    account_uuid = "00000000-0000-0000-0000-000000000001"
    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": {"id": 7, "type": "realm_user", "person": {}},
            },
            {
                "account_uuid": account_uuid,
                "queue_id": "later",
                "event_id": 8,
                "body": {"id": 8, "type": "realm_user", "person": {}},
            },
        ]
    )
    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [
            {"record_uuid": "accepted-prefix"},
            {"record_uuid": "stale-target"},
        ],
    )

    assert _delivery_service(store).process_provider_journal() == 0
    assert store.enqueued == [
        (
            {"record_uuid": "accepted-prefix"},
            0,
            "queue",
            7,
        )
    ]
    assert store.assignment_changes == [(account_uuid, "queue", 7)]
    assert store.delivering == []
    assert store.retried == []


def test_live_event_preempts_large_backfill_and_is_exactly_once_across_restart(
    monkeypatch,
):
    account_uuid = "00000000-0000-4000-8000-000000000001"

    class State:
        cursor = {"queue_id": "queue", "last_event_id": 7}
        processing_state = "pending"
        retries = []
        deliveries = [
            (
                {
                    "record_uuid": f"backfill-record-{index}",
                    "operation_uuid": f"backfill-operation-{index}",
                },
                2,
            )
            for index in range(100)
        ]

    class Store:
        def __init__(self, crash_after_enqueue=False):
            self.crash_after_enqueue = crash_after_enqueue

        def pending_provider_events(self, limit):
            assert limit == service.BridgeService.PROVIDER_JOURNAL_QUANTUM
            if State.processing_state != "pending":
                return []
            return [
                {
                    "account_uuid": account_uuid,
                    "queue_id": "queue",
                    "event_id": 7,
                    "retry_count": len(State.retries),
                    "body": {
                        "id": 7,
                        "type": "message",
                        "message": {"id": 70, "type": "stream", "stream_id": 42},
                    },
                }
            ]

        def account_is_active(self, requested):
            return True

        def account_resource(self, requested):
            return None

        def enqueue_workspace_delivery(self, record, priority):
            if any(
                existing[0]["operation_uuid"] == record["operation_uuid"]
                for existing in State.deliveries
            ):
                return False
            State.deliveries.append((record, priority))
            return True

        def finalize_provider_event(
            self,
            requested,
            queue_id,
            event_id,
            supported,
            deleted_message_ids,
        ):
            if self.crash_after_enqueue:
                self.crash_after_enqueue = False
                raise RuntimeError("simulated process crash")
            State.processing_state = "processed"

        def mark_provider_event_invalid(self, *args):
            raise AssertionError("valid buffered event must not be quarantined")

        def retry_provider_event(self, *args):
            State.retries.append(args)

        def mark_health(self, *args):
            return None

    def bridge_instance(crash_after_enqueue=False):
        instance = object.__new__(service.BridgeService)
        instance.store = Store(crash_after_enqueue)
        instance.file_client = None
        instance.provider_adapters = lambda requested: ProviderAdapter()
        return instance

    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [
            {
                "record_uuid": "live-record",
                "operation_uuid": "live-operation",
            }
        ],
    )

    first_process = bridge_instance(crash_after_enqueue=True)
    assert first_process.process_provider_journal() == 0
    assert State.processing_state == "pending"
    assert State.cursor == {"queue_id": "queue", "last_event_id": 7}
    assert [retry[-1] for retry in State.retries] == [
        "provider_event_processing_failed"
    ]
    live_deliveries = [
        delivery
        for delivery in State.deliveries
        if delivery[0]["operation_uuid"] == "live-operation"
    ]
    assert live_deliveries == [
        (
            {"record_uuid": "live-record", "operation_uuid": "live-operation"},
            0,
        )
    ]
    assert sorted(State.deliveries, key=lambda delivery: delivery[1])[0] == (
        {"record_uuid": "live-record", "operation_uuid": "live-operation"},
        0,
    )

    restarted_process = bridge_instance()
    assert restarted_process.process_provider_journal() == 1
    assert State.processing_state == "processed"
    assert restarted_process.process_provider_journal() == 0
    assert (
        len(
            [
                delivery
                for delivery in State.deliveries
                if delivery[0]["operation_uuid"] == "live-operation"
            ]
        )
        == 1
    )


def test_malformed_provider_event_is_quarantined_and_next_event_continues(
    monkeypatch,
):
    account_uuid = "00000000-0000-0000-0000-000000000001"
    store = DeliveryStore(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": {"id": 7, "type": "message"},
            },
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 8,
                "body": {"id": 8, "type": "realm_user", "person": {}},
            },
        ]
    )

    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [{"record_uuid": "record"}],
    )

    assert _delivery_service(store).process_provider_journal() == 2
    assert store.invalid == [(account_uuid, "queue", 7, "KeyError")]
    assert store.enqueued == [({"record_uuid": "record"}, 0)]
    assert store.processed == [(account_uuid, "queue", 8, True)]


def test_registration_snapshot_queues_account_live_ready_and_chat_catalog_reports():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    owner_uuid = "00000000-0000-4000-8000-000000000002"
    project_uuid = "00000000-0000-4000-8000-000000000003"

    class Store:
        def __init__(self):
            self.reports = []
            self.mappings = []

        def account_resource(self, requested):
            assert requested == account_uuid
            return {
                "uuid": account_uuid,
                "generation": 7,
                "owner_user_uuid": owner_uuid,
                "settings": {
                    "selection_mode": "all",
                    "default_project_id": project_uuid,
                },
            }

        def enqueue_observed_report(self, report):
            self.reports.append(report)
            return True

        def remember_provider_mapping(self, *args):
            self.mappings.append(args)

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance._queue_registration_reports(
        account_uuid,
        {
            "user_id": 1,
            "realm_uuid": "00000000-0000-4000-8000-000000000004",
            "realm_users": [
                {
                    "user_id": 1,
                    "full_name": "Owner",
                    "email": "owner@example.invalid",
                },
                {
                    "user_id": 2,
                    "full_name": "Other User",
                    "email": "other@example.invalid",
                },
            ],
            "subscriptions": [
                {
                    "stream_id": 42,
                    "name": "Engineering",
                    "subscribers": [1, 2],
                }
            ],
            "recent_private_conversations": [{"user_ids": [2], "max_message_id": 99}],
        },
        "https://zulip.example.invalid",
    )
    instance._queue_account_report(account_uuid, "live_ready")

    reports = instance.store.reports
    account = [r for r in reports if r["resource_type"] == "external_account"]
    catalog = [r for r in reports if r["resource_type"] == "external_chat_catalog"]
    assert len(account) == 1
    assert account[0]["status"] == "live_ready"
    assert account[0]["observed_generation"] == 7
    assert len(catalog) == 2
    assert {r["catalog"]["source"]["provider_chat_key"] for r in catalog} == {
        "channel:42",
        "direct:1,2",
    }
    assert {r["catalog"]["display_name"] for r in catalog} == {
        "Engineering",
        "Other User",
    }
    assert instance.store.mappings[0][1:4] == ("identity", "1", owner_uuid)
    channel = next(
        report
        for report in catalog
        if report["catalog"]["source"]["provider_chat_key"] == "channel:42"
    )
    direct = next(
        report
        for report in catalog
        if report["catalog"]["source"]["provider_chat_key"] == "direct:1,2"
    )
    assert channel["catalog"]["source"]["original_url"].endswith("/#narrow/channel/42")
    assert direct["catalog"]["source"]["original_url"].endswith("/#narrow/dm/1,2-dm")
    assert channel["catalog"]["participants"] == [
        {
            "provider_user_id": "1",
            "display_name": "Owner",
            "email": "owner@example.invalid",
            "avatar_urn": None,
            "is_owner": True,
        }
    ]
    assert channel["catalog"]["capabilities"]["messenger.stream.rename"]["available"]
    assert channel["catalog"]["capabilities"]["messenger.membership.write"]["available"]
    assert "messenger.stream.rename" not in direct["catalog"]["capabilities"]
    assert "messenger.membership.write" not in direct["catalog"]["capabilities"]
    assert set(channel["catalog"]["source"]) == {
        "kind",
        "chat_type",
        "provider_chat_key",
        "provider_realm_uuid",
        "provider_owner_user_id",
        "original_url",
    }
    assert (
        channel["catalog"]["source"]["provider_realm_uuid"]
        == "00000000-0000-4000-8000-000000000004"
    )
    assert channel["catalog"]["source"]["provider_owner_user_id"] == "1"
    assert direct["catalog"]["participants"] == [
        {
            "provider_user_id": "1",
            "display_name": "Owner",
            "email": "owner@example.invalid",
            "avatar_urn": None,
            "is_owner": True,
        },
        {
            "provider_user_id": "2",
            "display_name": "Other User",
            "email": "other@example.invalid",
            "avatar_urn": None,
            "is_owner": False,
        },
    ]
    assert direct["catalog"]["topics"] == [
        {
            "provider_topic_id": "direct:1,2:default",
            "name": "Zulip",
            "is_default": True,
        }
    ]
    for report in catalog:
        expected = converter.stable_entity_uuid(
            account_uuid,
            "external_chat",
            report["catalog"]["source"]["provider_chat_key"],
        )
        assert report["resource_uuid"] == expected


@pytest.mark.parametrize(
    ("projected_user_ids", "expected_ready"),
    [(["1"], False), (["1", "2"], True)],
)
def test_selected_channel_participants_gate_messages_until_projection_matches(
    projected_user_ids, expected_ready
):
    account_uuid = "00000000-0000-4000-8000-000000000001"
    owner_uuid = "00000000-0000-4000-8000-000000000002"
    project_uuid = "00000000-0000-4000-8000-000000000003"
    assignment = {
        "uuid": "00000000-0000-4000-8000-000000000004",
        "generation": 3,
        "selected": True,
        "workspace_projection": {
            "participants": [
                {"provider_user_id": user_id} for user_id in projected_user_ids
            ]
        },
    }

    class Store:
        def __init__(self):
            self.reports = []
            self.completed = []

        def claim_participant_sync(self):
            return {
                "account_uuid": account_uuid,
                "provider_chat_key": "channel:42",
                "assignment_generation": 3,
            }

        def assignment_for_provider_chat(self, requested, chat_key):
            assert (requested, chat_key) == (account_uuid, "channel:42")
            return assignment

        def provider_event_cursor(self, requested):
            return {
                "queue_id": "queue",
                "last_event_id": 7,
                "provider_realm_uuid": ("00000000-0000-4000-8000-000000000005"),
                "provider_owner_user_id": "1",
            }

        def account_resource(self, requested):
            return {
                "generation": 2,
                "owner_user_uuid": owner_uuid,
                "settings": {
                    "selection_mode": "manual",
                    "default_project_id": project_uuid,
                },
            }

        def enqueue_observed_report(self, report):
            self.reports.append(report)
            return True

        def remember_provider_mapping(self, *args):
            return None

        def complete_participant_sync(self, *args):
            self.completed.append(args)

        def release_participant_sync(self, *args):
            raise AssertionError("valid participant synchronization was released")

        def mark_health(self, *args):
            return None

    class Adapter:
        server_url = "https://zulip.example.invalid"

        def channel_catalog(self, chat_key):
            assert chat_key == "channel:42"
            return {
                "user_id": 1,
                "realm_users": [
                    {"user_id": 1, "full_name": "Owner"},
                    {"user_id": 2, "full_name": "Other User"},
                ],
                "subscriptions": [
                    {
                        "stream_id": 42,
                        "name": "Engineering",
                        "subscribers": [1, 2],
                    }
                ],
            }

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()

    assert instance.refresh_selected_participants_once()
    assert instance.store.completed == [
        (account_uuid, "channel:42", 3, [1, 2], expected_ready)
    ]
    catalog = instance.store.reports[0]["catalog"]
    assert {
        participant["provider_user_id"] for participant in catalog["participants"]
    } == {"1", "2"}


def test_selected_channel_participants_are_refreshed_in_one_account_batch():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    owner_uuid = "00000000-0000-4000-8000-000000000002"
    project_uuid = "00000000-0000-4000-8000-000000000003"
    assignments = {
        "channel:42": {
            "uuid": "00000000-0000-4000-8000-000000000042",
            "generation": 3,
            "selected": True,
            "workspace_projection": {
                "participants": [
                    {"provider_user_id": "1"},
                    {"provider_user_id": "2"},
                ]
            },
        },
        "channel:43": {
            "uuid": "00000000-0000-4000-8000-000000000043",
            "generation": 4,
            "selected": True,
            "workspace_projection": {"participants": [{"provider_user_id": "1"}]},
        },
    }

    class Store:
        def __init__(self):
            self.completed = []
            self.reports = []

        def claim_participant_sync_batch(self, limit):
            assert limit == service.BridgeService.PARTICIPANT_SYNC_BATCH_SIZE
            return [
                {
                    "account_uuid": account_uuid,
                    "provider_chat_key": chat_key,
                    "assignment_generation": assignment["generation"],
                    "assignment": assignment,
                }
                for chat_key, assignment in assignments.items()
            ]

        def assignment_for_provider_chat(self, *args):
            raise AssertionError("claimed batches already carry their assignments")

        def provider_event_cursor(self, requested):
            assert requested == account_uuid
            return {"provider_realm_uuid": "00000000-0000-4000-8000-000000000005"}

        def account_resource(self, requested):
            assert requested == account_uuid
            return {
                "generation": 2,
                "owner_user_uuid": owner_uuid,
                "settings": {
                    "selection_mode": "manual",
                    "default_project_id": project_uuid,
                    "email": "owner@example.invalid",
                },
            }

        def remember_provider_mapping(self, *args):
            return None

        def enqueue_observed_report(self, report):
            self.reports.append(report)
            return True

        def complete_participant_sync_batch(self, updates):
            self.completed.extend(updates)

        def release_participant_sync_batch(self, jobs):
            raise AssertionError(f"valid participant jobs were released: {jobs}")

    class Adapter:
        server_url = "https://zulip.example.invalid"

        def __init__(self):
            self.requests = []

        def channel_catalogs(self, chat_keys):
            self.requests.append(list(chat_keys))
            return {
                "user_id": 1,
                "realm_users": [
                    {"user_id": 1, "full_name": "Owner"},
                    {"user_id": 2, "full_name": "Other User"},
                ],
                "subscriptions": [
                    {
                        "stream_id": 42,
                        "name": "Engineering",
                        "subscribers": [1, 2],
                    },
                    {
                        "stream_id": 43,
                        "name": "Operations",
                        "subscribers": [1],
                    },
                ],
            }

    adapter = Adapter()
    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: adapter

    assert instance.refresh_selected_participants_once()
    assert adapter.requests == [["channel:42", "channel:43"]]
    assert [update["provider_chat_key"] for update in instance.store.completed] == [
        "channel:42",
        "channel:43",
    ]
    assert all(update["ready"] for update in instance.store.completed)


def test_subscription_peer_event_invalidates_only_affected_channels():
    account_uuid = "00000000-0000-4000-8000-000000000001"

    class Store:
        def __init__(self):
            self.invalidations = []

        def account_resource(self, requested):
            assert requested == account_uuid
            return {
                "generation": 1,
                "owner_user_uuid": "00000000-0000-4000-8000-000000000002",
                "settings": {
                    "default_project_id": ("00000000-0000-4000-8000-000000000003")
                },
            }

        def invalidate_participant_sync(self, requested, chat_keys):
            self.invalidations.append((requested, chat_keys))

    instance = object.__new__(service.BridgeService)
    instance.store = Store()

    instance._queue_event_catalog(
        account_uuid,
        {
            "type": "subscription",
            "op": "peer_add",
            "stream_ids": [42, "invalid", 43],
            "user_ids": [7],
        },
        "https://zulip.example.invalid",
    )

    assert instance.store.invalidations == [
        (account_uuid, ["channel:42", "channel:43"])
    ]


def test_user_topic_event_catalog_is_durable_and_marks_dependency():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    marker = (account_uuid, "queue-1", 8)

    class Store:
        def __init__(self):
            self.report = None
            self.marker = None

        def account_resource(self, requested):
            assert requested == account_uuid
            return {
                "generation": 1,
                "owner_user_uuid": "00000000-0000-4000-8000-000000000002",
                "settings": {
                    "default_project_id": ("00000000-0000-4000-8000-000000000003")
                },
            }

        def provider_mapping(self, requested, entity_kind, provider_id):
            assert (requested, entity_kind, provider_id) == (
                account_uuid,
                "stream",
                "channel:42",
            )
            return {
                "workspace_uuid": "00000000-0000-4000-8000-000000000004",
                "metadata": {"name": "Engineering"},
            }

        def provider_event_cursor(self, requested):
            assert requested == account_uuid
            return {
                "provider_realm_uuid": "00000000-0000-4000-8000-000000000005",
                "provider_owner_user_id": "1",
            }

        def merge_catalog_topology(
            self,
            _account_uuid,
            _chat_key,
            participants,
            topics,
            *,
            authoritative_participants=False,
        ):
            assert not authoritative_participants
            return participants, topics

        def ensure_provider_event_catalog_report(self, report, *supplied_marker):
            self.report = report
            self.marker = supplied_marker
            return True

    instance = object.__new__(service.BridgeService)
    instance.store = Store()

    assert instance._queue_event_catalog(
        account_uuid,
        {
            "id": 8,
            "type": "user_topic",
            "stream_id": 42,
            "topic_name": "Bridge",
            "visibility_policy": 3,
            "last_updated": 1_800_000_020,
        },
        "https://zulip.example.invalid",
        marker,
    )

    assert instance.store.marker == marker
    assert instance.store.report["catalog"]["topics"] == [
        {
            "provider_topic_id": "42:Bridge",
            "name": "Bridge",
            "is_default": False,
        }
    ]


def test_live_message_does_not_wait_for_full_participant_projection(monkeypatch):
    account_uuid = "00000000-0000-4000-8000-000000000001"

    class Store(DeliveryStore):
        def assignment_participants_ready(self, *args):
            raise AssertionError("live messages must not enter the history gate")

    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": {
                    "id": 7,
                    "type": "message",
                    "message": {
                        "id": 70,
                        "type": "stream",
                        "stream_id": 42,
                    },
                },
            }
        ]
    )

    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [{"record_uuid": "live-message"}],
    )

    assert _delivery_service(store).process_provider_journal() == 1
    assert store.retried == []
    assert store.enqueued == [({"record_uuid": "live-message"}, 0)]


def test_backfill_waits_for_selected_channel_participant_projection():
    class Store(DeliveryStore):
        def assignment_participants_ready(self, *args):
            return False

    instance = _delivery_service(Store())

    with pytest.raises(ValueError, match="provider_chat_participants_pending"):
        instance.enqueue_backfill(
            "00000000-0000-4000-8000-000000000001",
            "channel:42",
            [{"id": 7, "timestamp": 7}],
        )


def test_catalog_reports_accumulate_full_replacement_topology():
    class Store:
        def __init__(self):
            self.participants = {}
            self.topics = {}
            self.reports = []

        def merge_catalog_topology(
            self,
            _account,
            _chat,
            participants,
            topics,
            *,
            authoritative_participants=False,
        ):
            if authoritative_participants:
                self.participants = {}
            self.participants.update(
                (value["provider_user_id"], value) for value in participants
            )
            self.topics.update((value["provider_topic_id"], value) for value in topics)
            return list(self.participants.values()), list(self.topics.values())

        def enqueue_observed_report(self, report):
            self.reports.append(report)
            return True

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    common = (
        "10000000-0000-4000-8000-000000000001",
        "10000000-0000-4000-8000-000000000002",
        "10000000-0000-4000-8000-000000000003",
        1,
        "channel:42",
        "channel",
        "Engineering",
        "https://zulip.example.invalid",
    )
    owner = {
        "provider_user_id": "1",
        "display_name": "Owner",
        "is_owner": True,
    }
    for user_id, topic in (("2", "T1"), ("3", "T2"), ("4", "T1")):
        instance._queue_catalog_report(
            *common,
            participants=[
                owner,
                {
                    "provider_user_id": user_id,
                    "display_name": f"User {user_id}",
                    "is_owner": False,
                },
            ],
            topics=[
                {
                    "provider_topic_id": f"42:{topic}",
                    "name": topic,
                    "is_default": False,
                }
            ],
            provider_realm_uuid="10000000-0000-4000-8000-000000000004",
            provider_owner_user_id="1",
        )
    final = instance.store.reports[-1]["catalog"]
    assert {value["provider_user_id"] for value in final["participants"]} == {
        "1",
        "2",
        "3",
        "4",
    }
    assert {value["provider_topic_id"] for value in final["topics"]} == {
        "42:T1",
        "42:T2",
    }


def test_catalog_report_repairs_authenticated_owner_in_persisted_topology():
    class Store:
        def __init__(self):
            self.participants = []
            self.reports = []

        def merge_catalog_topology(
            self,
            _account,
            _chat,
            participants,
            topics,
            *,
            authoritative_participants=False,
        ):
            self.participants = participants
            return participants, topics

        def enqueue_observed_report(self, report):
            self.reports.append(report)
            return True

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance._queue_catalog_report(
        "10000000-0000-4000-8000-000000000001",
        "10000000-0000-4000-8000-000000000002",
        "10000000-0000-4000-8000-000000000003",
        1,
        "direct:9,14",
        "direct",
        "Direct chat",
        "https://zulip.example.invalid",
        participants=[
            {
                "provider_user_id": "9",
                "display_name": "Owner",
                "is_owner": False,
            },
            {
                "provider_user_id": "14",
                "display_name": "Peer",
                "is_owner": False,
            },
        ],
        topics=[
            {
                "provider_topic_id": "direct:9,14:default",
                "name": "Zulip",
                "is_default": True,
            }
        ],
        provider_realm_uuid="10000000-0000-4000-8000-000000000004",
        provider_owner_user_id="9",
    )

    participants = instance.store.participants
    assert [value["provider_user_id"] for value in participants] == ["9", "14"]
    assert [value["is_owner"] for value in participants] == [True, False]
    assert instance.store.reports[0]["catalog"]["participants"] == participants
    assert instance.store.reports[0]["catalog"]["display_name"] == "Peer"


def test_direct_message_event_catalog_excludes_authenticated_owner_from_name():
    class Store:
        def __init__(self):
            self.reports = []

        def account_resource(self, _account_uuid):
            return {
                "owner_user_uuid": "10000000-0000-4000-8000-000000000001",
                "generation": 1,
                "settings": {
                    "default_project_id": "10000000-0000-4000-8000-000000000002"
                },
            }

        def provider_event_cursor(self, _account_uuid):
            return {
                "provider_realm_uuid": "10000000-0000-4000-8000-000000000004",
                "provider_owner_user_id": "9",
            }

        def enqueue_observed_report(self, report):
            self.reports.append(report)
            return True

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance._queue_event_catalog(
        "10000000-0000-4000-8000-000000000003",
        {
            "type": "message",
            "message": {
                "type": "private",
                "display_recipient": [
                    {"id": 9, "full_name": "Owner"},
                    {"id": 14, "full_name": "Peer One"},
                    {"id": 18, "full_name": "Peer Two"},
                ],
            },
        },
        "https://zulip.example.invalid",
    )

    catalog = instance.store.reports[0]["catalog"]
    assert catalog["display_name"] == "Peer One, Peer Two"
    assert [value["is_owner"] for value in catalog["participants"]] == [
        True,
        False,
        False,
    ]


@pytest.mark.parametrize(
    ("subject", "expected_topic"),
    [
        (
            "General",
            {
                "provider_topic_id": "42:General",
                "name": "General",
                "is_default": False,
            },
        ),
        (
            "",
            {
                "provider_topic_id": "42:general chat",
                "name": "general chat",
                "is_default": True,
            },
        ),
    ],
)
def test_channel_message_catalog_does_not_turn_authors_or_mentions_into_members(
    subject, expected_topic
):
    class Store:
        def __init__(self):
            self.reports = []
            self.merge_calls = []

        def account_resource(self, _account_uuid):
            return {
                "owner_user_uuid": "10000000-0000-4000-8000-000000000001",
                "generation": 1,
                "settings": {
                    "default_project_id": "10000000-0000-4000-8000-000000000002"
                },
            }

        def provider_event_cursor(self, _account_uuid):
            return {
                "provider_realm_uuid": ("10000000-0000-4000-8000-000000000004"),
                "provider_owner_user_id": "1",
            }

        def merge_catalog_topology(
            self,
            _account,
            _chat,
            participants,
            topics,
            *,
            authoritative_participants=False,
        ):
            self.merge_calls.append((participants, topics, authoritative_participants))
            return participants, topics

        def enqueue_observed_report(self, report):
            self.reports.append(report)
            return True

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance._queue_event_catalog(
        "10000000-0000-4000-8000-000000000003",
        {
            "type": "message",
            "message": {
                "type": "stream",
                "stream_id": 42,
                "display_recipient": "Engineering",
                "subject": subject,
                "sender_id": 2,
                "sender_full_name": "Former Member",
                "sender_email": "former@example.test",
                "content": "Hello @_**Unrelated User|3**",
            },
        },
        "https://zulip.example.invalid",
    )

    participants, topics, authoritative = instance.store.merge_calls[0]
    assert participants == []
    assert topics == [expected_topic]
    assert authoritative is False


def test_catalog_original_urls_follow_zulip_dm_permalink_shapes():
    site = "https://zulip.example.invalid"
    assert service.BridgeService._catalog_original_url(site, "direct:1,2") == (
        f"{site}/#narrow/dm/1,2-dm"
    )
    assert (
        service.BridgeService._catalog_original_url(site, "group_direct:1,2,3")
        == f"{site}/#narrow/dm/1,2,3-group"
    )


def test_first_provider_poll_processes_registration_and_reports_live_ready():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    owner_uuid = "00000000-0000-4000-8000-000000000002"
    project_uuid = "00000000-0000-4000-8000-000000000003"

    class Store:
        def __init__(self):
            self.reports = []
            self.cursor = None
            self.ready = False
            self.mappings = []

        def active_account_uuids(self):
            return [account_uuid]

        def provider_event_cursor(self, requested):
            return self.cursor

        def provider_catchup_ready(self, requested):
            return True

        def pending_provider_catchup(self, requested):
            assert self.cursor == {
                "queue_id": "queue",
                "last_event_id": 10,
                "provider_realm_uuid": ("00000000-0000-4000-8000-000000000004"),
                "provider_owner_user_id": "1",
                "provider_account_generation": 2,
            }
            return None

        def update_provider_event_cursor(
            self,
            requested,
            queue_id,
            event_id,
            provider_realm_uuid=None,
            provider_owner_user_id=None,
            provider_account_generation=None,
        ):
            self.cursor = {
                "queue_id": queue_id,
                "last_event_id": event_id,
                "provider_realm_uuid": (
                    provider_realm_uuid
                    if provider_realm_uuid is not None
                    else (
                        None
                        if self.cursor is None
                        else self.cursor["provider_realm_uuid"]
                    )
                ),
                "provider_owner_user_id": (
                    provider_owner_user_id
                    if provider_owner_user_id is not None
                    else (
                        None
                        if self.cursor is None
                        else self.cursor["provider_owner_user_id"]
                    )
                ),
                "provider_account_generation": (
                    provider_account_generation
                    if provider_account_generation is not None
                    else (
                        None
                        if self.cursor is None
                        else self.cursor["provider_account_generation"]
                    )
                ),
            }

        def account_resource(self, requested):
            return {
                "generation": 2,
                "owner_user_uuid": owner_uuid,
                "settings": {
                    "selection_mode": "all",
                    "default_project_id": project_uuid,
                },
            }

        def enqueue_observed_report(self, report):
            self.reports.append(report)
            return True

        def remember_provider_mapping(self, *args):
            self.mappings.append(args)

        def reconcile_backfill_jobs(self):
            return None

        def catalog_reports_accepted(self, requested, generation):
            return self.ready

        def catalog_assignments_ready(self, requested, generation):
            return self.ready

        def initial_backfill_ready(self, requested):
            return self.ready

        def assignments_needing_live_report(self, requested):
            if not self.ready:
                return []
            return [
                {
                    "uuid": "00000000-0000-4000-8000-000000000042",
                    "generation": 5,
                }
            ]

        def mark_health(self, *args):
            return None

    class Adapter:
        server_url = "https://zulip.example.invalid"

        def __init__(self):
            self.cached_queue = True
            self.invalidations = 0

        def invalidate_queue(self):
            self.cached_queue = False
            self.invalidations += 1

        def ensure_queue(self):
            assert not self.cached_queue
            return "queue", 10

        def restore_queue(self, queue_id, event_id):
            return None

        def take_registration_snapshot(self):
            return {
                "user_id": 1,
                "realm_uuid": "00000000-0000-4000-8000-000000000004",
                "subscriptions": [{"stream_id": 42, "name": "Engineering"}],
                "realm_users": [],
                "recent_private_conversations": [],
            }

        def events(self, queue_id, event_id):
            return []

    adapter = Adapter()
    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: adapter
    instance.scheduler = type(
        "Scheduler", (), {"reconcile_local_echo": lambda *args: None}
    )()
    instance.provider_retry_attempts = {}
    instance.provider_retry_after = {}
    instance.provider_random = type(
        "Random", (), {"uniform": lambda self, lower, upper: lower}
    )()
    instance.account_state_recheck_interval_seconds = 0

    assert instance._poll_provider_account(account_uuid) == (0, None)
    assert adapter.invalidations == 1
    assert instance.store.cursor == {
        "queue_id": "queue",
        "last_event_id": 10,
        "provider_realm_uuid": "00000000-0000-4000-8000-000000000004",
        "provider_owner_user_id": "1",
        "provider_account_generation": 2,
    }
    assert {report["resource_type"] for report in instance.store.reports} == {
        "external_account",
        "external_chat_catalog",
    }
    account_report = next(
        report
        for report in instance.store.reports
        if report["resource_type"] == "external_account"
    )
    assert account_report["status"] == "backfill"

    instance.store.ready = True
    assert instance._poll_provider_account(account_uuid) == (0, None)
    account_report = [
        report
        for report in instance.store.reports
        if report["resource_type"] == "external_account"
    ][-1]
    assert account_report["status"] == "live_ready"
    assignment_report = next(
        report
        for report in instance.store.reports
        if report["resource_type"] == "external_chat_assignment"
    )
    assert assignment_report["resource_uuid"] == (
        "00000000-0000-4000-8000-000000000042"
    )
    assert assignment_report["observed_generation"] == 5
    assert assignment_report["status"] == "live_ready"
    assert assignment_report["progress"]["phase"] == "live"


def test_live_ready_requires_catalog_assignment_but_not_initial_backfill():
    account_uuid = "00000000-0000-4000-8000-000000000001"

    class Store:
        def __init__(self):
            self.cursor = None
            self.account_calls = 0
            self.ready = {
                "catchup": True,
                "catalog": False,
                "assignment": False,
                "backfill": False,
            }

        def account_resource(self, requested):
            self.account_calls += 1
            return {"generation": 3}

        def provider_event_cursor(self, requested):
            return self.cursor

        def reconcile_backfill_jobs(self):
            return None

        def provider_catchup_ready(self, requested):
            return self.ready["catchup"]

        def catalog_reports_accepted(self, requested, generation):
            return self.ready["catalog"]

        def catalog_assignments_ready(self, requested, generation):
            return self.ready["assignment"]

        def initial_backfill_ready(self, requested):
            return self.ready["backfill"]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.account_state_recheck_interval_seconds = 0
    assert not instance._initial_sync_ready(account_uuid)
    instance.store.cursor = {
        "provider_realm_uuid": "00000000-0000-4000-8000-000000000002",
        "provider_owner_user_id": "1",
    }
    assert not instance._initial_sync_ready(account_uuid)
    instance.store.cursor["provider_account_generation"] = 2
    assert not instance._initial_sync_ready(account_uuid)
    instance.store.cursor["provider_account_generation"] = 3
    assert not instance._initial_sync_ready(account_uuid)
    instance.store.ready["catalog"] = True
    assert not instance._initial_sync_ready(account_uuid)
    instance.store.ready["assignment"] = True
    assert instance._initial_sync_ready(account_uuid)
    account_calls = instance.store.account_calls
    instance.store.ready["backfill"] = True
    assert instance._initial_sync_ready(account_uuid)
    assert instance.store.account_calls == account_calls


def test_initial_sync_readiness_backs_off_repeated_negative_probes():
    class Store:
        def __init__(self):
            self.account_calls = 0

        def account_resource(self, requested):
            self.account_calls += 1
            return {"generation": 3}

        def provider_event_cursor(self, requested):
            return {
                "provider_account_generation": 3,
                "provider_realm_uuid": "00000000-0000-4000-8000-000000000001",
                "provider_owner_user_id": "1",
            }

        def provider_catchup_ready(self, requested):
            return False

    instance = object.__new__(service.BridgeService)
    instance.store = Store()

    assert not instance._initial_sync_ready("account")
    assert not instance._initial_sync_ready("account")
    assert instance.store.account_calls == 1


def test_account_report_is_queued_only_when_observed_state_changes():
    reports = []

    class Store:
        def account_resource(self, requested):
            return {"generation": 3}

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance._queue_observed_report = lambda *args, **kwargs: reports.append(
        (args, kwargs)
    )

    instance._queue_account_report("account", "backfill")
    instance._queue_account_report("account", "backfill")
    instance._queue_account_report("account", "live_ready")

    assert [args[3] for args, _kwargs in reports] == ["backfill", "live_ready"]


def test_tick_reconciles_global_backfill_state_once_not_once_per_account(
    tmp_path, monkeypatch
):
    now = 10.0
    monkeypatch.setattr(time, "monotonic", lambda: now)
    account_uuid = "00000000-0000-4000-8000-000000000001"

    class Store:
        def __init__(self):
            self.reconciliations = 0

        def reconcile_backfill_jobs(self):
            self.reconciliations += 1

        def account_resource(self, requested):
            return {"generation": 1}

        def provider_catchup_ready(self, requested):
            return True

        def catalog_reports_accepted(self, requested, generation):
            return True

        def catalog_assignments_ready(self, requested, generation):
            return True

        def initial_backfill_ready(self, requested):
            return True

    class Scheduler:
        def reconcile_once(self):
            return False

        def run_once(self):
            return False

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.last_heartbeat = now
    instance.last_control = now
    instance.last_certificate_check = now
    instance.last_provider_poll = now
    instance.last_history_quantum = now
    instance.health_file = tmp_path / "progress"
    instance.scheduler = Scheduler()
    instance._run_heartbeat = lambda current: False
    instance._run_control_poll = lambda current: False
    instance.poll_provider_operations = lambda: 0
    instance.poll_provider_events = lambda: (
        sum(instance._initial_sync_ready(account_uuid) for _index in range(150)) * 0
    )
    instance._flush_observed_reports = lambda current: 0
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance.flush_provider_events = (
        lambda minimum_priority=0, maximum_priority=2, limit=100: 0
    )
    instance._flush_history_events = lambda: (0, 20, True)
    instance.run_backfill_once = lambda: False

    assert not instance.tick()
    assert instance.store.reconciliations == 1
    assert not instance.tick()
    assert instance.store.reconciliations == 1


def test_observed_report_flush_uses_a_bounded_batch():
    reports = [
        {"report_uuid": str(uuid.uuid4())},
        {"report_uuid": str(uuid.uuid4())},
    ]

    class Store:
        def __init__(self):
            self.applied = []
            self.pending = list(reports)

        def pending_observed_reports(self, limit):
            assert limit == service.BridgeService.OBSERVED_REPORT_BATCH_SIZE
            return self.pending[:limit]

        def apply_observed_report_results(self, results):
            self.applied.extend(results)
            del self.pending[: len(results)]

    class Control:
        def __init__(self):
            self.batches = []

        def observed_reports(self, supplied):
            self.batches.append(list(supplied))
            return {
                "results": [
                    {
                        "report_uuid": report["report_uuid"],
                        "status": "rejected" if report == reports[1] else "applied",
                        "safe_error": (
                            {
                                "code": "temporarily_unavailable",
                                "message": "Try again later.",
                                "retryable": True,
                            }
                            if report == reports[1]
                            else None
                        ),
                    }
                    for report in supplied
                ]
            }

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()
    assert instance.flush_observed_reports() == 2
    assert instance.control.batches == [reports]
    assert [result["status"] for result in instance.store.applied] == [
        "applied",
        "rejected",
    ]


def test_nonretryable_account_report_rejection_releases_observed_state_cache():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    report = {
        "report_uuid": str(uuid.uuid4()),
        "resource_type": "external_account",
        "resource_uuid": account_uuid,
        "observed_generation": 3,
        "status": "live_ready",
        "safe_error": None,
    }

    class Store:
        def pending_observed_reports(self, limit):
            return [report]

        def apply_observed_report_results(self, results):
            return None

    class Control:
        def observed_reports(self, supplied):
            return {
                "results": [
                    {
                        "report_uuid": report["report_uuid"],
                        "status": "rejected",
                        "safe_error": {
                            "code": "invalid_state",
                            "message": "The report cannot be applied.",
                            "retryable": False,
                        },
                    }
                ]
            }

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()
    instance.provider_account_report_states = {account_uuid: (3, "live_ready", None)}

    assert instance.flush_observed_reports() == 1
    assert account_uuid not in instance.provider_account_report_states


def test_suppressed_account_report_is_retried_after_cooldown(monkeypatch):
    account_uuid = "00000000-0000-4000-8000-000000000001"
    queued_reports = []
    enqueue_results = iter([False, True])

    class Store:
        def account_resource(self, supplied_account_uuid):
            assert supplied_account_uuid == account_uuid
            return {"generation": 3}

        def enqueue_observed_report(self, report):
            queued_reports.append(report)
            return next(enqueue_results)

    now = [100.0]
    monkeypatch.setattr(service.time, "monotonic", lambda: now[0])
    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_account_report_states = {}
    instance.provider_account_report_retry_after = {}

    instance._queue_account_report(account_uuid, "live_ready")
    assert len(queued_reports) == 1
    assert account_uuid not in instance.provider_account_report_states
    assert instance.provider_account_report_retry_after[account_uuid] == (
        (3, "live_ready", None),
        400.0,
    )

    now[0] = 399.0
    instance._queue_account_report(account_uuid, "live_ready")
    assert len(queued_reports) == 1

    now[0] = 400.0
    instance._queue_account_report(account_uuid, "live_ready")
    assert len(queued_reports) == 2
    assert instance.provider_account_report_states[account_uuid] == (
        3,
        "live_ready",
        None,
    )
    assert account_uuid not in instance.provider_account_report_retry_after


def test_observed_reports_can_resolve_dependency_with_ready_live_delivery():
    calls = []

    class Store:
        def has_pending_provider_events(self):
            return True

        def has_pending_workspace_deliveries(self, minimum, maximum):
            calls.append(("ready-deliveries", minimum, maximum))
            return True

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control_retry_after = 0.0
    instance.flush_observed_reports = lambda: calls.append("reports") or 1
    instance._set_control_lane_health = lambda *args: None

    assert instance._flush_observed_reports(1.0) == 1
    assert calls == ["reports"]


def test_message_catalog_is_published_once_across_assignment_retries(monkeypatch):
    account_uuid = "00000000-0000-0000-0000-000000000001"
    event = {
        "id": 7,
        "type": "message",
        "message": {
            "id": 601,
            "type": "stream",
            "stream_id": 42,
            "display_recipient": "Engineering",
            "subject": "New topic",
        },
    }

    class Store(DeliveryStore):
        def __init__(self, events):
            super().__init__(events)
            self.catalog_marks = []

        def mark_provider_event_catalog_reported(
            self, requested_account, queue_id, event_id
        ):
            self.catalog_marks.append((requested_account, queue_id, event_id))
            return True

    first_row = {
        "account_uuid": account_uuid,
        "queue_id": "queue",
        "event_id": 7,
        "assignment_catalog_reported_at": None,
        "body": event,
    }
    store = Store([first_row])
    instance = _delivery_service(store)
    catalog_events = []
    instance._queue_event_catalog = (
        lambda requested, supplied, server_url, marker=None: (
            catalog_events.append((requested, supplied, server_url))
            or (store.mark_provider_event_catalog_reported(*marker) if marker else True)
        )
    )
    monkeypatch.setattr(
        instance,
        "_event_records_with_pending_delete_recreations",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValueError("provider_chat_assignment_pending")
        ),
    )

    assert instance.process_provider_journal() == 0
    assert len(catalog_events) == 1
    assert store.catalog_marks == [(account_uuid, "queue", 7)]

    store.events = [
        {
            **first_row,
            "assignment_catalog_reported_at": datetime.datetime.now(datetime.UTC),
            "processing_reason": "provider_chat_assignment_pending",
        }
    ]
    assert instance.process_provider_journal() == 0
    assert len(catalog_events) == 1
    assert store.catalog_marks == [(account_uuid, "queue", 7)]
    assert [retry[-1] for retry in store.retried] == [
        "provider_chat_assignment_pending",
        "provider_chat_assignment_pending",
    ]


def test_retryable_attachment_failure_reschedules_only_the_current_event(monkeypatch):
    account_uuid = "00000000-0000-0000-0000-000000000001"
    store = DeliveryStore(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": {
                    "id": 7,
                    "type": "message",
                    "message": {"id": 7, "type": "stream", "stream_id": 42},
                },
            }
        ]
    )
    instance = _delivery_service(store)
    instance.file_client = object()

    def records(*args, **kwargs):
        raise zulip_adapter.ZulipOperationError("provider_file_unavailable", True)

    monkeypatch.setattr(converter, "event_records", records)
    assert instance.process_provider_journal() == 0
    assert store.retried == [(account_uuid, "queue", 7, "provider_file_unavailable")]
    assert store.processed == []
    assert store.invalid == []


def test_incoming_file_uses_external_chat_uuid_not_projection_stream_uuid(
    monkeypatch,
):
    account_uuid = "00000000-0000-4000-8000-000000000001"
    event = {
        "id": 7,
        "type": "message",
        "message": {"id": 70, "type": "stream", "stream_id": 42},
    }

    class Store(DeliveryStore):
        def effective_file_limit(self, hard_limit):
            return min(hard_limit, 1024)

    class Adapter(ProviderAdapter):
        def download_file(self, provider_url, max_bytes):
            return zulip_adapter.ProviderFile("report.pdf", "application/pdf", b"pdf")

    class FileClient:
        def __init__(self):
            self.chat_uuid = None

        def import_file(
            self, operation_uuid, supplied_account_uuid, chat_uuid, incoming, max_bytes
        ):
            self.chat_uuid = chat_uuid
            return f"urn:file:{incoming.file_uuid}"

    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": event,
            }
        ]
    )
    file_client = FileClient()
    instance = _delivery_service(store)
    instance.file_client = file_client
    instance.provider_adapters = lambda requested: Adapter()

    def records(*args, **kwargs):
        resolver = args[6]
        resolver("/user_uploads/report.pdf", "report.pdf")
        return [{"record_uuid": "record"}]

    monkeypatch.setattr(converter, "event_records", records)
    assert instance.process_provider_journal() == 1
    assert str(file_client.chat_uuid) == converter.stable_entity_uuid(
        account_uuid, "external_chat", "channel:42"
    )
    assert str(file_client.chat_uuid) != converter.stable_entity_uuid(
        account_uuid, "stream", "channel:42"
    )


def test_incoming_update_file_reuses_mapped_message_external_chat_uuid(monkeypatch):
    account_uuid = "00000000-0000-4000-8000-000000000001"
    event = {
        "id": 8,
        "type": "update_message",
        "message_id": 70,
        "message_ids": [70],
        "content": "[report.pdf](/user_uploads/report.pdf)",
    }

    class Store(DeliveryStore):
        def effective_file_limit(self, hard_limit):
            return min(hard_limit, 1024)

        def provider_mapping(self, requested, entity_kind, provider_id):
            assert (requested, entity_kind, provider_id) == (
                account_uuid,
                "message",
                "70",
            )
            return {"metadata": {"chat_key": "channel:42"}}

        def assignment_for_provider_chat(self, requested, chat_key):
            assert (requested, chat_key) == (account_uuid, "channel:42")
            return None

    class Adapter(ProviderAdapter):
        def download_file(self, provider_url, max_bytes):
            return zulip_adapter.ProviderFile("report.pdf", "application/pdf", b"pdf")

    class FileClient:
        def __init__(self):
            self.chat_uuid = None

        def import_file(
            self, operation_uuid, supplied_account_uuid, chat_uuid, incoming, max_bytes
        ):
            self.chat_uuid = chat_uuid
            return f"urn:file:{incoming.file_uuid}"

    store = Store(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 8,
                "body": event,
            }
        ]
    )
    file_client = FileClient()
    instance = _delivery_service(store)
    instance.file_client = file_client
    instance.provider_adapters = lambda requested: Adapter()

    def records(*args, **kwargs):
        args[6]("/user_uploads/report.pdf", "report.pdf")
        return [{"record_uuid": "record"}]

    monkeypatch.setattr(converter, "event_records", records)
    assert instance.process_provider_journal() == 1
    assert str(file_client.chat_uuid) == converter.stable_entity_uuid(
        account_uuid, "external_chat", "channel:42"
    )


def test_permanent_attachment_failure_does_not_enqueue_broken_fallback(monkeypatch):
    account_uuid = "00000000-0000-0000-0000-000000000001"
    store = DeliveryStore(
        [
            {
                "account_uuid": account_uuid,
                "queue_id": "queue",
                "event_id": 7,
                "body": {
                    "id": 7,
                    "type": "message",
                    "message": {"id": 7, "type": "stream", "stream_id": 42},
                },
            }
        ]
    )
    instance = _delivery_service(store)
    instance.file_client = object()

    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            zulip_adapter.ZulipOperationError("provider_file_too_large", False)
        ),
    )
    assert instance.process_provider_journal() == 1
    assert store.retried == []
    assert store.invalid == [(account_uuid, "queue", 7, "provider_file_too_large")]
    assert store.enqueued == []
    assert store.processed == []


@pytest.mark.parametrize(
    ("status_code", "retryable"),
    [(403, False), (503, True)],
)
def test_workspace_file_import_http_failure_is_classified(status_code, retryable):
    account_uuid = "00000000-0000-0000-0000-000000000001"

    class Store(DeliveryStore):
        def effective_file_limit(self, hard_limit):
            return min(hard_limit, 1024)

    class Adapter(ProviderAdapter):
        def download_file(self, provider_url, max_bytes):
            return zulip_adapter.ProviderFile("report.pdf", "application/pdf", b"pdf")

    class FileClient:
        def import_file(self, *args, **kwargs):
            request = httpx.Request("PUT", "https://object.example.invalid/upload")
            raise httpx.HTTPStatusError(
                "file import failed",
                request=request,
                response=httpx.Response(status_code, request=request),
            )

    instance = _delivery_service(Store())
    instance.file_client = FileClient()
    resolver = instance._file_resolver(
        Adapter(),
        account_uuid,
        "00000000-0000-0000-0000-000000000090",
    )

    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        resolver("/user_uploads/report.pdf", "report.pdf")

    assert captured.value.code == "workspace_file_import_unavailable"
    assert captured.value.retryable is retryable


def test_incoming_file_identity_is_stable_per_account_chat_and_provider_url():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    chat_uuid = "00000000-0000-4000-8000-000000000090"

    class Store(DeliveryStore):
        def effective_file_limit(self, hard_limit):
            return min(hard_limit, 1024)

    class Adapter(ProviderAdapter):
        def download_file(self, provider_url, max_bytes):
            return zulip_adapter.ProviderFile("report.pdf", "application/pdf", b"pdf")

    class FileClient:
        def __init__(self):
            self.files = []

        def import_file(
            self,
            operation_uuid,
            supplied_account_uuid,
            supplied_chat_uuid,
            incoming,
            max_bytes,
        ):
            self.files.append((operation_uuid, incoming.file_uuid, incoming.name))
            return f"urn:file:{incoming.file_uuid}"

    instance = _delivery_service(Store())
    instance.file_client = FileClient()
    resolver = instance._file_resolver(Adapter(), account_uuid, chat_uuid)
    first = resolver("/user_uploads/report.pdf", "report.pdf")
    second = resolver("/user_uploads/report.pdf", "renamed.pdf")
    other_chat = instance._file_resolver(
        Adapter(),
        account_uuid,
        "00000000-0000-4000-8000-000000000091",
    )("/user_uploads/report.pdf", "report.pdf")

    assert first == second
    assert first != other_chat
    assert instance.file_client.files[0] == instance.file_client.files[1]
    assert instance.file_client.files[0][2] == "report.pdf"


def test_backfill_is_discovered_newest_first_and_queued_at_priority_two(
    monkeypatch,
):
    store = DeliveryStore()
    queue_ids = []

    def records(*args, **kwargs):
        event = args[3]
        queue_ids.append(args[2])
        return [
            {
                "record_uuid": f"record-{event['message']['id']}",
                "operation_uuid": f"operation-{event['message']['id']}",
            }
        ]

    monkeypatch.setattr(converter, "event_records", records)
    monkeypatch.setattr(
        converter, "provider_chat_reference", lambda message: ("channel", "channel:42")
    )
    messages = [
        {"id": 1, "timestamp": 10},
        {"id": 3, "timestamp": 11},
        {"id": 2, "timestamp": 11},
    ]
    assert (
        _delivery_service(store).enqueue_backfill(
            "00000000-0000-0000-0000-000000000001",
            "channel:42",
            messages,
        )
        == 3
    )
    assert [record["record_uuid"] for record, _ in store.enqueued] == [
        "record-3",
        "record-2",
        "record-1",
    ]
    assert {priority for _, priority in store.enqueued} == {2}
    assert set(queue_ids) == {
        "backfill:channel:42:00000000-0000-4000-8000-000000000090:1"
    }


def test_backfill_bounds_catalog_and_message_transactions(monkeypatch):
    class Store(DeliveryStore):
        def __init__(self):
            super().__init__()
            self.transactions = 0

        @contextlib.contextmanager
        def transaction(self):
            self.transactions += 1
            yield

    store = Store()
    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [
            {
                "record_uuid": f"record-{args[3]['message']['id']}",
                "operation_uuid": f"operation-{args[3]['message']['id']}",
            }
        ],
    )
    monkeypatch.setattr(
        converter, "provider_chat_reference", lambda message: ("channel", "channel:42")
    )

    assert (
        _delivery_service(store).enqueue_backfill(
            "00000000-0000-0000-0000-000000000001",
            "channel:42",
            [{"id": value, "timestamp": value} for value in range(1, 13)],
        )
        == 12
    )
    assert store.transactions == 4


def test_backfill_uses_single_message_transactions_while_live_work_is_pending(
    monkeypatch,
):
    class Store(DeliveryStore):
        def __init__(self):
            super().__init__()
            self.transactions = 0

        @contextlib.contextmanager
        def transaction(self):
            self.transactions += 1
            yield

    store = Store()
    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [
            {
                "record_uuid": f"record-{args[3]['message']['id']}",
                "operation_uuid": f"operation-{args[3]['message']['id']}",
            }
        ],
    )
    monkeypatch.setattr(
        converter, "provider_chat_reference", lambda message: ("channel", "channel:42")
    )
    instance = _delivery_service(store)
    instance._live_workspace_delivery_pending = lambda: True

    assert (
        instance.enqueue_backfill(
            "00000000-0000-0000-0000-000000000001",
            "channel:42",
            [{"id": value, "timestamp": value} for value in range(1, 13)],
        )
        == 12
    )
    assert store.transactions == 24


def test_backfill_isolates_attachment_transfers_from_batched_transactions(
    monkeypatch,
):
    class Store(DeliveryStore):
        def __init__(self):
            super().__init__()
            self.current_transaction = None
            self.completed_transactions = []

        @contextlib.contextmanager
        def transaction(self):
            self.current_transaction = []
            try:
                yield
            finally:
                self.completed_transactions.append(self.current_transaction)
                self.current_transaction = None

    store = Store()

    def records(*args, **kwargs):
        message_id = int(args[3]["message"]["id"])
        store.current_transaction.append(message_id)
        return [
            {
                "record_uuid": f"record-{message_id}",
                "operation_uuid": f"operation-{message_id}",
            }
        ]

    monkeypatch.setattr(converter, "event_records", records)
    monkeypatch.setattr(
        converter, "provider_chat_reference", lambda message: ("channel", "channel:42")
    )
    instance = _delivery_service(store)
    instance.file_client = object()
    messages = [
        {
            "id": value,
            "timestamp": value,
            "content": (
                "[report.pdf](/user_uploads/report.pdf)"
                if value == 6
                else f"message {value}"
            ),
        }
        for value in range(1, 13)
    ]

    assert (
        instance.enqueue_backfill(
            "00000000-0000-0000-0000-000000000001",
            "channel:42",
            messages,
        )
        == 12
    )
    assert store.completed_transactions[-3:] == [
        [12, 11, 10, 9, 8, 7],
        [6],
        [5, 4, 3, 2, 1],
    ]


def test_backfill_keeps_first_accepted_digest_for_repeated_history(monkeypatch):
    class Store(DeliveryStore):
        def __init__(self):
            super().__init__()
            self.attempted = []

        def enqueue_workspace_delivery(self, record, priority):
            self.attempted.append((record, priority))
            if record["operation_uuid"] == "operation-2":
                raise ValueError("Operation UUID reused with a different digest")
            return True

    store = Store()

    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: [
            {
                "record_uuid": f"record-{args[3]['message']['id']}",
                "operation_uuid": f"operation-{args[3]['message']['id']}",
            }
        ],
    )
    monkeypatch.setattr(
        converter, "provider_chat_reference", lambda message: ("channel", "channel:42")
    )

    assert (
        _delivery_service(store).enqueue_backfill(
            "00000000-0000-0000-0000-000000000001",
            "channel:42",
            [
                {"id": 1, "timestamp": 10},
                {"id": 2, "timestamp": 11},
            ],
        )
        == 1
    )
    assert [record["operation_uuid"] for record, _ in store.attempted] == [
        "operation-2",
        "operation-1",
    ]


def test_backfill_caches_durable_topic_upsert_per_assignment_generation(monkeypatch):
    store = DeliveryStore()
    instance = _delivery_service(store)

    def records(*args, **kwargs):
        message_id = args[3]["message"]["id"]
        return [
            {
                "record_uuid": f"topic-{message_id}",
                "operation_uuid": f"topic-operation-{message_id}",
                "operation": {
                    "kind": "topic.upsert",
                    "entity_uuid": "00000000-0000-4000-8000-000000000091",
                },
            },
            {
                "record_uuid": f"message-{message_id}",
                "operation_uuid": f"message-operation-{message_id}",
                "operation": {
                    "kind": "message.update",
                    "entity_uuid": f"00000000-0000-4000-8000-{message_id:012d}",
                },
            },
        ]

    monkeypatch.setattr(converter, "event_records", records)
    monkeypatch.setattr(
        converter, "provider_chat_reference", lambda message: ("channel", "channel:42")
    )

    assert (
        instance.enqueue_backfill(
            "00000000-0000-0000-0000-000000000001",
            "channel:42",
            [{"id": 2, "timestamp": 2}, {"id": 1, "timestamp": 1}],
        )
        == 3
    )
    assert (
        instance.enqueue_backfill(
            "00000000-0000-0000-0000-000000000001",
            "channel:42",
            [{"id": 0, "timestamp": 0}],
        )
        == 1
    )
    assert [record["record_uuid"] for record, _priority in store.enqueued] == [
        "topic-2",
        "message-2",
        "message-1",
        "message-0",
    ]


def test_backfill_does_not_enqueue_broken_attachment_fallback(monkeypatch):
    store = DeliveryStore()
    instance = _delivery_service(store)

    def records(*args, **kwargs):
        resolver = args[6]
        if resolver is not None:
            resolver("/user_uploads/report.pdf", "report.pdf")
        pytest.fail("attachment failure must abort conversion")

    monkeypatch.setattr(converter, "event_records", records)
    monkeypatch.setattr(
        converter, "provider_chat_reference", lambda message: ("channel", "channel:42")
    )
    instance._file_resolver = lambda *args: (
        lambda *resolver_args: (_ for _ in ()).throw(
            zulip_adapter.ZulipOperationError(
                "workspace_file_import_unavailable", False
            )
        )
    )

    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        instance.enqueue_backfill(
            "00000000-0000-0000-0000-000000000001",
            "channel:42",
            [{"id": 7, "timestamp": 7}],
        )
    assert captured.value.code == "workspace_file_import_unavailable"
    assert store.enqueued == []


def test_backfill_discovers_all_topics_before_waiting_for_workspace_mappings(
    monkeypatch,
):
    instance = _delivery_service(DeliveryStore())
    discovered = []
    instance._queue_event_catalog = lambda account, event, server: discovered.append(
        event["message"]["subject"]
    )
    monkeypatch.setattr(
        converter,
        "event_records",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            ValueError("provider_chat_assignment_pending")
        ),
    )

    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        instance.enqueue_backfill(
            "00000000-0000-0000-0000-000000000001",
            "channel:42",
            [
                {
                    "id": 1,
                    "timestamp": 10,
                    "type": "stream",
                    "stream_id": 42,
                    "subject": "older",
                },
                {
                    "id": 2,
                    "timestamp": 11,
                    "type": "stream",
                    "stream_id": 42,
                    "subject": "newer",
                },
            ],
        )

    assert discovered == ["newer", "older"]


def test_queue_loss_catchup_recovers_create_edit_delete_before_live_ready(
    monkeypatch,
):
    store = CatchupStore()
    instance = _delivery_service(store)
    created_batches = []

    def enqueue_backfill(account_uuid, chat_key, messages):
        message_ids = [message["id"] for message in messages]
        created_batches.append(message_ids)
        store.created.extend(message_ids)
        return len(messages)

    instance.enqueue_backfill = enqueue_backfill
    converted_events = []

    def records(*args, **kwargs):
        event = args[3]
        converted_events.append((event["type"], event.get("message_id"), kwargs))
        message_id = event.get("message_id", event.get("message_ids", [0])[0])
        return [
            {
                "operation_uuid": f"{event['type']}:{message_id}",
                "record_uuid": f"record:{event['type']}:{message_id}",
            }
        ]

    monkeypatch.setattr(converter, "event_records", records)
    assert instance._run_provider_queue_catchup(
        "00000000-0000-0000-0000-000000000001", CatchupAdapter()
    )
    assert created_batches == [[13, 12]]
    assert store.created == [13, 12]
    assert [
        (event_type, message_id) for event_type, message_id, _ in converted_events
    ] == [
        ("update_message", 10),
        ("delete_message", None),
    ]
    assert {priority for _, priority in store.enqueued} == {2}
    assert store.advanced == [([10, 12, 13], 9, True, None)]


@pytest.mark.parametrize(
    "pending_gate",
    [
        "provider_chat_assignment_pending",
        "provider_chat_participants_pending",
    ],
)
def test_queue_loss_catchup_waits_for_workspace_chat_gates(pending_gate):
    store = CatchupStore()
    store.mappings = {}
    instance = _delivery_service(store)
    instance.enqueue_backfill = lambda *args: (_ for _ in ()).throw(
        ValueError(pending_gate)
    )

    assert not instance._run_provider_queue_catchup(
        "00000000-0000-0000-0000-000000000001", CatchupAdapter()
    )
    assert store.advanced == []


def test_unauthorized_catchup_account_is_quarantined_without_blocking_healthy():
    unauthorized_uuid = "00000000-0000-4000-8000-000000000001"
    healthy_uuid = "00000000-0000-4000-8000-000000000002"
    failures = []
    successes = []
    health = []
    reports = []
    catchups = []

    class Store:
        def active_account_uuids(self):
            return [unauthorized_uuid, healthy_uuid]

        def eligible_account_uuids(self):
            return [unauthorized_uuid, healthy_uuid]

        def provider_catchup_ready(self, account_uuid):
            return False

        def record_provider_account_failure(
            self, account_uuid, attempted_generation, code, retryable
        ):
            failures.append((account_uuid, attempted_generation, code, retryable))
            return {"provider_state": "auth_required"}

        def record_provider_account_success(self, account_uuid, attempted_generation):
            successes.append((account_uuid, attempted_generation))
            return False

        def mark_health(self, *args):
            health.append(args)

        def clear_health(self, *_args):
            return None

        def account_resource(self, account_uuid):
            return {"generation": 1}

        def enqueue_observed_report(self, report):
            reports.append(report)
            return True

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda account_uuid: (
        (_ for _ in ()).throw(
            zulip_adapter.ZulipOperationError("bad_api_key", False, 1)
        )
        if account_uuid == unauthorized_uuid
        else type("Adapter", (), {"account_generation": 1})()
    )
    instance._run_provider_queue_catchup = lambda account_uuid, _adapter: (
        catchups.append(account_uuid) or True
    )

    assert instance.run_provider_catchup_once()
    assert failures == [(unauthorized_uuid, 1, "unauthorized_account", False)]
    assert catchups == [healthy_uuid]
    assert successes == [(healthy_uuid, 1)]
    assert health == [
        (
            f"provider:{unauthorized_uuid}",
            "degraded",
            "unauthorized_account",
        )
    ]
    assert reports[0]["status"] == "auth_required"
    assert reports[0]["progress"]["phase"] == "auth_required"


def test_ready_live_work_preempts_slow_history_and_backfill_delivery(tmp_path):
    calls = []

    class Scheduler:
        def reconcile_once(self):
            calls.append("reconcile")
            return False

        def run_once(self):
            calls.append("live-provider-call")
            return True

    instance = object.__new__(service.BridgeService)
    instance.last_heartbeat = time.monotonic()
    instance.last_control = time.monotonic()
    instance.last_certificate_check = time.monotonic()
    instance.last_provider_poll = time.monotonic()
    instance.health_file = tmp_path / "progress"
    instance.scheduler = Scheduler()
    instance.poll_provider_operations = lambda: calls.append("provider-operations") or 1
    instance.poll_provider_events = lambda: calls.append("provider-events") or 0
    instance.process_provider_journal = lambda: calls.append("journal") or 0
    instance.flush_provider_results = lambda: calls.append("results") or 0
    instance.flush_provider_events = (
        lambda minimum_priority=0, maximum_priority=2, limit=100: (
            calls.append(f"delivery:{minimum_priority}:{maximum_priority}:{limit}") or 0
        )
    )

    def slow_history():
        raise AssertionError("slow backfill must not run while live work is ready")

    instance.run_backfill_once = slow_history
    assert instance.tick()
    assert calls.index("live-provider-call") < calls.index("delivery:0:0:20")
    assert "delivery:2:2:1" not in calls


def test_pending_provider_journal_preempts_history_before_live_outbox_exists():
    calls = []

    class Store:
        def has_pending_provider_events(self):
            calls.append("journal")
            return True

        def pending_workspace_deliveries(self, **kwargs):
            calls.append(("outbox", kwargs))
            return []

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.flush_provider_events = lambda **kwargs: calls.append(kwargs) or 0
    instance._run_history_quantum_once = lambda: calls.append("history") or True

    assert not instance._run_history_lane_once()
    assert calls == ["journal"]


def test_live_pending_probe_uses_cheap_outbox_exists_query():
    calls = []

    class Store:
        def has_pending_provider_events(self):
            return False

        def has_pending_workspace_deliveries(self, minimum, maximum):
            calls.append((minimum, maximum))
            return True

        def pending_workspace_deliveries(self, **kwargs):
            raise AssertionError("the dependency projection query must not run")

    instance = object.__new__(service.BridgeService)
    instance.store = Store()

    assert instance._live_workspace_delivery_pending()
    assert calls == [(0, 0)]


def test_idle_history_uses_full_large_profile_delivery_batch():
    calls = []

    instance = object.__new__(service.BridgeService)
    instance.provider_batch_size = 100
    instance._live_workspace_delivery_pending = lambda: False
    instance._flush_provider_events_locked = (
        lambda **kwargs: calls.append(kwargs) or 100
    )
    instance._run_history_quantum_once = lambda: False

    assert instance._run_history_lane_once()
    assert calls == [
        {
            "minimum_priority": 2,
            "maximum_priority": 2,
            "limit": service.BridgeService.HISTORY_DELIVERY_BATCH_SIZE,
        }
    ]


def test_provider_delivery_backoff_pauses_history_discovery(monkeypatch):
    now = [10.0]
    calls = []
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    instance = object.__new__(service.BridgeService)
    instance.provider_batch_size = 100
    instance.provider_delivery_retry_after = now[0] + 30.0
    instance._live_workspace_delivery_pending = lambda: False
    instance._flush_provider_events_locked = lambda **kwargs: (_ for _ in ()).throw(
        AssertionError("history delivery must remain paused during Provider backoff")
    )
    instance._run_history_quantum_once = lambda: calls.append("discovery") or True

    assert not instance._run_history_lane_once()
    assert calls == []


def test_background_live_worker_does_not_race_dedicated_delivery_worker():
    calls = []

    class Scheduler:
        def reconcile_once(self):
            return False

        def run_once(self):
            return False

    instance = object.__new__(service.BridgeService)
    instance.background_live_delivery_enabled = True
    instance.scheduler = Scheduler()
    instance.poll_provider_operations = lambda: 0
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance._ready_live_workspace_delivery_pending = lambda: True
    instance.flush_provider_events = lambda **kwargs: calls.append(kwargs) or 1

    assert not instance._run_live_lane_once()
    assert calls == []


def test_history_workers_serialize_delivery_selection_and_submission():
    first_delivery_started = threading.Event()
    release_first_delivery = threading.Event()
    second_worker_started = threading.Event()
    calls = []

    instance = object.__new__(service.BridgeService)
    instance.history_delivery_lock = threading.Lock()
    instance.provider_batch_size = service.BridgeService.HISTORY_DELIVERY_BATCH_SIZE
    instance._live_workspace_delivery_pending = lambda: False
    instance._run_history_quantum_once = lambda: False

    def flush_history_events():
        calls.append("history")
        if len(calls) == 1:
            first_delivery_started.set()
            assert release_first_delivery.wait(timeout=1.0)
        return (
            service.BridgeService.HISTORY_DELIVERY_BATCH_SIZE,
            service.BridgeService.HISTORY_DELIVERY_BATCH_SIZE,
            True,
        )

    instance._flush_history_events = flush_history_events

    first_worker = threading.Thread(target=instance._run_history_lane_once)
    second_worker = threading.Thread(
        target=lambda: (
            second_worker_started.set(),
            instance._run_history_lane_once(),
        )
    )
    first_worker.start()
    assert first_delivery_started.wait(timeout=1.0)
    second_worker.start()
    assert second_worker_started.wait(timeout=1.0)
    time.sleep(0.05)
    assert len(calls) == 1

    release_first_delivery.set()
    first_worker.join(timeout=1.0)
    second_worker.join(timeout=1.0)

    assert not first_worker.is_alive()
    assert not second_worker.is_alive()
    assert len(calls) == 2


def test_history_rechecks_live_work_after_waiting_for_provider_mutex(monkeypatch):
    started = threading.Event()
    live_pending = [False]
    calls = []
    result = []
    now = [10.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    instance = object.__new__(service.BridgeService)
    instance.provider_batch_size = 100
    instance.last_history_quantum = now[0] - 1.0
    instance.provider_delivery_lock = threading.Lock()
    instance._live_workspace_delivery_pending = lambda: live_pending[0]
    instance._flush_provider_events_locked = (
        lambda **kwargs: calls.append(kwargs) or 1
    )

    instance.provider_delivery_lock.acquire()

    def flush_history():
        started.set()
        result.append(instance._flush_history_events())

    worker = threading.Thread(target=flush_history)
    worker.start()
    assert started.wait(timeout=1.0)
    live_pending[0] = True
    instance.provider_delivery_lock.release()
    worker.join(timeout=1.0)

    assert not worker.is_alive()
    assert result == [(1, 1, True)]
    assert calls == [
        {
            "minimum_priority": 2,
            "maximum_priority": 2,
            "limit": service.BridgeService.HISTORY_LIVE_DELIVERY_BATCH_SIZE,
        }
    ]


def test_continuous_live_work_still_runs_bounded_history_quantum(tmp_path, monkeypatch):
    calls = []
    now = [10.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    class Scheduler:
        def reconcile_once(self):
            return False

        def run_once(self):
            calls.append("live")
            return True

    instance = object.__new__(service.BridgeService)
    instance.last_heartbeat = now[0]
    instance.last_control = now[0]
    instance.last_certificate_check = now[0]
    instance.last_provider_poll = now[0]
    instance.last_history_quantum = now[0] - 1.0
    instance.health_file = tmp_path / "progress"
    instance.scheduler = Scheduler()
    instance._run_heartbeat = lambda current: False
    instance._run_control_poll = lambda current: False
    instance.poll_provider_operations = lambda: 0
    instance.poll_provider_events = lambda: 0
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance.flush_provider_events = (
        lambda minimum_priority=0, maximum_priority=2, limit=100: (
            calls.append(f"delivery:{minimum_priority}:{maximum_priority}:{limit}") or 0
        )
    )
    instance._flush_history_events = lambda: (
        calls.append(
            f"delivery:2:2:{service.BridgeService.HISTORY_LIVE_DELIVERY_BATCH_SIZE}"
        )
        or (0, service.BridgeService.HISTORY_LIVE_DELIVERY_BATCH_SIZE, True)
    )
    instance.run_backfill_once = lambda: calls.append("backfill") or True

    assert instance.tick()
    assert calls == [
        "live",
        "delivery:0:0:20",
        f"delivery:2:2:{service.BridgeService.HISTORY_LIVE_DELIVERY_BATCH_SIZE}",
        "backfill",
    ]

    calls.clear()
    instance.background_history_enabled = True
    now[0] += 1.0
    assert instance.tick()
    assert calls == ["live", "delivery:0:0:20"]


def test_history_quantum_runs_in_the_main_service_thread(tmp_path, monkeypatch):
    history_threads = []
    now = [10.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    class Scheduler:
        def reconcile_once(self):
            return False

        def run_once(self):
            return False

    instance = object.__new__(service.BridgeService)
    instance.last_heartbeat = now[0]
    instance.last_control = now[0]
    instance.last_certificate_check = now[0]
    instance.last_provider_poll = now[0]
    instance.last_history_quantum = now[0] - 1.0
    instance.health_file = tmp_path / "progress"
    instance.scheduler = Scheduler()
    instance._run_heartbeat = lambda current: False
    instance._run_control_poll = lambda current: False
    instance.poll_provider_operations = lambda: 0
    instance.poll_provider_events = lambda: 0
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance.flush_provider_events = lambda **kwargs: 0
    instance._flush_history_events = lambda: (0, 20, True)

    def history():
        history_threads.append(threading.get_ident())
        return True

    instance._run_history_quantum_once = history

    calling_thread = threading.get_ident()
    assert instance.tick()
    assert history_threads == [calling_thread]


def test_longpoll_persists_live_event_while_main_thread_runs_history(
    tmp_path, monkeypatch
):
    account_uuid = "00000000-0000-4000-8000-000000000001"
    now = [10.0]
    history_started = threading.Event()
    release_history = threading.Event()
    event_recorded = threading.Event()
    release_second_poll = threading.Event()
    tick_finished = threading.Event()
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    class Store:
        def __init__(self):
            self.events = []

        def active_account_uuids(self):
            return [account_uuid]

        def provider_event_cursor(self, requested):
            return {"queue_id": "queue", "last_event_id": 4}

        def account_resource(self, requested):
            return None

        def record_provider_event(self, requested, queue_id, event):
            self.events.append((requested, queue_id, event["id"]))
            event_recorded.set()

        def update_provider_event_cursor(self, *args):
            return None

        def mark_health(self, *args):
            return None

    class Adapter:
        def __init__(self):
            self.calls = 0

        def restore_queue(self, queue_id, last_event_id):
            return None

        def events(self, queue_id, last_event_id):
            self.calls += 1
            if self.calls == 1:
                assert history_started.wait(timeout=2)
                return [{"id": 5, "type": "realm_user"}]
            assert release_second_poll.wait(timeout=2)
            return [{"id": 6, "type": "heartbeat"}]

    class Scheduler:
        def reconcile_once(self):
            return False

        def run_once(self):
            return False

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.provider_retry_attempts = {}
    instance.provider_retry_after = {}
    instance.provider_random = type(
        "Random", (), {"uniform": lambda self, lower, upper: lower}
    )()
    instance.last_heartbeat = now[0]
    instance.last_control = now[0]
    instance.last_certificate_check = now[0]
    instance.last_provider_poll = 0.0
    instance.last_history_quantum = now[0] - 1.0
    instance.health_file = tmp_path / "progress"
    instance.scheduler = Scheduler()
    instance._run_heartbeat = lambda current: False
    instance._run_control_poll = lambda current: False
    instance.poll_provider_operations = lambda: 0
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance.flush_provider_events = lambda **kwargs: 0
    instance._flush_history_events = lambda: (0, 20, True)
    instance._flush_observed_reports = lambda current: 0

    def history():
        history_started.set()
        assert release_history.wait(timeout=2)
        return True

    instance._run_history_quantum_once = history

    def run_tick():
        instance.tick()
        tick_finished.set()

    service_thread = threading.Thread(target=run_tick)
    service_thread.start()
    assert history_started.wait(timeout=1)
    assert event_recorded.wait(timeout=1)
    assert not tick_finished.is_set()
    assert instance.store.events == [(account_uuid, "queue", 5)]

    instance.provider_poll_stops[account_uuid].set()
    release_history.set()
    release_second_poll.set()
    service_thread.join(timeout=1)
    instance.provider_poll_threads[account_uuid].join(timeout=1)
    assert tick_finished.is_set()


def test_full_history_delivery_batch_defers_more_provider_io(tmp_path, monkeypatch):
    calls = []
    now = [10.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    class Scheduler:
        def reconcile_once(self):
            return False

        def run_once(self):
            return False

    instance = object.__new__(service.BridgeService)
    instance.last_heartbeat = now[0]
    instance.last_control = now[0]
    instance.last_certificate_check = now[0]
    instance.last_provider_poll = now[0]
    instance.last_history_quantum = now[0] - 1.0
    instance.health_file = tmp_path / "progress"
    instance.scheduler = Scheduler()
    instance.poll_provider_operations = lambda: 0
    instance.poll_provider_events = lambda: 0
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance.flush_provider_events = (
        lambda minimum_priority=0, maximum_priority=2, limit=100: (
            calls.append(f"delivery:{minimum_priority}:{maximum_priority}:{limit}")
            or 0
        )
    )
    instance._flush_history_events = lambda: (
        calls.append(
            f"delivery:2:2:{service.BridgeService.HISTORY_DELIVERY_BATCH_SIZE}"
        )
        or (
            service.BridgeService.HISTORY_DELIVERY_BATCH_SIZE,
            service.BridgeService.HISTORY_DELIVERY_BATCH_SIZE,
            True,
        )
    )
    instance.run_backfill_once = lambda: calls.append("backfill") or True

    assert instance.tick()
    assert calls == [
        "delivery:0:0:20",
        f"delivery:2:2:{service.BridgeService.HISTORY_DELIVERY_BATCH_SIZE}",
    ]


def test_retryable_backfill_error_is_durably_deferred_with_full_jitter():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    deferred = []
    health = []

    class Store:
        def claim_backfill_job(self):
            return {
                "account_uuid": account_uuid,
                "provider_chat_key": "channel:42",
                "next_anchor": None,
                "cutoff_at": None,
                "retry_count": 2,
            }

        def account_is_active(self, requested):
            return True

        def defer_backfill_job(self, *args):
            deferred.append(args)

        def mark_health(self, *args):
            health.append(args)

    class Adapter:
        def message_history(self, provider_chat_key, anchor):
            raise zulip_adapter.ZulipOperationError("provider_unavailable", True)

    class FixedRandom:
        def uniform(self, lower, upper):
            assert (lower, upper) == (0.0, 4.0)
            return upper

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.provider_random = FixedRandom()

    before = time.time()
    assert instance.run_backfill_once()
    after = time.time()
    assert deferred[0][0:2] == (account_uuid, "channel:42")
    assert 3.9 <= deferred[0][2].timestamp() - before
    assert deferred[0][2].timestamp() - after <= 4.1
    assert deferred[0][3] == "provider_unavailable"
    assert health == [("provider", "degraded", "provider_unavailable")]


def test_retryable_file_import_during_backfill_is_durably_deferred():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    deferred = []

    class Store:
        def claim_backfill_job(self):
            return {
                "account_uuid": account_uuid,
                "provider_chat_key": "channel:42",
                "next_anchor": None,
                "cutoff_at": None,
                "retry_count": 0,
            }

        def account_is_active(self, requested):
            return True

        def defer_backfill_job(self, *args):
            deferred.append(args)

        def mark_health(self, *args):
            return None

    class Adapter:
        def message_history(self, provider_chat_key, anchor):
            return [{"id": 7, "timestamp": 7}]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.provider_random = type(
        "Random", (), {"uniform": lambda self, lower, upper: upper}
    )()
    instance.enqueue_backfill = lambda *args: (_ for _ in ()).throw(
        zulip_adapter.ZulipOperationError("workspace_file_import_unavailable", True)
    )

    assert instance.run_backfill_once()
    assert deferred[0][0:2] == (account_uuid, "channel:42")
    assert deferred[0][3] == "workspace_file_import_unavailable"


def test_backfill_waits_for_workspace_chat_mappings_without_crashing():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    released = []
    advanced = []

    class Store:
        def claim_backfill_job(self):
            return {
                "account_uuid": account_uuid,
                "provider_chat_key": "channel:42",
                "next_anchor": None,
                "cutoff_at": None,
                "retry_count": 0,
            }

        def account_is_active(self, requested):
            return True

        def release_backfill_job(self, *args):
            released.append(args)

        def advance_backfill_job(self, *args):
            advanced.append(args)

    class Adapter:
        def message_history(self, provider_chat_key, anchor):
            return [{"id": 7, "timestamp": 7}]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.enqueue_backfill = lambda *args: (_ for _ in ()).throw(
        ValueError("provider_chat_assignment_pending")
    )

    assert not instance.run_backfill_once()
    assert released == [(account_uuid, "channel:42")]
    assert advanced == []


def test_backfill_releases_job_after_retryable_database_conflict():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    released = []

    class DeadlockDetected(Exception):
        sqlstate = "40P01"

    class Store:
        def claim_backfill_job(self):
            return {
                "account_uuid": account_uuid,
                "provider_chat_key": "channel:42",
                "next_anchor": None,
                "cutoff_at": None,
                "retry_count": 0,
            }

        def account_is_active(self, requested):
            return True

        def release_backfill_job(self, *args):
            released.append(args)

    class Adapter:
        def message_history(self, provider_chat_key, anchor):
            return [{"id": 7, "timestamp": 7}]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.enqueue_backfill = lambda *args: (_ for _ in ()).throw(DeadlockDetected())

    assert instance.run_backfill_once()
    assert released == [(account_uuid, "channel:42")]


def test_background_history_lane_retries_database_conflict(monkeypatch):
    calls = []

    class SerializationFailure(Exception):
        sqlstate = "40001"

    class TerminalFailure(Exception):
        pass

    instance = object.__new__(service.BridgeService)
    instance.background_history_error = None

    def history_once():
        calls.append("history")
        if len(calls) == 1:
            raise SerializationFailure()
        raise TerminalFailure()

    instance._run_history_lane_once = history_once
    monkeypatch.setattr(service.random, "uniform", lambda lower, upper: lower)
    monkeypatch.setattr(service.time, "sleep", lambda seconds: None)

    instance._run_background_history_lane()

    assert calls == ["history", "history"]
    assert isinstance(instance.background_history_error, TerminalFailure)


def test_background_history_lane_yields_after_successful_quanta(monkeypatch):
    calls = []
    sleeps = []

    class TerminalFailure(Exception):
        pass

    instance = object.__new__(service.BridgeService)
    instance.background_history_error = None

    def history_once():
        calls.append("history")
        if len(calls) == 1:
            return True
        raise TerminalFailure()

    instance._run_history_lane_once = history_once
    monkeypatch.setattr(service.time, "sleep", sleeps.append)

    instance._run_background_history_lane()

    assert calls == ["history", "history"]
    assert sleeps == [service.BridgeService.HISTORY_PROGRESS_YIELD_SECONDS]
    assert isinstance(instance.background_history_error, TerminalFailure)


def test_retryable_database_conflict_unwraps_driver_error():
    class DeadlockDetected(Exception):
        code = "40P01"

    try:
        try:
            raise DeadlockDetected()
        except DeadlockDetected as error:
            raise RuntimeError("database operation failed") from error
    except RuntimeError as error:
        assert service.BridgeService._is_retryable_database_conflict(error)


def test_non_retryable_backfill_error_fails_only_affected_job_and_reports_it():
    account_uuid = "00000000-0000-4000-8000-000000000001"
    failed = []
    health = []
    reports = []

    class Store:
        def claim_backfill_job(self):
            return {
                "account_uuid": account_uuid,
                "provider_chat_key": "channel:42",
                "next_anchor": None,
                "cutoff_at": None,
                "retry_count": 0,
            }

        def account_is_active(self, requested):
            return True

        def fail_backfill_job(self, *args):
            failed.append(args)

        def mark_health(self, *args):
            health.append(args)

        def account_resource(self, requested):
            return {"generation": 3}

        def enqueue_observed_report(self, report):
            reports.append(report)
            return True

    class Adapter:
        def message_history(self, provider_chat_key, anchor):
            raise zulip_adapter.ZulipOperationError("provider_forbidden", False)

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()

    assert instance.run_backfill_once()
    assert failed == [(account_uuid, "channel:42", "provider_forbidden")]
    assert health == [
        (
            f"provider:{account_uuid}:channel:42",
            "degraded",
            "provider_forbidden",
        )
    ]
    assert reports[0]["resource_type"] == "external_account"
    assert reports[0]["status"] == "degraded"
    assert reports[0]["progress"]["phase"] == "retry"
    assert reports[0]["safe_error"]["code"] == "provider_forbidden"


def test_provider_events_use_a_monotonic_two_second_schedule(tmp_path, monkeypatch):
    calls = []

    class Scheduler:
        def reconcile_once(self):
            return False

        def run_once(self):
            return False

    now = [1.99]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    instance = object.__new__(service.BridgeService)
    instance.last_heartbeat = 100.0
    instance.last_control = 100.0
    instance.last_certificate_check = 100.0
    instance.last_provider_poll = 0.0
    instance.health_file = tmp_path / "progress"
    instance.scheduler = Scheduler()
    instance.poll_provider_operations = lambda: 0
    instance.poll_provider_events = lambda: calls.append(now[0]) or 0
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance.flush_provider_events = (
        lambda minimum_priority=0, maximum_priority=2, limit=100: 0
    )
    instance._flush_history_events = lambda: (0, 20, True)
    instance.run_backfill_once = lambda: False

    assert not instance.tick()
    assert calls == []

    now[0] = 2.0
    assert not instance.tick()
    assert calls == [2.0]

    now[0] = 3.99
    assert not instance.tick()
    assert calls == [2.0]

    now[0] = 4.0
    assert not instance.tick()
    assert calls == [2.0, 4.0]


def test_control_transport_outage_retries_with_full_jitter_and_recovers(
    tmp_path, monkeypatch
):
    now = [10.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    request = httpx.Request("PUT", "https://control.invalid/v1/heartbeat")
    heartbeat_results = iter(
        (
            httpx.ConnectError("temporarily unavailable", request=request),
            httpx.ConnectError("temporarily unavailable", request=request),
            {"heartbeat_uuid": "ignored"},
        )
    )
    ceilings = []

    class Random:
        def uniform(self, lower, upper):
            ceilings.append((lower, upper))
            return upper / 2

    class Store:
        def __init__(self):
            self.cursor = "cursor-7"
            self.health = []

        def mark_health(self, component, status, code=None):
            self.health.append((component, status, code))

    class Control:
        def heartbeat(self, blocked_batch=None):
            result = next(heartbeat_results)
            if isinstance(result, BaseException):
                raise result
            return result

    class Scheduler:
        def reconcile_once(self):
            return False

        def run_once(self):
            return False

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()
    instance.last_heartbeat = 0.0
    instance.last_control = 100.0
    instance.last_certificate_check = 100.0
    instance.last_provider_poll = 100.0
    instance.heartbeat_interval_seconds = 10.0
    instance.control_retry_base_seconds = 1.0
    instance.control_retry_cap_seconds = 30.0
    instance.control_retry_after_cap_seconds = 300.0
    instance.heartbeat_retry_attempts = 0
    instance.heartbeat_retry_after = 0.0
    instance.control_retry_attempts = 0
    instance.control_retry_after = 0.0
    instance.control_random = Random()
    instance.certificate_renewer = None
    instance.health_file = tmp_path / "progress"
    instance.scheduler = Scheduler()
    instance.poll_provider_operations = lambda: 0
    instance.poll_provider_events = lambda: 0
    instance.flush_observed_reports = lambda: 0
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance.flush_provider_events = (
        lambda minimum_priority=0, maximum_priority=2, limit=100: 0
    )
    instance._flush_history_events = lambda: (0, 20, True)
    instance.run_backfill_once = lambda: False

    assert not instance.tick()
    assert instance.store.cursor == "cursor-7"
    assert instance.heartbeat_retry_attempts == 1
    assert instance.heartbeat_retry_after == 10.5
    assert instance.health_file.is_file()

    now[0] = 10.49
    assert not instance.tick()
    assert instance.heartbeat_retry_attempts == 1

    now[0] = 10.5
    assert not instance.tick()
    assert instance.heartbeat_retry_attempts == 2
    assert instance.heartbeat_retry_after == 11.5

    now[0] = 11.5
    assert instance.tick()
    assert instance.last_heartbeat == 11.5
    assert instance.heartbeat_retry_attempts == 0
    assert instance.heartbeat_retry_after == 0.0
    assert instance.store.cursor == "cursor-7"
    assert ceilings == [(0.0, 1.0), (0.0, 2.0)]
    assert instance.control_lane_health == {
        "heartbeat": True,
        "control": None,
        "desired": None,
    }
    assert instance.store.health[-1] == (
        "control",
        "degraded",
        "control_transport_unavailable",
    )


@pytest.mark.parametrize(
    "failure",
    [
        httpx.HTTPStatusError(
            "unauthorized",
            request=httpx.Request("PUT", "https://control.invalid/v1/heartbeat"),
            response=httpx.Response(
                401,
                request=httpx.Request("PUT", "https://control.invalid/v1/heartbeat"),
            ),
        ),
        ValueError("Heartbeat response UUID mismatch"),
    ],
)
def test_heartbeat_does_not_mask_authentication_or_protocol_errors(failure):
    class Store:
        def mark_health(self, *args):
            raise AssertionError("non-transport errors must not be marked as outage")

    class Control:
        def heartbeat(self, blocked_batch=None):
            raise failure

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()
    instance.last_heartbeat = 0.0
    instance.heartbeat_interval_seconds = 10.0
    instance.heartbeat_retry_after = 0.0

    with pytest.raises(type(failure)):
        instance._run_heartbeat(10.0)


def test_heartbeat_protocol_type_error_is_not_retried_with_a_second_signature():
    class Store:
        def blocked_batch(self):
            return {"code": "unsupported_desired_batch"}

    class Control:
        def __init__(self):
            self.calls = []

        def heartbeat(self, blocked_batch):
            self.calls.append(blocked_batch)
            raise TypeError("invalid heartbeat payload")

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()

    with pytest.raises(TypeError, match="invalid heartbeat payload"):
        instance.heartbeat()

    assert instance.control.calls == [{"code": "unsupported_desired_batch"}]


def test_heartbeat_success_does_not_clear_degraded_feed_report_lane():
    request = httpx.Request("GET", "https://control.invalid/v1/desired-state/changes")

    class Random:
        def uniform(self, lower, upper):
            return 0.0

    class Store:
        def __init__(self):
            self.health = []

        def mark_health(self, component, status, code=None):
            self.health.append((component, status, code))

    class Control:
        def heartbeat(self, blocked_batch):
            return {}

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()
    instance.last_heartbeat = 0.0
    instance.last_control = 0.0
    instance.heartbeat_interval_seconds = 10.0
    instance.control_poll_interval_seconds = 2.0
    instance.control_retry_base_seconds = 1.0
    instance.control_retry_cap_seconds = 30.0
    instance.control_retry_after_cap_seconds = 300.0
    instance.heartbeat_retry_after = 0.0
    instance.control_retry_after = 0.0
    instance.heartbeat_retry_attempts = 0
    instance.control_retry_attempts = 0
    instance.control_random = Random()
    poll_attempts = [0]

    def synchronize_control():
        poll_attempts[0] += 1
        if poll_attempts[0] == 1:
            raise httpx.ConnectError("temporarily unavailable", request=request)

    instance.synchronize_control = synchronize_control

    assert not instance._run_control_poll(2.0)
    assert instance.control_lane_health == {
        "heartbeat": None,
        "control": False,
        "desired": None,
    }
    assert instance.store.health[-1] == (
        "control",
        "degraded",
        "control_transport_unavailable",
    )

    assert instance._run_heartbeat(10.0)
    assert instance.control_lane_health == {
        "heartbeat": True,
        "control": False,
        "desired": None,
    }
    assert instance.store.health[-1] == (
        "control",
        "degraded",
        "control_transport_unavailable",
    )

    assert instance._run_control_poll(10.0)
    assert instance.control_lane_health == {
        "heartbeat": True,
        "control": True,
        "desired": True,
    }
    assert instance.store.health[-1] == ("control", "healthy", None)


def test_heartbeat_success_preserves_incompatible_control_health_code():
    class Store:
        def __init__(self):
            self.health = []

        def mark_health(self, component, status, code=None):
            self.health.append((component, status, code))

    class Control:
        def heartbeat(self, blocked_batch):
            return {}

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()
    instance.last_heartbeat = 0.0
    instance.heartbeat_interval_seconds = 10.0
    instance.heartbeat_retry_after = 0.0
    instance.heartbeat_retry_attempts = 0
    instance.control_lane_health = {
        "heartbeat": False,
        "control": True,
        "desired": False,
    }
    instance.control_lane_errors = {
        "heartbeat": "control_transport_unavailable",
        "control": None,
        "desired": "unsupported_desired_batch",
    }

    assert instance._run_heartbeat(10.0)
    assert instance.store.health[-1] == (
        "control",
        "degraded",
        "unsupported_desired_batch",
    )


def test_report_success_cannot_clear_blocked_desired_feed_in_same_tick(
    tmp_path, monkeypatch
):
    now = [10.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])

    class Store:
        def __init__(self):
            self.cursor = "cursor-1"
            self.blocked = None
            self.compatible = False
            self.health = []

        def blocked_batch(self):
            return self.blocked

        def control_cursor(self):
            return self.cursor

        def apply_desired_changes(self, changes, next_cursor):
            if not self.compatible:
                raise ValueError("unsupported")
            self.cursor = next_cursor

        def set_blocked_batch(self, cursor, next_cursor, code):
            self.blocked = {"cursor": cursor, "next_cursor": next_cursor, "code": code}

        def clear_blocked_batch(self):
            self.blocked = None

        def mark_health(self, component, status, code=None):
            self.health.append((component, status, code))

    class Control:
        def heartbeat(self, blocked_batch):
            return {}

        def desired_changes(self, cursor):
            assert cursor == "cursor-1"
            return {"changes": [{"resource_type": "future"}], "next_cursor": "cursor-2"}

    class Scheduler:
        def reconcile_once(self):
            return False

        def run_once(self):
            return False

    reports = iter((1, 0))
    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()
    instance.last_heartbeat = 0.0
    instance.last_control = 0.0
    instance.last_certificate_check = 100.0
    instance.last_provider_poll = 100.0
    instance.heartbeat_interval_seconds = 10.0
    instance.control_poll_interval_seconds = 2.0
    instance.heartbeat_retry_after = 0.0
    instance.control_retry_after = 0.0
    instance.heartbeat_retry_attempts = 0
    instance.control_retry_attempts = 0
    instance.health_file = tmp_path / "progress"
    instance.scheduler = Scheduler()
    instance.certificate_renewer = None
    instance.poll_provider_operations = lambda: 0
    instance.poll_provider_events = lambda: 0
    instance.flush_observed_reports = lambda: next(reports)
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance.flush_provider_events = (
        lambda minimum_priority=0, maximum_priority=2, limit=100: 0
    )
    instance._flush_history_events = lambda: (0, 20, True)
    instance.run_backfill_once = lambda: False

    assert instance.tick()
    assert instance.store.cursor == "cursor-1"
    assert instance.store.blocked == {
        "cursor": "cursor-1",
        "next_cursor": "cursor-2",
        "code": "unsupported_desired_batch",
    }
    assert instance.control_lane_health == {
        "heartbeat": True,
        "control": True,
        "desired": False,
    }
    assert instance.store.health[-1] == (
        "control",
        "degraded",
        "unsupported_desired_batch",
    )

    instance.store.compatible = True
    now[0] = 12.0
    assert instance.tick()
    assert instance.store.cursor == "cursor-2"
    assert instance.store.blocked is None
    assert instance.control_lane_health == {
        "heartbeat": True,
        "control": True,
        "desired": True,
    }
    assert instance.store.health[-1] == ("control", "healthy", None)


def test_desired_cursor_waits_for_retryable_control_recovery():
    class Random:
        def uniform(self, lower, upper):
            return upper

    class Store:
        def __init__(self):
            self.cursor = "cursor-1"
            self.health = []

        def control_cursor(self):
            return self.cursor

        def apply_desired_changes(self, changes, next_cursor):
            self.cursor = next_cursor

        def clear_blocked_batch(self):
            pass

        def mark_health(self, component, status, code=None):
            self.health.append((component, status, code))

    class Control:
        def __init__(self):
            self.attempts = 0

        def desired_changes(self, cursor):
            self.attempts += 1
            if self.attempts == 1:
                raise control.ControlRetryableError(503, 900.0)
            assert cursor == "cursor-1"
            return {"changes": [], "next_cursor": "cursor-2"}

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()
    instance.last_control = 0.0
    instance.control_poll_interval_seconds = 2.0
    instance.control_retry_base_seconds = 1.0
    instance.control_retry_cap_seconds = 30.0
    instance.control_retry_after_cap_seconds = 300.0
    instance.control_retry_attempts = 0
    instance.control_retry_after = 0.0
    instance.control_random = Random()

    assert not instance._run_control_poll(2.0)
    assert instance.store.cursor == "cursor-1"
    assert instance.control_retry_after == 302.0
    assert not instance._run_control_poll(301.99)
    assert instance.control.attempts == 1

    assert instance._run_control_poll(302.0)
    assert instance.store.cursor == "cursor-2"
    assert instance.control_retry_attempts == 0
    assert instance.control_retry_after == 0.0
    assert not instance._run_control_poll(303.99)
    assert instance.control.attempts == 2


def test_control_full_jitter_ceiling_caps_at_thirty_seconds():
    ceilings = []

    class Random:
        def uniform(self, lower, upper):
            ceilings.append((lower, upper))
            return 0.0

    instance = object.__new__(service.BridgeService)
    instance.control_retry_base_seconds = 1.0
    instance.control_retry_cap_seconds = 30.0
    instance.control_retry_after_cap_seconds = 300.0
    instance.control_retry_attempts = 0
    instance.control_random = Random()

    for _ in range(7):
        instance._defer_control_call("control", 0.0, None)

    assert ceilings == [
        (0.0, 1.0),
        (0.0, 2.0),
        (0.0, 4.0),
        (0.0, 8.0),
        (0.0, 16.0),
        (0.0, 30.0),
        (0.0, 30.0),
    ]


def test_ca_migration_heartbeat_renews_and_reloads_mtls_clients():
    events = []

    class Store:
        def mark_health(self, component, status, code=None):
            events.append(("health", component, status, code))

    class Client:
        def heartbeat(self, blocked_batch):
            return {"ca_migration": {"renewal_required": True}}

        def reload_tls(self):
            events.append(("reload", "control"))

    class FileClient:
        def reload_tls(self):
            events.append(("reload", "file"))

    class ProviderClient:
        def reload_tls(self):
            events.append(("reload", "provider"))

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Client()
    instance.provider_api = ProviderClient()
    instance.file_client = FileClient()
    instance.certificate_renewer = lambda force: events.append(("renew", force)) or True

    instance.heartbeat()
    assert events == [
        ("renew", True),
        ("reload", "control"),
        ("reload", "provider"),
        ("reload", "file"),
        ("health", "certificate", "healthy", None),
    ]


def test_incompatible_desired_batch_blocks_without_advancing_and_recovers():
    class Store:
        def __init__(self):
            self.cursor = "cursor-1"
            self.blocked = None
            self.compatible = False
            self.stale_recoveries = 0

        def control_cursor(self):
            return self.cursor

        def apply_desired_changes(self, changes, next_cursor):
            if not self.compatible:
                raise ValueError("unsupported")
            self.cursor = next_cursor

        def set_blocked_batch(self, cursor, next_cursor, code):
            self.blocked = {"cursor": cursor, "next_cursor": next_cursor, "code": code}

        def clear_blocked_batch(self):
            self.blocked = None

        def reset_stale_workspace_deliveries(self):
            self.stale_recoveries += 1

        def mark_health(self, *args):
            pass

    class Client:
        def desired_changes(self, cursor):
            assert cursor == "cursor-1"
            return {
                "changes": [{"resource_type": "future"}],
                "next_cursor": "cursor-2",
            }

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Client()
    assert instance.synchronize_control() is False
    assert instance.store.cursor == "cursor-1"
    assert instance.store.stale_recoveries == 0
    assert instance.store.blocked == {
        "cursor": "cursor-1",
        "next_cursor": "cursor-2",
        "code": "unsupported_desired_batch",
    }
    instance.store.compatible = True
    assert instance.synchronize_control() is True
    assert instance.store.cursor == "cursor-2"
    assert instance.store.blocked is None
    assert instance.store.stale_recoveries == 1


def test_registration_notification_snapshots_are_persisted_in_provider_journal():
    recorded = []

    class Store:
        def record_provider_event(self, account_uuid, queue_id, event):
            recorded.append((account_uuid, queue_id, event))
            return True

        def provider_topic_mappings(self, _account_uuid):
            return []

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    count = instance._record_registration_notification_snapshots(
        "account-1",
        "queue-1",
        {
            "user_settings": {"enable_stream_desktop_notifications": True},
            "subscriptions": [
                {
                    "stream_id": 42,
                    "is_muted": False,
                    "desktop_notifications": True,
                }
            ],
            "user_topics": [
                {
                    "stream_id": 42,
                    "topic_name": "bridge",
                    "visibility_policy": 3,
                    "last_updated": 1_800_000_020,
                }
            ],
        },
    )

    assert count == 2
    assert [event[2]["id"] for event in recorded] == [-1, -2]
    assert recorded[0][2]["type"] == "subscription"
    assert recorded[0][2]["op"] == "notification_snapshot"
    assert recorded[0][2]["enable_stream_desktop_notifications"] is True
    assert isinstance(recorded[0][2]["observed_at"], float)
    assert recorded[1][2] == {
        "id": -2,
        "type": "user_topic",
        "stream_id": 42,
        "topic_name": "bridge",
        "visibility_policy": 3,
        "last_updated": 1_800_000_020,
    }


def test_registration_synthesizes_default_for_missing_mapped_user_topic():
    recorded = []

    class Store:
        def record_provider_event(self, account_uuid, queue_id, event):
            recorded.append((account_uuid, queue_id, event))
            return True

        def provider_topic_mappings(self, _account_uuid):
            return [
                {
                    "provider_id": "42:explicit",
                    "metadata": {"notification_mode": "follow"},
                },
                {
                    "provider_id": "42:reset",
                    "metadata": {"notification_mode": "mute"},
                },
                {
                    "provider_id": "99:unsubscribed",
                    "metadata": {"notification_mode": "follow"},
                },
                {
                    "provider_id": "direct:1,2:default",
                    "metadata": {"notification_mode": "mute"},
                },
            ]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()

    count = instance._record_registration_notification_snapshots(
        "account-1",
        "queue-1",
        {
            "user_settings": {"enable_stream_desktop_notifications": True},
            "subscriptions": [
                {
                    "stream_id": 42,
                    "is_muted": False,
                    "desktop_notifications": None,
                }
            ],
            "user_topics": [
                {
                    "stream_id": 42,
                    "topic_name": "explicit",
                    "visibility_policy": 3,
                    "last_updated": 1_800_000_020,
                }
            ],
        },
    )

    assert count == 3
    assert recorded[1][2]["topic_name"] == "explicit"
    assert recorded[2][2]["topic_name"] == "reset"
    assert recorded[2][2]["visibility_policy"] == 0
    assert isinstance(recorded[2][2]["observed_at"], float)


def test_live_global_notification_update_captures_current_subscriptions():
    recorded = []

    class Store:
        def provider_event_cursor(self, _account_uuid):
            return {"queue_id": "queue-1", "last_event_id": 7}

        def account_resource(self, _account_uuid):
            return None

        def record_provider_event(self, account_uuid, queue_id, event):
            recorded.append((account_uuid, queue_id, event))
            return True

        def update_provider_event_cursor(self, *_args):
            return None

    class Adapter:
        server_url = "https://zulip.example.invalid"

        def restore_queue(self, *_args):
            return None

        def events(self, *_args):
            return [
                {
                    "id": 8,
                    "type": "user_settings",
                    "op": "update",
                    "property": "enable_stream_desktop_notifications",
                    "value": False,
                }
            ]

        def notification_subscriptions(self):
            return [
                {
                    "stream_id": 42,
                    "is_muted": False,
                    "desktop_notifications": None,
                }
            ]

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.scheduler = type(
        "Scheduler", (), {"reconcile_local_echo": lambda *args: None}
    )()
    instance._initial_sync_ready = lambda _account_uuid: False
    instance._queue_account_report = lambda *_args: None

    processed, error = instance._poll_provider_account("account-1", Adapter())

    assert processed == 1
    assert error is None
    assert recorded[0][2]["subscriptions"] == [
        {
            "stream_id": 42,
            "is_muted": False,
            "desktop_notifications": None,
        }
    ]
    assert isinstance(recorded[0][2]["observed_at"], float)


def test_live_global_notification_snapshot_failure_keeps_event_unacknowledged():
    cursor_updates = []

    class Store:
        def provider_event_cursor(self, _account_uuid):
            return {"queue_id": "queue-1", "last_event_id": 7}

        def account_resource(self, _account_uuid):
            return None

        def record_provider_event(self, *_args):
            raise AssertionError("incomplete global event must not be persisted")

        def update_provider_event_cursor(self, *args):
            cursor_updates.append(args)

    class Adapter:
        def restore_queue(self, *_args):
            return None

        def events(self, *_args):
            return [
                {
                    "id": 8,
                    "type": "user_settings",
                    "op": "update",
                    "property": "enable_stream_desktop_notifications",
                    "value": False,
                }
            ]

        def notification_subscriptions(self):
            raise zulip_adapter.ZulipOperationError("provider_unavailable", True)

    instance = object.__new__(service.BridgeService)
    instance.store = Store()

    processed, error = instance._poll_provider_account("account-1", Adapter())

    assert processed == 0
    assert error is not None and error.code == "provider_unavailable"
    assert cursor_updates == []


def test_certificate_renewal_failure_is_degraded_without_stopping_message_work():
    health = []

    class Store:
        def mark_health(self, component, status, code=None):
            health.append((component, status, code))

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.certificate_renewer = lambda force: (_ for _ in ()).throw(
        RuntimeError("temporarily unavailable")
    )

    assert not instance._renew_certificate(False)
    assert health == [("certificate", "degraded", "certificate_renewal_failed")]
