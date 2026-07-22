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

    def control_cursor(self):
        return self.cursor

    def install_snapshot(self, resources, anchor):
        self.installed.append((resources, anchor))


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
                "credential_envelope": {"ciphertext": "opaque"},
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

    monkeypatch.setattr(zulip_adapter, "OfficialZulipAdapter", Adapter)
    decryptor = Decryptor()
    registry = service.AdapterRegistry(Store(), decryptor)
    first = registry(account_uuid)
    second = registry(account_uuid)
    assert first is not second
    assert decryptor.calls == 2
    assert not hasattr(registry, "cache")


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
                "credential_envelope": {},
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


def test_retryable_provider_error_defers_only_failing_account(monkeypatch):
    accounts = ["account-a", "account-b"]

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

        def restore_queue(self, queue_id, last_event_id):
            return None

        def events(self, queue_id, last_event_id):
            if self.account_uuid == "account-a":
                raise zulip_adapter.ZulipOperationError("provider_unavailable", True)
            return [{"id": 5, "type": "realm_user"}]

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

    assert instance.poll_provider_events() == 1
    assert instance.store.recorded == [("account-b", "queue-account-b", 5)]
    assert instance.provider_retry_attempts == {"account-a": 1}
    assert instance.provider_retry_after["account-a"] > 100.0

    assert instance.poll_provider_events() == 1
    assert instance.provider_retry_attempts == {"account-a": 1}
    assert instance.store.recorded == [
        ("account-b", "queue-account-b", 5),
        ("account-b", "queue-account-b", 5),
    ]


def test_adapter_connection_failure_degrades_only_affected_account(monkeypatch):
    failed_account = "00000000-0000-4000-8000-000000000001"
    healthy_account = "00000000-0000-4000-8000-000000000002"

    class Store:
        def __init__(self):
            self.recorded = []
            self.reports = []
            self.health = []

        def active_account_uuids(self):
            return [failed_account, healthy_account]

        def provider_event_cursor(self, account_uuid):
            return {"queue_id": f"queue-{account_uuid}", "last_event_id": 4}

        def account_resource(self, account_uuid):
            if account_uuid == failed_account:
                return {"generation": 7}
            return None

        def record_provider_event(self, account_uuid, queue_id, event):
            self.recorded.append((account_uuid, queue_id, event["id"]))

        def update_provider_event_cursor(self, account_uuid, queue_id, event_id):
            return None

        def enqueue_observed_report(self, report):
            self.reports.append(report)

        def mark_health(self, component, status, code=None):
            self.health.append((component, status, code))

    class UnreachableClient:
        def __init__(self, **kwargs):
            raise zulip_adapter.zulip.UnrecoverableNetworkError("offline")

    class HealthyAdapter:
        def restore_queue(self, queue_id, last_event_id):
            return None

        def events(self, queue_id, last_event_id):
            return [{"id": 5, "type": "realm_user"}]

    def adapter_factory(account_uuid):
        if account_uuid == failed_account:
            return zulip_adapter.OfficialZulipAdapter(
                credentials=zulip_adapter.ZulipCredentials(
                    "https://unresolvable.example.invalid",
                    "user@example.invalid",
                    "opaque-api-key",
                )
            )
        return HealthyAdapter()

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = adapter_factory
    instance.scheduler = type(
        "Scheduler", (), {"reconcile_local_echo": lambda *args: None}
    )()
    instance.provider_retry_attempts = {}
    instance.provider_retry_after = {}
    instance.provider_random = type(
        "Random", (), {"uniform": lambda self, lower, upper: upper}
    )()
    monkeypatch.setattr(zulip_adapter.zulip, "Client", UnreachableClient)
    monkeypatch.setattr(time, "monotonic", lambda: 100.0)

    assert instance.poll_provider_events() == 1
    assert instance.store.recorded == [
        (healthy_account, f"queue-{healthy_account}", 5)
    ]
    assert instance.provider_retry_attempts == {failed_account: 1}
    assert instance.provider_retry_after[failed_account] > 100.0
    assert len(instance.store.reports) == 1
    assert instance.store.reports[0]["resource_uuid"] == failed_account
    assert instance.store.reports[0]["status"] == "degraded"
    assert instance.store.reports[0]["safe_error"]["code"] == (
        "provider_unavailable"
    )


def test_150_account_poll_is_bounded_by_worker_pool_not_serial_latency():
    accounts = [str(uuid.UUID(int=index + 1)) for index in range(150)]

    class Store:
        def active_account_uuids(self):
            return accounts

        def provider_event_cursor(self, account_uuid):
            return {"queue_id": f"queue-{account_uuid}", "last_event_id": 0}

        def account_resource(self, account_uuid):
            return None

        def mark_health(self, *args):
            return None

    active = 0
    maximum_active = 0
    adapters = []
    lock = threading.Lock()

    class Adapter:
        def __init__(self):
            adapters.append(self)

        def restore_queue(self, queue_id, last_event_id):
            return None

        def events(self, queue_id, last_event_id):
            nonlocal active, maximum_active
            with lock:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.04)
            with lock:
                active -= 1
            return []

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda account_uuid: Adapter()
    instance.provider_poll_workers = 16
    instance.provider_retry_attempts = {}
    instance.provider_retry_after = {}
    instance.provider_random = type(
        "Random", (), {"uniform": lambda self, lower, upper: lower}
    )()

    started = time.monotonic()
    assert instance.poll_provider_events() == 0
    elapsed = time.monotonic() - started

    assert maximum_active == 16
    assert len(adapters) == len(accounts)
    assert len({id(adapter) for adapter in adapters}) == len(accounts)
    assert elapsed < 5.0


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

    def pending_provider_events(self):
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


def test_provider_journal_waits_for_assignment_bound_delivery(monkeypatch):
    class Store(DeliveryStore):
        def __init__(self, events):
            super().__init__(events)
            self.delivering = []

        def reset_stale_workspace_deliveries(self):
            return 0

        def mark_provider_event_delivering(self, account_uuid, queue_id, event_id):
            self.delivering.append((account_uuid, queue_id, event_id))

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
    assert store.enqueued == [({"record_uuid": "record"}, 0, "queue", 7)]
    assert store.delivering == [(account_uuid, "queue", 7)]
    assert store.processed == []


def test_live_event_preempts_large_backfill_and_is_exactly_once_across_restart(
    monkeypatch,
):
    account_uuid = "00000000-0000-4000-8000-000000000001"

    class State:
        cursor = {"queue_id": "queue", "last_event_id": 7}
        processing_state = "pending"
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

        def pending_provider_events(self):
            if State.processing_state != "pending":
                return []
            return [
                {
                    "account_uuid": account_uuid,
                    "queue_id": "queue",
                    "event_id": 7,
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
            raise AssertionError("valid buffered event must not be retried")

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
    with pytest.raises(RuntimeError, match="simulated process crash"):
        first_process.process_provider_journal()
    assert State.processing_state == "pending"
    assert State.cursor == {"queue_id": "queue", "last_event_id": 7}
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
            "subscriptions": [{"stream_id": 42, "name": "Engineering"}],
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
    assert channel["catalog"]["capabilities"]["messenger.stream.rename"]["available"]
    assert "messenger.stream.rename" not in direct["catalog"]["capabilities"]
    assert set(channel["catalog"]["source"]) == {
        "kind",
        "chat_type",
        "provider_chat_key",
        "original_url",
    }
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
            "name": "default",
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


def test_catalog_reports_accumulate_full_replacement_topology():
    class Store:
        def __init__(self):
            self.participants = {}
            self.topics = {}
            self.reports = []

        def merge_catalog_topology(self, _account, _chat, participants, topics):
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
            return None

        def update_provider_event_cursor(self, requested, queue_id, event_id):
            self.cursor = {"queue_id": queue_id, "last_event_id": event_id}

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

        def mark_health(self, *args):
            return None

    class Adapter:
        server_url = "https://zulip.example.invalid"

        def ensure_queue(self):
            return "queue", 10

        def restore_queue(self, queue_id, event_id):
            return None

        def take_registration_snapshot(self):
            return {
                "user_id": 1,
                "subscriptions": [{"stream_id": 42, "name": "Engineering"}],
                "realm_users": [],
                "recent_private_conversations": [],
            }

        def events(self, queue_id, event_id):
            return []

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_adapters = lambda requested: Adapter()
    instance.scheduler = type(
        "Scheduler", (), {"reconcile_local_echo": lambda *args: None}
    )()
    instance.provider_retry_attempts = {}
    instance.provider_retry_after = {}
    instance.provider_random = type(
        "Random", (), {"uniform": lambda self, lower, upper: lower}
    )()

    assert instance.poll_provider_events() == 0
    assert instance.store.cursor == {"queue_id": "queue", "last_event_id": 10}
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
    assert instance.poll_provider_events() == 0
    account_report = [
        report
        for report in instance.store.reports
        if report["resource_type"] == "external_account"
    ][-1]
    assert account_report["status"] == "live_ready"


def test_live_ready_requires_catalog_assignment_and_initial_backfill_gates():
    account_uuid = "00000000-0000-4000-8000-000000000001"

    class Store:
        def __init__(self):
            self.ready = {
                "catchup": True,
                "catalog": False,
                "assignment": False,
                "backfill": False,
            }

        def account_resource(self, requested):
            return {"generation": 3}

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
    assert not instance._initial_sync_ready(account_uuid)
    instance.store.ready["catalog"] = True
    assert not instance._initial_sync_ready(account_uuid)
    instance.store.ready["assignment"] = True
    assert not instance._initial_sync_ready(account_uuid)
    instance.store.ready["backfill"] = True
    assert instance._initial_sync_ready(account_uuid)


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
    instance.run_backfill_once = lambda: False

    assert not instance.tick()
    assert instance.store.reconciliations == 1


def test_observed_report_flush_preserves_order_and_applies_partial_results():
    reports = [
        {"report_uuid": str(uuid.uuid4())},
        {"report_uuid": str(uuid.uuid4())},
    ]

    class Store:
        def __init__(self):
            self.applied = []

        def pending_observed_reports(self, limit):
            assert limit == 500
            return reports

        def apply_observed_report_results(self, results):
            self.applied.extend(results)

    class Control:
        def observed_reports(self, supplied):
            assert supplied == reports
            return {
                "results": [
                    {
                        "report_uuid": reports[0]["report_uuid"],
                        "status": "applied",
                        "safe_error": None,
                    },
                    {
                        "report_uuid": reports[1]["report_uuid"],
                        "status": "rejected",
                        "safe_error": {
                            "code": "temporarily_unavailable",
                            "message": "Try again later.",
                            "retryable": True,
                        },
                    },
                ]
            }

    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.control = Control()
    assert instance.flush_observed_reports() == 2
    assert [result["status"] for result in instance.store.applied] == [
        "applied",
        "rejected",
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


def test_permanent_attachment_failure_uses_loss_aware_fallback(monkeypatch):
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
        if args[6] is not None:
            raise zulip_adapter.ZulipOperationError("provider_file_too_large", False)
        return [{"record_uuid": "fallback-record"}]

    monkeypatch.setattr(converter, "event_records", records)
    assert instance.process_provider_journal() == 1
    assert store.retried == []
    assert store.invalid == []
    assert store.enqueued == [({"record_uuid": "fallback-record"}, 0)]
    assert store.processed == [(account_uuid, "queue", 7, True)]


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
    assert calls.index("live-provider-call") < calls.index("delivery:0:0:10")
    assert "delivery:2:2:1" not in calls


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
    instance.poll_provider_operations = lambda: 0
    instance.poll_provider_events = lambda: 0
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    instance.flush_provider_events = (
        lambda minimum_priority=0, maximum_priority=2, limit=100: (
            calls.append(f"delivery:{minimum_priority}:{maximum_priority}:{limit}") or 0
        )
    )
    instance.run_backfill_once = lambda: calls.append("backfill") or True

    assert instance.tick()
    assert calls == [
        "live",
        "delivery:0:0:10",
        "backfill",
        "delivery:2:2:1",
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
    assert instance.store.blocked == {
        "cursor": "cursor-1",
        "next_cursor": "cursor-2",
        "code": "unsupported_desired_batch",
    }
    instance.store.compatible = True
    assert instance.synchronize_control() is True
    assert instance.store.cursor == "cursor-2"
    assert instance.store.blocked is None


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
