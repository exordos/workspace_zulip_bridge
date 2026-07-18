import uuid
from pathlib import Path

import httpx
import pytest

from workspace_zulip_bridge import service

ACCOUNT_UUID = "10000000-0000-0000-0000-000000000001"
PROJECT_UUID = "20000000-0000-0000-0000-000000000002"
STREAM_UUID = "30000000-0000-0000-0000-000000000003"
TOPIC_UUID = "40000000-0000-0000-0000-000000000004"
MESSAGE_UUID = "50000000-0000-0000-0000-000000000005"
CHAT_UUID = "60000000-0000-0000-0000-000000000006"


class Store:
    def __init__(self):
        self.enqueued = []
        self.sent = []
        self.finalized = []
        self.accepted = []
        self.released = []

    def workspace_mapping(self, account_uuid, kind, workspace_uuid):
        return {
            "stream": {"provider_id": "channel:42", "metadata": {}},
            "message": {
                "provider_id": "101",
                "metadata": {"chat_key": "channel:42"},
            },
        }[kind]

    def assignment_for_provider_chat(self, account_uuid, chat_key):
        return {"uuid": CHAT_UUID}

    def producer_lane_position(self, operation_uuid, origin, causal_lane):
        return 0, None

    def bind_provider_lease(self, record):
        return False

    def enqueue(self, record, priority):
        self.enqueued.append((record, priority))
        return True

    def mark_health(self, *args):
        pass

    def pending_results(self, limit):
        return self.results

    def mark_result_sent(self, record_uuid):
        self.sent.append(record_uuid)

    def finalize_provider_result_response(self, record_uuid, status):
        self.finalized.append((record_uuid, status))
        if status in {"applied", "duplicate"}:
            self.sent.append(record_uuid)

    def pending_workspace_deliveries(self, **kwargs):
        return self.deliveries

    def account_is_active(self, account_uuid):
        return True

    def mark_workspace_delivery_submitting(self, record_uuid):
        return True

    def accept_result(self, result):
        self.accepted.append(result)

    def finalize_ready_provider_events(self):
        return 0

    def release_provider_event_submissions(self, record_uuids):
        self.released.extend(record_uuids)


class Provider:
    def __init__(self):
        self.leased = []
        self.reported = []
        self.events = []

    def lease_operations(self, request_uuid, **kwargs):
        return {"request_uuid": str(request_uuid), "operations": self.leased}

    def report_results(self, results):
        self.reported.extend(results)
        return {
            "results": [
                {"result_uuid": result["result_uuid"], "status": "applied"}
                for result in results
            ]
        }

    def apply_events(self, events):
        self.events.extend(events)
        return {
            "results": [
                {
                    "provider_event_uuid": event["provider_event_uuid"],
                    "status": "applied",
                    "target_uuid": event["payload"]["resource"]["uuid"],
                    "safe_error": None,
                    "duplicate": False,
                }
                for event in events
            ]
        }


def _instance():
    instance = object.__new__(service.BridgeService)
    instance.store = Store()
    instance.provider_api = Provider()
    instance.provider_batch_size = 20
    instance.provider_lease_seconds = 300
    instance.provider_lease_request_uuid = None
    return instance


def test_bridge_service_fails_closed_without_provider_api():
    with pytest.raises(ValueError, match="Provider API client is required"):
        service.BridgeService(
            store=object(),
            control_client=object(),
            operation_scheduler=object(),
            provider_adapters=object(),
            provider_client=None,
            health_file=Path("/tmp/unused"),
        )


def _lease():
    return {
        "provider_operation_uuid": str(uuid.uuid4()),
        "external_operation_uuid": str(uuid.uuid4()),
        "lease_uuid": str(uuid.uuid4()),
        "lease_expires_at": "2026-07-18T15:00:00Z",
        "external_account_uuid": ACCOUNT_UUID,
        "project_id": PROJECT_UUID,
        "operation_kind": "message.create",
        "required_capability": "messenger.message.send",
        "attempt": 1,
        "payload": {
            "uuid": MESSAGE_UUID,
            "stream_uuid": STREAM_UUID,
            "topic_uuid": TOPIC_UUID,
            "user_uuid": ACCOUNT_UUID,
            "payload": {"kind": "markdown", "content": "hello"},
        },
    }


def test_poll_provider_operations_durably_enqueues_exact_lease_binding():
    instance = _instance()
    leased = _lease()
    instance.provider_api.leased = [leased]

    assert instance.poll_provider_operations() == 1
    record, priority = instance.store.enqueued[0]
    assert priority == 0
    assert (
        record["transport"]["provider_operation_uuid"]
        == (leased["provider_operation_uuid"])
    )
    assert instance.provider_lease_request_uuid is None


def test_poll_provider_operations_durably_enqueues_exact_read_state_selector():
    instance = _instance()
    first_message_uuid = str(uuid.uuid4())
    last_message_uuid = str(uuid.uuid4())
    leased = _lease()
    leased.update(
        {
            "operation_kind": "read_state.set",
            "required_capability": "messenger.message.read",
            "payload": {
                "stream_uuid": STREAM_UUID,
                "topic_uuid": TOPIC_UUID,
                "reader_uuid": ACCOUNT_UUID,
                "message_uuids": [first_message_uuid, last_message_uuid],
                "read": True,
            },
        }
    )
    instance.provider_api.leased = [leased]

    assert instance.poll_provider_operations() == 1

    record, priority = instance.store.enqueued[0]
    assert priority == 0
    assert record["operation"]["kind"] == "read_state.set"
    assert record["operation"]["payload"]["message_uuids"] == [
        first_message_uuid,
        last_message_uuid,
    ]
    assert record["operation"]["provider"]["entity_id"] is None
    assert record["transport"]["required_capability"] == "messenger.message.read"


def test_flush_provider_results_reports_and_persists_backend_acceptance():
    instance = _instance()
    leased = _lease()
    instance.store.results = [
        {
            "record_uuid": str(uuid.uuid4()),
            "transport": {
                "provider_operation_uuid": leased["provider_operation_uuid"],
                "lease_uuid": leased["lease_uuid"],
            },
            "result": {"outcome": "committed", "safe_error": None},
        }
    ]

    assert instance.flush_provider_results() == 1
    assert instance.provider_api.reported[0]["status"] == "succeeded"
    assert instance.store.sent == [instance.store.results[0]["record_uuid"]]
    assert instance.store.finalized == [
        (instance.store.results[0]["record_uuid"], "applied")
    ]


@pytest.mark.parametrize("status", ["conflict", "rejected", "not_found", "stale_lease"])
def test_flush_provider_results_terminal_response_does_not_retry_forever(status):
    instance = _instance()
    leased = _lease()
    record_uuid = str(uuid.uuid4())
    instance.store.results = [
        {
            "record_uuid": record_uuid,
            "transport": {
                "provider_operation_uuid": leased["provider_operation_uuid"],
                "lease_uuid": leased["lease_uuid"],
            },
            "result": {"outcome": "committed", "safe_error": None},
        }
    ]

    def respond(results):
        return {
            "results": [{"result_uuid": results[0]["result_uuid"], "status": status}]
        }

    instance.provider_api.report_results = respond

    assert instance.flush_provider_results() == 0
    assert instance.store.finalized == [(record_uuid, status)]


def _inbound_record():
    return {
        "schema": "workspace.provider",
        "schema_version": 1,
        "record_kind": "operation",
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": str(uuid.uuid4()),
        "operation_sha256": "0" * 64,
        "attempt": 1,
        "account_uuid": ACCOUNT_UUID,
        "project_uuid": PROJECT_UUID,
        "origin": "zulip",
        "causal_lane": "chat:channel:42",
        "sequence": 1,
        "predecessor_operation_uuid": None,
        "created_at": "2026-07-18T12:00:00Z",
        "expires_at": None,
        "operation": {
            "kind": "message.create",
            "entity_uuid": MESSAGE_UUID,
            "provider": {
                "kind": "zulip",
                "chat_id": "channel:42",
                "entity_id": "101",
                "revision": None,
            },
            "payload": {
                "author_uuid": ACCOUNT_UUID,
                "stream_uuid": STREAM_UUID,
                "topic_uuid": TOPIC_UUID,
                "payload": {"kind": "markdown", "content": "hello"},
            },
            "extensions": {},
        },
    }


def test_flush_provider_events_applies_atomic_http_batch_then_commits_outbox():
    instance = _instance()
    instance.store.deliveries = [_inbound_record()]

    assert instance.flush_provider_events() == 1
    assert instance.provider_api.events[0]["kind"] == "message.upsert"
    assert instance.store.accepted[0]["result"]["outcome"] == "committed"


def test_retryable_provider_event_failure_releases_idempotent_http_submission():
    instance = _instance()
    record = _inbound_record()
    instance.store.deliveries = [record]

    def fail(_events):
        request = httpx.Request("POST", "https://provider.invalid/events")
        raise httpx.ConnectError("offline", request=request)

    instance.provider_api.apply_events = fail

    with pytest.raises(httpx.ConnectError):
        instance.flush_provider_events()
    assert instance.store.released == [record["record_uuid"]]


def test_unsupported_provider_mutation_is_released_not_acknowledged_as_committed():
    instance = _instance()
    record = _inbound_record()
    record["operation"]["kind"] = "message.forward"
    instance.store.deliveries = [record]

    with pytest.raises(ValueError, match="Unsupported Provider event operation kind"):
        instance.flush_provider_events()

    assert instance.store.released == [record["record_uuid"]]
    assert instance.store.accepted == []
    assert instance.provider_api.events == []


@pytest.mark.parametrize("failure", ["wrong_order", "non_applied"])
def test_invalid_provider_event_response_releases_submissions(failure):
    instance = _instance()
    record = _inbound_record()
    instance.store.deliveries = [record]

    def invalid(events):
        result = {
            "provider_event_uuid": (
                str(uuid.uuid4())
                if failure == "wrong_order"
                else events[0]["provider_event_uuid"]
            ),
            "status": "rejected" if failure == "non_applied" else "applied",
        }
        return {"results": [result]}

    instance.provider_api.apply_events = invalid

    with pytest.raises(ValueError):
        instance.flush_provider_events()
    assert instance.store.released == [record["record_uuid"]]
