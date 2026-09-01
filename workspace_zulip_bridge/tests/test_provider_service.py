import copy
import uuid
from pathlib import Path

import httpx
import pytest

from workspace_zulip_bridge import provider_api, service

ACCOUNT_UUID = "10000000-0000-0000-0000-000000000001"
PROJECT_UUID = "20000000-0000-0000-0000-000000000002"
STREAM_UUID = "30000000-0000-0000-0000-000000000003"
TOPIC_UUID = "40000000-0000-0000-0000-000000000004"
MESSAGE_UUID = "50000000-0000-0000-0000-000000000005"
CHAT_UUID = "60000000-0000-0000-0000-000000000006"
BINDING_UUID = "70000000-0000-0000-0000-000000000007"


class Store:
    def __init__(self):
        self.enqueued = []
        self.sent = []
        self.finalized = []
        self.accepted = []
        self.accepted_targets = []
        self.released = []
        self.rejected = []
        self.health = []

    def workspace_mapping(self, account_uuid, kind, workspace_uuid):
        return {
            "identity": {
                "provider_id": "9",
                "metadata": {
                    "display_name": "Account Owner",
                    "email": None,
                    "avatar_urn": None,
                    "active": True,
                },
            },
            "stream": {"provider_id": "channel:42", "metadata": {}},
            "topic": {"provider_id": "42:dev", "metadata": {}},
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

    def mark_health(self, component, status, code=None):
        self.health.append((component, status, code))

    def pending_results(self, limit):
        return self.results

    def mark_result_sent(self, record_uuid):
        self.sent.append(record_uuid)

    def finalize_provider_result_response(self, record_uuid, status, lease_uuid=None):
        self.finalized.append((record_uuid, status, lease_uuid))
        if status in {"applied", "duplicate"}:
            self.sent.append(record_uuid)

    def finalize_provider_result_responses(self, responses):
        self.finalized.extend(responses)
        self.sent.extend(
            record_uuid
            for record_uuid, status, _lease_uuid in responses
            if status in {"applied", "duplicate"}
        )

    def pending_workspace_deliveries(self, **kwargs):
        return self.deliveries

    def account_is_active(self, account_uuid):
        return True

    def mark_workspace_delivery_submitting(self, record_uuid):
        return True

    def accept_result(self, result, workspace_target_uuid=None):
        self.accepted.append(result)
        self.accepted_targets.append(workspace_target_uuid)

    def finalize_ready_provider_events(self):
        return 0

    def release_provider_event_submissions(self, record_uuids):
        self.released.extend(record_uuids)

    def reject_provider_event_submission(self, record_uuid, error_code):
        self.rejected.append((record_uuid, error_code))
        return True


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

    def apply_commands(self, commands):
        self.events.extend(commands)
        return {
            "results": [
                {
                    "provider_event_key": command["provider_event_key"],
                    "status": "applied",
                    "target_uuid": None,
                    "safe_error": None,
                    "duplicate": False,
                }
                for command in commands
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
    assert instance.store.health == [("provider_api", "healthy", None)]


def test_poll_provider_operations_keeps_create_then_edit_in_durable_lane():
    instance = _instance()
    original_workspace_mapping = instance.store.workspace_mapping

    def mapping_without_message(account_uuid, kind, workspace_uuid):
        if kind == "message":
            return None
        return original_workspace_mapping(account_uuid, kind, workspace_uuid)

    instance.store.workspace_mapping = mapping_without_message
    create = _lease()
    update = _lease()
    update["operation_kind"] = "message.update"
    update["required_capability"] = "messenger.message.edit"
    instance.provider_api.leased = [create, update]

    assert instance.poll_provider_operations() == 2

    records = [record for record, priority in instance.store.enqueued if priority == 0]
    assert [record["operation"]["kind"] for record in records] == [
        "message.create",
        "message.update",
    ]
    assert records[0]["causal_lane"] == records[1]["causal_lane"]
    assert records[1]["operation"]["provider"]["entity_id"] is None
    assert instance.provider_api.reported == []


def test_empty_provider_operation_poll_recovers_api_health():
    instance = _instance()

    assert instance.poll_provider_operations() == 0
    assert instance.store.health == [("provider_api", "healthy", None)]


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


def test_poll_provider_operations_keeps_lazy_read_pages_independently_idempotent():
    instance = _instance()
    pages = []
    for message_uuid in (str(uuid.uuid4()), str(uuid.uuid4())):
        leased = _lease()
        leased.update(
            {
                "operation_kind": "read_state.set",
                "required_capability": "messenger.message.read",
                "payload": {
                    "stream_uuid": STREAM_UUID,
                    "topic_uuid": TOPIC_UUID,
                    "reader_uuid": ACCOUNT_UUID,
                    "message_uuids": [message_uuid],
                    "read": True,
                },
            }
        )
        leased["external_operation_uuid"] = leased["provider_operation_uuid"]
        pages.append(leased)
    instance.provider_api.leased = pages

    assert instance.poll_provider_operations() == 2

    records = [record for record, priority in instance.store.enqueued if priority == 0]
    assert [record["operation_uuid"] for record in records] == [
        page["provider_operation_uuid"] for page in pages
    ]
    assert records[0]["operation_sha256"] != records[1]["operation_sha256"]


def test_poll_provider_operations_durably_enqueues_membership_write():
    instance = _instance()
    leased = _lease()
    leased.update(
        {
            "operation_kind": "membership.remove",
            "required_capability": "messenger.membership.write",
            "payload": {
                "uuid": BINDING_UUID,
                "stream_uuid": STREAM_UUID,
                "user_uuid": ACCOUNT_UUID,
                "who_uuid": PROJECT_UUID,
                "role": "member",
            },
        }
    )
    instance.provider_api.leased = [leased]

    assert instance.poll_provider_operations() == 1

    record, priority = instance.store.enqueued[0]
    assert priority == 0
    assert record["operation"]["kind"] == "membership.remove"
    assert record["operation"]["entity_uuid"] == BINDING_UUID
    assert record["operation"]["provider"]["chat_id"] == "channel:42"
    assert record["operation"]["provider"]["entity_id"] == "9"
    assert record["transport"]["required_capability"] == "messenger.membership.write"


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
        (
            instance.store.results[0]["record_uuid"],
            "applied",
            leased["lease_uuid"],
        )
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
    assert instance.store.finalized == [(record_uuid, status, leased["lease_uuid"])]


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


def test_provider_event_response_converges_to_authoritative_workspace_target():
    instance = _instance()
    instance.store.deliveries = [_inbound_record()]
    authoritative_uuid = str(uuid.uuid4())

    def apply(commands):
        return {
            "results": [
                {
                    "provider_event_key": commands[0]["provider_event_key"],
                    "status": "applied",
                    "target_uuid": authoritative_uuid,
                }
            ]
        }

    instance.provider_api.apply_commands = apply

    assert instance.flush_provider_events() == 1
    assert instance.store.accepted_targets == [authoritative_uuid]


def test_retryable_provider_event_failure_releases_idempotent_http_submission():
    instance = _instance()
    record = _inbound_record()
    instance.store.deliveries = [record]

    def fail(_events):
        request = httpx.Request("POST", "https://provider.invalid/events")
        raise httpx.ConnectError("offline", request=request)

    instance.provider_api.apply_commands = fail

    with pytest.raises(httpx.ConnectError):
        instance.flush_provider_events()
    assert instance.store.released == [record["record_uuid"]]


def test_retryable_provider_event_failure_uses_shared_bounded_backoff(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(service.time, "monotonic", lambda: now[0])

    class Random:
        def uniform(self, lower, upper):
            return upper

    instance = _instance()
    record = _inbound_record()
    instance.store.deliveries = [record]
    instance.provider_delivery_random = Random()
    calls = []

    def fail(events):
        calls.append(events)
        raise provider_api.ProviderApiRetryableError(409)

    instance.provider_api.apply_commands = fail

    with pytest.raises(provider_api.ProviderApiRetryableError):
        instance.flush_provider_events()
    assert len(calls) == 1
    assert instance.provider_delivery_retry_attempts == 1
    assert instance.provider_delivery_retry_after == 101.0

    assert instance.flush_provider_events() == 0
    assert len(calls) == 1

    now[0] = 101.0
    with pytest.raises(provider_api.ProviderApiRetryableError):
        instance.flush_provider_events()
    assert len(calls) == 2
    assert instance.provider_delivery_retry_attempts == 2
    assert instance.provider_delivery_retry_after == 103.0

    instance.provider_api.apply_commands = Provider().apply_commands
    now[0] = 103.0
    assert instance.flush_provider_events() == 1
    assert instance.provider_delivery_retry_attempts == 0
    assert instance.provider_delivery_retry_after == 0.0


def test_provider_event_backoff_honors_retry_after_header(monkeypatch):
    monkeypatch.setattr(service.time, "monotonic", lambda: 200.0)

    class Random:
        def uniform(self, lower, upper):
            return lower

    instance = _instance()
    instance.store.deliveries = [_inbound_record()]
    instance.provider_delivery_random = Random()
    instance.provider_api.apply_commands = lambda _commands: (_ for _ in ()).throw(
        provider_api.ProviderApiRetryableError(503, 17.0)
    )

    with pytest.raises(provider_api.ProviderApiRetryableError):
        instance.flush_provider_events()

    assert instance.provider_delivery_retry_after == 217.0


def test_permanent_rejection_isolates_one_record_and_commits_valid_sibling():
    instance = _instance()
    rejected = _inbound_record()
    accepted = copy.deepcopy(rejected)
    accepted["record_uuid"] = str(uuid.uuid4())
    accepted["operation_uuid"] = str(uuid.uuid4())
    accepted["operation"]["entity_uuid"] = str(uuid.uuid4())
    accepted["sequence"] = 2
    instance.store.deliveries = [rejected, accepted]
    calls = []

    def apply(commands):
        calls.append([command["provider_event_key"] for command in commands])
        if any(
            command["delivery_uuid"] == rejected["operation_uuid"]
            for command in commands
        ):
            raise provider_api.ProviderEventRejectedError(422)
        return {
            "results": [
                {
                    "provider_event_key": command["provider_event_key"],
                    "status": "applied",
                }
                for command in commands
            ]
        }

    instance.provider_api.apply_commands = apply

    assert instance.flush_provider_events() == 1
    assert [len(call) for call in calls] == [2, 1, 1]
    assert instance.store.rejected == [
        (rejected["record_uuid"], "provider_api_http_422")
    ]
    assert [
        result["in_reply_to_record_uuid"] for result in instance.store.accepted
    ] == [accepted["record_uuid"]]
    assert instance.store.released == []
    assert instance.store.health == [
        ("provider_api", "degraded", "provider_event_rejected")
    ]


def test_history_rejection_isolation_submits_ready_live_work_between_requests():
    instance = _instance()
    rejected = _inbound_record()
    accepted = copy.deepcopy(rejected)
    accepted["record_uuid"] = str(uuid.uuid4())
    accepted["operation_uuid"] = str(uuid.uuid4())
    accepted["operation"]["entity_uuid"] = str(uuid.uuid4())
    accepted["sequence"] = 2
    live = copy.deepcopy(accepted)
    live["record_uuid"] = str(uuid.uuid4())
    live["operation_uuid"] = str(uuid.uuid4())
    live["operation"]["entity_uuid"] = str(uuid.uuid4())
    calls = []
    live_ready = [False]
    instance.provider_batch_size = 100
    instance._live_workspace_delivery_pending = lambda: False
    instance._ready_live_workspace_delivery_pending = lambda: live_ready[0]

    def pending_workspace_deliveries(**kwargs):
        if kwargs["minimum_priority"] == 2:
            return [rejected, accepted]
        if live_ready[0]:
            live_ready[0] = False
            return [live]
        return []

    instance.store.pending_workspace_deliveries = pending_workspace_deliveries

    def apply(commands):
        delivery_uuids = [command["delivery_uuid"] for command in commands]
        delivery_class = (
            "live" if delivery_uuids == [live["operation_uuid"]] else "history"
        )
        calls.append((delivery_class, len(commands)))
        if any(
            command["delivery_uuid"] == rejected["operation_uuid"]
            for command in commands
        ):
            if len(commands) > 1:
                live_ready[0] = True
            raise provider_api.ProviderEventRejectedError(422)
        return {
            "results": [
                {
                    "provider_event_key": command["provider_event_key"],
                    "status": "applied",
                }
                for command in commands
            ]
        }

    instance.provider_api.apply_commands = apply

    assert instance._flush_history_events() == (
        1,
        service.BridgeService.HISTORY_DELIVERY_BATCH_SIZE,
        True,
    )
    assert calls == [
        ("history", 2),
        ("live", 1),
        ("history", 1),
        ("history", 1),
    ]


def test_interleaved_live_failure_defers_once_and_releases_both_batches(
    monkeypatch,
):
    now = [100.0]
    monkeypatch.setattr(service.time, "monotonic", lambda: now[0])

    class Random:
        def uniform(self, _lower, upper):
            return upper

    instance = _instance()
    rejected = _inbound_record()
    accepted = copy.deepcopy(rejected)
    accepted["record_uuid"] = str(uuid.uuid4())
    accepted["operation_uuid"] = str(uuid.uuid4())
    accepted["operation"]["entity_uuid"] = str(uuid.uuid4())
    live = copy.deepcopy(accepted)
    live["record_uuid"] = str(uuid.uuid4())
    live["operation_uuid"] = str(uuid.uuid4())
    live["operation"]["entity_uuid"] = str(uuid.uuid4())
    live_ready = [False]
    instance.provider_batch_size = 100
    instance.provider_delivery_random = Random()
    instance._live_workspace_delivery_pending = lambda: False
    instance._ready_live_workspace_delivery_pending = lambda: live_ready[0]

    def pending_workspace_deliveries(**kwargs):
        if kwargs["minimum_priority"] == 2:
            return [rejected, accepted]
        if live_ready[0]:
            live_ready[0] = False
            return [live]
        return []

    instance.store.pending_workspace_deliveries = pending_workspace_deliveries

    def apply(commands):
        delivery_uuids = [command["delivery_uuid"] for command in commands]
        if delivery_uuids == [live["operation_uuid"]]:
            raise provider_api.ProviderApiRetryableError(503)
        live_ready[0] = True
        raise provider_api.ProviderEventRejectedError(422)

    instance.provider_api.apply_commands = apply

    with pytest.raises(provider_api.ProviderApiRetryableError):
        instance._flush_history_events()

    assert instance.provider_delivery_retry_attempts == 1
    assert instance.provider_delivery_retry_after == 101.0
    assert instance.store.released == [
        live["record_uuid"],
        rejected["record_uuid"],
        accepted["record_uuid"],
    ]


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

    def invalid(commands):
        result = {
            "provider_event_key": (
                str(uuid.uuid4())
                if failure == "wrong_order"
                else commands[0]["provider_event_key"]
            ),
            "status": "rejected" if failure == "non_applied" else "applied",
        }
        return {"results": [result]}

    instance.provider_api.apply_commands = invalid

    with pytest.raises(ValueError):
        instance.flush_provider_events()
    assert instance.store.released == [record["record_uuid"]]
