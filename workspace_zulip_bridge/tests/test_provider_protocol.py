import datetime
import uuid

import pytest

from workspace_zulip_bridge import provider_protocol

ACCOUNT_UUID = "10000000-0000-0000-0000-000000000001"
PROJECT_UUID = "20000000-0000-0000-0000-000000000002"
STREAM_UUID = "30000000-0000-0000-0000-000000000003"
TOPIC_UUID = "40000000-0000-0000-0000-000000000004"
MESSAGE_UUID = "50000000-0000-0000-0000-000000000005"
CHAT_UUID = "60000000-0000-0000-0000-000000000006"


class Store:
    def provider_mapping(self, account_uuid, kind, provider_id):
        assert (account_uuid, kind, provider_id) == (
            ACCOUNT_UUID,
            "message",
            "101",
        )
        return {"metadata": {"provider_timestamp": 1_752_840_000}}

    def workspace_mapping(self, account_uuid, kind, workspace_uuid):
        assert account_uuid == ACCOUNT_UUID
        return {
            "stream": {"provider_id": "channel:42", "metadata": {}},
            "topic": {"provider_id": "42:dev", "metadata": {}},
            "message": {
                "provider_id": "101",
                "metadata": {"chat_key": "channel:42"},
            },
            "identity": {
                "provider_id": "42",
                "metadata": {
                    "display_name": "Former User",
                    "email": None,
                    "avatar_urn": None,
                    "active": True,
                },
            },
        }[kind]

    def assignment_for_provider_chat(self, account_uuid, chat_key):
        assert (account_uuid, chat_key) == (ACCOUNT_UUID, "channel:42")
        return {"uuid": CHAT_UUID}


def _lease(kind="message.create"):
    return {
        "provider_operation_uuid": str(uuid.uuid4()),
        "external_operation_uuid": str(uuid.uuid4()),
        "lease_uuid": str(uuid.uuid4()),
        "lease_expires_at": "2026-07-18T15:00:00Z",
        "external_account_uuid": ACCOUNT_UUID,
        "project_id": PROJECT_UUID,
        "operation_kind": kind,
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


def test_provider_lease_adapts_to_existing_durable_zulip_scheduler():
    leased = _lease()
    record = provider_protocol.leased_operation_record(Store(), leased)

    assert record["record_uuid"] == leased["provider_operation_uuid"]
    assert record["operation_uuid"] == leased["external_operation_uuid"]
    assert record["sequence"] == 0
    assert record["operation"]["provider"]["chat_id"] == "channel:42"
    assert record["operation"]["kind"] == "message.create"
    assert record["transport"]["lease_uuid"] == leased["lease_uuid"]


def test_zulip_record_adapts_to_atomic_provider_event_resource():
    leased = _lease()
    record = provider_protocol.leased_operation_record(Store(), leased)
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"]["provider"]["entity_id"] = "101"

    event = provider_protocol.event_payload(Store(), record)

    assert event["kind"] == "message.upsert"
    assert event["external_chat_uuid"] == CHAT_UUID
    resource = event["payload"]["resource"]
    assert resource["uuid"] == MESSAGE_UUID
    assert resource["provider_external_id"] == "101"
    assert resource["user_uuid"] == ACCOUNT_UUID
    assert resource["created_at"] == (
        datetime.datetime.fromtimestamp(1_752_840_000, datetime.UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )
    assert resource["author_identity"] == {
        "provider_external_id": "42",
        "display_name": "Former User",
        "email": None,
        "avatar_urn": None,
        "active": True,
    }


def test_provider_event_normalizes_naive_live_message_timestamp_to_utc():
    class LiveStore(Store):
        def provider_mapping(self, account_uuid, kind, provider_id):
            assert (account_uuid, kind, provider_id) == (
                ACCOUNT_UUID,
                "message",
                "101",
            )
            return {"metadata": {}}

    record = provider_protocol.leased_operation_record(LiveStore(), _lease())
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"]["provider"]["entity_id"] = "101"
    record["operation"]["occurred_at"] = "2026-07-23 21:09:36"

    event = provider_protocol.event_payload(LiveStore(), record)

    assert event["payload"]["resource"]["created_at"] == "2026-07-23T21:09:36Z"


def test_missing_identity_is_sent_as_provider_event_without_stream_membership():
    record = provider_protocol.leased_operation_record(Store(), _lease())
    record["operation"]["kind"] = "identity.upsert"
    record["operation"]["provider"]["entity_id"] = "42"
    record["operation"]["payload"] = {
        "display_name": "Former User",
        "email": None,
        "avatar_urn": None,
        "active": True,
    }

    event = provider_protocol.event_payload(Store(), record)

    assert event["kind"] == "identity.upsert"
    assert event["payload"]["resource"]["provider_external_id"] == "42"
    assert event["payload"]["resource"]["display_name"] == "Former User"


def test_provider_read_state_adapts_to_provider_event_without_losing_selector():
    record = provider_protocol.leased_operation_record(Store(), _lease())
    first_message_uuid = "70000000-0000-0000-0000-000000000007"
    last_message_uuid = "80000000-0000-0000-0000-000000000008"
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"].update(
        {
            "kind": "read_state.set",
            "entity_uuid": STREAM_UUID,
            "provider": {
                "kind": "zulip",
                "chat_id": "channel:42",
                "entity_id": None,
                "revision": None,
            },
            "payload": {
                "stream_uuid": STREAM_UUID,
                "topic_uuid": TOPIC_UUID,
                "reader_uuid": ACCOUNT_UUID,
                "message_uuids": [first_message_uuid, last_message_uuid],
                "read": True,
            },
        }
    )

    event = provider_protocol.event_payload(Store(), record)

    assert event["kind"] == "read_state.set"
    assert event["external_chat_uuid"] == CHAT_UUID
    resource = event["payload"]["resource"]
    assert resource["uuid"] == STREAM_UUID
    assert resource["provider_external_id"] == "channel:42"
    assert resource["stream_uuid"] == STREAM_UUID
    assert resource["topic_uuid"] == TOPIC_UUID
    assert resource["reader_uuid"] == ACCOUNT_UUID
    assert resource["message_uuids"] == [first_message_uuid, last_message_uuid]
    assert resource["read"] is True


def test_unknown_provider_mutation_fails_closed_instead_of_being_discarded():
    record = provider_protocol.leased_operation_record(Store(), _lease())
    record["operation"]["kind"] = "message.forward"

    with pytest.raises(ValueError, match="Unsupported Provider event operation kind"):
        provider_protocol.event_payload(Store(), record)


def test_terminal_result_is_bound_to_exact_provider_lease():
    leased = _lease()
    record = provider_protocol.leased_operation_record(Store(), leased)
    result = {
        **record,
        "record_kind": "result",
        "record_uuid": str(uuid.uuid4()),
        "result": {"outcome": "committed", "safe_error": None},
    }

    payload = provider_protocol.result_payload(result)

    assert payload["provider_operation_uuid"] == leased["provider_operation_uuid"]
    assert payload["lease_uuid"] == leased["lease_uuid"]
    assert payload["status"] == "succeeded"


def test_exact_read_lease_adapts_without_reinterpreting_message_order():
    first_message_uuid = "70000000-0000-0000-0000-000000000007"
    last_message_uuid = "80000000-0000-0000-0000-000000000008"

    class ReadStore(Store):
        def __init__(self):
            self.mapping_calls = []

        def workspace_mapping(self, account_uuid, kind, workspace_uuid):
            self.mapping_calls.append((account_uuid, kind, workspace_uuid))
            return super().workspace_mapping(account_uuid, kind, workspace_uuid)

    store = ReadStore()
    leased = _lease("read_state.set")
    leased["required_capability"] = "messenger.message.read"
    leased["payload"] = {
        "stream_uuid": STREAM_UUID,
        "topic_uuid": TOPIC_UUID,
        "reader_uuid": ACCOUNT_UUID,
        "message_uuids": [first_message_uuid, last_message_uuid],
        "read": True,
    }

    record = provider_protocol.leased_operation_record(store, leased)

    operation = record["operation"]
    assert operation["kind"] == "read_state.set"
    assert operation["entity_uuid"] == last_message_uuid
    assert operation["actor_uuid"] == ACCOUNT_UUID
    assert operation["provider"] == {
        "kind": "zulip",
        "chat_id": "channel:42",
        "entity_id": None,
        "revision": None,
    }
    assert operation["payload"]["message_uuids"] == [
        first_message_uuid,
        last_message_uuid,
    ]
    assert store.mapping_calls == [(ACCOUNT_UUID, "stream", STREAM_UUID)]


def test_provider_read_lease_requires_exact_nonempty_message_selector():
    leased = _lease("read_state.set")
    leased["payload"] = {
        "stream_uuid": STREAM_UUID,
        "topic_uuid": TOPIC_UUID,
        "reader_uuid": ACCOUNT_UUID,
        "message_uuids": [],
        "read": True,
    }

    with pytest.raises(ValueError, match="requires exact message UUIDs"):
        provider_protocol.leased_operation_record(Store(), leased)
