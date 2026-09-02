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
REACTION_UUID = "70000000-0000-0000-0000-000000000007"
BINDING_UUID = "80000000-0000-0000-0000-000000000008"


class Store:
    def provider_event_cursor(self, account_uuid):
        assert account_uuid == ACCOUNT_UUID
        return {"provider_owner_user_id": "42"}

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


class PendingMessageMappingStore(Store):
    def workspace_mapping(self, account_uuid, kind, workspace_uuid):
        if kind == "message":
            return None
        return super().workspace_mapping(account_uuid, kind, workspace_uuid)


class NewTopicMappingStore(Store):
    def workspace_mapping(self, account_uuid, kind, workspace_uuid):
        if kind == "topic":
            return None
        return super().workspace_mapping(account_uuid, kind, workspace_uuid)


class TombstonedIdentityStore(Store):
    def workspace_mapping(self, account_uuid, kind, workspace_uuid):
        if kind == "identity":
            return None
        return super().workspace_mapping(account_uuid, kind, workspace_uuid)

    def tombstoned_workspace_mapping(self, account_uuid, kind, workspace_uuid):
        assert (account_uuid, kind, workspace_uuid) == (
            ACCOUNT_UUID,
            "identity",
            ACCOUNT_UUID,
        )
        mapping = super().workspace_mapping(account_uuid, kind, workspace_uuid)
        return {
            **mapping,
            "metadata": {
                "display_name": "Unavailable Zulip user (ID 42)",
                "email": None,
                "avatar_urn": None,
                "active": False,
            },
        }


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


@pytest.mark.parametrize("kind", ["message.update", "message.delete"])
def test_provider_message_mutation_exposes_canonical_actor(kind):
    leased = _lease(kind)

    record = provider_protocol.leased_operation_record(Store(), leased)

    operation = record["operation"]
    assert operation["kind"] == kind
    assert operation["actor_uuid"] == ACCOUNT_UUID
    assert operation["payload"]["user_uuid"] == ACCOUNT_UUID
    assert "author_uuid" not in operation["payload"]


@pytest.mark.parametrize(
    ("kind", "required_capability", "payload", "expected_bridge_kind"),
    [
        (
            "stream.delete",
            "messenger.stream.delete",
            {"uuid": STREAM_UUID},
            "stream.delete",
        ),
        (
            "topic.create",
            "messenger.topic.create",
            {"uuid": TOPIC_UUID, "stream_uuid": STREAM_UUID, "name": "new topic"},
            "topic.create",
        ),
        (
            "topic.delete",
            "messenger.topic.delete",
            {"uuid": TOPIC_UUID, "stream_uuid": STREAM_UUID, "name": "dev"},
            "topic.delete",
        ),
    ],
)
def test_provider_lease_accepts_channel_lifecycle_operations(
    kind, required_capability, payload, expected_bridge_kind
):
    leased = _lease(kind)
    leased["required_capability"] = required_capability
    leased["payload"] = payload
    store = NewTopicMappingStore() if kind == "topic.create" else Store()

    record = provider_protocol.leased_operation_record(store, leased)

    assert record["operation"]["kind"] == expected_bridge_kind
    assert record["operation"]["provider"]["chat_id"] == "channel:42"
    assert record["operation"]["provider"]["entity_id"] == (
        None
        if kind == "topic.create"
        else "channel:42"
        if kind == "stream.delete"
        else "42:dev"
    )


@pytest.mark.parametrize("kind", ["topic.update", "topic.delete"])
def test_topic_mutation_can_be_leased_behind_pending_create(kind):
    leased = _lease(kind)
    leased["required_capability"] = (
        "messenger.topic.rename" if kind == "topic.update" else "messenger.topic.delete"
    )
    leased["payload"] = {
        "uuid": TOPIC_UUID,
        "stream_uuid": STREAM_UUID,
        "name": "new topic",
    }

    record = provider_protocol.leased_operation_record(NewTopicMappingStore(), leased)

    assert record["operation"]["provider"]["entity_id"] is None


def test_provider_read_lease_uses_physical_page_identity_internally():
    first = _lease("read_state.set")
    first["required_capability"] = "messenger.message.read"
    first["payload"] = {
        "stream_uuid": STREAM_UUID,
        "topic_uuid": TOPIC_UUID,
        "reader_uuid": ACCOUNT_UUID,
        "message_uuids": [MESSAGE_UUID],
        "read": True,
    }
    second = _lease("read_state.set")
    second["external_operation_uuid"] = second["provider_operation_uuid"]
    second["required_capability"] = "messenger.message.read"
    second["payload"] = {
        **first["payload"],
        "message_uuids": [str(uuid.uuid4())],
    }

    first_record = provider_protocol.leased_operation_record(Store(), first)
    second_record = provider_protocol.leased_operation_record(Store(), second)

    assert first["external_operation_uuid"] != first["provider_operation_uuid"]
    assert first_record["operation_uuid"] == first["provider_operation_uuid"]
    assert second["external_operation_uuid"] == second["provider_operation_uuid"]
    assert second_record["operation_uuid"] == second["provider_operation_uuid"]
    assert first_record["operation_uuid"] != second_record["operation_uuid"]
    assert first_record["operation_sha256"] != second_record["operation_sha256"]


def test_provider_read_lease_reconstruction_keeps_terminal_digest():
    leased = _lease("read_state.set")
    leased["required_capability"] = "messenger.message.read"
    leased["payload"] = {
        "stream_uuid": STREAM_UUID,
        "topic_uuid": TOPIC_UUID,
        "reader_uuid": ACCOUNT_UUID,
        "message_uuids": [MESSAGE_UUID],
        "read": True,
    }

    first = provider_protocol.leased_operation_record(Store(), leased)
    leased["lease_uuid"] = str(uuid.uuid4())
    leased["lease_expires_at"] = "2026-07-18T16:00:00Z"
    second = provider_protocol.leased_operation_record(Store(), leased)

    assert first["operation"]["occurred_at"] == "1970-01-01T00:00:00Z"
    assert second["operation"]["occurred_at"] == "1970-01-01T00:00:00Z"
    assert first["operation_uuid"] == second["operation_uuid"]
    assert first["operation_sha256"] == second["operation_sha256"]


def test_timestamp_free_delete_lease_reconstruction_keeps_terminal_digest():
    leased = _lease("stream.delete")
    leased["required_capability"] = "messenger.stream.delete"
    leased["payload"] = {"uuid": STREAM_UUID}

    first = provider_protocol.leased_operation_record(Store(), leased)
    leased["lease_uuid"] = str(uuid.uuid4())
    leased["lease_expires_at"] = "2026-07-18T16:00:00Z"
    second = provider_protocol.leased_operation_record(Store(), leased)

    assert first["operation"]["occurred_at"] == "1970-01-01T00:00:00Z"
    assert second["operation"]["occurred_at"] == "1970-01-01T00:00:00Z"
    assert first["operation_uuid"] == second["operation_uuid"]
    assert first["operation_sha256"] == second["operation_sha256"]


def test_provider_reaction_lease_resolves_the_target_message_mapping():
    leased = _lease("reaction.create")
    leased["required_capability"] = "messenger.reaction.write"
    leased["payload"] = {
        "uuid": REACTION_UUID,
        "message_uuid": MESSAGE_UUID,
        "user_uuid": ACCOUNT_UUID,
        "emoji_name": "thumbs_up",
    }

    record = provider_protocol.leased_operation_record(Store(), leased)

    assert record["operation"]["kind"] == "reaction.create"
    assert record["operation"]["entity_uuid"] == REACTION_UUID
    assert record["operation"]["provider"]["entity_id"] == "101"
    assert record["operation"]["provider"]["chat_id"] == "channel:42"


@pytest.mark.parametrize("kind", ["membership.add", "membership.remove"])
def test_provider_membership_lease_resolves_target_identity(kind):
    leased = _lease(kind)
    leased["required_capability"] = "messenger.membership.write"
    leased["payload"] = {
        "uuid": BINDING_UUID,
        "stream_uuid": STREAM_UUID,
        "user_uuid": ACCOUNT_UUID,
        "who_uuid": PROJECT_UUID,
        "role": "member",
    }

    record = provider_protocol.leased_operation_record(Store(), leased)

    assert record["operation"]["kind"] == kind
    assert record["operation"]["entity_uuid"] == BINDING_UUID
    assert record["operation"]["actor_uuid"] == PROJECT_UUID
    assert record["operation"]["provider"]["entity_id"] == "42"
    assert record["operation"]["provider"]["chat_id"] == "channel:42"


@pytest.mark.parametrize(
    ("kind", "entity_uuid"),
    [
        ("stream.notification.update", STREAM_UUID),
        ("topic.notification.update", TOPIC_UUID),
    ],
)
def test_provider_notification_lease_preserves_lww_timestamp(kind, entity_uuid):
    leased = _lease(kind)
    leased["required_capability"] = "messenger.notification.write"
    leased["payload"] = {
        "uuid": entity_uuid,
        "stream_uuid": STREAM_UUID,
        "user_uuid": ACCOUNT_UUID,
        "notification_mode": "muted" if kind.startswith("stream.") else "mute",
        "notification_updated_at": "2026-08-23T12:30:00Z",
    }

    record = provider_protocol.leased_operation_record(Store(), leased)

    assert record["operation"]["kind"] == kind
    assert record["operation"]["entity_uuid"] == entity_uuid
    assert record["operation"]["occurred_at"] == "2026-08-23T12:30:00Z"
    assert record["operation"]["provider"]["chat_id"] == "channel:42"


@pytest.mark.parametrize("kind", ["message.update", "message.delete"])
def test_provider_message_mutation_defers_missing_create_mapping(kind):
    leased = _lease(kind)

    record = provider_protocol.leased_operation_record(
        PendingMessageMappingStore(),
        leased,
    )

    assert record["operation"]["kind"] == kind
    assert record["operation"]["entity_uuid"] == MESSAGE_UUID
    assert record["operation"]["provider"]["entity_id"] is None
    assert record["operation"]["provider"]["chat_id"] == "channel:42"


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


def test_zulip_record_adapts_to_provider_native_v2_command():
    record = provider_protocol.leased_operation_record(Store(), _lease())
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"]["provider"]["entity_id"] = "101"

    command = provider_protocol.command_payload(Store(), record)

    assert command == {
        "provider_event_key": provider_protocol.provider_event_key(
            "message.upsert",
            "channel:42",
            {"kind": "message", "id": "101"},
            {"topic": "42:dev", "user": "42"},
            {
                "payload": {"kind": "markdown", "content": "hello"},
                "created_at": (
                    datetime.datetime.fromtimestamp(1_752_840_000, datetime.UTC)
                    .isoformat()
                    .replace("+00:00", "Z")
                ),
                "author_identity": {
                    "provider_external_id": "42",
                    "display_name": "Former User",
                    "email": None,
                    "avatar_urn": None,
                    "active": True,
                },
                "provider_metadata": {"provider_revision": None},
            },
        ),
        "delivery_uuid": record["operation_uuid"],
        "external_account_uuid": ACCOUNT_UUID,
        "provider_chat_key": "channel:42",
        "provider_sequence": None,
        "delivery_class": "live",
        "kind": "message.upsert",
        "provider_object": {"kind": "message", "id": "101"},
        "provider_references": {"topic": "42:dev", "user": "42"},
        "payload": {
            "payload": {"kind": "markdown", "content": "hello"},
            "created_at": (
                datetime.datetime.fromtimestamp(1_752_840_000, datetime.UTC)
                .isoformat()
                .replace("+00:00", "Z")
            ),
            "author_identity": {
                "provider_external_id": "42",
                "display_name": "Former User",
                "email": None,
                "avatar_urn": None,
                "active": True,
            },
            "provider_metadata": {"provider_revision": None},
        },
    }


def test_history_finalizer_command_preserves_transport_classification():
    record = {
        "operation_uuid": str(uuid.uuid4()),
        "account_uuid": ACCOUNT_UUID,
        "project_uuid": PROJECT_UUID,
        "sequence": 9,
        "created_at": "1970-01-01T00:00:00Z",
        "operation": {
            "kind": "history.finalize",
            "entity_uuid": STREAM_UUID,
            "actor_uuid": ACCOUNT_UUID,
            "occurred_at": "1970-01-01T00:00:00Z",
            "provider": {
                "kind": "zulip",
                "chat_id": "channel:42",
                "entity_id": "channel:42",
                "revision": None,
            },
            "payload": {"stream_uuid": STREAM_UUID, "generation": 3},
            "extensions": {
                "provider_badge": "zulip",
                "delivery_class": "backfill",
            },
        },
    }

    command = provider_protocol.command_payload(Store(), record)

    assert command["kind"] == "history.finalize"
    assert command["delivery_class"] == "backfill"
    assert command["provider_object"] == {
        "kind": "history",
        "id": "channel:42",
    }
    assert command["provider_references"] == {}
    assert command["payload"]["generation"] == 3
    assert "delivery_class" not in command["payload"]["provider_metadata"]


def test_provider_event_key_is_state_based_across_delivery_paths():
    record = provider_protocol.leased_operation_record(Store(), _lease())
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"]["provider"]["entity_id"] = "101"
    record["operation"]["extensions"] = {"delivery_class": "history"}

    history = provider_protocol.command_payload(Store(), record)
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"]["extensions"] = {"delivery_class": "live"}
    live = provider_protocol.command_payload(Store(), record)

    assert history["provider_event_key"] == live["provider_event_key"]
    assert history["delivery_uuid"] != live["delivery_uuid"]
    assert history["provider_event_key"].startswith(
        "provider-event:v1:message.upsert:message:3:101:"
    )


def test_provider_event_key_changes_with_desired_provider_state():
    record = provider_protocol.leased_operation_record(Store(), _lease())
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"]["provider"]["entity_id"] = "101"
    initial = provider_protocol.command_payload(Store(), record)
    record["operation"]["payload"]["payload"]["content"] = "updated"
    updated = provider_protocol.command_payload(Store(), record)

    assert initial["provider_event_key"] != updated["provider_event_key"]


def test_direct_conversation_key_is_sorted_and_includes_verified_owner():
    assert (
        provider_protocol.direct_conversation_key(
            Store(),
            ACCOUNT_UUID,
            "group_direct:77,7",
        )
        == "direct-conversation:v1:3:7,42,77"
    )


def test_provider_message_event_reuses_tombstoned_author_profile():
    record = provider_protocol.leased_operation_record(Store(), _lease())
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"]["provider"]["entity_id"] = "101"

    event = provider_protocol.event_payload(TombstonedIdentityStore(), record)

    assert event["payload"]["resource"]["author_identity"] == {
        "provider_external_id": "42",
        "display_name": "Unavailable Zulip user (ID 42)",
        "email": None,
        "avatar_urn": None,
        "active": False,
    }


def test_topic_only_message_update_preserves_move_in_provider_event():
    destination_topic_uuid = str(uuid.uuid4())
    record = provider_protocol.leased_operation_record(Store(), _lease())
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"]["kind"] = "message.update"
    record["operation"]["provider"]["entity_id"] = "101"
    record["operation"]["payload"].pop("payload")
    record["operation"]["payload"]["topic_uuid"] = destination_topic_uuid

    event = provider_protocol.event_payload(Store(), record)

    resource = event["payload"]["resource"]
    assert event["kind"] == "message.upsert"
    assert resource["topic_uuid"] == destination_topic_uuid
    assert "payload" not in resource


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


def test_provider_reaction_event_includes_actor_identity_and_delete_selector():
    record = provider_protocol.leased_operation_record(Store(), _lease())
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"].update(
        {
            "kind": "reaction.delete",
            "entity_uuid": REACTION_UUID,
            "actor_uuid": ACCOUNT_UUID,
            "provider": {
                "kind": "zulip",
                "chat_id": "channel:42",
                "entity_id": "101:42:unicode_emoji:1f44d",
                "revision": None,
            },
            "payload": {
                "stream_uuid": STREAM_UUID,
                "topic_uuid": TOPIC_UUID,
                "message_uuid": MESSAGE_UUID,
                "user_uuid": ACCOUNT_UUID,
                "emoji_name": "👍",
            },
            "extensions": {
                "emoji_name": "thumbs_up",
                "emoji_code": "1f44d",
                "reaction_type": "unicode_emoji",
            },
        }
    )

    event = provider_protocol.event_payload(Store(), record)

    assert event["kind"] == "reaction.delete"
    resource = event["payload"]["resource"]
    assert resource["uuid"] == REACTION_UUID
    assert resource["message_uuid"] == MESSAGE_UUID
    assert resource["user_uuid"] == ACCOUNT_UUID
    assert resource["emoji_name"] == "👍"
    assert resource["provider_metadata"]["emoji_name"] == "thumbs_up"
    assert resource["provider_metadata"]["emoji_code"] == "1f44d"
    assert resource["user_identity"]["provider_external_id"] == "42"


def test_provider_reaction_event_reuses_tombstoned_actor_profile():
    record = provider_protocol.leased_operation_record(Store(), _lease())
    record["origin"] = "zulip"
    record["operation_uuid"] = str(uuid.uuid4())
    record["operation"].update(
        {
            "kind": "reaction.upsert",
            "entity_uuid": REACTION_UUID,
            "provider": {
                "kind": "zulip",
                "chat_id": "channel:42",
                "entity_id": "101:42:unicode_emoji:1f44d",
                "revision": None,
            },
            "payload": {
                "stream_uuid": STREAM_UUID,
                "topic_uuid": TOPIC_UUID,
                "message_uuid": MESSAGE_UUID,
                "user_uuid": ACCOUNT_UUID,
                "emoji_name": "👍",
            },
        }
    )

    event = provider_protocol.event_payload(TombstonedIdentityStore(), record)

    assert event["payload"]["resource"]["user_identity"] == {
        "provider_external_id": "42",
        "display_name": "Unavailable Zulip user (ID 42)",
        "email": None,
        "avatar_urn": None,
        "active": False,
    }


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


def test_committed_message_result_reports_provider_assigned_identifier():
    leased = _lease()
    record = provider_protocol.leased_operation_record(Store(), leased)
    result = {
        **record,
        "record_kind": "result",
        "record_uuid": str(uuid.uuid4()),
        "result": {
            "outcome": "committed",
            "provider_entity_id": "14019",
            "safe_error": None,
        },
    }

    assert provider_protocol.result_payload(result)["provider_entity_id"] == "14019"


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
