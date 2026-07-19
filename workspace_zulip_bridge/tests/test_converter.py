import uuid

import pytest

from workspace_zulip_bridge import converter

ACCOUNT_UUID = str(uuid.uuid4())
OWNER_UUID = str(uuid.uuid4())
PROJECT_UUID = str(uuid.uuid4())


class FakeStore:
    def __init__(self, selection_mode="all", auto_materialize=True):
        self.account = {
            "owner_user_uuid": OWNER_UUID,
            "settings": {
                "selection_mode": selection_mode,
                "default_project_id": PROJECT_UUID,
            },
        }
        self.assignments = {}
        self.mappings = {}
        self.positions = {}
        self.auto_materialize = auto_materialize

    def account_resource(self, account_uuid):
        return self.account if account_uuid == ACCOUNT_UUID else None

    def account_settings(self, account_uuid):
        resource = self.account_resource(account_uuid)
        return None if resource is None else resource["settings"]

    def assignment_for_provider_chat(self, account_uuid, provider_chat_key):
        if provider_chat_key in self.assignments:
            return self.assignments[provider_chat_key]
        if self.account["settings"]["selection_mode"] == "all":
            return {"selected": True, "project_id": PROJECT_UUID}
        return None

    def producer_lane_position(self, operation_uuid, origin, causal_lane):
        lane = self.positions.setdefault(causal_lane, [])
        if operation_uuid not in lane:
            lane.append(operation_uuid)
        index = lane.index(operation_uuid)
        return index + 1, None if index == 0 else lane[index - 1]

    def provider_mapping(self, account_uuid, entity_kind, provider_id):
        mapping = self.mappings.get((entity_kind, provider_id))
        if mapping is not None or not self.auto_materialize:
            return mapping
        workspace_uuid = converter.stable_entity_uuid(
            ACCOUNT_UUID, entity_kind, provider_id
        )
        metadata = {}
        if entity_kind == "identity":
            metadata = {"display_name": f"User {provider_id}", "active": True}
        elif entity_kind == "stream":
            chat_type, _, raw_participants = provider_id.partition(":")
            participant_ids = (
                raw_participants.split(",")
                if chat_type in {"direct", "group_direct"}
                else ["2", "3", "4"]
            )
            metadata = {
                "chat_type": chat_type,
                "project_uuid": PROJECT_UUID,
                "participants": [
                    OWNER_UUID,
                    *[
                        converter.stable_entity_uuid(ACCOUNT_UUID, "identity", value)
                        for value in participant_ids
                        if value != "1"
                    ],
                ],
                "name": "Engineering",
                "description": "",
                "private": True,
                "default_topic_uuid": None,
            }
        elif entity_kind == "topic":
            metadata = {"chat_key": "channel:42"}
        else:
            return None
        mapping = {
            "workspace_uuid": workspace_uuid,
            "provider_id": provider_id,
            "provider_revision": None,
            "metadata": metadata,
            "convergent_alias": False,
        }
        self.mappings[(entity_kind, provider_id)] = mapping
        return mapping

    def remember_provider_mapping(
        self,
        account_uuid,
        entity_kind,
        provider_id,
        workspace_uuid,
        metadata,
        provider_revision=None,
    ):
        existing = self.mappings.get((entity_kind, provider_id))
        self.mappings[(entity_kind, provider_id)] = {
            "workspace_uuid": (
                workspace_uuid if existing is None else existing["workspace_uuid"]
            ),
            "provider_id": provider_id,
            "provider_revision": provider_revision,
            "metadata": metadata,
            "convergent_alias": (
                False if existing is None else existing.get("convergent_alias", False)
            ),
        }

    def rename_provider_mapping(
        self,
        account_uuid,
        entity_kind,
        old_provider_id,
        new_provider_id,
        metadata,
        provider_revision=None,
    ):
        mapping = self.mappings.pop((entity_kind, old_provider_id), None)
        if mapping is None:
            return None
        mapping.update(
            provider_id=new_provider_id,
            provider_revision=provider_revision,
            metadata=metadata,
        )
        self.mappings[(entity_kind, new_provider_id)] = mapping
        return mapping

    def mark_provider_mapping_deleted(self, account_uuid, entity_kind, provider_id):
        self.mappings.pop((entity_kind, provider_id), None)


def _dm_message(message_id=501):
    return {
        "id": message_id,
        "type": "private",
        "display_recipient": [
            {
                "id": 1,
                "is_me": True,
                "full_name": "Owner",
                "email": "owner@example.invalid",
            },
            {
                "id": 2,
                "is_me": False,
                "full_name": "Other User",
                "email": "other@example.invalid",
            },
        ],
        "sender_id": 2,
        "sender_full_name": "Other User",
        "sender_email": "other@example.invalid",
        "is_me_message": False,
        "recipient_display_name": "Owner, Other User",
        "subject": "",
        "timestamp": 1_700_000_000,
        "content": (
            "@**Other User** see [report.pdf](/user_uploads/a/report.pdf)\n"
            "~~~ quote\nquoted\n~~~"
        ),
    }


def _stream_message(message_id=601, subject="Topic"):
    return {
        "id": message_id,
        "type": "stream",
        "stream_id": 42,
        "display_recipient": "Engineering",
        "sender_id": 2,
        "sender_full_name": "Other User",
        "sender_email": "other@example.invalid",
        "subject": subject,
        "timestamp": 1_700_000_000,
        "content": "hello",
    }


def _operations(records):
    return [record["operation"] for record in records]


def test_dm_conversion_has_owner_membership_identity_urn_and_copied_file():
    store = FakeStore()
    event = {"id": 10, "type": "message", "message": _dm_message()}
    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        event,
        original_url="https://chat.example.invalid",
        file_resolver=lambda url, name: "urn:file:00000000-0000-0000-0000-000000000001",
    )
    operations = _operations(records)
    stream = next(op for op in operations if op["kind"] == "stream.upsert")
    message = next(op for op in operations if op["kind"] == "message.create")
    participants = stream["payload"]["participant_uuids"]
    assert len(participants) == 2
    assert OWNER_UUID in participants
    assert message["actor_uuid"] != OWNER_UUID
    markdown = message["payload"]["payload"]["content"]
    assert "urn:user:" in markdown
    assert "urn:file:" in markdown
    assert "/user_uploads/" not in markdown
    assert "> quoted" in markdown


def test_lossy_markdown_without_original_url_does_not_add_empty_link():
    converted, lossy = converter.convert_markdown("@**Unknown User**", {}, "")

    assert lossy
    assert converted == "@Unknown User"
    assert "[Open original]" not in converted


def test_new_chat_waits_for_backend_assignment_before_materialization():
    event = {"id": 10, "type": "message", "message": _stream_message()}
    with pytest.raises(ValueError, match="provider_chat_not_selected"):
        converter.event_records(FakeStore("explicit"), ACCOUNT_UUID, "queue", event)
    pending = FakeStore("all")
    pending.assignment_for_provider_chat = lambda *args: None
    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        converter.event_records(pending, ACCOUNT_UUID, "queue", event)
    materialized = FakeStore("all")
    operations = _operations(
        converter.event_records(materialized, ACCOUNT_UUID, "queue", event)
    )
    stream = next(op for op in operations if op["kind"] == "stream.upsert")
    assert stream["extensions"]["assignment_materialized"] is True
    assert stream["payload"]["participant_uuids"] == sorted(
        materialized.mappings[("stream", "channel:42")]["metadata"]["participants"]
    )


def test_channel_message_waits_for_exact_author_and_topic_projection():
    store = FakeStore(auto_materialize=False)
    stream_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": [OWNER_UUID],
            "name": "Engineering",
            "description": "",
            "private": False,
            "default_topic_uuid": None,
        },
    )
    event = {"id": 10, "type": "message", "message": _stream_message()}
    with pytest.raises(ValueError, match="provider_chat_assignment_pending"):
        converter.event_records(store, ACCOUNT_UUID, "queue", event)
    author_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "identity",
        "2",
        author_uuid,
        {"display_name": "Other User", "active": True},
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:Topic",
        topic_uuid,
        {"stream_uuid": stream_uuid, "chat_key": "channel:42"},
    )
    store.mappings[("stream", "channel:42")]["metadata"]["participants"] = [
        OWNER_UUID,
        author_uuid,
    ]
    operations = _operations(
        converter.event_records(store, ACCOUNT_UUID, "queue", event)
    )
    message = next(value for value in operations if value["kind"] == "message.create")
    topic = next(value for value in operations if value["kind"] == "topic.upsert")
    assert message["actor_uuid"] == author_uuid
    assert message["payload"]["topic_uuid"] == topic_uuid
    assert topic["entity_uuid"] == topic_uuid


def test_message_mutations_and_topic_rename_reuse_stable_mappings():
    store = FakeStore()
    create = {"id": 10, "type": "message", "message": _stream_message()}
    created = converter.event_records(store, ACCOUNT_UUID, "queue", create)
    created_message = next(
        operation
        for operation in _operations(created)
        if operation["kind"] == "message.create"
    )
    external_author_uuid = created_message["payload"]["author_uuid"]
    topic_uuid = next(
        op["entity_uuid"] for op in _operations(created) if op["kind"] == "topic.upsert"
    )
    update = {
        "id": 11,
        "type": "update_message",
        "message_id": 601,
        "message_ids": [601],
        "stream_id": 42,
        "orig_subject": "Topic",
        "subject": "Renamed",
        "content": "edited",
        "edit_timestamp": 1_700_000_010,
    }
    updated = _operations(converter.event_records(store, ACCOUNT_UUID, "queue", update))
    assert [operation["kind"] for operation in updated] == [
        "topic.upsert",
        "message.update",
    ]
    assert updated[0]["entity_uuid"] == topic_uuid
    assert updated[1]["actor_uuid"] == external_author_uuid
    assert store.provider_mapping(ACCOUNT_UUID, "message", "601")["metadata"][
        "content_sha256"
    ]
    deleted = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 12, "type": "delete_message", "message_ids": [601]},
        )
    )
    assert deleted[0]["kind"] == "message.delete"
    assert deleted[0]["actor_uuid"] == external_author_uuid
    assert deleted[0]["payload"]["author_uuid"] == external_author_uuid
    # Conversion is side-effect free for deletion. The service tombstones the
    # mapping atomically with the provider journal after durable enqueue.
    assert store.provider_mapping(ACCOUNT_UUID, "message", "601") is not None

    converter.event_records(store, ACCOUNT_UUID, "queue", create)
    read = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 13,
                "type": "update_message_flags",
                "flag": "read",
                "op": "add",
                "messages": [601],
            },
        )
    )
    assert read[0]["kind"] == "read_state.set"
    assert read[0]["payload"]["reader_uuid"] == OWNER_UUID
    assert read[0]["payload"]["message_uuids"] == [created_message["entity_uuid"]]
    assert "through_message_uuid" not in read[0]["payload"]


def test_channel_event_preserves_backend_owned_privacy_and_topology():
    store = FakeStore()
    stream_uuid = str(uuid.uuid4())
    topic_uuid = str(uuid.uuid4())
    participant_uuids = [
        OWNER_UUID,
        converter.stable_entity_uuid(ACCOUNT_UUID, "identity", "2"),
        str(uuid.uuid4()),
    ]
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "stream",
        "channel:42",
        stream_uuid,
        {
            "chat_type": "channel",
            "project_uuid": PROJECT_UUID,
            "participants": participant_uuids,
            "name": "Canonical name",
            "description": "Canonical description",
            "private": False,
            "default_topic_uuid": None,
        },
    )
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "topic",
        "42:Topic",
        topic_uuid,
        {"stream_uuid": stream_uuid, "chat_key": "channel:42"},
    )
    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 30, "type": "message", "message": _stream_message()},
    )
    stream = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "stream.upsert"
    )
    assert stream["payload"]["private"] is False
    assert stream["payload"]["name"] == "Canonical name"
    assert stream["payload"]["description"] == "Canonical description"
    assert set(stream["payload"]["participant_uuids"]).issuperset(participant_uuids)


def test_message_create_and_update_mentions_use_provider_identity_ids_and_urns():
    store = FakeStore()
    store.remember_provider_mapping(
        ACCOUNT_UUID,
        "identity",
        "1",
        OWNER_UUID,
        {"display_name": "Owner", "active": True},
    )
    message = _stream_message()
    message["content"] = "hello @**Mentioned User|3** and @**Owner|1**"
    created = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {"id": 20, "type": "message", "message": message},
        )
    )
    mentioned_uuid = converter.stable_entity_uuid(ACCOUNT_UUID, "identity", "3")
    identity = next(
        operation
        for operation in created
        if operation["kind"] == "identity.upsert"
        and operation["provider"]["entity_id"] == "3"
    )
    created_message = next(
        operation for operation in created if operation["kind"] == "message.create"
    )
    assert identity["entity_uuid"] == mentioned_uuid
    assert (
        f"[Mentioned User](urn:user:{mentioned_uuid})"
        in created_message["payload"]["payload"]["content"]
    )
    assert (
        f"[Owner](urn:user:{OWNER_UUID})"
        in created_message["payload"]["payload"]["content"]
    )
    assert not any(
        operation["kind"] == "identity.upsert"
        and operation["entity_uuid"] == OWNER_UUID
        for operation in created
    )

    resolved = []
    updated = _operations(
        converter.event_records(
            store,
            ACCOUNT_UUID,
            "queue",
            {
                "id": 21,
                "type": "update_message",
                "message_id": 601,
                "message_ids": [601],
                "stream_id": 42,
                "content": (
                    "edited @_**Another User|4** "
                    "[report.pdf](/user_uploads/a/report.pdf)"
                ),
                "edit_timestamp": 1_700_000_010,
            },
            file_resolver=lambda url, name: (
                resolved.append((url, name))
                or "urn:file:00000000-0000-0000-0000-000000000001"
            ),
        )
    )
    update_identity = next(
        operation for operation in updated if operation["kind"] == "identity.upsert"
    )
    updated_message = next(
        operation for operation in updated if operation["kind"] == "message.update"
    )
    assert update_identity["provider"]["entity_id"] == "4"
    assert (
        "[Another User](urn:user:" in updated_message["payload"]["payload"]["content"]
    )
    assert "urn:file:" in updated_message["payload"]["payload"]["content"]
    assert updated_message["actor_uuid"] == created_message["payload"]["author_uuid"]
    assert resolved == [("/user_uploads/a/report.pdf", "report.pdf")]


def test_inbound_zulip_reply_resolves_provider_target_to_workspace_message():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message(601)},
    )
    original = store.provider_mapping(ACCOUNT_UUID, "message", "601")
    reply = _stream_message(602)
    reply["content"] = (
        "@_**Other User|2** "
        "[said](https://zulip.example.invalid/#narrow/near/601):\n"
        "```quote\noriginal\n```\n\nreply"
    )

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 11, "type": "message", "message": reply},
    )
    message = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.create"
    )

    assert message["payload"]["reply_to_message_uuid"] == original["workspace_uuid"]
    assert message["extensions"]["unresolved_reply_provider_id"] is None


def test_convergent_workspace_alias_suppresses_provider_history_duplicate_create():
    store = FakeStore()
    workspace_uuid = str(uuid.uuid4())
    store.mappings[("message", "601")] = {
        "workspace_uuid": workspace_uuid,
        "provider_id": "601",
        "provider_revision": None,
        "metadata": {
            "mapping_origin": "workspace",
            "workspace_delivery_state": "committed",
        },
        "convergent_alias": True,
    }

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "history",
        {"id": 601, "type": "message", "message": _stream_message(601)},
        "backfill",
    )

    assert all(
        operation["kind"] != "message.create" for operation in _operations(records)
    )
    assert (
        store.provider_mapping(ACCOUNT_UUID, "message", "601")["workspace_uuid"]
        == workspace_uuid
    )


def test_provider_mapping_before_event_delivery_replays_same_workspace_message_uuid():
    store = FakeStore()
    pending_workspace_uuid = str(uuid.uuid4())
    store.mappings[("message", "601")] = {
        "workspace_uuid": pending_workspace_uuid,
        "provider_id": "601",
        "provider_revision": None,
        "metadata": {
            "mapping_origin": "zulip",
            "workspace_delivery_state": "pending",
        },
        "convergent_alias": False,
    }

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "history",
        {"id": 601, "type": "message", "message": _stream_message(601)},
        "backfill",
    )
    message = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.create"
    )

    assert message["entity_uuid"] == pending_workspace_uuid
    assert message["entity_uuid"] != converter.stable_entity_uuid(
        ACCOUNT_UUID, "message", "601"
    )


def test_unresolved_inbound_zulip_reply_is_preserved_as_safe_fallback():
    store = FakeStore()
    reply = _stream_message(602)
    reply["content"] = "[said](#narrow/near/999):\n```quote\nmissing\n```\n\nreply"

    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 11, "type": "message", "message": reply},
    )
    message = next(
        operation
        for operation in _operations(records)
        if operation["kind"] == "message.create"
    )

    assert message["payload"]["reply_to_message_uuid"] is None
    assert message["extensions"]["unresolved_reply_provider_id"] == "999"
    assert "missing" in message["payload"]["payload"]["content"]


def test_subscription_rename_reuses_stream_uuid():
    store = FakeStore()
    converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {"id": 10, "type": "message", "message": _stream_message()},
    )
    mapping = store.provider_mapping(ACCOUNT_UUID, "stream", "channel:42")
    records = converter.event_records(
        store,
        ACCOUNT_UUID,
        "queue",
        {
            "id": 14,
            "type": "subscription",
            "op": "update",
            "property": "name",
            "stream_id": 42,
            "value": "Platform",
        },
    )
    operation = records[0]["operation"]
    assert operation["kind"] == "stream.upsert"
    assert operation["entity_uuid"] == mapping["workspace_uuid"]
    assert operation["payload"]["name"] == "Platform"


def test_newest_first_uses_timestamp_then_numeric_message_id():
    messages = [
        {"id": 1, "timestamp": 10},
        {"id": 3, "timestamp": 10},
        {"id": 2, "timestamp": 11},
    ]
    assert [message["id"] for message in converter.newest_first(messages)] == [
        2,
        3,
        1,
    ]
