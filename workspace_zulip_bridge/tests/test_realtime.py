# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from collections.abc import Mapping
from typing import Any
from uuid import UUID

import pytest

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.models import ExternalAccount
from workspace_zulip_bridge.models import MessageLink
from workspace_zulip_bridge.models import StreamLink
from workspace_zulip_bridge.models import ZulipIdentity
from workspace_zulip_bridge.realtime import WorkspaceRealtimeForwarder
from workspace_zulip_bridge.realtime import ZulipRealtimeProcessor
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_stream_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid

ACCOUNT_UUID = UUID("10000000-0000-0000-0000-000000000001")
OWNER_UUID = UUID("10000000-0000-0000-0000-000000000002")
PROJECT_UUID = UUID("10000000-0000-0000-0000-000000000003")
MESSAGE_UUID = UUID("10000000-0000-0000-0000-000000000004")
ENDPOINT = "https://zulip.example.test"
IDENTITY = ZulipIdentity(42, "owner@example.test", "Owner")


def _account() -> ExternalAccount:
    return ExternalAccount(
        ACCOUNT_UUID,
        OWNER_UUID,
        1,
        PROJECT_UUID,
        ENDPOINT,
        "owner@example.test",
        "secret",
        queue_id="queue-1",
        last_event_id=4,
    )


class FakeWorkspace:
    def __init__(self) -> None:
        self.batches: list[list[dict[str, Any]]] = []

    async def apply(self, operations: list[dict[str, Any]]) -> None:
        self.batches.append(operations)


class FakeStore:
    def __init__(self) -> None:
        self.reverse: dict[tuple[UUID, int], UUID] = {}
        self.messages: dict[UUID, MessageLink] = {}
        self.streams: dict[UUID, StreamLink] = {}
        self.topics: dict[UUID, str] = {}

    async def workspace_message_uuid(
        self, account_uuid: UUID, message_id: int
    ) -> UUID | None:
        return self.reverse.get((account_uuid, message_id))

    async def message_link(self, message_uuid: UUID) -> MessageLink | None:
        return self.messages.get(message_uuid)

    async def stream_link(self, stream_uuid: UUID) -> StreamLink | None:
        return self.streams.get(stream_uuid)

    async def topic_name(self, topic_uuid: UUID) -> str | None:
        return self.topics.get(topic_uuid)

    async def upsert_realtime_links(
        self,
        account_uuid: UUID,
        stream_uuid: UUID,
        chat_key: str,
        topic_uuid: UUID,
        topic_name: str,
        message_uuid: UUID,
        message_id: int | None,
    ) -> None:
        account = _account()
        assert account_uuid == account.uuid
        self.streams[stream_uuid] = StreamLink(account, chat_key)
        self.topics[topic_uuid] = topic_name
        self.messages[message_uuid] = MessageLink(
            account,
            message_id,
            stream_uuid,
            topic_uuid,
        )
        if message_id is not None:
            self.reverse[(account_uuid, message_id)] = message_uuid

    async def delete_message_link(self, message_uuid: UUID) -> None:
        self.messages.pop(message_uuid, None)


def _message_event(**overrides: object) -> dict[str, Any]:
    message: dict[str, Any] = {
        "id": 71,
        "type": "stream",
        "stream_id": 9,
        "display_recipient": "Realtime",
        "subject": "Now",
        "sender_id": 88,
        "sender_email": "sender@example.test",
        "sender_full_name": "Sender",
        "content": "**raw Markdown**",
        "timestamp": 1_700_000_000,
    }
    message.update(overrides)
    return {"id": 5, "type": "message", "message": message}


def test_realtime_identifiers_are_isolated_per_external_account() -> None:
    other_account_uuid = UUID("10000000-0000-0000-0000-000000000099")

    assert stable_stream_uuid(ACCOUNT_UUID, "channel:9") != stable_stream_uuid(
        other_account_uuid,
        "channel:9",
    )
    assert stable_message_uuid(ACCOUNT_UUID, 71) != stable_message_uuid(
        other_account_uuid,
        71,
    )


def test_zulip_event_writes_only_current_message_context() -> None:
    async def run() -> None:
        store = FakeStore()
        workspace = FakeWorkspace()
        await ZulipRealtimeProcessor(  # type: ignore[arg-type]
            store,
            workspace,  # type: ignore[arg-type]
        ).apply(_account(), IDENTITY, _message_event())

        operations = workspace.batches[0]
        assert [operation["type"] for operation in operations] == [
            "users",
            "users",
            "streams",
            "stream_bindings",
            "topics",
            "topic_bindings",
            "messages",
        ]
        assert operations[2]["data"]["history_public_to_subscribers"] is False
        assert operations[-1]["data"]["payload"] == {
            "kind": "markdown",
            "content": "**raw Markdown**",
        }
        assert set(store.messages) == {stable_message_uuid(ACCOUNT_UUID, 71)}

    asyncio.run(run())


def test_zulip_update_event_writes_the_fetched_message() -> None:
    async def run() -> None:
        store = FakeStore()
        workspace = FakeWorkspace()
        event = _message_event(content="edited Markdown")
        event["type"] = "update_message"
        event["edit_timestamp"] = 1_700_000_100

        await ZulipRealtimeProcessor(  # type: ignore[arg-type]
            store,
            workspace,  # type: ignore[arg-type]
        ).apply(_account(), IDENTITY, event)

        assert workspace.batches[0][-1]["data"]["payload"] == {
            "kind": "markdown",
            "content": "edited Markdown",
        }
        assert workspace.batches[0][-1]["source_updated_at"] == ("2023-11-14T22:15:00Z")

    asyncio.run(run())


def test_local_echo_wins_over_an_existing_reverse_mapping() -> None:
    async def run() -> None:
        store = FakeStore()
        workspace = FakeWorkspace()
        stream_uuid = stable_stream_uuid(ACCOUNT_UUID, "channel:9")
        topic_uuid = stable_topic_uuid(stream_uuid, "Now")
        await store.upsert_realtime_links(
            ACCOUNT_UUID,
            stream_uuid,
            "channel:9",
            topic_uuid,
            "Now",
            MESSAGE_UUID,
            71,
        )
        event = _message_event()
        event["local_message_id"] = str(MESSAGE_UUID)

        await ZulipRealtimeProcessor(  # type: ignore[arg-type]
            store,
            workspace,  # type: ignore[arg-type]
        ).apply(_account(), IDENTITY, event)

        assert workspace.batches == []
        assert store.reverse[(ACCOUNT_UUID, 71)] == MESSAGE_UUID

    asyncio.run(run())


def test_workspace_zulip_source_event_is_not_echoed_back() -> None:
    async def run() -> None:
        store = FakeStore()
        frame: Mapping[str, Any] = {
            "uuid": "10000000-0000-0000-0000-000000000005",
            "object_type": "message",
            "action": "created",
            "payload": {
                "uuid": str(MESSAGE_UUID),
                "source_name": "zulip",
            },
        }

        await WorkspaceRealtimeForwarder(  # type: ignore[arg-type]
            store,
            Settings(),
        ).apply(frame)

        assert store.messages == {}

    asyncio.run(run())


def test_workspace_message_is_sent_then_updated_and_deleted_by_mapping() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def own_user_id(self) -> int:
            return 42

        def send_message(
            self,
            chat_key: str,
            own_user_id: int,
            content: str,
            *,
            topic: str | None,
            queue_id: str,
            local_id: str,
        ) -> int:
            self.calls.append(
                ("send", chat_key, own_user_id, content, topic, queue_id, local_id)
            )
            return 71

        def update_message(
            self,
            message_id: int,
            *,
            content: str,
            topic: str | None,
        ) -> None:
            self.calls.append(("update", message_id, content, topic))

        def delete_message(self, message_id: int) -> None:
            self.calls.append(("delete", message_id))

        def close(self) -> None:
            return None

    async def run() -> None:
        store = FakeStore()
        stream_uuid = stable_stream_uuid(ACCOUNT_UUID, "channel:9")
        topic_uuid = stable_topic_uuid(stream_uuid, "Now")
        store.streams[stream_uuid] = StreamLink(_account(), "channel:9")
        store.topics[topic_uuid] = "Now"
        client = FakeClient()
        forwarder = WorkspaceRealtimeForwarder(  # type: ignore[arg-type]
            store,
            Settings(),
        )
        forwarder._client = lambda _account: client  # type: ignore[method-assign]
        payload = {
            "uuid": str(MESSAGE_UUID),
            "stream_uuid": str(stream_uuid),
            "topic_uuid": str(topic_uuid),
            "payload": {"kind": "markdown", "content": "first"},
            "source_name": "native",
        }
        frame = {
            "uuid": "10000000-0000-0000-0000-000000000005",
            "object_type": "message",
            "action": "created",
            "payload": payload,
        }

        await forwarder.apply(frame)
        payload["payload"] = {"kind": "markdown", "content": "edited"}
        frame["action"] = "updated"
        await forwarder.apply(frame)
        frame["action"] = "deleted"
        await forwarder.apply(frame)

        assert client.calls == [
            (
                "send",
                "channel:9",
                42,
                "first",
                "Now",
                "queue-1",
                str(MESSAGE_UUID),
            ),
            ("update", 71, "edited", "Now"),
            ("delete", 71),
        ]
        assert MESSAGE_UUID not in store.messages

    asyncio.run(run())


def test_workspace_message_uses_event_topic_name_for_a_new_topic() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.topic: str | None = None

        def own_user_id(self) -> int:
            return 42

        def send_message(
            self,
            chat_key: str,
            own_user_id: int,
            content: str,
            *,
            topic: str | None,
            queue_id: str,
            local_id: str,
        ) -> int:
            del chat_key, own_user_id, content, queue_id, local_id
            self.topic = topic
            return 72

        def close(self) -> None:
            return None

    async def run() -> None:
        store = FakeStore()
        stream_uuid = stable_stream_uuid(ACCOUNT_UUID, "channel:9")
        topic_uuid = UUID("10000000-0000-0000-0000-000000000008")
        store.streams[stream_uuid] = StreamLink(_account(), "channel:9")
        client = FakeClient()
        forwarder = WorkspaceRealtimeForwarder(  # type: ignore[arg-type]
            store,
            Settings(),
        )
        forwarder._client = lambda _account: client  # type: ignore[method-assign]
        frame = {
            "object_type": "message",
            "action": "created",
            "payload": {
                "uuid": str(MESSAGE_UUID),
                "stream_uuid": str(stream_uuid),
                "topic_uuid": str(topic_uuid),
                "topic_name": "Workspace topic",
                "payload": {"kind": "markdown", "content": "first"},
                "source_name": "native",
            },
        }

        await forwarder.apply(frame)

        assert client.topic == "Workspace topic"
        assert store.topics[topic_uuid] == "Workspace topic"

    asyncio.run(run())


def test_workspace_message_rejects_an_unmapped_topic_without_a_name() -> None:
    async def run() -> None:
        store = FakeStore()
        stream_uuid = stable_stream_uuid(ACCOUNT_UUID, "channel:9")
        store.streams[stream_uuid] = StreamLink(_account(), "channel:9")
        frame = {
            "object_type": "message",
            "action": "created",
            "payload": {
                "uuid": str(MESSAGE_UUID),
                "stream_uuid": str(stream_uuid),
                "topic_uuid": "10000000-0000-0000-0000-000000000008",
                "payload": {"kind": "markdown", "content": "first"},
                "source_name": "native",
            },
        }

        with pytest.raises(ValueError, match="topic_name"):
            await WorkspaceRealtimeForwarder(  # type: ignore[arg-type]
                store,
                Settings(),
            ).apply(frame)

    asyncio.run(run())
