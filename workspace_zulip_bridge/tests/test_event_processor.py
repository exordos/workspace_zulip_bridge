# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import UUID

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.event_processor import ZulipEventProcessor
from workspace_zulip_bridge.message_history import message_content_hash
from workspace_zulip_bridge.models import ZulipFileMetadata
from workspace_zulip_bridge.models import ZulipMessage
from workspace_zulip_bridge.stable_ids import stable_file_uuid


def test_claim_scope_uses_a_plan_specific_timestamp_predicate() -> None:
    settings = Settings.from_env({})
    processors = {
        scope: ZulipEventProcessor(
            cast(Any, object()),
            cast(Any, object()),
            settings,
            claim_scope=scope,
        )
        for scope in ("all", "backlog", "realtime")
    }

    assert processors["all"]._claim_scope_clause("event") == (
        "$5::double precision > 0"
    )
    assert processors["backlog"]._claim_scope_clause("event") == (
        "event.created_at < "
        "(statement_timestamp() - make_interval(secs => $5::double precision))"
    )
    assert processors["realtime"]._claim_scope_clause("event") == (
        "event.created_at >= "
        "(statement_timestamp() - make_interval(secs => $5::double precision))"
    )


def test_new_message_reuses_an_already_finalized_file_projection() -> None:
    async def run() -> None:
        endpoint = "https://zulip.example.test"
        source_path = "/user_uploads/a/test.png"
        source_uuid = stable_file_uuid(endpoint, source_path)
        workspace_urn = "urn:image:11111111-2222-4333-8444-555555555555"
        pool = SimpleNamespace(
            fetch=AsyncMock(
                return_value=[
                    {
                        "chat_key": "channel:7",
                        "file_uuid": source_uuid,
                        "workspace_urn": workspace_urn,
                    }
                ]
            )
        )
        processor = ZulipEventProcessor(
            cast(Any, pool),
            cast(Any, object()),
            Settings.from_env({}),
        )
        sender_uuid = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
        placeholder = f"![test.png](urn:file:{source_uuid})"
        message = ZulipMessage(
            message_id=42,
            chat_key="channel:7",
            topic_name="test",
            sender_user_uuid=sender_uuid,
            content="![test.png](/user_uploads/a/test.png)",
            workspace_content=placeholder,
            is_read=False,
            is_starred=False,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json="[]",
            message_hash=b"m" * 32,
            content_hash=b"c" * 32,
            sent_at=123,
            files=(ZulipFileMetadata(source_path, "test.png"),),
        )

        (resolved,) = await processor._replace_finalized_file_urns_in_messages(
            endpoint,
            (message,),
        )

        assert resolved.workspace_content == f"![test.png]({workspace_urn})"
        assert resolved.content_hash == message_content_hash(
            sender_user_uuid=sender_uuid,
            chat_key="channel:7",
            topic_name="test",
            content=f"![test.png]({workspace_urn})",
            sent_at=123,
        )
        pool.fetch.assert_awaited_once()

    asyncio.run(run())


def test_processing_timeout_keeps_processor_alive() -> None:
    async def run() -> None:
        processor = ZulipEventProcessor(
            cast(Any, object()),
            cast(Any, object()),
            Settings.from_env({}),
            claim_scope="realtime",
        )
        calls = 0

        async def process_once() -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError
            raise asyncio.CancelledError

        processor.process_once = process_once  # type: ignore[method-assign]
        with patch(
            "workspace_zulip_bridge.event_processor.asyncio.sleep",
            new_callable=AsyncMock,
        ) as sleep:
            try:
                await processor.run()
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError("processor cancellation was swallowed")

        assert calls == 2
        sleep.assert_awaited_once_with(1.0)

    asyncio.run(run())


def test_cleanup_timeout_keeps_processor_alive_and_reduces_batch() -> None:
    async def run() -> None:
        processor = ZulipEventProcessor(
            cast(Any, object()),
            cast(Any, object()),
            Settings.from_env({"WZB_EVENT_CLEANUP_BATCH_SIZE": "10000"}),
        )

        async def timeout() -> int:
            raise TimeoutError

        processor.cleanup_expired_events = timeout  # type: ignore[method-assign]
        assert await processor._maybe_cleanup_expired_events() == 0
        assert processor._cleanup_batch_size == 5000
        assert processor._next_cleanup_at > 0

    asyncio.run(run())


def test_cleanup_cancellation_is_not_swallowed() -> None:
    async def run() -> None:
        processor = ZulipEventProcessor(
            cast(Any, object()),
            cast(Any, object()),
            Settings.from_env({}),
        )

        async def cancel() -> int:
            raise asyncio.CancelledError

        processor.cleanup_expired_events = cancel  # type: ignore[method-assign]
        try:
            await processor._maybe_cleanup_expired_events()
        except asyncio.CancelledError:
            return
        raise AssertionError("cleanup cancellation was swallowed")

    asyncio.run(run())
