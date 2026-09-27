# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from typing import Any
from typing import cast

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.event_processor import ZulipEventProcessor


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
