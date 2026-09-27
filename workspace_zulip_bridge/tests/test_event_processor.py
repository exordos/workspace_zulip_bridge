# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import patch

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.event_processor import ZulipEventProcessor


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
