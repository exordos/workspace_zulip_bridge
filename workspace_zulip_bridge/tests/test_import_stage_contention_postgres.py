# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Historical contention must leave shared pool capacity for realtime work."""

import asyncio
from unittest.mock import AsyncMock

import asyncpg
import httpx
import pytest

from workspace_zulip_bridge.tests.test_import_pipeline_postgres import diff
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import files
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import projection
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import setup
from workspace_zulip_bridge.workspace_file_transfer import WorkspaceFileTransferWorker
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker


@pytest.mark.parametrize("kind", ["file", "entity"])
def test_historical_contenders_release_small_pool_for_realtime(kind: str) -> None:
    asyncio.run(_contended_claim(kind))


async def _contended_claim(kind: str) -> None:
    initial_pool, settings = await setup()
    await initial_pool.close()
    pool = await asyncpg.create_pool(settings.database_dsn, min_size=2, max_size=2)
    holder = await asyncpg.connect(settings.database_dsn)
    transaction = holder.transaction()
    tasks: list[asyncio.Task[int]] = []
    locked = False
    try:
        await files(pool, 1)
        await projection(pool)
        await diff(pool, "topics")
        await diff(pool, "messages")
        await diff(pool, "message_flags", live=True)
        if kind == "entity":
            await pool.execute(
                "UPDATE workspace_zulip_bridge.workspace_file_projections "
                "SET available_at = clock_timestamp() + interval '1 hour'"
            )
        file_worker = WorkspaceFileTransferWorker(pool, settings)
        # Coordinator progress must not turn a missed claim into a busy retry.
        file_worker._seed_projection_jobs = AsyncMock(return_value=1)  # type: ignore[method-assign]
        file_worker._complete_file_outbox = AsyncMock(return_value=0)  # type: ignore[method-assign]
        historical = WorkspaceDiffWorker(pool, settings, delivery_priority=1)
        realtime = WorkspaceDiffWorker(pool, settings, delivery_priority=0)
        historical._write_to_zulip = AsyncMock()  # type: ignore[method-assign]
        realtime._write_to_zulip = AsyncMock()  # type: ignore[method-assign]
        await transaction.start()
        locked = True
        await holder.execute(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('workspace_zulip_bridge:historical-stage', 0))"
        )
        async with httpx.AsyncClient() as client:
            started = asyncio.get_running_loop().time()
            for _ in range(8):
                operation = (
                    file_worker.process_once()
                    if kind == "file"
                    else historical._drain(client)
                )
                tasks.append(asyncio.create_task(operation))
            # Queue contenders first. A blocking advisory lock consumes both
            # pool slots here and prevents this independent live claim.
            await asyncio.sleep(0.02)
            assert await asyncio.wait_for(realtime.process_once(client), 1) == 1
            assert await asyncio.wait_for(pool.fetchval("SELECT 1"), 1) == 1
            outcomes = await asyncio.wait_for(asyncio.gather(*tasks), 1)
            assert outcomes == ([1] * 8 if kind == "file" else [0] * 8)
            if kind == "file":
                assert asyncio.get_running_loop().time() - started >= 0.08
            assert (
                await pool.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.sync_diffs "
                    "WHERE delivery_priority = 1 AND processing_status = 'processing'"
                )
                == 0
            )
            assert (
                await pool.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.workspace_file_projections "
                    "WHERE processing_status = 'processing'"
                )
                == 0
            )
            await transaction.rollback()
            locked = False
            if kind == "file":
                assert await file_worker._claim_job() is not None
                assert await historical.process_once(client) == 0
            else:
                assert await historical.process_once(client) == 1
                await pool.execute(
                    "UPDATE workspace_zulip_bridge.workspace_file_projections "
                    "SET available_at = clock_timestamp()"
                )
                assert await file_worker._claim_job() is None
                assert await historical.process_once(client) == 0
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if locked:
            await transaction.rollback()
        await holder.close()
        await pool.close()
