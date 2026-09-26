#!/usr/bin/env python3
# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Benchmark durable Workspace event ingestion in a disposable database."""

import argparse
import asyncio
import json
import os
import time
from uuid import UUID

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.database import open_pool
from workspace_zulip_bridge.database import prepare_database
from workspace_zulip_bridge.workspace_events import WorkspaceEvent
from workspace_zulip_bridge.workspace_events import WorkspaceEventStore

PROVIDER_UUID = UUID("10000000-0000-0000-0000-000000000001")
PROJECT_UUID = UUID("10000000-0000-0000-0000-000000000002")
GENERATION = UUID("10000000-0000-0000-0000-000000000003")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--batch-size", type=int, default=500)
    return parser.parse_args()


def _event(index: int) -> WorkspaceEvent:
    event_uuid = UUID(f"20000000-0000-0000-0000-{index:012d}")
    entity_uuid = UUID(f"30000000-0000-0000-0000-{index:012d}")
    frame = {
        "schema_version": 1,
        "uuid": str(event_uuid),
        "epoch_version": index,
        "project_id": str(PROJECT_UUID),
        "user_uuid": str(PROVIDER_UUID),
        "object_type": "message",
        "action": "updated",
        "payload": {
            "kind": "message.updated",
            "uuid": str(entity_uuid),
            "payload": {"kind": "markdown", "content": f"message {index}"},
        },
    }
    return WorkspaceEvent(
        uuid=event_uuid,
        epoch_version=index,
        object_type="message",
        action="updated",
        entity_uuid=entity_uuid,
        frame=frame,
    )


async def _run(event_count: int, batch_size: int) -> None:
    dsn = os.environ.get("WZB_BENCHMARK_DATABASE_DSN")
    if not dsn or not any(marker in dsn for marker in ("test", "benchmark")):
        raise RuntimeError(
            "WZB_BENCHMARK_DATABASE_DSN must name an explicit disposable test database"
        )
    settings = Settings.from_env(
        {
            "WZB_DATABASE_DSN": dsn,
            "WZB_DB_POOL_MIN_SIZE": "1",
            "WZB_DB_POOL_MAX_SIZE": "4",
            "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
        }
    )
    pool = await open_pool(settings)
    await prepare_database(pool)
    async with pool.acquire() as connection:
        await connection.execute(
            """
            TRUNCATE workspace_zulip_bridge.workspace_events,
                     workspace_zulip_bridge.workspace_event_cursors
            """
        )
    store = WorkspaceEventStore(pool)
    await store.cursor(PROVIDER_UUID, PROJECT_UUID)
    started = time.perf_counter()
    inserted = 0
    for offset in range(1, event_count + 1, batch_size):
        batch = [
            _event(index)
            for index in range(offset, min(offset + batch_size, event_count + 1))
        ]
        inserted += await store.persist(
            PROVIDER_UUID,
            PROJECT_UUID,
            GENERATION,
            batch,
        )
    elapsed = time.perf_counter() - started
    async with pool.acquire() as connection:
        table_bytes = await connection.fetchval(
            """
            SELECT pg_total_relation_size(
                'workspace_zulip_bridge.workspace_events'
            )
            """
        )
    print(
        json.dumps(
            {
                "events_requested": event_count,
                "events_inserted": inserted,
                "batch_size": batch_size,
                "elapsed_seconds": round(elapsed, 6),
                "events_per_second": round(event_count / elapsed, 2),
                "table_bytes": table_bytes,
                "bytes_per_event": round(table_bytes / event_count, 2),
            },
            sort_keys=True,
        )
    )
    await pool.close()


def main() -> None:
    args = _arguments()
    if args.events < 1 or args.batch_size < 1:
        raise ValueError("events and batch size must be positive")
    asyncio.run(_run(args.events, args.batch_size))


if __name__ == "__main__":
    main()
