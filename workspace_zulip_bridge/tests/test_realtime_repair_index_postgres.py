# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Keep an idle realtime worker independent of historical table volume."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from workspace_zulip_bridge.database import prepare_database
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import PROVIDER
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import REALM
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import setup
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker


def test_live_topic_repair_uses_partial_index_with_history() -> None:
    asyncio.run(_live_topic_repair_uses_partial_index_with_history())


async def _live_topic_repair_uses_partial_index_with_history() -> None:
    pool, settings = await setup()
    try:
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs
                (provider_uuid, entity_type, entity_uuid, realm_uuid,
                 direction, delivery_priority, processing_status,
                 source_updated_at)
            SELECT $1, 'messages', md5(value::text)::uuid, $2,
                   'to_workspace', 1,
                   CASE WHEN value % 2 = 0 THEN 'applied' ELSE 'pending' END,
                   clock_timestamp()
            FROM generate_series(1, 20000) AS value
            """,
            PROVIDER,
            REALM,
        )
        # Exercise the upgrade of an existing table, then its idempotent restart.
        await pool.execute(
            "DROP INDEX workspace_zulip_bridge.sync_diffs_live_topic_repair_idx"
        )
        await prepare_database(pool)
        await prepare_database(pool)
        await pool.execute("ANALYZE workspace_zulip_bridge.sync_diffs")
        for scope, entity_types in (
            ("unpartitioned", None),
            ("partitioned", frozenset({"messages"})),
        ):
            worker = WorkspaceDiffWorker(
                pool,
                settings,
                plan_enabled=False,
                delivery_priority=0,
                scope=scope,
                entity_types=entity_types,
                partition=0,
                partition_count=2,
            )
            capture = AsyncMock(return_value=[])
            worker._pool = SimpleNamespace(fetch=capture)
            async with httpx.AsyncClient() as client:
                assert (
                    await worker._repair_blocked_workspace_topic_dependencies(client)
                    == 0
                )
            query, *parameters = capture.call_args.args
            async with pool.acquire() as connection:
                plan = json.loads(
                    await connection.fetchval(
                        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query,
                        *parameters,
                    )
                )
            serialized = json.dumps(plan)
            assert "sync_diffs_live_topic_repair_idx" in serialized
            pending_nodes = [plan[0]["Plan"]]
            while pending_nodes:
                node = pending_nodes.pop()
                if node.get("Relation Name") == "sync_diffs":
                    assert node["Node Type"] != "Seq Scan"
                pending_nodes.extend(node.get("Plans", []))
            assert plan[0]["Plan"]["Actual Rows"] == 0
    finally:
        await pool.close()
