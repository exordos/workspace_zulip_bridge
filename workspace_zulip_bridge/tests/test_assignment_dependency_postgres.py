# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""A partial backend assignment must not terminate realtime delivery."""

import asyncio
import json

import httpx
import pytest

from workspace_zulip_bridge.tests.test_import_pipeline_postgres import ACCOUNT
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import PROVIDER
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import REALM
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import STREAM
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import TOPIC
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import messages
from workspace_zulip_bridge.tests.test_import_pipeline_postgres import setup
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker


@pytest.mark.parametrize(
    ("entity_type", "missing"),
    [
        ("messages", "topic"),
        ("topics", "topic"),
        ("messages", "stream"),
        ("streams", "stream"),
        ("messages", "invalid_topic"),
        ("streams", "invalid_stream"),
    ],
)
def test_incomplete_assignment_defers_live_diff(entity_type: str, missing: str) -> None:
    asyncio.run(_incomplete_assignment_defers_live_diff(entity_type, missing))


async def _incomplete_assignment_defers_live_diff(
    entity_type: str, missing: str
) -> None:
    pool, settings = await setup()
    try:
        await messages(pool, 1)
        entity_uuid = (
            await pool.fetchval("SELECT md5('message1')::uuid")
            if entity_type == "messages"
            else TOPIC
            if entity_type == "topics"
            else STREAM
        )
        projection = {
            "stream": {"uuid": str(STREAM)},
            "topics": [{"provider_topic_id": "1:Synthetic", "topic_uuid": str(TOPIC)}],
        }
        incomplete = json.loads(json.dumps(projection))
        if missing == "stream":
            del incomplete["stream"]
        elif missing == "topic":
            incomplete["topics"] = []
        elif missing == "invalid_stream":
            incomplete["stream"]["uuid"] = "pending"
        else:
            incomplete["topics"][0]["topic_uuid"] = "pending"
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
            SET assignment = $2::jsonb
            WHERE external_account_uuid = $1
            """,
            ACCOUNT,
            json.dumps({"workspace_projection": incomplete}),
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs
                (provider_uuid, entity_type, entity_uuid, realm_uuid, partition_key,
                 direction, source_hash, source_updated_at, delivery_priority)
            VALUES ($1, $2, $3, $4, $5, 'to_workspace', $6, clock_timestamp(), 0)
            """,
            PROVIDER,
            entity_type,
            entity_uuid,
            REALM,
            STREAM,
            b"m" * 32,
        )
        worker = WorkspaceDiffWorker(
            pool, settings, plan_enabled=False, delivery_priority=0
        )

        def unexpected_request(request: httpx.Request) -> httpx.Response:
            raise AssertionError("An incomplete assignment must not issue a write")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(unexpected_request)
        ) as client:
            assert await worker.process_once(client) == 1
            assert await worker.process_once(client) == 0
        state = await pool.fetchrow(
            """
            SELECT processing_status, attempt_count, dependency_wait_count,
                   claimed_at, last_error, available_at > clock_timestamp() AS delayed
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = $2 AND entity_uuid = $3
            """,
            PROVIDER,
            entity_type,
            entity_uuid,
        )
        assert dict(state) == {
            "processing_status": "pending",
            "attempt_count": 0,
            "dependency_wait_count": 1,
            "claimed_at": None,
            "last_error": "waiting_for_workspace_dependencies",
            "delayed": True,
        }
        assert worker._assignment_blocked_entities == {(entity_type, entity_uuid)}
        assert (entity_type, entity_uuid) not in worker._workspace_entity_ids
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
            SET assignment = $2::jsonb
            WHERE external_account_uuid = $1
            """,
            ACCOUNT,
            json.dumps({"workspace_projection": projection}),
        )
        worker._assignment_blocked_entities.clear()
        loaded = await worker._load_zulip_entities(entity_type, [entity_uuid])
        assert (entity_type, entity_uuid) in loaded
        assert not worker._assignment_blocked_entities
    finally:
        await pool.close()
