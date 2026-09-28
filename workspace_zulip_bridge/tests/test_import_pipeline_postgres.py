# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Recovery and stage guarantees against a disposable PostgreSQL database."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import httpx
import pytest

from workspace_zulip_bridge import workspace_file_transfer
from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.import_pipeline import historical_files_runnable
from workspace_zulip_bridge.monitor import collect_import_pipeline
from workspace_zulip_bridge.tests.test_postgres import _dsn
from workspace_zulip_bridge.tests.test_postgres import _pool
from workspace_zulip_bridge.workspace_chat_catalog import WorkspaceChatCatalogWorker
from workspace_zulip_bridge.workspace_file_transfer import CATALOG_PROJECTION_REVISION
from workspace_zulip_bridge.workspace_file_transfer import WorkspaceFileTransferWorker
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker

REALM, USER, STREAM, TOPIC, ACCOUNT, PROVIDER, PROJECT, GENERATION = (
    UUID(int=number) for number in range(1, 9)
)


async def setup() -> tuple[asyncpg.Pool, Settings]:
    dsn = _dsn()
    pool = await _pool(dsn)
    await pool.execute(
        "UPDATE workspace_zulip_bridge.import_scan_cursors SET sequence = 0"
    )
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_realms
            (uuid, identity_key, endpoint, workspace_provider_uuid, workspace_project_id)
        VALUES ($1, 'synthetic', 'synthetic', $2, $3)
        """,
        REALM,
        PROVIDER,
        PROJECT,
    )
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_users
            (uuid, realm_uuid, zulip_user_id, login, full_name, role)
        VALUES ($1, $2, 1, 'synthetic', 'Synthetic', 100)
        """,
        USER,
        REALM,
    )
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_connections
            (uuid, realm_uuid, zulip_user_uuid, external_account_uuid, login, api_key,
             lifecycle_status)
        VALUES ($1, $2, $1, $3, 'synthetic', 'synthetic', 'active')
        """,
        USER,
        REALM,
        ACCOUNT,
    )
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_streams
            (uuid, realm_uuid, chat_type, chat_key, name, content_hash, source_connection_uuid)
        VALUES ($1, $2, 'channel', 'channel:1', 'Synthetic', $3, $4)
        """,
        STREAM,
        REALM,
        b"s" * 32,
        USER,
    )
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_topics (uuid, zulip_stream_uuid, name, content_hash)
        VALUES ($1, $2, 'Synthetic', $3)
        """,
        TOPIC,
        STREAM,
        b"t" * 32,
    )
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_topic_catalog_identities
            (topic_uuid, zulip_stream_uuid, catalog_topic_key, provider_topic_id)
        VALUES ($1, $2, 'Synthetic', '1:Synthetic')
        """,
        TOPIC,
        STREAM,
    )
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.workspace_mirror_state
            (provider_uuid, workspace_project_id, active_generation, bootstrap_status)
        VALUES ($1, $2, $3, 'ready')
        """,
        PROVIDER,
        PROJECT,
        GENERATION,
    )
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.workspace_chat_catalog_reports
            (external_account_uuid, zulip_stream_uuid, resource_uuid, observed_generation,
             catalog, catalog_hash, report_uuid, report, processing_status,
             projection_revision, source_updated_at, assignment, assignment_reconciled)
        VALUES ($1, $2, gen_random_uuid(), 1, '{}', $3, gen_random_uuid(), '{}',
                'reported', $4, clock_timestamp(),
                jsonb_build_object('workspace_projection', jsonb_build_object('stream',
                  jsonb_build_object('uuid', $2::uuid::text), 'topics', jsonb_build_array(
                  jsonb_build_object('provider_topic_id', '1:Synthetic', 'topic_uuid', $5::uuid::text)))), true)
        """,
        ACCOUNT,
        STREAM,
        b"c" * 32,
        CATALOG_PROJECTION_REVISION,
        TOPIC,
    )
    return pool, Settings(
        database_dsn=dsn,
        workspace_control_url="synthetic",
        workspace_bridge_instance_uuid=UUID(int=9),
        workspace_provider_uuid=PROVIDER,
        workspace_project_id=PROJECT,
        workspace_token_file=Path("unused"),
    )


async def files(pool: asyncpg.Pool, count: int) -> None:
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_files
            (uuid, realm_uuid, owner_user_uuid, zulip_attachment_id, source_path,
             name, source_created_at, metadata_hash)
        SELECT md5('file' || n)::uuid, $1, $2, n, '/user_uploads/' || n,
               'synthetic', clock_timestamp(), $3 FROM generate_series(1, $4::int) n
        """,
        REALM,
        USER,
        b"f" * 32,
        count,
    )
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.workspace_outbox
            (realm_uuid, entity_type, action, entity_uuid)
        SELECT $1, 'file', 'upsert', md5('file' || n)::uuid
        FROM generate_series(1, $2::int) n
        """,
        REALM,
        count,
    )


async def messages(pool: asyncpg.Pool, count: int) -> None:
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_messages
            (uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid, topic_uuid,
             sender_user_uuid, zulip_message_id, content, workspace_content,
             content_hash, message_hash, created_at, source_updated_at)
        SELECT md5('message' || n)::uuid, $1, $2, $3, $4, $2, n, '', '', $5, $5,
               '2026-01-01'::timestamptz + make_interval(secs => n), clock_timestamp()
        FROM generate_series(1, $6::int) n
        """,
        REALM,
        USER,
        STREAM,
        TOPIC,
        b"m" * 32,
        count,
    )


async def projection(pool: asyncpg.Pool, number: int = 1) -> None:
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.workspace_file_projections
            (uuid, file_uuid, zulip_stream_uuid, operation_uuid)
        VALUES (md5('projection' || $1::text)::uuid, md5('file' || $1::text)::uuid,
                $2, md5('projection' || $1::text)::uuid)
        """,
        str(number),
        STREAM,
    )


async def diff(pool: asyncpg.Pool, entity_type: str, *, live: bool = False) -> None:
    await pool.execute(
        """
        INSERT INTO workspace_zulip_bridge.sync_diffs
            (provider_uuid, entity_type, entity_uuid, realm_uuid, direction,
             source_updated_at, delivery_priority)
        VALUES ($1, $2, md5($2)::uuid, $3, 'to_zulip', clock_timestamp(), $4)
        """,
        PROVIDER,
        entity_type,
        REALM,
        int(not live),
    )


def test_seed_cursor_survives_restart_beyond_unready_twenty_thousand() -> None:
    asyncio.run(_seed_cursor())


async def _seed_cursor() -> None:
    pool, settings = await setup()
    try:
        await files(pool, 20_001)
        await messages(pool, 2)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_messages SET workspace_content = NULL "
            "WHERE zulip_message_id = 1"
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_message_files (message_uuid, file_uuid, position)
            SELECT md5('message' || CASE WHEN n <= 20000 THEN 1 ELSE 2 END)::uuid,
                   md5('file' || n)::uuid, n FROM generate_series(1, 20001) n
            """
        )
        first = WorkspaceFileTransferWorker(pool, settings)
        assert await first._seed_projection_jobs() == 0
        assert await pool.fetchval(
            "SELECT sequence > 0 FROM workspace_zulip_bridge.import_scan_cursors "
            "WHERE name = 'file_projection_seed'"
        )
        # A new worker starts from the persisted checkpoint, not the same head.
        restarted = WorkspaceFileTransferWorker(pool, settings)
        assert await restarted._seed_projection_jobs() == 1
        assert await restarted._claim_job() is not None
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox "
                "WHERE delivery_status <> 'delivered'"
            )
            == 20_001
        )
    finally:
        await pool.close()


def test_historical_stage_switch_waits_for_inflight_and_realtime_is_separate() -> None:
    asyncio.run(_stage_switch())


async def _stage_switch() -> None:
    pool, settings = await setup()
    try:
        await files(pool, 2)
        await projection(pool)
        await projection(pool, 2)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_file_projections "
            "SET processing_status = 'failed', available_at = clock_timestamp() + interval '1 hour'"
        )
        assert not await historical_files_runnable(pool)
        await diff(pool, "topics")
        await diff(pool, "messages")
        await diff(pool, "message_flags", live=True)
        first = WorkspaceDiffWorker(pool, settings, delivery_priority=1)
        second = WorkspaceDiffWorker(pool, settings, delivery_priority=1)
        live = WorkspaceDiffWorker(pool, settings, delivery_priority=0)
        first._write_to_zulip = AsyncMock()  # type: ignore[method-assign]
        second._write_to_zulip = AsyncMock()  # type: ignore[method-assign]
        live._write_to_zulip = AsyncMock()  # type: ignore[method-assign]
        file_worker = WorkspaceFileTransferWorker(pool, settings)
        async with httpx.AsyncClient() as client:
            assert await first.process_once(client) == 1
            await pool.execute(
                "UPDATE workspace_zulip_bridge.workspace_file_projections "
                "SET available_at = clock_timestamp()"
            )
            # Retry became ready during the historical topic's network request.
            assert await file_worker._claim_job() is None
            assert await second.process_once(client) == 0
            assert await live.process_once(client) == 1
            await pool.execute(
                "UPDATE workspace_zulip_bridge.sync_diffs SET processing_status = 'applied' "
                "WHERE entity_type = 'topics'"
            )
            jobs = await asyncio.gather(
                file_worker._claim_job(), file_worker._claim_job()
            )
            assert all(jobs) and jobs[0] != jobs[1]
            assert await second.process_once(client) == 0
            for job in jobs:
                assert job is not None
                await file_worker._fail_job(job, "synthetic_terminal", retryable=False)
            assert not await historical_files_runnable(pool)
            assert await second.process_once(client) == 1
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_file_projections "
                "WHERE processing_status = 'blocked'"
            )
            == 2
        )
    finally:
        await pool.close()


def test_expired_file_lease_is_reclaimed_and_stale_failure_cannot_overwrite() -> None:
    asyncio.run(_lease_recovery())


async def _lease_recovery() -> None:
    pool, settings = await setup()
    try:
        await files(pool, 1)
        await projection(pool)
        worker = WorkspaceFileTransferWorker(pool, settings)
        old = await worker._claim_job()
        assert old is not None and old.claimed_at is not None
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_file_projections "
            "SET heartbeat_at = clock_timestamp() - interval '11 minutes'"
        )
        current = await worker._claim_job()
        assert current is not None and current.claimed_at != old.claimed_at
        await worker._fail_job(old, "stale", retryable=False)
        assert (
            await pool.fetchval(
                "SELECT processing_status FROM workspace_zulip_bridge.workspace_file_projections"
            )
            == "processing"
        )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_file_projections SET attempt_count = 12"
        )
        await worker._fail_job(current, "synthetic_retry", retryable=True)
        assert await worker._claim_job() is None
        assert (
            await pool.fetchval(
                "SELECT last_error FROM workspace_zulip_bridge.workspace_file_projections"
            )
            == "retry_exhausted:synthetic_retry"
        )
    finally:
        await pool.close()


def test_heartbeat_preserves_original_claim_and_prevents_reclaim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(workspace_file_transfer, "_FILE_HEARTBEAT_SECONDS", 0.01)
    asyncio.run(_heartbeat())


async def _heartbeat() -> None:
    pool, settings = await setup()
    try:
        await files(pool, 1)
        await projection(pool)
        worker = WorkspaceFileTransferWorker(pool, settings)
        job = await worker._claim_job()
        assert job is not None
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_file_projections "
            "SET heartbeat_at = clock_timestamp() - interval '11 minutes'"
        )
        task = asyncio.create_task(worker._heartbeat_job(job))
        try:
            async with asyncio.timeout(2):
                while not await pool.fetchval(  # noqa: ASYNC110 - observe the database lease
                    "SELECT heartbeat_at > clock_timestamp() - interval '1 minute' "
                    "FROM workspace_zulip_bridge.workspace_file_projections"
                ):
                    await asyncio.sleep(0.01)
            assert await worker._claim_job() is None
            assert (
                await pool.fetchval(
                    "SELECT claimed_at FROM workspace_zulip_bridge.workspace_file_projections"
                )
                == job.claimed_at
            )
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        await pool.close()


def test_assignment_repair_scans_matching_prefix_and_resumes_current_revision() -> None:
    asyncio.run(_assignment_repair())


async def _assignment_repair() -> None:
    pool, settings = await setup()
    try:
        await messages(pool, 2001)
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_messages
                (provider_uuid, snapshot_generation, uuid, workspace_project_id,
                 content_hash, source_updated_at, data)
            SELECT $1, $2, uuid, $3, content_hash, source_updated_at,
                   jsonb_build_object('stream_uuid', $4::uuid::text, 'topic_uuid', $5::uuid::text)
            FROM workspace_zulip_bridge.zulip_messages WHERE zulip_message_id > 1
            """,
            PROVIDER,
            GENERATION,
            PROJECT,
            STREAM,
            TOPIC,
        )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports "
            "SET assignment_reconciled = false, assignment_repair_stage = 4, projection_revision = 1"
        )
        worker = WorkspaceChatCatalogWorker(pool, settings)
        assert await worker._requeue_assignment_messages() == 0
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports SET projection_revision = $1",
            CATALOG_PROJECTION_REVISION,
        )
        assert await worker._requeue_assignment_messages() == 1000
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox"
            )
            == 0
        )
        restarted = WorkspaceChatCatalogWorker(pool, settings)
        assert await restarted._requeue_assignment_messages() == 1000
        assert await restarted._requeue_assignment_messages() == 1
        assert await restarted._requeue_assignment_messages() == 1
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox"
            )
            == 1
        )
        assert await pool.fetchval(
            "SELECT assignment_reconciled FROM workspace_zulip_bridge.workspace_chat_catalog_reports"
        )
    finally:
        await pool.close()


def test_blocked_completion_and_metrics_remain_honest() -> None:
    asyncio.run(_completion_metrics())


async def _completion_metrics() -> None:
    pool, settings = await setup()
    try:
        worker = WorkspaceDiffWorker(pool, settings)
        await files(pool, 1)
        await projection(pool)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_file_projections SET processing_status = 'blocked'"
        )
        assert not await worker._complete_initial_sync()
        snapshot = await collect_import_pipeline(pool, window_seconds=60)
        assert snapshot["files"][0]["wait_reason"] == "terminal"
        assert snapshot["files"][0]["count"] == 1
        assert snapshot["file_confirmations"]["files_per_second"] == 0
        assert snapshot["outbox"][0]["count"] == 1
        await pool.execute(
            "DELETE FROM workspace_zulip_bridge.workspace_file_projections"
        )
        assert not await worker._complete_initial_sync()  # unseeded outbox still exists
        await pool.execute("DELETE FROM workspace_zulip_bridge.workspace_outbox")
        await diff(pool, "messages")
        await pool.execute(
            "UPDATE workspace_zulip_bridge.sync_diffs "
            "SET processing_status = 'blocked', direction = 'to_workspace'"
        )
        assert not await worker._complete_initial_sync()
        await pool.execute(
            "UPDATE workspace_zulip_bridge.sync_diffs SET processing_status = 'applied'"
        )
        assert await worker._complete_initial_sync()
    finally:
        await pool.close()


def test_file_completion_sweep_passes_unfinished_prefix_without_false_ack() -> None:
    asyncio.run(_completion_sweep())


async def _completion_sweep() -> None:
    pool, settings = await setup()
    try:
        await files(pool, 101)
        await messages(pool, 1)
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_message_files (message_uuid, file_uuid, position)
            SELECT md5('message1')::uuid, md5('file' || n)::uuid, n
            FROM generate_series(1, 101) n
            """
        )
        await projection(pool, 101)
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_file_projections
            SET processing_status = 'finalized', workspace_urn = 'urn:file:' || uuid,
                content_type = 'text/plain', size_bytes = 1, sha256 = repeat('a', 64),
                finalized_at = clock_timestamp()
            """
        )
        worker = WorkspaceFileTransferWorker(pool, settings)
        assert await worker._complete_file_outbox() == 0
        assert await worker._complete_file_outbox() == 1
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox "
                "WHERE delivery_status = 'pending'"
            )
            == 100
        )
        message_uuid = await pool.fetchval("SELECT md5('message1')::uuid")
        diff_worker = WorkspaceDiffWorker(pool, settings)
        assert (
            await diff_worker._load_file_ready_message_ids(
                [
                    (
                        {"entity_type": "messages", "entity_uuid": message_uuid},
                        {},
                        b"",
                        {"action": "upsert"},
                    )
                ]  # type: ignore[list-item]
            )
            == set()
        )
    finally:
        await pool.close()


def test_assignment_repair_requeues_wrong_topic_with_correct_stream() -> None:
    asyncio.run(_topic_repair())


async def _topic_repair() -> None:
    pool, settings = await setup()
    try:
        await messages(pool, 1)
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_messages
                (provider_uuid, snapshot_generation, uuid, workspace_project_id,
                 content_hash, source_updated_at, data)
            SELECT $1, $2, uuid, $3, content_hash, source_updated_at,
                   jsonb_build_object('stream_uuid', $4::uuid::text,
                     'topic_uuid', gen_random_uuid()::text)
            FROM workspace_zulip_bridge.zulip_messages
            """,
            PROVIDER,
            GENERATION,
            PROJECT,
            STREAM,
        )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports "
            "SET assignment_reconciled = false, assignment_repair_stage = 4"
        )
        assert (
            await WorkspaceChatCatalogWorker(
                pool, settings
            )._requeue_assignment_messages()
            == 1
        )
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox WHERE entity_type = 'message'"
            )
            == 1
        )
    finally:
        await pool.close()


def test_malformed_assignment_is_deferred_without_starving_other_reports() -> None:
    asyncio.run(_repair_failure())


async def _repair_failure() -> None:
    pool, settings = await setup()
    try:
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_streams
                (uuid, realm_uuid, chat_type, chat_key, name, content_hash, source_connection_uuid)
            SELECT $1, realm_uuid, chat_type, 'channel:2', name, content_hash, source_connection_uuid
            FROM workspace_zulip_bridge.zulip_streams WHERE uuid = $2
            """,
            UUID(int=20),
            STREAM,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_chat_catalog_reports
                (external_account_uuid, zulip_stream_uuid, resource_uuid, observed_generation,
                 catalog, catalog_hash, report_uuid, report, processing_status, projection_revision,
                 source_updated_at, assignment, assignment_reconciled)
            SELECT external_account_uuid, $1, gen_random_uuid(), observed_generation,
                   catalog, catalog_hash, gen_random_uuid(), report, processing_status, projection_revision,
                   source_updated_at, assignment, false
            FROM workspace_zulip_bridge.workspace_chat_catalog_reports
            """,
            UUID(int=20),
        )
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
            SET assignment_reconciled = false,
                updated_at = clock_timestamp() - interval '1 day',
                assignment = jsonb_set(assignment, '{workspace_projection,stream,uuid}', '"invalid"')
            WHERE zulip_stream_uuid = $1
            """,
            STREAM,
        )
        worker = WorkspaceChatCatalogWorker(pool, settings)
        assert await worker._requeue_assignment_messages() == 1
        with pytest.raises(ValueError):
            await worker._requeue_assignment_messages()
        assert await worker._requeue_assignment_messages() == 1
        assert (
            await pool.fetchval(
                "SELECT assignment_repair_stage FROM workspace_zulip_bridge.workspace_chat_catalog_reports "
                "WHERE zulip_stream_uuid = $1",
                UUID(int=20),
            )
            == 2
        )
        assert (
            await pool.fetchval(
                "SELECT assignment_repair_last_error FROM workspace_zulip_bridge.workspace_chat_catalog_reports "
                "WHERE zulip_stream_uuid = $1",
                STREAM,
            )
            == "ValueError"
        )
    finally:
        await pool.close()


def test_initial_watermark_keeps_old_live_failures_but_allows_new_live_work() -> None:
    asyncio.run(_watermark())


async def _watermark() -> None:
    pool, settings = await setup()
    try:
        await files(pool, 1)
        await projection(pool)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_file_projections "
            "SET delivery_priority = 0, processing_status = 'failed'"
        )
        await diff(pool, "messages", live=True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.sync_diffs "
            "SET processing_status = 'failed', direction = 'to_workspace'"
        )
        worker = WorkspaceDiffWorker(pool, settings)
        assert not await worker._complete_initial_sync()
        watermark = await pool.fetchval(
            "SELECT initial_sync_watermark_at FROM workspace_zulip_bridge.workspace_mirror_state"
        )
        await pool.execute(
            "DELETE FROM workspace_zulip_bridge.workspace_file_projections"
        )
        await pool.execute("DELETE FROM workspace_zulip_bridge.workspace_outbox")
        assert not await worker._complete_initial_sync()  # old priority-0 failure
        await pool.execute(
            "UPDATE workspace_zulip_bridge.sync_diffs SET processing_status = 'applied'"
        )
        # Late catalog repair is historical even when queued after the boundary.
        await pool.execute(
            "INSERT INTO workspace_zulip_bridge.workspace_outbox "
            "(realm_uuid, entity_type, action, entity_uuid, import_required) "
            "VALUES ($1, 'message', 'upsert', gen_random_uuid(), true)",
            REALM,
        )
        assert not await worker._complete_initial_sync()
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_outbox SET delivery_status = 'delivered'"
        )
        # New realtime file, outbox, diff, and event work arrive after the boundary.
        await projection(pool)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_file_projections SET delivery_priority = 0"
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_outbox (realm_uuid, entity_type, action, entity_uuid)
            VALUES ($1, 'message', 'upsert', gen_random_uuid())
            """,
            REALM,
        )
        await diff(pool, "message_flags", live=True)
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_events
                (uuid, provider_uuid, workspace_project_id, epoch_version, object_type, action, payload)
            VALUES (gen_random_uuid(), $1, $2, 1, 'message', 'updated', '{}')
            """,
            PROVIDER,
            PROJECT,
        )
        assert await worker._complete_initial_sync()
        assert (
            await pool.fetchval(
                "SELECT initial_sync_watermark_at FROM workspace_zulip_bridge.workspace_mirror_state"
            )
            == watermark
        )
    finally:
        await pool.close()


def test_assignment_change_invalidates_completion_without_moving_watermark() -> None:
    asyncio.run(_assignment_boundary())


async def _assignment_boundary() -> None:
    from dataclasses import replace

    from workspace_zulip_bridge.workspace_control import WorkspaceControlWorker

    pool, settings = await setup()
    try:
        diff_worker = WorkspaceDiffWorker(pool, settings)
        assert await diff_worker._complete_initial_sync()
        boundary = await pool.fetchval(
            "SELECT initial_sync_watermark_at FROM workspace_zulip_bridge.workspace_mirror_state"
        )
        resource_uuid = await pool.fetchval(
            "SELECT resource_uuid FROM workspace_zulip_bridge.workspace_chat_catalog_reports"
        )
        worker = WorkspaceControlWorker(
            pool,
            replace(
                settings,
                workspace_control_bootstrap_url="synthetic",
                workspace_control_hostname="synthetic",
                workspace_realm_uuid=REALM,
                workspace_enrollment_secret_file=Path("unused"),
            ),
        )
        resource = {
            "uuid": str(resource_uuid),
            "external_account_uuid": str(ACCOUNT),
            "selected": True,
            "project_id": str(PROJECT),
            "generation": 1,
            "provider_chat": {"kind": "zulip"},
            "workspace_projection": {
                "stream": {"uuid": str(STREAM)},
                "topics": [
                    {"provider_topic_id": "1:Synthetic", "topic_uuid": str(TOPIC)}
                ],
            },
        }
        await worker._apply_chat_assignment(resource)
        row = await pool.fetchrow(
            "SELECT initial_sync_completed_at, initial_sync_watermark_at "
            "FROM workspace_zulip_bridge.workspace_mirror_state"
        )
        assert row is not None and row["initial_sync_completed_at"] is None
        assert row["initial_sync_watermark_at"] == boundary
        assert (
            not await diff_worker._complete_initial_sync()
        )  # mandatory repair remains
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_mirror_state "
            "SET initial_sync_completed_at = clock_timestamp()"
        )
        await worker._apply_chat_assignment(resource)
        assert await pool.fetchval(
            "SELECT initial_sync_completed_at IS NOT NULL "
            "FROM workspace_zulip_bridge.workspace_mirror_state"
        )  # duplicate assignment does not invalidate or reset progress
    finally:
        await pool.close()


def test_legacy_repair_checkpoint_resets_once_and_resumes_on_next_start() -> None:
    asyncio.run(_upgrade_checkpoint())


async def _upgrade_checkpoint() -> None:
    from workspace_zulip_bridge.database import prepare_database

    pool, _ = await setup()
    try:
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports "
            "SET assignment_repair_created_at = clock_timestamp(), assignment_repair_uuid = $1",
            UUID(int=23),
        )
        await pool.execute(
            "ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports "
            "DROP COLUMN assignment_repair_stage"
        )
        await prepare_database(pool)
        row = await pool.fetchrow(
            "SELECT assignment_reconciled, assignment_repair_created_at, assignment_repair_uuid "
            "FROM workspace_zulip_bridge.workspace_chat_catalog_reports"
        )
        assert row is not None and tuple(row) == (False, None, None)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports "
            "SET assignment_repair_stage = 4, assignment_repair_uuid = $1",
            UUID(int=23),
        )
        await prepare_database(pool)
        row = await pool.fetchrow(
            "SELECT assignment_repair_stage, assignment_repair_uuid "
            "FROM workspace_zulip_bridge.workspace_chat_catalog_reports"
        )
        assert row is not None and tuple(row) == (4, UUID(int=23))
    finally:
        await pool.close()


def test_historical_entity_stage_is_global_across_providers() -> None:
    asyncio.run(_global_stage())


async def _global_stage() -> None:
    from dataclasses import replace

    pool, settings = await setup()
    try:
        await diff(pool, "topics")
        await diff(pool, "messages")
        await pool.execute(
            "UPDATE workspace_zulip_bridge.sync_diffs SET provider_uuid = $1 WHERE entity_type = 'messages'",
            UUID(int=25),
        )
        first = WorkspaceDiffWorker(pool, settings, delivery_priority=1)
        other = WorkspaceDiffWorker(
            pool,
            replace(settings, workspace_provider_uuid=UUID(int=25)),
            delivery_priority=1,
        )
        first._write_to_zulip = AsyncMock()  # type: ignore[method-assign]
        other._write_to_zulip = AsyncMock()  # type: ignore[method-assign]
        async with httpx.AsyncClient() as client:
            assert await first.process_once(client) == 1
            assert await other.process_once(client) == 0
            await pool.execute(
                "UPDATE workspace_zulip_bridge.sync_diffs SET processing_status = 'applied' WHERE entity_type = 'topics'"
            )
            assert await other.process_once(client) == 1
    finally:
        await pool.close()


def test_explicit_recovery_preserves_failure_evidence_and_operation_identity() -> None:
    asyncio.run(_explicit_recovery())


async def _explicit_recovery() -> None:
    from workspace_zulip_bridge.import_recovery import retry_file_projections

    pool, settings = await setup()
    try:
        await files(pool, 1)
        await projection(pool)
        await pool.execute("TRUNCATE workspace_zulip_bridge.file_projection_recoveries")
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_file_projections "
            "SET processing_status = 'blocked', attempt_count = 47, last_error = 'synthetic_cause'"
        )
        row = await pool.fetchrow(
            "SELECT uuid, operation_uuid FROM workspace_zulip_bridge.workspace_file_projections"
        )
        assert row is not None
        assert (
            await retry_file_projections(pool, [row["uuid"]], reason="source_fix") == 1
        )
        assert (
            await pool.fetchval(
                "SELECT attempt_count FROM workspace_zulip_bridge.workspace_file_projections"
            )
            == 47
        )
        assert (
            await retry_file_projections(
                pool, [row["uuid"]], reason="source_fix", apply=True
            )
            == 1
        )
        assert (
            await retry_file_projections(
                pool, [row["uuid"]], reason="source_fix", apply=True
            )
            == 0
        )
        audit = await pool.fetchrow(
            "SELECT previous_status, previous_attempt_count, previous_error, recovery_reason "
            "FROM workspace_zulip_bridge.file_projection_recoveries"
        )
        assert audit is not None and tuple(audit) == (
            "blocked",
            47,
            "synthetic_cause",
            "source_fix",
        )
        job = await WorkspaceFileTransferWorker(pool, settings)._claim_job()
        assert job is not None and job.operation_uuid == row["operation_uuid"]
        with pytest.raises(ValueError):
            await retry_file_projections(
                pool, [UUID(int=i) for i in range(101)], reason="source_fix"
            )
    finally:
        await pool.close()


@pytest.mark.parametrize("correct_target", [False, True])
def test_assignment_repair_uses_reserved_identity_after_rename(
    correct_target: bool,
) -> None:
    asyncio.run(_reserved_identity_repair(correct_target))


async def _reserved_identity_repair(correct_target: bool) -> None:
    pool, settings = await setup()
    try:
        await messages(pool, 1)
        # The persisted catalog identity and assignment retain the original key.
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_topics "
            "SET name = 'Renamed', is_done = true WHERE uuid = $1",
            TOPIC,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_messages
                (provider_uuid, snapshot_generation, uuid, workspace_project_id,
                 content_hash, source_updated_at, data)
            SELECT $1, $2, uuid, $3, content_hash, source_updated_at,
                   jsonb_build_object('stream_uuid', $4::uuid::text,
                     'topic_uuid', $5::uuid::text)
            FROM workspace_zulip_bridge.zulip_messages
            """,
            PROVIDER,
            GENERATION,
            PROJECT,
            STREAM,
            TOPIC if correct_target else UUID(int=30),
        )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports "
            "SET assignment_reconciled = false, assignment_repair_stage = 4"
        )
        assert (
            await WorkspaceChatCatalogWorker(
                pool, settings
            )._requeue_assignment_messages()
            == 1
        )
        assert await pool.fetchval(
            "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox "
            "WHERE entity_type = 'message'"
        ) == int(not correct_target)
        assert (
            await pool.fetchval(
                "SELECT assignment_repair_stage "
                "FROM workspace_zulip_bridge.workspace_chat_catalog_reports"
            )
            == 5
        )
    finally:
        await pool.close()


@pytest.mark.parametrize("unbound_reservation", [False, True])
def test_assignment_repair_requires_bound_channel_identity(
    unbound_reservation: bool,
) -> None:
    asyncio.run(_missing_identity_repair(unbound_reservation))


async def _missing_identity_repair(unbound_reservation: bool) -> None:
    pool, settings = await setup()
    try:
        await messages(pool, 1)
        if unbound_reservation:
            await pool.execute(
                "UPDATE workspace_zulip_bridge.zulip_topic_catalog_identities "
                "SET topic_uuid = NULL WHERE topic_uuid = $1",
                TOPIC,
            )
        else:
            await pool.execute(
                "DELETE FROM workspace_zulip_bridge.zulip_topic_catalog_identities "
                "WHERE topic_uuid = $1",
                TOPIC,
            )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports "
            "SET assignment_reconciled = false, assignment_repair_stage = 4"
        )
        with pytest.raises(ValueError, match="catalog identity"):
            await WorkspaceChatCatalogWorker(
                pool, settings
            )._requeue_assignment_messages()
        row = await pool.fetchrow(
            """
            SELECT assignment_repair_stage, assignment_reconciled,
                   assignment_repair_created_at, assignment_repair_uuid,
                   assignment_repair_last_error,
                   assignment_repair_available_at > clock_timestamp() AS deferred
            FROM workspace_zulip_bridge.workspace_chat_catalog_reports
            """
        )
        assert row is not None
        assert row["assignment_repair_stage"] == 4
        assert not row["assignment_reconciled"]
        assert row["assignment_repair_created_at"] is None
        assert row["assignment_repair_uuid"] is None
        assert row["assignment_repair_last_error"] == "ValueError"
        assert row["deferred"]
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox "
                "WHERE entity_type = 'message'"
            )
            == 0
        )
    finally:
        await pool.close()
