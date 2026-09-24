# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from importlib import resources

import asyncpg

from workspace_zulip_bridge.config import Settings

_EVENT_QUEUE_BACKFILL_MIGRATION = "2026-09-24-zulip-event-queue-registry"
_SCHEMA_PREPARATION_TIMEOUT_SECONDS = 3600.0


async def open_pool(settings: Settings) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        dsn=settings.database_dsn,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        command_timeout=settings.db_command_timeout_seconds,
        max_inactive_connection_lifetime=300.0,
        server_settings={
            "application_name": "workspace-zulip-bridge",
            "timezone": "UTC",
            "default_text_search_config": "pg_catalog.simple",
        },
    )
    return pool


async def prepare_database(pool: asyncpg.Pool) -> None:
    upgrades = (
        resources.files("workspace_zulip_bridge")
        .joinpath("schema_upgrades.sql")
        .read_text(encoding="utf-8")
    )
    schema = (
        resources.files("workspace_zulip_bridge")
        .joinpath("schema.sql")
        .read_text(encoding="utf-8")
    )
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('workspace_zulip_bridge:schema', 0))"
        )
        await connection.execute(
            upgrades,
            timeout=_SCHEMA_PREPARATION_TIMEOUT_SECONDS,
        )
        await connection.execute(
            schema,
            timeout=_SCHEMA_PREPARATION_TIMEOUT_SECONDS,
        )
        queue_registry_backfilled = await connection.fetchval(
            "SELECT EXISTS ("
            "SELECT 1 FROM workspace_zulip_bridge.maintenance_migrations "
            "WHERE name = $1)",
            _EVENT_QUEUE_BACKFILL_MIGRATION,
        )
        if not queue_registry_backfilled:
            await connection.execute(
                "DROP INDEX IF EXISTS "
                "workspace_zulip_bridge.zulip_events_pending_scope_idx"
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_event_queues
                    (zulip_connection_uuid, queue_id, created_at)
                SELECT event.zulip_connection_uuid, event.queue_id,
                       min(event.created_at)
                FROM workspace_zulip_bridge.zulip_events AS event
                GROUP BY event.zulip_connection_uuid, event.queue_id
                ON CONFLICT (zulip_connection_uuid, queue_id) DO NOTHING
                """,
                timeout=_SCHEMA_PREPARATION_TIMEOUT_SECONDS,
            )
            await connection.execute(
                "INSERT INTO workspace_zulip_bridge.maintenance_migrations (name) "
                "VALUES ($1) ON CONFLICT (name) DO NOTHING",
                _EVENT_QUEUE_BACKFILL_MIGRATION,
            )
        await connection.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_events
            SET processing_status = 'pending', claimed_at = NULL,
                updated_at = clock_timestamp()
            WHERE processing_status = 'processing'
            """
        )
        await connection.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET processing_status = 'pending', claimed_at = NULL,
                available_at = clock_timestamp(), updated_at = clock_timestamp()
            WHERE processing_status = 'processing'
            """
        )


async def probe_database(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        await connection.execute("SELECT 1")
