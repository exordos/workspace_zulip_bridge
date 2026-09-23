# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from importlib import resources

import asyncpg

from workspace_zulip_bridge.config import Settings


async def open_pool(settings: Settings) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=settings.database_dsn,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        command_timeout=settings.db_command_timeout_seconds,
        max_inactive_connection_lifetime=300.0,
        server_settings={
            "application_name": "workspace-zulip-bridge-v4",
            "timezone": "UTC",
        },
    )


async def prepare_database(pool: asyncpg.Pool) -> None:
    schema = (
        resources.files("workspace_zulip_bridge")
        .joinpath("schema_v4.sql")
        .read_text(encoding="utf-8")
    )
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('workspace_zulip_bridge:v4:schema', 0))"
        )
        await connection.execute(schema)


async def probe_database(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        await connection.execute("SELECT 1")
