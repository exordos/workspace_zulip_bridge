# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from importlib import resources

import asyncpg

from workspace_zulip_bridge.config import Settings


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
        await connection.execute(schema)


async def probe_database(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        await connection.execute("SELECT 1")
