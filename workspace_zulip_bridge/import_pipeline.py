# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Coordinate short claims for one heavy historical stage at a time."""

import asyncpg
from asyncpg.pool import PoolConnectionProxy


async def lock_historical_stage(
    connection: asyncpg.Connection | PoolConnectionProxy,
) -> None:
    # Hold only while selecting/claiming rows, never across network I/O. Both
    # file and entity workers inspect in-flight rows under this same lock.
    await connection.execute(
        "SELECT pg_advisory_xact_lock("
        "hashtextextended('workspace_zulip_bridge:historical-stage', 0))"
    )


async def historical_files_runnable(
    connection: asyncpg.Pool | asyncpg.Connection | PoolConnectionProxy,
) -> bool:
    from workspace_zulip_bridge.workspace_file_transfer import (
        CATALOG_PROJECTION_REVISION,
    )

    return bool(
        await connection.fetchval(
            """
        SELECT EXISTS (
            SELECT 1 FROM workspace_zulip_bridge.workspace_file_projections
            WHERE delivery_priority = 1 AND processing_status = 'processing'
        ) OR EXISTS (
            SELECT 1
            FROM workspace_zulip_bridge.workspace_file_projections AS projection
            JOIN workspace_zulip_bridge.zulip_streams AS stream
              ON stream.uuid = projection.zulip_stream_uuid
            JOIN workspace_zulip_bridge.zulip_connections AS connection
              ON connection.uuid = stream.source_connection_uuid
            JOIN workspace_zulip_bridge.workspace_chat_catalog_reports AS catalog
              ON catalog.external_account_uuid = connection.external_account_uuid
             AND catalog.zulip_stream_uuid = stream.uuid
            WHERE projection.delivery_priority = 1
              AND projection.processing_status IN ('pending', 'failed')
              AND projection.available_at <= clock_timestamp()
              AND connection.sync_enabled
              AND catalog.processing_status = 'reported'
              AND catalog.projection_revision >= $1
        )
        """,
            CATALOG_PROJECTION_REVISION,
        )
    )
