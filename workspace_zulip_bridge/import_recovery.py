# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Explicit bounded file retries after the operator fixes the diagnosed cause."""

import argparse
import asyncio
import re
from uuid import UUID

import asyncpg

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.database import open_pool
from workspace_zulip_bridge.workspace_file_transfer import CATALOG_PROJECTION_REVISION


async def retry_file_projections(
    pool: asyncpg.Pool,
    projection_uuids: list[UUID],
    *,
    reason: str,
    apply: bool = False,
) -> int:
    ids = sorted(set(projection_uuids))
    if not 1 <= len(ids) <= 100:
        raise ValueError("select between one and one hundred file projections")
    if re.fullmatch(r"[a-z0-9_:-]{1,128}", reason) is None:
        raise ValueError("recovery reason must be a short diagnostic code")
    async with pool.acquire() as connection, connection.transaction():
        rows = await connection.fetch(
            """
            SELECT projection.uuid, projection.processing_status,
                   projection.attempt_count, projection.last_error, realm.workspace_provider_uuid
            FROM workspace_zulip_bridge.workspace_file_projections AS projection
            JOIN workspace_zulip_bridge.zulip_streams AS stream
              ON stream.uuid = projection.zulip_stream_uuid
            JOIN workspace_zulip_bridge.zulip_realms AS realm ON realm.uuid = stream.realm_uuid
            JOIN workspace_zulip_bridge.zulip_connections AS supplier
              ON supplier.uuid = stream.source_connection_uuid AND supplier.sync_enabled
            JOIN workspace_zulip_bridge.workspace_chat_catalog_reports AS catalog
              ON catalog.external_account_uuid = supplier.external_account_uuid
             AND catalog.zulip_stream_uuid = stream.uuid
             AND catalog.processing_status = 'reported' AND catalog.projection_revision >= $2
            WHERE projection.uuid = ANY($1::uuid[])
              AND projection.processing_status IN ('failed', 'blocked')
            ORDER BY projection.uuid FOR UPDATE OF projection
            """,
            ids,
            CATALOG_PROJECTION_REVISION,
        )
        if not apply or not rows:
            return len(rows)
        await connection.executemany(
            """
            INSERT INTO workspace_zulip_bridge.file_projection_recoveries
                (projection_uuid, previous_status, previous_attempt_count, previous_error, recovery_reason)
            VALUES ($1, $2, $3, $4, $5)
            """,
            [
                (
                    row["uuid"],
                    row["processing_status"],
                    row["attempt_count"],
                    row["last_error"],
                    reason,
                )
                for row in rows
            ],
        )
        await connection.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_file_projections
            SET processing_status = 'pending', attempt_count = 0,
                claimed_at = NULL, heartbeat_at = NULL,
                available_at = clock_timestamp(), last_error = 'operator_recovery',
                updated_at = clock_timestamp()
            WHERE uuid = ANY($1::uuid[])
            """,
            [row["uuid"] for row in rows],
        )
        await connection.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_mirror_state
            SET initial_sync_completed_at = NULL, updated_at = clock_timestamp()
            WHERE provider_uuid = ANY($1::uuid[])
            """,
            [
                row["workspace_provider_uuid"]
                for row in rows
                if row["workspace_provider_uuid"]
            ],
        )
        return len(rows)


async def _run(ids: list[UUID], reason: str, apply: bool) -> None:
    pool = await open_pool(Settings.from_env())
    try:
        count = await retry_file_projections(pool, ids, reason=reason, apply=apply)
        print(f"{'Requeued' if apply else 'Eligible'} file projections: {count}")
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--projection", type=UUID, action="append", required=True)
    parser.add_argument(
        "--reason", required=True, help="diagnosed cause/fix code, no private data"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="persist the retry and recovery audit record",
    )
    args = parser.parse_args()
    asyncio.run(_run(args.projection, args.reason, args.apply))


if __name__ == "__main__":
    main()
