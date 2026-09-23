# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import argparse
import asyncio
import json
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from workspace_zulip_bridge.config import Settings


@dataclass(frozen=True, slots=True)
class MonitorSnapshot:
    sampled_at: datetime
    external_accounts: int
    enabled_accounts: int
    zulip_statuses: dict[str, int]
    workspace_connected: bool


async def collect_snapshot(connection: asyncpg.Connection) -> MonitorSnapshot:
    row = await connection.fetchrow(
        """
        SELECT clock_timestamp() AS sampled_at,
               count(*) AS external_accounts,
               count(*) FILTER (WHERE account.enabled) AS enabled_accounts,
               EXISTS (
                   SELECT 1
                   FROM workspace_zulip_bridge.v4_workspace_event_cursors
                   WHERE connected_at IS NOT NULL AND disconnected_at IS NULL
               ) AS workspace_connected
        FROM workspace_zulip_bridge.v4_external_accounts AS account
        """
    )
    if row is None:
        raise RuntimeError("monitor query returned no row")
    statuses = await connection.fetch(
        """
        SELECT connection_status, count(*) AS accounts
        FROM workspace_zulip_bridge.v4_zulip_queues
        GROUP BY connection_status
        ORDER BY connection_status
        """
    )
    return MonitorSnapshot(
        sampled_at=row["sampled_at"],
        external_accounts=row["external_accounts"],
        enabled_accounts=row["enabled_accounts"],
        zulip_statuses={
            item["connection_status"]: item["accounts"] for item in statuses
        },
        workspace_connected=row["workspace_connected"],
    )


async def _run() -> None:
    parser = argparse.ArgumentParser(description="Print v4 connection status")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    settings = Settings.from_env()
    connection = await asyncpg.connect(settings.database_dsn)
    try:
        snapshot = await collect_snapshot(connection)
    finally:
        await connection.close()
    if args.json:
        print(json.dumps(asdict(snapshot), default=str, sort_keys=True))
    else:
        print(
            f"workspace_connected={snapshot.workspace_connected} "
            f"accounts={snapshot.enabled_accounts}/{snapshot.external_accounts} "
            f"zulip_statuses={snapshot.zulip_statuses}"
        )


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
