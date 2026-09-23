# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from collections.abc import Collection
from uuid import UUID

import asyncpg

from workspace_zulip_bridge.models import ExternalAccount
from workspace_zulip_bridge.models import WorkspaceEventCursor


class V4Store:
    """Persistence used by v4 connection workers only.

    Every query is deliberately scoped to a ``v4_`` table.  Legacy bridge
    tables remain in place but are neither read nor mutated by this store.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def list_external_accounts(self) -> list[ExternalAccount]:
        rows = await self._pool.fetch(
            """
            SELECT account.uuid, account.owner_workspace_user_uuid,
                   account.desired_generation, account.workspace_project_id,
                   account.endpoint, account.login, account.api_key,
                   queue.queue_id, queue.last_event_id
            FROM workspace_zulip_bridge.v4_external_accounts AS account
            LEFT JOIN workspace_zulip_bridge.v4_zulip_queues AS queue
              ON queue.external_account_uuid = account.uuid
            WHERE account.enabled
            ORDER BY account.uuid
            """
        )
        return [
            ExternalAccount(
                uuid=row["uuid"],
                owner_workspace_user_uuid=row["owner_workspace_user_uuid"],
                desired_generation=row["desired_generation"],
                workspace_project_id=row["workspace_project_id"],
                endpoint=row["endpoint"],
                login=row["login"],
                api_key=row["api_key"],
                queue_id=row["queue_id"],
                last_event_id=row["last_event_id"],
            )
            for row in rows
        ]

    async def upsert_external_account(
        self,
        account_uuid: UUID,
        owner_workspace_user_uuid: UUID,
        desired_generation: int,
        workspace_project_id: UUID,
        endpoint: str,
        login: str,
        api_key: str,
    ) -> None:
        await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.v4_external_accounts (
                uuid, owner_workspace_user_uuid, desired_generation,
                workspace_project_id, endpoint, login, api_key, enabled
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, true)
            ON CONFLICT (uuid) DO UPDATE SET
                owner_workspace_user_uuid = EXCLUDED.owner_workspace_user_uuid,
                desired_generation = EXCLUDED.desired_generation,
                workspace_project_id = EXCLUDED.workspace_project_id,
                endpoint = EXCLUDED.endpoint,
                login = EXCLUDED.login,
                api_key = EXCLUDED.api_key,
                enabled = true,
                updated_at = clock_timestamp()
            """,
            account_uuid,
            owner_workspace_user_uuid,
            desired_generation,
            workspace_project_id,
            endpoint,
            login,
            api_key,
        )

    async def disable_external_account(self, account_uuid: UUID) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.v4_external_accounts
                SET enabled = false, updated_at = clock_timestamp()
                WHERE uuid = $1
                """,
                account_uuid,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.v4_zulip_queues
                SET queue_id = NULL, last_event_id = NULL,
                    connection_status = 'disconnected',
                    disconnected_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE external_account_uuid = $1
                """,
                account_uuid,
            )

    async def disable_absent_external_accounts(
        self, account_uuids: Collection[UUID]
    ) -> None:
        values = list(account_uuids)
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.v4_external_accounts
                SET enabled = false, updated_at = clock_timestamp()
                WHERE enabled AND NOT (uuid = ANY($1::uuid[]))
                """,
                values,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.v4_zulip_queues AS queue
                SET queue_id = NULL, last_event_id = NULL,
                    connection_status = 'disconnected',
                    disconnected_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                FROM workspace_zulip_bridge.v4_external_accounts AS account
                WHERE account.uuid = queue.external_account_uuid
                  AND NOT account.enabled
                """
            )

    async def set_zulip_queue(
        self,
        account_uuid: UUID,
        queue_id: str,
        last_event_id: int,
    ) -> bool:
        status = await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.v4_zulip_queues AS current_queue (
                external_account_uuid, queue_id, last_event_id,
                connection_status, connected_at, disconnected_at, last_error
            )
            SELECT uuid, $2, $3, 'connected', clock_timestamp(), NULL, NULL
            FROM workspace_zulip_bridge.v4_external_accounts
            WHERE uuid = $1 AND enabled
            ON CONFLICT (external_account_uuid) DO UPDATE SET
                queue_id = EXCLUDED.queue_id,
                last_event_id = EXCLUDED.last_event_id,
                connection_status = 'connected',
                connected_at = COALESCE(
                    current_queue.connected_at,
                    clock_timestamp()
                ),
                disconnected_at = NULL,
                last_error = NULL,
                updated_at = clock_timestamp()
            """,
            account_uuid,
            queue_id,
            last_event_id,
        )
        return status in {"INSERT 0 1", "UPDATE 1"}

    async def advance_zulip_cursor(
        self,
        account_uuid: UUID,
        queue_id: str,
        last_event_id: int,
    ) -> bool:
        status = await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.v4_zulip_queues AS queue
            SET last_event_id = GREATEST(queue.last_event_id, $3),
                connection_status = 'connected',
                disconnected_at = NULL, last_error = NULL,
                updated_at = clock_timestamp()
            FROM workspace_zulip_bridge.v4_external_accounts AS account
            WHERE account.uuid = $1 AND account.enabled
              AND queue.external_account_uuid = account.uuid
              AND queue.queue_id = $2
            """,
            account_uuid,
            queue_id,
            last_event_id,
        )
        return status == "UPDATE 1"

    async def clear_zulip_queue(self, account_uuid: UUID, queue_id: str) -> bool:
        status = await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.v4_zulip_queues
            SET queue_id = NULL, last_event_id = NULL,
                connection_status = 'disconnected',
                disconnected_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE external_account_uuid = $1 AND queue_id = $2
            """,
            account_uuid,
            queue_id,
        )
        return status == "UPDATE 1"

    async def mark_zulip_status(
        self,
        account_uuid: UUID,
        status: str,
        error: str | None = None,
    ) -> None:
        if status not in {"disconnected", "connecting", "auth_required"}:
            raise ValueError("unsupported Zulip connection status")
        await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.v4_zulip_queues (
                external_account_uuid, connection_status, disconnected_at,
                last_error
            )
            SELECT uuid, $2, clock_timestamp(), $3
            FROM workspace_zulip_bridge.v4_external_accounts
            WHERE uuid = $1
            ON CONFLICT (external_account_uuid) DO UPDATE SET
                connection_status = EXCLUDED.connection_status,
                disconnected_at = EXCLUDED.disconnected_at,
                last_error = EXCLUDED.last_error,
                updated_at = clock_timestamp()
            """,
            account_uuid,
            status,
            error,
        )

    async def workspace_cursor(
        self,
        provider_uuid: UUID,
        project_uuid: UUID,
    ) -> WorkspaceEventCursor:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.v4_workspace_event_cursors (
                    provider_uuid, workspace_project_id
                ) VALUES ($1, $2)
                ON CONFLICT (provider_uuid) DO NOTHING
                """,
                provider_uuid,
                project_uuid,
            )
            row = await connection.fetchrow(
                """
                SELECT workspace_project_id, epoch_generation, last_epoch_version
                FROM workspace_zulip_bridge.v4_workspace_event_cursors
                WHERE provider_uuid = $1
                """,
                provider_uuid,
            )
        if row is None or row["workspace_project_id"] != project_uuid:
            raise RuntimeError("Workspace cursor belongs to another project")
        return WorkspaceEventCursor(
            epoch_generation=row["epoch_generation"],
            last_epoch_version=row["last_epoch_version"],
        )

    async def advance_workspace_cursor(
        self,
        provider_uuid: UUID,
        project_uuid: UUID,
        epoch_generation: UUID,
        last_epoch_version: int,
    ) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.v4_workspace_event_cursors
            SET epoch_generation = $3,
                last_epoch_version = GREATEST(last_epoch_version, $4),
                connected_at = COALESCE(connected_at, clock_timestamp()),
                disconnected_at = NULL, last_error = NULL,
                updated_at = clock_timestamp()
            WHERE provider_uuid = $1 AND workspace_project_id = $2
            """,
            provider_uuid,
            project_uuid,
            epoch_generation,
            last_epoch_version,
        )

    async def reset_workspace_cursor(
        self,
        provider_uuid: UUID,
        project_uuid: UUID,
        last_epoch_version: int,
    ) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.v4_workspace_event_cursors
            SET epoch_generation = NULL, last_epoch_version = $3,
                disconnected_at = clock_timestamp(),
                last_error = 'workspace_cursor_expired',
                updated_at = clock_timestamp()
            WHERE provider_uuid = $1 AND workspace_project_id = $2
            """,
            provider_uuid,
            project_uuid,
            last_epoch_version,
        )

    async def mark_workspace_disconnected(
        self,
        provider_uuid: UUID,
        error: str | None,
    ) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.v4_workspace_event_cursors
            SET disconnected_at = clock_timestamp(), last_error = $2,
                updated_at = clock_timestamp()
            WHERE provider_uuid = $1
            """,
            provider_uuid,
            error,
        )
