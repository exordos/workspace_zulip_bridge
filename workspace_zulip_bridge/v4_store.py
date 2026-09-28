# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from collections.abc import Collection
from uuid import UUID

import asyncpg

from workspace_zulip_bridge.models import ExternalAccount
from workspace_zulip_bridge.models import MessageLink
from workspace_zulip_bridge.models import StreamLink
from workspace_zulip_bridge.models import WorkspaceEventCursor


class V4Store:
    """Persistence used by v4 connection workers only.

    Every query is deliberately scoped to a ``v4_`` table.  Legacy bridge
    tables remain in place but are neither read nor mutated by this store.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @staticmethod
    def _account(row: asyncpg.Record) -> ExternalAccount:
        return ExternalAccount(
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
        return [self._account(row) for row in rows]

    async def external_account(self, account_uuid: UUID) -> ExternalAccount | None:
        row = await self._pool.fetchrow(
            """
            SELECT account.uuid, account.owner_workspace_user_uuid,
                   account.desired_generation, account.workspace_project_id,
                   account.endpoint, account.login, account.api_key,
                   queue.queue_id, queue.last_event_id
            FROM workspace_zulip_bridge.v4_external_accounts AS account
            LEFT JOIN workspace_zulip_bridge.v4_zulip_queues AS queue
              ON queue.external_account_uuid = account.uuid
            WHERE account.uuid = $1 AND account.enabled
            """,
            account_uuid,
        )
        return None if row is None else self._account(row)

    async def upsert_realtime_links(
        self,
        account_uuid: UUID,
        stream_uuid: UUID,
        chat_key: str,
        topic_uuid: UUID,
        topic_name: str,
        message_uuid: UUID,
        zulip_message_id: int | None,
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.v4_stream_links (
                    workspace_stream_uuid, external_account_uuid, chat_key
                ) VALUES ($1, $2, $3)
                ON CONFLICT (workspace_stream_uuid) DO UPDATE SET
                    external_account_uuid = EXCLUDED.external_account_uuid,
                    chat_key = EXCLUDED.chat_key,
                    updated_at = clock_timestamp()
                """,
                stream_uuid,
                account_uuid,
                chat_key,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.v4_topic_links (
                    workspace_topic_uuid, workspace_stream_uuid, topic_name
                ) VALUES ($1, $2, $3)
                ON CONFLICT (workspace_topic_uuid) DO UPDATE SET
                    workspace_stream_uuid = EXCLUDED.workspace_stream_uuid,
                    topic_name = EXCLUDED.topic_name,
                    updated_at = clock_timestamp()
                """,
                topic_uuid,
                stream_uuid,
                topic_name,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.v4_message_links (
                    workspace_message_uuid, external_account_uuid,
                    zulip_message_id, workspace_stream_uuid,
                    workspace_topic_uuid
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (workspace_message_uuid) DO UPDATE SET
                    external_account_uuid = EXCLUDED.external_account_uuid,
                    zulip_message_id = EXCLUDED.zulip_message_id,
                    workspace_stream_uuid = EXCLUDED.workspace_stream_uuid,
                    workspace_topic_uuid = EXCLUDED.workspace_topic_uuid,
                    updated_at = clock_timestamp()
                """,
                message_uuid,
                account_uuid,
                zulip_message_id,
                stream_uuid,
                topic_uuid,
            )

    async def workspace_message_uuid(
        self,
        account_uuid: UUID,
        zulip_message_id: int,
    ) -> UUID | None:
        value = await self._pool.fetchval(
            """
            SELECT workspace_message_uuid
            FROM workspace_zulip_bridge.v4_message_links
            WHERE external_account_uuid = $1 AND zulip_message_id = $2
            """,
            account_uuid,
            zulip_message_id,
        )
        return None if value is None else UUID(str(value))

    async def stream_link(self, stream_uuid: UUID) -> StreamLink | None:
        row = await self._pool.fetchrow(
            """
            SELECT account.uuid, account.owner_workspace_user_uuid,
                   account.desired_generation, account.workspace_project_id,
                   account.endpoint, account.login, account.api_key,
                   queue.queue_id, queue.last_event_id, link.chat_key
            FROM workspace_zulip_bridge.v4_stream_links AS link
            JOIN workspace_zulip_bridge.v4_external_accounts AS account
              ON account.uuid = link.external_account_uuid AND account.enabled
            LEFT JOIN workspace_zulip_bridge.v4_zulip_queues AS queue
              ON queue.external_account_uuid = account.uuid
            WHERE link.workspace_stream_uuid = $1
            """,
            stream_uuid,
        )
        if row is None:
            return None
        return StreamLink(account=self._account(row), chat_key=row["chat_key"])

    async def topic_name(self, topic_uuid: UUID) -> str | None:
        value = await self._pool.fetchval(
            """
            SELECT topic_name FROM workspace_zulip_bridge.v4_topic_links
            WHERE workspace_topic_uuid = $1
            """,
            topic_uuid,
        )
        return None if value is None else str(value)

    async def message_link(self, message_uuid: UUID) -> MessageLink | None:
        row = await self._pool.fetchrow(
            """
            SELECT account.uuid, account.owner_workspace_user_uuid,
                   account.desired_generation, account.workspace_project_id,
                   account.endpoint, account.login, account.api_key,
                   queue.queue_id, queue.last_event_id,
                   link.zulip_message_id, link.workspace_stream_uuid,
                   link.workspace_topic_uuid
            FROM workspace_zulip_bridge.v4_message_links AS link
            JOIN workspace_zulip_bridge.v4_external_accounts AS account
              ON account.uuid = link.external_account_uuid AND account.enabled
            LEFT JOIN workspace_zulip_bridge.v4_zulip_queues AS queue
              ON queue.external_account_uuid = account.uuid
            WHERE link.workspace_message_uuid = $1
            """,
            message_uuid,
        )
        if row is None:
            return None
        return MessageLink(
            account=self._account(row),
            zulip_message_id=row["zulip_message_id"],
            workspace_stream_uuid=row["workspace_stream_uuid"],
            workspace_topic_uuid=row["workspace_topic_uuid"],
        )

    async def delete_message_link(self, message_uuid: UUID) -> None:
        await self._pool.execute(
            """
            DELETE FROM workspace_zulip_bridge.v4_message_links
            WHERE workspace_message_uuid = $1
            """,
            message_uuid,
        )

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
