# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from collections.abc import Sequence
from typing import cast
from uuid import UUID

import asyncpg
from asyncpg.pool import PoolConnectionProxy

from workspace_zulip_bridge.models import ChatCatalogWrite
from workspace_zulip_bridge.models import ChatScheduleReconcile
from workspace_zulip_bridge.models import HistoryWrite
from workspace_zulip_bridge.models import LiveMessageWrite
from workspace_zulip_bridge.models import MessagePageWrite
from workspace_zulip_bridge.models import ScheduledChat
from workspace_zulip_bridge.models import UserDirectoryWrite
from workspace_zulip_bridge.models import UserStatus
from workspace_zulip_bridge.models import ZulipChat
from workspace_zulip_bridge.models import ZulipChatCatalog
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.models import ZulipEvent
from workspace_zulip_bridge.models import ZulipMessage
from workspace_zulip_bridge.models import ZulipUser
from workspace_zulip_bridge.stable_ids import canonical_endpoint
from workspace_zulip_bridge.stable_ids import stable_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid


class EventStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def list_users(self) -> list[ZulipUser]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT uuid,
                       endpoint,
                       login,
                       api_key,
                       queue_id,
                       last_event_id,
                       status,
                       chats_hash,
                       zulip_user_id,
                       full_name,
                       role,
                       disabled,
                       EXISTS (
                           SELECT 1
                           FROM workspace_zulip_bridge.zulip_chats AS chat
                           WHERE chat.supplier_user_uuid = zulip_users.uuid
                             AND chat.history_loaded_at IS NULL
                       ) AS has_pending_history
                FROM workspace_zulip_bridge.zulip_users AS zulip_users
                WHERE NOT disabled
                  AND api_key IS NOT NULL
                ORDER BY uuid
                """
            )
        return [
            ZulipUser(
                uuid=row["uuid"],
                endpoint=row["endpoint"],
                login=row["login"],
                api_key=row["api_key"],
                queue_id=row["queue_id"],
                last_event_id=row["last_event_id"],
                status=row["status"],
                chats_hash=row["chats_hash"],
                zulip_user_id=row["zulip_user_id"],
                full_name=row["full_name"],
                role=row["role"],
                disabled=row["disabled"],
                has_pending_history=row["has_pending_history"],
            )
            for row in rows
        ]

    async def set_user_identity(
        self,
        user_uuid: UUID,
        endpoint: str,
        zulip_user_id: int,
        full_name: str,
        role: int,
    ) -> bool:
        if user_uuid != stable_user_uuid(endpoint, zulip_user_id):
            raise ValueError("Zulip user UUID does not match its stable identity")
        async with self._pool.acquire() as connection:
            status = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET zulip_user_id = $2,
                    full_name = $3,
                    role = $4
                WHERE uuid = $1
                  AND NOT disabled
                  AND api_key IS NOT NULL
                """,
                user_uuid,
                zulip_user_id,
                full_name,
                role,
            )
        return status == "UPDATE 1"

    async def list_user_chat_keys(self, user_uuid: UUID) -> set[str]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT chat.chat_key
                FROM workspace_zulip_bridge.zulip_chat_users AS chat_user
                JOIN workspace_zulip_bridge.zulip_chats AS chat
                  ON chat.uuid = chat_user.zulip_chat_uuid
                WHERE chat_user.zulip_user_uuid = $1
                """,
                user_uuid,
            )
        return {row["chat_key"] for row in rows}

    async def list_scheduled_chat_keys(self, user_uuid: UUID) -> set[str]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT chat_key
                FROM workspace_zulip_bridge.zulip_chats
                WHERE supplier_user_uuid = $1
                """,
                user_uuid,
            )
        return {row["chat_key"] for row in rows}

    async def has_pending_history(self, user_uuid: UUID, queue_id: str) -> bool:
        async with self._pool.acquire() as connection:
            return bool(
                await connection.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM workspace_zulip_bridge.zulip_users AS zulip_user
                        JOIN workspace_zulip_bridge.zulip_chats AS chat
                          ON chat.supplier_user_uuid = zulip_user.uuid
                         AND chat.history_loaded_at IS NULL
                        WHERE zulip_user.uuid = $1
                          AND zulip_user.queue_id = $2
                    )
                    """,
                    user_uuid,
                    queue_id,
                )
            )

    async def reconcile_chat_schedules(self) -> ChatScheduleReconcile:
        async with self._pool.acquire() as connection, connection.transaction():
            invalidated_row = await connection.fetchrow(
                """
                WITH invalid AS MATERIALIZED (
                    SELECT chat.uuid,
                           chat.supplier_user_uuid
                    FROM workspace_zulip_bridge.zulip_chats AS chat
                    LEFT JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                      ON zulip_user.uuid = chat.supplier_user_uuid
                    LEFT JOIN workspace_zulip_bridge.zulip_chat_users AS chat_user
                      ON chat_user.zulip_chat_uuid = chat.uuid
                     AND chat_user.zulip_user_uuid = chat.supplier_user_uuid
                    WHERE chat.supplier_user_uuid IS NOT NULL
                      AND (
                          zulip_user.uuid IS NULL
                          OR zulip_user.disabled
                          OR zulip_user.api_key IS NULL
                          OR chat_user.zulip_user_uuid IS NULL
                      )
                    FOR UPDATE OF chat
                ),
                removed AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    USING invalid
                    WHERE message.zulip_user_uuid = invalid.supplier_user_uuid
                      AND message.zulip_chat_uuid = invalid.uuid
                    RETURNING 1
                ),
                cleared AS (
                    UPDATE workspace_zulip_bridge.zulip_chats AS chat
                    SET supplier_user_uuid = NULL
                    FROM invalid
                    WHERE chat.uuid = invalid.uuid
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM cleared) AS invalidated_count,
                       (SELECT count(*) FROM removed) AS messages_deleted
                """
            )
            if invalidated_row is None:
                raise RuntimeError("chat schedule invalidation returned no row")
            assignment_row = await connection.fetchrow(
                """
                WITH ready_endpoints AS MATERIALIZED (
                    SELECT zulip_user.endpoint
                    FROM workspace_zulip_bridge.zulip_users AS zulip_user
                    WHERE NOT zulip_user.disabled
                      AND zulip_user.api_key IS NOT NULL
                    GROUP BY zulip_user.endpoint
                    HAVING bool_and(zulip_user.catalog_completed_at IS NOT NULL)
                ),
                winners AS MATERIALIZED (
                    SELECT DISTINCT ON (chat.uuid)
                           chat.uuid AS chat_uuid,
                           chat_user.zulip_user_uuid
                    FROM workspace_zulip_bridge.zulip_chats AS chat
                    JOIN ready_endpoints
                      ON ready_endpoints.endpoint = chat.endpoint
                    JOIN workspace_zulip_bridge.zulip_chat_users AS chat_user
                      ON chat_user.zulip_chat_uuid = chat.uuid
                    JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                      ON zulip_user.uuid = chat_user.zulip_user_uuid
                     AND NOT zulip_user.disabled
                     AND zulip_user.api_key IS NOT NULL
                    ORDER BY chat.uuid,
                             zulip_user.role,
                             zulip_user.uuid
                ),
                changed AS MATERIALIZED (
                    SELECT chat.uuid AS chat_uuid,
                           chat.supplier_user_uuid AS old_supplier_user_uuid,
                           winners.zulip_user_uuid AS new_supplier_user_uuid
                    FROM workspace_zulip_bridge.zulip_chats AS chat
                    JOIN winners ON winners.chat_uuid = chat.uuid
                    WHERE chat.supplier_user_uuid IS DISTINCT FROM
                          winners.zulip_user_uuid
                    FOR UPDATE OF chat
                ),
                removed AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    USING changed
                    WHERE changed.old_supplier_user_uuid IS NOT NULL
                      AND message.zulip_chat_uuid = changed.chat_uuid
                      AND message.zulip_user_uuid =
                          changed.old_supplier_user_uuid
                    RETURNING 1
                ),
                updated AS (
                    UPDATE workspace_zulip_bridge.zulip_chats AS chat
                    SET supplier_user_uuid = changed.new_supplier_user_uuid,
                        history_loaded_at = NULL
                    FROM changed
                    WHERE chat.uuid = changed.chat_uuid
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM updated) AS assigned_count,
                       (SELECT count(*) FROM removed) AS messages_deleted
                """
            )
            if assignment_row is None:
                raise RuntimeError("chat schedule assignment returned no row")
            await connection.execute(
                """
                WITH ready_endpoints AS MATERIALIZED (
                    SELECT zulip_user.endpoint
                    FROM workspace_zulip_bridge.zulip_users AS zulip_user
                    WHERE NOT zulip_user.disabled
                      AND zulip_user.api_key IS NOT NULL
                    GROUP BY zulip_user.endpoint
                    HAVING bool_and(zulip_user.catalog_completed_at IS NOT NULL)
                )
                UPDATE workspace_zulip_bridge.zulip_users AS zulip_user
                SET status = CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM workspace_zulip_bridge.zulip_chats AS chat
                        WHERE chat.supplier_user_uuid = zulip_user.uuid
                          AND chat.history_loaded_at IS NULL
                    ) THEN 'backfilling'
                    ELSE 'active'
                END
                FROM ready_endpoints
                WHERE zulip_user.endpoint = ready_endpoints.endpoint
                  AND (
                      zulip_user.status IN ('scheduling', 'backfilling')
                      OR EXISTS (
                          SELECT 1
                          FROM workspace_zulip_bridge.zulip_chats AS chat
                          WHERE chat.supplier_user_uuid = zulip_user.uuid
                            AND chat.history_loaded_at IS NULL
                      )
                  )
                """
            )
        return ChatScheduleReconcile(
            invalidated=invalidated_row["invalidated_count"],
            assigned=assignment_row["assigned_count"],
            messages_deleted=(
                invalidated_row["messages_deleted"] + assignment_row["messages_deleted"]
            ),
        )

    async def store_user_directory(
        self,
        endpoint: str,
        users: Sequence[ZulipDirectoryUser],
    ) -> UserDirectoryWrite:
        endpoint = canonical_endpoint(endpoint)
        users = tuple(user for user in users if not user.is_bot)
        user_uuids = [stable_user_uuid(endpoint, user.user_id) for user in users]
        user_ids = [user.user_id for user in users]
        logins = [user.login for user in users]
        full_names = [user.full_name for user in users]
        roles = [user.role for user in users]
        disabled_values = [user.disabled for user in users]
        async with self._pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """
                WITH incoming AS MATERIALIZED (
                    SELECT uuid, user_id, login, full_name, role, disabled
                    FROM unnest(
                        $2::uuid[],
                        $3::bigint[],
                        $4::text[],
                        $5::text[],
                        $6::smallint[],
                        $7::boolean[]
                    ) AS directory(uuid, user_id, login, full_name, role, disabled)
                ),
                upserted AS (
                    INSERT INTO workspace_zulip_bridge.zulip_users (
                        uuid,
                        endpoint,
                        login,
                        zulip_user_id,
                        full_name,
                        role,
                        disabled
                    )
                    SELECT incoming.uuid,
                           $1,
                           incoming.login,
                           incoming.user_id,
                           incoming.full_name,
                           incoming.role,
                           incoming.disabled
                    FROM incoming
                    ON CONFLICT (uuid) DO UPDATE
                    SET full_name = EXCLUDED.full_name,
                        role = EXCLUDED.role,
                        disabled = EXCLUDED.disabled
                    WHERE zulip_users.full_name IS DISTINCT FROM EXCLUDED.full_name
                       OR zulip_users.role IS DISTINCT FROM EXCLUDED.role
                       OR zulip_users.disabled IS DISTINCT FROM EXCLUDED.disabled
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM upserted) AS changed_count
                """,
                endpoint,
                user_uuids,
                user_ids,
                logins,
                full_names,
                roles,
                disabled_values,
            )
        if row is None:
            raise RuntimeError("user directory query returned no row")
        return UserDirectoryWrite(
            humans=len(users),
            changed=row["changed_count"],
        )

    async def begin_history(self, user_uuid: UUID, queue_id: str) -> "HistorySession":
        connection = await self._pool.acquire()
        session = HistorySession(self._pool, connection, user_uuid, queue_id)
        try:
            await session.initialize()
        except BaseException:
            await self._pool.release(connection)
            raise
        return session

    async def apply_live_messages(
        self,
        user_uuid: UUID,
        queue_id: str,
        chats: Sequence[ZulipChat],
        messages: Sequence[ZulipMessage],
        deleted_message_ids: Sequence[int],
    ) -> LiveMessageWrite:
        history = await self.begin_history(user_uuid, queue_id)
        try:
            await history.store_chats(chats)
            page_write = await history.store_page(messages)
        finally:
            await history.close()
        async with self._pool.acquire() as connection:
            deleted = await connection.fetchval(
                """
                WITH active_queue AS MATERIALIZED (
                    SELECT uuid
                    FROM workspace_zulip_bridge.zulip_users
                    WHERE uuid = $1
                      AND queue_id = $2
                ),
                removed AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    USING active_queue
                    WHERE message.zulip_user_uuid = active_queue.uuid
                      AND message.zulip_message_id = ANY($3::bigint[])
                    RETURNING 1
                )
                SELECT count(*) FROM removed
                """,
                user_uuid,
                queue_id,
                list(deleted_message_ids),
            )
        return LiveMessageWrite(
            messages_changed=page_write.changed,
            messages_unchanged=page_write.unchanged,
            messages_deleted=int(deleted),
            topics_inserted=page_write.topics_inserted,
        )

    async def set_queue(
        self,
        user_uuid: UUID,
        queue_id: str,
        last_event_id: int,
    ) -> bool:
        async with self._pool.acquire() as connection:
            status = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET queue_id = $2,
                    last_event_id = $3,
                    status = 'streaming'
                WHERE uuid = $1
                """,
                user_uuid,
                queue_id,
                last_event_id,
            )
        return status == "UPDATE 1"

    async def clear_queue(self, user_uuid: UUID, queue_id: str) -> bool:
        async with self._pool.acquire() as connection, connection.transaction():
            active = await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM workspace_zulip_bridge.zulip_users
                    WHERE uuid = $1
                      AND queue_id = $2
                    FOR UPDATE
                )
                """,
                user_uuid,
                queue_id,
            )
            if not active:
                return False
            await connection.execute(
                """
                DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                USING workspace_zulip_bridge.zulip_chats AS chat
                WHERE chat.supplier_user_uuid = $1
                  AND message.zulip_user_uuid = $1
                  AND message.zulip_chat_uuid = chat.uuid
                """,
                user_uuid,
            )
            await connection.execute(
                """
                DELETE FROM workspace_zulip_bridge.zulip_topics AS topic
                USING workspace_zulip_bridge.zulip_chats AS chat
                WHERE chat.supplier_user_uuid = $1
                  AND topic.zulip_user_uuid = $1
                  AND topic.zulip_chat_uuid = chat.uuid
                  AND NOT EXISTS (
                      SELECT 1
                      FROM workspace_zulip_bridge.zulip_messages AS message
                      WHERE message.topic_uuid = topic.uuid
                  )
                """,
                user_uuid,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_chats
                SET history_loaded_at = NULL
                WHERE supplier_user_uuid = $1
                """,
                user_uuid,
            )
            status = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET queue_id = NULL,
                    last_event_id = NULL,
                    status = 'init',
                    catalog_completed_at = NULL
                WHERE uuid = $1
                  AND queue_id = $2
                """,
                user_uuid,
                queue_id,
            )
        return status == "UPDATE 1"

    async def set_user_status(
        self,
        user_uuid: UUID,
        queue_id: str,
        status: UserStatus,
    ) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET status = $3
                WHERE uuid = $1
                  AND queue_id = $2
                """,
                user_uuid,
                queue_id,
                status,
            )
        return result == "UPDATE 1"

    async def get_user_status(
        self,
        user_uuid: UUID,
        queue_id: str,
    ) -> UserStatus | None:
        async with self._pool.acquire() as connection:
            status = await connection.fetchval(
                """
                SELECT status
                FROM workspace_zulip_bridge.zulip_users
                WHERE uuid = $1
                  AND queue_id = $2
                """,
                user_uuid,
                queue_id,
            )
        return cast(UserStatus | None, status)

    async def begin_catalog_fill(self, user_uuid: UUID, queue_id: str) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET status = 'filling',
                    catalog_completed_at = NULL
                WHERE uuid = $1
                  AND queue_id = $2
                """,
                user_uuid,
                queue_id,
            )
        return result == "UPDATE 1"

    async def list_pending_history_chats(
        self,
        user_uuid: UUID,
        queue_id: str,
    ) -> list[ScheduledChat]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT chat.chat_key,
                       chat_user.available_message_count
                FROM workspace_zulip_bridge.zulip_users AS zulip_user
                JOIN workspace_zulip_bridge.zulip_chats AS chat
                  ON chat.supplier_user_uuid = zulip_user.uuid
                 AND chat.history_loaded_at IS NULL
                JOIN workspace_zulip_bridge.zulip_chat_users AS chat_user
                  ON chat_user.zulip_chat_uuid = chat.uuid
                 AND chat_user.zulip_user_uuid = zulip_user.uuid
                WHERE zulip_user.uuid = $1
                  AND zulip_user.queue_id = $2
                  AND zulip_user.status = 'backfilling'
                ORDER BY chat.chat_key
                """,
                user_uuid,
                queue_id,
            )
        return [
            ScheduledChat(
                chat_key=row["chat_key"],
                available_message_count=row["available_message_count"],
            )
            for row in rows
        ]

    async def store_chat_catalog(
        self,
        user_uuid: UUID,
        queue_id: str,
        catalog: ZulipChatCatalog,
    ) -> ChatCatalogWrite:
        chat_uuids: list[UUID] = []
        chat_types = [chat.chat_type for chat in catalog.chats]
        chat_keys = [chat.chat_key for chat in catalog.chats]
        names = [chat.name for chat in catalog.chats]
        roles = [chat.role for chat in catalog.chats]
        chat_parameters = [chat.chat_parameters_json for chat in catalog.chats]
        membership_parameters = [
            chat.membership_parameters_json for chat in catalog.chats
        ]
        content_hashes = [chat.content_hash for chat in catalog.chats]
        membership_hashes = [chat.membership_hash for chat in catalog.chats]
        available_message_counts = [
            chat.available_message_count for chat in catalog.chats
        ]

        async with self._pool.acquire() as connection, connection.transaction():
            user_row = await connection.fetchrow(
                """
                SELECT endpoint, chats_hash
                FROM workspace_zulip_bridge.zulip_users
                WHERE uuid = $1
                  AND queue_id = $2
                FOR UPDATE
                """,
                user_uuid,
                queue_id,
            )
            if user_row is None:
                return ChatCatalogWrite(False, False, 0, 0)
            endpoint = user_row["endpoint"]
            chat_uuids = [stable_chat_uuid(endpoint, key) for key in chat_keys]
            current_hash = user_row["chats_hash"]
            if current_hash == catalog.content_hash:
                await connection.execute(
                    """
                    UPDATE workspace_zulip_bridge.zulip_users
                    SET status = 'scheduling',
                        catalog_completed_at = clock_timestamp()
                    WHERE uuid = $1
                      AND queue_id = $2
                    """,
                    user_uuid,
                    queue_id,
                )
                return ChatCatalogWrite(True, True, 0, 0)

            row = await connection.fetchrow(
                """
                WITH incoming AS (
                    SELECT uuid,
                           chat_type,
                           chat_key,
                           name,
                           role,
                           chat_parameters::jsonb,
                           membership_parameters::jsonb,
                           content_hash,
                           membership_hash,
                           available_message_count
                    FROM unnest(
                        $2::uuid[],
                        $3::text[],
                        $4::text[],
                        $5::text[],
                        $6::text[],
                        $7::text[],
                        $8::text[],
                        $9::bytea[],
                        $10::bytea[],
                        $11::bigint[]
                    ) AS chat(
                        uuid,
                        chat_type,
                        chat_key,
                        name,
                        role,
                        chat_parameters,
                        membership_parameters,
                        content_hash,
                        membership_hash,
                        available_message_count
                    )
                ),
                chats_upserted AS (
                    INSERT INTO workspace_zulip_bridge.zulip_chats (
                        uuid,
                        endpoint,
                        chat_type,
                        chat_key,
                        name,
                        chat_parameters,
                        content_hash
                    )
                    SELECT incoming.uuid,
                           $12,
                           incoming.chat_type,
                           incoming.chat_key,
                           incoming.name,
                           incoming.chat_parameters,
                           incoming.content_hash
                    FROM incoming
                    ON CONFLICT (uuid) DO UPDATE
                    SET chat_type = EXCLUDED.chat_type,
                        name = EXCLUDED.name,
                        chat_parameters = EXCLUDED.chat_parameters,
                        content_hash = EXCLUDED.content_hash
                    WHERE zulip_chats.content_hash IS DISTINCT FROM
                          EXCLUDED.content_hash
                    RETURNING uuid
                ),
                memberships_upserted AS (
                    INSERT INTO workspace_zulip_bridge.zulip_chat_users (
                        zulip_chat_uuid,
                        zulip_user_uuid,
                        role,
                        membership_parameters,
                        content_hash,
                        available_message_count
                    )
                    SELECT incoming.uuid,
                           $1,
                           incoming.role,
                           incoming.membership_parameters,
                           incoming.membership_hash,
                           incoming.available_message_count
                    FROM incoming
                    CROSS JOIN (
                        SELECT count(*) FROM chats_upserted
                    ) AS chat_write_barrier
                    ON CONFLICT (zulip_chat_uuid, zulip_user_uuid) DO UPDATE
                    SET role = EXCLUDED.role,
                        membership_parameters = EXCLUDED.membership_parameters,
                        content_hash = EXCLUDED.content_hash,
                        available_message_count = EXCLUDED.available_message_count
                    WHERE zulip_chat_users.content_hash IS DISTINCT FROM
                              EXCLUDED.content_hash
                       OR zulip_chat_users.available_message_count IS DISTINCT FROM
                              EXCLUDED.available_message_count
                    RETURNING 1
                ),
                removed_memberships AS MATERIALIZED (
                    SELECT chat_user.zulip_chat_uuid
                    FROM workspace_zulip_bridge.zulip_chat_users AS chat_user
                    WHERE chat_user.zulip_user_uuid = $1
                      AND NOT (chat_user.zulip_chat_uuid = ANY($2::uuid[]))
                ),
                messages_deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    USING removed_memberships
                    WHERE message.zulip_user_uuid = $1
                      AND message.zulip_chat_uuid =
                          removed_memberships.zulip_chat_uuid
                    RETURNING 1
                ),
                suppliers_cleared AS (
                    UPDATE workspace_zulip_bridge.zulip_chats AS chat
                    SET supplier_user_uuid = NULL
                    FROM removed_memberships
                    WHERE chat.uuid = removed_memberships.zulip_chat_uuid
                      AND chat.supplier_user_uuid = $1
                    RETURNING 1
                ),
                memberships_deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_chat_users AS chat_user
                    USING removed_memberships
                    WHERE chat_user.zulip_user_uuid = $1
                      AND chat_user.zulip_chat_uuid =
                          removed_memberships.zulip_chat_uuid
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM memberships_upserted)
                           AS upserted_count,
                       (SELECT count(*) FROM memberships_deleted) AS deleted_count
                """,
                user_uuid,
                chat_uuids,
                chat_types,
                chat_keys,
                names,
                roles,
                chat_parameters,
                membership_parameters,
                content_hashes,
                membership_hashes,
                available_message_counts,
                endpoint,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET status = 'scheduling',
                    chats_hash = $3,
                    catalog_completed_at = clock_timestamp()
                WHERE uuid = $1
                  AND queue_id = $2
                """,
                user_uuid,
                queue_id,
                catalog.content_hash,
            )
        if row is None:
            raise RuntimeError("chat catalog query returned no row")
        return ChatCatalogWrite(
            activated=True,
            reused=False,
            upserted=row["upserted_count"],
            deleted=row["deleted_count"],
        )

    async def store_events(
        self,
        user_uuid: UUID,
        queue_id: str,
        events: Sequence[ZulipEvent],
        last_event_id: int,
    ) -> tuple[int, bool]:
        event_ids = [event.event_id for event in events]
        event_types = [event.event_type for event in events]
        payloads = [event.payload_json for event in events]
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                WITH active_queue AS MATERIALIZED (
                    SELECT uuid
                    FROM workspace_zulip_bridge.zulip_users
                    WHERE uuid = $1
                      AND queue_id = $2
                    FOR UPDATE
                ),
                incoming AS (
                    SELECT event_id,
                           event_type,
                           payload::jsonb
                    FROM unnest($3::bigint[], $4::text[], $5::text[])
                        AS event(event_id, event_type, payload)
                ),
                inserted AS (
                    INSERT INTO workspace_zulip_bridge.zulip_events (
                        zulip_user_uuid,
                        queue_id,
                        event_id,
                        event_type,
                        payload
                    )
                    SELECT active_queue.uuid,
                           $2,
                           incoming.event_id,
                           incoming.event_type,
                           incoming.payload
                    FROM incoming
                    CROSS JOIN active_queue
                    ON CONFLICT (zulip_user_uuid, queue_id, event_id) DO NOTHING
                    RETURNING 1
                ),
                cursor_update AS (
                    UPDATE workspace_zulip_bridge.zulip_users AS zulip_user
                    SET last_event_id = GREATEST(
                        COALESCE(zulip_user.last_event_id, -1),
                        $6
                    )
                    FROM active_queue
                    WHERE zulip_user.uuid = active_queue.uuid
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM inserted) AS inserted_count,
                       EXISTS(SELECT FROM cursor_update) AS cursor_updated
                """,
                user_uuid,
                queue_id,
                event_ids,
                event_types,
                payloads,
                last_event_id,
            )
        if row is None:
            return 0, False
        return row["inserted_count"], row["cursor_updated"]


class HistorySession:
    def __init__(
        self,
        pool: asyncpg.Pool,
        connection: PoolConnectionProxy,
        user_uuid: UUID,
        queue_id: str,
    ) -> None:
        self._pool = pool
        self._connection = connection
        self._user_uuid = user_uuid
        self._queue_id = queue_id
        self._endpoint: str | None = None
        self._closed = False

    async def initialize(self) -> None:
        self._endpoint = await self._connection.fetchval(
            """
            SELECT endpoint
            FROM workspace_zulip_bridge.zulip_users
            WHERE uuid = $1
              AND queue_id = $2
            """,
            self._user_uuid,
            self._queue_id,
        )
        if self._endpoint is None:
            raise RuntimeError("history session does not own the active queue")
        await self._connection.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS wzb_seen_messages (
                zulip_message_id bigint PRIMARY KEY
            ) ON COMMIT PRESERVE ROWS;

            CREATE TEMP TABLE IF NOT EXISTS wzb_message_page (
                uuid uuid NOT NULL,
                zulip_message_id bigint NOT NULL,
                chat_uuid uuid NOT NULL,
                chat_key text NOT NULL,
                topic_uuid uuid,
                topic_name text,
                sender_user_uuid uuid NOT NULL,
                content text NOT NULL,
                is_read boolean NOT NULL,
                is_starred boolean NOT NULL,
                is_collapsed boolean NOT NULL,
                is_mentioned boolean NOT NULL,
                is_stream_wildcard_mentioned boolean NOT NULL,
                is_topic_wildcard_mentioned boolean NOT NULL,
                has_alert_word boolean NOT NULL,
                is_historical boolean NOT NULL,
                reactions text NOT NULL,
                message_hash bytea NOT NULL,
                sent_at bigint NOT NULL
            ) ON COMMIT DELETE ROWS;

            TRUNCATE wzb_seen_messages, wzb_message_page;
            """
        )

    async def store_page(
        self,
        messages: Sequence[ZulipMessage],
    ) -> MessagePageWrite:
        if self._closed:
            raise RuntimeError("history session is closed")
        if not messages:
            return MessagePageWrite(0, 0, 0, 0, 0)
        if self._endpoint is None:
            raise RuntimeError("history session has no endpoint")
        records = [
            (
                stable_message_uuid(self._endpoint, message.message_id),
                message.message_id,
                (chat_uuid := stable_chat_uuid(self._endpoint, message.chat_key)),
                message.chat_key,
                (
                    stable_topic_uuid(chat_uuid, message.topic_name)
                    if message.topic_name is not None
                    else None
                ),
                message.topic_name,
                message.sender_user_uuid,
                message.content,
                message.is_read,
                message.is_starred,
                message.is_collapsed,
                message.is_mentioned,
                message.is_stream_wildcard_mentioned,
                message.is_topic_wildcard_mentioned,
                message.has_alert_word,
                message.is_historical,
                message.reactions_json,
                message.message_hash,
                message.sent_at,
            )
            for message in messages
        ]
        async with self._connection.transaction():
            active = await self._connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM workspace_zulip_bridge.zulip_users
                    WHERE uuid = $1
                      AND queue_id = $2
                )
                """,
                self._user_uuid,
                self._queue_id,
            )
            if not active:
                return MessagePageWrite(0, 0, 0, 0, 0)
            await self._connection.execute("TRUNCATE wzb_message_page")
            await self._connection.copy_records_to_table(
                "wzb_message_page",
                records=records,
                columns=(
                    "uuid",
                    "zulip_message_id",
                    "chat_uuid",
                    "chat_key",
                    "topic_uuid",
                    "topic_name",
                    "sender_user_uuid",
                    "content",
                    "is_read",
                    "is_starred",
                    "is_collapsed",
                    "is_mentioned",
                    "is_stream_wildcard_mentioned",
                    "is_topic_wildcard_mentioned",
                    "has_alert_word",
                    "is_historical",
                    "reactions",
                    "message_hash",
                    "sent_at",
                ),
            )
            topics_inserted = await self._connection.fetchval(
                """
                WITH inserted AS (
                    INSERT INTO workspace_zulip_bridge.zulip_topics (
                        uuid,
                        zulip_user_uuid,
                        zulip_chat_uuid,
                        name
                    )
                    SELECT DISTINCT page.topic_uuid,
                           $1,
                           page.chat_uuid,
                           page.topic_name
                    FROM wzb_message_page AS page
                    JOIN workspace_zulip_bridge.zulip_chats AS chat
                      ON chat.uuid = page.chat_uuid
                     AND chat.supplier_user_uuid = $1
                    WHERE page.topic_name IS NOT NULL
                    ON CONFLICT (uuid) DO UPDATE
                    SET zulip_user_uuid = EXCLUDED.zulip_user_uuid
                    WHERE zulip_topics.zulip_user_uuid IS DISTINCT FROM
                          EXCLUDED.zulip_user_uuid
                    RETURNING 1
                )
                SELECT count(*) FROM inserted
                """,
                self._user_uuid,
            )
            counts = await self._connection.fetchrow(
                """
                WITH resolved AS MATERIALIZED (
                    SELECT page.*
                    FROM wzb_message_page AS page
                    JOIN workspace_zulip_bridge.zulip_chats AS chat
                      ON chat.uuid = page.chat_uuid
                     AND chat.supplier_user_uuid = $1
                    JOIN workspace_zulip_bridge.zulip_chat_users AS chat_user
                      ON chat_user.zulip_chat_uuid = chat.uuid
                     AND chat_user.zulip_user_uuid = $1
                    LEFT JOIN workspace_zulip_bridge.zulip_topics AS topic
                      ON topic.uuid = page.topic_uuid
                    WHERE page.topic_name IS NULL OR topic.uuid IS NOT NULL
                ),
                seen AS (
                    INSERT INTO wzb_seen_messages (zulip_message_id)
                    SELECT zulip_message_id
                    FROM resolved
                    ON CONFLICT DO NOTHING
                    RETURNING 1
                ),
                changed AS (
                    INSERT INTO workspace_zulip_bridge.zulip_messages (
                        uuid,
                        zulip_user_uuid,
                        zulip_chat_uuid,
                        topic_uuid,
                        sender_user_uuid,
                        zulip_message_id,
                        content,
                        is_read,
                        is_starred,
                        is_collapsed,
                        is_mentioned,
                        is_stream_wildcard_mentioned,
                        is_topic_wildcard_mentioned,
                        has_alert_word,
                        is_historical,
                        reactions,
                        message_hash,
                        created_at
                    )
                    SELECT resolved.uuid,
                           $1,
                           resolved.chat_uuid,
                           resolved.topic_uuid,
                           resolved.sender_user_uuid,
                           resolved.zulip_message_id,
                           resolved.content,
                           resolved.is_read,
                           resolved.is_starred,
                           resolved.is_collapsed,
                           resolved.is_mentioned,
                           resolved.is_stream_wildcard_mentioned,
                           resolved.is_topic_wildcard_mentioned,
                           resolved.has_alert_word,
                           resolved.is_historical,
                           resolved.reactions::jsonb,
                           resolved.message_hash,
                           to_timestamp(resolved.sent_at)
                    FROM resolved
                    ON CONFLICT (uuid) DO UPDATE
                    SET zulip_user_uuid = EXCLUDED.zulip_user_uuid,
                        zulip_chat_uuid = EXCLUDED.zulip_chat_uuid,
                        topic_uuid = EXCLUDED.topic_uuid,
                        sender_user_uuid = EXCLUDED.sender_user_uuid,
                        content = EXCLUDED.content,
                        is_read = EXCLUDED.is_read,
                        is_starred = EXCLUDED.is_starred,
                        is_collapsed = EXCLUDED.is_collapsed,
                        is_mentioned = EXCLUDED.is_mentioned,
                        is_stream_wildcard_mentioned =
                            EXCLUDED.is_stream_wildcard_mentioned,
                        is_topic_wildcard_mentioned =
                            EXCLUDED.is_topic_wildcard_mentioned,
                        has_alert_word = EXCLUDED.has_alert_word,
                        is_historical = EXCLUDED.is_historical,
                        reactions = EXCLUDED.reactions,
                        message_hash = EXCLUDED.message_hash,
                        created_at = EXCLUDED.created_at,
                        updated_at = clock_timestamp()
                    WHERE zulip_messages.message_hash IS DISTINCT FROM
                          EXCLUDED.message_hash
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM resolved) AS resolved_count,
                       (SELECT count(*) FROM changed) AS changed_count,
                       (SELECT count(*) FROM seen) AS seen_count
                """,
                self._user_uuid,
            )
        if counts is None:
            raise RuntimeError("message page query returned no row")
        received = len(messages)
        resolved = counts["resolved_count"]
        if counts["seen_count"] != resolved:
            raise RuntimeError("message page failed to track scheduled messages")
        changed = counts["changed_count"]
        return MessagePageWrite(
            received=resolved,
            changed=changed,
            unchanged=resolved - changed,
            unassigned=received - resolved,
            topics_inserted=topics_inserted,
        )

    async def store_chats(self, chats: Sequence[ZulipChat]) -> int:
        if self._closed:
            raise RuntimeError("history session is closed")
        if not chats:
            return 0
        if self._endpoint is None:
            raise RuntimeError("history session has no endpoint")
        chat_uuids = [stable_chat_uuid(self._endpoint, chat.chat_key) for chat in chats]
        chat_types = [chat.chat_type for chat in chats]
        chat_keys = [chat.chat_key for chat in chats]
        names = [chat.name for chat in chats]
        roles = [chat.role for chat in chats]
        chat_parameters = [chat.chat_parameters_json for chat in chats]
        membership_parameters = [chat.membership_parameters_json for chat in chats]
        content_hashes = [chat.content_hash for chat in chats]
        membership_hashes = [chat.membership_hash for chat in chats]
        available_message_counts = [chat.available_message_count for chat in chats]
        changed = await self._connection.fetchval(
            """
            WITH active_queue AS MATERIALIZED (
                SELECT uuid
                FROM workspace_zulip_bridge.zulip_users
                WHERE uuid = $1
                  AND queue_id = $2
            ),
            incoming AS (
                SELECT uuid,
                       chat_type,
                       chat_key,
                       name,
                       role,
                       chat_parameters::jsonb,
                       membership_parameters::jsonb,
                       content_hash,
                       membership_hash,
                       available_message_count
                FROM unnest(
                    $3::uuid[],
                    $4::text[],
                    $5::text[],
                    $6::text[],
                    $7::text[],
                    $8::text[],
                    $9::text[],
                    $10::bytea[],
                    $11::bytea[],
                    $12::bigint[]
                ) AS chat(
                    uuid,
                    chat_type,
                    chat_key,
                    name,
                    role,
                    chat_parameters,
                    membership_parameters,
                    content_hash,
                    membership_hash,
                    available_message_count
                )
            ),
            chats_upserted AS (
                INSERT INTO workspace_zulip_bridge.zulip_chats (
                    uuid,
                    endpoint,
                    chat_type,
                    chat_key,
                    name,
                    chat_parameters,
                    content_hash
                )
                SELECT incoming.uuid,
                       $13,
                       incoming.chat_type,
                       incoming.chat_key,
                       incoming.name,
                       incoming.chat_parameters,
                       incoming.content_hash
                FROM incoming
                CROSS JOIN active_queue
                ON CONFLICT (uuid) DO UPDATE
                SET chat_type = EXCLUDED.chat_type,
                    name = EXCLUDED.name,
                    chat_parameters = EXCLUDED.chat_parameters,
                    content_hash = EXCLUDED.content_hash
                WHERE zulip_chats.content_hash IS DISTINCT FROM
                      EXCLUDED.content_hash
                RETURNING uuid
            ),
            memberships_upserted AS (
                INSERT INTO workspace_zulip_bridge.zulip_chat_users (
                    zulip_chat_uuid,
                    zulip_user_uuid,
                    role,
                    membership_parameters,
                    content_hash,
                    available_message_count
                )
                SELECT incoming.uuid,
                       active_queue.uuid,
                       incoming.role,
                       incoming.membership_parameters,
                       incoming.membership_hash,
                       incoming.available_message_count
                FROM incoming
                CROSS JOIN active_queue
                CROSS JOIN (
                    SELECT count(*) FROM chats_upserted
                ) AS chat_write_barrier
                ON CONFLICT (zulip_chat_uuid, zulip_user_uuid) DO UPDATE
                SET role = EXCLUDED.role,
                    membership_parameters = EXCLUDED.membership_parameters,
                    content_hash = EXCLUDED.content_hash
                WHERE zulip_chat_users.content_hash IS DISTINCT FROM
                      EXCLUDED.content_hash
                RETURNING 1
            )
            SELECT count(*) FROM memberships_upserted
            """,
            self._user_uuid,
            self._queue_id,
            chat_uuids,
            chat_types,
            chat_keys,
            names,
            roles,
            chat_parameters,
            membership_parameters,
            content_hashes,
            membership_hashes,
            available_message_counts,
            self._endpoint,
        )
        return int(changed)

    async def finish(self, chat_keys: Sequence[str]) -> HistoryWrite:
        if self._closed:
            raise RuntimeError("history session is closed")
        if self._endpoint is None:
            raise RuntimeError("history session has no endpoint")
        chat_uuids = [stable_chat_uuid(self._endpoint, key) for key in chat_keys]
        async with self._connection.transaction():
            user_row = await self._connection.fetchrow(
                """
                SELECT uuid
                FROM workspace_zulip_bridge.zulip_users
                WHERE uuid = $1
                  AND queue_id = $2
                FOR UPDATE
                """,
                self._user_uuid,
                self._queue_id,
            )
            if user_row is None:
                return HistoryWrite(False, 0, 0, 0)
            deleted_messages = await self._connection.fetchval(
                """
                WITH deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    WHERE message.zulip_user_uuid = $1
                      AND message.zulip_chat_uuid = ANY($2::uuid[])
                      AND NOT EXISTS (
                          SELECT 1
                          FROM wzb_seen_messages AS seen
                          WHERE seen.zulip_message_id = message.zulip_message_id
                      )
                    RETURNING 1
                )
                SELECT count(*) FROM deleted
                """,
                self._user_uuid,
                chat_uuids,
            )
            deleted_topics = await self._connection.fetchval(
                """
                WITH deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_topics AS topic
                    WHERE topic.zulip_user_uuid = $1
                      AND topic.zulip_chat_uuid = ANY($2::uuid[])
                      AND NOT EXISTS (
                          SELECT 1
                          FROM workspace_zulip_bridge.zulip_messages AS message
                          WHERE message.topic_uuid = topic.uuid
                      )
                    RETURNING 1
                )
                SELECT count(*) FROM deleted
                """,
                self._user_uuid,
                chat_uuids,
            )
            schedules_loaded = await self._connection.fetchval(
                """
                WITH loaded AS (
                    UPDATE workspace_zulip_bridge.zulip_chats
                    SET history_loaded_at = clock_timestamp()
                    WHERE uuid = ANY($2::uuid[])
                      AND supplier_user_uuid = $1
                      AND history_loaded_at IS NULL
                    RETURNING 1
                )
                SELECT count(*) FROM loaded
                """,
                self._user_uuid,
                chat_uuids,
            )
            await self._connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users AS zulip_user
                SET status = CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM workspace_zulip_bridge.zulip_chats AS chat
                        WHERE chat.supplier_user_uuid = $1
                          AND chat.history_loaded_at IS NULL
                    ) THEN 'backfilling'
                    ELSE 'active'
                END
                WHERE zulip_user.uuid = $1
                  AND zulip_user.queue_id = $2
                """,
                self._user_uuid,
                self._queue_id,
            )
            await self._connection.execute("TRUNCATE wzb_seen_messages")
        return HistoryWrite(
            True,
            int(deleted_messages),
            int(deleted_topics),
            int(schedules_loaded),
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._pool.release(self._connection)
