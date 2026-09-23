# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import hashlib
import json
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from typing import cast
from uuid import UUID

import asyncpg
from asyncpg.pool import PoolConnectionProxy

from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder
from workspace_zulip_bridge.message_history import message_flags_hash
from workspace_zulip_bridge.models import ChatCatalogWrite
from workspace_zulip_bridge.models import ChatScheduleReconcile
from workspace_zulip_bridge.models import HistoryWrite
from workspace_zulip_bridge.models import LiveMessageWrite
from workspace_zulip_bridge.models import MessagePageWrite
from workspace_zulip_bridge.models import ScheduledChat
from workspace_zulip_bridge.models import UserDirectoryWrite
from workspace_zulip_bridge.models import UserStatus
from workspace_zulip_bridge.models import ZulipAttachment
from workspace_zulip_bridge.models import ZulipChat
from workspace_zulip_bridge.models import ZulipChatCatalog
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.models import ZulipEvent
from workspace_zulip_bridge.models import ZulipMessage
from workspace_zulip_bridge.models import ZulipUser
from workspace_zulip_bridge.models import ZulipUserPresence
from workspace_zulip_bridge.models import ZulipUserProfileStatus
from workspace_zulip_bridge.models import ZulipUserTopic
from workspace_zulip_bridge.stable_ids import canonical_endpoint
from workspace_zulip_bridge.stable_ids import stable_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_file_uuid
from workspace_zulip_bridge.stable_ids import stable_message_flag_uuid
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_reaction_uuid
from workspace_zulip_bridge.stable_ids import stable_realm_uuid
from workspace_zulip_bridge.stable_ids import stable_stream_binding_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_binding_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid


class EventStore:
    SCHEDULE_STREAM_BATCH_SIZE = 64
    SCHEDULE_MESSAGE_BATCH_SIZE = 10_000

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def list_users(self) -> list[ZulipUser]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT connection.uuid, connection.realm_uuid,
                       realm.identity_key AS endpoint, connection.login,
                       connection.api_key, connection.queue_id,
                       connection.last_event_id,
                       connection.lifecycle_status AS status,
                       connection.streams_hash AS chats_hash,
                       zulip_user.zulip_user_id, zulip_user.full_name,
                       zulip_user.role, zulip_user.disabled,
                       EXISTS (
                           SELECT 1
                           FROM workspace_zulip_bridge.zulip_streams AS stream
                           WHERE stream.source_connection_uuid = connection.uuid
                             AND stream.history_loaded_at IS NULL
                       ) AS has_pending_history
                FROM workspace_zulip_bridge.zulip_connections AS connection
                JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                  ON zulip_user.uuid = connection.zulip_user_uuid
                JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = connection.realm_uuid
                WHERE connection.sync_enabled AND NOT zulip_user.disabled
                  AND NOT zulip_user.is_bot
                ORDER BY connection.uuid
                """
            )
        return [
            ZulipUser(
                uuid=row["uuid"],
                realm_uuid=row["realm_uuid"],
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
        connection_uuid: UUID,
        endpoint: str,
        zulip_user_id: int,
        full_name: str,
        role: int,
    ) -> bool:
        identity_uuid = stable_user_uuid(endpoint, zulip_user_id)
        async with self._pool.acquire() as connection:
            status = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users AS zulip_user
                SET full_name = $3, role = $4
                FROM workspace_zulip_bridge.zulip_connections AS connection
                JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = connection.realm_uuid
                WHERE connection.uuid = $1
                  AND connection.zulip_user_uuid = zulip_user.uuid
                  AND zulip_user.zulip_user_id = $2
                  AND zulip_user.uuid = $6
                  AND realm.identity_key = $5
                  AND connection.sync_enabled AND NOT zulip_user.disabled
                  AND NOT zulip_user.is_bot
                """,
                connection_uuid,
                zulip_user_id,
                full_name,
                role,
                canonical_endpoint(endpoint),
                identity_uuid,
            )
        return status == "UPDATE 1"

    async def list_user_chat_keys(self, user_uuid: UUID) -> set[str]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT stream.chat_key
                FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
                JOIN workspace_zulip_bridge.zulip_connections AS connection
                  ON connection.uuid = $1
                 AND connection.zulip_user_uuid = binding.zulip_user_uuid
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = binding.zulip_stream_uuid
                """,
                user_uuid,
            )
        return {row["chat_key"] for row in rows}

    async def list_scheduled_chat_keys(self, user_uuid: UUID) -> set[str]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT chat_key FROM workspace_zulip_bridge.zulip_streams "
                "WHERE source_connection_uuid = $1",
                user_uuid,
            )
        return {row["chat_key"] for row in rows}

    async def request_catalog_refresh(
        self,
        connection_uuid: UUID,
        queue_id: str,
    ) -> bool:
        async with self._pool.acquire() as connection:
            status = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET lifecycle_status = 'filling', streams_hash = NULL,
                    catalog_completed_at = NULL
                WHERE uuid = $1 AND queue_id = $2 AND sync_enabled
                """,
                connection_uuid,
                queue_id,
            )
        return status == "UPDATE 1"

    async def store_direct_message_chat(
        self,
        connection_uuid: UUID,
        queue_id: str,
        message: Mapping[str, object],
    ) -> bool:
        return await self._store_direct_message_chat(
            connection_uuid,
            queue_id,
            message,
        )

    async def _store_direct_message_chat(
        self,
        connection_uuid: UUID,
        queue_id: str,
        message: Mapping[str, object],
    ) -> bool:
        async with self._pool.acquire() as connection, connection.transaction():
            owner = await connection.fetchrow(
                """
                SELECT connection.realm_uuid, connection.zulip_user_uuid,
                       realm.identity_key AS endpoint,
                       zulip_user.zulip_user_id, zulip_user.full_name,
                       zulip_user.role
                FROM workspace_zulip_bridge.zulip_connections AS connection
                JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = connection.realm_uuid
                JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                  ON zulip_user.uuid = connection.zulip_user_uuid
                WHERE connection.uuid = $1 AND connection.queue_id = $2
                  AND connection.sync_enabled AND NOT zulip_user.disabled
                """,
                connection_uuid,
                queue_id,
            )
            if owner is None:
                return False
            builder = ChatCatalogBuilder(
                owner["zulip_user_id"],
                owner["full_name"],
                owner["role"],
            )
            chats = builder.add_direct_messages([message])
            if not chats:
                return False
            changed, _ = await _store_chats(
                connection,
                owner["endpoint"],
                owner["realm_uuid"],
                owner["zulip_user_uuid"],
                chats,
                replace_catalog=False,
            )
            if changed == 0:
                return False
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET lifecycle_status = CASE
                        WHEN lifecycle_status = 'filling' THEN 'filling'
                        ELSE 'scheduling'
                    END,
                    catalog_completed_at = CASE
                        WHEN lifecycle_status = 'filling' THEN NULL
                        ELSE COALESCE(catalog_completed_at, clock_timestamp())
                    END
                WHERE uuid = $1 AND queue_id = $2
                """,
                connection_uuid,
                queue_id,
            )
        return True

    async def has_pending_history(self, user_uuid: UUID, queue_id: str) -> bool:
        async with self._pool.acquire() as connection:
            return bool(
                await connection.fetchval(
                    """
                SELECT EXISTS (
                    SELECT 1
                    FROM workspace_zulip_bridge.zulip_connections AS connection
                    JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
                      ON binding.zulip_user_uuid = connection.zulip_user_uuid
                    JOIN workspace_zulip_bridge.zulip_streams AS stream
                      ON stream.uuid = binding.zulip_stream_uuid
                    WHERE connection.uuid = $1 AND connection.queue_id = $2
                      AND (
                          (stream.source_connection_uuid = connection.uuid
                           AND stream.history_loaded_at IS NULL)
                          OR (stream.history_loaded_at IS NOT NULL
                              AND binding.personal_state_loaded_at IS NULL)
                      )
                )
                """,
                    user_uuid,
                    queue_id,
                )
            )

    async def reconcile_chat_schedules(self) -> ChatScheduleReconcile:
        async with self._pool.acquire() as connection, connection.transaction():
            invalidated = await connection.fetchrow(
                """
                WITH invalid AS MATERIALIZED (
                    SELECT stream.uuid, stream.source_connection_uuid
                    FROM workspace_zulip_bridge.zulip_streams AS stream
                    LEFT JOIN workspace_zulip_bridge.zulip_connections AS connection
                      ON connection.uuid = stream.source_connection_uuid
                    LEFT JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                      ON zulip_user.uuid = connection.zulip_user_uuid
                    LEFT JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
                      ON binding.zulip_stream_uuid = stream.uuid
                     AND binding.zulip_user_uuid = connection.zulip_user_uuid
                    WHERE stream.source_connection_uuid IS NOT NULL
                      AND (connection.uuid IS NULL OR NOT connection.sync_enabled
                           OR zulip_user.disabled OR zulip_user.is_bot
                           OR binding.uuid IS NULL)
                    FOR UPDATE OF stream
                ), cleared AS (
                    UPDATE workspace_zulip_bridge.zulip_streams AS stream
                    SET source_connection_uuid = NULL,
                        history_loaded_at = NULL,
                        updated_at = clock_timestamp()
                    FROM invalid WHERE stream.uuid = invalid.uuid RETURNING 1
                )
                SELECT (SELECT count(*) FROM cleared) AS invalidated_count,
                       0::bigint AS messages_deleted
                """
            )
            assigned = await connection.fetchrow(
                """
                WITH ready_realms AS MATERIALIZED (
                    SELECT connection.realm_uuid
                    FROM workspace_zulip_bridge.zulip_connections AS connection
                    JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                      ON zulip_user.uuid = connection.zulip_user_uuid
                    WHERE connection.sync_enabled AND NOT zulip_user.disabled
                      AND NOT zulip_user.is_bot
                      AND connection.catalog_completed_at IS NOT NULL
                    GROUP BY connection.realm_uuid
                ), winners AS MATERIALIZED (
                    SELECT DISTINCT ON (stream.uuid)
                           stream.uuid AS stream_uuid,
                           connection.uuid AS connection_uuid,
                           CASE
                               WHEN binding.membership_parameters ->> 'color'
                                    ~ '^#[0-9A-Fa-f]{6}$'
                               THEN ('x' || substr(
                                   binding.membership_parameters ->> 'color', 2
                               ))::bit(24)::int
                               ELSE NULL
                           END AS color
                    FROM workspace_zulip_bridge.zulip_streams AS stream
                    JOIN ready_realms ON ready_realms.realm_uuid = stream.realm_uuid
                    JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
                      ON binding.zulip_stream_uuid = stream.uuid
                    JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                      ON zulip_user.uuid = binding.zulip_user_uuid
                     AND NOT zulip_user.disabled AND NOT zulip_user.is_bot
                    JOIN workspace_zulip_bridge.zulip_connections AS connection
                      ON connection.zulip_user_uuid = zulip_user.uuid
                     AND connection.sync_enabled
                     AND connection.catalog_completed_at IS NOT NULL
                    ORDER BY stream.uuid, zulip_user.role,
                             zulip_user.uuid,
                             connection.uuid
                ), changed AS MATERIALIZED (
                    SELECT stream.uuid,
                           stream.source_connection_uuid AS old_connection_uuid,
                           winners.connection_uuid AS new_connection_uuid,
                           winners.color AS new_color
                    FROM workspace_zulip_bridge.zulip_streams AS stream
                    JOIN winners ON winners.stream_uuid = stream.uuid
                    WHERE stream.source_connection_uuid IS DISTINCT FROM
                          winners.connection_uuid
                       OR EXISTS (
                           SELECT 1
                           FROM workspace_zulip_bridge.zulip_messages AS message
                           WHERE message.zulip_stream_uuid = stream.uuid
                             AND message.source_connection_uuid IS DISTINCT FROM
                                 winners.connection_uuid
                       )
                    ORDER BY stream.uuid
                    FOR UPDATE OF stream SKIP LOCKED
                    LIMIT $1
                ), message_counts AS MATERIALIZED (
                    SELECT changed.uuid AS stream_uuid,
                           count(message.uuid) AS mismatch_count
                    FROM changed
                    LEFT JOIN workspace_zulip_bridge.zulip_messages AS message
                      ON message.zulip_stream_uuid = changed.uuid
                     AND message.source_connection_uuid IS DISTINCT FROM
                         changed.new_connection_uuid
                    GROUP BY changed.uuid
                ), candidates AS MATERIALIZED (
                    SELECT message.uuid, changed.uuid AS stream_uuid,
                           changed.new_connection_uuid
                    FROM changed
                    JOIN workspace_zulip_bridge.zulip_messages AS message
                      ON message.zulip_stream_uuid = changed.uuid
                     AND message.source_connection_uuid IS DISTINCT FROM
                         changed.new_connection_uuid
                    ORDER BY changed.uuid, message.uuid
                    FOR UPDATE OF message SKIP LOCKED
                    LIMIT $2
                ), adopted AS (
                    UPDATE workspace_zulip_bridge.zulip_messages AS message
                    SET source_connection_uuid = candidates.new_connection_uuid
                    FROM candidates
                    WHERE message.uuid = candidates.uuid
                    RETURNING candidates.stream_uuid
                ), adopted_counts AS MATERIALIZED (
                    SELECT stream_uuid, count(*) AS adopted_count
                    FROM adopted
                    GROUP BY stream_uuid
                ), updated AS (
                    UPDATE workspace_zulip_bridge.zulip_streams AS stream
                    SET source_connection_uuid = changed.new_connection_uuid,
                        color = changed.new_color,
                        history_loaded_at = NULL,
                        updated_at = clock_timestamp()
                    FROM changed
                    JOIN message_counts
                      ON message_counts.stream_uuid = changed.uuid
                    LEFT JOIN adopted_counts
                      ON adopted_counts.stream_uuid = changed.uuid
                    WHERE stream.uuid = changed.uuid
                      AND message_counts.mismatch_count =
                          COALESCE(adopted_counts.adopted_count, 0)
                    RETURNING 1
                )
                SELECT (SELECT count(*) FROM updated) AS assigned_count,
                       0::bigint AS messages_deleted
                """,
                self.SCHEDULE_STREAM_BATCH_SIZE,
                self.SCHEDULE_MESSAGE_BATCH_SIZE,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections AS connection
                SET lifecycle_status = CASE WHEN EXISTS (
                    SELECT 1
                    FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
                    JOIN workspace_zulip_bridge.zulip_streams AS stream
                      ON stream.uuid = binding.zulip_stream_uuid
                    WHERE binding.zulip_user_uuid = connection.zulip_user_uuid
                      AND (
                          (stream.source_connection_uuid = connection.uuid
                           AND stream.history_loaded_at IS NULL)
                          OR (stream.history_loaded_at IS NOT NULL
                              AND binding.personal_state_loaded_at IS NULL)
                      )
                ) THEN 'backfilling' ELSE 'active' END
                FROM workspace_zulip_bridge.zulip_users AS zulip_user
                WHERE connection.zulip_user_uuid = zulip_user.uuid
                  AND connection.sync_enabled
                  AND NOT zulip_user.disabled AND NOT zulip_user.is_bot
                  AND connection.catalog_completed_at IS NOT NULL
                """
            )
        if invalidated is None or assigned is None:
            raise RuntimeError("chat schedule reconciliation returned no row")
        return ChatScheduleReconcile(
            invalidated=invalidated["invalidated_count"],
            assigned=assigned["assigned_count"],
            messages_deleted=(
                invalidated["messages_deleted"] + assigned["messages_deleted"]
            ),
        )

    async def store_user_directory(
        self, endpoint: str, users: Sequence[ZulipDirectoryUser]
    ) -> UserDirectoryWrite:
        endpoint = canonical_endpoint(endpoint)
        realm_uuid = stable_realm_uuid(endpoint)
        users = tuple(users)
        bot_count = sum(user.is_bot for user in users)
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_realms
                    (uuid, identity_key, endpoint)
                VALUES ($1, $2, $2)
                ON CONFLICT (uuid) DO UPDATE SET endpoint = EXCLUDED.endpoint
                """,
                realm_uuid,
                endpoint,
            )
            row = await connection.fetchrow(
                """
                WITH incoming AS MATERIALIZED (
                    SELECT uuid, user_id, login, full_name, role, disabled, is_bot,
                           avatar_url, profile_hash
                    FROM unnest($2::uuid[], $3::bigint[], $4::text[], $5::text[],
                                $6::smallint[], $7::boolean[], $8::boolean[],
                                $9::text[], $10::bytea[])
                      AS directory(uuid, user_id, login, full_name, role, disabled,
                                   is_bot, avatar_url, profile_hash)
                ), upserted AS (
                    INSERT INTO workspace_zulip_bridge.zulip_users
                        (uuid, realm_uuid, zulip_user_id, login, full_name, role,
                         disabled, is_bot, avatar_url, profile_hash)
                    SELECT uuid, $1, user_id, login, full_name, role, disabled,
                           is_bot, avatar_url, profile_hash FROM incoming
                    ON CONFLICT (uuid) DO UPDATE
                    SET login = EXCLUDED.login, full_name = EXCLUDED.full_name,
                        role = EXCLUDED.role, disabled = EXCLUDED.disabled,
                        is_bot = EXCLUDED.is_bot,
                        avatar_url = EXCLUDED.avatar_url,
                        profile_hash = EXCLUDED.profile_hash,
                        updated_at = clock_timestamp()
                    WHERE (zulip_users.login, zulip_users.full_name, zulip_users.role,
                           zulip_users.disabled, zulip_users.is_bot,
                           zulip_users.avatar_url)
                          IS DISTINCT FROM
                          (EXCLUDED.login, EXCLUDED.full_name, EXCLUDED.role,
                           EXCLUDED.disabled, EXCLUDED.is_bot,
                           EXCLUDED.avatar_url)
                    RETURNING 1
                ) SELECT count(*) AS changed_count FROM upserted
                """,
                realm_uuid,
                [stable_user_uuid(endpoint, user.user_id) for user in users],
                [user.user_id for user in users],
                [user.login for user in users],
                [user.full_name for user in users],
                [user.role for user in users],
                [user.disabled for user in users],
                [user.is_bot for user in users],
                [user.avatar_url for user in users],
                [_directory_user_profile_hash(user) for user in users],
            )
        if row is None:
            raise RuntimeError("user directory query returned no row")
        return UserDirectoryWrite(
            users=len(users), bots=bot_count, changed=row["changed_count"]
        )

    async def store_user_topics(
        self,
        connection_uuid: UUID,
        queue_id: str,
        topics: Sequence[ZulipUserTopic],
        *,
        replace_all: bool,
    ) -> int | None:
        async with self._pool.acquire() as connection, connection.transaction():
            owner = await connection.fetchrow(
                """
                SELECT source.realm_uuid, source.zulip_user_uuid
                FROM workspace_zulip_bridge.zulip_connections AS source
                WHERE source.uuid = $1 AND source.queue_id = $2
                  AND source.sync_enabled
                """,
                connection_uuid,
                queue_id,
            )
            if owner is None:
                return None
            return await _store_user_topics(
                connection,
                owner["realm_uuid"],
                owner["zulip_user_uuid"],
                topics,
                replace_all=replace_all,
            )

    async def store_user_presences(
        self,
        endpoint: str,
        presences: Sequence[ZulipUserPresence],
    ) -> int:
        return await self._store_user_live_state(
            endpoint,
            presences=presences,
            statuses=(),
        )

    async def set_presence_offline_threshold(
        self,
        endpoint: str,
        threshold_seconds: int,
    ) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_realms
            SET presence_offline_threshold_seconds = $2,
                updated_at = clock_timestamp()
            WHERE uuid = $1
              AND presence_offline_threshold_seconds IS DISTINCT FROM $2
            """,
            stable_realm_uuid(endpoint),
            threshold_seconds,
        )

    async def expire_user_presences(self, batch_size: int) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                WITH expired AS MATERIALIZED (
                    SELECT zulip_user.uuid
                    FROM workspace_zulip_bridge.zulip_users AS zulip_user
                    JOIN workspace_zulip_bridge.zulip_realms AS realm
                      ON realm.uuid = zulip_user.realm_uuid
                    WHERE zulip_user.presence_status <> 'offline'
                      AND zulip_user.last_ping_at < (
                          clock_timestamp() - make_interval(
                              secs => realm.presence_offline_threshold_seconds
                          )
                      )
                    ORDER BY zulip_user.last_ping_at, zulip_user.uuid
                    LIMIT $1
                    FOR UPDATE OF zulip_user SKIP LOCKED
                )
                UPDATE workspace_zulip_bridge.zulip_users AS zulip_user
                SET presence_status = 'offline',
                    updated_at = clock_timestamp()
                FROM expired
                WHERE zulip_user.uuid = expired.uuid
                RETURNING zulip_user.*
                """,
                batch_size,
            )
            if rows:
                await connection.executemany(
                    """
                    UPDATE workspace_zulip_bridge.zulip_users
                    SET profile_hash = $2 WHERE uuid = $1
                    """,
                    [(row["uuid"], _user_profile_hash(dict(row))) for row in rows],
                )
        return len(rows)

    async def store_user_statuses(
        self,
        endpoint: str,
        statuses: Sequence[ZulipUserProfileStatus],
    ) -> int:
        return await self._store_user_live_state(
            endpoint,
            presences=(),
            statuses=statuses,
        )

    async def _store_user_live_state(
        self,
        endpoint: str,
        *,
        presences: Sequence[ZulipUserPresence],
        statuses: Sequence[ZulipUserProfileStatus],
    ) -> int:
        realm_uuid = stable_realm_uuid(endpoint)
        changed_user_ids: set[int] = set()
        async with self._pool.acquire() as connection, connection.transaction():
            for presence in presences:
                status = await connection.execute(
                    """
                    UPDATE workspace_zulip_bridge.zulip_users
                    SET presence_status = $3,
                        last_ping_at = to_timestamp($4),
                        updated_at = clock_timestamp()
                    WHERE realm_uuid = $1 AND zulip_user_id = $2
                      AND (
                          last_ping_at IS NULL
                          OR last_ping_at <= to_timestamp($4)
                      )
                      AND (presence_status, last_ping_at) IS DISTINCT FROM
                          ($3, to_timestamp($4))
                    """,
                    realm_uuid,
                    presence.user_id,
                    presence.status,
                    presence.last_ping_at,
                )
                if status == "UPDATE 1":
                    changed_user_ids.add(presence.user_id)
            for user_status in statuses:
                status = await connection.execute(
                    """
                    UPDATE workspace_zulip_bridge.zulip_users
                    SET status_text = CASE WHEN $5 THEN $3 ELSE status_text END,
                        status_emoji = CASE WHEN $6 THEN $4 ELSE status_emoji END,
                        updated_at = clock_timestamp()
                    WHERE realm_uuid = $1 AND zulip_user_id = $2
                      AND (
                          ($5 AND status_text IS DISTINCT FROM $3)
                          OR ($6 AND status_emoji IS DISTINCT FROM $4)
                      )
                    """,
                    realm_uuid,
                    user_status.user_id,
                    user_status.status_text,
                    user_status.status_emoji,
                    user_status.update_status_text,
                    user_status.update_status_emoji,
                )
                if status == "UPDATE 1":
                    changed_user_ids.add(user_status.user_id)
            if changed_user_ids:
                rows = await connection.fetch(
                    """
                    SELECT * FROM workspace_zulip_bridge.zulip_users
                    WHERE realm_uuid = $1
                      AND zulip_user_id = ANY($2::bigint[])
                    """,
                    realm_uuid,
                    sorted(changed_user_ids),
                )
                await connection.executemany(
                    """
                    UPDATE workspace_zulip_bridge.zulip_users
                    SET profile_hash = $2 WHERE uuid = $1
                    """,
                    [(row["uuid"], _user_profile_hash(dict(row))) for row in rows],
                )
        return len(changed_user_ids)

    async def store_user_attachments(
        self,
        connection_uuid: UUID,
        queue_id: str,
        attachments: Sequence[ZulipAttachment],
        *,
        replace_all: bool,
    ) -> int:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                owner = await connection.fetchrow(
                    """
                    SELECT connection.realm_uuid, connection.zulip_user_uuid,
                           realm.identity_key AS endpoint
                    FROM workspace_zulip_bridge.zulip_connections AS connection
                    JOIN workspace_zulip_bridge.zulip_realms AS realm
                      ON realm.uuid = connection.realm_uuid
                    WHERE connection.uuid = $1 AND connection.queue_id = $2
                    """,
                    connection_uuid,
                    queue_id,
                )
                if owner is None:
                    return 0
                file_uuids = [
                    stable_file_uuid(owner["endpoint"], attachment.source_path)
                    for attachment in attachments
                ]
                row = await connection.fetchrow(
                    """
                    WITH active_connection AS MATERIALIZED (
                        SELECT $1::uuid AS uuid
                    ), incoming_json AS MATERIALIZED (
                        SELECT file_uuid, attachment_id, source_path, name, size_bytes,
                               source_created_at, message_ids, metadata_hash
                        FROM unnest(
                            $4::uuid[], $5::bigint[], $6::text[], $7::text[],
                            $8::bigint[], $9::bigint[], $10::jsonb[], $11::bytea[]
                        ) AS attachment(
                            file_uuid, attachment_id, source_path, name, size_bytes,
                            source_created_at, message_ids, metadata_hash
                        )
                    ), incoming AS MATERIALIZED (
                        SELECT file_uuid, attachment_id, source_path, name, size_bytes,
                               source_created_at,
                               ARRAY(
                                   SELECT value::bigint
                                   FROM jsonb_array_elements_text(message_ids) AS value
                               ) AS message_ids,
                               metadata_hash
                        FROM incoming_json
                    ), upserted AS (
                        INSERT INTO workspace_zulip_bridge.zulip_files
                            (uuid, realm_uuid, owner_user_uuid, zulip_attachment_id,
                             source_path, name, size_bytes, source_created_at,
                             message_ids, metadata_hash)
                        SELECT file_uuid, $2, $3, attachment_id, source_path, name,
                               size_bytes, to_timestamp(source_created_at), message_ids,
                               metadata_hash
                        FROM incoming ORDER BY file_uuid
                        ON CONFLICT (uuid) DO UPDATE SET
                            owner_user_uuid = EXCLUDED.owner_user_uuid,
                            zulip_attachment_id = EXCLUDED.zulip_attachment_id,
                            name = EXCLUDED.name,
                            size_bytes = EXCLUDED.size_bytes,
                            source_created_at = EXCLUDED.source_created_at,
                            message_ids = EXCLUDED.message_ids,
                            metadata_hash = EXCLUDED.metadata_hash
                        WHERE (zulip_files.owner_user_uuid,
                               zulip_files.zulip_attachment_id, zulip_files.name,
                               zulip_files.size_bytes, zulip_files.source_created_at,
                               zulip_files.message_ids, zulip_files.metadata_hash)
                            IS DISTINCT FROM
                              (EXCLUDED.owner_user_uuid,
                               EXCLUDED.zulip_attachment_id, EXCLUDED.name,
                               EXCLUDED.size_bytes, EXCLUDED.source_created_at,
                               EXCLUDED.message_ids, EXCLUDED.metadata_hash)
                        RETURNING uuid
                    ), removed AS (
                        DELETE FROM workspace_zulip_bridge.zulip_files AS file
                        WHERE $12 AND file.realm_uuid = $2
                          AND file.owner_user_uuid = $3
                          AND NOT (file.uuid = ANY($4::uuid[]))
                        RETURNING uuid
                    ), changes AS (
                        SELECT uuid, 'upsert'::text AS action FROM upserted
                        UNION ALL SELECT uuid, 'delete'::text FROM removed
                    ), outbox AS (
                        INSERT INTO workspace_zulip_bridge.workspace_outbox
                            (realm_uuid, entity_type, action, entity_uuid)
                        SELECT $2, 'file', action, uuid FROM changes
                        ON CONFLICT (realm_uuid, entity_type, entity_uuid)
                            WHERE delivery_status = 'pending'
                        DO UPDATE SET action = EXCLUDED.action,
                                      updated_at = clock_timestamp()
                        RETURNING 1
                    )
                    SELECT (SELECT count(*) FROM changes) AS changed_count
                    FROM active_connection
                    """,
                    connection_uuid,
                    owner["realm_uuid"],
                    owner["zulip_user_uuid"],
                    file_uuids,
                    [attachment.attachment_id for attachment in attachments],
                    [attachment.source_path for attachment in attachments],
                    [attachment.name for attachment in attachments],
                    [attachment.size_bytes for attachment in attachments],
                    [attachment.created_at for attachment in attachments],
                    [json.dumps(attachment.message_ids) for attachment in attachments],
                    [attachment.metadata_hash for attachment in attachments],
                    replace_all,
                )
            if row is None:
                raise RuntimeError("attachment metadata query returned no row")
            async with connection.transaction():
                await connection.execute(
                    """
                    WITH active_connection AS MATERIALIZED (
                        SELECT 1
                        FROM workspace_zulip_bridge.zulip_connections
                        WHERE uuid = $1 AND queue_id = $2
                    ), incoming_json AS MATERIALIZED (
                        SELECT file_uuid, message_ids
                        FROM unnest($4::uuid[], $5::jsonb[])
                            AS attachment(file_uuid, message_ids)
                    ), incoming AS MATERIALIZED (
                        SELECT file_uuid,
                               ARRAY(
                                   SELECT value::bigint
                                   FROM jsonb_array_elements_text(message_ids) AS value
                               ) AS message_ids
                        FROM incoming_json
                    ), removed_links AS (
                        DELETE FROM workspace_zulip_bridge.zulip_message_files AS link
                        USING incoming, active_connection
                        WHERE link.file_uuid = incoming.file_uuid
                          AND NOT EXISTS (
                              SELECT 1
                              FROM workspace_zulip_bridge.zulip_messages AS message
                              WHERE message.uuid = link.message_uuid
                                AND message.zulip_message_id = ANY(incoming.message_ids)
                          )
                        RETURNING 1
                    ), links AS (
                        INSERT INTO workspace_zulip_bridge.zulip_message_files
                            (message_uuid, file_uuid, position)
                        SELECT message.uuid, incoming.file_uuid, 0
                        FROM active_connection
                        CROSS JOIN incoming
                        JOIN workspace_zulip_bridge.zulip_messages AS message
                          ON message.realm_uuid = $3
                         AND message.zulip_message_id = ANY(incoming.message_ids)
                        ORDER BY message.uuid, incoming.file_uuid
                        ON CONFLICT (message_uuid, file_uuid) DO NOTHING
                        RETURNING 1
                    )
                    SELECT (SELECT count(*) FROM removed_links)
                         + (SELECT count(*) FROM links)
                    """,
                    connection_uuid,
                    queue_id,
                    owner["realm_uuid"],
                    file_uuids,
                    [json.dumps(attachment.message_ids) for attachment in attachments],
                )
        return int(row["changed_count"])

    async def remove_user_attachment(
        self,
        connection_uuid: UUID,
        queue_id: str,
        attachment_id: int,
    ) -> bool:
        async with self._pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """
                WITH owner AS MATERIALIZED (
                    SELECT realm_uuid, zulip_user_uuid
                    FROM workspace_zulip_bridge.zulip_connections
                    WHERE uuid = $1 AND queue_id = $2
                ), removed AS (
                    DELETE FROM workspace_zulip_bridge.zulip_files AS file
                    USING owner
                    WHERE file.realm_uuid = owner.realm_uuid
                      AND file.owner_user_uuid = owner.zulip_user_uuid
                      AND file.zulip_attachment_id = $3
                    RETURNING file.uuid, file.realm_uuid
                ), outbox AS (
                    INSERT INTO workspace_zulip_bridge.workspace_outbox
                        (realm_uuid, entity_type, action, entity_uuid)
                    SELECT realm_uuid, 'file', 'delete', uuid FROM removed
                    ON CONFLICT (realm_uuid, entity_type, entity_uuid)
                        WHERE delivery_status = 'pending'
                    DO UPDATE SET action = 'delete', updated_at = clock_timestamp()
                    RETURNING 1
                )
                SELECT EXISTS(SELECT FROM removed) AS removed
                """,
                connection_uuid,
                queue_id,
                attachment_id,
            )
        return bool(row and row["removed"])

    async def begin_history(self, user_uuid: UUID, queue_id: str) -> "HistorySession":
        connection = await self._pool.acquire()
        session = HistorySession(
            self._pool,
            connection,
            user_uuid,
            queue_id,
        )
        try:
            await session.initialize()
        except BaseException:
            await self._pool.release(connection)
            raise
        return session

    async def link_local_message(
        self,
        connection_uuid: UUID,
        queue_id: str,
        workspace_uuid: UUID,
        zulip_message_id: int,
    ) -> bool:
        row = await self._pool.fetchrow(
            """
            INSERT INTO workspace_zulip_bridge.zulip_entity_links (
                realm_uuid, entity_type, workspace_uuid, zulip_external_key
            )
            SELECT connection.realm_uuid, 'message', $3, $4
            FROM workspace_zulip_bridge.zulip_connections AS connection
            WHERE connection.uuid = $1 AND connection.queue_id = $2
            ON CONFLICT (realm_uuid, entity_type, workspace_uuid) DO UPDATE
            SET zulip_external_key = EXCLUDED.zulip_external_key,
                updated_at = clock_timestamp()
            RETURNING workspace_uuid
            """,
            connection_uuid,
            queue_id,
            workspace_uuid,
            str(zulip_message_id),
        )
        return row is not None

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
            page = await history.store_page(messages)
        finally:
            await history.close()
        if messages:
            await self._enqueue_live_message_diffs(
                user_uuid,
                queue_id,
                [message.message_id for message in messages],
            )
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    partition_key, direction, delivery_priority,
                    source_hash, target_hash, source_updated_at, target_updated_at
                )
                SELECT realm.workspace_provider_uuid, 'messages', source.uuid,
                       source.realm_uuid, source.zulip_stream_uuid,
                       'to_workspace', 0, NULL, target.content_hash,
                       clock_timestamp(), target.source_updated_at
                FROM workspace_zulip_bridge.zulip_connections AS owner
                JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = owner.realm_uuid
                 AND realm.workspace_provider_uuid IS NOT NULL
                JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
                  ON mirror.provider_uuid = realm.workspace_provider_uuid
                 AND mirror.bootstrap_status = 'ready'
                 AND mirror.active_generation IS NOT NULL
                JOIN workspace_zulip_bridge.zulip_messages AS source
                  ON source.source_connection_uuid = owner.uuid
                 AND source.zulip_message_id = ANY($3::bigint[])
                JOIN workspace_zulip_bridge.workspace_messages AS target
                  ON target.provider_uuid = realm.workspace_provider_uuid
                 AND target.snapshot_generation = mirror.active_generation
                 AND target.uuid = source.uuid
                WHERE owner.uuid = $1 AND owner.queue_id = $2
                ON CONFLICT (provider_uuid, entity_type, entity_uuid)
                DO UPDATE SET direction = 'to_workspace', delivery_priority = 0,
                    partition_key = EXCLUDED.partition_key,
                    source_hash = NULL, target_hash = EXCLUDED.target_hash,
                    source_updated_at = EXCLUDED.source_updated_at,
                    target_updated_at = EXCLUDED.target_updated_at,
                    processing_status = 'pending', attempt_count = 0,
                    available_at = clock_timestamp(), claimed_at = NULL,
                    processed_at = NULL, last_error = NULL,
                    updated_at = clock_timestamp()
                """,
                user_uuid,
                queue_id,
                list(deleted_message_ids),
            )
            deleted = await connection.fetchval(
                """
                WITH active AS (
                    SELECT uuid FROM workspace_zulip_bridge.zulip_connections
                    WHERE uuid = $1 AND queue_id = $2
                ), removed AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    USING active
                    WHERE message.source_connection_uuid = active.uuid
                      AND message.zulip_message_id = ANY($3::bigint[])
                    RETURNING 1
                ) SELECT count(*) FROM removed
                """,
                user_uuid,
                queue_id,
                list(deleted_message_ids),
            )
        return LiveMessageWrite(
            messages_changed=page.changed,
            messages_unchanged=page.unchanged,
            messages_deleted=int(deleted),
            topics_inserted=page.topics_inserted,
            flags_changed=page.flags_changed,
            reactions_changed=page.reactions_changed,
            files_changed=page.files_changed,
        )

    async def _enqueue_live_message_diffs(
        self,
        connection_uuid: UUID,
        queue_id: str,
        message_ids: Sequence[int],
    ) -> int:
        result = await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                partition_key, direction, delivery_priority,
                source_hash, target_hash, source_updated_at, target_updated_at
            )
            SELECT realm.workspace_provider_uuid, 'messages', source.uuid,
                   source.realm_uuid, source.zulip_stream_uuid,
                   CASE WHEN target.source_updated_at > source.source_updated_at
                        THEN 'to_zulip' ELSE 'to_workspace' END,
                   0, source.content_hash, target.content_hash,
                   source.source_updated_at, target.source_updated_at
            FROM workspace_zulip_bridge.zulip_connections AS owner
            JOIN workspace_zulip_bridge.zulip_realms AS realm
              ON realm.uuid = owner.realm_uuid
             AND realm.workspace_provider_uuid IS NOT NULL
            JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
              ON mirror.provider_uuid = realm.workspace_provider_uuid
             AND mirror.bootstrap_status = 'ready'
             AND mirror.active_generation IS NOT NULL
            JOIN workspace_zulip_bridge.zulip_messages AS source
              ON source.realm_uuid = owner.realm_uuid
             AND source.zulip_message_id = ANY($3::bigint[])
            JOIN workspace_zulip_bridge.zulip_users AS sender
              ON sender.uuid = source.sender_user_uuid
            LEFT JOIN workspace_zulip_bridge.workspace_messages AS target
              ON target.provider_uuid = realm.workspace_provider_uuid
             AND target.snapshot_generation = mirror.active_generation
             AND target.uuid = source.uuid
            WHERE owner.uuid = $1 AND owner.queue_id = $2
              AND EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.workspace_streams AS parent
                  WHERE parent.provider_uuid = realm.workspace_provider_uuid
                    AND parent.snapshot_generation = mirror.active_generation
                    AND parent.uuid = source.zulip_stream_uuid
              )
              AND EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.workspace_topics AS parent
                  WHERE parent.provider_uuid = realm.workspace_provider_uuid
                    AND parent.snapshot_generation = mirror.active_generation
                    AND parent.uuid = source.topic_uuid
              )
              AND EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.workspace_users AS parent
                  WHERE parent.provider_uuid = realm.workspace_provider_uuid
                    AND parent.snapshot_generation = mirror.active_generation
                    AND parent.uuid = COALESCE(
                        sender.workspace_user_uuid, source.sender_user_uuid
                    )
              )
            ON CONFLICT (provider_uuid, entity_type, entity_uuid)
            DO UPDATE SET
                direction = EXCLUDED.direction,
                partition_key = EXCLUDED.partition_key,
                delivery_priority = 0,
                source_hash = EXCLUDED.source_hash,
                target_hash = EXCLUDED.target_hash,
                source_updated_at = EXCLUDED.source_updated_at,
                target_updated_at = EXCLUDED.target_updated_at,
                processing_status = CASE
                    WHEN sync_diffs.source_hash IS DISTINCT FROM EXCLUDED.source_hash
                      OR sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                      OR sync_diffs.source_updated_at
                         IS DISTINCT FROM EXCLUDED.source_updated_at
                      OR sync_diffs.target_updated_at
                         IS DISTINCT FROM EXCLUDED.target_updated_at
                      OR sync_diffs.direction IS DISTINCT FROM EXCLUDED.direction
                    THEN 'pending' ELSE sync_diffs.processing_status END,
                available_at = CASE
                    WHEN sync_diffs.source_hash IS DISTINCT FROM EXCLUDED.source_hash
                      OR sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                      OR sync_diffs.source_updated_at
                         IS DISTINCT FROM EXCLUDED.source_updated_at
                      OR sync_diffs.target_updated_at
                         IS DISTINCT FROM EXCLUDED.target_updated_at
                      OR sync_diffs.direction IS DISTINCT FROM EXCLUDED.direction
                    THEN clock_timestamp() ELSE sync_diffs.available_at END,
                last_error = CASE
                    WHEN sync_diffs.source_hash IS DISTINCT FROM EXCLUDED.source_hash
                      OR sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                      OR sync_diffs.source_updated_at
                         IS DISTINCT FROM EXCLUDED.source_updated_at
                      OR sync_diffs.target_updated_at
                         IS DISTINCT FROM EXCLUDED.target_updated_at
                      OR sync_diffs.direction IS DISTINCT FROM EXCLUDED.direction
                    THEN NULL ELSE sync_diffs.last_error END,
                updated_at = clock_timestamp()
            """,
            connection_uuid,
            queue_id,
            list(message_ids),
        )
        return int(result.rsplit(" ", 1)[-1])

    async def apply_message_flags(
        self,
        connection_uuid: UUID,
        queue_id: str,
        message_ids: Sequence[int],
        field: str,
        value: bool,
    ) -> int:
        allowed_fields = (
            "is_read",
            "is_starred",
            "is_collapsed",
            "is_mentioned",
            "is_stream_wildcard_mentioned",
            "is_topic_wildcard_mentioned",
            "has_alert_word",
            "is_historical",
        )
        if field not in allowed_fields:
            raise ValueError(f"unsupported message flag field: {field}")
        async with self._pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                SELECT source.realm_uuid, source.zulip_user_uuid,
                       message.uuid AS message_uuid, message.zulip_stream_uuid,
                       flags.uuid, flags.is_read, flags.is_starred,
                       flags.is_collapsed, flags.is_mentioned,
                       flags.is_stream_wildcard_mentioned,
                       flags.is_topic_wildcard_mentioned, flags.has_alert_word,
                       flags.is_historical
                FROM workspace_zulip_bridge.zulip_connections AS source
                JOIN workspace_zulip_bridge.zulip_messages AS message
                  ON message.realm_uuid = source.realm_uuid
                JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
                  ON binding.zulip_stream_uuid = message.zulip_stream_uuid
                 AND binding.zulip_user_uuid = source.zulip_user_uuid
                LEFT JOIN workspace_zulip_bridge.zulip_message_flags AS flags
                  ON flags.message_uuid = message.uuid
                 AND flags.zulip_user_uuid = source.zulip_user_uuid
                WHERE source.uuid = $1 AND source.queue_id = $2
                  AND message.zulip_message_id = ANY($3::bigint[])
                """,
                connection_uuid,
                queue_id,
                list(message_ids),
            )
            changed: list[tuple[object, ...]] = []
            for row in rows:
                state = {
                    name: bool(row[name]) if row["uuid"] is not None else False
                    for name in allowed_fields
                }
                if state[field] == value:
                    continue
                state[field] = value
                changed.append(
                    (
                        row["uuid"]
                        or stable_message_flag_uuid(
                            row["message_uuid"], row["zulip_user_uuid"]
                        ),
                        row["realm_uuid"],
                        row["zulip_stream_uuid"],
                        row["message_uuid"],
                        row["zulip_user_uuid"],
                        *(state[name] for name in allowed_fields),
                        message_flags_hash(**state),
                    )
                )
            if not changed:
                return 0
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_message_flags
                    (uuid, realm_uuid, zulip_stream_uuid, message_uuid,
                     zulip_user_uuid, is_read, is_starred, is_collapsed,
                     is_mentioned, is_stream_wildcard_mentioned,
                     is_topic_wildcard_mentioned, has_alert_word, is_historical,
                     flags_hash)
                SELECT * FROM unnest(
                    $1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::uuid[],
                    $6::boolean[], $7::boolean[], $8::boolean[], $9::boolean[],
                    $10::boolean[], $11::boolean[], $12::boolean[], $13::boolean[],
                    $14::bytea[]
                )
                ON CONFLICT (message_uuid, zulip_user_uuid) DO UPDATE SET
                    is_read = EXCLUDED.is_read, is_starred = EXCLUDED.is_starred,
                    is_collapsed = EXCLUDED.is_collapsed,
                    is_mentioned = EXCLUDED.is_mentioned,
                    is_stream_wildcard_mentioned = EXCLUDED.is_stream_wildcard_mentioned,
                    is_topic_wildcard_mentioned = EXCLUDED.is_topic_wildcard_mentioned,
                    has_alert_word = EXCLUDED.has_alert_word,
                    is_historical = EXCLUDED.is_historical,
                    flags_hash = EXCLUDED.flags_hash
                """,
                *([item[index] for item in changed] for index in range(14)),
            )
        return len(changed)

    async def set_queue(
        self, user_uuid: UUID, queue_id: str, last_event_id: int
    ) -> bool:
        return await self._update_connection(
            user_uuid,
            "queue_id = $2, last_event_id = $3, lifecycle_status = 'streaming'",
            queue_id,
            last_event_id,
        )

    async def clear_queue(self, user_uuid: UUID, queue_id: str) -> bool:
        async with self._pool.acquire() as connection, connection.transaction():
            active = await connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM workspace_zulip_bridge.zulip_connections "
                "WHERE uuid = $1 AND queue_id = $2 FOR UPDATE)",
                user_uuid,
                queue_id,
            )
            if not active:
                return False
            await connection.execute(
                "UPDATE workspace_zulip_bridge.zulip_streams "
                "SET history_loaded_at = NULL WHERE source_connection_uuid = $1",
                user_uuid,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_stream_bindings AS binding
                SET personal_state_loaded_at = NULL
                FROM workspace_zulip_bridge.zulip_connections AS connection
                WHERE connection.uuid = $1
                  AND binding.zulip_user_uuid = connection.zulip_user_uuid
                """,
                user_uuid,
            )
            status = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET queue_id = NULL, last_event_id = NULL,
                    lifecycle_status = 'init', catalog_completed_at = NULL
                WHERE uuid = $1 AND queue_id = $2
                """,
                user_uuid,
                queue_id,
            )
        return status == "UPDATE 1"

    async def disable_unauthorized_connection(self, user_uuid: UUID) -> bool:
        """Remove a connection with rejected credentials from scheduling."""
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET sync_enabled = false, queue_id = NULL, last_event_id = NULL,
                    lifecycle_status = 'init', catalog_completed_at = NULL,
                    updated_at = clock_timestamp()
                WHERE uuid = $1 AND sync_enabled
                """,
                user_uuid,
            )
        return result == "UPDATE 1"

    async def set_user_status(
        self, user_uuid: UUID, queue_id: str, status: UserStatus
    ) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                "UPDATE workspace_zulip_bridge.zulip_connections "
                "SET lifecycle_status = $3 WHERE uuid = $1 AND queue_id = $2",
                user_uuid,
                queue_id,
                status,
            )
        return result == "UPDATE 1"

    async def get_user_status(
        self, user_uuid: UUID, queue_id: str
    ) -> UserStatus | None:
        async with self._pool.acquire() as connection:
            value = await connection.fetchval(
                "SELECT lifecycle_status FROM workspace_zulip_bridge.zulip_connections "
                "WHERE uuid = $1 AND queue_id = $2",
                user_uuid,
                queue_id,
            )
        return cast(UserStatus | None, value)

    async def begin_catalog_fill(self, user_uuid: UUID, queue_id: str) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                "UPDATE workspace_zulip_bridge.zulip_connections "
                "SET lifecycle_status = 'filling', catalog_completed_at = NULL "
                "WHERE uuid = $1 AND queue_id = $2",
                user_uuid,
                queue_id,
            )
        return result == "UPDATE 1"

    async def _update_connection(
        self, user_uuid: UUID, assignment: str, *values: object
    ) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                f"UPDATE workspace_zulip_bridge.zulip_connections SET {assignment} "
                "WHERE uuid = $1",
                user_uuid,
                *values,
            )
        return result == "UPDATE 1"

    async def list_pending_history_chats(
        self, user_uuid: UUID, queue_id: str
    ) -> list[ScheduledChat]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT stream.chat_key, binding.available_message_count
                FROM workspace_zulip_bridge.zulip_connections AS connection
                JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
                  ON binding.zulip_user_uuid = connection.zulip_user_uuid
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = binding.zulip_stream_uuid
                WHERE connection.uuid = $1 AND connection.queue_id = $2
                  AND (
                      (stream.source_connection_uuid = connection.uuid
                       AND stream.history_loaded_at IS NULL)
                      OR (stream.history_loaded_at IS NOT NULL
                          AND binding.personal_state_loaded_at IS NULL)
                  )
                ORDER BY stream.chat_key
                """,
                user_uuid,
                queue_id,
            )
        return [
            ScheduledChat(row["chat_key"], row["available_message_count"])
            for row in rows
        ]

    async def store_chat_catalog(
        self,
        user_uuid: UUID,
        queue_id: str,
        catalog: ZulipChatCatalog,
        *,
        bootstrap_user_topics: Sequence[ZulipUserTopic] | None = None,
    ) -> ChatCatalogWrite:
        return await self._store_chat_catalog(
            user_uuid,
            queue_id,
            catalog,
            bootstrap_user_topics=bootstrap_user_topics,
        )

    async def _store_chat_catalog(
        self,
        user_uuid: UUID,
        queue_id: str,
        catalog: ZulipChatCatalog,
        *,
        bootstrap_user_topics: Sequence[ZulipUserTopic] | None = None,
    ) -> ChatCatalogWrite:
        async with self._pool.acquire() as connection, connection.transaction():
            owner = await connection.fetchrow(
                """
                SELECT connection.realm_uuid, connection.zulip_user_uuid,
                       connection.streams_hash, realm.identity_key AS endpoint
                FROM workspace_zulip_bridge.zulip_connections AS connection
                JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = connection.realm_uuid
                WHERE connection.uuid = $1 AND connection.queue_id = $2
                FOR UPDATE OF connection
                """,
                user_uuid,
                queue_id,
            )
            if owner is None:
                return ChatCatalogWrite(False, False, 0, 0)
            topic_changes = 0
            if owner["streams_hash"] == catalog.content_hash:
                if bootstrap_user_topics is not None:
                    stored = await _store_user_topics(
                        connection,
                        owner["realm_uuid"],
                        owner["zulip_user_uuid"],
                        bootstrap_user_topics,
                        replace_all=True,
                    )
                    if stored is None:
                        return ChatCatalogWrite(False, True, 0, 0)
                    topic_changes = stored
                await connection.execute(
                    "UPDATE workspace_zulip_bridge.zulip_connections "
                    "SET lifecycle_status = 'scheduling', "
                    "catalog_completed_at = clock_timestamp() "
                    "WHERE uuid = $1 AND queue_id = $2",
                    user_uuid,
                    queue_id,
                )
                return ChatCatalogWrite(True, True, 0, 0, topic_changes)
            upserted, deleted = await _store_chats(
                connection,
                owner["endpoint"],
                owner["realm_uuid"],
                owner["zulip_user_uuid"],
                catalog.chats,
                replace_catalog=True,
            )
            if bootstrap_user_topics is not None:
                stored = await _store_user_topics(
                    connection,
                    owner["realm_uuid"],
                    owner["zulip_user_uuid"],
                    bootstrap_user_topics,
                    replace_all=True,
                )
                if stored is None:
                    return ChatCatalogWrite(False, False, upserted, deleted)
                topic_changes = stored
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET lifecycle_status = 'scheduling', streams_hash = $3,
                    catalog_completed_at = clock_timestamp()
                WHERE uuid = $1 AND queue_id = $2
                """,
                user_uuid,
                queue_id,
                catalog.content_hash,
            )
        return ChatCatalogWrite(True, False, upserted, deleted, topic_changes)

    async def store_events(
        self,
        user_uuid: UUID,
        queue_id: str,
        events: Sequence[ZulipEvent],
        last_event_id: int,
    ) -> tuple[int, bool]:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                WITH active AS MATERIALIZED (
                    SELECT uuid FROM workspace_zulip_bridge.zulip_connections
                    WHERE uuid = $1 AND queue_id = $2 FOR UPDATE
                ), incoming AS (
                    SELECT event_id, event_type, payload::jsonb
                    FROM unnest($3::bigint[], $4::text[], $5::text[])
                      AS event(event_id, event_type, payload)
                ), inserted AS (
                    INSERT INTO workspace_zulip_bridge.zulip_events
                        (zulip_connection_uuid, queue_id, event_id, event_type, payload)
                    SELECT active.uuid, $2, incoming.event_id, incoming.event_type,
                           incoming.payload FROM incoming CROSS JOIN active
                    ON CONFLICT (zulip_connection_uuid, queue_id, event_id) DO NOTHING
                    RETURNING 1
                ), cursor_update AS (
                    UPDATE workspace_zulip_bridge.zulip_connections AS connection
                    SET last_event_id = GREATEST(COALESCE(connection.last_event_id, -1), $6)
                    FROM active WHERE connection.uuid = active.uuid RETURNING 1
                )
                SELECT (SELECT count(*) FROM inserted) AS inserted_count,
                       EXISTS(SELECT FROM cursor_update) AS cursor_updated
                """,
                user_uuid,
                queue_id,
                [event.event_id for event in events],
                [event.event_type for event in events],
                [event.payload_json for event in events],
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
        self._connection_uuid = user_uuid
        self._queue_id = queue_id
        self._realm_uuid: UUID | None = None
        self._zulip_user_uuid: UUID | None = None
        self._endpoint: str | None = None
        self._started_at: datetime | None = None
        self._closed = False

    async def initialize(self) -> None:
        owner = await self._connection.fetchrow(
            """
            SELECT connection.realm_uuid, connection.zulip_user_uuid,
                   realm.identity_key AS endpoint
            FROM workspace_zulip_bridge.zulip_connections AS connection
            JOIN workspace_zulip_bridge.zulip_realms AS realm
              ON realm.uuid = connection.realm_uuid
            WHERE connection.uuid = $1 AND connection.queue_id = $2
            """,
            self._connection_uuid,
            self._queue_id,
        )
        if owner is None:
            raise RuntimeError("history session does not own the active queue")
        self._realm_uuid = owner["realm_uuid"]
        self._zulip_user_uuid = owner["zulip_user_uuid"]
        self._endpoint = owner["endpoint"]
        self._started_at = await self._connection.fetchval("SELECT clock_timestamp()")
        await self._connection.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS wzb_seen_messages (
                zulip_message_id bigint PRIMARY KEY
            ) ON COMMIT PRESERVE ROWS;
            CREATE TEMP TABLE IF NOT EXISTS wzb_message_page (
                uuid uuid NOT NULL, zulip_message_id bigint NOT NULL,
                stream_uuid uuid NOT NULL, topic_uuid uuid, topic_name text,
                topic_hash bytea,
                sender_user_uuid uuid NOT NULL, content text NOT NULL,
                reactions jsonb NOT NULL, reaction_users jsonb NOT NULL,
                content_hash bytea NOT NULL, message_hash bytea NOT NULL,
                flag_uuid uuid NOT NULL, write_flags boolean NOT NULL,
                flags_hash bytea NOT NULL,
                is_read boolean NOT NULL,
                is_starred boolean NOT NULL, is_collapsed boolean NOT NULL,
                is_mentioned boolean NOT NULL,
                is_stream_wildcard_mentioned boolean NOT NULL,
                is_topic_wildcard_mentioned boolean NOT NULL,
                has_alert_word boolean NOT NULL, is_historical boolean NOT NULL,
                sent_at bigint NOT NULL, source_updated_at bigint NOT NULL
            ) ON COMMIT DELETE ROWS;
            CREATE TEMP TABLE IF NOT EXISTS wzb_file_page (
                message_uuid uuid NOT NULL, file_uuid uuid NOT NULL,
                source_path text NOT NULL, name text NOT NULL,
                position integer NOT NULL
            ) ON COMMIT DELETE ROWS;
            CREATE TEMP TABLE IF NOT EXISTS wzb_reaction_page (
                message_uuid uuid NOT NULL, reaction_uuid uuid NOT NULL,
                user_uuid uuid NOT NULL, emoji_name text NOT NULL,
                emoji_code text NOT NULL, reaction_type text NOT NULL
            ) ON COMMIT DELETE ROWS;
            TRUNCATE wzb_seen_messages, wzb_message_page, wzb_file_page,
                     wzb_reaction_page;
            """
        )

    async def store_page(self, messages: Sequence[ZulipMessage]) -> MessagePageWrite:
        if self._closed:
            raise RuntimeError("history session is closed")
        if not messages:
            return MessagePageWrite(0, 0, 0, 0, 0)
        if (
            self._endpoint is None
            or self._realm_uuid is None
            or self._zulip_user_uuid is None
        ):
            raise RuntimeError("history session is not initialized")
        linked_rows = await self._connection.fetch(
            """
            SELECT workspace_uuid, zulip_external_key
            FROM workspace_zulip_bridge.zulip_entity_links
            WHERE realm_uuid = $1 AND entity_type = 'message'
              AND zulip_external_key = ANY($2::text[])
            """,
            self._realm_uuid,
            [str(message.message_id) for message in messages],
        )
        linked_messages = {
            int(row["zulip_external_key"]): UUID(str(row["workspace_uuid"]))
            for row in linked_rows
        }
        records = []
        file_records: list[tuple[UUID, UUID, str, str, int]] = []
        reaction_records: list[tuple[UUID, UUID, UUID, str, str, str]] = []
        for message in messages:
            message_uuid = linked_messages.get(
                message.message_id,
                stable_message_uuid(self._endpoint, message.message_id),
            )
            stream_uuid = stable_chat_uuid(self._endpoint, message.chat_key)
            topic_name = message.topic_name or "General"
            topic_uuid = stable_topic_uuid(stream_uuid, topic_name)
            flags_hash = message_flags_hash(
                is_read=message.is_read,
                is_starred=message.is_starred,
                is_collapsed=message.is_collapsed,
                is_mentioned=message.is_mentioned,
                is_stream_wildcard_mentioned=message.is_stream_wildcard_mentioned,
                is_topic_wildcard_mentioned=message.is_topic_wildcard_mentioned,
                has_alert_word=message.has_alert_word,
                is_historical=message.is_historical,
            )
            records.append(
                (
                    message_uuid,
                    message.message_id,
                    stream_uuid,
                    topic_uuid,
                    topic_name,
                    hashlib.sha256(topic_name.encode("utf-8")).digest(),
                    message.sender_user_uuid,
                    message.content,
                    message.reactions_json,
                    message.reaction_users_json,
                    message.content_hash,
                    message.message_hash,
                    stable_message_flag_uuid(message_uuid, self._zulip_user_uuid),
                    message.write_flags,
                    flags_hash,
                    message.is_read,
                    message.is_starred,
                    message.is_collapsed,
                    message.is_mentioned,
                    message.is_stream_wildcard_mentioned,
                    message.is_topic_wildcard_mentioned,
                    message.has_alert_word,
                    message.is_historical,
                    message.sent_at,
                    message.source_updated_at or message.sent_at,
                )
            )
            file_records.extend(
                (
                    message_uuid,
                    stable_file_uuid(self._endpoint, file.source_path),
                    file.source_path,
                    file.name,
                    position,
                )
                for position, file in enumerate(message.files)
            )
            raw_reactions = json.loads(message.reactions_json)
            reaction_records.extend(
                (
                    message_uuid,
                    stable_reaction_uuid(
                        message_uuid,
                        UUID(reaction["user_uuid"]),
                        reaction["reaction_type"],
                        reaction["emoji_code"],
                    ),
                    UUID(reaction["user_uuid"]),
                    reaction["emoji_name"],
                    reaction["emoji_code"],
                    reaction["reaction_type"],
                )
                for reaction in raw_reactions
            )
        async with self._connection.transaction():
            active = await self._connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM workspace_zulip_bridge.zulip_connections "
                "WHERE uuid = $1 AND queue_id = $2)",
                self._connection_uuid,
                self._queue_id,
            )
            if not active:
                return MessagePageWrite(0, 0, 0, 0, 0)
            await self._connection.execute(
                "TRUNCATE wzb_message_page, wzb_file_page, wzb_reaction_page"
            )
            await self._connection.copy_records_to_table(
                "wzb_message_page",
                records=records,
                columns=(
                    "uuid",
                    "zulip_message_id",
                    "stream_uuid",
                    "topic_uuid",
                    "topic_name",
                    "topic_hash",
                    "sender_user_uuid",
                    "content",
                    "reactions",
                    "reaction_users",
                    "content_hash",
                    "message_hash",
                    "flag_uuid",
                    "write_flags",
                    "flags_hash",
                    "is_read",
                    "is_starred",
                    "is_collapsed",
                    "is_mentioned",
                    "is_stream_wildcard_mentioned",
                    "is_topic_wildcard_mentioned",
                    "has_alert_word",
                    "is_historical",
                    "sent_at",
                    "source_updated_at",
                ),
            )
            if file_records:
                await self._connection.copy_records_to_table(
                    "wzb_file_page",
                    records=file_records,
                    columns=(
                        "message_uuid",
                        "file_uuid",
                        "source_path",
                        "name",
                        "position",
                    ),
                )
            if reaction_records:
                await self._connection.copy_records_to_table(
                    "wzb_reaction_page",
                    records=reaction_records,
                    columns=(
                        "message_uuid",
                        "reaction_uuid",
                        "user_uuid",
                        "emoji_name",
                        "emoji_code",
                        "reaction_type",
                    ),
                )
            await self._reuse_topic_identities()
            topics_inserted = await self._store_topics()
            counts = await self._store_messages_and_personal_state()
        received = len(messages)
        resolved = counts["resolved_count"]
        if counts["seen_count"] != resolved:
            raise RuntimeError("message page failed to track scheduled messages")
        return MessagePageWrite(
            received=resolved,
            changed=counts["changed_count"],
            unchanged=resolved - counts["changed_count"],
            unassigned=received - resolved,
            topics_inserted=topics_inserted,
            flags_changed=counts["flags_changed"],
            reactions_changed=counts["reactions_changed"],
            files_changed=counts["files_changed"],
        )

    async def _reuse_topic_identities(self) -> None:
        await self._connection.execute(
            """
            UPDATE wzb_message_page AS page
            SET topic_uuid = topic.uuid,
                topic_name = topic.name,
                topic_hash = topic.content_hash
            FROM workspace_zulip_bridge.zulip_topic_aliases AS alias
            JOIN workspace_zulip_bridge.zulip_topics AS topic
              ON topic.uuid = alias.topic_uuid
            WHERE alias.zulip_stream_uuid = page.stream_uuid
              AND alias.alias = page.topic_name
              AND alias.active
              AND page.topic_uuid IS DISTINCT FROM topic.uuid
            """
        )
        await self._connection.execute(
            """
            UPDATE wzb_message_page AS page
            SET topic_uuid = topic.uuid
            FROM workspace_zulip_bridge.zulip_topics AS topic
            WHERE topic.zulip_stream_uuid = page.stream_uuid
              AND topic.name = page.topic_name
              AND page.topic_uuid IS DISTINCT FROM topic.uuid
            """
        )

    async def _store_topics(self) -> int:
        return int(
            await self._connection.fetchval(
                """
            WITH incoming AS MATERIALIZED (
                SELECT DISTINCT page.topic_uuid, page.stream_uuid, page.topic_name,
                       page.topic_hash AS content_hash
                FROM wzb_message_page AS page
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = page.stream_uuid
                 AND stream.source_connection_uuid = $1
                WHERE page.topic_uuid IS NOT NULL
            ), topics AS (
                INSERT INTO workspace_zulip_bridge.zulip_topics
                    (uuid, zulip_stream_uuid, name, content_hash)
                SELECT topic_uuid, stream_uuid, topic_name, content_hash FROM incoming
                ON CONFLICT (uuid) DO UPDATE SET name = EXCLUDED.name,
                    content_hash = EXCLUDED.content_hash
                WHERE zulip_topics.content_hash IS DISTINCT FROM EXCLUDED.content_hash
                RETURNING 1
            ), aliases AS (
                INSERT INTO workspace_zulip_bridge.zulip_topic_aliases
                    (zulip_stream_uuid, alias, topic_uuid)
                SELECT stream_uuid, topic_name, topic_uuid FROM incoming
                ON CONFLICT (zulip_stream_uuid, alias) DO UPDATE
                SET topic_uuid = EXCLUDED.topic_uuid, active = true
                RETURNING 1
            ) SELECT count(*) FROM topics
            """,
                self._connection_uuid,
            )
        )

    async def _store_messages_and_personal_state(self) -> asyncpg.Record:
        row = await self._connection.fetchrow(
            """
            WITH resolved AS MATERIALIZED (
                SELECT page.*,
                       stream.source_connection_uuid = $1 AS write_common
                FROM wzb_message_page AS page
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = page.stream_uuid
                JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
                  ON binding.zulip_stream_uuid = stream.uuid
                 AND binding.zulip_user_uuid = $3
                LEFT JOIN workspace_zulip_bridge.zulip_topics AS topic
                  ON topic.uuid = page.topic_uuid
                LEFT JOIN workspace_zulip_bridge.zulip_messages AS existing
                  ON existing.uuid = page.uuid
                 AND existing.zulip_stream_uuid = stream.uuid
                WHERE (page.topic_uuid IS NULL OR topic.uuid IS NOT NULL)
                  AND (stream.source_connection_uuid = $1
                       OR existing.uuid IS NOT NULL)
            ), seen AS (
                INSERT INTO wzb_seen_messages (zulip_message_id)
                SELECT zulip_message_id FROM resolved ON CONFLICT DO NOTHING RETURNING 1
            ), changed AS (
                INSERT INTO workspace_zulip_bridge.zulip_messages
                    (uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                     topic_uuid, sender_user_uuid, zulip_message_id, content,
                     reactions, reaction_users, content_hash, message_hash, created_at,
                     source_updated_at)
                SELECT uuid, $2, $1, stream_uuid, topic_uuid, sender_user_uuid,
                       zulip_message_id, content, reactions, reaction_users,
                       content_hash, message_hash, to_timestamp(sent_at),
                       to_timestamp(source_updated_at)
                FROM resolved
                WHERE write_common
                ON CONFLICT (uuid) DO UPDATE SET
                    source_connection_uuid = EXCLUDED.source_connection_uuid,
                    zulip_stream_uuid = EXCLUDED.zulip_stream_uuid,
                    topic_uuid = EXCLUDED.topic_uuid,
                    sender_user_uuid = EXCLUDED.sender_user_uuid,
                    content = EXCLUDED.content, reactions = EXCLUDED.reactions,
                    reaction_users = EXCLUDED.reaction_users,
                    content_hash = EXCLUDED.content_hash,
                    message_hash = EXCLUDED.message_hash,
                    created_at = CASE WHEN EXISTS (
                        SELECT 1
                        FROM workspace_zulip_bridge.zulip_entity_links AS link
                        WHERE link.realm_uuid = $2
                          AND link.entity_type = 'message'
                          AND link.workspace_uuid = EXCLUDED.uuid
                    ) THEN zulip_messages.created_at ELSE EXCLUDED.created_at END,
                    source_updated_at = EXCLUDED.source_updated_at,
                    updated_at = clock_timestamp()
                WHERE zulip_messages.message_hash IS DISTINCT FROM EXCLUDED.message_hash
                RETURNING uuid, zulip_message_id
            ), flags AS (
                INSERT INTO workspace_zulip_bridge.zulip_message_flags
                    (uuid, realm_uuid, zulip_stream_uuid, message_uuid,
                     zulip_user_uuid, is_read, is_starred, is_collapsed,
                     is_mentioned, is_stream_wildcard_mentioned,
                     is_topic_wildcard_mentioned, has_alert_word, is_historical,
                     flags_hash)
                SELECT resolved.flag_uuid,
                       $2, resolved.stream_uuid, resolved.uuid, $3,
                       resolved.is_read, resolved.is_starred, resolved.is_collapsed,
                       resolved.is_mentioned, resolved.is_stream_wildcard_mentioned,
                       resolved.is_topic_wildcard_mentioned, resolved.has_alert_word,
                       resolved.is_historical, resolved.flags_hash
                FROM resolved
                WHERE resolved.write_flags
                ON CONFLICT (message_uuid, zulip_user_uuid) DO UPDATE SET
                    is_read = EXCLUDED.is_read, is_starred = EXCLUDED.is_starred,
                    is_collapsed = EXCLUDED.is_collapsed,
                    is_mentioned = EXCLUDED.is_mentioned,
                    is_stream_wildcard_mentioned = EXCLUDED.is_stream_wildcard_mentioned,
                    is_topic_wildcard_mentioned = EXCLUDED.is_topic_wildcard_mentioned,
                    has_alert_word = EXCLUDED.has_alert_word,
                    is_historical = EXCLUDED.is_historical,
                    flags_hash = EXCLUDED.flags_hash
                WHERE zulip_message_flags.flags_hash IS DISTINCT FROM EXCLUDED.flags_hash
                RETURNING 1
            ), removed_reactions AS MATERIALIZED (
                SELECT reaction.uuid, reaction.message_uuid,
                       message.zulip_stream_uuid
                FROM workspace_zulip_bridge.zulip_message_reactions AS reaction
                JOIN changed ON changed.uuid = reaction.message_uuid
                JOIN workspace_zulip_bridge.zulip_messages AS message
                  ON message.uuid = reaction.message_uuid
                WHERE NOT EXISTS (
                    SELECT 1 FROM wzb_reaction_page AS incoming
                    WHERE incoming.reaction_uuid = reaction.uuid
                )
            ), reaction_tombstones AS (
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    partition_key, direction, delivery_priority,
                    source_hash, target_hash, source_updated_at,
                    target_updated_at
                )
                SELECT realm.workspace_provider_uuid, 'message_reactions',
                       removed.uuid, $2, removed.zulip_stream_uuid,
                       'to_workspace', 0, NULL, target.content_hash,
                       clock_timestamp(), target.source_updated_at
                FROM removed_reactions AS removed
                JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = $2
                 AND realm.workspace_provider_uuid IS NOT NULL
                JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
                  ON mirror.provider_uuid = realm.workspace_provider_uuid
                 AND mirror.bootstrap_status = 'ready'
                 AND mirror.active_generation IS NOT NULL
                JOIN workspace_zulip_bridge.workspace_message_reactions AS target
                  ON target.provider_uuid = realm.workspace_provider_uuid
                 AND target.snapshot_generation = mirror.active_generation
                 AND target.uuid = removed.uuid
                ON CONFLICT (provider_uuid, entity_type, entity_uuid)
                DO UPDATE SET direction = 'to_workspace', delivery_priority = 0,
                    partition_key = EXCLUDED.partition_key,
                    source_hash = NULL, target_hash = EXCLUDED.target_hash,
                    source_updated_at = EXCLUDED.source_updated_at,
                    target_updated_at = EXCLUDED.target_updated_at,
                    processing_status = 'pending', attempt_count = 0,
                    available_at = clock_timestamp(), claimed_at = NULL,
                    processed_at = NULL, last_error = NULL,
                    updated_at = clock_timestamp()
                RETURNING 1
            ), old_reactions AS (
                DELETE FROM workspace_zulip_bridge.zulip_message_reactions AS reaction
                USING changed WHERE reaction.message_uuid = changed.uuid RETURNING 1
            ), reactions AS (
                INSERT INTO workspace_zulip_bridge.zulip_message_reactions
                    (uuid, realm_uuid, message_uuid, zulip_user_uuid,
                     emoji_name, emoji_code, reaction_type)
                SELECT value.reaction_uuid, $2, changed.uuid, value.user_uuid,
                       value.emoji_name, value.emoji_code, value.reaction_type
                FROM changed
                JOIN wzb_reaction_page AS value ON value.message_uuid = changed.uuid
                ON CONFLICT (uuid) DO NOTHING RETURNING 1
            ), old_links AS (
                DELETE FROM workspace_zulip_bridge.zulip_message_files AS link
                USING changed WHERE link.message_uuid = changed.uuid RETURNING 1
            ), link_candidates AS MATERIALIZED (
                SELECT file.message_uuid, file.file_uuid, file.position, 0 AS priority
                FROM wzb_file_page AS file
                JOIN changed ON changed.uuid = file.message_uuid
                JOIN workspace_zulip_bridge.zulip_files AS metadata
                  ON metadata.uuid = file.file_uuid
                UNION ALL
                SELECT resolved.uuid, metadata.uuid, 0, 1
                FROM resolved
                JOIN workspace_zulip_bridge.zulip_files AS metadata
                  ON metadata.realm_uuid = $2
                 AND metadata.message_ids @> ARRAY[resolved.zulip_message_id]
            ), file_links AS (
                INSERT INTO workspace_zulip_bridge.zulip_message_files
                    (message_uuid, file_uuid, position)
                SELECT DISTINCT ON (message_uuid, file_uuid)
                       message_uuid, file_uuid, position
                FROM link_candidates
                ORDER BY message_uuid, file_uuid, priority
                ON CONFLICT (message_uuid, file_uuid) DO UPDATE
                SET position = EXCLUDED.position RETURNING 1
            ), outbox AS (
                INSERT INTO workspace_zulip_bridge.workspace_outbox
                    (realm_uuid, entity_type, action, entity_uuid)
                SELECT $2, 'message', 'upsert', uuid FROM changed
                ON CONFLICT (realm_uuid, entity_type, entity_uuid)
                    WHERE delivery_status = 'pending'
                DO UPDATE SET action = 'upsert', updated_at = clock_timestamp()
                RETURNING 1
            )
            SELECT (SELECT count(*) FROM resolved) AS resolved_count,
                   (SELECT count(*) FROM seen) AS seen_count,
                   (SELECT count(*) FROM changed) AS changed_count,
                   (SELECT count(*) FROM flags) AS flags_changed,
                   (SELECT count(*) FROM reactions) AS reactions_changed,
                   (SELECT count(*) FROM reaction_tombstones)
                       AS reaction_tombstones,
                   (SELECT count(*) FROM file_links) AS files_changed
            """,
            self._connection_uuid,
            self._realm_uuid,
            self._zulip_user_uuid,
        )
        if row is None:
            raise RuntimeError("message page query returned no row")
        return row

    async def store_chats(self, chats: Sequence[ZulipChat]) -> int:
        if self._closed:
            raise RuntimeError("history session is closed")
        if not chats:
            return 0
        if (
            self._endpoint is None
            or self._realm_uuid is None
            or self._zulip_user_uuid is None
        ):
            raise RuntimeError("history session is not initialized")
        async with self._connection.transaction():
            changed, _ = await _store_chats(
                self._connection,
                self._endpoint,
                self._realm_uuid,
                self._zulip_user_uuid,
                chats,
                replace_catalog=False,
            )
        return changed

    async def finish(self, chat_keys: Sequence[str]) -> HistoryWrite:
        if self._closed:
            raise RuntimeError("history session is closed")
        if (
            self._endpoint is None
            or self._zulip_user_uuid is None
            or self._started_at is None
        ):
            raise RuntimeError("history session is not initialized")
        stream_uuids = [stable_chat_uuid(self._endpoint, key) for key in chat_keys]
        async with self._connection.transaction():
            active = await self._connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM workspace_zulip_bridge.zulip_connections "
                "WHERE uuid = $1 AND queue_id = $2 FOR UPDATE)",
                self._connection_uuid,
                self._queue_id,
            )
            if not active:
                return HistoryWrite(False, 0, 0, 0)
            deleted_messages = int(
                await self._connection.fetchval(
                    """
                WITH candidates AS MATERIALIZED (
                    SELECT message.uuid, message.realm_uuid,
                           message.zulip_stream_uuid
                    FROM workspace_zulip_bridge.zulip_messages AS message
                    WHERE message.source_connection_uuid = $1
                      AND message.zulip_stream_uuid = ANY($2::uuid[])
                      AND message.updated_at <= $3
                      AND NOT EXISTS (
                          SELECT 1 FROM wzb_seen_messages AS seen
                          WHERE seen.zulip_message_id = message.zulip_message_id
                      )
                    FOR UPDATE OF message
                ), tombstones AS (
                    INSERT INTO workspace_zulip_bridge.sync_diffs (
                        provider_uuid, entity_type, entity_uuid, realm_uuid,
                        partition_key, direction, delivery_priority,
                        source_hash, target_hash, source_updated_at,
                        target_updated_at
                    )
                    SELECT realm.workspace_provider_uuid, 'messages',
                           candidate.uuid, candidate.realm_uuid,
                           candidate.zulip_stream_uuid, 'to_workspace', 0,
                           NULL, target.content_hash, clock_timestamp(),
                           target.source_updated_at
                    FROM candidates AS candidate
                    JOIN workspace_zulip_bridge.zulip_realms AS realm
                      ON realm.uuid = candidate.realm_uuid
                     AND realm.workspace_provider_uuid IS NOT NULL
                    JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
                      ON mirror.provider_uuid = realm.workspace_provider_uuid
                     AND mirror.bootstrap_status = 'ready'
                     AND mirror.active_generation IS NOT NULL
                    JOIN workspace_zulip_bridge.workspace_messages AS target
                      ON target.provider_uuid = realm.workspace_provider_uuid
                     AND target.snapshot_generation = mirror.active_generation
                     AND target.uuid = candidate.uuid
                    ON CONFLICT (provider_uuid, entity_type, entity_uuid)
                    DO UPDATE SET direction = 'to_workspace',
                        delivery_priority = 0,
                        partition_key = EXCLUDED.partition_key,
                        source_hash = NULL,
                        target_hash = EXCLUDED.target_hash,
                        source_updated_at = EXCLUDED.source_updated_at,
                        target_updated_at = EXCLUDED.target_updated_at,
                        processing_status = 'pending', attempt_count = 0,
                        available_at = clock_timestamp(), claimed_at = NULL,
                        processed_at = NULL, last_error = NULL,
                        updated_at = clock_timestamp()
                    RETURNING 1
                ), deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    USING candidates
                    WHERE message.uuid = candidates.uuid
                    RETURNING 1
                ) SELECT count(*) FROM deleted
                """,
                    self._connection_uuid,
                    stream_uuids,
                    self._started_at,
                )
            )
            await self._connection.execute(
                """
                WITH candidates AS MATERIALIZED (
                    SELECT flag.uuid, flag.realm_uuid,
                           flag.zulip_stream_uuid
                    FROM workspace_zulip_bridge.zulip_message_flags AS flag
                    JOIN workspace_zulip_bridge.zulip_messages AS message
                      ON message.uuid = flag.message_uuid
                    WHERE flag.zulip_user_uuid = $1
                      AND flag.zulip_stream_uuid = ANY($2::uuid[])
                      AND flag.updated_at <= $3
                      AND NOT EXISTS (
                          SELECT 1 FROM wzb_seen_messages AS seen
                          WHERE seen.zulip_message_id = message.zulip_message_id
                      )
                    FOR UPDATE OF flag
                ), tombstones AS (
                    INSERT INTO workspace_zulip_bridge.sync_diffs (
                        provider_uuid, entity_type, entity_uuid, realm_uuid,
                        partition_key, direction, delivery_priority,
                        source_hash, target_hash, source_updated_at,
                        target_updated_at
                    )
                    SELECT realm.workspace_provider_uuid, 'message_flags',
                           candidate.uuid, candidate.realm_uuid,
                           candidate.zulip_stream_uuid, 'to_workspace', 0,
                           NULL, target.content_hash, clock_timestamp(),
                           target.source_updated_at
                    FROM candidates AS candidate
                    JOIN workspace_zulip_bridge.zulip_realms AS realm
                      ON realm.uuid = candidate.realm_uuid
                     AND realm.workspace_provider_uuid IS NOT NULL
                    JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
                      ON mirror.provider_uuid = realm.workspace_provider_uuid
                     AND mirror.bootstrap_status = 'ready'
                     AND mirror.active_generation IS NOT NULL
                    JOIN workspace_zulip_bridge.workspace_message_flags AS target
                      ON target.provider_uuid = realm.workspace_provider_uuid
                     AND target.snapshot_generation = mirror.active_generation
                     AND target.uuid = candidate.uuid
                    ON CONFLICT (provider_uuid, entity_type, entity_uuid)
                    DO UPDATE SET direction = 'to_workspace',
                        delivery_priority = 0,
                        partition_key = EXCLUDED.partition_key,
                        source_hash = NULL,
                        target_hash = EXCLUDED.target_hash,
                        source_updated_at = EXCLUDED.source_updated_at,
                        target_updated_at = EXCLUDED.target_updated_at,
                        processing_status = 'pending', attempt_count = 0,
                        available_at = clock_timestamp(), claimed_at = NULL,
                        processed_at = NULL, last_error = NULL,
                        updated_at = clock_timestamp()
                    RETURNING 1
                ), deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_message_flags AS flag
                    USING candidates
                    WHERE flag.uuid = candidates.uuid
                )
                SELECT count(*) FROM tombstones
                """,
                self._zulip_user_uuid,
                stream_uuids,
                self._started_at,
            )
            deleted_topics = int(
                await self._connection.fetchval(
                    """
                WITH candidates AS MATERIALIZED (
                    SELECT topic.uuid, topic.zulip_stream_uuid, stream.realm_uuid
                    FROM workspace_zulip_bridge.zulip_topics AS topic
                    JOIN workspace_zulip_bridge.zulip_streams AS stream
                      ON stream.uuid = topic.zulip_stream_uuid
                    WHERE topic.zulip_stream_uuid = ANY($1::uuid[])
                      AND NOT EXISTS (
                          SELECT 1
                          FROM workspace_zulip_bridge.zulip_messages AS message
                          WHERE message.topic_uuid = topic.uuid
                      )
                    FOR UPDATE OF topic
                ), tombstones AS (
                    INSERT INTO workspace_zulip_bridge.sync_diffs (
                        provider_uuid, entity_type, entity_uuid, realm_uuid,
                        partition_key, direction, delivery_priority,
                        source_hash, target_hash, source_updated_at,
                        target_updated_at
                    )
                    SELECT realm.workspace_provider_uuid, 'topics',
                           candidate.uuid, candidate.realm_uuid,
                           candidate.zulip_stream_uuid, 'to_workspace', 0,
                           NULL, target.content_hash, clock_timestamp(),
                           target.source_updated_at
                    FROM candidates AS candidate
                    JOIN workspace_zulip_bridge.zulip_realms AS realm
                      ON realm.uuid = candidate.realm_uuid
                     AND realm.workspace_provider_uuid IS NOT NULL
                    JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
                      ON mirror.provider_uuid = realm.workspace_provider_uuid
                     AND mirror.bootstrap_status = 'ready'
                     AND mirror.active_generation IS NOT NULL
                    JOIN workspace_zulip_bridge.workspace_topics AS target
                      ON target.provider_uuid = realm.workspace_provider_uuid
                     AND target.snapshot_generation = mirror.active_generation
                     AND target.uuid = candidate.uuid
                    ON CONFLICT (provider_uuid, entity_type, entity_uuid)
                    DO UPDATE SET direction = 'to_workspace',
                        delivery_priority = 0,
                        partition_key = EXCLUDED.partition_key,
                        source_hash = NULL,
                        target_hash = EXCLUDED.target_hash,
                        source_updated_at = EXCLUDED.source_updated_at,
                        target_updated_at = EXCLUDED.target_updated_at,
                        processing_status = 'pending', attempt_count = 0,
                        available_at = clock_timestamp(), claimed_at = NULL,
                        processed_at = NULL, last_error = NULL,
                        updated_at = clock_timestamp()
                    RETURNING 1
                ), deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_topics AS topic
                    USING candidates
                    WHERE topic.uuid = candidates.uuid
                    RETURNING 1
                ) SELECT count(*) FROM deleted
                """,
                    stream_uuids,
                )
            )
            loaded = int(
                await self._connection.fetchval(
                    """
                WITH loaded AS (
                    UPDATE workspace_zulip_bridge.zulip_streams
                    SET history_loaded_at = clock_timestamp()
                    WHERE uuid = ANY($2::uuid[]) AND source_connection_uuid = $1
                      AND history_loaded_at IS NULL RETURNING 1
                ) SELECT count(*) FROM loaded
                """,
                    self._connection_uuid,
                    stream_uuids,
                )
            )
            await self._connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_stream_bindings AS binding
                SET personal_state_loaded_at = clock_timestamp()
                FROM workspace_zulip_bridge.zulip_streams AS stream
                WHERE binding.zulip_stream_uuid = stream.uuid
                  AND binding.zulip_user_uuid = $1
                  AND binding.zulip_stream_uuid = ANY($2::uuid[])
                  AND stream.history_loaded_at IS NOT NULL
                """,
                self._zulip_user_uuid,
                stream_uuids,
            )
            await self._connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections AS connection
                SET lifecycle_status = CASE WHEN EXISTS (
                    SELECT 1
                    FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
                    JOIN workspace_zulip_bridge.zulip_streams AS stream
                      ON stream.uuid = binding.zulip_stream_uuid
                    WHERE binding.zulip_user_uuid = connection.zulip_user_uuid
                      AND (
                          (stream.source_connection_uuid = connection.uuid
                           AND stream.history_loaded_at IS NULL)
                          OR (stream.history_loaded_at IS NOT NULL
                              AND binding.personal_state_loaded_at IS NULL)
                      )
                ) THEN 'backfilling' ELSE 'active' END
                WHERE connection.uuid = $1 AND connection.queue_id = $2
                """,
                self._connection_uuid,
                self._queue_id,
            )
            await self._connection.execute("TRUNCATE wzb_seen_messages")
        return HistoryWrite(True, deleted_messages, deleted_topics, loaded)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._pool.release(self._connection)


def _directory_user_profile_hash(user: ZulipDirectoryUser) -> bytes:
    payload = {
        "avatar_url": user.avatar_url,
        "disabled": user.disabled,
        "full_name": user.full_name,
        "is_bot": user.is_bot,
        "login": user.login,
        "role": user.role,
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()


def _user_profile_hash(user: Mapping[str, object]) -> bytes:
    last_ping_at = user.get("last_ping_at")
    payload = {
        "avatar_url": user.get("avatar_url"),
        "disabled": user.get("disabled"),
        "full_name": user.get("full_name"),
        "is_bot": user.get("is_bot"),
        "last_ping_at": (
            last_ping_at.isoformat() if isinstance(last_ping_at, datetime) else None
        ),
        "login": user.get("login"),
        "presence_status": user.get("presence_status"),
        "role": user.get("role"),
        "status_emoji": user.get("status_emoji"),
        "status_text": user.get("status_text"),
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()


def _topic_binding_hash(
    stream_uuid: UUID,
    topic_uuid: UUID,
    user_uuid: UUID,
    notification_mode: str,
    created_at: datetime,
) -> bytes:
    return hashlib.sha256(
        json.dumps(
            {
                "stream_uuid": str(stream_uuid),
                "topic_uuid": str(topic_uuid),
                "user_uuid": str(user_uuid),
                "notification_mode": notification_mode,
                "created_at": created_at.astimezone(UTC).isoformat(),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()


async def _store_user_topics(
    connection: asyncpg.Connection | PoolConnectionProxy,
    realm_uuid: UUID,
    user_uuid: UUID,
    topics: Sequence[ZulipUserTopic],
    *,
    replace_all: bool,
) -> int | None:
    changed = 0
    if replace_all:
        rows = await connection.fetch(
            """
            SELECT topic_binding.uuid, topic_binding.zulip_stream_uuid,
                   topic_binding.topic_uuid, topic_binding.zulip_user_uuid,
                   topic_binding.created_at
            FROM workspace_zulip_bridge.zulip_topic_bindings AS topic_binding
            JOIN workspace_zulip_bridge.zulip_streams AS stream
              ON stream.uuid = topic_binding.zulip_stream_uuid
            WHERE stream.realm_uuid = $1
              AND topic_binding.zulip_user_uuid = $2
              AND topic_binding.notification_mode <> 'default'
            """,
            realm_uuid,
            user_uuid,
        )
        for row in rows:
            status = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_topic_bindings
                SET notification_mode = 'default', content_hash = $2,
                    updated_at = clock_timestamp()
                WHERE uuid = $1 AND notification_mode <> 'default'
                """,
                row["uuid"],
                _topic_binding_hash(
                    row["zulip_stream_uuid"],
                    row["topic_uuid"],
                    row["zulip_user_uuid"],
                    "default",
                    row["created_at"],
                ),
            )
            changed += status == "UPDATE 1"
    for topic in topics:
        topic_change = await _store_user_topic(
            connection,
            realm_uuid,
            user_uuid,
            topic,
        )
        if topic_change is None:
            if not replace_all:
                return None
            continue
        changed += topic_change
    return changed


async def _store_user_topic(
    connection: asyncpg.Connection | PoolConnectionProxy,
    realm_uuid: UUID,
    user_uuid: UUID,
    topic: ZulipUserTopic,
) -> int | None:
    stream = await connection.fetchrow(
        """
        SELECT stream.uuid
        FROM workspace_zulip_bridge.zulip_streams AS stream
        JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
          ON binding.zulip_stream_uuid = stream.uuid
         AND binding.zulip_user_uuid = $3
        WHERE stream.realm_uuid = $1 AND stream.chat_key = $2
        """,
        realm_uuid,
        f"channel:{topic.stream_id}",
        user_uuid,
    )
    if stream is None:
        return None
    stream_uuid = UUID(str(stream["uuid"]))
    topic_uuid = stable_topic_uuid(stream_uuid, topic.topic_name)
    topic_created_at = datetime.fromtimestamp(topic.last_updated, UTC)
    await connection.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_topics (
            uuid, zulip_stream_uuid, name, content_hash, created_at
        ) VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (uuid) DO UPDATE
        SET name = EXCLUDED.name, content_hash = EXCLUDED.content_hash,
            updated_at = clock_timestamp()
        WHERE zulip_topics.content_hash IS DISTINCT FROM EXCLUDED.content_hash
        """,
        topic_uuid,
        stream_uuid,
        topic.topic_name,
        hashlib.sha256(topic.topic_name.encode("utf-8")).digest(),
        topic_created_at,
    )
    await connection.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_topic_aliases (
            zulip_stream_uuid, alias, topic_uuid
        ) VALUES ($1, $2, $3)
        ON CONFLICT (zulip_stream_uuid, alias) DO UPDATE
        SET topic_uuid = EXCLUDED.topic_uuid, active = true,
            updated_at = clock_timestamp()
        """,
        stream_uuid,
        topic.topic_name,
        topic_uuid,
    )
    mode = {0: "default", 1: "mute", 2: "unmute", 3: "follow"}[topic.visibility_policy]
    created_at = await connection.fetchval(
        """
        SELECT created_at
        FROM workspace_zulip_bridge.zulip_topic_bindings
        WHERE topic_uuid = $1 AND zulip_user_uuid = $2
        """,
        topic_uuid,
        user_uuid,
    )
    if not isinstance(created_at, datetime):
        created_at = topic_created_at
    content_hash = _topic_binding_hash(
        stream_uuid,
        topic_uuid,
        user_uuid,
        mode,
        created_at,
    )
    status = await connection.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_topic_bindings (
            uuid, zulip_stream_uuid, topic_uuid, zulip_user_uuid,
            notification_mode, content_hash, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (uuid) DO UPDATE
        SET notification_mode = EXCLUDED.notification_mode,
            content_hash = EXCLUDED.content_hash,
            updated_at = clock_timestamp()
        WHERE zulip_topic_bindings.notification_mode
                  IS DISTINCT FROM EXCLUDED.notification_mode
           OR zulip_topic_bindings.content_hash
                  IS DISTINCT FROM EXCLUDED.content_hash
        """,
        stable_topic_binding_uuid(topic_uuid, user_uuid),
        stream_uuid,
        topic_uuid,
        user_uuid,
        mode,
        content_hash,
        created_at,
    )
    return int(status.rsplit(" ", 1)[-1])


async def _store_chats(
    connection: asyncpg.Connection | PoolConnectionProxy,
    endpoint: str,
    realm_uuid: UUID,
    zulip_user_uuid: UUID,
    chats: Sequence[ZulipChat],
    *,
    replace_catalog: bool,
) -> tuple[int, int]:
    linked_rows = await connection.fetch(
        """
        SELECT workspace_uuid, zulip_external_key
        FROM workspace_zulip_bridge.zulip_entity_links
        WHERE realm_uuid = $1 AND entity_type = 'stream'
          AND zulip_external_key = ANY($2::text[])
        """,
        realm_uuid,
        [chat.chat_key for chat in chats],
    )
    linked_streams = {
        str(row["zulip_external_key"]): UUID(str(row["workspace_uuid"]))
        for row in linked_rows
    }
    stream_uuids = [
        linked_streams.get(chat.chat_key, stable_chat_uuid(endpoint, chat.chat_key))
        for chat in chats
    ]
    binding_uuids = [
        stable_stream_binding_uuid(stream_uuid, zulip_user_uuid)
        for stream_uuid in stream_uuids
    ]
    # Catalogs from many accounts share most stream rows.  Lock the existing
    # rows in one deterministic order before the bulk upsert so concurrent
    # bootstrap and realtime writes cannot form a row-lock cycle.  Unlike a
    # process-wide mutex, this only makes transactions with overlapping chats
    # wait and lets unrelated realtime work continue during history imports.
    await connection.fetch(
        """
        SELECT uuid FROM workspace_zulip_bridge.zulip_streams
        WHERE uuid = ANY($1::uuid[])
        ORDER BY uuid
        FOR UPDATE
        """,
        stream_uuids,
    )
    await connection.fetch(
        """
        SELECT uuid FROM workspace_zulip_bridge.zulip_stream_bindings
        WHERE uuid = ANY($1::uuid[])
        ORDER BY uuid
        FOR UPDATE
        """,
        binding_uuids,
    )
    row = await connection.fetchrow(
        """
        WITH incoming AS MATERIALIZED (
            SELECT stream_uuid, binding_uuid, chat_type, chat_key, name, role,
                   membership_kind, notification_mode, chat_parameters::jsonb,
                   membership_parameters::jsonb, content_hash, membership_hash,
                   available_message_count, first_visible_message_id
            FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::text[],
                        $6::text[], $7::text[], $8::text[], $9::text[], $10::text[],
                        $11::bytea[], $12::bytea[], $13::bigint[], $14::bigint[])
              AS value(stream_uuid, binding_uuid, chat_type, chat_key, name, role,
                       membership_kind, notification_mode, chat_parameters,
                       membership_parameters, content_hash, membership_hash,
                       available_message_count, first_visible_message_id)
        ), streams AS (
            INSERT INTO workspace_zulip_bridge.zulip_streams
                (uuid, realm_uuid, chat_type, chat_key, name, description,
                 invite_only, announce, private, is_archived, color,
                 chat_parameters, content_hash)
            SELECT stream_uuid, $15, chat_type, chat_key, name,
                   chat_parameters ->> 'description',
                   COALESCE((chat_parameters ->> 'invite_only')::boolean, false),
                   COALESCE(
                       (chat_parameters ->> 'is_announcement_only')::boolean,
                       false
                   ),
                   -- Workspace private streams are strictly 1:1. Zulip group
                   -- DMs remain ordinary multi-user streams there, while their
                   -- group_direct kind stays canonical in this bridge table.
                   chat_type = 'direct',
                   COALESCE((chat_parameters ->> 'is_archived')::boolean, false),
                   CASE
                       WHEN membership_parameters ->> 'color'
                            ~ '^#[0-9A-Fa-f]{6}$'
                       THEN ('x' || substr(
                           membership_parameters ->> 'color', 2
                       ))::bit(24)::int
                       ELSE NULL
                   END,
                   chat_parameters, content_hash FROM incoming
            ORDER BY stream_uuid
            ON CONFLICT (uuid) DO UPDATE SET chat_type = EXCLUDED.chat_type,
                name = EXCLUDED.name, description = EXCLUDED.description,
                invite_only = EXCLUDED.invite_only,
                announce = EXCLUDED.announce,
                private = EXCLUDED.private,
                is_archived = EXCLUDED.is_archived,
                color = CASE WHEN EXISTS (
                    SELECT 1
                    FROM workspace_zulip_bridge.zulip_connections AS source
                    WHERE source.uuid = zulip_streams.source_connection_uuid
                      AND source.zulip_user_uuid = $16
                ) THEN EXCLUDED.color ELSE zulip_streams.color END,
                chat_parameters = EXCLUDED.chat_parameters,
                content_hash = EXCLUDED.content_hash
            WHERE zulip_streams.content_hash IS DISTINCT FROM EXCLUDED.content_hash
               OR (
                    zulip_streams.color IS DISTINCT FROM EXCLUDED.color
                    AND EXISTS (
                        SELECT 1
                        FROM workspace_zulip_bridge.zulip_connections AS source
                        WHERE source.uuid = zulip_streams.source_connection_uuid
                          AND source.zulip_user_uuid = $16
                    )
               )
            RETURNING 1
        ), bindings AS (
            INSERT INTO workspace_zulip_bridge.zulip_stream_bindings
                (uuid, zulip_stream_uuid, zulip_user_uuid, role, membership_kind,
                 notification_mode, membership_parameters, content_hash,
                 available_message_count, first_visible_message_id)
            SELECT binding_uuid, stream_uuid, $16, role, membership_kind,
                   notification_mode, membership_parameters, membership_hash,
                   available_message_count, first_visible_message_id FROM incoming
            ORDER BY stream_uuid
            ON CONFLICT (zulip_stream_uuid, zulip_user_uuid) DO UPDATE SET
                role = EXCLUDED.role, membership_kind = EXCLUDED.membership_kind,
                notification_mode = EXCLUDED.notification_mode,
                membership_parameters = EXCLUDED.membership_parameters,
                personal_state_loaded_at = CASE
                    WHEN zulip_stream_bindings.content_hash
                         IS DISTINCT FROM EXCLUDED.content_hash
                      OR zulip_stream_bindings.first_visible_message_id
                         IS DISTINCT FROM EXCLUDED.first_visible_message_id
                    THEN NULL
                    ELSE zulip_stream_bindings.personal_state_loaded_at
                END,
                content_hash = EXCLUDED.content_hash,
                available_message_count = CASE WHEN $17
                    THEN EXCLUDED.available_message_count
                    ELSE zulip_stream_bindings.available_message_count END,
                first_visible_message_id = CASE WHEN $17
                    THEN EXCLUDED.first_visible_message_id
                    ELSE zulip_stream_bindings.first_visible_message_id END
            WHERE zulip_stream_bindings.content_hash IS DISTINCT FROM
                  EXCLUDED.content_hash
               OR ($17 AND (
                    zulip_stream_bindings.available_message_count,
                    zulip_stream_bindings.first_visible_message_id
                  ) IS DISTINCT FROM (
                    EXCLUDED.available_message_count,
                    EXCLUDED.first_visible_message_id
                  ))
            RETURNING 1
        ), removed_bindings AS MATERIALIZED (
            SELECT binding.uuid, binding.zulip_stream_uuid
            FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
            WHERE $17 AND binding.zulip_user_uuid = $16
              AND NOT (binding.zulip_stream_uuid = ANY($1::uuid[]))
        ), binding_tombstones AS (
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                partition_key, direction, delivery_priority,
                source_hash, target_hash, source_updated_at,
                target_updated_at
            )
            SELECT realm.workspace_provider_uuid, 'stream_bindings',
                   removed.uuid, $15, removed.zulip_stream_uuid,
                   'to_workspace', 0, NULL, target.content_hash,
                   clock_timestamp(), target.source_updated_at
            FROM removed_bindings AS removed
            JOIN workspace_zulip_bridge.zulip_realms AS realm
              ON realm.uuid = $15
             AND realm.workspace_provider_uuid IS NOT NULL
            JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
              ON mirror.provider_uuid = realm.workspace_provider_uuid
             AND mirror.bootstrap_status = 'ready'
             AND mirror.active_generation IS NOT NULL
            JOIN workspace_zulip_bridge.workspace_stream_bindings AS target
              ON target.provider_uuid = realm.workspace_provider_uuid
             AND target.snapshot_generation = mirror.active_generation
             AND target.uuid = removed.uuid
            ON CONFLICT (provider_uuid, entity_type, entity_uuid)
            DO UPDATE SET direction = 'to_workspace', delivery_priority = 0,
                partition_key = EXCLUDED.partition_key,
                source_hash = NULL, target_hash = EXCLUDED.target_hash,
                source_updated_at = EXCLUDED.source_updated_at,
                target_updated_at = EXCLUDED.target_updated_at,
                processing_status = 'pending', attempt_count = 0,
                available_at = clock_timestamp(), claimed_at = NULL,
                processed_at = NULL, last_error = NULL,
                updated_at = clock_timestamp()
            RETURNING 1
        ), removed AS (
            DELETE FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
            USING removed_bindings AS candidate
            WHERE binding.uuid = candidate.uuid
            RETURNING binding.zulip_stream_uuid
        ), cleared AS (
            UPDATE workspace_zulip_bridge.zulip_streams AS stream
            SET source_connection_uuid = NULL
            FROM removed
            WHERE stream.uuid = removed.zulip_stream_uuid
              AND stream.source_connection_uuid IN (
                  SELECT uuid FROM workspace_zulip_bridge.zulip_connections
                  WHERE zulip_user_uuid = $16
              ) RETURNING 1
        )
        SELECT GREATEST(
                   (SELECT count(*) FROM streams),
                   (SELECT count(*) FROM bindings)
               ) AS changed_count,
               (SELECT count(*) FROM removed) AS deleted_count,
               (SELECT count(*) FROM binding_tombstones)
                   AS binding_tombstones
        """,
        stream_uuids,
        binding_uuids,
        [chat.chat_type for chat in chats],
        [chat.chat_key for chat in chats],
        [chat.name for chat in chats],
        [chat.role for chat in chats],
        [chat.membership_kind for chat in chats],
        [chat.notification_mode for chat in chats],
        [chat.chat_parameters_json for chat in chats],
        [chat.membership_parameters_json for chat in chats],
        [chat.content_hash for chat in chats],
        [chat.membership_hash for chat in chats],
        [chat.available_message_count for chat in chats],
        [chat.first_visible_message_id for chat in chats],
        realm_uuid,
        zulip_user_uuid,
        replace_catalog,
    )
    if row is None:
        raise RuntimeError("stream catalog query returned no row")
    return row["changed_count"], row["deleted_count"]
