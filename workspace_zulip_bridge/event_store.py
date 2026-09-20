# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import hashlib
import json
from collections.abc import Sequence
from typing import cast
from uuid import UUID

import asyncpg
from asyncpg.pool import PoolConnectionProxy

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
from workspace_zulip_bridge.stable_ids import canonical_endpoint
from workspace_zulip_bridge.stable_ids import stable_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_file_uuid
from workspace_zulip_bridge.stable_ids import stable_message_flag_uuid
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_reaction_uuid
from workspace_zulip_bridge.stable_ids import stable_realm_uuid
from workspace_zulip_bridge.stable_ids import stable_stream_binding_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid


class EventStore:
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

    async def has_pending_history(self, user_uuid: UUID, queue_id: str) -> bool:
        async with self._pool.acquire() as connection:
            return bool(
                await connection.fetchval(
                    """
                SELECT EXISTS (
                    SELECT 1
                    FROM workspace_zulip_bridge.zulip_connections AS connection
                    JOIN workspace_zulip_bridge.zulip_streams AS stream
                      ON stream.source_connection_uuid = connection.uuid
                     AND stream.history_loaded_at IS NULL
                    WHERE connection.uuid = $1 AND connection.queue_id = $2
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
                ), removed AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    USING invalid
                    WHERE message.zulip_stream_uuid = invalid.uuid
                      AND message.source_connection_uuid = invalid.source_connection_uuid
                    RETURNING 1
                ), cleared AS (
                    UPDATE workspace_zulip_bridge.zulip_streams AS stream
                    SET source_connection_uuid = NULL
                    FROM invalid WHERE stream.uuid = invalid.uuid RETURNING 1
                )
                SELECT (SELECT count(*) FROM cleared) AS invalidated_count,
                       (SELECT count(*) FROM removed) AS messages_deleted
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
                    GROUP BY connection.realm_uuid
                    HAVING bool_and(connection.catalog_completed_at IS NOT NULL)
                ), winners AS MATERIALIZED (
                    SELECT DISTINCT ON (stream.uuid)
                           stream.uuid AS stream_uuid,
                           connection.uuid AS connection_uuid
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
                    ORDER BY stream.uuid,
                             CASE binding.role
                               WHEN 'owner' THEN 1 WHEN 'administrator' THEN 2
                               WHEN 'moderator' THEN 3 WHEN 'member' THEN 4
                               ELSE 5 END,
                             connection.uuid
                ), changed AS MATERIALIZED (
                    SELECT stream.uuid,
                           stream.source_connection_uuid AS old_connection_uuid,
                           winners.connection_uuid AS new_connection_uuid
                    FROM workspace_zulip_bridge.zulip_streams AS stream
                    JOIN winners ON winners.stream_uuid = stream.uuid
                    WHERE stream.source_connection_uuid IS DISTINCT FROM
                          winners.connection_uuid
                    FOR UPDATE OF stream
                ), removed AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    USING changed
                    WHERE changed.old_connection_uuid IS NOT NULL
                      AND message.zulip_stream_uuid = changed.uuid
                      AND message.source_connection_uuid = changed.old_connection_uuid
                    RETURNING 1
                ), updated AS (
                    UPDATE workspace_zulip_bridge.zulip_streams AS stream
                    SET source_connection_uuid = changed.new_connection_uuid
                    FROM changed WHERE stream.uuid = changed.uuid RETURNING 1
                )
                SELECT (SELECT count(*) FROM updated) AS assigned_count,
                       (SELECT count(*) FROM removed) AS messages_deleted
                """
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections AS connection
                SET lifecycle_status = CASE WHEN EXISTS (
                    SELECT 1 FROM workspace_zulip_bridge.zulip_streams AS stream
                    WHERE stream.source_connection_uuid = connection.uuid
                      AND stream.history_loaded_at IS NULL
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
                           avatar_url
                    FROM unnest($2::uuid[], $3::bigint[], $4::text[], $5::text[],
                                $6::smallint[], $7::boolean[], $8::boolean[],
                                $9::text[])
                      AS directory(uuid, user_id, login, full_name, role, disabled,
                                   is_bot, avatar_url)
                ), upserted AS (
                    INSERT INTO workspace_zulip_bridge.zulip_users
                        (uuid, realm_uuid, zulip_user_id, login, full_name, role,
                         disabled, is_bot, avatar_url)
                    SELECT uuid, $1, user_id, login, full_name, role, disabled,
                           is_bot, avatar_url FROM incoming
                    ON CONFLICT (uuid) DO UPDATE
                    SET login = EXCLUDED.login, full_name = EXCLUDED.full_name,
                        role = EXCLUDED.role, disabled = EXCLUDED.disabled,
                        is_bot = EXCLUDED.is_bot,
                        avatar_url = EXCLUDED.avatar_url
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
            )
        if row is None:
            raise RuntimeError("user directory query returned no row")
        return UserDirectoryWrite(
            users=len(users), bots=bot_count, changed=row["changed_count"]
        )

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
            page = await history.store_page(messages)
        finally:
            await history.close()
        async with self._pool.acquire() as connection:
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
                "DELETE FROM workspace_zulip_bridge.zulip_messages "
                "WHERE source_connection_uuid = $1",
                user_uuid,
            )
            await connection.execute(
                "UPDATE workspace_zulip_bridge.zulip_streams "
                "SET history_loaded_at = NULL WHERE source_connection_uuid = $1",
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
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.source_connection_uuid = connection.uuid
                 AND stream.history_loaded_at IS NULL
                JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
                  ON binding.zulip_stream_uuid = stream.uuid
                 AND binding.zulip_user_uuid = connection.zulip_user_uuid
                WHERE connection.uuid = $1 AND connection.queue_id = $2
                  AND connection.lifecycle_status = 'backfilling'
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
        self, user_uuid: UUID, queue_id: str, catalog: ZulipChatCatalog
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
            if owner["streams_hash"] == catalog.content_hash:
                await connection.execute(
                    "UPDATE workspace_zulip_bridge.zulip_connections "
                    "SET lifecycle_status = 'scheduling', "
                    "catalog_completed_at = clock_timestamp() "
                    "WHERE uuid = $1 AND queue_id = $2",
                    user_uuid,
                    queue_id,
                )
                return ChatCatalogWrite(True, True, 0, 0)
            upserted, deleted = await _store_chats(
                connection,
                owner["endpoint"],
                owner["realm_uuid"],
                owner["zulip_user_uuid"],
                catalog.chats,
                replace_catalog=True,
            )
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
        return ChatCatalogWrite(True, False, upserted, deleted)

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
                SELECT page.*
                FROM wzb_message_page AS page
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = page.stream_uuid
                 AND stream.source_connection_uuid = $1
                JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
                  ON binding.zulip_stream_uuid = stream.uuid
                 AND binding.zulip_user_uuid = $3
                LEFT JOIN workspace_zulip_bridge.zulip_topics AS topic
                  ON topic.uuid = page.topic_uuid
                WHERE page.topic_uuid IS NULL OR topic.uuid IS NOT NULL
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
        if self._endpoint is None:
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
                WITH deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_messages AS message
                    WHERE message.source_connection_uuid = $1
                      AND message.zulip_stream_uuid = ANY($2::uuid[])
                      AND NOT EXISTS (SELECT 1 FROM wzb_seen_messages AS seen
                                      WHERE seen.zulip_message_id = message.zulip_message_id)
                    RETURNING 1
                ) SELECT count(*) FROM deleted
                """,
                    self._connection_uuid,
                    stream_uuids,
                )
            )
            deleted_topics = int(
                await self._connection.fetchval(
                    """
                WITH deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_topics AS topic
                    WHERE topic.zulip_stream_uuid = ANY($1::uuid[])
                      AND NOT EXISTS (SELECT 1 FROM workspace_zulip_bridge.zulip_messages
                                      WHERE topic_uuid = topic.uuid)
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
                UPDATE workspace_zulip_bridge.zulip_connections AS connection
                SET lifecycle_status = CASE WHEN EXISTS (
                    SELECT 1 FROM workspace_zulip_bridge.zulip_streams AS stream
                    WHERE stream.source_connection_uuid = $1
                      AND stream.history_loaded_at IS NULL
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
                (uuid, realm_uuid, chat_type, chat_key, name, chat_parameters,
                 content_hash)
            SELECT stream_uuid, $15, chat_type, chat_key, name, chat_parameters,
                   content_hash FROM incoming
            ON CONFLICT (uuid) DO UPDATE SET chat_type = EXCLUDED.chat_type,
                name = EXCLUDED.name, chat_parameters = EXCLUDED.chat_parameters,
                content_hash = EXCLUDED.content_hash
            WHERE zulip_streams.content_hash IS DISTINCT FROM EXCLUDED.content_hash
            RETURNING 1
        ), bindings AS (
            INSERT INTO workspace_zulip_bridge.zulip_stream_bindings
                (uuid, zulip_stream_uuid, zulip_user_uuid, role, membership_kind,
                 notification_mode, membership_parameters, content_hash,
                 available_message_count, first_visible_message_id)
            SELECT binding_uuid, stream_uuid, $16, role, membership_kind,
                   notification_mode, membership_parameters, membership_hash,
                   available_message_count, first_visible_message_id FROM incoming
            ON CONFLICT (zulip_stream_uuid, zulip_user_uuid) DO UPDATE SET
                role = EXCLUDED.role, membership_kind = EXCLUDED.membership_kind,
                notification_mode = EXCLUDED.notification_mode,
                membership_parameters = EXCLUDED.membership_parameters,
                content_hash = EXCLUDED.content_hash,
                available_message_count = EXCLUDED.available_message_count,
                first_visible_message_id = EXCLUDED.first_visible_message_id
            WHERE (zulip_stream_bindings.content_hash,
                   zulip_stream_bindings.available_message_count,
                   zulip_stream_bindings.first_visible_message_id) IS DISTINCT FROM
                  (EXCLUDED.content_hash, EXCLUDED.available_message_count,
                   EXCLUDED.first_visible_message_id)
            RETURNING 1
        ), removed AS (
            DELETE FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
            WHERE $17 AND binding.zulip_user_uuid = $16
              AND NOT (binding.zulip_stream_uuid = ANY($1::uuid[]))
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
        SELECT (SELECT count(*) FROM bindings) AS changed_count,
               (SELECT count(*) FROM removed) AS deleted_count
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
