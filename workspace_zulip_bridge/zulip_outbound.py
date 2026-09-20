# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Apply canonical Workspace provider changes to Zulip as their real actors."""

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.message_history import message_content_hash
from workspace_zulip_bridge.message_history import message_flags_hash
from workspace_zulip_bridge.message_history import message_state_hash
from workspace_zulip_bridge.zulip_api import ZulipApiClient


class ZulipOutboundError(RuntimeError):
    """A Workspace mutation cannot currently be represented in Zulip."""


@dataclass(frozen=True, slots=True)
class _Actor:
    connection_uuid: UUID
    realm_uuid: UUID
    user_uuid: UUID
    zulip_user_id: int
    endpoint: str
    login: str
    api_key: str


class ZulipOutboundWriter:
    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings
        self._clients: dict[UUID, ZulipApiClient] = {}

    async def close(self) -> None:
        clients, self._clients = self._clients, {}
        await asyncio.gather(
            *(asyncio.to_thread(client.close) for client in clients.values())
        )

    async def apply(
        self,
        entity_type: str,
        entity_uuid: UUID,
        source: dict[str, Any] | None,
        target: dict[str, Any] | None,
        target_updated_at: datetime | None,
    ) -> None:
        handler = getattr(self, f"_apply_{entity_type}", None)
        if handler is None:
            raise ZulipOutboundError(f"unsupported entity type: {entity_type}")
        await handler(entity_uuid, source, target, target_updated_at)

    async def _apply_users(
        self,
        entity_uuid: UUID,
        source: dict[str, Any] | None,
        target: dict[str, Any] | None,
        _target_updated_at: datetime | None,
    ) -> None:
        if source == target:
            return
        raise ZulipOutboundError("Workspace user profile writes are not supported")

    async def _apply_streams(
        self,
        entity_uuid: UUID,
        source: dict[str, Any] | None,
        target: dict[str, Any] | None,
        _target_updated_at: datetime | None,
    ) -> None:
        if target is None:
            if source is None:
                return
            stream = await self._stream(entity_uuid)
            if stream is None or not str(stream["chat_key"]).startswith("channel:"):
                raise ZulipOutboundError("direct conversations cannot be deleted")
            actor = await self._actor(UUID(str(source["owner_uuid"])), entity_uuid)
            await asyncio.to_thread(
                self._client(actor).update_stream,
                int(str(stream["chat_key"]).removeprefix("channel:")),
                name=None,
                description=None,
                is_archived=True,
            )
            return
        if source is None:
            await self._create_stream(entity_uuid, target)
            return
        stream = await self._required_stream(entity_uuid)
        if not str(stream["chat_key"]).startswith("channel:"):
            return
        actor = await self._actor(UUID(str(target["owner_uuid"])), entity_uuid)
        stream_id = int(str(stream["chat_key"]).removeprefix("channel:"))
        name = str(target["name"]) if target.get("name") != source.get("name") else None
        description = (
            str(target.get("description") or "")
            if target.get("description") != source.get("description")
            else None
        )
        await asyncio.to_thread(
            self._client(actor).update_stream,
            stream_id,
            name=name,
            description=description,
            is_archived=(
                bool(target.get("is_archived"))
                if target.get("is_archived") != source.get("is_archived")
                else None
            ),
        )

    async def _create_stream(
        self,
        entity_uuid: UUID,
        target: dict[str, Any],
    ) -> None:
        workspace_owner_uuid = UUID(str(target["owner_uuid"]))
        actor = await self._actor(workspace_owner_uuid)
        owner_uuid = actor.user_uuid
        if bool(target.get("private")):
            user_rows = await self._pool.fetch(
                """
                SELECT DISTINCT zulip_user.zulip_user_id
                FROM workspace_zulip_bridge.workspace_stream_bindings AS binding
                JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
                  ON mirror.provider_uuid = binding.provider_uuid
                 AND mirror.active_generation = binding.snapshot_generation
                JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                  ON zulip_user.uuid = (binding.data ->> 'user_uuid')::uuid
                  OR zulip_user.workspace_user_uuid =
                     (binding.data ->> 'user_uuid')::uuid
                WHERE binding.provider_uuid = $1
                  AND (binding.data ->> 'stream_uuid')::uuid = $2
                ORDER BY zulip_user.zulip_user_id
                """,
                self._settings.workspace_provider_uuid,
                entity_uuid,
            )
            participant_ids = [int(row["zulip_user_id"]) for row in user_rows]
            if actor.zulip_user_id not in participant_ids:
                participant_ids.append(actor.zulip_user_id)
            if not 1 <= len(set(participant_ids)) <= 2:
                raise ZulipOutboundError(
                    "private stream must contain at most two users"
                )
            chat_key = "direct:" + ",".join(
                str(value) for value in sorted(set(participant_ids))
            )
            chat_type = "direct"
            direct_user_uuid = next(
                (
                    UUID(str(row["data"]["user_uuid"]))
                    for row in await self._target_rows(
                        "stream_bindings", "stream_uuid", entity_uuid
                    )
                    if UUID(str(row["data"]["user_uuid"])) != workspace_owner_uuid
                ),
                None,
            )
            if direct_user_uuid is not None:
                direct_user_uuid = await self._zulip_user_uuid(direct_user_uuid)
        else:
            stream_id = await asyncio.to_thread(
                self._client(actor).create_stream,
                str(target["name"]),
                description=(
                    str(target["description"])
                    if target.get("description") is not None
                    else None
                ),
                invite_only=bool(target.get("invite_only")),
            )
            chat_key = f"channel:{stream_id}"
            chat_type = "channel"
            direct_user_uuid = None
        await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_entity_links (
                realm_uuid, entity_type, workspace_uuid, zulip_external_key
            ) VALUES ($1, 'stream', $2, $3)
            ON CONFLICT (realm_uuid, entity_type, workspace_uuid) DO UPDATE
            SET zulip_external_key = EXCLUDED.zulip_external_key,
                updated_at = clock_timestamp()
            """,
            actor.realm_uuid,
            entity_uuid,
            chat_key,
        )
        await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_streams (
                uuid, realm_uuid, chat_type, chat_key, name, description,
                owner_user_uuid, invite_only, announce, direct_user_uuid,
                private, is_archived, color, chat_parameters, content_hash,
                source_connection_uuid, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14::jsonb, $15, $16, $17
            ) ON CONFLICT (uuid) DO NOTHING
            """,
            entity_uuid,
            actor.realm_uuid,
            chat_type,
            chat_key,
            str(target["name"]),
            target.get("description"),
            owner_uuid,
            bool(target.get("invite_only")),
            bool(target.get("announce")),
            direct_user_uuid,
            bool(target.get("private")),
            bool(target.get("is_archived")),
            target.get("color"),
            json.dumps(
                {
                    "history_public_to_subscribers": bool(
                        target.get("history_public_to_subscribers", True)
                    )
                },
                separators=(",", ":"),
            ),
            _canonical_hash(target),
            actor.connection_uuid,
            datetime.fromisoformat(str(target["created_at"]).replace("Z", "+00:00")),
        )

    async def _apply_stream_bindings(
        self,
        entity_uuid: UUID,
        source: dict[str, Any] | None,
        target: dict[str, Any] | None,
        _target_updated_at: datetime | None,
    ) -> None:
        data = target or source
        if data is None:
            return
        stream = await self._required_stream(UUID(str(data["stream_uuid"])))
        actor = await self._actor(UUID(str(data["user_uuid"])), stream["uuid"])
        if str(stream["chat_key"]).startswith("channel:"):
            await asyncio.to_thread(
                self._client(actor).update_subscription,
                str(stream["name"]),
                enabled=target is not None,
            )
            if target is not None:
                muted = target.get("notification_mode") == "muted"
                await asyncio.to_thread(
                    self._client(actor).update_subscription_property,
                    int(str(stream["chat_key"]).removeprefix("channel:")),
                    "is_muted",
                    muted,
                )
        if target is None:
            await self._pool.execute(
                "DELETE FROM workspace_zulip_bridge.zulip_stream_bindings "
                "WHERE uuid = $1",
                entity_uuid,
            )
            return
        created_at = datetime.fromisoformat(
            str(target["created_at"]).replace("Z", "+00:00")
        )
        await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                uuid, zulip_stream_uuid, zulip_user_uuid, role,
                membership_kind, notification_mode, content_hash, created_at
            ) VALUES ($1, $2, $3, $4, 'subscriber', $5, $6, $7)
            ON CONFLICT (uuid) DO UPDATE SET
                role = EXCLUDED.role,
                notification_mode = EXCLUDED.notification_mode,
                content_hash = EXCLUDED.content_hash,
                updated_at = clock_timestamp()
            """,
            entity_uuid,
            UUID(str(target["stream_uuid"])),
            actor.user_uuid,
            str(target.get("role", "member")),
            str(target.get("notification_mode", "all_messages")),
            _canonical_hash(target),
            created_at,
        )

    async def _apply_topics(
        self,
        entity_uuid: UUID,
        source: dict[str, Any] | None,
        target: dict[str, Any] | None,
        _target_updated_at: datetime | None,
    ) -> None:
        if target is None:
            raise ZulipOutboundError("Zulip topics cannot be deleted directly")
        if source is None:
            return
        desired_name = str(target["name"])
        if bool(target.get("is_done")) and not desired_name.startswith("✔"):
            desired_name = f"✔ {desired_name}"
        if desired_name == source.get("name"):
            return
        row = await self._pool.fetchrow(
            """
            SELECT message.zulip_message_id, stream.uuid AS stream_uuid
            FROM workspace_zulip_bridge.zulip_messages AS message
            JOIN workspace_zulip_bridge.zulip_streams AS stream
              ON stream.uuid = message.zulip_stream_uuid
            WHERE message.topic_uuid = $1
            ORDER BY message.zulip_message_id DESC LIMIT 1
            """,
            entity_uuid,
        )
        if row is None:
            return
        actor = await self._stream_actor(row["stream_uuid"])
        await asyncio.to_thread(
            self._client(actor).update_message,
            int(row["zulip_message_id"]),
            topic=desired_name,
            propagate_mode="change_all",
        )

    async def _apply_topic_bindings(
        self,
        entity_uuid: UUID,
        source: dict[str, Any] | None,
        target: dict[str, Any] | None,
        _target_updated_at: datetime | None,
    ) -> None:
        data = target or source
        if data is None:
            return
        stream = await self._required_stream(UUID(str(data["stream_uuid"])))
        if not str(stream["chat_key"]).startswith("channel:"):
            return
        topic = await self._target_or_source_topic(UUID(str(data["topic_uuid"])))
        actor = await self._actor(UUID(str(data["user_uuid"])), stream["uuid"])
        mode = target.get("notification_mode", "default") if target else "default"
        await asyncio.to_thread(
            self._client(actor).update_topic_notification,
            int(str(stream["chat_key"]).removeprefix("channel:")),
            str(topic["name"]),
            muted=mode == "mute",
        )
        if target is None:
            await self._pool.execute(
                "DELETE FROM workspace_zulip_bridge.zulip_topic_bindings "
                "WHERE uuid = $1",
                entity_uuid,
            )
            return
        created_at = datetime.fromisoformat(
            str(target["created_at"]).replace("Z", "+00:00")
        )
        await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_topic_bindings (
                uuid, zulip_stream_uuid, topic_uuid, zulip_user_uuid,
                notification_mode, content_hash, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (uuid) DO UPDATE SET
                notification_mode = EXCLUDED.notification_mode,
                content_hash = EXCLUDED.content_hash,
                updated_at = clock_timestamp()
            """,
            entity_uuid,
            UUID(str(target["stream_uuid"])),
            UUID(str(target["topic_uuid"])),
            actor.user_uuid,
            str(target.get("notification_mode", "default")),
            _canonical_hash(target),
            created_at,
        )

    async def _apply_messages(
        self,
        entity_uuid: UUID,
        source: dict[str, Any] | None,
        target: dict[str, Any] | None,
        target_updated_at: datetime | None,
    ) -> None:
        if target is None:
            row = await self._message(entity_uuid)
            if row is None or source is None:
                return
            actor = await self._actor(
                UUID(str(source["author_uuid"])), row["stream_uuid"]
            )
            await asyncio.to_thread(
                self._client(actor).delete_message,
                int(row["zulip_message_id"]),
            )
            return
        if source is None:
            await self._create_message(entity_uuid, target, target_updated_at)
            return
        row = await self._message(entity_uuid)
        if row is None:
            raise ZulipOutboundError("Zulip message identity is unavailable")
        actor = await self._actor(UUID(str(target["author_uuid"])), row["stream_uuid"])
        source_payload = source.get("payload") or {}
        target_payload = target.get("payload") or {}
        content = (
            str(target_payload.get("content", ""))
            if target_payload != source_payload
            else None
        )
        topic = None
        if target.get("topic_uuid") != source.get("topic_uuid"):
            topic_data = await self._target_or_source_topic(
                UUID(str(target["topic_uuid"]))
            )
            topic = str(topic_data["name"])
        await asyncio.to_thread(
            self._client(actor).update_message,
            int(row["zulip_message_id"]),
            content=content,
            topic=topic,
        )

    async def _create_message(
        self,
        entity_uuid: UUID,
        target: dict[str, Any],
        target_updated_at: datetime | None,
    ) -> None:
        stream_uuid = UUID(str(target["stream_uuid"]))
        stream = await self._required_stream(stream_uuid)
        author_uuid = UUID(str(target["author_uuid"]))
        actor = await self._actor(author_uuid, stream_uuid)
        topic_uuid = UUID(str(target["topic_uuid"]))
        topic = await self._ensure_topic(topic_uuid, stream_uuid)
        payload = target.get("payload")
        if not isinstance(payload, dict) or payload.get("kind") != "markdown":
            raise ZulipOutboundError("only markdown messages can be sent to Zulip")
        content = str(payload.get("content", ""))
        message_id = await asyncio.to_thread(
            self._client(actor).send_message,
            str(stream["chat_key"]),
            actor.zulip_user_id,
            content,
            topic=str(topic["name"]),
        )
        await asyncio.to_thread(
            self._client(actor).update_message_flag,
            message_id,
            "read",
            True,
        )
        created_at = datetime.fromisoformat(
            str(target["created_at"]).replace("Z", "+00:00")
        )
        sent_at = int(created_at.timestamp())
        content_hash = message_content_hash(
            sender_user_uuid=actor.user_uuid,
            chat_key=str(stream["chat_key"]),
            topic_name=str(topic["name"]),
            content=content,
            sent_at=sent_at,
        )
        state_hash = message_state_hash(
            sender_user_uuid=actor.user_uuid,
            chat_key=str(stream["chat_key"]),
            topic_name=str(topic["name"]),
            content=content,
            sent_at=sent_at,
            reactions=(),
        )
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_entity_links (
                    realm_uuid, entity_type, workspace_uuid, zulip_external_key
                ) VALUES ($1, 'message', $2, $3)
                ON CONFLICT (realm_uuid, entity_type, workspace_uuid) DO UPDATE
                SET zulip_external_key = EXCLUDED.zulip_external_key,
                    updated_at = clock_timestamp()
                """,
                actor.realm_uuid,
                entity_uuid,
                str(message_id),
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    topic_uuid, sender_user_uuid, zulip_message_id, content,
                    reactions, reaction_users, content_hash, message_hash,
                    created_at, source_updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    '[]'::jsonb, '{}'::jsonb, $9, $10, $11, $12
                ) ON CONFLICT (uuid) DO NOTHING
                """,
                entity_uuid,
                actor.realm_uuid,
                stream["source_connection_uuid"],
                stream_uuid,
                topic_uuid,
                actor.user_uuid,
                message_id,
                content,
                content_hash,
                state_hash,
                created_at,
                target_updated_at or created_at,
            )

    async def _apply_message_flags(
        self,
        entity_uuid: UUID,
        source: dict[str, Any] | None,
        target: dict[str, Any] | None,
        _target_updated_at: datetime | None,
    ) -> None:
        data = target or source
        if data is None:
            return
        message = await self._message(UUID(str(data["message_uuid"])))
        if message is None:
            raise ZulipOutboundError("Zulip message identity is unavailable")
        actor = await self._actor(UUID(str(data["user_uuid"])), message["stream_uuid"])
        desired = target or {
            **data,
            "read": False,
            "starred": False,
        }
        for field, zulip_flag in (("read", "read"), ("starred", "starred")):
            if source is None or bool(source.get(field)) != bool(desired.get(field)):
                await asyncio.to_thread(
                    self._client(actor).update_message_flag,
                    int(message["zulip_message_id"]),
                    zulip_flag,
                    bool(desired.get(field)),
                )
        if target is None:
            await self._pool.execute(
                "DELETE FROM workspace_zulip_bridge.zulip_message_flags "
                "WHERE uuid = $1",
                entity_uuid,
            )
            return
        values = {
            "is_read": bool(target.get("read")),
            "is_starred": bool(target.get("starred")),
            "is_collapsed": False,
            "is_mentioned": bool(target.get("mentioned")),
            "is_stream_wildcard_mentioned": False,
            "is_topic_wildcard_mentioned": False,
            "has_alert_word": False,
            "is_historical": False,
        }
        await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_message_flags (
                uuid, realm_uuid, zulip_stream_uuid, message_uuid,
                zulip_user_uuid, is_read, is_starred, is_collapsed,
                is_mentioned, is_stream_wildcard_mentioned,
                is_topic_wildcard_mentioned, has_alert_word, is_historical,
                flags_hash
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
            ) ON CONFLICT (uuid) DO UPDATE SET
                is_read = EXCLUDED.is_read,
                is_starred = EXCLUDED.is_starred,
                is_mentioned = EXCLUDED.is_mentioned,
                flags_hash = EXCLUDED.flags_hash,
                updated_at = clock_timestamp()
            """,
            entity_uuid,
            actor.realm_uuid,
            UUID(str(target["stream_uuid"])),
            UUID(str(target["message_uuid"])),
            actor.user_uuid,
            *values.values(),
            message_flags_hash(**values),
        )

    async def _apply_message_reactions(
        self,
        _entity_uuid: UUID,
        source: dict[str, Any] | None,
        target: dict[str, Any] | None,
        _target_updated_at: datetime | None,
    ) -> None:
        data = target or source
        if data is None:
            return
        message = await self._message(UUID(str(data["message_uuid"])))
        if message is None:
            raise ZulipOutboundError("Zulip message identity is unavailable")
        actor = await self._actor(UUID(str(data["user_uuid"])), message["stream_uuid"])
        await asyncio.to_thread(
            self._client(actor).update_reaction,
            int(message["zulip_message_id"]),
            str(data["emoji_name"]),
            enabled=target is not None,
        )

    async def _actor(
        self,
        user_uuid: UUID,
        stream_uuid: UUID | None = None,
    ) -> _Actor:
        row = await self._pool.fetchrow(
            """
            SELECT connection.uuid AS connection_uuid, connection.realm_uuid,
                   connection.zulip_user_uuid AS user_uuid,
                   zulip_user.zulip_user_id, realm.identity_key AS endpoint,
                   connection.login, connection.api_key
            FROM workspace_zulip_bridge.zulip_connections AS connection
            JOIN workspace_zulip_bridge.zulip_users AS zulip_user
              ON zulip_user.uuid = connection.zulip_user_uuid
            JOIN workspace_zulip_bridge.zulip_realms AS realm
              ON realm.uuid = connection.realm_uuid
            WHERE (
                    connection.zulip_user_uuid = $1
                    OR zulip_user.workspace_user_uuid = $1
                  )
              AND NOT zulip_user.disabled
            ORDER BY (connection.zulip_user_uuid = $1) DESC,
                     connection.sync_enabled DESC, connection.uuid
            LIMIT 1
            """,
            user_uuid,
        )
        if row is None and stream_uuid is not None:
            row = await self._pool.fetchrow(
                """
                SELECT connection.uuid AS connection_uuid, connection.realm_uuid,
                       connection.zulip_user_uuid AS user_uuid,
                       zulip_user.zulip_user_id, realm.identity_key AS endpoint,
                       connection.login, connection.api_key
                FROM workspace_zulip_bridge.zulip_streams AS stream
                JOIN workspace_zulip_bridge.zulip_connections AS connection
                  ON connection.uuid = stream.source_connection_uuid
                JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                  ON zulip_user.uuid = connection.zulip_user_uuid
                JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = connection.realm_uuid
                WHERE stream.uuid = $1 AND NOT zulip_user.disabled
                """,
                stream_uuid,
            )
        if row is None:
            raise ZulipOutboundError(f"Zulip credential is unavailable for {user_uuid}")
        return _Actor(**dict(row))

    async def _zulip_user_uuid(self, user_uuid: UUID) -> UUID:
        value = await self._pool.fetchval(
            """
            SELECT uuid
            FROM workspace_zulip_bridge.zulip_users
            WHERE uuid = $1 OR workspace_user_uuid = $1
            ORDER BY (uuid = $1) DESC
            LIMIT 1
            """,
            user_uuid,
        )
        if value is None:
            raise ZulipOutboundError(f"Zulip identity is unavailable for {user_uuid}")
        return UUID(str(value))

    async def _stream_actor(self, stream_uuid: UUID) -> _Actor:
        stream = await self._required_stream(stream_uuid)
        owner_uuid = stream["owner_user_uuid"]
        if owner_uuid is not None:
            return await self._actor(UUID(str(owner_uuid)), stream_uuid)
        return await self._actor(UUID(int=0), stream_uuid)

    def _client(self, actor: _Actor) -> ZulipApiClient:
        client = self._clients.get(actor.connection_uuid)
        if client is None:
            client = ZulipApiClient(
                actor.endpoint,
                actor.login,
                actor.api_key,
                ca_file=self._settings.zulip_ca_file,
                connect_timeout_seconds=self._settings.zulip_connect_timeout_seconds,
                default_longpoll_timeout_seconds=(
                    self._settings.zulip_default_longpoll_timeout_seconds
                ),
                idle_queue_timeout_seconds=self._settings.zulip_idle_queue_timeout_seconds,
                chat_fill_timeout_seconds=self._settings.zulip_chat_fill_timeout_seconds,
                message_page_size=self._settings.zulip_message_page_size,
            )
            self._clients[actor.connection_uuid] = client
        return client

    async def _message(self, message_uuid: UUID) -> asyncpg.Record | None:
        return await self._pool.fetchrow(
            """
            SELECT uuid, zulip_message_id, zulip_stream_uuid AS stream_uuid,
                   sender_user_uuid
            FROM workspace_zulip_bridge.zulip_messages WHERE uuid = $1
            """,
            message_uuid,
        )

    async def _stream(self, stream_uuid: UUID) -> asyncpg.Record | None:
        return await self._pool.fetchrow(
            """
            SELECT uuid, realm_uuid, chat_key, name, owner_user_uuid,
                   source_connection_uuid
            FROM workspace_zulip_bridge.zulip_streams WHERE uuid = $1
            """,
            stream_uuid,
        )

    async def _required_stream(self, stream_uuid: UUID) -> asyncpg.Record:
        stream = await self._stream(stream_uuid)
        if stream is None:
            raise ZulipOutboundError("Zulip stream identity is unavailable")
        return stream

    async def _target_rows(
        self,
        entity_type: str,
        field: str,
        value: UUID,
    ) -> list[asyncpg.Record]:
        return list(
            await self._pool.fetch(
                f"""
                SELECT entity.data
                FROM workspace_zulip_bridge.workspace_{entity_type} AS entity
                JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
                  ON mirror.provider_uuid = entity.provider_uuid
                 AND mirror.active_generation = entity.snapshot_generation
                WHERE entity.provider_uuid = $1
                  AND (entity.data ->> '{field}')::uuid = $2
                """,
                self._settings.workspace_provider_uuid,
                value,
            )
        )

    async def _target_or_source_topic(self, topic_uuid: UUID) -> dict[str, Any]:
        row = await self._pool.fetchrow(
            """
            SELECT data FROM workspace_zulip_bridge.workspace_topics AS topic
            JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
              ON mirror.provider_uuid = topic.provider_uuid
             AND mirror.active_generation = topic.snapshot_generation
            WHERE topic.provider_uuid = $1 AND topic.uuid = $2
            """,
            self._settings.workspace_provider_uuid,
            topic_uuid,
        )
        if row is not None:
            return dict(row["data"])
        row = await self._pool.fetchrow(
            "SELECT name FROM workspace_zulip_bridge.zulip_topics WHERE uuid = $1",
            topic_uuid,
        )
        if row is None:
            raise ZulipOutboundError("Zulip topic identity is unavailable")
        return {"name": row["name"]}

    async def _ensure_topic(
        self,
        topic_uuid: UUID,
        stream_uuid: UUID,
    ) -> asyncpg.Record:
        row = await self._pool.fetchrow(
            "SELECT uuid, name FROM workspace_zulip_bridge.zulip_topics WHERE uuid = $1",
            topic_uuid,
        )
        if row is not None:
            return row
        data = await self._target_or_source_topic(topic_uuid)
        name = str(data["name"])
        await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_topics (
                uuid, zulip_stream_uuid, name, is_done, version, content_hash,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (uuid) DO NOTHING
            """,
            topic_uuid,
            stream_uuid,
            name,
            bool(data.get("is_done")),
            int(data.get("version", 0)),
            hashlib.sha256(name.encode("utf-8")).digest(),
            datetime.fromisoformat(str(data["created_at"]).replace("Z", "+00:00")),
        )
        row = await self._pool.fetchrow(
            "SELECT uuid, name FROM workspace_zulip_bridge.zulip_topics WHERE uuid = $1",
            topic_uuid,
        )
        assert row is not None
        return row


def _canonical_hash(data: dict[str, Any]) -> bytes:
    return hashlib.sha256(
        json.dumps(
            data,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()
