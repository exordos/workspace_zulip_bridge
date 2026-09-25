# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import httpx
import pytest
from websockets.asyncio.server import ServerConnection
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosedOK

from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder
from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.database import open_pool
from workspace_zulip_bridge.database import prepare_database
from workspace_zulip_bridge.event_processor import ZulipEventProcessor
from workspace_zulip_bridge.event_processor import _user_status_change
from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.message_history import message_flags_hash
from workspace_zulip_bridge.models import UserDirectoryWrite
from workspace_zulip_bridge.models import ZulipAttachment
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.models import ZulipEvent
from workspace_zulip_bridge.models import ZulipMessage
from workspace_zulip_bridge.models import ZulipUserPresence
from workspace_zulip_bridge.models import ZulipUserProfileStatus
from workspace_zulip_bridge.models import ZulipUserTopic
from workspace_zulip_bridge.monitor import collect_snapshot
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
from workspace_zulip_bridge.workspace_control import WorkspaceControlWorker
from workspace_zulip_bridge.workspace_events import WorkspaceEvent
from workspace_zulip_bridge.workspace_events import WorkspaceEventReceiver
from workspace_zulip_bridge.workspace_events import WorkspaceEventStore
from workspace_zulip_bridge.workspace_sync import _SOURCE_TABLES
from workspace_zulip_bridge.workspace_sync import ProviderApiError
from workspace_zulip_bridge.workspace_sync import WorkspaceBootstrapper
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker
from workspace_zulip_bridge.workspace_sync import WorkspaceEventProcessor
from workspace_zulip_bridge.zulip_api import ZulipApiError
from workspace_zulip_bridge.zulip_outbound import ZulipOutboundError
from workspace_zulip_bridge.zulip_outbound import ZulipOutboundPending
from workspace_zulip_bridge.zulip_outbound import ZulipOutboundWriter

ENDPOINT = "https://zulip.example.test"


def test_user_status_event_accepts_one_field_updates() -> None:
    text_only = _user_status_change({"user_id": 7, "status_text": "Focused"})
    emoji_only = _user_status_change({"user_id": 7, "emoji_name": "target"})

    assert text_only == ZulipUserProfileStatus(
        7,
        "Focused",
        None,
        update_status_emoji=False,
    )
    assert emoji_only == ZulipUserProfileStatus(
        7,
        None,
        "target",
        update_status_text=False,
    )
    assert _user_status_change({"user_id": 7}) is None


def _dsn() -> str:
    dsn = os.environ.get("WZB_TEST_DATABASE_DSN")
    if dsn is None:
        pytest.skip("WZB_TEST_DATABASE_DSN does not name a disposable database")
    return dsn


async def _pool(dsn: str) -> asyncpg.Pool:
    settings = Settings.from_env(
        {
            "WZB_DATABASE_DSN": dsn,
            "WZB_DB_POOL_MIN_SIZE": "1",
            "WZB_DB_POOL_MAX_SIZE": "4",
            "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
        }
    )
    pool = await open_pool(settings)
    await prepare_database(pool)
    async with pool.acquire() as connection:
        await connection.execute(
            """
            TRUNCATE workspace_zulip_bridge.sync_diffs,
                     workspace_zulip_bridge.sync_plan_cursors,
                     workspace_zulip_bridge.sync_repair_cursors,
                     workspace_zulip_bridge.workspace_users,
                     workspace_zulip_bridge.workspace_streams,
                     workspace_zulip_bridge.workspace_stream_bindings,
                     workspace_zulip_bridge.workspace_topics,
                     workspace_zulip_bridge.workspace_topic_bindings,
                     workspace_zulip_bridge.workspace_messages,
                     workspace_zulip_bridge.workspace_message_flags,
                     workspace_zulip_bridge.workspace_message_reactions,
                     workspace_zulip_bridge.workspace_mirror_state,
                     workspace_zulip_bridge.workspace_events,
                     workspace_zulip_bridge.workspace_event_cursors,
                     workspace_zulip_bridge.zulip_realms CASCADE
            """
        )
    return pool


def test_normalized_entity_tables_start_empty() -> None:
    asyncio.run(_normalized_tables_round_trip(_dsn()))


def test_concurrent_message_flag_events_merge_independent_fields() -> None:
    asyncio.run(_concurrent_message_flag_events_merge_independent_fields(_dsn()))


def test_prepare_database_upgrades_legacy_workspace_events() -> None:
    asyncio.run(_prepare_database_upgrades_legacy_workspace_events(_dsn()))


async def _prepare_database_upgrades_legacy_workspace_events(dsn: str) -> None:
    settings = Settings.from_env(
        {
            "WZB_DATABASE_DSN": dsn,
            "WZB_DB_POOL_MIN_SIZE": "1",
            "WZB_DB_POOL_MAX_SIZE": "4",
            "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
        }
    )
    pool = await open_pool(settings)
    event_uuid = UUID("10000000-0000-0000-0000-000000000001")
    provider_uuid = UUID("20000000-0000-0000-0000-000000000001")
    project_uuid = UUID("30000000-0000-0000-0000-000000000001")
    try:
        async with pool.acquire() as connection:
            await connection.execute(
                "DROP SCHEMA IF EXISTS workspace_zulip_bridge CASCADE"
            )
            await connection.execute(
                """
                CREATE SCHEMA workspace_zulip_bridge;
                CREATE TABLE workspace_zulip_bridge.workspace_events (
                    sequence bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    uuid uuid NOT NULL UNIQUE,
                    provider_uuid uuid NOT NULL,
                    workspace_project_id uuid NOT NULL,
                    epoch_generation uuid,
                    epoch_version bigint NOT NULL,
                    object_type text NOT NULL,
                    action text NOT NULL,
                    entity_uuid uuid,
                    payload jsonb NOT NULL,
                    processing_status text NOT NULL DEFAULT 'pending',
                    attempt_count integer NOT NULL DEFAULT 0,
                    claimed_at timestamptz,
                    processed_at timestamptz,
                    last_error text,
                    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
                );
                CREATE INDEX workspace_events_pending_idx
                    ON workspace_zulip_bridge.workspace_events (sequence)
                    WHERE processing_status = 'pending';
                """
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_events (
                    uuid, provider_uuid, workspace_project_id, epoch_version,
                    object_type, action, payload
                ) VALUES ($1, $2, $3, 1, 'message', 'upsert', '{}'::jsonb)
                """,
                event_uuid,
                provider_uuid,
                project_uuid,
            )

        await prepare_database(pool)

        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT uuid, available_at
                FROM workspace_zulip_bridge.workspace_events
                WHERE uuid = $1
                """,
                event_uuid,
            )
            index_definition = await connection.fetchval(
                """
                SELECT indexdef
                FROM pg_indexes
                WHERE schemaname = 'workspace_zulip_bridge'
                  AND indexname = 'workspace_events_pending_idx'
                """
            )
        assert row is not None
        assert row["uuid"] == event_uuid
        assert row["available_at"] is not None
        assert "available_at" in index_definition
    finally:
        await pool.close()


async def _normalized_tables_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        tables = (
            "zulip_realms",
            "zulip_users",
            "zulip_connections",
            "zulip_streams",
            "zulip_stream_bindings",
            "zulip_topics",
            "zulip_message_flags",
            "zulip_message_reactions",
            "zulip_files",
            "zulip_message_files",
            "workspace_outbox",
            "workspace_event_cursors",
            "workspace_events",
        )
        async with pool.acquire() as connection:
            for table in tables:
                assert (
                    await connection.fetchval(
                        "SELECT count(*) FROM workspace_zulip_bridge." + table
                    )
                    == 0
                )
    finally:
        await pool.close()


def test_outbound_message_adopts_a_racing_zulip_echo() -> None:
    asyncio.run(_outbound_message_adopts_a_racing_zulip_echo(_dsn()))


async def _outbound_message_adopts_a_racing_zulip_echo(dsn: str) -> None:
    pool = await _pool(dsn)
    realm_uuid = stable_realm_uuid(ENDPOINT)
    message_id = 987654
    canonical_uuid = UUID("10000000-0000-0000-0000-000000000061")
    stream_uuid = stable_chat_uuid(ENDPOINT, "channel:61")
    topic_uuid = stable_topic_uuid(stream_uuid, "General")
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(
                connection,
                61,
                400,
                queue_id="queue-61",
                status="active",
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', 'channel:61', 'Race', $3, $4)
                """,
                stream_uuid,
                realm_uuid,
                b"s" * 32,
                user_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'General', $3)
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    topic_uuid, sender_user_uuid, zulip_message_id, content,
                    content_hash, message_hash, created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, $3, $6, 'echo', $7, $8,
                          clock_timestamp(), clock_timestamp())
                """,
                stable_message_uuid(ENDPOINT, message_id),
                realm_uuid,
                user_uuid,
                stream_uuid,
                topic_uuid,
                message_id,
                b"c" * 32,
                b"m" * 32,
            )

        calls = {"send": 0}

        def send_message(*_args, **_kwargs) -> int:
            calls["send"] += 1
            return message_id

        client = SimpleNamespace(
            send_message=send_message,
            update_message_flag=lambda *_args, **_kwargs: None,
        )
        actor = SimpleNamespace(
            realm_uuid=realm_uuid,
            user_uuid=user_uuid,
            zulip_user_id=61,
            queue_id="queue-61",
        )
        writer = ZulipOutboundWriter(pool, Settings(database_dsn=dsn))

        async def required_stream(_stream_uuid: UUID):
            return {
                "chat_key": "channel:61",
                "source_connection_uuid": user_uuid,
            }

        async def resolve_actor(_author_uuid: UUID):
            return actor

        async def ensure_topic(_topic_uuid: UUID, _stream_uuid: UUID):
            return {"name": "General"}

        writer._required_stream = required_stream  # type: ignore[method-assign]
        writer._actor = resolve_actor  # type: ignore[method-assign]
        writer._ensure_topic = ensure_topic  # type: ignore[method-assign]
        writer._client = lambda _actor: client  # type: ignore[method-assign]
        target = {
            "stream_uuid": str(stream_uuid),
            "author_uuid": str(user_uuid),
            "topic_uuid": str(topic_uuid),
            "payload": {"kind": "markdown", "content": "canonical"},
            "created_at": "2026-09-21T01:00:00Z",
        }

        await writer._create_message(canonical_uuid, target, None)
        await writer._create_message(canonical_uuid, target, None)

        rows = await pool.fetch(
            """
            SELECT uuid, content FROM workspace_zulip_bridge.zulip_messages
            WHERE realm_uuid = $1 AND zulip_message_id = $2
            """,
            realm_uuid,
            message_id,
        )
        linked_uuid = await pool.fetchval(
            """
            SELECT workspace_uuid
            FROM workspace_zulip_bridge.zulip_entity_links
            WHERE realm_uuid = $1 AND entity_type = 'message'
              AND zulip_external_key = $2
            """,
            realm_uuid,
            str(message_id),
        )
        assert [(row["uuid"], row["content"]) for row in rows] == [
            (canonical_uuid, "canonical")
        ]
        assert linked_uuid == canonical_uuid
        assert calls["send"] == 1

        uncertain_uuid = UUID("10000000-0000-0000-0000-000000000062")

        def uncertain_send(*_args, **_kwargs) -> int:
            calls["send"] += 1
            raise RuntimeError("connection closed after remote acceptance")

        client.send_message = uncertain_send
        with pytest.raises(RuntimeError, match="remote acceptance"):
            await writer._create_message(uncertain_uuid, target, None)
        with pytest.raises(ZulipOutboundPending, match="local-echo receipt"):
            await writer._create_message(uncertain_uuid, target, None)
        assert calls["send"] == 2

        assert await EventStore(pool).link_local_message(
            user_uuid,
            "queue-61",
            uncertain_uuid,
            987655,
        )
        client.send_message = send_message
        await writer._create_message(uncertain_uuid, target, None)
        assert calls["send"] == 2

        retryable_uuid = UUID("10000000-0000-0000-0000-000000000065")

        def retryable_send(*_args, **_kwargs) -> int:
            calls["send"] += 1
            raise ZulipApiError("TEMPORARY_FAILURE", retryable=True)

        client.send_message = retryable_send
        with pytest.raises(ZulipApiError, match="TEMPORARY_FAILURE"):
            await writer._create_message(retryable_uuid, target, None)
        with pytest.raises(ZulipOutboundPending, match="local-echo receipt"):
            await writer._create_message(retryable_uuid, target, None)
        assert await pool.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.zulip_entity_links
                WHERE realm_uuid = $1 AND entity_type = 'message'
                  AND workspace_uuid = $2
                  AND zulip_external_key = $3
            )
            """,
            realm_uuid,
            retryable_uuid,
            f"pending:{retryable_uuid}",
        )

        rate_limited_uuid = UUID("10000000-0000-0000-0000-000000000066")

        def rate_limited_send(*_args, **_kwargs) -> int:
            calls["send"] += 1
            raise ZulipApiError(
                "RATE_LIMIT_HIT",
                retryable=True,
                status_code=429,
            )

        client.send_message = rate_limited_send
        with pytest.raises(ZulipApiError, match="RATE_LIMIT_HIT"):
            await writer._create_message(rate_limited_uuid, target, None)
        assert not await pool.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.zulip_entity_links
                WHERE realm_uuid = $1 AND entity_type = 'message'
                  AND workspace_uuid = $2
            )
            """,
            realm_uuid,
            rate_limited_uuid,
        )

        rejected_uuid = UUID("10000000-0000-0000-0000-000000000063")

        def rejected_send(*_args, **_kwargs) -> int:
            calls["send"] += 1
            raise ZulipApiError("BAD_REQUEST", retryable=False)

        client.send_message = rejected_send
        with pytest.raises(ZulipApiError, match="BAD_REQUEST"):
            await writer._create_message(rejected_uuid, target, None)
        assert not await pool.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.zulip_entity_links
                WHERE realm_uuid = $1 AND entity_type = 'message'
                  AND workspace_uuid = $2
            )
            """,
            realm_uuid,
            rejected_uuid,
        )

        def recovered_send(*_args, **_kwargs) -> int:
            calls["send"] += 1
            return 987656

        client.send_message = recovered_send
        await writer._create_message(rejected_uuid, target, None)
        assert (
            await pool.fetchval(
                """
            SELECT zulip_external_key
            FROM workspace_zulip_bridge.zulip_entity_links
            WHERE realm_uuid = $1 AND entity_type = 'message'
              AND workspace_uuid = $2
            """,
                realm_uuid,
                rejected_uuid,
            )
            == "987656"
        )

        stale_uuid = UUID("10000000-0000-0000-0000-000000000064")
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_entity_links (
                realm_uuid, entity_type, workspace_uuid,
                zulip_external_key, updated_at
            ) VALUES ($1, 'message', $2, $3, clock_timestamp() - interval '2 minutes')
            """,
            realm_uuid,
            stale_uuid,
            f"pending:{stale_uuid}",
        )

        def stale_retry_send(*_args, **_kwargs) -> int:
            calls["send"] += 1
            return 987657

        client.send_message = stale_retry_send
        send_count = calls["send"]
        with pytest.raises(
            ZulipOutboundError,
            match="confirmation timed out; manual reconciliation is required",
        ):
            await writer._create_message(stale_uuid, target, None)
        assert (
            await pool.fetchval(
                """
            SELECT zulip_external_key
            FROM workspace_zulip_bridge.zulip_entity_links
            WHERE realm_uuid = $1 AND entity_type = 'message'
              AND workspace_uuid = $2
            """,
                realm_uuid,
                stale_uuid,
            )
            == f"pending:{stale_uuid}"
        )
        assert calls["send"] == send_count
    finally:
        await pool.close()


def test_outbound_user_scoped_write_requires_exact_actor() -> None:
    asyncio.run(_outbound_user_scoped_write_requires_exact_actor(_dsn()))


def test_outbound_message_flags_adopt_workspace_identity() -> None:
    asyncio.run(_outbound_message_flags_adopt_workspace_identity(_dsn()))


async def _outbound_message_flags_adopt_workspace_identity(dsn: str) -> None:
    pool = await _pool(dsn)
    realm_uuid = stable_realm_uuid(ENDPOINT)
    stream_uuid = stable_chat_uuid(ENDPOINT, "channel:64")
    message_uuid = stable_message_uuid(ENDPOINT, 6400)
    workspace_flag_uuid = UUID("10000000-0000-0000-0000-000000000064")
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 64, 400)
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', 'channel:64', 'Flags', $3, $4)
                """,
                stream_uuid,
                realm_uuid,
                b"s" * 32,
                user_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                    uuid, zulip_stream_uuid, zulip_user_uuid, role,
                    membership_kind, content_hash
                ) VALUES ($1, $2, $3, 'member', 'subscriber', $4)
                """,
                stable_stream_binding_uuid(stream_uuid, user_uuid),
                stream_uuid,
                user_uuid,
                b"b" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    sender_user_uuid, zulip_message_id, content, content_hash,
                    message_hash, created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $3, 6400, 'flags', $5, $6,
                          clock_timestamp(), clock_timestamp())
                """,
                message_uuid,
                realm_uuid,
                user_uuid,
                stream_uuid,
                b"c" * 32,
                b"m" * 32,
            )
            existing = {
                "is_read": False,
                "is_starred": False,
                "is_collapsed": True,
                "is_mentioned": True,
                "is_stream_wildcard_mentioned": True,
                "is_topic_wildcard_mentioned": False,
                "has_alert_word": True,
                "is_historical": True,
            }
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_message_flags (
                    uuid, realm_uuid, zulip_stream_uuid, message_uuid,
                    zulip_user_uuid, is_read, is_starred, is_collapsed,
                    is_mentioned, is_stream_wildcard_mentioned,
                    is_topic_wildcard_mentioned, has_alert_word, is_historical,
                    flags_hash
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                          $11, $12, $13, $14)
                """,
                stable_message_flag_uuid(message_uuid, user_uuid),
                realm_uuid,
                stream_uuid,
                message_uuid,
                user_uuid,
                *existing.values(),
                message_flags_hash(**existing),
            )

        calls: list[tuple[int, str, bool]] = []
        fail_starred_once = True

        def update_message_flag(message_id: int, flag: str, enabled: bool) -> None:
            nonlocal fail_starred_once
            calls.append((message_id, flag, enabled))
            if flag == "starred" and fail_starred_once:
                fail_starred_once = False
                raise RuntimeError("simulated rate limit between flag updates")

        actor = SimpleNamespace(realm_uuid=realm_uuid, user_uuid=user_uuid)
        writer = ZulipOutboundWriter(pool, Settings(database_dsn=dsn))
        writer._message = AsyncMock(return_value={"zulip_message_id": 6400})  # type: ignore[method-assign]
        writer._actor = AsyncMock(return_value=actor)  # type: ignore[method-assign]
        writer._client = lambda _actor: SimpleNamespace(  # type: ignore[method-assign]
            update_message_flag=update_message_flag
        )
        target = {
            "stream_uuid": str(stream_uuid),
            "message_uuid": str(message_uuid),
            "user_uuid": str(user_uuid),
            "read": True,
            "starred": True,
            "pinned": False,
            # Workspace may have a newer user-owned flag while its projection
            # still lacks Zulip's computed mention.  The writer must apply only
            # read/starred and preserve the provider-owned mention.
            "mentioned": False,
        }

        with pytest.raises(RuntimeError, match="simulated rate limit"):
            await writer._apply_message_flags(
                workspace_flag_uuid,
                None,
                target,
                None,
            )

        partial = await pool.fetchrow(
            """
            SELECT uuid, is_read, is_starred
            FROM workspace_zulip_bridge.zulip_message_flags
            WHERE message_uuid = $1 AND zulip_user_uuid = $2
            """,
            message_uuid,
            user_uuid,
        )
        assert partial is not None
        assert partial["uuid"] == workspace_flag_uuid
        assert partial["is_read"]
        assert not partial["is_starred"]

        await writer._apply_message_flags(
            workspace_flag_uuid,
            None,
            target,
            None,
        )

        row = await pool.fetchrow(
            """
            SELECT uuid, is_read, is_starred, is_collapsed, is_mentioned,
                   is_stream_wildcard_mentioned, is_topic_wildcard_mentioned,
                   has_alert_word, is_historical, flags_hash
            FROM workspace_zulip_bridge.zulip_message_flags
            WHERE message_uuid = $1 AND zulip_user_uuid = $2
            """,
            message_uuid,
            user_uuid,
        )
        assert row is not None
        assert row["uuid"] == workspace_flag_uuid
        assert row["is_read"]
        assert row["is_starred"]
        assert row["is_collapsed"]
        assert row["is_mentioned"]
        assert row["is_stream_wildcard_mentioned"]
        assert not row["is_topic_wildcard_mentioned"]
        assert row["has_alert_word"]
        assert row["is_historical"]
        actual = {
            name: bool(row[name])
            for name in (
                "is_read",
                "is_starred",
                "is_collapsed",
                "is_mentioned",
                "is_stream_wildcard_mentioned",
                "is_topic_wildcard_mentioned",
                "has_alert_word",
                "is_historical",
            )
        }
        assert row["flags_hash"] == message_flags_hash(**actual)
        assert calls == [
            (6400, "read", True),
            (6400, "starred", True),
            (6400, "starred", True),
        ]

        await writer._apply_message_flags(
            workspace_flag_uuid,
            None,
            target,
            None,
        )
        assert calls == [
            (6400, "read", True),
            (6400, "starred", True),
            (6400, "starred", True),
        ]

        await writer._apply_message_flags(
            workspace_flag_uuid,
            {**target, "mentioned": True},
            None,
            None,
        )
        preserved = await pool.fetchrow(
            """
            SELECT is_read, is_starred, is_mentioned
            FROM workspace_zulip_bridge.zulip_message_flags
            WHERE message_uuid = $1 AND zulip_user_uuid = $2
            """,
            message_uuid,
            user_uuid,
        )
        assert preserved is not None
        assert not preserved["is_read"]
        assert not preserved["is_starred"]
        assert preserved["is_mentioned"]
        assert calls[-2:] == [
            (6400, "read", False),
            (6400, "starred", False),
        ]
        assert calls == [
            (6400, "read", True),
            (6400, "starred", True),
            (6400, "starred", True),
            (6400, "read", False),
            (6400, "starred", False),
        ]
    finally:
        await pool.close()


def test_outbound_rejects_stream_properties_it_cannot_apply() -> None:
    asyncio.run(_outbound_rejects_stream_properties_it_cannot_apply())


async def _outbound_rejects_stream_properties_it_cannot_apply() -> None:
    writer = ZulipOutboundWriter.__new__(ZulipOutboundWriter)

    async def required_stream(_stream_uuid: UUID):
        return {"chat_key": "channel:7"}

    writer._required_stream = required_stream  # type: ignore[method-assign]
    source = {
        "name": "General",
        "owner_uuid": "10000000-0000-0000-0000-000000000001",
        "invite_only": False,
        "announce": False,
        "color": 1,
    }
    for property_name, value in (
        ("owner_uuid", "10000000-0000-0000-0000-000000000003"),
        ("invite_only", True),
        ("announce", True),
        ("direct_user_uuid", "10000000-0000-0000-0000-000000000004"),
        ("private", True),
        ("color", 2),
        ("history_public_to_subscribers", False),
    ):
        with pytest.raises(ZulipOutboundError, match=property_name):
            await writer._apply_streams(
                UUID("10000000-0000-0000-0000-000000000002"),
                source,
                {**source, property_name: value},
                None,
            )


def test_outbound_rejects_workspace_stream_deletion() -> None:
    asyncio.run(_outbound_rejects_workspace_stream_deletion())


async def _outbound_rejects_workspace_stream_deletion() -> None:
    writer = ZulipOutboundWriter.__new__(ZulipOutboundWriter)
    with pytest.raises(ZulipOutboundError, match="deletion is not supported"):
        await writer._apply_streams(
            UUID("10000000-0000-0000-0000-000000000006"),
            {
                "name": "General",
                "owner_uuid": "10000000-0000-0000-0000-000000000001",
            },
            None,
            None,
        )


def test_outbound_rejects_direct_chat_and_cross_chat_mutations() -> None:
    asyncio.run(_outbound_rejects_direct_chat_and_cross_chat_mutations())


async def _outbound_rejects_direct_chat_and_cross_chat_mutations() -> None:
    writer = ZulipOutboundWriter.__new__(ZulipOutboundWriter)

    async def required_stream(_stream_uuid: UUID):
        return {"chat_key": "direct:1,2"}

    writer._required_stream = required_stream  # type: ignore[method-assign]
    stream_uuid = UUID("10000000-0000-0000-0000-000000000006")
    user_uuid = UUID("10000000-0000-0000-0000-000000000007")
    source_stream = {"name": "Direct", "owner_uuid": str(user_uuid)}
    with pytest.raises(ZulipOutboundError, match="direct-message stream"):
        await writer._apply_streams(
            stream_uuid,
            source_stream,
            {**source_stream, "name": "Renamed"},
            None,
        )

    binding = {
        "stream_uuid": str(stream_uuid),
        "user_uuid": str(user_uuid),
        "role": "member",
    }
    with pytest.raises(ZulipOutboundError, match="direct-message membership"):
        await writer._apply_stream_bindings(
            UUID("10000000-0000-0000-0000-000000000008"),
            binding,
            {**binding, "notification_mode": "muted"},
            None,
        )

    topic_binding = {
        "stream_uuid": str(stream_uuid),
        "topic_uuid": "10000000-0000-0000-0000-000000000015",
        "user_uuid": str(user_uuid),
        "notification_mode": "default",
    }
    with pytest.raises(ZulipOutboundError, match="topic preferences"):
        await writer._apply_topic_bindings(
            UUID("10000000-0000-0000-0000-000000000016"),
            topic_binding,
            {**topic_binding, "notification_mode": "mute"},
            None,
        )

    source_message = {
        "stream_uuid": str(stream_uuid),
        "author_uuid": str(user_uuid),
    }
    with pytest.raises(ZulipOutboundError, match="between Zulip conversations"):
        await writer._apply_messages(
            UUID("10000000-0000-0000-0000-000000000009"),
            source_message,
            {
                **source_message,
                "stream_uuid": "10000000-0000-0000-0000-000000000010",
            },
            None,
        )
    with pytest.raises(ZulipOutboundError, match="message authors"):
        await writer._apply_messages(
            UUID("10000000-0000-0000-0000-000000000009"),
            source_message,
            {
                **source_message,
                "author_uuid": "10000000-0000-0000-0000-000000000011",
            },
            None,
        )


def test_outbound_rejects_unrepresentable_channel_binding_changes() -> None:
    asyncio.run(_outbound_rejects_unrepresentable_channel_binding_changes())


async def _outbound_rejects_unrepresentable_channel_binding_changes() -> None:
    writer = ZulipOutboundWriter.__new__(ZulipOutboundWriter)

    async def required_stream(_stream_uuid: UUID):
        return {"chat_key": "channel:7", "name": "General"}

    writer._required_stream = required_stream  # type: ignore[method-assign]
    writer._actor = AsyncMock()  # type: ignore[method-assign]
    stream_uuid = UUID("10000000-0000-0000-0000-000000000011")
    user_uuid = UUID("10000000-0000-0000-0000-000000000012")
    source = {
        "stream_uuid": str(stream_uuid),
        "user_uuid": str(user_uuid),
        "role": "member",
        "notification_mode": "all_messages",
    }
    with pytest.raises(ZulipOutboundError, match="roles cannot be updated"):
        await writer._apply_stream_bindings(
            UUID("10000000-0000-0000-0000-000000000013"),
            source,
            {**source, "role": "administrator"},
            None,
        )
    with pytest.raises(ZulipOutboundError, match="mentions-only"):
        await writer._apply_stream_bindings(
            UUID("10000000-0000-0000-0000-000000000014"),
            source,
            {**source, "notification_mode": "mentions_only"},
            None,
        )


def test_outbound_rejects_unsupported_message_flag_changes() -> None:
    asyncio.run(_outbound_rejects_unsupported_message_flag_changes())


async def _outbound_rejects_unsupported_message_flag_changes() -> None:
    writer = ZulipOutboundWriter.__new__(ZulipOutboundWriter)
    writer._message = AsyncMock()  # type: ignore[method-assign]
    base = {
        "stream_uuid": "10000000-0000-0000-0000-000000000001",
        "message_uuid": "10000000-0000-0000-0000-000000000002",
        "user_uuid": "10000000-0000-0000-0000-000000000003",
        "read": True,
        "starred": False,
        "pinned": False,
        "mentioned": False,
    }
    with pytest.raises(ZulipOutboundError, match="pinned"):
        await writer._apply_message_flags(
            UUID("10000000-0000-0000-0000-000000000004"),
            base,
            {**base, "pinned": True},
            None,
        )
    writer._message.assert_not_awaited()


def test_outbound_maps_every_topic_notification_mode() -> None:
    asyncio.run(_outbound_maps_every_topic_notification_mode())


async def _outbound_maps_every_topic_notification_mode() -> None:
    writer = ZulipOutboundWriter.__new__(ZulipOutboundWriter)
    stream_uuid = UUID("10000000-0000-0000-0000-000000000003")
    topic_uuid = UUID("10000000-0000-0000-0000-000000000004")
    user_uuid = UUID("10000000-0000-0000-0000-000000000005")
    calls: list[tuple[int, str, int]] = []

    async def required_stream(_stream_uuid: UUID):
        return {"chat_key": "channel:7"}

    async def ensure_topic(_topic_uuid: UUID, _stream_uuid: UUID):
        return {"name": "General"}

    async def actor(_user_uuid: UUID):
        return SimpleNamespace(user_uuid=user_uuid)

    client = SimpleNamespace(
        update_topic_notification=lambda stream_id, topic, *, visibility_policy: (
            calls.append((stream_id, topic, visibility_policy))
        )
    )
    writer._pool = SimpleNamespace(execute=AsyncMock())
    writer._required_stream = required_stream  # type: ignore[method-assign]
    writer._ensure_topic = ensure_topic  # type: ignore[method-assign]
    writer._actor = actor  # type: ignore[method-assign]
    writer._client = lambda _actor: client  # type: ignore[method-assign]

    for mode, policy in (("default", 0), ("mute", 1), ("unmute", 2), ("follow", 3)):
        target = {
            "stream_uuid": str(stream_uuid),
            "topic_uuid": str(topic_uuid),
            "user_uuid": str(user_uuid),
            "notification_mode": mode,
            "created_at": "2026-09-21T08:00:00Z",
        }
        await writer._apply_topic_bindings(UUID(int=policy + 10), None, target, None)

    assert calls == [
        (7, "General", 0),
        (7, "General", 1),
        (7, "General", 2),
        (7, "General", 3),
    ]


async def _outbound_user_scoped_write_requires_exact_actor(dsn: str) -> None:
    pool = await _pool(dsn)
    stream_uuid = stable_chat_uuid(ENDPOINT, "channel:62")
    try:
        async with pool.acquire() as connection:
            supplier_connection_uuid = await _insert_user(connection, 62, 400)
            missing_user_uuid = await _insert_user(
                connection,
                63,
                400,
                api_key=None,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', 'channel:62', 'Exact actor', $3, $4)
                """,
                stream_uuid,
                stable_realm_uuid(ENDPOINT),
                b"s" * 32,
                supplier_connection_uuid,
            )
        writer = ZulipOutboundWriter(pool, Settings(database_dsn=dsn))

        with pytest.raises(ZulipOutboundError, match="credential is unavailable"):
            await writer._actor(missing_user_uuid)

        fallback = await writer._actor(missing_user_uuid, stream_uuid)
        assert fallback.connection_uuid == supplier_connection_uuid

        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_connections
            SET sync_enabled = false
            WHERE uuid = $1
            """,
            supplier_connection_uuid,
        )
        with pytest.raises(ZulipOutboundError, match="credential is unavailable"):
            await writer._actor(stable_user_uuid(ENDPOINT, 62))
        with pytest.raises(ZulipOutboundError, match="credential is unavailable"):
            await writer._actor(missing_user_uuid, stream_uuid)
    finally:
        await pool.close()


def test_user_status_partial_updates_and_presence_expiry() -> None:
    asyncio.run(_user_status_partial_updates_and_presence_expiry(_dsn()))


async def _user_status_partial_updates_and_presence_expiry(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 77, 400)
        store = EventStore(pool)
        await store.store_user_statuses(
            ENDPOINT,
            (ZulipUserProfileStatus(77, "Working", "hammer"),),
        )
        await store.store_user_statuses(
            ENDPOINT,
            (
                ZulipUserProfileStatus(
                    77,
                    "Reviewing",
                    None,
                    update_status_emoji=False,
                ),
            ),
        )
        row = await pool.fetchrow(
            "SELECT status_text, status_emoji FROM "
            "workspace_zulip_bridge.zulip_users WHERE uuid = $1",
            user_uuid,
        )
        assert tuple(row) == ("Reviewing", "hammer")

        assert (
            await store.store_user_presences(
                ENDPOINT,
                (ZulipUserPresence(77, "active", 1_700_000_100),),
            )
            == 1
        )
        assert (
            await store.store_user_presences(
                ENDPOINT,
                (ZulipUserPresence(77, "idle", 1_700_000_000),),
            )
            == 0
        )
        current_presence = await pool.fetchrow(
            "SELECT presence_status, extract(epoch FROM last_ping_at)::bigint "
            "FROM workspace_zulip_bridge.zulip_users WHERE uuid = $1",
            user_uuid,
        )
        assert tuple(current_presence) == ("active", 1_700_000_100)

        await store.set_presence_offline_threshold(ENDPOINT, 1)
        previous_hash = await pool.fetchval(
            "UPDATE workspace_zulip_bridge.zulip_users "
            "SET presence_status = 'active', "
            "last_ping_at = clock_timestamp() - interval '2 seconds' "
            "WHERE uuid = $1 RETURNING profile_hash",
            user_uuid,
        )
        assert await store.expire_user_presences(10) == 1
        expired = await pool.fetchrow(
            "SELECT presence_status, profile_hash FROM "
            "workspace_zulip_bridge.zulip_users WHERE uuid = $1",
            user_uuid,
        )
        assert expired["presence_status"] == "offline"
        assert expired["profile_hash"] != previous_hash
    finally:
        await pool.close()


def test_catalog_removal_enqueues_workspace_binding_tombstone() -> None:
    asyncio.run(_catalog_removal_enqueues_workspace_binding_tombstone(_dsn()))


def test_catalog_applies_registration_topic_snapshot_before_activation() -> None:
    asyncio.run(_catalog_applies_registration_topic_snapshot_before_activation(_dsn()))


async def _catalog_applies_registration_topic_snapshot_before_activation(
    dsn: str,
) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(
                connection,
                78,
                400,
                queue_id="queue-topic-snapshot",
                status="filling",
            )
        result = await store.store_chat_catalog(
            connection_uuid,
            "queue-topic-snapshot",
            _catalog(78, [(7, "Shared")], {"channel:7": 1}),
            bootstrap_user_topics=(ZulipUserTopic(7, "Race", 1, 1_700_000_000),),
        )
        assert result.activated
        assert result.bootstrap_topic_changes == 1
        snapshot = await pool.fetchrow(
            """
            SELECT binding.notification_mode, connection.lifecycle_status
            FROM workspace_zulip_bridge.zulip_topic_bindings AS binding
            JOIN workspace_zulip_bridge.zulip_connections AS connection
              ON connection.uuid = $1
            WHERE binding.zulip_user_uuid = connection.zulip_user_uuid
            """,
            connection_uuid,
        )
        assert tuple(snapshot) == ("mute", "scheduling")
        queued_types = await pool.fetch(
            """
            SELECT entity_type, count(*) AS count
            FROM workspace_zulip_bridge.workspace_outbox
            WHERE realm_uuid = $1 AND delivery_status = 'pending'
              AND entity_type IN ('topic', 'topic_binding')
            GROUP BY entity_type ORDER BY entity_type
            """,
            stable_realm_uuid(ENDPOINT),
        )
        assert [tuple(row.values()) for row in queued_types] == [
            ("topic", 1),
            ("topic_binding", 1),
        ]

        assert (
            await store.store_user_topics(
                connection_uuid,
                "queue-topic-snapshot",
                (ZulipUserTopic(7, "Race", 3, 1_700_000_001),),
                replace_all=False,
            )
            == 1
        )
        assert (
            await pool.fetchval(
                "SELECT notification_mode FROM "
                "workspace_zulip_bridge.zulip_topic_bindings"
            )
            == "follow"
        )
    finally:
        await pool.close()


async def _catalog_removal_enqueues_workspace_binding_tombstone(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000b1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000b2")
    generation = UUID("10000000-0000-0000-0000-0000000000b3")
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(
                connection,
                64,
                400,
                queue_id="queue-64",
                status="filling",
            )
        catalog = _catalog(64, [(64, "Removed")], {})
        assert (
            await store.store_chat_catalog(connection_uuid, "queue-64", catalog)
        ).activated
        user_uuid = stable_user_uuid(ENDPOINT, 64)
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:64")
        binding_uuid = stable_stream_binding_uuid(stream_uuid, user_uuid)
        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                stable_realm_uuid(ENDPOINT),
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_stream_bindings (
                    provider_uuid, snapshot_generation, uuid,
                    workspace_project_id, content_hash, source_updated_at, data
                ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{}'::jsonb)
                """,
                provider_uuid,
                generation,
                binding_uuid,
                project_uuid,
                b"b" * 32,
            )

        removed = await store.store_chat_catalog(
            connection_uuid,
            "queue-64",
            _catalog(64, [], {}),
        )
        assert removed.deleted == 1
        row = await pool.fetchrow(
            """
            SELECT direction, source_hash, target_hash, partition_key,
                   processing_status
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'stream_bindings'
              AND entity_uuid = $2
            """,
            provider_uuid,
            binding_uuid,
        )
        assert row is not None
        assert dict(row) == {
            "direction": "to_workspace",
            "source_hash": None,
            "target_hash": b"b" * 32,
            "partition_key": stream_uuid,
            "processing_status": "pending",
        }
    finally:
        await pool.close()


def test_reaction_removal_enqueues_workspace_tombstone() -> None:
    asyncio.run(_reaction_removal_enqueues_workspace_tombstone(_dsn()))


def test_message_move_rehomes_personal_flags() -> None:
    asyncio.run(_message_move_rehomes_personal_flags(_dsn()))


async def _message_move_rehomes_personal_flags(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(
                connection,
                66,
                400,
                queue_id="queue-66",
                status="filling",
            )
        catalog = _catalog(
            66,
            [(66, "Before move"), (67, "After move")],
            {"channel:66": 1, "channel:67": 1},
        )
        assert (
            await store.store_chat_catalog(connection_uuid, "queue-66", catalog)
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 2
        user_uuid = stable_user_uuid(ENDPOINT, 66)
        message_uuid = stable_message_uuid(ENDPOINT, 660)
        flag_uuid = stable_message_flag_uuid(message_uuid, user_uuid)
        first_stream_uuid = stable_chat_uuid(ENDPOINT, "channel:66")
        second_stream_uuid = stable_chat_uuid(ENDPOINT, "channel:67")
        message = ZulipMessage(
            message_id=660,
            chat_key="channel:66",
            topic_name="General",
            sender_user_uuid=user_uuid,
            content="before move",
            is_read=True,
            is_starred=True,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json="[]",
            content_hash=b"a" * 32,
            message_hash=b"b" * 32,
            sent_at=1_700_000_000,
        )
        history = await store.begin_history(connection_uuid, "queue-66")
        try:
            assert (await history.store_page([message])).flags_changed == 1
        finally:
            await history.close()
        assert (
            await pool.fetchval(
                "SELECT zulip_stream_uuid FROM "
                "workspace_zulip_bridge.zulip_message_flags WHERE uuid = $1",
                flag_uuid,
            )
            == first_stream_uuid
        )

        moved = replace(
            message,
            chat_key="channel:67",
            content_hash=b"c" * 32,
            message_hash=b"d" * 32,
            source_updated_at=1_700_000_001,
        )
        history = await store.begin_history(connection_uuid, "queue-66")
        try:
            write = await history.store_page([moved])
            assert (write.changed, write.flags_changed) == (1, 1)
        finally:
            await history.close()
        row = await pool.fetchrow(
            """
            SELECT message.zulip_stream_uuid AS message_stream_uuid,
                   flag.zulip_stream_uuid AS flag_stream_uuid
            FROM workspace_zulip_bridge.zulip_messages AS message
            JOIN workspace_zulip_bridge.zulip_message_flags AS flag
              ON flag.message_uuid = message.uuid
            WHERE message.uuid = $1
            """,
            message_uuid,
        )
        assert row is not None
        assert tuple(row) == (second_stream_uuid, second_stream_uuid)
    finally:
        await pool.close()


def test_projection_upgrade_repairs_moved_message_flags(tmp_path: Path) -> None:
    asyncio.run(_projection_upgrade_repairs_moved_message_flags(_dsn(), tmp_path))


async def _projection_upgrade_repairs_moved_message_flags(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000d1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000d2")
    generation = UUID("10000000-0000-0000-0000-0000000000d3")
    old_stream_uuid = UUID("10000000-0000-0000-0000-0000000000d4")
    new_stream_uuid = UUID("10000000-0000-0000-0000-0000000000d5")
    message_uuid = UUID("10000000-0000-0000-0000-0000000000d6")
    token_file = tmp_path / "moved-message-flags.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            owner_connection_uuid = await _insert_user(connection, 68, 400)
            guest_connection_uuid = await _insert_user(connection, 69, 400)
            owner_uuid = stable_user_uuid(ENDPOINT, 68)
            guest_uuid = stable_user_uuid(ENDPOINT, 69)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status, reconciliation_version
                ) VALUES ($1, $2, $3, 'ready', 17)
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    content_hash, source_connection_uuid
                ) VALUES
                    ($1, $3, 'channel', 'channel:68', 'Before', $4, $5),
                    ($2, $3, 'channel', 'channel:69', 'After', $4, $5)
                """,
                old_stream_uuid,
                new_stream_uuid,
                realm_uuid,
                b"s" * 32,
                owner_connection_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                    uuid, zulip_stream_uuid, zulip_user_uuid, role,
                    membership_kind, notification_mode, content_hash
                ) VALUES
                    ($1, $3, $5, 'member', 'subscriber', 'all_messages', $7),
                    ($2, $3, $6, 'member', 'subscriber', 'all_messages', $7),
                    ($4, $8, $5, 'member', 'subscriber', 'all_messages', $7)
                """,
                stable_stream_binding_uuid(old_stream_uuid, owner_uuid),
                stable_stream_binding_uuid(old_stream_uuid, guest_uuid),
                old_stream_uuid,
                stable_stream_binding_uuid(new_stream_uuid, owner_uuid),
                owner_uuid,
                guest_uuid,
                b"b" * 32,
                new_stream_uuid,
            )
            topic_uuid = stable_topic_uuid(new_stream_uuid, "General")
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'General', $3)
                """,
                topic_uuid,
                new_stream_uuid,
                b"t" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    topic_uuid, sender_user_uuid, zulip_message_id, content,
                    content_hash, message_hash, created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, 680, 'moved', $7, $8,
                          clock_timestamp(), clock_timestamp())
                """,
                message_uuid,
                realm_uuid,
                owner_connection_uuid,
                new_stream_uuid,
                topic_uuid,
                owner_uuid,
                b"m" * 32,
                b"n" * 32,
            )
            owner_flag_uuid = stable_message_flag_uuid(message_uuid, owner_uuid)
            guest_flag_uuid = stable_message_flag_uuid(message_uuid, guest_uuid)
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_message_flags (
                    uuid, realm_uuid, zulip_stream_uuid, message_uuid,
                    zulip_user_uuid, is_read, flags_hash
                ) VALUES
                    ($1, $3, $4, $5, $6, true, $8),
                    ($2, $3, $4, $5, $7, true, $8)
                """,
                owner_flag_uuid,
                guest_flag_uuid,
                realm_uuid,
                old_stream_uuid,
                message_uuid,
                owner_uuid,
                guest_uuid,
                b"f" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    partition_key, direction, processing_status,
                    source_hash, source_updated_at, attempt_count, last_error
                ) VALUES
                    ($1, 'message_flags', $2, $4, $5, 'to_workspace',
                     'failed', $6, clock_timestamp(), 3, 'error=unknown'),
                    ($1, 'message_flags', $3, $4, $5, 'to_workspace',
                     'failed', $6, clock_timestamp(), 3, 'error=unknown')
                """,
                provider_uuid,
                owner_flag_uuid,
                guest_flag_uuid,
                realm_uuid,
                old_stream_uuid,
                b"f" * 32,
            )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)
        assert await worker._repair_moved_message_flags(realm_uuid, generation) == 2
        flags = await pool.fetch(
            """
            SELECT uuid, zulip_stream_uuid
            FROM workspace_zulip_bridge.zulip_message_flags
            ORDER BY uuid
            """
        )
        assert [(row["uuid"], row["zulip_stream_uuid"]) for row in flags] == [
            (owner_flag_uuid, new_stream_uuid)
        ]
        diffs = await pool.fetch(
            """
            SELECT entity_uuid, partition_key, source_hash,
                   processing_status, attempt_count, last_error
            FROM workspace_zulip_bridge.sync_diffs
            ORDER BY entity_uuid
            """
        )
        by_uuid = {row["entity_uuid"]: row for row in diffs}
        assert by_uuid[owner_flag_uuid]["partition_key"] == new_stream_uuid
        assert by_uuid[owner_flag_uuid]["source_hash"] == b"f" * 32
        assert by_uuid[guest_flag_uuid]["partition_key"] == new_stream_uuid
        assert by_uuid[guest_flag_uuid]["source_hash"] is None
        for row in by_uuid.values():
            assert row["processing_status"] == "pending"
            assert row["attempt_count"] == 0
            assert row["last_error"] == "requeued_moved_message_flag"
        assert guest_connection_uuid != owner_connection_uuid
    finally:
        await pool.close()


async def _reaction_removal_enqueues_workspace_tombstone(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000c1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000c2")
    generation = UUID("10000000-0000-0000-0000-0000000000c3")
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(
                connection,
                65,
                400,
                queue_id="queue-65",
                status="filling",
            )
        catalog = _catalog(65, [(65, "Reactions")], {"channel:65": 1})
        assert (
            await store.store_chat_catalog(connection_uuid, "queue-65", catalog)
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:65")
        message_uuid = stable_message_uuid(ENDPOINT, 650)
        user_uuid = stable_user_uuid(ENDPOINT, 65)
        reaction_uuid = stable_reaction_uuid(
            message_uuid,
            user_uuid,
            "unicode_emoji",
            "1f44d",
        )
        flag_uuid = stable_message_flag_uuid(message_uuid, user_uuid)
        topic_uuid = stable_topic_uuid(stream_uuid, "General")
        message = ZulipMessage(
            message_id=650,
            chat_key="channel:65",
            topic_name="General",
            sender_user_uuid=user_uuid,
            content="reaction",
            is_read=True,
            is_starred=False,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json=json.dumps(
                [
                    {
                        "user_uuid": str(user_uuid),
                        "emoji_name": "thumbs_up",
                        "emoji_code": "1f44d",
                        "reaction_type": "unicode_emoji",
                    }
                ]
            ),
            message_hash=b"a" * 32,
            sent_at=1_700_000_000,
        )
        history = await store.begin_history(connection_uuid, "queue-65")
        try:
            assert (await history.store_page([message])).reactions_changed == 1
        finally:
            await history.close()
        outbox_types = await pool.fetch(
            """
            SELECT entity_type
            FROM workspace_zulip_bridge.workspace_outbox
            WHERE realm_uuid = $1
              AND entity_uuid = ANY($2::uuid[])
            ORDER BY entity_type
            """,
            stable_realm_uuid(ENDPOINT),
            [message_uuid, flag_uuid, reaction_uuid, topic_uuid],
        )
        assert [row["entity_type"] for row in outbox_types] == [
            "message",
            "message_flag",
            "message_reaction",
            "topic",
        ]
        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                stable_realm_uuid(ENDPOINT),
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_message_reactions (
                    provider_uuid, snapshot_generation, uuid,
                    workspace_project_id, content_hash, source_updated_at, data
                ) VALUES (
                    $1, $2, $3, $4, $5, clock_timestamp(), '{}'::jsonb
                )
                """,
                provider_uuid,
                generation,
                reaction_uuid,
                project_uuid,
                b"r" * 32,
            )
        without_reaction = replace(
            message,
            reactions_json="[]",
            message_hash=b"z" * 32,
            source_updated_at=1_700_000_001,
        )
        history = await store.begin_history(connection_uuid, "queue-65")
        try:
            await history.store_page([without_reaction])
        finally:
            await history.close()

        assert not await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM "
            "workspace_zulip_bridge.zulip_message_reactions WHERE uuid = $1)",
            reaction_uuid,
        )
        row = await pool.fetchrow(
            """
            SELECT direction, source_hash, target_hash, partition_key,
                   processing_status
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'message_reactions'
              AND entity_uuid = $2
            """,
            provider_uuid,
            reaction_uuid,
        )
        assert row is not None
        assert dict(row) == {
            "direction": "to_workspace",
            "source_hash": None,
            "target_hash": b"r" * 32,
            "partition_key": stream_uuid,
            "processing_status": "pending",
        }
    finally:
        await pool.close()


def test_history_rescan_enqueues_message_and_topic_tombstones() -> None:
    asyncio.run(_history_rescan_enqueues_tombstones(_dsn()))


def test_unchanged_catalog_rows_do_not_wait_on_stream_locks() -> None:
    asyncio.run(_unchanged_catalog_rows_do_not_wait_on_stream_locks(_dsn()))


async def _unchanged_catalog_rows_do_not_wait_on_stream_locks(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            first_connection = await _insert_user(
                connection,
                10,
                400,
                queue_id="queue-catalog-first",
                status="filling",
            )
        first_catalog = _catalog(10, [(7, "Shared")], {"channel:7": 1})
        expanded_catalog = _catalog(
            10,
            [(7, "Shared"), (8, "New")],
            {"channel:7": 1, "channel:8": 1},
        )
        assert (
            await store.store_chat_catalog(
                first_connection,
                "queue-catalog-first",
                first_catalog,
            )
        ).activated
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:7")

        async with pool.acquire() as locked, locked.transaction():
            await locked.fetchval(
                """
                SELECT uuid
                FROM workspace_zulip_bridge.zulip_streams
                WHERE uuid = $1
                FOR UPDATE
                """,
                stream_uuid,
            )
            result = await asyncio.wait_for(
                store.store_chat_catalog(
                    first_connection,
                    "queue-catalog-first",
                    expanded_catalog,
                ),
                timeout=1.0,
            )
            assert result.activated
            assert result.upserted == 1
    finally:
        await pool.close()


async def _history_rescan_enqueues_tombstones(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000e1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000e2")
    generation = UUID("10000000-0000-0000-0000-0000000000e3")
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(
                connection,
                66,
                400,
                queue_id="queue-66",
                status="filling",
            )
        assert (
            await store.store_chat_catalog(
                connection_uuid,
                "queue-66",
                _catalog(66, [(66, "History")], {"channel:66": 1}),
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:66")
        topic_uuid = stable_topic_uuid(stream_uuid, "General")
        message_uuid = stable_message_uuid(ENDPOINT, 660)
        message = ZulipMessage(
            message_id=660,
            chat_key="channel:66",
            topic_name="General",
            sender_user_uuid=stable_user_uuid(ENDPOINT, 66),
            content="removed upstream",
            is_read=True,
            is_starred=False,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json="[]",
            message_hash=b"m" * 32,
            sent_at=1_700_000_000,
        )
        history = await store.begin_history(connection_uuid, "queue-66")
        try:
            assert (await history.store_page([message])).changed == 1
        finally:
            await history.close()
        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                stable_realm_uuid(ENDPOINT),
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            for table, entity_uuid, content_hash in (
                ("workspace_messages", message_uuid, b"m" * 32),
                ("workspace_topics", topic_uuid, b"t" * 32),
            ):
                await connection.execute(
                    f"""
                    INSERT INTO workspace_zulip_bridge.{table} (
                        provider_uuid, snapshot_generation, uuid,
                        workspace_project_id, content_hash,
                        source_updated_at, data
                    ) VALUES ($1, $2, $3, $4, $5,
                              clock_timestamp(), '{{}}'::jsonb)
                    """,
                    provider_uuid,
                    generation,
                    entity_uuid,
                    project_uuid,
                    content_hash,
                )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET last_event_cursor_at = clock_timestamp() - interval '2 hours'
                WHERE uuid = $1
                """,
                connection_uuid,
            )

        assert await store.clear_queue(connection_uuid, "queue-66", 3600.0)
        reconcile_age = await pool.fetchval(
            """
            SELECT EXTRACT(EPOCH FROM (clock_timestamp() - reconcile_since))
            FROM workspace_zulip_bridge.zulip_connections
            WHERE uuid = $1
            """,
            connection_uuid,
        )
        assert 3590 <= float(reconcile_age) <= 3610
        assert await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM "
            "workspace_zulip_bridge.zulip_messages WHERE uuid = $1)",
            message_uuid,
        )
        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET queue_id = 'queue-66-rescan', last_event_id = 0,
                    lifecycle_status = 'filling'
                WHERE uuid = $1
                """,
                connection_uuid,
            )

        rescan = await store.begin_history(connection_uuid, "queue-66-rescan")
        try:
            concurrent_message_uuid = stable_message_uuid(ENDPOINT, 661)
            concurrent_topic_uuid = stable_topic_uuid(stream_uuid, "Realtime")
            await pool.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'Realtime', $3)
                """,
                concurrent_topic_uuid,
                stream_uuid,
                b"r" * 32,
            )
            await pool.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    topic_uuid, sender_user_uuid, zulip_message_id, content,
                    content_hash, message_hash, created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, $3, 661, 'live during rescan',
                          $6, $6, clock_timestamp(), clock_timestamp())
                """,
                concurrent_message_uuid,
                stable_realm_uuid(ENDPOINT),
                connection_uuid,
                stream_uuid,
                concurrent_topic_uuid,
                b"n" * 32,
            )
            finished = await rescan.finish(["channel:66"])
        finally:
            await rescan.close()

        assert (finished.messages_deleted, finished.topics_deleted) == (1, 1)
        assert await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM "
            "workspace_zulip_bridge.zulip_messages WHERE uuid = $1)",
            concurrent_message_uuid,
        )
        diffs = await pool.fetch(
            """
            SELECT entity_type, entity_uuid, direction, source_hash,
                   partition_key, processing_status
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1
            ORDER BY entity_type
            """,
            provider_uuid,
        )
        assert [dict(row) for row in diffs] == [
            {
                "entity_type": "messages",
                "entity_uuid": message_uuid,
                "direction": "to_workspace",
                "source_hash": None,
                "partition_key": stream_uuid,
                "processing_status": "pending",
            },
            {
                "entity_type": "topics",
                "entity_uuid": topic_uuid,
                "direction": "to_workspace",
                "source_hash": None,
                "partition_key": stream_uuid,
                "processing_status": "pending",
            },
        ]

        old_message_uuid = stable_message_uuid(ENDPOINT, 662)
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_messages (
                uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                sender_user_uuid, zulip_message_id, content, content_hash,
                message_hash, created_at, source_updated_at
            ) VALUES ($1, $2, $3, $4, $3, 662, 'outside recovery window',
                      $5, $5, clock_timestamp() - interval '2 days',
                      clock_timestamp() - interval '2 days')
            """,
            old_message_uuid,
            stable_realm_uuid(ENDPOINT),
            connection_uuid,
            stream_uuid,
            b"o" * 32,
        )
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_connections
            SET last_event_cursor_at = clock_timestamp()
            WHERE uuid = $1
            """,
            connection_uuid,
        )
        assert await store.clear_queue(
            connection_uuid,
            "queue-66-rescan",
            3600.0,
        )
        assert await store.set_queue(connection_uuid, "queue-66-bounded", 0)
        pending = await store.list_pending_history_chats(
            connection_uuid,
            "queue-66-bounded",
        )
        assert len(pending) == 1
        assert pending[0].reconcile_since is not None
        bounded = await store.begin_history(connection_uuid, "queue-66-bounded")
        try:
            await bounded.finish(
                ["channel:66"],
                reconcile_since=pending[0].reconcile_since,
            )
        finally:
            await bounded.close()
        assert await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM "
            "workspace_zulip_bridge.zulip_messages WHERE uuid = $1)",
            old_message_uuid,
        )
    finally:
        await pool.close()


def test_live_topic_pruning_enqueues_workspace_tombstone() -> None:
    asyncio.run(_live_topic_pruning_enqueues_tombstone(_dsn()))


async def _live_topic_pruning_enqueues_tombstone(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000f1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000f2")
    generation = UUID("10000000-0000-0000-0000-0000000000f3")
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(
                connection,
                67,
                400,
                queue_id="queue-67",
                status="active",
            )
        assert (
            await store.store_chat_catalog(
                connection_uuid,
                "queue-67",
                _catalog(67, [(67, "Live")], {"channel:67": 0}),
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:67")
        topic_uuid = stable_topic_uuid(stream_uuid, "Empty")
        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                stable_realm_uuid(ENDPOINT),
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'Empty', $3)
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_topics (
                    provider_uuid, snapshot_generation, uuid,
                    workspace_project_id, content_hash,
                    source_updated_at, data
                ) VALUES ($1, $2, $3, $4, $5,
                          clock_timestamp(), '{}'::jsonb)
                """,
                provider_uuid,
                generation,
                topic_uuid,
                project_uuid,
                b"w" * 32,
            )
        processor = ZulipEventProcessor(
            pool,
            store,
            Settings(database_dsn=dsn),
        )

        await processor._prune_empty_topics(connection_uuid)

        assert not await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM workspace_zulip_bridge.zulip_topics "
            "WHERE uuid = $1)",
            topic_uuid,
        )
        diff = await pool.fetchrow(
            """
            SELECT direction, source_hash, target_hash, partition_key,
                   processing_status
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'topics'
              AND entity_uuid = $2
            """,
            provider_uuid,
            topic_uuid,
        )
        assert diff is not None
        assert dict(diff) == {
            "direction": "to_workspace",
            "source_hash": None,
            "target_hash": b"w" * 32,
            "partition_key": stream_uuid,
            "processing_status": "pending",
        }
    finally:
        await pool.close()


def test_prepare_database_requeues_claimed_workspace_work() -> None:
    asyncio.run(_prepare_database_requeues_claimed_workspace_work(_dsn()))


async def _prepare_database_requeues_claimed_workspace_work(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000071")
    project_uuid = UUID("10000000-0000-0000-0000-000000000072")
    entity_uuid = UUID("10000000-0000-0000-0000-000000000073")
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 71, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_events (
                    uuid, provider_uuid, workspace_project_id, epoch_version,
                    object_type, action, entity_uuid, payload,
                    processing_status, claimed_at
                ) VALUES ($1, $2, $3, 1, 'user', 'updated', $4, '{}'::jsonb,
                          'processing', clock_timestamp())
                """,
                UUID("10000000-0000-0000-0000-000000000074"),
                provider_uuid,
                project_uuid,
                user_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    direction, source_updated_at, processing_status, claimed_at
                ) VALUES ($1, 'users', $2, $3, 'to_workspace',
                          clock_timestamp(), 'processing', clock_timestamp())
                """,
                provider_uuid,
                entity_uuid,
                realm_uuid,
            )
        await prepare_database(pool)
        assert await pool.fetchval(
            "SELECT processing_status = 'pending' AND claimed_at IS NULL "
            "FROM workspace_zulip_bridge.workspace_events WHERE provider_uuid = $1",
            provider_uuid,
        )
        assert await pool.fetchval(
            "SELECT processing_status = 'pending' AND claimed_at IS NULL "
            "FROM workspace_zulip_bridge.sync_diffs WHERE provider_uuid = $1",
            provider_uuid,
        )
    finally:
        await pool.close()


def test_workspace_event_processor_prioritizes_live_messages() -> None:
    asyncio.run(_workspace_event_processor_prioritizes_live_messages(_dsn()))


def test_workspace_event_processor_requeues_transient_failure() -> None:
    asyncio.run(_workspace_event_processor_requeues_transient_failure(_dsn()))


def test_workspace_event_processor_ignores_retry_older_than_applied_event() -> None:
    asyncio.run(_workspace_event_processor_ignores_stale_retry(_dsn()))


async def _workspace_event_processor_ignores_stale_retry(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000d1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000d2")
    generation = UUID("10000000-0000-0000-0000-0000000000d3")
    entity_uuid = UUID("10000000-0000-0000-0000-0000000000d4")
    epoch_generation = UUID("10000000-0000-0000-0000-0000000000d5")
    try:
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                provider_uuid, workspace_project_id, active_generation,
                bootstrap_status
            ) VALUES ($1, $2, $3, 'ready')
            """,
            provider_uuid,
            project_uuid,
            generation,
        )
        newer_data = {
            "uuid": str(entity_uuid),
            "display_name": "newer",
            "updated_at": "2026-09-21T12:00:02Z",
        }
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_users (
                provider_uuid, snapshot_generation, uuid,
                workspace_project_id, content_hash, source_updated_at, data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            """,
            provider_uuid,
            generation,
            entity_uuid,
            project_uuid,
            b"n" * 32,
            datetime(2026, 9, 21, 12, 0, 2, tzinfo=UTC),
            json.dumps(newer_data),
        )
        older_frame = {
            "updated_at": "2026-09-21T12:00:01Z",
            "payload": {
                "kind": "user.updated",
                "uuid": str(entity_uuid),
                "display_name": "older retry",
                "updated_at": "2026-09-21T12:00:01Z",
            },
        }
        newer_frame = {
            "updated_at": "2026-09-21T12:00:02Z",
            "payload": {"kind": "user.updated", **newer_data},
        }
        await pool.executemany(
            """
            INSERT INTO workspace_zulip_bridge.workspace_events (
                uuid, provider_uuid, workspace_project_id, epoch_generation,
                epoch_version, object_type, action, entity_uuid, payload,
                processing_status
            ) VALUES ($1, $2, $3, $4, $5, 'user', 'updated', $6, $7::jsonb, $8)
            """,
            [
                (
                    UUID("10000000-0000-0000-0000-0000000000d6"),
                    provider_uuid,
                    project_uuid,
                    epoch_generation,
                    1,
                    entity_uuid,
                    json.dumps(older_frame),
                    "processing",
                ),
                (
                    UUID("10000000-0000-0000-0000-0000000000d7"),
                    provider_uuid,
                    project_uuid,
                    epoch_generation,
                    2,
                    entity_uuid,
                    json.dumps(newer_frame),
                    "applied",
                ),
            ],
        )
        older = await pool.fetchrow(
            "SELECT * FROM workspace_zulip_bridge.workspace_events "
            "WHERE provider_uuid = $1 AND epoch_version = 1",
            provider_uuid,
        )
        assert older is not None
        processor = WorkspaceEventProcessor.__new__(WorkspaceEventProcessor)
        processor._pool = pool
        processor._provider_uuid = provider_uuid

        assert not await processor._apply(older)
        assert (
            json.loads(
                await pool.fetchval(
                    "SELECT data::text FROM workspace_zulip_bridge.workspace_users "
                    "WHERE provider_uuid = $1 AND snapshot_generation = $2 "
                    "AND uuid = $3",
                    provider_uuid,
                    generation,
                    entity_uuid,
                )
            )["display_name"]
            == "newer"
        )
    finally:
        await pool.close()


async def _workspace_event_processor_requeues_transient_failure(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000081")
    project_uuid = UUID("10000000-0000-0000-0000-000000000082")
    try:
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_events (
                uuid, provider_uuid, workspace_project_id, epoch_version,
                object_type, action, entity_uuid, payload
            ) VALUES ($1, $2, $3, 1, 'message', 'updated', $4, $5::jsonb)
            """,
            UUID("10000000-0000-0000-0000-000000000083"),
            provider_uuid,
            project_uuid,
            UUID("10000000-0000-0000-0000-000000000084"),
            json.dumps({"payload": {"content": "temporary"}}),
        )
        settings = Settings(
            database_dsn=dsn,
            workspace_provider_uuid=provider_uuid,
            workspace_project_id=project_uuid,
            workspace_event_max_attempts=3,
            workspace_retry_base_seconds=0.001,
            workspace_retry_cap_seconds=0.001,
        )
        processor = WorkspaceEventProcessor(pool, settings)

        assert await processor.process_once() == 1
        row = await pool.fetchrow(
            "SELECT processing_status, attempt_count, available_at, claimed_at, "
            "processed_at, "
            "last_error FROM workspace_zulip_bridge.workspace_events "
            "WHERE provider_uuid = $1",
            provider_uuid,
        )
        assert row is not None
        assert row["processing_status"] == "pending"
        assert row["attempt_count"] == 1
        assert row["available_at"] is not None
        assert row["claimed_at"] is None
        assert row["processed_at"] is None
        assert row["last_error"] == "workspace_event_error:RuntimeError"
    finally:
        await pool.close()


async def _workspace_event_processor_prioritizes_live_messages(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000091")
    project_uuid = UUID("10000000-0000-0000-0000-000000000092")
    try:
        await pool.executemany(
            """
            INSERT INTO workspace_zulip_bridge.workspace_events (
                uuid, provider_uuid, workspace_project_id, epoch_version,
                object_type, action, entity_uuid, payload
            ) VALUES ($1, $2, $3, $4, $5, 'updated', $6, '{}'::jsonb)
            """,
            [
                (
                    UUID("10000000-0000-0000-0000-000000000093"),
                    provider_uuid,
                    project_uuid,
                    1,
                    "message_reaction",
                    UUID("10000000-0000-0000-0000-000000000094"),
                ),
                (
                    UUID("10000000-0000-0000-0000-000000000095"),
                    provider_uuid,
                    project_uuid,
                    2,
                    "message",
                    UUID("10000000-0000-0000-0000-000000000096"),
                ),
            ],
        )
        processor = WorkspaceEventProcessor.__new__(WorkspaceEventProcessor)
        processor._pool = pool
        processor._provider_uuid = provider_uuid
        processor._settings = SimpleNamespace(
            workspace_event_batch_size=1,
            event_processor_claim_timeout_seconds=60.0,
            workspace_event_max_attempts=8,
            workspace_retry_base_seconds=0.25,
            workspace_retry_cap_seconds=30.0,
        )

        assert await processor.process_once() == 1
        rows = await pool.fetch(
            """
            SELECT object_type, processing_status
            FROM workspace_zulip_bridge.workspace_events
            WHERE provider_uuid = $1
            ORDER BY epoch_version
            """,
            provider_uuid,
        )
        assert [tuple(row) for row in rows] == [
            ("message_reaction", "pending"),
            ("message", "skipped"),
        ]
    finally:
        await pool.close()


def test_live_zulip_message_bypasses_backfill_diff_queue() -> None:
    asyncio.run(_live_zulip_message_bypasses_backfill_diff_queue(_dsn()))


async def _live_zulip_message_bypasses_backfill_diff_queue(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000a1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000a2")
    generation = UUID("10000000-0000-0000-0000-0000000000a3")
    historical_uuid = UUID("10000000-0000-0000-0000-0000000000a4")
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
        catalog = _catalog(10, [(7, "Shared")], {"channel:7": 1})
        assert (
            await store.store_chat_catalog(owner_uuid, "queue-owner", catalog)
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
        topic_uuid = stable_topic_uuid(stream_uuid, "Live")
        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                stable_realm_uuid(ENDPOINT),
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_users (
                    provider_uuid, snapshot_generation, uuid,
                    workspace_project_id, content_hash, source_updated_at, data
                ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{}'::jsonb)
                """,
                provider_uuid,
                generation,
                owner_uuid,
                project_uuid,
                b"p" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_streams (
                    provider_uuid, snapshot_generation, uuid,
                    workspace_project_id, content_hash, source_updated_at, data
                ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{}'::jsonb)
                """,
                provider_uuid,
                generation,
                stream_uuid,
                project_uuid,
                b"p" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_topics (
                    provider_uuid, snapshot_generation, uuid,
                    workspace_project_id, content_hash, source_updated_at, data
                ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{}'::jsonb)
                """,
                provider_uuid,
                generation,
                topic_uuid,
                project_uuid,
                b"p" * 32,
            )
        message = ZulipMessage(
            message_id=9001,
            chat_key="channel:7",
            topic_name="Live",
            sender_user_uuid=owner_uuid,
            content="live",
            is_read=True,
            is_starred=False,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json="[]",
            message_hash=b"m" * 32,
            content_hash=b"c" * 32,
            sent_at=1_800_000_000,
        )
        result = await store.apply_live_messages(
            owner_uuid,
            "queue-owner",
            (),
            (message,),
            (),
        )
        assert result.messages_changed == 1
        live_uuid = stable_message_uuid(ENDPOINT, 9001)
        live_diff = await pool.fetchrow(
            """
            SELECT direction, delivery_priority, processing_status, source_hash
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'messages'
              AND entity_uuid = $2
            """,
            provider_uuid,
            live_uuid,
        )
        assert live_diff is not None
        assert dict(live_diff) == {
            "direction": "to_workspace",
            "delivery_priority": 0,
            "processing_status": "pending",
            "source_hash": b"c" * 32,
        }

        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET direction = 'to_zulip', processing_status = 'processing',
                claimed_at = clock_timestamp() - interval '2 minutes'
            WHERE provider_uuid = $1 AND entity_type = 'messages'
              AND entity_uuid = $2
            """,
            provider_uuid,
            live_uuid,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                direction, source_updated_at
            ) VALUES ($1, 'messages', $2, $3, 'to_zulip',
                      clock_timestamp() - interval '1 year')
            """,
            provider_uuid,
            historical_uuid,
            stable_realm_uuid(ENDPOINT),
        )
        claimed: list[UUID] = []

        async def capture(rows: list[asyncpg.Record]) -> None:
            claimed.extend(row["entity_uuid"] for row in rows)

        worker = WorkspaceDiffWorker.__new__(WorkspaceDiffWorker)
        worker._pool = pool
        worker._provider_uuid = provider_uuid
        worker._settings = SimpleNamespace(
            workspace_sync_batch_size=1,
            event_processor_claim_timeout_seconds=60.0,
        )
        worker._partition = 0
        worker._partition_count = 1
        worker._write_to_zulip = capture  # type: ignore[method-assign]
        async with httpx.AsyncClient() as client:
            assert await worker.process_once(client) == 1
        assert claimed == [live_uuid]

        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_messages (
                provider_uuid, snapshot_generation, uuid,
                workspace_project_id, content_hash, source_updated_at, data
            ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{}'::jsonb)
            """,
            provider_uuid,
            generation,
            live_uuid,
            project_uuid,
            b"w" * 32,
        )
        deleted = await store.apply_live_messages(
            owner_uuid,
            "queue-owner",
            (),
            (),
            (9001,),
        )
        assert deleted.messages_deleted == 1
        assert not await pool.fetchval(
            "SELECT EXISTS(SELECT 1 FROM workspace_zulip_bridge.zulip_messages "
            "WHERE uuid = $1)",
            live_uuid,
        )
        deletion_diff = await pool.fetchrow(
            """
            SELECT direction, delivery_priority, processing_status,
                   source_hash, target_hash
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'messages'
              AND entity_uuid = $2
            """,
            provider_uuid,
            live_uuid,
        )
        assert dict(deletion_diff) == {
            "direction": "to_workspace",
            "delivery_priority": 0,
            "processing_status": "pending",
            "source_hash": None,
            "target_hash": b"w" * 32,
        }
    finally:
        await pool.close()


def test_workspace_event_inbox_deduplicates_and_advances_cursor() -> None:
    asyncio.run(_workspace_event_inbox_round_trip(_dsn()))


async def _workspace_event_inbox_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000001")
    project_uuid = UUID("10000000-0000-0000-0000-000000000002")
    generation = UUID("10000000-0000-0000-0000-000000000003")
    try:
        store = WorkspaceEventStore(pool)
        assert (await store.cursor(provider_uuid, project_uuid)).last_epoch_version == 0
        events = [
            WorkspaceEvent(
                uuid=UUID(f"20000000-0000-0000-0000-{index:012d}"),
                epoch_version=index,
                object_type="message",
                action="updated",
                entity_uuid=UUID(f"30000000-0000-0000-0000-{index:012d}"),
                frame={
                    "uuid": f"20000000-0000-0000-0000-{index:012d}",
                    "epoch_version": index,
                    "object_type": "message",
                    "action": "updated",
                },
            )
            for index in range(1, 1001)
        ]

        assert (
            await store.persist(
                provider_uuid,
                project_uuid,
                generation,
                events,
            )
            == 1000
        )
        assert (
            await store.persist(
                provider_uuid,
                project_uuid,
                generation,
                events,
            )
            == 0
        )
        cursor = await store.cursor(provider_uuid, project_uuid)
        assert cursor.epoch_generation == generation
        assert cursor.last_epoch_version == 1000
        async with pool.acquire() as connection:
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.workspace_events"
                )
                == 1000
            )
    finally:
        await pool.close()


def test_workspace_websocket_round_trip_uses_one_provider_cursor(
    tmp_path: Path,
) -> None:
    asyncio.run(_workspace_websocket_round_trip(_dsn(), tmp_path))


def test_workspace_diff_worker_batches_and_converges(
    tmp_path: Path,
) -> None:
    asyncio.run(_workspace_diff_worker_round_trip(_dsn(), tmp_path))


def test_workspace_planner_does_not_rescan_completed_history(tmp_path: Path) -> None:
    asyncio.run(_workspace_planner_does_not_rescan_completed_history(_dsn(), tmp_path))


def test_workspace_identity_rebind_conflict_is_requeued_as_backfill(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _workspace_identity_rebind_conflict_is_requeued_as_backfill(_dsn(), tmp_path)
    )


def test_live_message_update_advances_source_version() -> None:
    asyncio.run(_live_message_update_advances_source_version(_dsn()))


async def _live_message_update_advances_source_version(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 122, 100, queue_id="queue-version", status="filling"
            )
        catalog = _catalog(122, [(7, "Shared")], {"channel:7": 1})
        assert (
            await store.store_chat_catalog(owner_uuid, "queue-version", catalog)
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        message = ZulipMessage(
            message_id=12201,
            chat_key="channel:7",
            topic_name="Version",
            sender_user_uuid=owner_uuid,
            content="first",
            is_read=False,
            is_starred=False,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json="[]",
            message_hash=b"a" * 32,
            sent_at=1_700_000_000,
        )
        await _load_one_chat(store, pool, owner_uuid, "queue-version", message)
        first_version = await pool.fetchval(
            """
            SELECT source_updated_at
            FROM workspace_zulip_bridge.zulip_messages
            WHERE zulip_message_id = 12201
            """
        )
        changed = replace(
            message,
            content="second",
            message_hash=b"b" * 32,
        )
        result = await store.apply_live_messages(
            owner_uuid,
            "queue-version",
            (),
            (changed,),
            (),
        )
        assert result.messages_changed == 1
        row = await pool.fetchrow(
            """
            SELECT content, source_updated_at
            FROM workspace_zulip_bridge.zulip_messages
            WHERE zulip_message_id = 12201
            """
        )
        assert row is not None
        assert row["content"] == "second"
        assert row["source_updated_at"] > first_version
    finally:
        await pool.close()


async def _workspace_identity_rebind_conflict_is_requeued_as_backfill(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000c1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000c2")
    generation = UUID("10000000-0000-0000-0000-0000000000c3")
    workspace_user_uuid = UUID("10000000-0000-0000-0000-0000000000c4")
    stream_uuid = UUID("10000000-0000-0000-0000-0000000000c5")
    topic_uuid = UUID("10000000-0000-0000-0000-0000000000c6")
    message_uuid = UUID("10000000-0000-0000-0000-0000000000c7")
    source_version = datetime(2026, 9, 24, 12, tzinfo=UTC)
    token_file = tmp_path / "workspace-identity-rebind-conflict.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 121, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET workspace_user_uuid = $2
                WHERE uuid = $1
                """,
                user_uuid,
                workspace_user_uuid,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status, reconciliation_version
                ) VALUES ($1, $2, $3, 'ready', 10)
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    owner_user_uuid, content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', 'identity-rebind',
                          'Identity rebind', $3, $4, $3)
                """,
                stream_uuid,
                realm_uuid,
                user_uuid,
                b"s" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'Identity rebind', $3)
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid,
                    zulip_stream_uuid, topic_uuid, sender_user_uuid,
                    zulip_message_id, content, content_hash, message_hash,
                    created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, $3, 12101,
                          'identity rebind', $6, $7, $8, $8)
                """,
                message_uuid,
                realm_uuid,
                user_uuid,
                stream_uuid,
                topic_uuid,
                b"m" * 32,
                b"h" * 32,
                source_version,
            )
            target_data = {
                "stream_uuid": str(stream_uuid),
                "topic_uuid": str(topic_uuid),
                "author_uuid": str(user_uuid),
                "payload": {"kind": "markdown", "content": "identity rebind"},
                "created_at": source_version.isoformat(),
            }
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_messages (
                    provider_uuid, snapshot_generation, uuid,
                    workspace_project_id, content_hash, source_updated_at, data
                ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                """,
                provider_uuid,
                generation,
                message_uuid,
                project_uuid,
                b"w" * 32,
                source_version,
                json.dumps(target_data),
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    partition_key, direction, delivery_priority,
                    processing_status, source_hash, target_hash,
                    source_updated_at, target_updated_at, attempt_count,
                    last_error
                ) VALUES ($1, 'messages', $2, $3, $4, 'to_workspace', 0,
                          'blocked', $5, $6, $7, $7, 1,
                          'Workspace Provider API returned 409 '
                          'error=entity_version_conflict item_index=0')
                """,
                provider_uuid,
                message_uuid,
                realm_uuid,
                stream_uuid,
                b"m" * 32,
                b"w" * 32,
                source_version,
            )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_DB_POOL_MIN_SIZE": "1",
                "WZB_DB_POOL_MAX_SIZE": "4",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)
        event_worker = WorkspaceEventProcessor(pool, settings)

        assert await worker._apply_projection_upgrade(10, realm_uuid, generation) == 3
        repaired = await pool.fetchrow(
            """
            SELECT direction, delivery_priority, processing_status,
                   source_updated_at, target_updated_at, attempt_count, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'messages'
              AND entity_uuid = $2
            """,
            provider_uuid,
            message_uuid,
        )
        assert repaired is not None
        assert repaired["direction"] == "to_workspace"
        assert repaired["delivery_priority"] == 1
        assert repaired["processing_status"] == "pending"
        assert repaired["source_updated_at"] == source_version + timedelta(
            microseconds=1
        )
        assert repaired["target_updated_at"] == source_version
        assert repaired["attempt_count"] == 0
        assert repaired["last_error"] == ("requeued_workspace_identity_rebind_version")
        reaction_uuid = UUID("10000000-0000-0000-0000-0000000000c8")
        invalid_binding_uuid = UUID("10000000-0000-0000-0000-0000000000c9")
        outside_scope_uuid = UUID("10000000-0000-0000-0000-0000000000ca")
        orphaned_uuid = UUID("10000000-0000-0000-0000-0000000000cb")
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET delivery_priority = 1, processing_status = 'blocked',
                source_updated_at = $3, target_updated_at = $3,
                attempt_count = 2,
                last_error = 'Workspace Provider API returned 409 '
                             'error=entity_version_conflict item_index=0'
            WHERE provider_uuid = $1 AND entity_uuid = $2
            """,
            provider_uuid,
            message_uuid,
            source_version,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                partition_key, direction, delivery_priority,
                processing_status, source_updated_at, target_updated_at,
                attempt_count, last_error
            ) VALUES
                ($1, 'message_reactions', $2, $6, $7, 'to_workspace', 1,
                 'blocked', $8, $8, 2,
                 'Workspace Provider API returned 409 '
                 'error=entity_identity_conflict item_index=0'),
                ($1, 'topic_bindings', $3, $6, $7, 'to_workspace', 1,
                 'blocked', $8, NULL, 2,
                 'Workspace Provider API returned 422 '
                 'error=invalid_entity item_index=0'),
                ($1, 'message_flags', $4, $6, $7, 'to_workspace', 1,
                 'blocked', $8, NULL, 2,
                 'Workspace Provider API returned 409 '
                 'error=entity_not_provider_owned item_index=0'),
                ($1, 'message_reactions', $5, $6, $7, 'to_workspace', 1,
                 'blocked', $8, NULL, 2, 'legacy orphan')
            """,
            provider_uuid,
            reaction_uuid,
            invalid_binding_uuid,
            outside_scope_uuid,
            orphaned_uuid,
            realm_uuid,
            stream_uuid,
            source_version,
        )

        assert await worker._apply_projection_upgrade(11, realm_uuid, generation) == 5
        version_twelve_rows = await pool.fetch(
            """
            SELECT entity_type, entity_uuid, delivery_priority,
                   processing_status, source_updated_at, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1
            ORDER BY entity_uuid
            """,
            provider_uuid,
        )
        by_uuid = {row["entity_uuid"]: row for row in version_twelve_rows}
        repaired_message = by_uuid[message_uuid]
        assert repaired_message["processing_status"] == "pending"
        assert repaired_message["delivery_priority"] == 0
        assert repaired_message["source_updated_at"] > source_version
        assert repaired_message["last_error"] == "requeued_monotonic_message_version"
        assert by_uuid[reaction_uuid]["processing_status"] == "pending"
        assert by_uuid[reaction_uuid]["delivery_priority"] == 0
        assert by_uuid[reaction_uuid]["last_error"] == (
            "requeued_reaction_identity_rebind"
        )
        assert by_uuid[invalid_binding_uuid]["processing_status"] == "pending"
        assert by_uuid[invalid_binding_uuid]["last_error"] == (
            "requeued_valid_topic_binding"
        )
        assert by_uuid[outside_scope_uuid]["processing_status"] == "skipped"
        assert by_uuid[outside_scope_uuid]["last_error"] == (
            "workspace_entity_outside_provider_scope"
        )
        assert by_uuid[orphaned_uuid]["processing_status"] == "skipped"
        assert by_uuid[orphaned_uuid]["last_error"] == "orphaned_sync_diff"
        assert await worker._apply_projection_upgrade(12, realm_uuid, generation) == 0

        ownerless_stream_uuid = UUID("10000000-0000-0000-0000-0000000000cc")
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_users (
                provider_uuid, snapshot_generation, uuid,
                workspace_project_id, content_hash, source_updated_at, data
            ) VALUES
                ($1, $2, $3, $4, $5, $7, '{}'::jsonb),
                ($1, $2, $6, $4, $5, $7, '{}'::jsonb)
            """,
            provider_uuid,
            generation,
            user_uuid,
            project_uuid,
            b"u" * 32,
            workspace_user_uuid,
            source_version,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_streams (
                uuid, realm_uuid, chat_type, chat_key, name,
                private, content_hash
            ) VALUES ($1, $2, 'channel', 'ownerless-empty',
                      'Ownerless empty', true, $3)
            """,
            ownerless_stream_uuid,
            realm_uuid,
            b"e" * 32,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                direction, processing_status, source_updated_at,
                target_updated_at, last_error
            ) VALUES
                ($1, 'users', $2, $5, 'to_workspace', 'blocked', $6, $6,
                 'Workspace Provider API returned 409 '
                 'error=provider_user_is_referenced item_index=0'),
                ($1, 'users', $3, $5, 'to_zulip', 'blocked', $6, $6,
                 'Workspace user profile writes are not supported'),
                ($1, 'streams', $4, $5, 'to_workspace', 'blocked', $6, NULL,
                 'Workspace Provider API returned 422 '
                 'error=invalid_uuid item_index=0')
            """,
            provider_uuid,
            user_uuid,
            workspace_user_uuid,
            ownerless_stream_uuid,
            realm_uuid,
            source_version,
        )

        assert await worker._apply_projection_upgrade(13, realm_uuid, generation) == 3
        repaired_tail = await pool.fetch(
            """
            SELECT entity_uuid, processing_status, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1
              AND entity_uuid = ANY($2::uuid[])
            ORDER BY entity_uuid
            """,
            provider_uuid,
            [user_uuid, workspace_user_uuid, ownerless_stream_uuid],
        )
        repaired_tail_by_uuid = {row["entity_uuid"]: row for row in repaired_tail}
        assert repaired_tail_by_uuid[user_uuid]["processing_status"] == "skipped"
        assert repaired_tail_by_uuid[user_uuid]["last_error"] == (
            "workspace_linked_user_projection"
        )
        assert repaired_tail_by_uuid[workspace_user_uuid]["processing_status"] == (
            "skipped"
        )
        assert repaired_tail_by_uuid[workspace_user_uuid]["last_error"] == (
            "workspace_linked_user_projection"
        )
        assert repaired_tail_by_uuid[ownerless_stream_uuid]["processing_status"] == (
            "skipped"
        )
        assert repaired_tail_by_uuid[ownerless_stream_uuid]["last_error"] == (
            "ownerless_empty_stream"
        )
        assert (
            await worker._plan_target_only(
                "users", _SOURCE_TABLES["users"], realm_uuid, generation
            )
            == 0
        )
        for linked_uuid in (user_uuid, workspace_user_uuid):
            assert not await event_worker._upsert_diff(
                "users",
                linked_uuid,
                None,
                b"v" * 32,
                source_version + timedelta(seconds=1),
                project_uuid,
            )
        assert await worker._apply_projection_upgrade(14, realm_uuid, generation) == 0
    finally:
        await pool.close()


async def _workspace_planner_does_not_rescan_completed_history(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000a1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000a2")
    generation = UUID("10000000-0000-0000-0000-0000000000a3")
    stream_uuid = UUID("10000000-0000-0000-0000-0000000000a4")
    topic_uuid = UUID("10000000-0000-0000-0000-0000000000a5")
    token_file = tmp_path / "workspace-cursor-driven.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(connection, 101, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status, reconciliation_version
                ) VALUES ($1, $2, $3, 'ready', 2)
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    owner_user_uuid, content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', 'repair', 'Repair',
                          $3, $4, $3)
                """,
                stream_uuid,
                realm_uuid,
                owner_uuid,
                b"s" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'Recovered', $3)
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_DB_POOL_MIN_SIZE": "1",
                "WZB_DB_POOL_MAX_SIZE": "4",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)

        blocked_flag_uuid = UUID("10000000-0000-0000-0000-0000000000a6")
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                partition_key, direction, source_hash, source_updated_at,
                processing_status, attempt_count, last_error
            ) VALUES (
                $1, 'message_flags', $2, $3, $4, 'to_zulip', $5,
                clock_timestamp(), 'blocked', 41,
                'Zulip message flags cannot be updated: mentioned'
            )
            """,
            provider_uuid,
            blocked_flag_uuid,
            realm_uuid,
            stream_uuid,
            b"f" * 32,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                partition_key, direction, source_hash, source_updated_at,
                processing_status, attempt_count, last_error
            ) VALUES (
                $1, 'streams', $2, $3, $2, 'to_workspace', $4,
                clock_timestamp(), 'blocked', 1,
                'Workspace Provider API returned 422 error=invalid_entity item_index=0'
            )
            """,
            provider_uuid,
            stream_uuid,
            realm_uuid,
            b"s" * 32,
        )
        topic_binding_uuid = UUID("10000000-0000-0000-0000-0000000000a7")
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                partition_key, direction, source_hash, source_updated_at,
                processing_status, attempt_count, last_error
            ) VALUES (
                $1, 'topic_bindings', $2, $3, $4, 'to_workspace', $5,
                clock_timestamp(), 'blocked', 1,
                'Workspace Provider API returned 422 error=invalid_entity item_index=1'
            )
            """,
            provider_uuid,
            topic_binding_uuid,
            realm_uuid,
            stream_uuid,
            b"b" * 32,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.sync_plan_cursors (
                provider_uuid, entity_type, snapshot_generation,
                source_updated_at, entity_uuid
            ) VALUES
                ($1, 'topics', $2, clock_timestamp(), $3),
                ($1, 'messages', $2, clock_timestamp(), $3)
            """,
            provider_uuid,
            generation,
            topic_uuid,
        )
        assert await worker._apply_projection_upgrade() == 6
        recovered = await pool.fetchrow(
            """
            SELECT processing_status, attempt_count, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'message_flags'
              AND entity_uuid = $2
            """,
            provider_uuid,
            blocked_flag_uuid,
        )
        assert recovered is not None
        assert recovered["processing_status"] == "pending"
        assert recovered["attempt_count"] == 0
        assert recovered["last_error"] == "requeued_provider_owned_mentioned"
        stream_recovered = await pool.fetchrow(
            """
            SELECT processing_status, attempt_count, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'streams'
              AND entity_uuid = $2
            """,
            provider_uuid,
            stream_uuid,
        )
        assert stream_recovered is not None
        assert stream_recovered["processing_status"] == "pending"
        assert stream_recovered["attempt_count"] == 0
        assert stream_recovered["last_error"] == (
            "requeued_normalized_stream_description"
        )
        topic_binding_recovered = await pool.fetchrow(
            """
            SELECT processing_status, attempt_count, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'topic_bindings'
              AND entity_uuid = $2
            """,
            provider_uuid,
            topic_binding_uuid,
        )
        assert topic_binding_recovered is not None
        assert topic_binding_recovered["processing_status"] == "skipped"
        assert topic_binding_recovered["attempt_count"] == 0
        assert topic_binding_recovered["last_error"] == "orphaned_topic_binding"
        repaired_cursors = await pool.fetch(
            """
            SELECT entity_type, source_updated_at, entity_uuid
            FROM workspace_zulip_bridge.sync_plan_cursors
            WHERE provider_uuid = $1
              AND entity_type IN ('topics', 'messages')
            ORDER BY entity_type
            """,
            provider_uuid,
        )
        assert [row["entity_type"] for row in repaired_cursors] == [
            "messages",
            "topics",
        ]
        assert all(row["source_updated_at"] is None for row in repaired_cursors)
        assert all(row["entity_uuid"] is None for row in repaired_cursors)

        repaired_topic = await pool.fetchrow(
            """
            SELECT processing_status, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'topics'
              AND entity_uuid = $2
            """,
            provider_uuid,
            topic_uuid,
        )
        assert repaired_topic is not None
        assert tuple(repaired_topic) == ("pending", None)
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET processing_status = 'applied', processed_at = clock_timestamp()
            WHERE provider_uuid = $1 AND entity_type = 'topics'
              AND entity_uuid = $2
            """,
            provider_uuid,
            topic_uuid,
        )
        assert await worker._apply_projection_upgrade(8, realm_uuid, generation) == 1
        repaired_topic = await pool.fetchrow(
            """
            SELECT processing_status, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'topics'
              AND entity_uuid = $2
            """,
            provider_uuid,
            topic_uuid,
        )
        assert tuple(repaired_topic) == (
            "pending",
            "requeued_missing_catalog_dependency",
        )
        assert (
            await worker._plan_entity(
                "topics", _SOURCE_TABLES["topics"], realm_uuid, generation
            )
            == 1
        )
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET processing_status = 'blocked', last_error = 'invalid_entity'
            WHERE provider_uuid = $1 AND entity_type = 'topics'
              AND entity_uuid = $2
            """,
            provider_uuid,
            topic_uuid,
        )
        assert (
            await worker._plan_entity(
                "topics", _SOURCE_TABLES["topics"], realm_uuid, generation
            )
            == 0
        )
        assert (
            await pool.fetchval(
                """
                SELECT processing_status
                FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'topics'
                  AND entity_uuid = $2
                """,
                provider_uuid,
                topic_uuid,
            )
            == "blocked"
        )
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_topics
            SET content_hash = $2, updated_at = clock_timestamp()
            WHERE uuid = $1
            """,
            topic_uuid,
            b"u" * 32,
        )
        assert (
            await worker._plan_entity(
                "topics", _SOURCE_TABLES["topics"], realm_uuid, generation
            )
            == 1
        )
        assert (
            await pool.fetchval(
                """
                SELECT processing_status
                FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'topics'
                  AND entity_uuid = $2
                """,
                provider_uuid,
                topic_uuid,
            )
            == "pending"
        )
        await pool.execute(
            """
            DELETE FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'topics'
              AND entity_uuid = $2
            """,
            provider_uuid,
            topic_uuid,
        )
        assert (
            await worker._plan_entity(
                "topics", _SOURCE_TABLES["topics"], realm_uuid, generation
            )
            == 0
        )
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_plan_cursors
            SET source_updated_at = clock_timestamp(), entity_uuid = $2
            WHERE provider_uuid = $1
              AND entity_type IN ('topics', 'messages')
            """,
            provider_uuid,
            topic_uuid,
        )
        assert await worker._apply_projection_upgrade(5) == 1
        version_six_cursors = await pool.fetch(
            """
            SELECT entity_type, source_updated_at, entity_uuid
            FROM workspace_zulip_bridge.sync_plan_cursors
            WHERE provider_uuid = $1
              AND entity_type IN ('topics', 'messages')
            ORDER BY entity_type
            """,
            provider_uuid,
        )
        assert version_six_cursors[0]["entity_type"] == "messages"
        assert version_six_cursors[0]["source_updated_at"] is None
        assert version_six_cursors[0]["entity_uuid"] is None
        assert version_six_cursors[1]["entity_type"] == "topics"
        assert version_six_cursors[1]["source_updated_at"] is None
        assert version_six_cursors[1]["entity_uuid"] is None
    finally:
        await pool.close()


def test_workspace_diff_dependencies_gate_children_and_batch_errors_isolate(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _workspace_diff_dependencies_gate_children_and_batch_errors_isolate(
            _dsn(), tmp_path
        )
    )


async def _workspace_diff_dependencies_gate_children_and_batch_errors_isolate(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000b1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000b2")
    generation = UUID("10000000-0000-0000-0000-0000000000b3")
    stream_uuid = UUID("10000000-0000-0000-0000-0000000000b4")
    topic_uuid = UUID("10000000-0000-0000-0000-0000000000b5")
    message_uuid = UUID("10000000-0000-0000-0000-0000000000b6")
    second_uuid = UUID("10000000-0000-0000-0000-0000000000b7")
    claimed_at = datetime(2026, 9, 22, tzinfo=UTC)
    token_file = tmp_path / "workspace-dependencies.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 111, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            for table, entity_uuid in (
                ("users", user_uuid),
                ("streams", stream_uuid),
            ):
                await connection.execute(
                    f"""
                    INSERT INTO workspace_zulip_bridge.workspace_{table} (
                        provider_uuid, snapshot_generation, uuid,
                        workspace_project_id, content_hash, source_updated_at, data
                    ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{{}}'::jsonb)
                    """,
                    provider_uuid,
                    generation,
                    entity_uuid,
                    project_uuid,
                    b"x" * 32,
                )
            await connection.executemany(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    partition_key, direction, processing_status,
                    source_updated_at, attempt_count, claimed_at
                ) VALUES ($1, 'messages', $2, $3, $4, 'to_workspace',
                          'processing', clock_timestamp(), 1, $5)
                """,
                [
                    (provider_uuid, message_uuid, realm_uuid, stream_uuid, claimed_at),
                    (provider_uuid, second_uuid, realm_uuid, stream_uuid, claimed_at),
                ],
            )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_DB_POOL_MIN_SIZE": "1",
                "WZB_DB_POOL_MAX_SIZE": "4",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)
        rows = await pool.fetch(
            """
            SELECT * FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 ORDER BY entity_uuid
            """,
            provider_uuid,
        )
        message_data = {
            "stream_uuid": str(stream_uuid),
            "topic_uuid": str(topic_uuid),
            "author_uuid": str(user_uuid),
        }
        candidate = (
            rows[0],
            message_data,
            b"m" * 32,
            {"action": "upsert", "type": "messages", "uuid": str(message_uuid)},
        )

        ready, deferred = await worker._partition_dependency_ready([candidate])
        assert ready == []
        assert deferred == [rows[0]]

        await worker._defer_for_dependencies(deferred)
        deferred_state = await pool.fetchrow(
            """
            SELECT processing_status, attempt_count, dependency_wait_count,
                   EXTRACT(EPOCH FROM (available_at - updated_at)) AS retry_seconds
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'messages'
              AND entity_uuid = $2
            """,
            provider_uuid,
            message_uuid,
        )
        assert deferred_state is not None
        assert deferred_state["processing_status"] == "pending"
        assert deferred_state["attempt_count"] == 0
        assert deferred_state["dependency_wait_count"] == 1
        assert 1.5 <= float(deferred_state["retry_seconds"]) <= 2.5
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET processing_status = 'processing', claimed_at = $3,
                attempt_count = 1
            WHERE provider_uuid = $1 AND entity_uuid = $2
            """,
            provider_uuid,
            message_uuid,
            claimed_at,
        )

        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_topics (
                provider_uuid, snapshot_generation, uuid,
                workspace_project_id, content_hash, source_updated_at, data
            ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{}'::jsonb)
            """,
            provider_uuid,
            generation,
            topic_uuid,
            project_uuid,
            b"t" * 32,
        )
        ready, deferred = await worker._partition_dependency_ready([candidate])
        assert ready == [candidate]
        assert deferred == []

        binding_uuid = stable_stream_binding_uuid(stream_uuid, user_uuid)
        flag_uuid = stable_message_flag_uuid(message_uuid, user_uuid)
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', 'channel:111', 'Dependencies',
                          $3, $4)
                """,
                stream_uuid,
                realm_uuid,
                b"s" * 32,
                user_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                    uuid, zulip_stream_uuid, zulip_user_uuid, role,
                    membership_kind, content_hash
                ) VALUES ($1, $2, $3, 'member', 'subscriber', $4)
                """,
                binding_uuid,
                stream_uuid,
                user_uuid,
                b"b" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    sender_user_uuid, zulip_message_id, content, content_hash,
                    message_hash, created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $3, 11101, 'dependencies', $5, $6,
                          clock_timestamp(), clock_timestamp())
                """,
                message_uuid,
                realm_uuid,
                user_uuid,
                stream_uuid,
                b"c" * 32,
                b"m" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_message_flags (
                    uuid, realm_uuid, zulip_stream_uuid, message_uuid,
                    zulip_user_uuid, is_read, flags_hash
                ) VALUES ($1, $2, $3, $4, $5, true, $6)
                """,
                flag_uuid,
                realm_uuid,
                stream_uuid,
                message_uuid,
                user_uuid,
                b"f" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_messages (
                    provider_uuid, snapshot_generation, uuid,
                    workspace_project_id, content_hash, source_updated_at, data
                ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{}'::jsonb)
                """,
                provider_uuid,
                generation,
                message_uuid,
                project_uuid,
                b"m" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    partition_key, direction, processing_status,
                    source_updated_at, attempt_count, claimed_at
                ) VALUES ($1, 'message_flags', $2, $3, $4, 'to_workspace',
                          'processing', clock_timestamp(), 1, $5)
                """,
                provider_uuid,
                flag_uuid,
                realm_uuid,
                stream_uuid,
                claimed_at,
            )
        flag_row = await pool.fetchrow(
            """
            SELECT * FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'message_flags'
              AND entity_uuid = $2
            """,
            provider_uuid,
            flag_uuid,
        )
        assert flag_row is not None
        flag_data = {
            "stream_uuid": str(stream_uuid),
            "message_uuid": str(message_uuid),
            "user_uuid": str(user_uuid),
        }
        flag_candidate = (
            flag_row,
            flag_data,
            b"f" * 32,
            {"action": "upsert", "type": "message_flags", "uuid": str(flag_uuid)},
        )

        ready, deferred = await worker._partition_dependency_ready([flag_candidate])
        assert ready == []
        assert deferred == [flag_row]

        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_stream_bindings (
                provider_uuid, snapshot_generation, uuid,
                workspace_project_id, content_hash, source_updated_at, data
            ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), '{}'::jsonb)
            """,
            provider_uuid,
            generation,
            binding_uuid,
            project_uuid,
            b"b" * 32,
        )
        ready, deferred = await worker._partition_dependency_ready([flag_candidate])
        assert ready == [flag_candidate]
        assert deferred == []

        assert await worker._isolate_provider_failure(
            [(rows[0], message_data, b"m" * 32), (rows[1], message_data, b"n" * 32)],
            ProviderApiError(422, "invalid_entity", 0),
        )
        states = await pool.fetch(
            """
            SELECT entity_uuid, processing_status, attempt_count, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 ORDER BY entity_uuid
            """,
            provider_uuid,
        )
        assert states[0]["processing_status"] == "blocked"
        assert states[0]["attempt_count"] == 1
        assert states[0]["last_error"].endswith("item_index=0")
        assert states[1]["processing_status"] == "pending"
        assert states[1]["attempt_count"] == 0
        assert states[1]["last_error"] == "workspace_batch_rolled_back item_index=0"

        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET processing_status = 'processing', claimed_at = clock_timestamp(),
                attempt_count = 1, last_error = NULL
            WHERE provider_uuid = $1
            """,
            provider_uuid,
        )
        rows = await pool.fetch(
            """
            SELECT * FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 ORDER BY entity_uuid
            """,
            provider_uuid,
        )
        assert not await worker._isolate_provider_failure(
            [(rows[0], message_data, b"m" * 32), (rows[1], message_data, b"n" * 32)],
            ProviderApiError(503, "provider_unavailable", 0),
        )
        states = await pool.fetch(
            """
            SELECT entity_uuid, processing_status, attempt_count, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 ORDER BY entity_uuid
            """,
            provider_uuid,
        )
        assert states[0]["processing_status"] == "failed"
        assert states[0]["last_error"].endswith("item_index=0")
        assert states[1]["processing_status"] == "pending"
        assert states[1]["attempt_count"] == 0
    finally:
        await pool.close()


def test_workspace_diff_completion_requires_current_claim() -> None:
    asyncio.run(_workspace_diff_completion_requires_current_claim(_dsn()))


async def _workspace_diff_completion_requires_current_claim(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000091")
    project_uuid = UUID("10000000-0000-0000-0000-000000000092")
    generation = UUID("10000000-0000-0000-0000-000000000093")
    entity_uuid = UUID("10000000-0000-0000-0000-000000000094")
    old_claim = datetime(2026, 1, 1, tzinfo=UTC)
    new_claim = datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC)
    try:
        async with pool.acquire() as connection:
            await _insert_user(connection, 91, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    direction, source_updated_at, processing_status, claimed_at
                ) VALUES ($1, 'users', $2, $3, 'to_workspace',
                          clock_timestamp(), 'processing', $4)
                """,
                provider_uuid,
                entity_uuid,
                realm_uuid,
                old_claim,
            )
            stale_row = await connection.fetchrow(
                "SELECT * FROM workspace_zulip_bridge.sync_diffs "
                "WHERE provider_uuid = $1 AND entity_type = 'users' "
                "AND entity_uuid = $2",
                provider_uuid,
                entity_uuid,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.sync_diffs
                SET claimed_at = $4, attempt_count = attempt_count + 1
                WHERE provider_uuid = $1 AND entity_type = $2 AND entity_uuid = $3
                """,
                provider_uuid,
                "users",
                entity_uuid,
                new_claim,
            )
        assert stale_row is not None
        worker = WorkspaceDiffWorker.__new__(WorkspaceDiffWorker)
        worker._pool = pool
        worker._provider_uuid = provider_uuid
        worker._project_uuid = project_uuid
        worker._settings = Settings(database_dsn=dsn)

        await worker._mark([stale_row], "blocked", "stale failure")
        await worker._accept_to_zulip(stale_row, "stale success")
        stale_result = [(stale_row, {}, b"s" * 32)]
        await worker._accept(stale_result)
        await worker._accept_equivalent(stale_result)

        current = await pool.fetchrow(
            """
            SELECT processing_status, claimed_at, last_error
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'users' AND entity_uuid = $2
            """,
            provider_uuid,
            entity_uuid,
        )
        assert current is not None
        assert dict(current) == {
            "processing_status": "processing",
            "claimed_at": new_claim,
            "last_error": None,
        }
        assert not await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM workspace_zulip_bridge.workspace_users "
            "WHERE provider_uuid = $1 AND uuid = $2)",
            provider_uuid,
            entity_uuid,
        )

        fresh_row = await pool.fetchrow(
            "SELECT * FROM workspace_zulip_bridge.sync_diffs "
            "WHERE provider_uuid = $1 AND entity_type = 'users' "
            "AND entity_uuid = $2",
            provider_uuid,
            entity_uuid,
        )
        assert fresh_row is not None
        await worker._mark([fresh_row], "skipped", "current claim")
        assert (
            await pool.fetchval(
                "SELECT processing_status FROM workspace_zulip_bridge.sync_diffs "
                "WHERE provider_uuid = $1 AND entity_type = 'users' "
                "AND entity_uuid = $2",
                provider_uuid,
                entity_uuid,
            )
            == "skipped"
        )
    finally:
        await pool.close()


def test_workspace_diff_message_outbox_ignores_source_timestamp(tmp_path: Path) -> None:
    asyncio.run(
        _workspace_diff_message_outbox_ignores_source_timestamp(_dsn(), tmp_path)
    )


def test_workspace_diff_planner_schedules_unready_entity_graph(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _workspace_diff_planner_schedules_unready_entity_graph(_dsn(), tmp_path)
    )


def test_workspace_outbox_waits_until_source_becomes_eligible(
    tmp_path: Path,
) -> None:
    asyncio.run(_workspace_outbox_waits_until_source_becomes_eligible(_dsn(), tmp_path))


def test_workspace_projection_upgrade_restores_missing_topic_journal(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _workspace_projection_upgrade_restores_missing_topic_journal(_dsn(), tmp_path)
    )


def test_workspace_projection_upgrade_repairs_recent_flag_journal_gap(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _workspace_projection_upgrade_repairs_recent_flag_journal_gap(_dsn(), tmp_path)
    )


def test_workspace_sync_scopes_provider_realm_to_project(tmp_path: Path) -> None:
    asyncio.run(_workspace_sync_scopes_provider_realm_to_project(_dsn(), tmp_path))


def test_workspace_control_replaces_inactive_provider_realm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(
        _workspace_control_replaces_inactive_provider_realm(
            _dsn(),
            tmp_path,
            monkeypatch,
        )
    )


def test_workspace_diff_materializes_topic_bindings(tmp_path: Path) -> None:
    asyncio.run(_workspace_diff_materializes_topic_bindings(_dsn(), tmp_path))


def test_workspace_diff_topic_binding_repair_survives_concurrent_topic_removal(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _workspace_diff_topic_binding_repair_survives_concurrent_topic_removal(
            _dsn(), tmp_path
        )
    )


def test_workspace_bootstrap_activates_verified_generation(
    tmp_path: Path,
) -> None:
    asyncio.run(_workspace_bootstrap_round_trip(_dsn(), tmp_path))


def test_workspace_bootstrap_discards_interrupted_generation(
    tmp_path: Path,
) -> None:
    asyncio.run(_workspace_bootstrap_discards_interrupted_generation(_dsn(), tmp_path))


def test_workspace_identity_reconcile_preserves_external_account_owner(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _workspace_identity_reconcile_preserves_external_account_owner(_dsn(), tmp_path)
    )


def test_workspace_event_uses_entity_timestamp_for_diff_direction() -> None:
    asyncio.run(_workspace_event_uses_entity_timestamp(_dsn()))


def test_workspace_batched_event_suppresses_older_item_retry() -> None:
    asyncio.run(_workspace_batched_event_suppresses_older_item_retry(_dsn()))


async def _workspace_sync_scopes_provider_realm_to_project(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000201")
    project_uuid = UUID("10000000-0000-0000-0000-000000000202")
    other_project_uuid = UUID("10000000-0000-0000-0000-000000000203")
    realm_uuid = UUID("10000000-0000-0000-0000-000000000204")
    other_realm_uuid = UUID("10000000-0000-0000-0000-000000000205")
    entity_uuid = UUID("10000000-0000-0000-0000-000000000206")
    user_uuid = UUID("10000000-0000-0000-0000-000000000207")
    token_file = tmp_path / "workspace-sync-project.token"
    token_file.write_text("integration-token")
    try:
        await pool.executemany(
            """
            INSERT INTO workspace_zulip_bridge.zulip_realms (
                uuid, identity_key, endpoint, workspace_project_id,
                workspace_provider_uuid
            ) VALUES ($1, $2, $2, $3, $4)
            """,
            [
                (
                    realm_uuid,
                    "https://current.example.test",
                    project_uuid,
                    provider_uuid,
                ),
                (
                    other_realm_uuid,
                    "https://other.example.test",
                    other_project_uuid,
                    provider_uuid,
                ),
            ],
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_users (
                uuid, realm_uuid, zulip_user_id, login, full_name, role
            ) VALUES ($1, $2, 7, 'member@example.test', 'Member', 400)
            """,
            user_uuid,
            other_realm_uuid,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_connections (
                uuid, realm_uuid, zulip_user_uuid, login, api_key, sync_enabled
            ) VALUES ($1, $2, $3, 'member@example.test', 'private-key', true)
            """,
            UUID("10000000-0000-0000-0000-000000000208"),
            other_realm_uuid,
            user_uuid,
        )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_WORKSPACE_API_URL": "http://workspace.test/api/workspace/v1",
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)
        assert await worker._link_realm() == other_realm_uuid
        current_realm = await pool.fetchrow(
            """
            SELECT workspace_project_id, workspace_provider_uuid
            FROM workspace_zulip_bridge.zulip_realms WHERE uuid = $1
            """,
            realm_uuid,
        )
        active_realm = await pool.fetchrow(
            """
            SELECT workspace_project_id, workspace_provider_uuid
            FROM workspace_zulip_bridge.zulip_realms WHERE uuid = $1
            """,
            other_realm_uuid,
        )
        assert current_realm is not None
        assert current_realm["workspace_project_id"] is None
        assert current_realm["workspace_provider_uuid"] is None
        assert active_realm is not None
        assert active_realm["workspace_project_id"] == project_uuid
        assert active_realm["workspace_provider_uuid"] == provider_uuid

        processor = WorkspaceEventProcessor(pool, settings)
        assert await processor._upsert_diff(
            "users",
            entity_uuid,
            None,
            b"t" * 32,
            datetime.now(UTC),
            project_uuid,
        )
        row = await pool.fetchrow(
            """
            SELECT realm_uuid
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'users'
              AND entity_uuid = $2
            """,
            provider_uuid,
            entity_uuid,
        )
        assert row is not None
        assert row["realm_uuid"] == other_realm_uuid
    finally:
        await pool.close()


async def _workspace_control_replaces_inactive_provider_realm(
    dsn: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000211")
    project_uuid = UUID("10000000-0000-0000-0000-000000000212")
    other_project_uuid = UUID("10000000-0000-0000-0000-000000000213")
    stale_realm_uuid = UUID("10000000-0000-0000-0000-000000000214")
    target_endpoint = "https://replacement.example.test"
    target_realm_uuid = stable_realm_uuid(target_endpoint)
    account_uuid = UUID("10000000-0000-0000-0000-000000000215")
    owner_uuid = UUID("10000000-0000-0000-0000-000000000216")
    provider_user_uuid = stable_user_uuid(target_endpoint, 7)
    secret = tmp_path / "enrollment.secret"
    secret.write_text("integration-secret")
    try:
        await pool.executemany(
            """
            INSERT INTO workspace_zulip_bridge.zulip_realms (
                uuid, identity_key, endpoint, workspace_project_id,
                workspace_provider_uuid
            ) VALUES ($1, $2, $2, $3, $4)
            """,
            [
                (
                    stale_realm_uuid,
                    "https://stale.example.test",
                    project_uuid,
                    provider_uuid,
                ),
                (
                    target_realm_uuid,
                    target_endpoint,
                    other_project_uuid,
                    provider_uuid,
                ),
            ],
        )
        settings = Settings(
            database_dsn=dsn,
            workspace_control_url="https://control.example.test",
            workspace_control_bootstrap_url="http://control.example.test",
            workspace_control_hostname="control.example.test",
            workspace_project_id=project_uuid,
            workspace_provider_uuid=provider_uuid,
            workspace_realm_uuid=UUID("10000000-0000-0000-0000-000000000217"),
            workspace_bridge_instance_uuid=UUID("10000000-0000-0000-0000-000000000218"),
            workspace_enrollment_secret_file=secret,
            workspace_control_state_dir=tmp_path / "control",
        )
        worker = WorkspaceControlWorker(pool, settings)
        monkeypatch.setattr(
            worker,
            "_decrypt_credentials",
            lambda *args: {
                "server_url": target_endpoint,
                "email": "member@example.test",
                "api_key": "private-key",
            },
        )
        monkeypatch.setattr(
            worker,
            "_read_zulip_identity",
            lambda *args: SimpleNamespace(user_id=7, full_name="Member", role=400),
        )
        await worker._apply_account(
            {
                "uuid": str(account_uuid),
                "generation": 1,
                "owner_user_uuid": str(owner_uuid),
                "synchronization_enabled": True,
                "settings": {
                    "server_url": target_endpoint,
                    "default_project_id": str(project_uuid),
                },
                "credential_envelope": {},
            }
        )
        stale = await pool.fetchrow(
            """
            SELECT workspace_project_id, workspace_provider_uuid
            FROM workspace_zulip_bridge.zulip_realms WHERE uuid = $1
            """,
            stale_realm_uuid,
        )
        target = await pool.fetchrow(
            """
            SELECT workspace_project_id, workspace_provider_uuid
            FROM workspace_zulip_bridge.zulip_realms WHERE uuid = $1
            """,
            target_realm_uuid,
        )
        connection = await pool.fetchrow(
            """
            SELECT realm_uuid, zulip_user_uuid, sync_enabled
            FROM workspace_zulip_bridge.zulip_connections WHERE uuid = $1
            """,
            account_uuid,
        )
        assert stale is not None
        assert stale["workspace_project_id"] is None
        assert stale["workspace_provider_uuid"] is None
        assert target is not None
        assert target["workspace_project_id"] == project_uuid
        assert target["workspace_provider_uuid"] == provider_uuid
        assert connection is not None
        assert connection["realm_uuid"] == target_realm_uuid
        assert connection["zulip_user_uuid"] == provider_user_uuid
        assert connection["sync_enabled"] is True
    finally:
        await pool.close()


async def _workspace_batched_event_suppresses_older_item_retry(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000091")
    project_uuid = UUID("10000000-0000-0000-0000-000000000092")
    generation = UUID("10000000-0000-0000-0000-000000000093")
    entity_uuid = UUID("10000000-0000-0000-0000-000000000094")
    try:
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_events (
                uuid, provider_uuid, workspace_project_id, epoch_generation,
                epoch_version, object_type, action, entity_uuid, payload,
                processing_status
            ) VALUES
                ($1, $3, $4, $5, 1, 'user', 'updated', $6,
                 '{"payload":{"kind":"user.updated"}}'::jsonb, 'pending'),
                ($2, $3, $4, $5, 2, 'user', 'updated', NULL, $7::jsonb,
                 'applied')
            """,
            UUID("10000000-0000-0000-0000-000000000095"),
            UUID("10000000-0000-0000-0000-000000000096"),
            provider_uuid,
            project_uuid,
            generation,
            entity_uuid,
            json.dumps(
                {
                    "payload": {
                        "kind": "user.updated",
                        "items": [{"uuid": str(entity_uuid)}],
                    }
                }
            ),
        )
        older = await pool.fetchrow(
            """
            SELECT * FROM workspace_zulip_bridge.workspace_events
            WHERE provider_uuid = $1 AND epoch_version = 1
            """,
            provider_uuid,
        )
        assert older is not None
        settings = replace(
            Settings.from_env({"WZB_DATABASE_DSN": dsn}),
            workspace_provider_uuid=provider_uuid,
        )
        processor = WorkspaceEventProcessor(pool, settings)
        assert await processor._newer_entity_event_applied(older, entity_uuid)
    finally:
        await pool.close()


async def _workspace_event_uses_entity_timestamp(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000081")
    project_uuid = UUID("10000000-0000-0000-0000-000000000082")
    generation = UUID("10000000-0000-0000-0000-000000000083")
    entity_time = datetime(2026, 9, 19, 10, tzinfo=UTC)
    event_time = datetime(2026, 9, 19, 11, tzinfo=UTC)
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 81, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    direction, processing_status, source_hash, target_hash,
                    source_updated_at, target_updated_at
                ) VALUES ($1, 'users', $2, $3, 'to_workspace', 'applied',
                          decode(repeat('01', 32), 'hex'),
                          decode(repeat('01', 32), 'hex'), $4, $4)
                """,
                provider_uuid,
                user_uuid,
                realm_uuid,
                entity_time,
            )
            frame = {
                "updated_at": event_time.isoformat().replace("+00:00", "Z"),
                "payload": {
                    "kind": "user.updated",
                    "uuid": str(user_uuid),
                    "display_name": "Updated from Workspace",
                    "updated_at": entity_time.isoformat().replace("+00:00", "Z"),
                },
            }
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_events (
                    uuid, provider_uuid, workspace_project_id, epoch_version,
                    object_type, action, entity_uuid, payload
                ) VALUES ($1, $2, $3, 1, 'user', 'updated', $4, $5::jsonb)
                """,
                UUID("10000000-0000-0000-0000-000000000084"),
                provider_uuid,
                project_uuid,
                user_uuid,
                json.dumps(frame),
            )
            event = await connection.fetchrow(
                "SELECT * FROM workspace_zulip_bridge.workspace_events "
                "WHERE provider_uuid = $1",
                provider_uuid,
            )
        assert event is not None
        processor = WorkspaceEventProcessor.__new__(WorkspaceEventProcessor)
        processor._pool = pool
        processor._provider_uuid = provider_uuid
        assert await processor._apply(event)
        target_time = await pool.fetchval(
            "SELECT source_updated_at FROM workspace_zulip_bridge.workspace_users "
            "WHERE provider_uuid = $1 AND snapshot_generation = $2 AND uuid = $3",
            provider_uuid,
            generation,
            user_uuid,
        )
        assert target_time == entity_time
        diff = await pool.fetchrow(
            "SELECT target_updated_at, direction, processing_status "
            "FROM workspace_zulip_bridge.sync_diffs "
            "WHERE provider_uuid = $1 AND entity_type = 'users' AND entity_uuid = $2",
            provider_uuid,
            user_uuid,
        )
        assert diff is not None
        assert tuple(diff) == (entity_time, "to_workspace", "pending")

        message_uuid = UUID("10000000-0000-0000-0000-000000000085")
        stream_uuid = UUID("10000000-0000-0000-0000-000000000086")
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_streams (
                uuid, realm_uuid, chat_type, chat_key, name,
                content_hash, source_connection_uuid
            ) VALUES ($1, $2, 'channel', 'channel:86', 'Mapped', $3, $4)
            """,
            stream_uuid,
            realm_uuid,
            b"s" * 32,
            user_uuid,
        )
        message_frame = {
            "updated_at": event_time.isoformat().replace("+00:00", "Z"),
            "payload": {
                "kind": "message.created",
                "uuid": str(message_uuid),
                "stream_uuid": str(stream_uuid),
                "payload": {"kind": "markdown", "content": "Live message"},
                "created_at": event_time.isoformat().replace("+00:00", "Z"),
                "updated_at": event_time.isoformat().replace("+00:00", "Z"),
            },
        }
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_events (
                uuid, provider_uuid, workspace_project_id, epoch_version,
                object_type, action, entity_uuid, payload
            ) VALUES ($1, $2, $3, 2, 'message', 'created', $4, $5::jsonb)
            """,
            UUID("10000000-0000-0000-0000-000000000087"),
            provider_uuid,
            project_uuid,
            message_uuid,
            json.dumps(message_frame),
        )
        message_event = await pool.fetchrow(
            "SELECT * FROM workspace_zulip_bridge.workspace_events "
            "WHERE provider_uuid = $1 AND entity_uuid = $2",
            provider_uuid,
            message_uuid,
        )
        assert message_event is not None
        assert await processor._apply(message_event)
        new_diff = await pool.fetchrow(
            """
            SELECT direction, processing_status, partition_key,
                   source_hash, target_hash IS NOT NULL
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'messages'
              AND entity_uuid = $2
            """,
            provider_uuid,
            message_uuid,
        )
        assert new_diff is not None
        assert tuple(new_diff) == (
            "to_zulip",
            "pending",
            stream_uuid,
            None,
            True,
        )

        reaction_uuid = UUID("10000000-0000-0000-0000-00000000008a")
        reaction_frame = {
            "updated_at": event_time.isoformat().replace("+00:00", "Z"),
            "payload": {
                "kind": "message_reaction.created",
                "uuid": str(reaction_uuid),
                "message_uuid": str(message_uuid),
                "user_uuid": str(user_uuid),
                "emoji_name": "eyes",
                "updated_at": event_time.isoformat().replace("+00:00", "Z"),
            },
        }
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_events (
                uuid, provider_uuid, workspace_project_id, epoch_version,
                object_type, action, entity_uuid, payload
            ) VALUES ($1, $2, $3, 3, 'message_reaction', 'created', $4, $5::jsonb)
            """,
            UUID("10000000-0000-0000-0000-00000000008b"),
            provider_uuid,
            project_uuid,
            reaction_uuid,
            json.dumps(reaction_frame),
        )
        reaction_event = await pool.fetchrow(
            "SELECT * FROM workspace_zulip_bridge.workspace_events "
            "WHERE provider_uuid = $1 AND entity_uuid = $2",
            provider_uuid,
            reaction_uuid,
        )
        assert reaction_event is not None
        assert await processor._apply(reaction_event)
        reaction_diff = await pool.fetchrow(
            """
            SELECT direction, processing_status, partition_key
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'message_reactions'
              AND entity_uuid = $2
            """,
            provider_uuid,
            reaction_uuid,
        )
        assert reaction_diff is not None
        assert tuple(reaction_diff) == ("to_zulip", "pending", stream_uuid)
        assert not await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM "
            "workspace_zulip_bridge.zulip_messages WHERE uuid = $1)",
            message_uuid,
        )

        native_message_uuid = UUID("10000000-0000-0000-0000-000000000088")
        native_stream_uuid = UUID("10000000-0000-0000-0000-000000000089")
        native_frame = {
            "updated_at": event_time.isoformat().replace("+00:00", "Z"),
            "payload": {
                "kind": "message.created",
                "uuid": str(native_message_uuid),
                "stream_uuid": str(native_stream_uuid),
                "payload": {"kind": "markdown", "content": "Native message"},
                "created_at": event_time.isoformat().replace("+00:00", "Z"),
                "updated_at": event_time.isoformat().replace("+00:00", "Z"),
            },
        }
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_events (
                uuid, provider_uuid, workspace_project_id, epoch_version,
                object_type, action, entity_uuid, payload
            ) VALUES ($1, $2, $3, 4, 'message', 'created', $4, $5::jsonb)
            """,
            UUID("10000000-0000-0000-0000-000000000090"),
            provider_uuid,
            project_uuid,
            native_message_uuid,
            json.dumps(native_frame),
        )
        native_event = await pool.fetchrow(
            "SELECT * FROM workspace_zulip_bridge.workspace_events "
            "WHERE provider_uuid = $1 AND entity_uuid = $2",
            provider_uuid,
            native_message_uuid,
        )
        assert native_event is not None
        assert await processor._apply(native_event)
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_messages "
                "WHERE provider_uuid = $1 AND uuid = $2",
                provider_uuid,
                native_message_uuid,
            )
            == 1
        )
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.sync_diffs "
                "WHERE provider_uuid = $1 AND entity_type = 'messages' "
                "AND entity_uuid = $2",
                provider_uuid,
                native_message_uuid,
            )
            == 0
        )
    finally:
        await pool.close()


async def _workspace_bootstrap_round_trip(dsn: str, tmp_path: Path) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000031")
    project_uuid = UUID("10000000-0000-0000-0000-000000000032")
    generation = UUID("10000000-0000-0000-0000-000000000033")
    epoch_generation = UUID("10000000-0000-0000-0000-000000000034")
    user_uuid = UUID("10000000-0000-0000-0000-000000000035")
    token_file = tmp_path / "workspace-bootstrap.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_DATABASE_DSN": dsn,
            "WZB_DB_POOL_MIN_SIZE": "1",
            "WZB_DB_POOL_MAX_SIZE": "4",
            "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
            "WZB_WORKSPACE_WEBSOCKET_URL": "ws://workspace.test/api/workspace/v1/events/ws",
            "WZB_WORKSPACE_API_URL": "http://workspace.test/api/workspace/v1",
            "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
            "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    entity = {
        "type": "users",
        "uuid": str(user_uuid),
        "content_hash": "01" * 32,
        "source_updated_at": "2026-09-19T08:00:00Z",
        "data": {"display_name": "Bootstrap User"},
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/users/"):
            return httpx.Response(200, json=[])
        if request.url.path.endswith("/provider/bootstrap"):
            assert request.url.params["mode"] == "paged"
            return httpx.Response(
                200,
                json={
                    "record": "manifest",
                    "schema_version": 2,
                    "snapshot_uuid": str(generation),
                    "project_id": str(project_uuid),
                    "provider_uuid": str(provider_uuid),
                    "epoch_generation": str(epoch_generation),
                    "snapshot_epoch_version": 41,
                    "created_at": "2026-09-19T08:00:00Z",
                },
            )
        entity_type = request.url.path.rsplit("/", 1)[-1]
        assert request.url.params["snapshot_after_uuid"] == str(UUID(int=0))
        return httpx.Response(
            200,
            json={
                "items": [entity] if entity_type == "users" else [],
                "next_cursor": None,
            },
        )

    try:
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                provider_uuid, workspace_project_id, bootstrap_status,
                initial_sync_completed_at
            ) VALUES ($1, $2, 'ready', clock_timestamp())
            """,
            provider_uuid,
            project_uuid,
        )
        bootstrapper = WorkspaceBootstrapper(pool, settings)
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer integration-token"},
        ) as client:
            await bootstrapper.bootstrap(client)
        state = await pool.fetchrow(
            """
            SELECT active_generation, epoch_generation, snapshot_epoch_version,
                   bootstrap_status, initial_sync_completed_at
            FROM workspace_zulip_bridge.workspace_mirror_state
            WHERE provider_uuid = $1
            """,
            provider_uuid,
        )
        assert state is not None
        assert tuple(state) == (generation, epoch_generation, 41, "ready", None)
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_users "
                "WHERE provider_uuid = $1 AND snapshot_generation = $2",
                provider_uuid,
                generation,
            )
            == 1
        )
    finally:
        await pool.close()


async def _workspace_bootstrap_discards_interrupted_generation(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000041")
    project_uuid = UUID("10000000-0000-0000-0000-000000000042")
    stale_generation = UUID("10000000-0000-0000-0000-000000000043")
    replacement_generation = UUID("10000000-0000-0000-0000-000000000044")
    epoch_generation = UUID("10000000-0000-0000-0000-000000000045")
    user_uuid = UUID("10000000-0000-0000-0000-000000000046")
    token_file = tmp_path / "workspace-interrupted-bootstrap.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_DATABASE_DSN": dsn,
            "WZB_DB_POOL_MIN_SIZE": "1",
            "WZB_DB_POOL_MAX_SIZE": "4",
            "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "ws://workspace.test/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_API_URL": "http://workspace.test/api/workspace/v1",
            "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
            "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/provider/bootstrap"):
            return httpx.Response(
                200,
                json={
                    "record": "manifest",
                    "schema_version": 2,
                    "snapshot_uuid": str(replacement_generation),
                    "project_id": str(project_uuid),
                    "provider_uuid": str(provider_uuid),
                    "epoch_generation": str(epoch_generation),
                    "snapshot_epoch_version": 42,
                    "created_at": "2026-09-20T15:00:00Z",
                },
            )
        return httpx.Response(
            503,
            json={"error": "snapshot_unavailable"},
            request=request,
        )

    try:
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                provider_uuid, workspace_project_id, bootstrap_status
            ) VALUES ($1, $2, 'failed')
            """,
            provider_uuid,
            project_uuid,
        )
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_users (
                provider_uuid, snapshot_generation, uuid,
                workspace_project_id, content_hash, source_updated_at, data
            ) VALUES (
                $1, $2, $3, $4, decode(repeat('01', 32), 'hex'),
                '2026-09-20T15:00:00Z', '{}'::jsonb
            )
            """,
            provider_uuid,
            stale_generation,
            user_uuid,
            project_uuid,
        )
        bootstrapper = WorkspaceBootstrapper(pool, settings)
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer integration-token"},
        ) as client:
            with pytest.raises(
                RuntimeError,
                match=(
                    "Workspace Provider API returned 503 error=snapshot_unavailable"
                ),
            ):
                await bootstrapper.bootstrap(client)
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_users "
                "WHERE provider_uuid = $1",
                provider_uuid,
            )
            == 0
        )
        failed_state = await pool.fetchrow(
            "SELECT bootstrap_status, last_error "
            "FROM workspace_zulip_bridge.workspace_mirror_state "
            "WHERE provider_uuid = $1",
            provider_uuid,
        )
        assert tuple(failed_state) == (
            "failed",
            "Workspace Provider API returned 503 error=snapshot_unavailable",
        )
        assert "workspace.test" not in failed_state["last_error"]
    finally:
        await pool.close()


async def _workspace_identity_reconcile_preserves_external_account_owner(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000071")
    project_uuid = UUID("10000000-0000-0000-0000-000000000072")
    generation = UUID("10000000-0000-0000-0000-000000000073")
    owner_uuid = UUID("10000000-0000-0000-0000-000000000074")
    account_uuid = UUID("10000000-0000-0000-0000-000000000075")
    token_file = tmp_path / "workspace-identity-owner.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 71, 400)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET login = 'external-account@example.test',
                    workspace_user_uuid = $2
                WHERE uuid = $1
                """,
                user_uuid,
                owner_uuid,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                stable_realm_uuid(ENDPOINT),
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET external_account_uuid = $2,
                    owner_workspace_user_uuid = $3,
                    login = 'external-account@example.test',
                    lifecycle_status = 'active'
                WHERE uuid = $1
                """,
                user_uuid,
                account_uuid,
                owner_uuid,
            )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_DB_POOL_MIN_SIZE": "1",
                "WZB_DB_POOL_MAX_SIZE": "4",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        bootstrapper = WorkspaceBootstrapper(pool, settings)

        async def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["authorization"] == "Bearer integration-token"
            return httpx.Response(
                200,
                json=[
                    {
                        "uuid": str(owner_uuid),
                        "username": "workspace-owner",
                        "display_name": "Workspace Owner",
                        "email": "different-iam-address@example.test",
                        "source": "iam",
                        "status": "offline",
                        "created_at": "2026-09-20T15:00:00Z",
                        "updated_at": "2026-09-20T15:00:00Z",
                    }
                ],
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            assert (
                await bootstrapper._reconcile_workspace_identities(
                    generation,
                    client=client,
                    schedule_changes=True,
                )
                == 1
            )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[])),
        ) as client:
            assert (
                await bootstrapper._reconcile_workspace_identities(
                    UUID("10000000-0000-0000-0000-000000000076"),
                    client=client,
                    schedule_changes=True,
                )
                == 0
            )
        assert (
            await pool.fetchval(
                "SELECT workspace_user_uuid "
                "FROM workspace_zulip_bridge.zulip_users WHERE uuid = $1",
                user_uuid,
            )
            == owner_uuid
        )
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_users "
                "WHERE snapshot_generation = $1",
                UUID("10000000-0000-0000-0000-000000000076"),
            )
            == 0
        )
    finally:
        await pool.close()


async def _workspace_diff_materializes_topic_bindings(dsn: str, tmp_path: Path) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000091")
    project_uuid = UUID("10000000-0000-0000-0000-000000000092")
    token_file = tmp_path / "workspace-topic-bindings.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 10, 400)
            stream_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
            topic_uuid = stable_topic_uuid(stream_uuid, "General")
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', 'channel:7', 'Test', $3, $4)
                """,
                stream_uuid,
                stable_realm_uuid(ENDPOINT),
                b"s" * 32,
                user_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                    uuid, zulip_stream_uuid, zulip_user_uuid, role,
                    membership_kind, content_hash
                ) VALUES ($1, $2, $3, 'member', 'subscriber', $4)
                """,
                stable_stream_binding_uuid(stream_uuid, user_uuid),
                stream_uuid,
                user_uuid,
                b"b" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'General', $3)
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_DB_POOL_MIN_SIZE": "1",
                "WZB_DB_POOL_MAX_SIZE": "4",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)
        await worker._ensure_topic_bindings(stable_realm_uuid(ENDPOINT))
        await worker._ensure_topic_bindings(stable_realm_uuid(ENDPOINT))

        row = await pool.fetchrow(
            """
            SELECT uuid, zulip_stream_uuid, topic_uuid, zulip_user_uuid,
                   notification_mode
            FROM workspace_zulip_bridge.zulip_topic_bindings
            """
        )
        assert row is not None
        assert dict(row) == {
            "uuid": stable_topic_binding_uuid(topic_uuid, user_uuid),
            "zulip_stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "zulip_user_uuid": user_uuid,
            "notification_mode": "default",
        }
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.zulip_topic_bindings"
            )
            == 1
        )
    finally:
        await pool.close()


async def _workspace_diff_topic_binding_repair_survives_concurrent_topic_removal(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000a1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000a2")
    token_file = tmp_path / "workspace-topic-binding-race.token"
    token_file.write_text("integration-token")
    locking_connection: asyncpg.Connection | None = None
    deleting_connection: asyncpg.Connection | None = None
    repair_task: asyncio.Task[bool] | None = None
    delete_task: asyncio.Task[str] | None = None
    advisory_locked = False
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 10, 400)
            stream_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
            topic_uuid = stable_topic_uuid(stream_uuid, "General")
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', 'channel:7', 'Test', $3, $4)
                """,
                stream_uuid,
                stable_realm_uuid(ENDPOINT),
                b"s" * 32,
                user_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                    uuid, zulip_stream_uuid, zulip_user_uuid, role,
                    membership_kind, content_hash
                ) VALUES ($1, $2, $3, 'member', 'subscriber', $4)
                """,
                stable_stream_binding_uuid(stream_uuid, user_uuid),
                stream_uuid,
                user_uuid,
                b"b" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'General', $3)
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_DB_POOL_MIN_SIZE": "1",
                "WZB_DB_POOL_MAX_SIZE": "4",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)
        async with pool.acquire() as connection:
            await connection.execute(
                """
                CREATE OR REPLACE FUNCTION
                    workspace_zulip_bridge.test_pause_topic_binding_insert()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    PERFORM pg_advisory_xact_lock(938475);
                    RETURN NEW;
                END
                $$;
                CREATE TRIGGER test_pause_topic_binding_insert
                BEFORE INSERT ON workspace_zulip_bridge.zulip_topic_bindings
                FOR EACH ROW EXECUTE FUNCTION
                    workspace_zulip_bridge.test_pause_topic_binding_insert()
                """
            )
        locking_connection = await pool.acquire()
        deleting_connection = await pool.acquire()
        await locking_connection.execute("SELECT pg_advisory_lock(938475)")
        advisory_locked = True

        repair_task = asyncio.create_task(
            worker._ensure_topic_bindings(stable_realm_uuid(ENDPOINT))
        )
        repair_waiting = False
        for _ in range(50):
            repair_waiting = bool(
                await pool.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_stat_activity
                        WHERE pid <> pg_backend_pid()
                          AND wait_event = 'advisory'
                    )
                    """
                )
            )
            if repair_waiting:
                break
            await asyncio.sleep(0.02)

        delete_task = asyncio.create_task(
            deleting_connection.execute(
                "DELETE FROM workspace_zulip_bridge.zulip_topics WHERE uuid = $1",
                topic_uuid,
            )
        )
        await asyncio.sleep(0.1)
        delete_waiting_for_topic_lock = not delete_task.done()
        await locking_connection.execute("SELECT pg_advisory_unlock(938475)")
        advisory_locked = False

        assert repair_waiting
        assert delete_waiting_for_topic_lock
        assert await asyncio.wait_for(repair_task, timeout=2)
        repair_task = None
        assert await asyncio.wait_for(delete_task, timeout=2) == "DELETE 1"
        delete_task = None
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.zulip_topic_bindings"
            )
            == 0
        )
    finally:
        if advisory_locked and locking_connection is not None:
            await locking_connection.execute("SELECT pg_advisory_unlock(938475)")
        if repair_task is not None:
            repair_task.cancel()
            try:
                await repair_task
            except asyncio.CancelledError:
                pass
        if delete_task is not None:
            delete_task.cancel()
            try:
                await delete_task
            except asyncio.CancelledError:
                pass
        if locking_connection is not None:
            await pool.release(locking_connection)
        if deleting_connection is not None:
            await pool.release(deleting_connection)
        async with pool.acquire() as connection:
            await connection.execute(
                """
                DROP TRIGGER IF EXISTS test_pause_topic_binding_insert
                    ON workspace_zulip_bridge.zulip_topic_bindings;
                DROP FUNCTION IF EXISTS
                    workspace_zulip_bridge.test_pause_topic_binding_insert()
                """
            )
        await pool.close()


async def _workspace_outbox_waits_until_source_becomes_eligible(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000b1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000b2")
    generation = UUID("10000000-0000-0000-0000-0000000000b3")
    stream_uuid = UUID("10000000-0000-0000-0000-0000000000b4")
    topic_uuid = UUID("10000000-0000-0000-0000-0000000000b5")
    token_file = tmp_path / "workspace-source-eligibility.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(connection, 82, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status, reconciliation_version
                ) VALUES ($1, $2, $3, 'ready', 15)
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name, content_hash
                ) VALUES ($1, $2, 'channel', '82', 'Deferred source', $3)
                """,
                stream_uuid,
                realm_uuid,
                b"s" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'Deferred topic', $3)
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_outbox (
                    realm_uuid, entity_type, action, entity_uuid
                ) VALUES ($1, 'topic', 'upsert', $2)
                """,
                realm_uuid,
                topic_uuid,
            )
        worker = WorkspaceDiffWorker(
            pool,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_WORKSPACE_WEBSOCKET_URL": (
                        "ws://workspace.test/api/workspace/v1/events/ws"
                    ),
                    "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                    "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                    "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
                }
            ),
        )
        assert await worker._plan_source_outbox(realm_uuid, generation) == 0
        pending = await pool.fetchrow(
            """
            SELECT delivery_status, attempt_count, last_error
            FROM workspace_zulip_bridge.workspace_outbox
            WHERE entity_type = 'topic' AND entity_uuid = $1
            """,
            topic_uuid,
        )
        assert pending is not None
        assert dict(pending) == {
            "delivery_status": "pending",
            "attempt_count": 1,
            "last_error": "source_not_ready",
        }
        assert not await pool.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'topics'
                  AND entity_uuid = $2
            )
            """,
            provider_uuid,
            topic_uuid,
        )

        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_streams
            SET source_connection_uuid = $1 WHERE uuid = $2
            """,
            connection_uuid,
            stream_uuid,
        )
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_outbox
            SET available_at = clock_timestamp()
            WHERE entity_type = 'topic' AND entity_uuid = $1
            """,
            topic_uuid,
        )
        assert await worker._plan_source_outbox(realm_uuid, generation) == 1
        assert (
            await pool.fetchval(
                """
            SELECT delivery_status
            FROM workspace_zulip_bridge.workspace_outbox
            WHERE entity_type = 'topic' AND entity_uuid = $1
            """,
                topic_uuid,
            )
            == "delivered"
        )
        assert (
            await pool.fetchval(
                """
            SELECT processing_status
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'topics'
              AND entity_uuid = $2
            """,
                provider_uuid,
                topic_uuid,
            )
            == "pending"
        )
    finally:
        await pool.close()


async def _workspace_projection_upgrade_repairs_recent_flag_journal_gap(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000e1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000e2")
    generation = UUID("10000000-0000-0000-0000-0000000000e3")
    old_repair_generation = UUID("10000000-0000-0000-0000-0000000000e4")
    stream_uuid = UUID("10000000-0000-0000-0000-0000000000e5")
    message_uuid = UUID("10000000-0000-0000-0000-0000000000e6")
    flag_uuid = UUID("10000000-0000-0000-0000-0000000000e7")
    token_file = tmp_path / "workspace-recent-flag-repair.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(connection, 84, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status, reconciliation_version,
                    initial_sync_completed_at
                ) VALUES ($1, $2, $3, 'ready', 18, clock_timestamp())
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name, content_hash,
                    source_connection_uuid
                ) VALUES ($1, $2, 'channel', '84', 'Recent flag gap', $3, $4)
                """,
                stream_uuid,
                realm_uuid,
                b"s" * 32,
                connection_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                    uuid, zulip_stream_uuid, zulip_user_uuid, role,
                    membership_kind, content_hash
                ) VALUES ($1, $2, $3, 'member', 'subscriber', $4)
                """,
                stable_stream_binding_uuid(stream_uuid, connection_uuid),
                stream_uuid,
                connection_uuid,
                b"b" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid,
                    zulip_stream_uuid, sender_user_uuid, zulip_message_id,
                    content, content_hash, message_hash, created_at,
                    source_updated_at
                ) VALUES ($1, $2, $3, $4, $3, 8401, 'recent flag gap',
                          $5, $6, clock_timestamp(), clock_timestamp())
                """,
                message_uuid,
                realm_uuid,
                connection_uuid,
                stream_uuid,
                b"m" * 32,
                b"h" * 32,
            )
            flag_timestamp = await connection.fetchval("SELECT clock_timestamp()")
            assert isinstance(flag_timestamp, datetime)
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_message_flags (
                    uuid, realm_uuid, zulip_stream_uuid, message_uuid,
                    zulip_user_uuid, is_read, flags_hash, updated_at
                ) VALUES ($1, $2, $3, $4, $5, true, $6, $7)
                """,
                flag_uuid,
                realm_uuid,
                stream_uuid,
                message_uuid,
                connection_uuid,
                b"f" * 32,
                flag_timestamp,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_repair_cursors (
                    provider_uuid, entity_type, snapshot_generation,
                    source_updated_at, entity_uuid
                ) VALUES ($1, 'message_flags', $2, $3, $4)
                """,
                provider_uuid,
                old_repair_generation,
                flag_timestamp + timedelta(hours=1),
                UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            )
        worker = WorkspaceDiffWorker(
            pool,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_WORKSPACE_WEBSOCKET_URL": (
                        "ws://workspace.test/api/workspace/v1/events/ws"
                    ),
                    "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                    "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                    "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
                }
            ),
        )
        worker.FLAG_REPAIR_SCAN_BATCH_SIZE = 1

        assert await worker.plan() >= 1
        assert (
            await pool.fetchval(
                """
                SELECT reconciliation_version
                FROM workspace_zulip_bridge.workspace_mirror_state
                WHERE provider_uuid = $1
                """,
                provider_uuid,
            )
            == 19
        )
        assert not await pool.fetchval(
            """
            SELECT initial_sync_completed_at IS NOT NULL
            FROM workspace_zulip_bridge.workspace_mirror_state
            WHERE provider_uuid = $1
            """,
            provider_uuid,
        )
        assert (
            await pool.fetchval(
                """
                SELECT processing_status
                FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'message_flags'
                  AND entity_uuid = $2
                """,
                provider_uuid,
                flag_uuid,
            )
            == "pending"
        )
        repair_cursor = await pool.fetchrow(
            """
            SELECT source_updated_at, entity_uuid, snapshot_generation
            FROM workspace_zulip_bridge.sync_repair_cursors
            WHERE provider_uuid = $1 AND entity_type = 'message_flags'
            """,
            provider_uuid,
        )
        assert repair_cursor is not None
        assert repair_cursor["source_updated_at"] == flag_timestamp
        assert repair_cursor["entity_uuid"] == flag_uuid
        assert repair_cursor["snapshot_generation"] != old_repair_generation
    finally:
        await pool.close()


async def _workspace_projection_upgrade_restores_missing_topic_journal(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000c1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000c2")
    generation = UUID("10000000-0000-0000-0000-0000000000c3")
    stream_uuid = UUID("10000000-0000-0000-0000-0000000000c4")
    topic_uuid = UUID("10000000-0000-0000-0000-0000000000c5")
    message_uuid = UUID("10000000-0000-0000-0000-0000000000c6")
    reaction_uuid = UUID("10000000-0000-0000-0000-0000000000c7")
    flag_uuid = UUID("10000000-0000-0000-0000-0000000000c8")
    stream_binding_uuid = UUID("10000000-0000-0000-0000-0000000000c9")
    token_file = tmp_path / "workspace-topic-journal-repair.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(connection, 83, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status, reconciliation_version,
                    initial_sync_completed_at
                ) VALUES ($1, $2, $3, 'ready', 14, clock_timestamp())
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name, content_hash,
                    source_connection_uuid
                ) VALUES ($1, $2, 'channel', '83', 'Missing topic', $3, $4)
                """,
                stream_uuid,
                realm_uuid,
                b"s" * 32,
                connection_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                    uuid, zulip_stream_uuid, zulip_user_uuid, role,
                    membership_kind, content_hash
                ) VALUES ($1, $2, $3, 'member', 'subscriber', $4)
                """,
                stream_binding_uuid,
                stream_uuid,
                connection_uuid,
                b"b" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'Recovered topic', $3)
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_outbox (
                    realm_uuid, entity_type, action, entity_uuid,
                    delivery_status, delivered_at
                ) VALUES ($1, 'topic', 'upsert', $2, 'delivered',
                          clock_timestamp())
                """,
                realm_uuid,
                topic_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid,
                    zulip_stream_uuid, topic_uuid, sender_user_uuid,
                    zulip_message_id, content, content_hash, message_hash,
                    created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, $3, 8301,
                          'reaction repair', $6, $7,
                          clock_timestamp(), clock_timestamp())
                """,
                message_uuid,
                realm_uuid,
                connection_uuid,
                stream_uuid,
                topic_uuid,
                b"m" * 32,
                b"h" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_message_flags (
                    uuid, realm_uuid, zulip_stream_uuid, message_uuid,
                    zulip_user_uuid, flags_hash
                ) VALUES ($1, $2, $3, $4, $5, $6)
                """,
                flag_uuid,
                realm_uuid,
                stream_uuid,
                message_uuid,
                connection_uuid,
                b"f" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_repair_cursors (
                    provider_uuid, entity_type, snapshot_generation,
                    source_updated_at, entity_uuid
                ) VALUES ($1, 'message_flags', $2,
                          clock_timestamp() + interval '1 day', $3)
                """,
                provider_uuid,
                generation,
                UUID("10000000-0000-0000-0000-0000000000ca"),
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_message_reactions (
                    uuid, realm_uuid, message_uuid, zulip_user_uuid,
                    emoji_name, emoji_code, reaction_type
                ) VALUES ($1, $2, $3, $4, 'thumbs_up', '1f44d', 'unicode_emoji')
                """,
                reaction_uuid,
                realm_uuid,
                message_uuid,
                connection_uuid,
            )
        worker = WorkspaceDiffWorker(
            pool,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_WORKSPACE_WEBSOCKET_URL": (
                        "ws://workspace.test/api/workspace/v1/events/ws"
                    ),
                    "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                    "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                    "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
                }
            ),
        )
        worker.FLAG_REPAIR_SCAN_BATCH_SIZE = 1
        assert await worker.plan() >= 3
        assert (
            await pool.fetchval(
                """
            SELECT reconciliation_version
            FROM workspace_zulip_bridge.workspace_mirror_state
            WHERE provider_uuid = $1
            """,
                provider_uuid,
            )
            == 19
        )
        assert (
            await pool.fetchval(
                """
            SELECT processing_status
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'topics'
              AND entity_uuid = $2
            """,
                provider_uuid,
                topic_uuid,
            )
            == "pending"
        )
        assert (
            await pool.fetchval(
                """
            SELECT processing_status
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'message_flags'
              AND entity_uuid = $2
            """,
                provider_uuid,
                flag_uuid,
            )
            == "pending"
        )
        assert not await pool.fetchval(
            """
            SELECT initial_sync_completed_at IS NOT NULL
            FROM workspace_zulip_bridge.workspace_mirror_state
            WHERE provider_uuid = $1
            """,
            provider_uuid,
        )
        repair_cursor = await pool.fetchrow(
            """
            SELECT source_updated_at IS NOT NULL AS advanced, entity_uuid,
                   snapshot_generation
            FROM workspace_zulip_bridge.sync_repair_cursors
            WHERE provider_uuid = $1 AND entity_type = 'message_flags'
            """,
            provider_uuid,
        )
        assert repair_cursor is not None
        assert repair_cursor["advanced"]
        assert repair_cursor["entity_uuid"] == flag_uuid
        assert repair_cursor["snapshot_generation"] != generation
        assert (
            await pool.fetchval(
                """
            SELECT count(*)
            FROM workspace_zulip_bridge.workspace_outbox
            WHERE entity_type = 'topic' AND entity_uuid = $1
              AND delivery_status = 'pending'
            """,
                topic_uuid,
            )
            == 1
        )
        assert (
            await pool.fetchval(
                """
            SELECT processing_status
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'message_reactions'
              AND entity_uuid = $2
            """,
                provider_uuid,
                reaction_uuid,
            )
            == "pending"
        )
    finally:
        await pool.close()


async def _workspace_diff_planner_schedules_unready_entity_graph(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000081")
    project_uuid = UUID("10000000-0000-0000-0000-000000000082")
    generation = UUID("10000000-0000-0000-0000-000000000083")
    stream_uuid = UUID("10000000-0000-0000-0000-000000000084")
    stream_binding_uuid = UUID("10000000-0000-0000-0000-000000000085")
    topic_uuid = UUID("10000000-0000-0000-0000-000000000086")
    topic_binding_uuid = UUID("10000000-0000-0000-0000-000000000087")
    message_uuid = UUID("10000000-0000-0000-0000-000000000088")
    flag_uuid = UUID("10000000-0000-0000-0000-000000000089")
    reaction_uuid = UUID("10000000-0000-0000-0000-000000000090")
    token_file = tmp_path / "workspace-unready-graph.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 81, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    owner_user_uuid, content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', '81', 'Unready graph',
                          $3, $4, $3)
                """,
                stream_uuid,
                realm_uuid,
                user_uuid,
                b"s" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                    uuid, zulip_stream_uuid, zulip_user_uuid, role,
                    membership_kind, content_hash
                ) VALUES ($1, $2, $3, 'member', 'subscriber', $4)
                """,
                stream_binding_uuid,
                stream_uuid,
                user_uuid,
                b"b" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'Unready topic', $3)
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topic_bindings (
                    uuid, zulip_stream_uuid, topic_uuid, zulip_user_uuid,
                    content_hash
                ) VALUES ($1, $2, $3, $4, $5)
                """,
                topic_binding_uuid,
                stream_uuid,
                topic_uuid,
                user_uuid,
                b"q" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid,
                    zulip_stream_uuid, topic_uuid, sender_user_uuid,
                    zulip_message_id, content, content_hash, message_hash,
                    created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, $3, 81, 'unready',
                          $6, $7, clock_timestamp(), clock_timestamp())
                """,
                message_uuid,
                realm_uuid,
                user_uuid,
                stream_uuid,
                topic_uuid,
                b"m" * 32,
                b"h" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_message_flags (
                    uuid, realm_uuid, zulip_stream_uuid, message_uuid,
                    zulip_user_uuid, flags_hash
                ) VALUES ($1, $2, $3, $4, $5, $6)
                """,
                flag_uuid,
                realm_uuid,
                stream_uuid,
                message_uuid,
                user_uuid,
                b"f" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_message_reactions (
                    uuid, realm_uuid, message_uuid, zulip_user_uuid,
                    emoji_name, emoji_code, reaction_type
                ) VALUES ($1, $2, $3, $4, 'heart', '2764', 'unicode_emoji')
                """,
                reaction_uuid,
                realm_uuid,
                message_uuid,
                user_uuid,
            )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_DB_POOL_MIN_SIZE": "1",
                "WZB_DB_POOL_MAX_SIZE": "4",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
                "WZB_WORKSPACE_SYNC_PLAN_BATCH_SIZE": "3",
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)

        assert await worker.plan() == 10
        assert (
            await pool.fetchval(
                """
                SELECT count(*) FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND direction = 'to_workspace'
                  AND processing_status = 'pending'
                """,
                provider_uuid,
            )
            == 8
        )
        assert (
            await pool.fetchval(
                """
                SELECT reconciliation_version
                FROM workspace_zulip_bridge.workspace_mirror_state
                WHERE provider_uuid = $1
                """,
                provider_uuid,
            )
            == 19
        )

        late_message_uuid = UUID("10000000-0000-0000-0000-000000000091")
        async with pool.acquire() as connection, connection.transaction():
            cursor_row = await connection.fetchrow(
                """
                UPDATE workspace_zulip_bridge.sync_plan_cursors
                SET source_updated_at = clock_timestamp(), entity_uuid = $2
                WHERE provider_uuid = $1
                  AND entity_type IN ('messages', 'topics')
                RETURNING source_updated_at, entity_type
                """,
                provider_uuid,
                message_uuid,
            )
            assert cursor_row is not None
            message_cursor = cursor_row["source_updated_at"]
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid,
                    zulip_stream_uuid, topic_uuid, sender_user_uuid,
                    zulip_message_id, content, content_hash, message_hash,
                    created_at, source_updated_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $3, 82, 'late commit',
                          $6, $7, clock_timestamp(), clock_timestamp(),
                          $8)
                """,
                late_message_uuid,
                realm_uuid,
                user_uuid,
                stream_uuid,
                topic_uuid,
                b"l" * 32,
                b"k" * 32,
                message_cursor - timedelta(seconds=1),
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_outbox (
                    realm_uuid, entity_type, action, entity_uuid
                ) VALUES ($1, 'message', 'upsert', $2)
                """,
                realm_uuid,
                late_message_uuid,
            )

        assert await worker.plan() == 3
        late_diff = await pool.fetchrow(
            """
            SELECT processing_status, source_hash
            FROM workspace_zulip_bridge.sync_diffs
            WHERE provider_uuid = $1 AND entity_type = 'messages'
              AND entity_uuid = $2
            """,
            provider_uuid,
            late_message_uuid,
        )
        assert late_diff is not None
        assert late_diff["processing_status"] == "pending"
        assert late_diff["source_hash"] == b"l" * 32
        assert (
            await pool.fetchval(
                """
                SELECT delivery_status
                FROM workspace_zulip_bridge.workspace_outbox
                WHERE realm_uuid = $1 AND entity_type = 'message'
                  AND entity_uuid = $2
                """,
                realm_uuid,
                late_message_uuid,
            )
            == "delivered"
        )

        backlog_message_uuids = (
            UUID("10000000-0000-0000-0000-000000000092"),
            UUID("10000000-0000-0000-0000-000000000093"),
            UUID("10000000-0000-0000-0000-000000000094"),
        )
        async with pool.acquire() as connection, connection.transaction():
            await connection.executemany(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid,
                    zulip_stream_uuid, topic_uuid, sender_user_uuid,
                    zulip_message_id, content, content_hash, message_hash,
                    created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, $3, $6, 'outbox backlog',
                          $7, $8, clock_timestamp(), clock_timestamp())
                """,
                [
                    (
                        entity_uuid,
                        realm_uuid,
                        user_uuid,
                        stream_uuid,
                        topic_uuid,
                        83 + offset,
                        bytes([109 + offset]) * 32,
                        bytes([104 + offset]) * 32,
                    )
                    for offset, entity_uuid in enumerate(backlog_message_uuids)
                ],
            )
            await connection.executemany(
                """
                INSERT INTO workspace_zulip_bridge.workspace_outbox (
                    realm_uuid, entity_type, action, entity_uuid
                ) VALUES ($1, 'message', 'upsert', $2)
                """,
                [(realm_uuid, entity_uuid) for entity_uuid in backlog_message_uuids],
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_outbox (
                    realm_uuid, entity_type, action, entity_uuid
                ) VALUES ($1, 'message_flag', 'upsert', $2),
                         ($1, 'message_reaction', 'upsert', $3)
                """,
                realm_uuid,
                flag_uuid,
                reaction_uuid,
            )

        assert await worker.plan() == 3
        delivered_types = await pool.fetch(
            """
            SELECT entity_type, count(*) AS count
            FROM workspace_zulip_bridge.workspace_outbox
            WHERE realm_uuid = $1 AND delivery_status = 'delivered'
              AND entity_type IN ('message', 'message_flag', 'message_reaction')
              AND entity_uuid = ANY($2::uuid[])
            GROUP BY entity_type
            ORDER BY entity_type
            """,
            realm_uuid,
            [*backlog_message_uuids, flag_uuid, reaction_uuid],
        )
        assert [tuple(row.values()) for row in delivered_types] == [
            ("message", 1),
            ("message_flag", 1),
            ("message_reaction", 1),
        ]
    finally:
        await pool.close()


async def _workspace_diff_worker_round_trip(dsn: str, tmp_path: Path) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000021")
    project_uuid = UUID("10000000-0000-0000-0000-000000000022")
    generation = UUID("10000000-0000-0000-0000-000000000023")
    token_file = tmp_path / "workspace-sync.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 10, 400)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET avatar_url = '/user_avatars/10/avatar.png',
                    profile_hash = decode(repeat('01', 32), 'hex')
                WHERE uuid = $1
                """,
                user_uuid,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                stable_realm_uuid(ENDPOINT),
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_DB_POOL_MIN_SIZE": "1",
                "WZB_DB_POOL_MAX_SIZE": "4",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                "WZB_WORKSPACE_WEBSOCKET_URL": "ws://workspace.test/api/workspace/v1/events/ws",
                "WZB_WORKSPACE_API_URL": "http://workspace.test/api/workspace/v1",
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)
        live_worker = WorkspaceDiffWorker(pool, settings, delivery_priority=0)
        history_worker = WorkspaceDiffWorker(pool, settings, delivery_priority=1)
        assert await worker.plan() >= 1
        requests: list[dict[str, object]] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["authorization"] == "Bearer integration-token"
            body = json.loads(request.content)
            requests.append(body)
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "type": item["type"],
                            "uuid": item["uuid"],
                            "status": "created",
                        }
                        for item in body["operations"]
                    ]
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer integration-token"},
        ) as client:
            assert await worker.process_once(client) == 1
            assert await worker.plan() == 0
            unchanged_before = await pool.fetchrow(
                """
                SELECT updated_at, processing_status, attempt_count
                FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'users'
                  AND entity_uuid = $2
                """,
                provider_uuid,
                user_uuid,
            )
            await pool.execute(
                """
                UPDATE workspace_zulip_bridge.sync_plan_cursors
                SET source_updated_at = NULL, entity_uuid = NULL
                WHERE provider_uuid = $1 AND entity_type = 'users'
                """,
                provider_uuid,
            )
            assert await worker.plan() == 1
            unchanged_after = await pool.fetchrow(
                """
                SELECT updated_at, processing_status, attempt_count
                FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'users'
                  AND entity_uuid = $2
                """,
                provider_uuid,
                user_uuid,
            )
            assert unchanged_after == unchanged_before
            assert await worker.plan() == 0
            await pool.execute(
                """
                UPDATE workspace_zulip_bridge.sync_diffs
                SET direction = 'to_workspace', processing_status = 'pending',
                    delivery_priority = 0,
                    source_updated_at = clock_timestamp(),
                    available_at = clock_timestamp()
                WHERE provider_uuid = $1 AND entity_type = 'users'
                  AND entity_uuid = $2
                """,
                provider_uuid,
                user_uuid,
            )
            assert await worker.process_once(client) == 1
            assert len(requests) == 1
            assert (
                await pool.fetchval(
                    """
                SELECT last_error FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'users'
                  AND entity_uuid = $2
                """,
                    provider_uuid,
                    user_uuid,
                )
                == "equivalent"
            )
            await pool.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    direction, source_updated_at
                ) VALUES ($1, 'users', $2, $3, 'to_workspace',
                          clock_timestamp() - interval '1 year')
                """,
                provider_uuid,
                UUID("10000000-0000-0000-0000-000000000024"),
                stable_realm_uuid(ENDPOINT),
            )
            await pool.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET full_name = 'Live User 10',
                    profile_hash = decode(repeat('02', 32), 'hex')
                WHERE uuid = $1
                """,
                user_uuid,
            )
            assert await worker.plan() == 1
            await pool.execute(
                """
                UPDATE workspace_zulip_bridge.sync_diffs
                SET delivery_priority = 0
                WHERE provider_uuid = $1 AND entity_type = 'users'
                  AND entity_uuid = $2
                """,
                provider_uuid,
                user_uuid,
            )
            assert await live_worker.process_once(client) == 1
            assert requests[-1]["delivery_class"] == "live"
            assert await history_worker.process_once(client) == 1
            assert len(requests) == 2
            assert (
                await pool.fetchval(
                    """
                SELECT last_error FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'users'
                  AND entity_uuid = $2
                """,
                    provider_uuid,
                    UUID("10000000-0000-0000-0000-000000000024"),
                )
                == "equivalent"
            )
            other_endpoint = "https://other-zulip.example.test"
            other_realm_uuid = stable_realm_uuid(other_endpoint)
            other_user_uuid = stable_user_uuid(other_endpoint, 99)
            await pool.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_realms (
                    uuid, identity_key, endpoint, workspace_project_id,
                    workspace_provider_uuid
                ) VALUES ($1, $2, $2, $3, $4)
                """,
                other_realm_uuid,
                other_endpoint,
                UUID("10000000-0000-0000-0000-000000000027"),
                UUID("10000000-0000-0000-0000-000000000028"),
            )
            await pool.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_users (
                    uuid, realm_uuid, zulip_user_id, login, full_name, role
                ) VALUES ($1, $2, 99, 'other@example.test', 'Other user', 400)
                """,
                other_user_uuid,
                other_realm_uuid,
            )
            await pool.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_connections (
                    uuid, realm_uuid, zulip_user_uuid, login, api_key,
                    lifecycle_status
                ) VALUES ($1, $2, $1, 'other@example.test', 'test-key', 'init')
                """,
                other_user_uuid,
                other_realm_uuid,
            )
            await pool.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET lifecycle_status = 'active'
                WHERE uuid = $1
                """,
                user_uuid,
            )
            assert await worker.process_once(client) == 0
            assert await worker._complete_initial_sync()
            assert await pool.fetchval(
                """
                SELECT initial_sync_completed_at IS NOT NULL
                FROM workspace_zulip_bridge.workspace_mirror_state
                WHERE provider_uuid = $1
                """,
                provider_uuid,
            )
            native_message_uuid = UUID("10000000-0000-0000-0000-000000000025")
            native_stream_uuid = UUID("10000000-0000-0000-0000-000000000026")
            native_data = {
                "uuid": str(native_message_uuid),
                "stream_uuid": str(native_stream_uuid),
                "payload": {"kind": "markdown", "content": "Native only"},
            }
            await pool.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_messages (
                    provider_uuid, snapshot_generation, uuid,
                    workspace_project_id, content_hash, source_updated_at, data
                ) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), $6::jsonb)
                """,
                provider_uuid,
                generation,
                native_message_uuid,
                project_uuid,
                b"n" * 32,
                json.dumps(native_data),
            )
            await pool.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    partition_key, direction, processing_status, target_hash,
                    source_updated_at, target_updated_at
                ) VALUES ($1, 'messages', $2, $3, $4, 'to_zulip', 'blocked',
                          $5, clock_timestamp(), clock_timestamp())
                """,
                provider_uuid,
                native_message_uuid,
                stable_realm_uuid(ENDPOINT),
                native_stream_uuid,
                b"n" * 32,
            )
            assert await worker.plan() == 0
            assert (
                await pool.fetchval(
                    """
                SELECT processing_status
                FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'messages'
                  AND entity_uuid = $2
                """,
                    provider_uuid,
                    native_message_uuid,
                )
                == "skipped"
            )
            await pool.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET full_name = 'Updated User 10',
                    profile_hash = decode(repeat('03', 32), 'hex')
                WHERE uuid = $1
                """,
                user_uuid,
            )
            await worker.plan()
            assert await worker.process_once(client) == 1
        assert len(requests) == 3
        assert requests[0]["delivery_class"] == "backfill"
        assert requests[1]["delivery_class"] == "live"
        assert requests[2]["delivery_class"] == "live"
        operation = requests[0]["operations"][0]  # type: ignore[index]
        assert operation["type"] == "users"  # type: ignore[index]
        assert operation["data"]["display_name"] == "User 10"  # type: ignore[index]
        assert operation["data"]["avatar"] == (  # type: ignore[index]
            "urn:url:https://zulip.example.test/user_avatars/10/avatar.png"
        )
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.workspace_users"
            )
            == 1
        )
        assert (
            await pool.fetchval(
                """
                SELECT processing_status FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'users'
                  AND entity_uuid = $2
                """,
                provider_uuid,
                user_uuid,
            )
            == "applied"
        )
    finally:
        await pool.close()


async def _workspace_diff_message_outbox_ignores_source_timestamp(
    dsn: str,
    tmp_path: Path,
) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000031")
    project_uuid = UUID("10000000-0000-0000-0000-000000000032")
    generation = UUID("10000000-0000-0000-0000-000000000033")
    token_file = tmp_path / "workspace-message-cursor.token"
    token_file.write_text("integration-token")
    try:
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(connection, 31, 400)
            realm_uuid = stable_realm_uuid(ENDPOINT)
            stream_uuid = stable_chat_uuid(ENDPOINT, "channel:31")
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_realms
                SET workspace_project_id = $2, workspace_provider_uuid = $3
                WHERE uuid = $1
                """,
                realm_uuid,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                    provider_uuid, workspace_project_id, active_generation,
                    bootstrap_status
                ) VALUES ($1, $2, $3, 'ready')
                """,
                provider_uuid,
                project_uuid,
                generation,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name, content_hash,
                    source_connection_uuid, history_loaded_at
                ) VALUES ($1, $2, 'channel', 'channel:31', 'Cursor', $3, $4,
                          clock_timestamp())
                """,
                stream_uuid,
                realm_uuid,
                b"s" * 32,
                connection_uuid,
            )

        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_DB_POOL_MIN_SIZE": "1",
                "WZB_DB_POOL_MAX_SIZE": "4",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                "WZB_WORKSPACE_WEBSOCKET_URL": (
                    "ws://workspace.test/api/workspace/v1/events/ws"
                ),
                "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            }
        )
        worker = WorkspaceDiffWorker(pool, settings)
        sender_uuid = stable_user_uuid(ENDPOINT, 31)
        first_uuid = stable_message_uuid(ENDPOINT, 310)
        second_uuid = stable_message_uuid(ENDPOINT, 311)
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    sender_user_uuid, zulip_message_id, content, content_hash,
                    message_hash, created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, 310, 'new source time', $6,
                          $7, clock_timestamp(),
                          clock_timestamp() + interval '1 day')
                """,
                first_uuid,
                realm_uuid,
                connection_uuid,
                stream_uuid,
                sender_uuid,
                b"a" * 32,
                b"a" * 32,
            )
        assert await worker.plan() >= 1
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    sender_user_uuid, zulip_message_id, content, content_hash,
                    message_hash, created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, 311, 'old source time', $6,
                          $7, clock_timestamp(),
                          clock_timestamp() - interval '1 year')
                """,
                second_uuid,
                realm_uuid,
                connection_uuid,
                stream_uuid,
                sender_uuid,
                b"b" * 32,
                b"b" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_outbox (
                    realm_uuid, entity_type, action, entity_uuid
                ) VALUES ($1, 'message', 'upsert', $2)
                """,
                realm_uuid,
                second_uuid,
            )
        assert await worker.plan() >= 1
        assert await pool.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1 AND entity_type = 'messages'
                  AND entity_uuid = $2 AND direction = 'to_workspace'
            )
            """,
            provider_uuid,
            second_uuid,
        )
    finally:
        await pool.close()


async def _workspace_websocket_round_trip(dsn: str, tmp_path: Path) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-000000000011")
    project_uuid = UUID("10000000-0000-0000-0000-000000000012")
    generation = UUID("10000000-0000-0000-0000-000000000013")
    token_file = tmp_path / "workspace.token"
    token_file.write_text("integration-token")

    def frame(version: int) -> dict[str, object]:
        return {
            "schema_version": 1,
            "uuid": f"20000000-0000-0000-0000-{version:012d}",
            "epoch_version": version,
            "project_id": str(project_uuid),
            "user_uuid": str(provider_uuid),
            "object_type": "message",
            "action": "updated",
            "payload": {
                "kind": "message.updated",
                "uuid": f"30000000-0000-0000-0000-{version:012d}",
            },
        }

    async def handler(websocket: ServerConnection) -> None:
        assert websocket.subprotocol == "workspace.events.v1"
        protocols = websocket.request.headers["Sec-WebSocket-Protocol"]
        assert "bearer.integration-token" in protocols
        await websocket.send(json.dumps(frame(1)))
        await websocket.send(
            json.dumps(
                {
                    "type": "ready",
                    "epoch_generation": str(generation),
                    "epoch_version": 1,
                }
            )
        )
        await websocket.send(json.dumps(frame(2)))

    try:
        async with serve(
            handler,
            "127.0.0.1",
            0,
            subprotocols=["workspace.events.v1"],
        ) as server:
            port = server.sockets[0].getsockname()[1]
            settings = Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_DB_POOL_MIN_SIZE": "1",
                    "WZB_DB_POOL_MAX_SIZE": "4",
                    "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
                    "WZB_WORKSPACE_WEBSOCKET_URL": f"ws://127.0.0.1:{port}/events/ws",
                    "WZB_WORKSPACE_PROJECT_ID": str(project_uuid),
                    "WZB_WORKSPACE_PROVIDER_UUID": str(provider_uuid),
                    "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
                }
            )
            receiver = WorkspaceEventReceiver(pool, settings)
            cursor = await receiver._store.cursor(provider_uuid, project_uuid)
            with pytest.raises(ConnectionClosedOK):
                await receiver._receive(cursor)

        stored = await pool.fetch(
            """
            SELECT epoch_generation, epoch_version
            FROM workspace_zulip_bridge.workspace_events
            ORDER BY epoch_version
            """
        )
        assert [(row["epoch_generation"], row["epoch_version"]) for row in stored] == [
            (generation, 1),
            (generation, 2),
        ]
        cursor = await receiver._store.cursor(provider_uuid, project_uuid)
        assert cursor.epoch_generation == generation
        assert cursor.last_epoch_version == 2
    finally:
        await pool.close()


async def _concurrent_message_flag_events_merge_independent_fields(
    dsn: str,
) -> None:
    pool = await _pool(dsn)
    stream_uuid = UUID("10000000-0000-0000-0000-0000000000d1")
    queue_id = "concurrent-flags"
    message_ids = tuple(range(9000, 9020))
    try:
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(
                connection,
                140,
                400,
                queue_id=queue_id,
                status="streaming",
            )
            realm_uuid = stable_realm_uuid(ENDPOINT)
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    owner_user_uuid, source_connection_uuid, content_hash
                ) VALUES ($1, $2, 'channel', '140', 'Concurrent flags',
                          $3, $3, $4)
                """,
                stream_uuid,
                realm_uuid,
                connection_uuid,
                b"s" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_stream_bindings (
                    uuid, zulip_stream_uuid, zulip_user_uuid, role,
                    membership_kind, content_hash
                ) VALUES ($1, $2, $3, 'member', 'subscriber', $4)
                """,
                stable_stream_binding_uuid(stream_uuid, connection_uuid),
                stream_uuid,
                connection_uuid,
                b"b" * 32,
            )
            await connection.executemany(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid,
                    zulip_stream_uuid, sender_user_uuid, zulip_message_id,
                    content, content_hash, message_hash, created_at,
                    source_updated_at
                ) VALUES ($1, $2, $3, $4, $3, $5, 'concurrent flags',
                          $6, $7, clock_timestamp(), clock_timestamp())
                """,
                [
                    (
                        stable_message_uuid(ENDPOINT, message_id),
                        realm_uuid,
                        connection_uuid,
                        stream_uuid,
                        message_id,
                        b"c" * 32,
                        b"m" * 32,
                    )
                    for message_id in message_ids
                ],
            )

        store = EventStore(pool)
        await asyncio.gather(
            store.apply_message_flags(
                connection_uuid,
                queue_id,
                message_ids,
                "is_read",
                True,
            ),
            store.apply_message_flags(
                connection_uuid,
                queue_id,
                message_ids,
                "is_starred",
                True,
            ),
        )
        rows = await pool.fetch(
            """
            SELECT is_read, is_starred
            FROM workspace_zulip_bridge.zulip_message_flags
            WHERE message_uuid = ANY($1::uuid[])
            """,
            [stable_message_uuid(ENDPOINT, message_id) for message_id in message_ids],
        )
        assert len(rows) == len(message_ids)
        assert all(row["is_read"] and row["is_starred"] for row in rows)
        assert await pool.fetchval(
            """
                SELECT count(*)
                FROM workspace_zulip_bridge.workspace_outbox
                WHERE entity_type = 'message_flag'
                  AND delivery_status = 'pending'
                """
        ) == len(message_ids)
    finally:
        await pool.close()


async def _insert_user(
    connection: asyncpg.Connection,
    user_id: int,
    role: int,
    *,
    api_key: str | None = "api-key-placeholder",
    queue_id: str | None = None,
    status: str = "init",
    is_bot: bool = False,
    connection_uuid: UUID | None = None,
) -> UUID:
    user_uuid = stable_user_uuid(ENDPOINT, user_id)
    connection_uuid = connection_uuid or user_uuid
    await connection.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_realms
            (uuid, identity_key, endpoint)
        VALUES ($1, $2, $2)
        ON CONFLICT (uuid) DO NOTHING
        """,
        stable_realm_uuid(ENDPOINT),
        ENDPOINT,
    )
    await connection.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_users
            (uuid, realm_uuid, zulip_user_id, login, full_name, role, is_bot)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        user_uuid,
        stable_realm_uuid(ENDPOINT),
        user_id,
        f"user-{user_id}@example.test",
        f"User {user_id}",
        role,
        is_bot,
    )
    if api_key is not None:
        await connection.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_connections
                (uuid, realm_uuid, zulip_user_uuid, login, api_key,
                 queue_id, last_event_id, lifecycle_status)
            VALUES ($1, $2, $3, $4, $5, $6, 0, $7)
            """,
            connection_uuid,
            stable_realm_uuid(ENDPOINT),
            user_uuid,
            f"user-{user_id}@example.test",
            api_key,
            queue_id,
            status,
        )
    return connection_uuid


def _catalog(
    own_user_id: int,
    channels: list[tuple[int, str]],
    counts: dict[str, int],
    first_visible_message_ids: dict[int, int | None] | None = None,
    colors: dict[int, str] | None = None,
):
    role = {10: 100, 20: 200, 30: 200, 99: 100}.get(own_user_id, 400)
    builder = ChatCatalogBuilder(own_user_id, f"User {own_user_id}", role)
    builder.add_subscriptions(
        [
            {
                "stream_id": stream_id,
                "name": name,
                **(
                    {"color": colors[stream_id]}
                    if colors and stream_id in colors
                    else {}
                ),
            }
            for stream_id, name in channels
        ],
        first_visible_message_ids=first_visible_message_ids,
    )
    return builder.build(counts)


def test_directory_uses_stable_user_ids_and_keeps_bots_without_connections() -> None:
    asyncio.run(_directory_round_trip(_dsn()))


def test_user_identity_allows_distinct_connection_uuid() -> None:
    asyncio.run(_distinct_connection_identity_round_trip(_dsn()))


async def _distinct_connection_identity_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    connection_uuid = UUID("00000000-0000-4000-8000-000000000010")
    try:
        async with pool.acquire() as connection:
            returned_uuid = await _insert_user(
                connection,
                10,
                400,
                connection_uuid=connection_uuid,
            )
        assert returned_uuid == connection_uuid

        store = EventStore(pool)
        users = await store.list_users()
        assert [user.uuid for user in users] == [connection_uuid]
        assert await store.set_user_identity(
            connection_uuid,
            ENDPOINT,
            10,
            "Current Name",
            200,
        )
        assert not await store.set_user_identity(
            connection_uuid,
            ENDPOINT,
            11,
            "Wrong User",
            100,
        )

        async with pool.acquire() as connection:
            identity = await connection.fetchrow(
                "SELECT full_name, role "
                "FROM workspace_zulip_bridge.zulip_users "
                "WHERE uuid = $1",
                stable_user_uuid(ENDPOINT, 10),
            )
        assert identity is not None
        assert identity["full_name"] == "Current Name"
        assert identity["role"] == 200
    finally:
        await pool.close()


async def _directory_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(connection, 10, 400)
        store = EventStore(pool)
        result = await store.store_user_directory(
            ENDPOINT,
            [
                ZulipDirectoryUser(
                    user_id=10,
                    login="masked-login@example.test",
                    full_name="Renamed User",
                    role=200,
                    disabled=True,
                    is_bot=False,
                ),
                ZulipDirectoryUser(
                    user_id=99,
                    login="bot@example.test",
                    full_name="Bot",
                    role=400,
                    disabled=False,
                    is_bot=True,
                ),
            ],
        )
        assert result == UserDirectoryWrite(users=2, bots=1, changed=2)
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT zulip_user.uuid, zulip_user.login, zulip_user.full_name,
                       zulip_user.role, zulip_user.disabled, zulip_user.is_bot,
                       zulip_user.profile_hash, zulip_user.updated_at,
                       connection.api_key
                FROM workspace_zulip_bridge.zulip_users AS zulip_user
                LEFT JOIN workspace_zulip_bridge.zulip_connections AS connection
                  ON connection.zulip_user_uuid = zulip_user.uuid
                ORDER BY zulip_user.zulip_user_id
                """
            )
        assert len(rows) == 2
        assert rows[0]["uuid"] == user_uuid
        assert rows[0]["login"] == "masked-login@example.test"
        assert rows[0]["full_name"] == "Renamed User"
        assert rows[0]["role"] == 200
        assert rows[0]["disabled"]
        assert not rows[0]["is_bot"]
        assert rows[0]["api_key"] == "api-key-placeholder"
        assert rows[1]["uuid"] == stable_user_uuid(ENDPOINT, 99)
        assert rows[1]["is_bot"]
        assert rows[1]["api_key"] is None
        original_profile_hash = rows[0].get("profile_hash")
        original_updated_at = rows[0].get("updated_at")
        assert original_profile_hash is not None

        changed = await store.store_user_directory(
            ENDPOINT,
            [
                ZulipDirectoryUser(
                    user_id=10,
                    login="masked-login@example.test",
                    full_name="Live Renamed User",
                    role=200,
                    disabled=True,
                    is_bot=False,
                )
            ],
        )
        assert changed.changed == 1
        updated = await pool.fetchrow(
            """
            SELECT profile_hash, updated_at
            FROM workspace_zulip_bridge.zulip_users
            WHERE uuid = $1
            """,
            user_uuid,
        )
        assert updated is not None
        assert updated["profile_hash"] != original_profile_hash
        assert updated["updated_at"] > original_updated_at
    finally:
        await pool.close()


def test_scheduler_uses_role_then_stable_uuid() -> None:
    asyncio.run(_scheduler_round_trip(_dsn()))


def test_scheduler_reassigns_large_histories_in_bounded_batches() -> None:
    asyncio.run(_scheduler_batched_reassignment_round_trip(_dsn()))


def test_scheduler_uses_completed_catalogs_while_another_account_is_filling() -> None:
    asyncio.run(_scheduler_incomplete_catalog_round_trip(_dsn()))


def test_scheduler_does_not_deadlock_with_concurrent_reconcile_request() -> None:
    asyncio.run(_scheduler_concurrent_request_round_trip(_dsn()))


async def _scheduler_concurrent_request_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
        assert (
            await store.store_chat_catalog(
                owner_uuid, "queue-owner", _catalog(10, [(7, "Shared")], {})
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_users "
            "SET disabled = true WHERE uuid = $1",
            owner_uuid,
        )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_schedule_reconcile_state "
            "SET requested_generation = requested_generation + 1 "
            "WHERE singleton"
        )

        async with pool.acquire() as blocker, blocker.transaction():
            await blocker.fetchval(
                "SELECT 1 FROM workspace_zulip_bridge.zulip_streams "
                "WHERE uuid = $1 FOR UPDATE",
                stream_uuid,
            )
            reconcile = asyncio.create_task(store.reconcile_chat_schedules())
            for _ in range(100):
                waiting = await pool.fetchval(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_stat_activity "
                    "WHERE datname = current_database() "
                    "AND pid <> pg_backend_pid() "
                    "AND wait_event_type = 'Lock' "
                    "AND query LIKE '%WITH invalid AS MATERIALIZED%')"
                )
                if waiting:
                    break
                await asyncio.sleep(0.01)
            else:
                reconcile.cancel()
                await asyncio.gather(reconcile, return_exceptions=True)
                raise AssertionError("scheduler did not wait on the locked stream")

            await asyncio.wait_for(
                blocker.execute(
                    "UPDATE workspace_zulip_bridge.zulip_schedule_reconcile_state "
                    "SET requested_generation = requested_generation + 1 "
                    "WHERE singleton"
                ),
                timeout=0.5,
            )

        result = await asyncio.wait_for(reconcile, timeout=2)
        assert result.invalidated == 1
        assert await store.chat_schedule_reconciliation_requested()
    finally:
        await pool.close()


async def _scheduler_incomplete_catalog_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            ready_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-ready", status="filling"
            )
            await _insert_user(
                connection, 20, 200, queue_id="queue-filling", status="filling"
            )
        assert (
            await store.store_chat_catalog(
                ready_uuid, "queue-ready", _catalog(10, [(7, "Shared")], {})
            )
        ).activated
        assert await store.chat_schedule_reconciliation_requested()
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
        user_uuid = stable_user_uuid(ENDPOINT, 10)
        topic_uuid = stable_topic_uuid(stream_uuid, "General")
        topic_binding_uuid = stable_topic_binding_uuid(topic_uuid, user_uuid)
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash, updated_at
                ) VALUES ($1, $2, 'General', $3, '2000-01-01T00:00:00Z')
                """,
                topic_uuid,
                stream_uuid,
                b"t" * 32,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topic_bindings (
                    uuid, zulip_stream_uuid, topic_uuid, zulip_user_uuid,
                    content_hash, updated_at
                ) VALUES ($1, $2, $3, $4, $5, '2000-01-01T00:00:00Z')
                """,
                topic_binding_uuid,
                stream_uuid,
                topic_uuid,
                user_uuid,
                b"b" * 32,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_stream_bindings
                SET updated_at = '2000-01-01T00:00:00Z'
                WHERE zulip_stream_uuid = $1
                """,
                stream_uuid,
            )

        reconciled = await store.reconcile_chat_schedules()
        assert reconciled.assigned == 1
        assert await store.chat_schedule_reconciliation_requested()
        settled = await store.reconcile_chat_schedules()
        assert (settled.invalidated, settled.assigned) == (0, 0)
        assert not await store.chat_schedule_reconciliation_requested()
        assert (
            await pool.fetchval(
                "SELECT source_connection_uuid "
                "FROM workspace_zulip_bridge.zulip_streams "
                "WHERE chat_key = 'channel:7'"
            )
            == ready_uuid
        )
        touched = await pool.fetchrow(
            """
            SELECT
                (SELECT min(updated_at)
                 FROM workspace_zulip_bridge.zulip_stream_bindings
                 WHERE zulip_stream_uuid = $1) AS binding_updated_at,
                (SELECT updated_at
                 FROM workspace_zulip_bridge.zulip_topics
                 WHERE uuid = $2) AS topic_updated_at,
                (SELECT updated_at
                 FROM workspace_zulip_bridge.zulip_topic_bindings
                 WHERE uuid = $3) AS topic_binding_updated_at
            """,
            stream_uuid,
            topic_uuid,
            topic_binding_uuid,
        )
        assert touched is not None
        old_timestamp = datetime(2000, 1, 1, tzinfo=UTC)
        assert touched["binding_updated_at"] > old_timestamp
        assert touched["topic_updated_at"] > old_timestamp
        assert touched["topic_binding_updated_at"] > old_timestamp
    finally:
        await pool.close()


async def _scheduler_batched_reassignment_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        store.SCHEDULE_MESSAGE_BATCH_SIZE = 1
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
            member_uuid = await _insert_user(
                connection, 20, 400, queue_id="queue-member", status="filling"
            )
        assert (
            await store.store_chat_catalog(
                owner_uuid, "queue-owner", _catalog(10, [(7, "Shared")], {})
            )
        ).activated
        assert (
            await store.store_chat_catalog(
                member_uuid, "queue-member", _catalog(20, [(7, "Shared")], {})
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
        async with pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    sender_user_uuid, zulip_message_id, content, content_hash,
                    message_hash, created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $3, $5, 'history', $6, $6,
                          clock_timestamp(), clock_timestamp())
                """,
                [
                    (
                        stable_message_uuid(ENDPOINT, message_id),
                        stable_realm_uuid(ENDPOINT),
                        owner_uuid,
                        stream_uuid,
                        message_id,
                        bytes([message_id]) * 32,
                    )
                    for message_id in (1, 2)
                ],
            )
            await connection.execute(
                "UPDATE workspace_zulip_bridge.zulip_users "
                "SET disabled = true WHERE uuid = $1",
                owner_uuid,
            )

        first = await store.reconcile_chat_schedules()
        assert (first.invalidated, first.assigned) == (1, 0)
        assert await pool.fetchval(
            "SELECT source_connection_uuid IS NULL "
            "FROM workspace_zulip_bridge.zulip_streams WHERE uuid = $1",
            stream_uuid,
        )
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.zulip_messages "
                "WHERE zulip_stream_uuid = $1 AND source_connection_uuid = $2",
                stream_uuid,
                member_uuid,
            )
            == 1
        )

        second = await store.reconcile_chat_schedules()
        assert (second.invalidated, second.assigned) == (0, 1)
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.zulip_messages "
                "WHERE zulip_stream_uuid = $1 AND source_connection_uuid = $2",
                stream_uuid,
                member_uuid,
            )
            == 2
        )
    finally:
        await pool.close()


def test_bot_connections_are_not_started_or_scheduled() -> None:
    asyncio.run(_bot_connection_round_trip(_dsn()))


async def _bot_connection_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            human_uuid = await _insert_user(
                connection, 20, 200, queue_id="queue-human", status="filling"
            )
            bot_uuid = await _insert_user(
                connection,
                99,
                100,
                queue_id="queue-bot",
                status="filling",
                is_bot=True,
            )

        assert (
            await store.store_chat_catalog(
                human_uuid, "queue-human", _catalog(20, [(7, "Shared")], {})
            )
        ).activated
        assert (
            await store.store_chat_catalog(
                bot_uuid, "queue-bot", _catalog(99, [(7, "Shared")], {})
            )
        ).activated

        assert [user.uuid for user in await store.list_users()] == [human_uuid]
        assert (await store.reconcile_chat_schedules()).assigned == 1
        async with pool.acquire() as connection:
            assert (
                await connection.fetchval(
                    "SELECT source_connection_uuid "
                    "FROM workspace_zulip_bridge.zulip_streams "
                    "WHERE chat_key = 'channel:7'"
                )
                == human_uuid
            )
    finally:
        await pool.close()


def test_lifecycle_updates_require_current_queue() -> None:
    asyncio.run(_lifecycle_queue_round_trip(_dsn()))


async def _lifecycle_queue_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(
                connection, 10, 400, queue_id="queue-current", status="streaming"
            )

        assert not await store.begin_catalog_fill(user_uuid, "queue-stale")
        assert await store.begin_catalog_fill(user_uuid, "queue-current")
        assert not await store.set_user_status(user_uuid, "queue-stale", "active")
        assert await store.set_user_status(user_uuid, "queue-current", "scheduling")

        async with pool.acquire() as connection:
            assert (
                await connection.fetchval(
                    "SELECT lifecycle_status "
                    "FROM workspace_zulip_bridge.zulip_connections "
                    "WHERE uuid = $1",
                    user_uuid,
                )
                == "scheduling"
            )
    finally:
        await pool.close()


async def _scheduler_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        admin_a_user_uuid = stable_user_uuid(ENDPOINT, 20)
        admin_b_user_uuid = stable_user_uuid(ENDPOINT, 30)
        low_connection_uuid = UUID("00000000-0000-4000-8000-000000000001")
        high_connection_uuid = UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")
        if admin_a_user_uuid < admin_b_user_uuid:
            admin_a_connection_uuid = high_connection_uuid
            admin_b_connection_uuid = low_connection_uuid
        else:
            admin_a_connection_uuid = low_connection_uuid
            admin_b_connection_uuid = high_connection_uuid
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
            admin_a_uuid = await _insert_user(
                connection,
                20,
                200,
                queue_id="queue-admin-a",
                status="filling",
                connection_uuid=admin_a_connection_uuid,
            )
            admin_b_uuid = await _insert_user(
                connection,
                30,
                200,
                queue_id="queue-admin-b",
                status="filling",
                connection_uuid=admin_b_connection_uuid,
            )

        owner_catalog = _catalog(
            10,
            [(7, "Shared")],
            {"channel:7": 1},
            {7: 101},
            {7: "#112233"},
        )
        admin_a_catalog = _catalog(
            20,
            [(7, "Shared"), (8, "Admins"), (9, "Tie")],
            {"channel:7": 100, "channel:8": 5, "channel:9": 2},
            colors={7: "#445566"},
        )
        admin_b_catalog = _catalog(
            30,
            [(7, "Shared"), (8, "Admins"), (9, "Tie")],
            {"channel:7": 1000, "channel:8": 9, "channel:9": 2},
            colors={7: "#778899"},
        )
        assert (
            await store.store_chat_catalog(owner_uuid, "queue-owner", owner_catalog)
        ).activated
        assert (
            await store.store_chat_catalog(
                admin_a_uuid, "queue-admin-a", admin_a_catalog
            )
        ).activated
        assert (
            await store.store_chat_catalog(
                admin_b_uuid, "queue-admin-b", admin_b_catalog
            )
        ).activated

        reconciled = await store.reconcile_chat_schedules()
        assert reconciled.assigned == 3
        async with pool.acquire() as connection:
            assignments = {
                row["chat_key"]: row["source_connection_uuid"]
                for row in await connection.fetch(
                    """
                    SELECT chat_key, source_connection_uuid
                    FROM workspace_zulip_bridge.zulip_streams
                    ORDER BY chat_key
                    """
                )
            }
            statuses = {
                row["uuid"]: row["lifecycle_status"]
                for row in await connection.fetch(
                    """
                    SELECT uuid, lifecycle_status
                    FROM workspace_zulip_bridge.zulip_connections
                    """
                )
            }
            first_visible_message_id = await connection.fetchval(
                """
                SELECT binding.first_visible_message_id
                FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = binding.zulip_stream_uuid
                WHERE stream.chat_key = 'channel:7'
                  AND binding.zulip_user_uuid = $1
                """,
                owner_uuid,
            )
            supplier_color = await connection.fetchval(
                """
                SELECT color FROM workspace_zulip_bridge.zulip_streams
                WHERE chat_key = 'channel:7'
                """
            )
        assert assignments == {
            "channel:7": owner_uuid,
            "channel:8": (
                admin_a_uuid if admin_a_user_uuid < admin_b_user_uuid else admin_b_uuid
            ),
            "channel:9": (
                admin_a_uuid if admin_a_user_uuid < admin_b_user_uuid else admin_b_uuid
            ),
        }
        assert statuses == {
            owner_uuid: "backfilling",
            admin_a_uuid: "active",
            admin_b_uuid: "backfilling",
        }
        assert first_visible_message_id == 101
        assert supplier_color == 0x112233

        admin_b_refresh = _catalog(
            30,
            [(7, "Shared"), (8, "Admins"), (9, "Tie")],
            {"channel:7": 1000, "channel:8": 9, "channel:9": 2},
            colors={7: "#ABCDEF"},
        )
        assert (
            await store.store_chat_catalog(
                admin_b_uuid, "queue-admin-b", admin_b_refresh
            )
        ).activated
        assert (
            await pool.fetchval(
                """
            SELECT color FROM workspace_zulip_bridge.zulip_streams
            WHERE chat_key = 'channel:7'
            """
            )
            == 0x112233
        )

        preserved_message_uuid = stable_message_uuid(ENDPOINT, 701)
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_messages (
                uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                sender_user_uuid, zulip_message_id, content, content_hash,
                message_hash, created_at, source_updated_at
            ) VALUES ($1, $2, $3, $4, $5, 701, 'supplier baseline', $6, $6,
                      clock_timestamp(), clock_timestamp())
            """,
            preserved_message_uuid,
            stable_realm_uuid(ENDPOINT),
            owner_uuid,
            stable_chat_uuid(ENDPOINT, "channel:7"),
            owner_uuid,
            b"m" * 32,
        )

        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET lifecycle_status = 'backfilling'
                WHERE uuid = $1
                """,
                admin_a_uuid,
            )
        await store.reconcile_chat_schedules()
        async with pool.acquire() as connection:
            assert (
                await connection.fetchval(
                    """
                    SELECT lifecycle_status
                    FROM workspace_zulip_bridge.zulip_connections
                    WHERE uuid = $1
                    """,
                    admin_a_uuid,
                )
                == "active"
            )

        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET disabled = true
                WHERE uuid = $1
                """,
                owner_uuid,
            )
        failover = await store.reconcile_chat_schedules()
        assert failover.invalidated == 1
        assert failover.assigned == 1
        async with pool.acquire() as connection:
            failover_state = await connection.fetchrow(
                """
                SELECT stream.source_connection_uuid, stream.history_loaded_at,
                       message.source_connection_uuid AS message_source_uuid
                FROM workspace_zulip_bridge.zulip_streams AS stream
                JOIN workspace_zulip_bridge.zulip_messages AS message
                  ON message.zulip_stream_uuid = stream.uuid
                 AND message.uuid = $1
                WHERE stream.chat_key = 'channel:7'
                """,
                preserved_message_uuid,
            )
            assert failover_state is not None
            assert failover_state["source_connection_uuid"] == admin_b_uuid
            assert failover_state["history_loaded_at"] is None
            assert failover_state["message_source_uuid"] == admin_b_uuid
    finally:
        await pool.close()


def test_history_ids_survive_queue_loss_and_supplier_deletion() -> None:
    asyncio.run(_history_round_trip(_dsn()))


def test_catalog_populates_normalized_stream_metadata() -> None:
    asyncio.run(_catalog_populates_normalized_stream_metadata(_dsn()))


async def _catalog_populates_normalized_stream_metadata(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            connection_uuid = await _insert_user(
                connection,
                10,
                100,
                queue_id="queue-metadata",
                status="filling",
            )
        builder = ChatCatalogBuilder(10, "User 10", 100)
        builder.add_subscriptions(
            [
                {
                    "stream_id": 7,
                    "name": "Metadata",
                    "description": "Normalized description",
                    "invite_only": True,
                    "is_announcement_only": True,
                    "is_archived": True,
                    "color": "#123456",
                }
            ]
        )
        builder.add_direct_messages(
            [
                {
                    "id": 701,
                    "recipient_id": 70,
                    "display_recipient": [
                        {"id": 10, "full_name": "User 10"},
                        {"id": 20, "full_name": "User 20"},
                        {"id": 30, "full_name": "User 30"},
                    ],
                }
            ]
        )
        assert (
            await store.store_chat_catalog(
                connection_uuid,
                "queue-metadata",
                builder.build({"channel:7": 0}),
            )
        ).activated
        row = await pool.fetchrow(
            """
            SELECT description, invite_only, announce, private, is_archived,
                   color
            FROM workspace_zulip_bridge.zulip_streams
            WHERE uuid = $1
            """,
            stable_chat_uuid(ENDPOINT, "channel:7"),
        )
        assert row is not None
        assert dict(row) == {
            "description": "Normalized description",
            "invite_only": True,
            "announce": True,
            "private": False,
            "is_archived": True,
            "color": 0x123456,
        }
        group_direct = await pool.fetchrow(
            """
            SELECT chat_type, private
            FROM workspace_zulip_bridge.zulip_streams
            WHERE chat_key = 'direct:10,20,30'
            """
        )
        assert group_direct is not None
        assert dict(group_direct) == {
            "chat_type": "group_direct",
            "private": False,
        }
    finally:
        await pool.close()


def test_history_persists_only_file_metadata_and_owner() -> None:
    asyncio.run(_file_metadata_round_trip(_dsn()))


async def _file_metadata_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
        catalog = _catalog(10, [(7, "Shared")], {"channel:7": 1})
        assert (
            await store.store_chat_catalog(owner_uuid, "queue-owner", catalog)
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        assert (
            await store.store_user_attachments(
                owner_uuid,
                "queue-owner",
                (
                    ZulipAttachment(
                        attachment_id=41,
                        source_path="/user_uploads/a/report.csv",
                        name="report.csv",
                        size_bytes=123,
                        created_at=1_699_999_999,
                        message_ids=(777,),
                        metadata_hash=b"m" * 32,
                    ),
                ),
                replace_all=True,
            )
            == 1
        )
        message = ZulipMessage(
            message_id=777,
            chat_key="channel:7",
            topic_name="Files",
            sender_user_uuid=owner_uuid,
            content="report attached",
            is_read=True,
            is_starred=False,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json="[]",
            message_hash=b"f" * 32,
            sent_at=1_700_000_000,
            files=(),
        )
        await _load_one_chat(store, pool, owner_uuid, "queue-owner", message)
        async with pool.acquire() as connection:
            file_row = await connection.fetchrow(
                """
                SELECT uuid, owner_user_uuid, zulip_attachment_id, source_path,
                       name, size_bytes, message_ids, metadata_hash
                FROM workspace_zulip_bridge.zulip_files
                """
            )
            links = await connection.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.zulip_message_files"
            )
            columns = {
                row["column_name"]
                for row in await connection.fetch(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'workspace_zulip_bridge'
                      AND table_name = 'zulip_files'
                    """
                )
            }
        assert file_row is not None
        assert dict(file_row) == {
            "uuid": stable_file_uuid(ENDPOINT, "/user_uploads/a/report.csv"),
            "owner_user_uuid": owner_uuid,
            "zulip_attachment_id": 41,
            "source_path": "/user_uploads/a/report.csv",
            "name": "report.csv",
            "size_bytes": 123,
            "message_ids": [777],
            "metadata_hash": b"m" * 32,
        }
        assert links == 1
        assert not ({"content", "data", "body", "blob", "bytes"} & columns)

        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_realms
            SET identity_key = $2 WHERE uuid = $1
            """,
            stable_realm_uuid(ENDPOINT),
            f"{ENDPOINT}/renamed-endpoint",
        )

        updated_attachment = {
            "id": 1,
            "type": "attachment",
            "op": "update",
            "attachment": {
                "id": 41,
                "path_id": "a/report.csv",
                "name": "report.csv",
                "size": 456,
                "create_time": 1_699_999_999,
                "message_ids": [],
            },
        }
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="attachment",
                    payload_json=json.dumps(updated_attachment),
                ),
            ),
            1,
        ) == (1, True)
        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env({"WZB_DATABASE_DSN": dsn}),
        )
        assert (await processor.process_once()).applied == 1
        async with pool.acquire() as connection:
            assert await connection.fetchval(
                "SELECT uuid FROM workspace_zulip_bridge.zulip_files"
            ) == stable_file_uuid(ENDPOINT, "/user_uploads/a/report.csv")
            assert (
                await connection.fetchval(
                    "SELECT size_bytes FROM workspace_zulip_bridge.zulip_files"
                )
                == 456
            )
            assert (
                await connection.fetchval(
                    "SELECT message_ids FROM workspace_zulip_bridge.zulip_files"
                )
                == []
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.zulip_message_files"
                )
                == 0
            )
    finally:
        await pool.close()


async def _history_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
            member_uuid = await _insert_user(
                connection, 20, 400, queue_id="queue-member", status="filling"
            )
            sender_uuid = await _insert_user(connection, 30, 400, api_key=None)

        catalog = _catalog(10, [(7, "Shared")], {"channel:7": 1})
        member_catalog = _catalog(20, [(7, "Shared")], {"channel:7": 1})
        assert (
            await store.store_chat_catalog(owner_uuid, "queue-owner", catalog)
        ).activated
        assert (
            await store.store_chat_catalog(member_uuid, "queue-member", member_catalog)
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1

        message = ZulipMessage(
            message_id=123,
            chat_key="channel:7",
            topic_name="Performance",
            sender_user_uuid=sender_uuid,
            content="first",
            is_read=False,
            is_starred=True,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json="[]",
            message_hash=b"a" * 32,
            sent_at=1_700_000_000,
        )
        first_message_uuid = await _load_one_chat(
            store, pool, owner_uuid, "queue-owner", message
        )
        member_message_uuid = await _load_one_chat(
            store,
            pool,
            member_uuid,
            "queue-member",
            replace(
                message,
                content="must not replace common message data",
                is_read=True,
                is_starred=False,
                message_hash=b"p" * 32,
            ),
            expected_changed=0,
            expected_schedules_loaded=0,
        )
        chat_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
        assert first_message_uuid == stable_message_uuid(ENDPOINT, 123)
        assert member_message_uuid == first_message_uuid
        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT message.topic_uuid, message.content,
                       owner_flags.is_read AS owner_is_read,
                       owner_flags.is_starred AS owner_is_starred,
                       member_flags.is_read AS member_is_read,
                       member_flags.is_starred AS member_is_starred,
                       message.created_at, message.updated_at
                FROM workspace_zulip_bridge.zulip_messages AS message
                JOIN workspace_zulip_bridge.zulip_message_flags AS owner_flags
                  ON owner_flags.message_uuid = message.uuid
                 AND owner_flags.zulip_user_uuid = $2
                JOIN workspace_zulip_bridge.zulip_message_flags AS member_flags
                  ON member_flags.message_uuid = message.uuid
                 AND member_flags.zulip_user_uuid = $3
                WHERE message.uuid = $1
                """,
                first_message_uuid,
                stable_user_uuid(ENDPOINT, 10),
                stable_user_uuid(ENDPOINT, 20),
            )
        assert row is not None
        assert row["topic_uuid"] == stable_topic_uuid(chat_uuid, "Performance")
        assert row["content"] == "first"
        assert not row["owner_is_read"]
        assert row["owner_is_starred"]
        assert row["member_is_read"]
        assert not row["member_is_starred"]
        assert int(row["created_at"].timestamp()) == 1_700_000_000

        assert await store.clear_queue(owner_uuid, "queue-owner")
        async with pool.acquire() as connection:
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.zulip_messages"
                )
                == 1
            )
            queue_reset = await connection.fetchrow(
                """
                SELECT lifecycle_status AS status, queue_id, catalog_completed_at
                FROM workspace_zulip_bridge.zulip_connections
                WHERE uuid = $1
                """,
                owner_uuid,
            )
            assert queue_reset is not None
            assert dict(queue_reset) == {
                "status": "init",
                "queue_id": None,
                "catalog_completed_at": None,
            }
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET queue_id = 'queue-owner-2',
                    last_event_id = 0,
                    lifecycle_status = 'filling'
                WHERE uuid = $1
                """,
                owner_uuid,
            )
        assert (
            await store.store_chat_catalog(owner_uuid, "queue-owner-2", catalog)
        ).reused
        await store.reconcile_chat_schedules()
        second_message_uuid = await _load_one_chat(
            store,
            pool,
            owner_uuid,
            "queue-owner-2",
            replace(message, content="after queue loss", message_hash=b"b" * 32),
        )
        assert second_message_uuid == first_message_uuid

        async with pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM workspace_zulip_bridge.zulip_connections
                WHERE uuid = $1
                """,
                owner_uuid,
            )
        failover = await store.reconcile_chat_schedules()
        assert failover.assigned == 1
        async with pool.acquire() as connection:
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.zulip_messages"
                )
                == 1
            )
            assert (
                await connection.fetchval(
                    """
                    SELECT source_connection_uuid
                    FROM workspace_zulip_bridge.zulip_streams
                    WHERE uuid = $1
                    """,
                    chat_uuid,
                )
                == member_uuid
            )

        third_message_uuid = await _load_one_chat(
            store,
            pool,
            member_uuid,
            "queue-member",
            replace(message, content="after failover", message_hash=b"c" * 32),
        )
        assert third_message_uuid == first_message_uuid
        async with pool.acquire() as connection:
            final = await connection.fetchrow(
                """
                SELECT uuid, source_connection_uuid AS zulip_user_uuid, content
                FROM workspace_zulip_bridge.zulip_messages
                """
            )
        assert final is not None
        assert dict(final) == {
            "uuid": first_message_uuid,
            "zulip_user_uuid": member_uuid,
            "content": "after failover",
        }
    finally:
        await pool.close()


def test_history_reuses_workspace_created_topic_identity() -> None:
    asyncio.run(_history_reuses_workspace_created_topic_identity(_dsn()))


def test_outbound_topic_rename_preserves_canonical_identity() -> None:
    asyncio.run(_outbound_topic_rename_preserves_canonical_identity(_dsn()))


async def _outbound_topic_rename_preserves_canonical_identity(dsn: str) -> None:
    pool = await _pool(dsn)
    realm_uuid = stable_realm_uuid(ENDPOINT)
    stream_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
    topic_uuid = UUID("10000000-0000-0000-0000-000000000702")
    message_uuid = stable_message_uuid(ENDPOINT, 702)
    calls: list[tuple[int, str, str]] = []
    try:
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_streams (
                    uuid, realm_uuid, chat_type, chat_key, name,
                    content_hash, source_connection_uuid
                ) VALUES ($1, $2, 'channel', 'channel:7', 'Shared', $3, $4)
                """,
                stream_uuid,
                realm_uuid,
                b"s" * 32,
                owner_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topics (
                    uuid, zulip_stream_uuid, name, content_hash
                ) VALUES ($1, $2, 'Old topic', $3)
                """,
                topic_uuid,
                stream_uuid,
                hashlib.sha256(b"Old topic").digest(),
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_messages (
                    uuid, realm_uuid, source_connection_uuid, zulip_stream_uuid,
                    topic_uuid, sender_user_uuid, zulip_message_id, content,
                    content_hash, message_hash, created_at, source_updated_at
                ) VALUES ($1, $2, $3, $4, $5, $3, 702, 'message', $6, $7,
                          clock_timestamp(), clock_timestamp())
                """,
                message_uuid,
                realm_uuid,
                owner_uuid,
                stream_uuid,
                topic_uuid,
                b"c" * 32,
                b"m" * 32,
            )

        client = SimpleNamespace(
            update_message=lambda message_id, *, topic, propagate_mode: calls.append(
                (message_id, topic, propagate_mode)
            )
        )
        writer = ZulipOutboundWriter(pool, Settings(database_dsn=dsn))
        writer._client = lambda _actor: client  # type: ignore[method-assign]
        await writer._apply_topics(
            topic_uuid,
            {"stream_uuid": str(stream_uuid), "name": "Old topic"},
            {
                "stream_uuid": str(stream_uuid),
                "name": "Renamed topic",
                "is_done": False,
                "version": 4,
            },
            None,
        )

        topic = await pool.fetchrow(
            """
            SELECT uuid, name, is_done, version
            FROM workspace_zulip_bridge.zulip_topics
            WHERE uuid = $1
            """,
            topic_uuid,
        )
        aliases = await pool.fetch(
            """
            SELECT alias, topic_uuid, active
            FROM workspace_zulip_bridge.zulip_topic_aliases
            WHERE zulip_stream_uuid = $1
            ORDER BY alias
            """,
            stream_uuid,
        )
        assert calls == [(702, "Renamed topic", "change_all")]
        assert topic is not None
        assert tuple(topic) == (topic_uuid, "Renamed topic", False, 4)
        assert [tuple(row) for row in aliases] == [
            ("Old topic", topic_uuid, False),
            ("Renamed topic", topic_uuid, True),
        ]
        assert stable_topic_uuid(stream_uuid, "Renamed topic") != topic_uuid
        assert (
            await pool.fetchval(
                """
                SELECT uuid FROM workspace_zulip_bridge.zulip_topics
                WHERE zulip_stream_uuid = $1 AND name = 'Renamed topic'
                """,
                stream_uuid,
            )
            == topic_uuid
        )
    finally:
        await pool.close()


async def _history_reuses_workspace_created_topic_identity(dsn: str) -> None:
    pool = await _pool(dsn)
    workspace_topic_uuid = UUID("10000000-0000-0000-0000-000000000701")
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
        catalog = _catalog(10, [(7, "Shared")], {"channel:7": 1})
        assert (
            await store.store_chat_catalog(owner_uuid, "queue-owner", catalog)
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        stream_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_topics (
                uuid, zulip_stream_uuid, name, content_hash
            ) VALUES ($1, $2, 'Workspace topic', $3)
            """,
            workspace_topic_uuid,
            stream_uuid,
            hashlib.sha256(b"Workspace topic").digest(),
        )
        message = ZulipMessage(
            message_id=701,
            chat_key="channel:7",
            topic_name="Workspace topic",
            sender_user_uuid=owner_uuid,
            content="echoed topic",
            is_read=True,
            is_starred=False,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json="[]",
            message_hash=b"t" * 32,
            sent_at=1_700_000_000,
        )
        message_uuid = await _load_one_chat(
            store, pool, owner_uuid, "queue-owner", message
        )
        assert (
            await pool.fetchval(
                "SELECT topic_uuid FROM workspace_zulip_bridge.zulip_messages "
                "WHERE uuid = $1",
                message_uuid,
            )
            == workspace_topic_uuid
        )
        assert (
            await pool.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.zulip_topics "
                "WHERE zulip_stream_uuid = $1 AND name = 'Workspace topic'",
                stream_uuid,
            )
            == 1
        )
    finally:
        await pool.close()


def test_event_processor_applies_only_the_selected_chat_supplier() -> None:
    asyncio.run(_event_processor_round_trip(_dsn()))


def test_event_processor_materializes_live_catalog_changes() -> None:
    asyncio.run(_event_processor_materializes_live_catalog_changes(_dsn()))


async def _event_processor_materializes_live_catalog_changes(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
            await _insert_user(connection, 20, 400, api_key=None)
        events = (
            {
                "id": 1,
                "type": "realm_user",
                "op": "update",
                "person": {
                    "user_id": 30,
                    "email": "user-30@example.test",
                    "full_name": "User 30",
                    "role": 400,
                    "is_active": True,
                    "is_bot": False,
                },
            },
            {
                "id": 2,
                "type": "subscription",
                "op": "add",
                "subscriptions": [{"stream_id": 7, "name": "New channel"}],
            },
            {
                "id": 3,
                "type": "message",
                "message": {
                    "id": 501,
                    "type": "private",
                    "sender_id": 20,
                    "content": "new direct conversation",
                    "timestamp": 1_700_000_000,
                    "flags": ["read"],
                    "reactions": [],
                    "display_recipient": [
                        {"id": 10, "full_name": "User 10"},
                        {"id": 20, "full_name": "User 20"},
                    ],
                },
            },
        )
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            tuple(
                ZulipEvent(
                    event_id=event["id"],
                    event_type=event["type"],
                    payload_json=json.dumps(event),
                )
                for event in events
            ),
            3,
        ) == (3, True)

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "0.001",
                    "WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS": "0.001",
                }
            ),
        )
        stats = await processor.process_once()
        assert (
            stats.claimed,
            stats.applied,
            stats.retried,
            stats.failed,
        ) == (3, 2, 1, 0)

        async with pool.acquire() as connection:
            lifecycle = await connection.fetchval(
                "SELECT lifecycle_status FROM workspace_zulip_bridge.zulip_connections "
                "WHERE uuid = $1",
                owner_uuid,
            )
            direct_stream = await connection.fetchrow(
                "SELECT chat_key, source_connection_uuid "
                "FROM workspace_zulip_bridge.zulip_streams "
                "WHERE chat_key = 'direct:10,20'"
            )
            directory_user = await connection.fetchrow(
                "SELECT full_name, is_bot FROM workspace_zulip_bridge.zulip_users "
                "WHERE zulip_user_id = 30"
            )
            statuses = dict(
                await connection.fetch(
                    "SELECT processing_status, count(*) "
                    "FROM workspace_zulip_bridge.zulip_events "
                    "GROUP BY processing_status"
                )
            )
            retry_event = await connection.fetchrow(
                "SELECT outcome_reason, attempt_count "
                "FROM workspace_zulip_bridge.zulip_events "
                "WHERE event_id = 3"
            )
        assert lifecycle == "filling"
        assert direct_stream is not None
        assert direct_stream["source_connection_uuid"] is None
        assert directory_user is not None
        assert dict(directory_user) == {"full_name": "User 30", "is_bot": False}
        assert statuses == {"applied": 2, "pending": 1}
        assert retry_event is not None
        assert dict(retry_event) == {
            "outcome_reason": "chat_unassigned",
            "attempt_count": 1,
        }
    finally:
        await pool.close()


def test_event_processor_defers_stale_supplier_during_catalog_refresh() -> None:
    asyncio.run(_event_processor_defers_stale_supplier(_dsn()))


def test_event_processor_applies_supplier_message_during_history_backfill() -> None:
    asyncio.run(_event_processor_applies_during_history_backfill(_dsn()))


def test_event_processor_retires_message_for_out_of_scope_chat() -> None:
    asyncio.run(_event_processor_retires_message_for_out_of_scope_chat(_dsn()))


async def _event_processor_retires_message_for_out_of_scope_chat(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
            await connection.execute(
                "UPDATE workspace_zulip_bridge.zulip_connections "
                "SET catalog_completed_at = clock_timestamp() WHERE uuid = $1",
                owner_uuid,
            )
        event = {
            "id": 1,
            "type": "message",
            "message": {
                "id": 701,
                "type": "stream",
                "stream_id": 77,
                "display_recipient": "Out of scope",
                "subject": "General",
                "sender_id": 10,
                "content": "not visible in the active catalog",
                "timestamp": 1_700_000_000,
                "flags": [],
                "reactions": [],
            },
        }
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="message",
                    payload_json=json.dumps(event),
                ),
            ),
            1,
        ) == (1, True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events SET attempt_count = 8"
        )

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env({"WZB_DATABASE_DSN": dsn}),
        )
        processed = await processor.process_once()
        assert (processed.claimed, processed.skipped, processed.retried) == (1, 1, 0)
        assert (
            await pool.fetchval(
                "SELECT outcome_reason FROM workspace_zulip_bridge.zulip_events"
            )
            == "chat_out_of_scope"
        )
    finally:
        await pool.close()


async def _event_processor_applies_during_history_backfill(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
        assert (
            await store.store_chat_catalog(
                owner_uuid,
                "queue-owner",
                _catalog(10, [(7, "Shared")], {"channel:7": 1}),
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        assert not await pool.fetchval(
            "SELECT history_loaded_at IS NOT NULL "
            "FROM workspace_zulip_bridge.zulip_streams "
            "WHERE chat_key = 'channel:7'"
        )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_connections "
            "SET lifecycle_status = 'filling', catalog_completed_at = NULL "
            "WHERE uuid = $1",
            owner_uuid,
        )
        event = {
            "id": 1,
            "type": "message",
            "message": {
                "id": 701,
                "type": "stream",
                "stream_id": 7,
                "display_recipient": "Shared",
                "subject": "General",
                "sender_id": 10,
                "content": "live during history",
                "timestamp": 1_700_000_000,
                "flags": [],
                "reactions": [],
            },
        }
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="message",
                    payload_json=json.dumps(event),
                ),
            ),
            1,
        ) == (1, True)

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env({"WZB_DATABASE_DSN": dsn}),
        )
        processed = await processor.process_once()
        assert (processed.claimed, processed.applied, processed.retried) == (1, 1, 0)
        assert await pool.fetchval(
            "SELECT EXISTS (SELECT 1 "
            "FROM workspace_zulip_bridge.zulip_messages "
            "WHERE zulip_message_id = 701)"
        )

        flag_event = {
            "id": 2,
            "type": "update_message_flags",
            "op": "add",
            "flag": "read",
            "messages": [701],
        }
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=2,
                    event_type="update_message_flags",
                    payload_json=json.dumps(flag_event),
                ),
            ),
            2,
        ) == (1, True)
        processed = await processor.process_once()
        assert (processed.claimed, processed.applied, processed.retried) == (1, 1, 0)
        assert await pool.fetchval(
            "SELECT flag.is_read "
            "FROM workspace_zulip_bridge.zulip_message_flags AS flag "
            "JOIN workspace_zulip_bridge.zulip_messages AS message "
            "ON message.uuid = flag.message_uuid "
            "WHERE flag.zulip_user_uuid = $1 "
            "AND message.zulip_message_id = 701",
            owner_uuid,
        )

        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_streams "
            "SET history_loaded_at = clock_timestamp() "
            "WHERE chat_key = 'channel:7'"
        )
        partial_flag_event = {
            "id": 3,
            "type": "update_message_flags",
            "op": "add",
            "flag": "starred",
            "messages": [701, 999999],
        }
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=3,
                    event_type="update_message_flags",
                    payload_json=json.dumps(partial_flag_event),
                ),
            ),
            3,
        ) == (1, True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET attempt_count = 8 "
            "WHERE event_id = 3"
        )
        processed = await processor.process_once()
        assert (processed.claimed, processed.applied, processed.retried) == (1, 1, 0)
        assert await pool.fetchval(
            "SELECT flag.is_starred "
            "FROM workspace_zulip_bridge.zulip_message_flags AS flag "
            "JOIN workspace_zulip_bridge.zulip_messages AS message "
            "ON message.uuid = flag.message_uuid "
            "WHERE flag.zulip_user_uuid = $1 "
            "AND message.zulip_message_id = 701",
            owner_uuid,
        )
    finally:
        await pool.close()


async def _event_processor_defers_stale_supplier(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
            member_uuid = await _insert_user(
                connection, 20, 400, queue_id="queue-member", status="filling"
            )
        assert (
            await store.store_chat_catalog(
                owner_uuid,
                "queue-owner",
                _catalog(10, [(7, "Shared")], {"channel:7": 1}),
            )
        ).activated
        assert (
            await store.store_chat_catalog(
                member_uuid,
                "queue-member",
                _catalog(20, [(7, "Shared")], {"channel:7": 1}),
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_streams "
            "SET history_loaded_at = clock_timestamp() "
            "WHERE chat_key = 'channel:7'"
        )

        subscription = {
            "id": 1,
            "type": "subscription",
            "op": "remove",
            "subscriptions": [{"stream_id": 7, "name": "Shared"}],
        }
        message = {
            "id": 1,
            "type": "message",
            "message": {
                "id": 700,
                "type": "stream",
                "stream_id": 7,
                "display_recipient": "Shared",
                "subject": "General",
                "sender_id": 20,
                "content": "arrived during reassignment",
                "timestamp": 1_700_000_000,
                "flags": [],
                "reactions": [],
            },
        }
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="subscription",
                    payload_json=json.dumps(subscription),
                ),
            ),
            1,
        ) == (1, True)
        assert await store.store_events(
            member_uuid,
            "queue-member",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="message",
                    payload_json=json.dumps(message),
                ),
            ),
            1,
        ) == (1, True)

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "2",
                    "WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS": "30",
                }
            ),
        )
        first = await processor.process_once()
        assert (first.claimed, first.applied, first.retried) == (2, 1, 1)
        pending = await pool.fetchrow(
            "SELECT processing_status, outcome_reason, attempt_count, "
            "EXTRACT(EPOCH FROM (available_at - clock_timestamp())) "
            "AS retry_seconds "
            "FROM workspace_zulip_bridge.zulip_events "
            "WHERE zulip_connection_uuid = $1 AND event_type = 'message'",
            member_uuid,
        )
        assert pending is not None
        assert pending["processing_status"] == "pending"
        assert pending["outcome_reason"] == "chat_rescheduling"
        assert pending["attempt_count"] == 1
        assert 0.5 <= float(pending["retry_seconds"]) <= 2.1
        assert not await pool.fetchval(
            "SELECT EXISTS (SELECT 1 "
            "FROM workspace_zulip_bridge.zulip_messages "
            "WHERE zulip_message_id = 700)"
        )

        assert (
            await store.store_chat_catalog(
                owner_uuid,
                "queue-owner",
                _catalog(10, [], {}),
            )
        ).activated
        schedule = await store.reconcile_chat_schedules()
        assert schedule.assigned == 1
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET available_at = clock_timestamp() "
            "WHERE zulip_connection_uuid = $1 AND event_type = 'message'",
            member_uuid,
        )
        assert (
            await pool.fetchval(
                "SELECT source_connection_uuid "
                "FROM workspace_zulip_bridge.zulip_streams "
                "WHERE chat_key = 'channel:7'"
            )
            == member_uuid
        )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_streams "
            "SET history_loaded_at = clock_timestamp() "
            "WHERE chat_key = 'channel:7'"
        )
        second = await processor.process_once()
        assert (second.claimed, second.applied, second.retried) == (1, 1, 0)
        assert await pool.fetchval(
            "SELECT EXISTS (SELECT 1 "
            "FROM workspace_zulip_bridge.zulip_messages "
            "WHERE zulip_message_id = 700)"
        )
    finally:
        await pool.close()


def test_event_processor_dependency_deferrals_do_not_block_later_events() -> None:
    asyncio.run(_event_processor_dependency_deferrals_do_not_block_later_events(_dsn()))


def test_event_processor_retires_missing_user_topic_after_history() -> None:
    asyncio.run(_event_processor_retires_missing_user_topic_after_history(_dsn()))


def test_backlog_dependency_deferrals_use_slow_retry_cap() -> None:
    asyncio.run(_backlog_dependency_deferrals_use_slow_retry_cap(_dsn()))


async def _backlog_dependency_deferrals_use_slow_retry_cap(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        assert (
            await store.store_chat_catalog(
                owner_uuid,
                "queue-owner",
                _catalog(10, [(7, "Shared")], {"channel:7": 1}),
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="update_message",
                    payload_json=json.dumps(
                        {"id": 1, "type": "update_message", "message_id": 999999}
                    ),
                ),
            ),
            1,
        ) == (1, True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET created_at = clock_timestamp() - interval '10 minutes', "
            "attempt_count = 16"
        )
        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_BATCH_SIZE": "1",
                    "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "0.001",
                    "WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS": "0.01",
                    "WZB_EVENT_PROCESSOR_BACKLOG_RETRY_CAP_SECONDS": "0.5",
                }
            ),
            claim_scope="backlog",
        )

        result = await processor.process_once()

        assert (result.claimed, result.retried) == (1, 1)
        retry_delay = await pool.fetchval(
            "SELECT extract(epoch FROM available_at - clock_timestamp()) "
            "FROM workspace_zulip_bridge.zulip_events"
        )
        assert float(retry_delay) > 0.35

        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_streams "
            "SET history_loaded_at = clock_timestamp() "
            "WHERE chat_key = 'channel:7'"
        )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET available_at = clock_timestamp()"
        )
        settled = await processor.process_once()
        assert (settled.claimed, settled.skipped, settled.retried) == (1, 1, 0)
        assert await pool.fetchval(
            "SELECT outcome_reason = 'message_out_of_scope' "
            "FROM workspace_zulip_bridge.zulip_events"
        )
    finally:
        await pool.close()


def test_prepare_database_backfills_zulip_event_queue_registry() -> None:
    asyncio.run(_prepare_database_backfills_zulip_event_queue_registry(_dsn()))


async def _prepare_database_backfills_zulip_event_queue_registry(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        assert await store.store_events(
            user_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 1, "type": "heartbeat"}),
                ),
            ),
            1,
        ) == (1, True)
        await pool.execute("DELETE FROM workspace_zulip_bridge.zulip_event_queues")
        await pool.execute(
            "DELETE FROM workspace_zulip_bridge.maintenance_migrations "
            "WHERE name = '2026-09-24-zulip-event-queue-registry'"
        )

        await prepare_database(pool)

        assert await pool.fetchval(
            "SELECT EXISTS ("
            "SELECT 1 FROM workspace_zulip_bridge.zulip_event_queues "
            "WHERE zulip_connection_uuid = $1 AND queue_id = 'queue-owner')",
            user_uuid,
        )
    finally:
        await pool.close()


async def _event_processor_retires_missing_user_topic_after_history(
    dsn: str,
) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        assert (
            await store.store_chat_catalog(
                owner_uuid,
                "queue-owner",
                _catalog(10, [(7, "Shared")], {"channel:7": 1}),
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        events = (
            {
                "id": 1,
                "type": "user_topic",
                "stream_id": 8,
                "topic_name": "Not selected",
                "visibility_policy": 1,
                "last_updated": 1_700_000_000,
            },
            {"id": 2, "type": "heartbeat"},
        )
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            tuple(
                ZulipEvent(
                    event_id=event["id"],
                    event_type=event["type"],
                    payload_json=json.dumps(event),
                )
                for event in events
            ),
            2,
        ) == (2, True)
        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_BATCH_SIZE": "1",
                    "WZB_EVENT_PROCESSOR_MAX_ATTEMPTS": "1",
                    "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "30",
                }
            ),
        )

        assert (await processor.process_once()).retried == 1
        bypass = await processor.process_once()
        assert (bypass.claimed, bypass.skipped) == (1, 1)

        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_streams "
            "SET history_loaded_at = clock_timestamp() "
            "WHERE chat_key = 'channel:7'"
        )
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET available_at = clock_timestamp() "
            "WHERE event_type = 'user_topic'"
        )
        settled = await processor.process_once()
        assert (settled.claimed, settled.skipped, settled.retried) == (1, 1, 0)
        assert await pool.fetchval(
            "SELECT outcome_reason = 'topic_out_of_scope' "
            "FROM workspace_zulip_bridge.zulip_events "
            "WHERE event_type = 'user_topic'"
        )
    finally:
        await pool.close()


async def _event_processor_dependency_deferrals_do_not_block_later_events(
    dsn: str,
) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        events = (
            {"id": 1, "type": "update_message", "message_id": 999999},
            {"id": 2, "type": "heartbeat"},
        )
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            tuple(
                ZulipEvent(
                    event_id=event["id"],
                    event_type=event["type"],
                    payload_json=json.dumps(event),
                )
                for event in events
            ),
            2,
        ) == (2, True)

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_BATCH_SIZE": "1",
                    "WZB_EVENT_PROCESSOR_MAX_ATTEMPTS": "2",
                    "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "0.05",
                    "WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS": "0.05",
                }
            ),
        )
        assert (await processor.process_once()).retried == 1
        second = await processor.process_once()
        assert (second.claimed, second.skipped) == (1, 1)
        await asyncio.sleep(0.06)
        assert (await processor.process_once()).retried == 1

        rows = await pool.fetch(
            """
            SELECT event_id, processing_status, attempt_count, outcome_reason,
                   processed_at IS NOT NULL AS processed
            FROM workspace_zulip_bridge.zulip_events
            ORDER BY event_id
            """
        )
        assert [dict(row) for row in rows] == [
            {
                "event_id": 1,
                "processing_status": "pending",
                "attempt_count": 2,
                "outcome_reason": "message_not_materialized",
                "processed": False,
            },
            {
                "event_id": 2,
                "processing_status": "skipped",
                "attempt_count": 1,
                "outcome_reason": "unsupported_event_type",
                "processed": True,
            },
        ]
    finally:
        await pool.close()


def test_event_processor_retries_due_deferral_before_newer_events() -> None:
    asyncio.run(_event_processor_retries_due_deferral_before_newer_events(_dsn()))


def test_event_processor_bounds_each_queue_in_a_batch() -> None:
    asyncio.run(_event_processor_bounds_each_queue_in_a_batch(_dsn()))


def test_event_processor_reserves_capacity_for_live_queues() -> None:
    asyncio.run(_event_processor_reserves_capacity_for_live_queues(_dsn()))


def test_realtime_processor_bypasses_backlog_in_the_same_queue() -> None:
    asyncio.run(_realtime_processor_bypasses_backlog_in_the_same_queue(_dsn()))


def test_realtime_processor_does_not_overlap_an_inflight_backlog_queue() -> None:
    asyncio.run(_realtime_processor_does_not_overlap_an_inflight_backlog_queue(_dsn()))


def test_realtime_processors_claim_distinct_queues_concurrently() -> None:
    asyncio.run(_realtime_processors_claim_distinct_queues_concurrently(_dsn()))


def test_realtime_processor_claims_oldest_recent_queue_first() -> None:
    asyncio.run(_realtime_processor_claims_oldest_recent_queue_first(_dsn()))


def test_event_processor_recovers_expired_claims_periodically() -> None:
    asyncio.run(_event_processor_recovers_expired_claims_periodically(_dsn()))


async def _event_processor_recovers_expired_claims_periodically(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        assert await store.store_events(
            user_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 1, "type": "heartbeat"}),
                ),
            ),
            1,
        ) == (1, True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET processing_status = 'processing', "
            "claimed_at = clock_timestamp() - interval '2 minutes'"
        )
        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_CLAIM_TIMEOUT_SECONDS": "60",
                }
            ),
            claim_scope="backlog",
        )

        assert await processor._maybe_requeue_expired_claims() == 1
        row = await pool.fetchrow(
            "SELECT processing_status, claimed_at, outcome_reason "
            "FROM workspace_zulip_bridge.zulip_events"
        )
        assert row is not None
        assert tuple(row) == ("pending", None, "claim_expired")

        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET processing_status = 'processing', "
            "claimed_at = clock_timestamp() - interval '2 minutes', "
            "outcome_reason = NULL"
        )
        assert await processor._maybe_requeue_expired_claims() == 0
        processor._next_claim_recovery_at = 0.0
        assert await processor._maybe_requeue_expired_claims() == 1
        row = await pool.fetchrow(
            "SELECT processing_status, claimed_at, outcome_reason "
            "FROM workspace_zulip_bridge.zulip_events"
        )
        assert row is not None
        assert tuple(row) == ("pending", None, "claim_expired")
    finally:
        await pool.close()


async def _realtime_processors_claim_distinct_queues_concurrently(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            first_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-first", status="active"
            )
            second_uuid = await _insert_user(
                connection, 11, 100, queue_id="queue-second", status="active"
            )
        for user_uuid, queue_id in (
            (first_uuid, "queue-first"),
            (second_uuid, "queue-second"),
        ):
            assert await store.store_events(
                user_uuid,
                queue_id,
                (
                    ZulipEvent(
                        event_id=1,
                        event_type="heartbeat",
                        payload_json=json.dumps({"id": 1, "type": "heartbeat"}),
                    ),
                ),
                1,
            ) == (1, True)
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_EVENT_PROCESSOR_REALTIME_BATCH_SIZE": "1",
            }
        )
        processors = [
            ZulipEventProcessor(pool, store, settings, claim_scope="realtime")
            for _ in range(2)
        ]

        claims = await asyncio.gather(
            *(processor._claim_events() for processor in processors)
        )

        assert sorted(len(batch) for batch in claims) == [1, 1]
        assert {batch[0].user_uuid for batch in claims} == {first_uuid, second_uuid}
    finally:
        await pool.close()


async def _realtime_processor_claims_oldest_recent_queue_first(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            older_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-older", status="active"
            )
            newer_uuid = await _insert_user(
                connection, 11, 100, queue_id="queue-newer", status="active"
            )
        for user_uuid, queue_id in (
            (older_uuid, "queue-older"),
            (newer_uuid, "queue-newer"),
        ):
            assert await store.store_events(
                user_uuid,
                queue_id,
                (
                    ZulipEvent(
                        event_id=1,
                        event_type="heartbeat",
                        payload_json=json.dumps({"id": 1, "type": "heartbeat"}),
                    ),
                ),
                1,
            ) == (1, True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET created_at = clock_timestamp() - interval '2 minutes' "
            "WHERE zulip_connection_uuid = $1",
            older_uuid,
        )
        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_REALTIME_BATCH_SIZE": "1",
                }
            ),
            claim_scope="realtime",
        )

        claims = await processor._claim_events()

        assert len(claims) == 1
        assert claims[0].user_uuid == older_uuid
    finally:
        await pool.close()


async def _realtime_processor_does_not_overlap_an_inflight_backlog_queue(
    dsn: str,
) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        assert await store.store_events(
            user_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 1, "type": "heartbeat"}),
                ),
                ZulipEvent(
                    event_id=2,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 2, "type": "heartbeat"}),
                ),
            ),
            2,
        ) == (2, True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET created_at = clock_timestamp() - interval '10 minutes' "
            "WHERE zulip_connection_uuid = $1 AND event_id = 1",
            user_uuid,
        )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_EVENT_PROCESSOR_BATCH_SIZE": "1",
                "WZB_EVENT_PROCESSOR_REALTIME_BATCH_SIZE": "1",
            }
        )
        backlog = ZulipEventProcessor(pool, store, settings, claim_scope="backlog")
        realtime = ZulipEventProcessor(pool, store, settings, claim_scope="realtime")

        assert len(await backlog._claim_events()) == 1
        assert await realtime._claim_events() == []
    finally:
        await pool.close()


async def _realtime_processor_bypasses_backlog_in_the_same_queue(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        assert await store.store_events(
            user_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 1, "type": "heartbeat"}),
                ),
                ZulipEvent(
                    event_id=2,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 2, "type": "heartbeat"}),
                ),
            ),
            2,
        ) == (2, True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET created_at = clock_timestamp() - interval '10 minutes' "
            "WHERE zulip_connection_uuid = $1 AND event_id = 1",
            user_uuid,
        )
        settings = Settings.from_env(
            {
                "WZB_DATABASE_DSN": dsn,
                "WZB_EVENT_PROCESSOR_BATCH_SIZE": "1",
                "WZB_EVENT_PROCESSOR_REALTIME_BATCH_SIZE": "1",
                "WZB_EVENT_PROCESSOR_REALTIME_WINDOW_SECONDS": "300",
            }
        )

        realtime = ZulipEventProcessor(pool, store, settings, claim_scope="realtime")
        realtime_result = await realtime.process_once()
        assert (realtime_result.claimed, realtime_result.skipped) == (1, 1)
        assert [
            tuple(row)
            for row in await pool.fetch(
                "SELECT event_id, processing_status "
                "FROM workspace_zulip_bridge.zulip_events ORDER BY event_id"
            )
        ] == [(1, "pending"), (2, "skipped")]

        backlog = ZulipEventProcessor(pool, store, settings, claim_scope="backlog")
        backlog_result = await backlog.process_once()
        assert (backlog_result.claimed, backlog_result.skipped) == (1, 1)
        assert [
            tuple(row)
            for row in await pool.fetch(
                "SELECT event_id, processing_status "
                "FROM workspace_zulip_bridge.zulip_events ORDER BY event_id"
            )
        ] == [(1, "skipped"), (2, "skipped")]
    finally:
        await pool.close()


async def _event_processor_reserves_capacity_for_live_queues(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            deferred_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-deferred", status="active"
            )
            live_uuid = await _insert_user(
                connection, 11, 100, queue_id="queue-live", status="active"
            )
        deferred_events = tuple(
            ZulipEvent(
                event_id=event_id,
                event_type="update_message",
                payload_json=json.dumps(
                    {
                        "id": event_id,
                        "type": "update_message",
                        "message_id": 900000 + event_id,
                    }
                ),
            )
            for event_id in range(1, 5)
        )
        assert await store.store_events(
            deferred_uuid, "queue-deferred", deferred_events, 4
        ) == (4, True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET outcome_reason = 'message_not_materialized', attempt_count = 1, "
            "available_at = clock_timestamp() - interval '1 second' "
            "WHERE zulip_connection_uuid = $1",
            deferred_uuid,
        )
        assert await store.store_events(
            live_uuid,
            "queue-live",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 1, "type": "heartbeat"}),
                ),
            ),
            1,
        ) == (1, True)

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_BATCH_SIZE": "4",
                }
            ),
        )
        processed = await processor.process_once()
        assert (processed.claimed, processed.skipped, processed.retried) == (4, 1, 3)
        assert (
            await pool.fetchval(
                "SELECT processing_status "
                "FROM workspace_zulip_bridge.zulip_events "
                "WHERE zulip_connection_uuid = $1 AND event_id = 1",
                live_uuid,
            )
            == "skipped"
        )
    finally:
        await pool.close()


async def _event_processor_bounds_each_queue_in_a_batch(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            first_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-first", status="active"
            )
            second_uuid = await _insert_user(
                connection, 11, 100, queue_id="queue-second", status="active"
            )
        first_events = tuple(
            ZulipEvent(
                event_id=event_id,
                event_type="heartbeat",
                payload_json=json.dumps({"id": event_id, "type": "heartbeat"}),
            )
            for event_id in range(1, 130)
        )
        assert await store.store_events(
            first_uuid, "queue-first", first_events, 129
        ) == (129, True)
        assert await store.store_events(
            second_uuid,
            "queue-second",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 1, "type": "heartbeat"}),
                ),
            ),
            1,
        ) == (1, True)

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_BATCH_SIZE": "129",
                }
            ),
        )
        processed = await processor.process_once()
        assert (processed.claimed, processed.skipped) == (9, 9)

        rows = await pool.fetch(
            "SELECT queue_id, processing_status, count(*) AS count "
            "FROM workspace_zulip_bridge.zulip_events "
            "GROUP BY queue_id, processing_status "
            "ORDER BY queue_id, processing_status"
        )
        assert [dict(row) for row in rows] == [
            {
                "queue_id": "queue-first",
                "processing_status": "pending",
                "count": 121,
            },
            {
                "queue_id": "queue-first",
                "processing_status": "skipped",
                "count": 8,
            },
            {
                "queue_id": "queue-second",
                "processing_status": "skipped",
                "count": 1,
            },
        ]
    finally:
        await pool.close()


async def _event_processor_retries_due_deferral_before_newer_events(
    dsn: str,
) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        deferred = {"id": 1, "type": "update_message", "message_id": 999999}
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="update_message",
                    payload_json=json.dumps(deferred),
                ),
            ),
            1,
        ) == (1, True)
        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_BATCH_SIZE": "1",
                    "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "0.001",
                    "WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS": "0.001",
                }
            ),
        )
        assert (await processor.process_once()).retried == 1
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=2,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 2, "type": "heartbeat"}),
                ),
            ),
            2,
        ) == (1, True)

        await asyncio.sleep(0.01)
        retried = await processor.process_once()
        assert (retried.claimed, retried.retried) == (1, 1)
        rows = await pool.fetch(
            "SELECT event_id, processing_status, attempt_count "
            "FROM workspace_zulip_bridge.zulip_events ORDER BY event_id"
        )
        assert [dict(row) for row in rows] == [
            {
                "event_id": 1,
                "processing_status": "pending",
                "attempt_count": 2,
            },
            {
                "event_id": 2,
                "processing_status": "pending",
                "attempt_count": 0,
            },
        ]
    finally:
        await pool.close()


def test_history_finish_preserves_catalog_refresh() -> None:
    asyncio.run(_history_finish_preserves_catalog_refresh(_dsn()))


async def _history_finish_preserves_catalog_refresh(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
        assert (
            await store.store_chat_catalog(
                owner_uuid,
                "queue-owner",
                _catalog(10, [(7, "Shared")], {"channel:7": 0}),
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1

        history = await store.begin_history(owner_uuid, "queue-owner")
        try:
            assert await store.request_catalog_refresh(owner_uuid, "queue-owner")
            finished = await history.finish(["channel:7"])
            assert finished.activated
        finally:
            await history.close()

        row = await pool.fetchrow(
            "SELECT lifecycle_status, catalog_completed_at "
            "FROM workspace_zulip_bridge.zulip_connections WHERE uuid = $1",
            owner_uuid,
        )
        assert row is not None
        assert dict(row) == {
            "lifecycle_status": "filling",
            "catalog_completed_at": None,
        }
        assert await store.get_user_status(owner_uuid, "queue-owner") == "filling"
    finally:
        await pool.close()


def test_event_processor_retry_delay_blocks_later_queue_events() -> None:
    asyncio.run(_event_processor_retry_delay_blocks_later_queue_events(_dsn()))


async def _event_processor_retry_delay_blocks_later_queue_events(
    dsn: str,
) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 1, "type": "heartbeat"}),
                ),
                ZulipEvent(
                    event_id=2,
                    event_type="heartbeat",
                    payload_json=json.dumps({"id": 2, "type": "heartbeat"}),
                ),
            ),
            2,
        ) == (2, True)
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_events "
            "SET available_at = clock_timestamp() + interval '1 hour', "
            "outcome_reason = 'handler_error:RuntimeError' "
            "WHERE event_id = 1"
        )

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env({"WZB_DATABASE_DSN": dsn}),
        )
        assert (await processor.process_once()).claimed == 0
        assert await pool.fetchval(
            "SELECT processing_status = 'pending' "
            "FROM workspace_zulip_bridge.zulip_events WHERE event_id = 2"
        )
    finally:
        await pool.close()


def test_event_processor_blocks_later_queue_events_behind_a_retry() -> None:
    asyncio.run(_event_processor_blocks_later_queue_events_behind_a_retry(_dsn()))


async def _event_processor_blocks_later_queue_events_behind_a_retry(
    dsn: str,
) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        assert (
            await store.store_chat_catalog(
                owner_uuid,
                "queue-owner",
                _catalog(10, [(7, "Shared")], {"channel:7": 1}),
            )
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1
        await pool.execute(
            "UPDATE workspace_zulip_bridge.zulip_streams "
            "SET history_loaded_at = clock_timestamp() "
            "WHERE chat_key = 'channel:7'"
        )
        events = (
            {
                "id": 1,
                "type": "message",
                "message": {
                    "id": 100,
                    "type": "stream",
                    "stream_id": 7,
                    "display_recipient": "Shared",
                    "subject": "General",
                    "sender_id": 10,
                    "content": "transient create",
                    "timestamp": 1_700_000_000,
                    "flags": [],
                    "reactions": [],
                },
            },
            {
                "id": 2,
                "type": "delete_message",
                "message_ids": [100],
            },
        )
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            tuple(
                ZulipEvent(
                    event_id=event["id"],
                    event_type=event["type"],
                    payload_json=json.dumps(event),
                )
                for event in events
            ),
            2,
        ) == (2, True)

        original_apply = store.apply_live_messages
        calls = 0

        async def fail_first_apply(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("transient write failure")
            return await original_apply(*args, **kwargs)

        store.apply_live_messages = fail_first_apply  # type: ignore[method-assign]
        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "0.001",
                    "WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS": "0.001",
                }
            ),
        )
        first = await processor.process_once()
        assert (first.claimed, first.applied, first.retried, calls) == (2, 0, 2, 1)
        rows = await pool.fetch(
            "SELECT event_id, processing_status, attempt_count, outcome_reason "
            "FROM workspace_zulip_bridge.zulip_events ORDER BY event_id"
        )
        assert [dict(row) for row in rows] == [
            {
                "event_id": 1,
                "processing_status": "pending",
                "attempt_count": 1,
                "outcome_reason": "handler_error:RuntimeError",
            },
            {
                "event_id": 2,
                "processing_status": "pending",
                "attempt_count": 0,
                "outcome_reason": "blocked_by_prior_nonterminal_event",
            },
        ]
        assert not await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM workspace_zulip_bridge.zulip_messages)"
        )

        await asyncio.sleep(0.01)
        second = await processor.process_once()
        assert (second.claimed, second.applied, second.retried, calls) == (2, 2, 0, 3)
        assert not await pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM workspace_zulip_bridge.zulip_messages)"
        )
    finally:
        await pool.close()


def test_event_processor_requeues_transient_batch_preparation_failure() -> None:
    asyncio.run(_event_processor_requeues_preparation_failure(_dsn()))


async def _event_processor_requeues_preparation_failure(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        events = (
            {"id": 1, "type": "subscription", "op": "add"},
            {"id": 2, "type": "presence"},
        )
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            tuple(
                ZulipEvent(
                    event_id=event["id"],
                    event_type=event["type"],
                    payload_json=json.dumps(event),
                )
                for event in events
            ),
            2,
        ) == (2, True)

        async def fail_refresh(_connection_uuid: UUID, _queue_id: str) -> bool:
            raise RuntimeError("temporary failure")

        store.request_catalog_refresh = fail_refresh  # type: ignore[method-assign]
        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "0.001",
                    "WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS": "0.001",
                }
            ),
        )
        stats = await processor.process_once()
        assert (stats.claimed, stats.retried, stats.failed) == (2, 2, 0)

        async with pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT event_id, processing_status, attempt_count, claimed_at, "
                "processed_at, outcome_reason "
                "FROM workspace_zulip_bridge.zulip_events ORDER BY event_id"
            )
        assert len(rows) == 2
        assert dict(rows[0]) == {
            "event_id": 1,
            "processing_status": "pending",
            "attempt_count": 1,
            "claimed_at": None,
            "processed_at": None,
            "outcome_reason": "preparation_error:RuntimeError",
        }
        assert dict(rows[1]) == {
            "event_id": 2,
            "processing_status": "pending",
            "attempt_count": 1,
            "claimed_at": None,
            "processed_at": None,
            "outcome_reason": "preparation_error:RuntimeError",
        }
    finally:
        await pool.close()


def test_event_processor_recovers_exhausted_preparation_deadlocks() -> None:
    asyncio.run(_event_processor_recovers_preparation_deadlocks(_dsn()))


async def _event_processor_recovers_preparation_deadlocks(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="active"
            )
        assert await store.store_events(
            owner_uuid,
            "queue-owner",
            (
                ZulipEvent(
                    event_id=1,
                    event_type="message",
                    payload_json=json.dumps({"id": 1, "type": "message"}),
                ),
                ZulipEvent(
                    event_id=2,
                    event_type="message",
                    payload_json=json.dumps({"id": 2, "type": "message"}),
                ),
            ),
            2,
        ) == (2, True)
        await pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_events
            SET processing_status = 'failed', attempt_count = 10,
                processed_at = clock_timestamp(),
                outcome_reason =
                  'retry_exhausted:preparation_error:DeadlockDetectedError'
            WHERE event_id = 1;

            UPDATE workspace_zulip_bridge.zulip_events
            SET attempt_count = 7,
                available_at = clock_timestamp() + interval '1 hour',
                outcome_reason = 'preparation_error:DeadlockDetectedError'
            WHERE event_id = 2
            """
        )

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env({"WZB_DATABASE_DSN": dsn}),
        )
        assert await processor._requeue_preparation_deadlocks() == 2
        rows = await pool.fetch(
            "SELECT processing_status, attempt_count, claimed_at, processed_at, "
            "outcome_reason FROM workspace_zulip_bridge.zulip_events "
            "ORDER BY event_id"
        )
        assert [dict(row) for row in rows] == [
            {
                "processing_status": "pending",
                "attempt_count": 0,
                "claimed_at": None,
                "processed_at": None,
                "outcome_reason": "requeued_preparation_deadlock",
            },
            {
                "processing_status": "pending",
                "attempt_count": 0,
                "claimed_at": None,
                "processed_at": None,
                "outcome_reason": "requeued_preparation_deadlock",
            },
        ]
    finally:
        await pool.close()


def test_event_processor_expires_only_old_terminal_events() -> None:
    asyncio.run(_event_retention_round_trip(_dsn()))


async def _event_retention_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            user_uuid = await _insert_user(
                connection,
                10,
                100,
                queue_id="queue-owner",
                status="active",
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_events (
                    zulip_connection_uuid,
                    queue_id,
                    event_id,
                    event_type,
                    payload,
                    processing_status,
                    created_at
                )
                SELECT $1,
                       'queue-owner',
                       seed.event_id,
                       'presence',
                       jsonb_build_object('id', seed.event_id, 'type', 'presence'),
                       seed.processing_status,
                       clock_timestamp() - seed.age
                FROM (
                    VALUES
                        (101::bigint, 'applied'::text, interval '25 hours'),
                        (102::bigint, 'skipped'::text, interval '25 hours'),
                        (103::bigint, 'failed'::text, interval '25 hours'),
                        (104::bigint, 'pending'::text, interval '25 hours'),
                        (105::bigint, 'processing'::text, interval '25 hours'),
                        (106::bigint, 'applied'::text, interval '23 hours')
                ) AS seed(event_id, processing_status, age)
                """,
                user_uuid,
            )

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_RETENTION_SECONDS": "86400",
                    "WZB_EVENT_CLEANUP_BATCH_SIZE": "2",
                }
            ),
        )
        assert await processor.cleanup_expired_events() == 2
        assert await processor.cleanup_expired_events() == 1
        assert await processor.cleanup_expired_events() == 0

        async with pool.acquire() as connection:
            remaining = {
                row["event_id"]: row["processing_status"]
                for row in await connection.fetch(
                    """
                    SELECT event_id, processing_status
                    FROM workspace_zulip_bridge.zulip_events
                    ORDER BY event_id
                    """
                )
            }
        assert remaining == {
            104: "pending",
            105: "processing",
            106: "applied",
        }
    finally:
        await pool.close()


def test_workspace_event_processor_expires_only_old_terminal_events() -> None:
    asyncio.run(_workspace_event_retention_round_trip(_dsn()))


async def _workspace_event_retention_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    provider_uuid = UUID("10000000-0000-0000-0000-0000000000e1")
    project_uuid = UUID("10000000-0000-0000-0000-0000000000e2")
    try:
        await pool.executemany(
            """
            INSERT INTO workspace_zulip_bridge.workspace_events (
                uuid, provider_uuid, workspace_project_id, epoch_version,
                object_type, action, payload, processing_status, received_at
            ) VALUES (
                $1, $2, $3, $4, 'user', 'updated', '{}'::jsonb, $5,
                clock_timestamp() - $6::interval
            )
            """,
            [
                (
                    UUID(f"10000000-0000-0000-0000-{number:012d}"),
                    provider_uuid,
                    project_uuid,
                    number,
                    status,
                    age,
                )
                for number, status, age in (
                    (201, "applied", timedelta(hours=25)),
                    (202, "skipped", timedelta(hours=25)),
                    (203, "failed", timedelta(hours=25)),
                    (204, "pending", timedelta(hours=25)),
                    (205, "processing", timedelta(hours=25)),
                    (206, "applied", timedelta(hours=23)),
                )
            ],
        )
        processor = WorkspaceEventProcessor(
            pool,
            Settings(
                database_dsn=dsn,
                workspace_provider_uuid=provider_uuid,
                workspace_project_id=project_uuid,
                event_retention_seconds=86400,
                event_cleanup_batch_size=2,
            ),
        )
        assert await processor.cleanup_expired_events() == 2
        assert await processor.cleanup_expired_events() == 1
        assert await processor.cleanup_expired_events() == 0
        rows = await pool.fetch(
            "SELECT epoch_version, processing_status "
            "FROM workspace_zulip_bridge.workspace_events "
            "WHERE provider_uuid = $1 ORDER BY epoch_version",
            provider_uuid,
        )
        assert [tuple(row) for row in rows] == [
            (204, "pending"),
            (205, "processing"),
            (206, "applied"),
        ]
    finally:
        await pool.close()


async def _event_processor_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
            member_uuid = await _insert_user(
                connection, 20, 400, queue_id="queue-member", status="filling"
            )

        catalog = _catalog(10, [(7, "Shared")], {"channel:7": 1})
        member_catalog = _catalog(20, [(7, "Shared")], {"channel:7": 1})
        assert (
            await store.store_chat_catalog(owner_uuid, "queue-owner", catalog)
        ).activated
        assert (
            await store.store_chat_catalog(member_uuid, "queue-member", member_catalog)
        ).activated
        assert (await store.reconcile_chat_schedules()).assigned == 1

        message = ZulipMessage(
            message_id=123,
            chat_key="channel:7",
            topic_name="Performance",
            sender_user_uuid=owner_uuid,
            content="first",
            is_read=False,
            is_starred=False,
            is_collapsed=False,
            is_mentioned=False,
            is_stream_wildcard_mentioned=False,
            is_topic_wildcard_mentioned=False,
            has_alert_word=False,
            is_historical=False,
            reactions_json="[]",
            message_hash=b"a" * 32,
            sent_at=1_700_000_000,
        )
        await _load_one_chat(store, pool, owner_uuid, "queue-owner", message)
        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET lifecycle_status = 'backfilling'
                WHERE uuid = $1
                """,
                owner_uuid,
            )

        owner_events = [
            {
                "id": 1,
                "type": "reaction",
                "message_id": 123,
                "op": "add",
                "user_id": 20,
                "emoji_name": "tada",
                "emoji_code": "1f389",
                "reaction_type": "unicode_emoji",
            },
            {
                "id": 2,
                "type": "update_message",
                "message_id": 123,
                "message_ids": [123],
                "stream_id": 7,
                "orig_subject": "Performance",
                "subject": "Renamed",
                "content": "second",
                "edit_timestamp": 1_700_000_100,
                "flags": [],
            },
            {
                "id": 3,
                "type": "update_message_flags",
                "op": "add",
                "flag": "read",
                "messages": [123],
                "all": False,
            },
            {
                "id": 4,
                "type": "update_message_flags",
                "op": "add",
                "flag": "starred",
                "messages": [123],
                "all": False,
            },
            {
                "id": 5,
                "type": "stream",
                "op": "update",
                "stream_id": 7,
                "property": "name",
                "value": "Renamed channel",
            },
            {
                "id": 6,
                "type": "presence",
                "presences": {
                    "20": {
                        "active_timestamp": 1_700_000_003,
                        "idle_timestamp": 1_700_000_002,
                    }
                },
            },
            {
                "id": 7,
                "type": "message",
                "message": {
                    "id": 124,
                    "type": "stream",
                    "stream_id": 7,
                    "display_recipient": "Renamed channel",
                    "subject": "Live",
                    "sender_id": 20,
                    "content": "new live message",
                    "timestamp": 1_700_000_001,
                    "reactions": [],
                },
            },
            {
                "id": 8,
                "type": "message",
                "message": {
                    "id": 125,
                    "type": "stream",
                    "stream_id": 7,
                    "display_recipient": "Renamed channel",
                    "subject": "Live",
                    "sender_id": 10,
                    "content": "second live message",
                    "timestamp": 1_700_000_002,
                    "flags": [],
                    "reactions": [],
                },
            },
            {
                "id": 9,
                "type": "reaction",
                "message_id": 124,
                "op": "add",
                "user_id": 10,
                "emoji_name": "rocket",
                "emoji_code": "1f680",
                "reaction_type": "unicode_emoji",
            },
            {
                "id": 10,
                "type": "reaction",
                "message_id": 123,
                "op": "add",
                "user_id": 20,
                "emoji_name": "heart",
                "emoji_code": "2764",
                "reaction_type": "unicode_emoji",
            },
            {
                "id": 11,
                "type": "reaction",
                "message_id": 123,
                "op": "add",
            },
            {
                "id": 12,
                "type": "stream",
                "op": "update",
                "stream_id": 7,
                "property": "description",
                "value": "Live normalized description",
                "rendered_description": "<p>Live normalized description</p>",
            },
        ]
        workspace_echo_uuid = UUID("10000000-0000-0000-0000-000000000065")
        member_events = [
            {
                "id": 1,
                "type": "reaction",
                "message_id": 123,
                "op": "add",
                "user_id": 20,
                "emoji_name": "wrong",
                "emoji_code": "274c",
                "reaction_type": "unicode_emoji",
            },
            {
                "id": 2,
                "type": "stream",
                "op": "update",
                "stream_id": 7,
                "property": "name",
                "value": "Wrong channel",
            },
            {
                "id": 3,
                "type": "message",
                "local_message_id": str(workspace_echo_uuid),
                "message": {
                    "id": 124,
                    "type": "stream",
                    "stream_id": 7,
                    "display_recipient": "Shared",
                    "subject": "Live",
                    "sender_id": 20,
                    "content": "wrong non-supplier content",
                    "timestamp": 1_700_000_001,
                    "flags": ["read", "mentioned"],
                    "reactions": [
                        {
                            "user_id": 20,
                            "emoji_name": "wrong",
                            "emoji_code": "274c",
                            "reaction_type": "unicode_emoji",
                        }
                    ],
                },
            },
            {
                "id": 4,
                "type": "reaction",
                "message_id": 124,
                "op": "add",
                "user_id": 20,
                "emoji_name": "wrong",
                "emoji_code": "274c",
                "reaction_type": "unicode_emoji",
            },
            {
                "id": 5,
                "type": "update_message_flags",
                "op": "add",
                "flag": "read",
                "messages": [123],
                "all": False,
            },
        ]
        await pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_entity_links (
                realm_uuid, entity_type, workspace_uuid, zulip_external_key
            ) VALUES ($1, 'message', $2, $3)
            """,
            stable_realm_uuid(ENDPOINT),
            workspace_echo_uuid,
            f"pending:{workspace_echo_uuid}",
        )
        assert (
            await store.store_events(
                owner_uuid,
                "queue-owner",
                [
                    ZulipEvent(
                        event_id=event["id"],
                        event_type=event["type"],
                        payload_json=json.dumps(event),
                    )
                    for event in owner_events
                ],
                12,
            )
        ) == (12, True)
        assert (
            await store.store_events(
                member_uuid,
                "queue-member",
                [
                    ZulipEvent(
                        event_id=event["id"],
                        event_type=event["type"],
                        payload_json=json.dumps(event),
                    )
                    for event in member_events
                ],
                5,
            )
        ) == (5, True)
        async with pool.acquire() as connection:
            processing_started_at = await connection.fetchval(
                "SELECT clock_timestamp()"
            )
            assert dict(
                await connection.fetch(
                    """
                    SELECT processing_status, count(*)
                    FROM workspace_zulip_bridge.zulip_events
                    GROUP BY processing_status
                    """
                )
            ) == {"pending": 17}

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_BATCH_SIZE": "100",
                    "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "0.001",
                    "WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS": "0.001",
                }
            ),
        )
        passes = []
        for _ in range(50):
            stats = await processor.process_once()
            if stats.claimed:
                passes.append(stats)
                continue
            if not await pool.fetchval(
                "SELECT EXISTS (SELECT 1 "
                "FROM workspace_zulip_bridge.zulip_events "
                "WHERE processing_status = 'pending')"
            ):
                break
            await asyncio.sleep(0.002)
        retried = sum(stats.retried for stats in passes)
        assert max(stats.claimed for stats in passes) > 1
        assert sum(stats.claimed for stats in passes) == 17 + retried
        assert (
            sum(stats.applied for stats in passes),
            sum(stats.skipped for stats in passes),
            sum(stats.failed for stats in passes),
        ) == (13, 3, 1)
        assert sum(stats.messages_changed for stats in passes) == 9
        assert sum(stats.chats_changed for stats in passes) == 2

        async with pool.acquire() as connection:
            final_message = await connection.fetchrow(
                """
                SELECT message.content,
                       flags.is_read,
                       flags.is_starred,
                       message.reactions::text AS reactions,
                       message.created_at,
                       message.source_updated_at,
                       message.updated_at,
                       topic.name AS topic_name
                FROM workspace_zulip_bridge.zulip_messages AS message
                LEFT JOIN workspace_zulip_bridge.zulip_topics AS topic
                  ON topic.uuid = message.topic_uuid
                JOIN workspace_zulip_bridge.zulip_message_flags AS flags
                  ON flags.message_uuid = message.uuid
                 AND flags.zulip_user_uuid = $1
                WHERE message.zulip_message_id = 123
                """,
                owner_uuid,
            )
            chat_metadata = await connection.fetchrow(
                """
                SELECT name, description, updated_at
                FROM workspace_zulip_bridge.zulip_streams
                WHERE chat_key = 'channel:7'
                """
            )
            statuses = dict(
                await connection.fetch(
                    """
                    SELECT processing_status, count(*)
                    FROM workspace_zulip_bridge.zulip_events
                    GROUP BY processing_status
                    """
                )
            )
            skip_reasons = dict(
                await connection.fetch(
                    """
                    SELECT outcome_reason, count(*)
                    FROM workspace_zulip_bridge.zulip_events
                    WHERE processing_status = 'skipped'
                    GROUP BY outcome_reason
                    """
                )
            )
            live_reactions = await connection.fetchval(
                """
                SELECT reactions::text
                FROM workspace_zulip_bridge.zulip_messages
                WHERE zulip_message_id = 124
                """
            )
            live_messages = await connection.fetchval(
                """
                SELECT count(*)
                FROM workspace_zulip_bridge.zulip_messages
                WHERE zulip_message_id IN (124, 125)
                """
            )
            live_flags = await connection.fetchval(
                """
                SELECT count(*)
                FROM workspace_zulip_bridge.zulip_message_flags AS flags
                JOIN workspace_zulip_bridge.zulip_messages AS message
                  ON message.uuid = flags.message_uuid
                WHERE message.zulip_message_id = 124
                  AND flags.zulip_user_uuid = $1
                """,
                owner_uuid,
            )
            member_live_message = await connection.fetchrow(
                """
                SELECT message.content, flags.is_read, flags.is_mentioned
                FROM workspace_zulip_bridge.zulip_messages AS message
                JOIN workspace_zulip_bridge.zulip_message_flags AS flags
                  ON flags.message_uuid = message.uuid
                WHERE message.zulip_message_id = 124
                  AND flags.zulip_user_uuid = $1
                """,
                member_uuid,
            )
            member_read = await connection.fetchval(
                """
                SELECT flags.is_read
                FROM workspace_zulip_bridge.zulip_message_flags AS flags
                JOIN workspace_zulip_bridge.zulip_messages AS message
                  ON message.uuid = flags.message_uuid
                WHERE flags.zulip_user_uuid = $1
                  AND message.zulip_message_id = 123
                """,
                member_uuid,
            )
            normalized_reactions = await connection.fetchval(
                "SELECT count(*) FROM workspace_zulip_bridge.zulip_message_reactions"
            )
            echo_external_key = await connection.fetchval(
                """
                SELECT zulip_external_key
                FROM workspace_zulip_bridge.zulip_entity_links
                WHERE realm_uuid = $1 AND entity_type = 'message'
                  AND workspace_uuid = $2
                """,
                stable_realm_uuid(ENDPOINT),
                workspace_echo_uuid,
            )
            member_presence = await connection.fetchrow(
                """
                SELECT presence_status, last_ping_at
                FROM workspace_zulip_bridge.zulip_users
                WHERE uuid = $1
                """,
                member_uuid,
            )
        assert final_message is not None
        assert final_message["content"] == "second"
        assert final_message["is_read"]
        assert final_message["is_starred"]
        assert final_message["topic_name"] == "Renamed"
        assert final_message["created_at"] == datetime.fromtimestamp(
            1_700_000_000, tz=UTC
        )
        assert final_message["source_updated_at"] > datetime.fromtimestamp(
            1_700_000_100, tz=UTC
        )
        assert final_message["updated_at"] >= processing_started_at
        assert json.loads(final_message["reactions"]) == [
            {
                "emoji_code": "1f389",
                "emoji_name": "tada",
                "reaction_type": "unicode_emoji",
                "user_uuid": str(member_uuid),
            },
            {
                "emoji_code": "2764",
                "emoji_name": "heart",
                "reaction_type": "unicode_emoji",
                "user_uuid": str(member_uuid),
            },
        ]
        assert chat_metadata is not None
        assert chat_metadata["name"] == "Renamed channel"
        assert chat_metadata["description"] == "Live normalized description"
        assert chat_metadata["updated_at"] >= processing_started_at
        assert json.loads(live_reactions) == [
            {
                "emoji_code": "1f680",
                "emoji_name": "rocket",
                "reaction_type": "unicode_emoji",
                "user_uuid": str(owner_uuid),
            }
        ]
        assert live_messages == 2
        assert live_flags == 0
        assert tuple(member_live_message) == ("new live message", True, True)
        assert member_read is True
        assert normalized_reactions == 3
        assert echo_external_key == "124"
        assert member_presence is not None
        assert member_presence["presence_status"] == "active"
        assert member_presence["last_ping_at"] == datetime.fromtimestamp(
            1_700_000_003, tz=UTC
        )
        assert statuses == {"applied": 13, "failed": 1, "skipped": 3}
        assert skip_reasons == {
            "not_chat_supplier": 3,
        }

        async with pool.acquire() as connection:
            stale_event_uuid = await connection.fetchval(
                """
                INSERT INTO workspace_zulip_bridge.zulip_events (
                    zulip_connection_uuid,
                    queue_id,
                    event_id,
                    event_type,
                    payload,
                    processing_status,
                    claimed_at
                )
                VALUES (
                    $1,
                    'queue-owner',
                    13,
                    'presence',
                    '{"id":13,"type":"presence"}'::jsonb,
                    'processing',
                    clock_timestamp() - interval '2 minutes'
                )
                RETURNING uuid
                """,
                owner_uuid,
            )
        recovered = await processor.process_once()
        assert (recovered.claimed, recovered.applied, recovered.failed) == (1, 0, 1)
        async with pool.acquire() as connection:
            recovered_row = await connection.fetchrow(
                """
                SELECT processing_status, attempt_count, outcome_reason
                FROM workspace_zulip_bridge.zulip_events
                WHERE uuid = $1
                """,
                stale_event_uuid,
            )
        assert recovered_row is not None
        assert dict(recovered_row) == {
            "processing_status": "failed",
            "attempt_count": 1,
            "outcome_reason": "invalid_presence_event",
        }
        async with pool.acquire() as connection:
            snapshot = await collect_snapshot(
                connection,
                window_seconds=300,
                exact=True,
            )
        assert snapshot.event_processing_statuses == {
            "applied": 13,
            "failed": 2,
            "skipped": 3,
        }
        assert snapshot.oldest_pending_event_seconds == 0
    finally:
        await pool.close()


async def _load_one_chat(
    store: EventStore,
    pool: asyncpg.Pool,
    user_uuid: UUID,
    queue_id: str,
    message: ZulipMessage,
    *,
    expected_changed: int = 1,
    expected_schedules_loaded: int = 1,
) -> UUID:
    pending = await store.list_pending_history_chats(user_uuid, queue_id)
    assert [chat.chat_key for chat in pending] == ["channel:7"]
    history = await store.begin_history(user_uuid, queue_id)
    try:
        write = await history.store_page([message])
        assert (write.received, write.changed, write.unassigned) == (
            1,
            expected_changed,
            0,
        )
        finished = await history.finish(["channel:7"])
        assert finished.activated
        assert finished.schedules_loaded == expected_schedules_loaded
    finally:
        await history.close()
    async with pool.acquire() as connection:
        return await connection.fetchval(
            "SELECT uuid FROM workspace_zulip_bridge.zulip_messages"
        )
