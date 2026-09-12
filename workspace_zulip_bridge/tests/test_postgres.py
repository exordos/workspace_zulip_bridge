# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import json
import os
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from uuid import UUID

import asyncpg
import pytest

from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder
from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.database import open_pool
from workspace_zulip_bridge.database import prepare_database
from workspace_zulip_bridge.event_processor import ZulipEventProcessor
from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.models import UserDirectoryWrite
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.models import ZulipEvent
from workspace_zulip_bridge.models import ZulipMessage
from workspace_zulip_bridge.monitor import collect_snapshot
from workspace_zulip_bridge.stable_ids import stable_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid

ENDPOINT = "https://zulip.example.test"


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
        await connection.execute("TRUNCATE workspace_zulip_bridge.zulip_users CASCADE")
    return pool


def test_workspace_entity_mirrors_match_source_columns_and_start_empty() -> None:
    asyncio.run(_workspace_mirror_round_trip(_dsn()))


async def _workspace_mirror_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        table_pairs = (
            ("zulip_chats", "workspace_chats"),
            ("zulip_topics", "workspace_topics"),
            ("zulip_messages", "workspace_messages"),
        )
        async with pool.acquire() as connection:
            for source_table, mirror_table in table_pairs:
                columns = await connection.fetch(
                    """
                    SELECT table_name,
                           column_name,
                           data_type,
                           is_nullable,
                           column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'workspace_zulip_bridge'
                      AND table_name = ANY($1::text[])
                    ORDER BY ordinal_position
                    """,
                    [source_table, mirror_table],
                )
                by_table = {
                    table: [
                        tuple(
                            row[key]
                            for key in (
                                "column_name",
                                "data_type",
                                "is_nullable",
                                "column_default",
                            )
                        )
                        for row in columns
                        if row["table_name"] == table
                    ]
                    for table in (source_table, mirror_table)
                }
                assert by_table[mirror_table] == by_table[source_table]
                assert (
                    await connection.fetchval(
                        "SELECT count(*) FROM workspace_zulip_bridge." + mirror_table
                    )
                    == 0
                )
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
) -> UUID:
    user_uuid = stable_user_uuid(ENDPOINT, user_id)
    await connection.execute(
        """
        INSERT INTO workspace_zulip_bridge.zulip_users (
            uuid,
            endpoint,
            login,
            api_key,
            zulip_user_id,
            full_name,
            role,
            queue_id,
            last_event_id,
            status
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9)
        """,
        user_uuid,
        ENDPOINT,
        f"user-{user_id}@example.test",
        api_key,
        user_id,
        f"User {user_id}",
        role,
        queue_id,
        status,
    )
    return user_uuid


def _catalog(
    own_user_id: int,
    channels: list[tuple[int, str]],
    counts: dict[str, int],
):
    builder = ChatCatalogBuilder(own_user_id, f"User {own_user_id}")
    builder.add_subscriptions(
        [{"stream_id": stream_id, "name": name} for stream_id, name in channels]
    )
    return builder.build(counts)


def test_directory_uses_stable_user_ids_and_excludes_bots() -> None:
    asyncio.run(_directory_round_trip(_dsn()))


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
        assert result == UserDirectoryWrite(humans=1, changed=1)
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT uuid, login, full_name, role, disabled, api_key
                FROM workspace_zulip_bridge.zulip_users
                """
            )
        assert len(rows) == 1
        assert rows[0]["uuid"] == user_uuid
        assert rows[0]["login"] == "user-10@example.test"
        assert rows[0]["full_name"] == "Renamed User"
        assert rows[0]["role"] == 200
        assert rows[0]["disabled"]
        assert rows[0]["api_key"] == "api-key-placeholder"
    finally:
        await pool.close()


def test_scheduler_uses_role_then_stable_uuid() -> None:
    asyncio.run(_scheduler_round_trip(_dsn()))


async def _scheduler_round_trip(dsn: str) -> None:
    pool = await _pool(dsn)
    try:
        store = EventStore(pool)
        async with pool.acquire() as connection:
            owner_uuid = await _insert_user(
                connection, 10, 100, queue_id="queue-owner", status="filling"
            )
            admin_a_uuid = await _insert_user(
                connection, 20, 200, queue_id="queue-admin-a", status="filling"
            )
            admin_b_uuid = await _insert_user(
                connection, 30, 200, queue_id="queue-admin-b", status="filling"
            )

        owner_catalog = _catalog(10, [(7, "Shared")], {"channel:7": 1})
        admin_a_catalog = _catalog(
            20,
            [(7, "Shared"), (8, "Admins"), (9, "Tie")],
            {"channel:7": 100, "channel:8": 5, "channel:9": 2},
        )
        admin_b_catalog = _catalog(
            30,
            [(7, "Shared"), (8, "Admins"), (9, "Tie")],
            {"channel:7": 1000, "channel:8": 9, "channel:9": 2},
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
                row["chat_key"]: row["supplier_user_uuid"]
                for row in await connection.fetch(
                    """
                    SELECT chat_key, supplier_user_uuid
                    FROM workspace_zulip_bridge.zulip_chats
                    ORDER BY chat_key
                    """
                )
            }
            statuses = {
                row["uuid"]: row["status"]
                for row in await connection.fetch(
                    """
                    SELECT uuid, status
                    FROM workspace_zulip_bridge.zulip_users
                    """
                )
            }
        assert assignments == {
            "channel:7": owner_uuid,
            "channel:8": min(admin_a_uuid, admin_b_uuid),
            "channel:9": min(admin_a_uuid, admin_b_uuid),
        }
        assert statuses == {
            owner_uuid: "backfilling",
            admin_a_uuid: "active",
            admin_b_uuid: "backfilling",
        }

        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET status = 'backfilling'
                WHERE uuid = $1
                """,
                admin_a_uuid,
            )
        await store.reconcile_chat_schedules()
        async with pool.acquire() as connection:
            assert (
                await connection.fetchval(
                    """
                    SELECT status
                    FROM workspace_zulip_bridge.zulip_users
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
            assert (
                await connection.fetchval(
                    """
                    SELECT supplier_user_uuid
                    FROM workspace_zulip_bridge.zulip_chats
                    WHERE chat_key = 'channel:7'
                    """
                )
                == admin_b_uuid
            )
    finally:
        await pool.close()


def test_history_ids_survive_queue_loss_and_supplier_deletion() -> None:
    asyncio.run(_history_round_trip(_dsn()))


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
        chat_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
        assert first_message_uuid == stable_message_uuid(ENDPOINT, 123)
        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT topic_uuid, content, is_starred, created_at, updated_at
                FROM workspace_zulip_bridge.zulip_messages
                WHERE uuid = $1
                """,
                first_message_uuid,
            )
        assert row is not None
        assert row["topic_uuid"] == stable_topic_uuid(chat_uuid, "Performance")
        assert row["content"] == "first"
        assert row["is_starred"]
        assert int(row["created_at"].timestamp()) == 1_700_000_000

        assert await store.clear_queue(owner_uuid, "queue-owner")
        async with pool.acquire() as connection:
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.zulip_messages"
                )
                == 0
            )
            queue_reset = await connection.fetchrow(
                """
                SELECT status, queue_id, catalog_completed_at
                FROM workspace_zulip_bridge.zulip_users
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
                UPDATE workspace_zulip_bridge.zulip_users
                SET queue_id = 'queue-owner-2',
                    last_event_id = 0,
                    status = 'filling'
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
                DELETE FROM workspace_zulip_bridge.zulip_users
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
                == 0
            )
            assert (
                await connection.fetchval(
                    """
                    SELECT supplier_user_uuid
                    FROM workspace_zulip_bridge.zulip_chats
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
                SELECT uuid, zulip_user_uuid, content
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


def test_event_processor_applies_only_the_selected_chat_supplier() -> None:
    asyncio.run(_event_processor_round_trip(_dsn()))


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
                    zulip_user_uuid,
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
                "edit_timestamp": 1,
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
            {"id": 6, "type": "presence"},
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
                    "flags": [],
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
        ]
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
                "message": {
                    "id": 124,
                    "type": "stream",
                    "stream_id": 7,
                    "display_recipient": "Shared",
                    "subject": "Live",
                    "sender_id": 20,
                    "content": "new live message",
                    "timestamp": 1_700_000_001,
                    "flags": [],
                    "reactions": [],
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
        ]
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
                11,
            )
        ) == (11, True)
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
                4,
            )
        ) == (4, True)
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
            ) == {"pending": 15}

        processor = ZulipEventProcessor(
            pool,
            store,
            Settings.from_env(
                {
                    "WZB_DATABASE_DSN": dsn,
                    "WZB_EVENT_PROCESSOR_BATCH_SIZE": "100",
                }
            ),
        )
        stats = await processor.process_once()
        assert (stats.claimed, stats.applied, stats.skipped, stats.failed) == (
            15,
            9,
            5,
            1,
        )
        assert stats.messages_changed == 7
        assert stats.chats_changed == 1

        async with pool.acquire() as connection:
            final_message = await connection.fetchrow(
                """
                SELECT message.content,
                       message.is_read,
                       message.is_starred,
                       message.reactions::text AS reactions,
                       message.created_at,
                       message.updated_at,
                       topic.name AS topic_name
                FROM workspace_zulip_bridge.zulip_messages AS message
                LEFT JOIN workspace_zulip_bridge.zulip_topics AS topic
                  ON topic.uuid = message.topic_uuid
                WHERE message.zulip_message_id = 123
                """
            )
            chat_name = await connection.fetchval(
                """
                SELECT name
                FROM workspace_zulip_bridge.zulip_chats
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
        assert final_message is not None
        assert final_message["content"] == "second"
        assert final_message["is_read"]
        assert final_message["is_starred"]
        assert final_message["topic_name"] == "Renamed"
        assert final_message["created_at"] == datetime.fromtimestamp(
            1_700_000_000, tz=UTC
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
        assert chat_name == "Renamed channel"
        assert json.loads(live_reactions) == [
            {
                "emoji_code": "1f680",
                "emoji_name": "rocket",
                "reaction_type": "unicode_emoji",
                "user_uuid": str(owner_uuid),
            }
        ]
        assert live_messages == 2
        assert statuses == {"applied": 9, "failed": 1, "skipped": 5}
        assert skip_reasons == {
            "not_chat_supplier": 4,
            "unsupported_event_type": 1,
        }

        async with pool.acquire() as connection:
            stale_event_uuid = await connection.fetchval(
                """
                INSERT INTO workspace_zulip_bridge.zulip_events (
                    zulip_user_uuid,
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
                    12,
                    'presence',
                    '{"id":12,"type":"presence"}'::jsonb,
                    'processing',
                    clock_timestamp() - interval '2 minutes'
                )
                RETURNING uuid
                """,
                owner_uuid,
            )
        recovered = await processor.process_once()
        assert (recovered.claimed, recovered.applied, recovered.skipped) == (1, 0, 1)
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
            "processing_status": "skipped",
            "attempt_count": 1,
            "outcome_reason": "unsupported_event_type",
        }
        async with pool.acquire() as connection:
            snapshot = await collect_snapshot(
                connection,
                window_seconds=300,
                exact=True,
            )
        assert snapshot.event_processing_statuses == {
            "applied": 9,
            "failed": 1,
            "skipped": 6,
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
) -> UUID:
    pending = await store.list_pending_history_chats(user_uuid, queue_id)
    assert [chat.chat_key for chat in pending] == ["channel:7"]
    history = await store.begin_history(user_uuid, queue_id)
    try:
        write = await history.store_page([message])
        assert (write.received, write.changed, write.unassigned) == (1, 1, 0)
        finished = await history.finish(["channel:7"])
        assert finished.activated
        assert finished.schedules_loaded == 1
    finally:
        await history.close()
    async with pool.acquire() as connection:
        return await connection.fetchval(
            "SELECT uuid FROM workspace_zulip_bridge.zulip_messages"
        )
