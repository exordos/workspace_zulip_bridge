#!/usr/bin/env python3
# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Run a repeatable synthetic Zulip history import against a disposable DB."""

import argparse
import asyncio
import hashlib
import json
import os
import time

from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder
from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.database import open_pool
from workspace_zulip_bridge.database import prepare_database
from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.message_history import build_message_page
from workspace_zulip_bridge.models import ZulipAttachment
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.stable_ids import stable_realm_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid

ENDPOINT = "https://benchmark.zulip.invalid"
USERS = 275
STREAMS = 50
TOPICS_PER_STREAM = 20


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages", type=int, default=100_000)
    parser.add_argument("--page-size", type=int, default=5_000)
    return parser.parse_args()


def _raw_message(index: int) -> dict[str, object]:
    stream_id = index % STREAMS + 1
    sender_id = index % USERS + 1
    flags = ["read"] if index % 2 else []
    if index % 100 == 0:
        flags.append("starred")
    if index % 10 == 0:
        flags.append("mentioned")
    reactions = []
    if index % 5 == 0:
        reactions.append(
            {
                "user_id": (sender_id % USERS) + 1,
                "emoji_name": "thumbs_up",
                "emoji_code": "1f44d",
                "reaction_type": "unicode_emoji",
            }
        )
    content = f"message {index}"
    if index % 50 == 0:
        content += f" [report-{index}.txt](/user_uploads/1/report-{index}.txt)"
    return {
        "id": index + 1,
        "sender_id": sender_id,
        "content": content,
        "timestamp": 1_700_000_000 + index,
        "type": "stream",
        "stream_id": stream_id,
        "display_recipient": f"Stream {stream_id}",
        "subject": f"Topic {index % TOPICS_PER_STREAM}",
        "flags": flags,
        "reactions": reactions,
    }


async def _run(messages: int, page_size: int) -> None:
    dsn = os.environ.get("WZB_BENCHMARK_DATABASE_DSN")
    if not dsn or not any(marker in dsn for marker in ("test", "benchmark")):
        raise RuntimeError(
            "WZB_BENCHMARK_DATABASE_DSN must name an explicit disposable test database"
        )
    settings = Settings.from_env(
        {
            "WZB_DATABASE_DSN": dsn,
            "WZB_DB_POOL_MIN_SIZE": "1",
            "WZB_DB_POOL_MAX_SIZE": "8",
            "WZB_ZULIP_HISTORY_CONCURRENCY": "2",
        }
    )
    pool = await open_pool(settings)
    async with pool.acquire() as connection:
        await connection.execute("DROP SCHEMA IF EXISTS workspace_zulip_bridge CASCADE")
    await prepare_database(pool)
    store = EventStore(pool)
    supplier_uuid = stable_user_uuid(ENDPOINT, 1)
    directory = [
        ZulipDirectoryUser(
            user_id=user_id,
            login=f"user-{user_id}@example.test",
            full_name=f"User {user_id}",
            role=100 if user_id == 1 else 400,
            disabled=False,
            is_bot=False,
        )
        for user_id in range(1, USERS + 1)
    ]
    await store.store_user_directory(ENDPOINT, directory)
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO workspace_zulip_bridge.zulip_connections
                (uuid, realm_uuid, zulip_user_uuid, login, api_key,
                 queue_id, last_event_id, lifecycle_status)
            VALUES ($1, $2, $1, 'user-1@example.test', 'benchmark-key',
                    'benchmark-queue', 0, 'filling')
            """,
            supplier_uuid,
            stable_realm_uuid(ENDPOINT),
        )
    attachments = [
        ZulipAttachment(
            attachment_id=index // 50 + 1,
            source_path=f"/user_uploads/1/report-{index}.txt",
            name=f"report-{index}.txt",
            size_bytes=1_024 + index,
            created_at=1_700_000_000 + index,
            message_ids=(index + 1,),
            metadata_hash=hashlib.sha256(str(index).encode()).digest(),
        )
        for index in range(0, messages, 50)
    ]
    await store.store_user_attachments(
        supplier_uuid,
        "benchmark-queue",
        attachments,
        replace_all=True,
    )
    builder = ChatCatalogBuilder(1, "User 1", 100)
    builder.add_subscriptions(
        [
            {"stream_id": stream_id, "name": f"Stream {stream_id}"}
            for stream_id in range(1, STREAMS + 1)
        ]
    )
    await store.store_chat_catalog(supplier_uuid, "benchmark-queue", builder.build())
    await store.reconcile_chat_schedules()
    history = await store.begin_history(supplier_uuid, "benchmark-queue")
    user_uuids = {
        user_id: stable_user_uuid(ENDPOINT, user_id) for user_id in range(1, USERS + 1)
    }
    stream_ids = {
        f"Stream {stream_id}": stream_id for stream_id in range(1, STREAMS + 1)
    }
    allowed = {f"channel:{stream_id}" for stream_id in range(1, STREAMS + 1)}
    started = time.perf_counter()
    changed = 0
    for offset in range(0, messages, page_size):
        raw = [
            _raw_message(index)
            for index in range(offset, min(offset + page_size, messages))
        ]
        built = build_message_page(
            raw,
            own_user_id=1,
            user_uuids=user_uuids,
            stream_ids_by_name=stream_ids,
            allowed_chat_keys=allowed,
        )
        result = await history.store_page(built.messages)
        changed += result.changed
    await history.finish(sorted(allowed))
    elapsed = time.perf_counter() - started
    await history.close()
    tables = (
        "zulip_realms",
        "zulip_users",
        "zulip_connections",
        "zulip_streams",
        "zulip_stream_bindings",
        "zulip_topics",
        "zulip_topic_aliases",
        "zulip_messages",
        "zulip_message_flags",
        "zulip_message_reactions",
        "zulip_files",
        "zulip_message_files",
        "workspace_outbox",
    )
    async with pool.acquire() as connection:
        counts = {
            table: await connection.fetchval(
                f"SELECT count(*) FROM workspace_zulip_bridge.{table}"
            )
            for table in tables
        }
        database_bytes = await connection.fetchval(
            "SELECT pg_database_size(current_database())"
        )
        schema_bytes = await connection.fetchval(
            """
            SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' ||
                                              quote_ident(tablename)))
            FROM pg_tables WHERE schemaname = 'workspace_zulip_bridge'
            """
        )
    print(
        json.dumps(
            {
                "version": "normalized",
                "messages_requested": messages,
                "messages_changed": changed,
                "elapsed_seconds": round(elapsed, 6),
                "messages_per_second": round(messages / elapsed, 2),
                "database_bytes": database_bytes,
                "schema_bytes": int(schema_bytes),
                "counts": counts,
            },
            sort_keys=True,
        )
    )
    await pool.close()


def main() -> None:
    args = _arguments()
    asyncio.run(_run(args.messages, args.page_size))


if __name__ == "__main__":
    main()
