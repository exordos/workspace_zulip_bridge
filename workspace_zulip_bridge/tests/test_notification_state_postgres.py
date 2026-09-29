# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.event_store import _store_channel_notification_modes
from workspace_zulip_bridge.models import ZulipUserTopic
from workspace_zulip_bridge.stable_ids import stable_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_realm_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.tests.test_postgres import ENDPOINT
from workspace_zulip_bridge.tests.test_postgres import _catalog
from workspace_zulip_bridge.tests.test_postgres import _dsn
from workspace_zulip_bridge.tests.test_postgres import _insert_user
from workspace_zulip_bridge.tests.test_postgres import _pool
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker


async def _setup():
    pool = await _pool(_dsn())
    store = EventStore(pool)
    async with pool.acquire() as connection:
        owner = await _insert_user(
            connection, 78, 400, queue_id="preferences", status="active"
        )
    catalog = _catalog(78, [(7, "Channel")], {"channel:7": 1})
    await store.store_chat_catalog(owner, "preferences", catalog)
    return pool, store, owner, catalog


async def _snapshot(store, owner, catalog, topics, observed_at):
    return await store.store_notification_snapshot(
        owner,
        "preferences",
        catalog.chats,
        topics,
        enable_stream_desktop_notifications=True,
        observed_at=observed_at,
    )


@pytest.mark.parametrize(
    "legacy, observe_before_defaults", [(False, False), (False, True), (True, False)]
)
def test_snapshot_repairs_generated_defaults(legacy, observe_before_defaults):
    async def run():
        pool, store, owner, catalog = await _setup()
        try:
            stream = stable_chat_uuid(ENDPOINT, "channel:7")
            topic = stable_topic_uuid(stream, "Old preference")
            await pool.execute(
                "INSERT INTO workspace_zulip_bridge.zulip_topics "
                "(uuid, zulip_stream_uuid, name, content_hash) VALUES ($1, $2, $3, $4)",
                topic,
                stream,
                "Old preference",
                b"t" * 32,
            )
            started_at = datetime.now(UTC)
            worker = object.__new__(WorkspaceDiffWorker)
            worker._pool = pool
            await worker._ensure_topic_bindings(stable_realm_uuid(ENDPOINT))
            if legacy:
                # Reproduce the old upgrade's backfill from local write time.
                await pool.execute(
                    "UPDATE workspace_zulip_bridge.zulip_topic_bindings "
                    "SET source_updated_at = updated_at"
                )
            observed_at = started_at if observe_before_defaults else datetime.now(UTC)
            await _snapshot(
                store,
                owner,
                catalog,
                (ZulipUserTopic(7, "Old preference", 1, 1_700_000_000),),
                observed_at,
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_topic_bindings"
                )
                == "mute"
            )
        finally:
            await pool.close()

    asyncio.run(run())


def test_unchanged_topic_event_advances_version_without_outbox_churn():
    async def run():
        pool, store, owner, _ = await _setup()
        try:
            await store.store_user_topics(
                owner,
                "preferences",
                (ZulipUserTopic(7, "Ordering", 3, 100),),
                replace_all=False,
            )
            await pool.execute("DELETE FROM workspace_zulip_bridge.workspace_outbox")
            assert (
                await store.store_user_topics(
                    owner,
                    "preferences",
                    (ZulipUserTopic(7, "Ordering", 3, 300),),
                    replace_all=False,
                )
                == 0
            )
            assert (
                await store.store_user_topics(
                    owner,
                    "preferences",
                    (ZulipUserTopic(7, "Ordering", 1, 200),),
                    replace_all=False,
                )
                == 0
            )
            row = await pool.fetchrow(
                "SELECT notification_mode, extract(epoch FROM source_updated_at)::int FROM workspace_zulip_bridge.zulip_topic_bindings"
            )
            assert tuple(row) == ("follow", 300)
            assert (
                await pool.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox"
                )
                == 0
            )
        finally:
            await pool.close()

    asyncio.run(run())


def test_empty_snapshot_fences_an_already_default_binding():
    async def run():
        pool, store, owner, catalog = await _setup()
        try:
            await store.store_user_topics(
                owner,
                "preferences",
                (ZulipUserTopic(7, "Default", 0, 100),),
                replace_all=False,
            )
            at = datetime.now(UTC)
            await _snapshot(store, owner, catalog, (), at)
            await store.store_user_topics(
                owner,
                "preferences",
                (ZulipUserTopic(7, "Default", 1, 200),),
                replace_all=False,
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_topic_bindings"
                )
                == "default"
            )
        finally:
            await pool.close()

    asyncio.run(run())


def test_unchanged_channel_snapshot_advances_version():
    async def run():
        pool, _, owner, catalog = await _setup()
        try:
            at = datetime.now(UTC) + timedelta(seconds=1)
            async with pool.acquire() as connection:
                changed = await _store_channel_notification_modes(
                    connection,
                    stable_realm_uuid(ENDPOINT),
                    owner,
                    catalog.chats,
                    observed_at=at,
                )
            assert changed == 0
            assert (
                await pool.fetchval(
                    "SELECT source_updated_at FROM workspace_zulip_bridge.zulip_stream_bindings"
                )
                == at
            )
        finally:
            await pool.close()

    asyncio.run(run())


@pytest.mark.parametrize("snapshot_contains_topic", [False, True])
def test_snapshot_waiting_for_newer_realtime_cannot_overwrite_it(
    snapshot_contains_topic,
):
    async def run():
        pool, store, owner, catalog = await _setup()
        try:
            old = int(datetime.now(UTC).timestamp()) - 20
            await store.store_user_topics(
                owner,
                "preferences",
                (ZulipUserTopic(7, "Concurrent", 3, old),),
                replace_all=False,
            )
            observed_at = datetime.now(UTC)
            async with pool.acquire() as connection, connection.transaction():
                # Hold the same owner lock as EventStore.store_user_topics and
                # commit an equal-state realtime event after observation began.
                await connection.fetchval(
                    "SELECT uuid FROM workspace_zulip_bridge.zulip_connections WHERE uuid = $1 FOR UPDATE",
                    owner,
                )
                from workspace_zulip_bridge.event_store import _store_user_topics

                await _store_user_topics(
                    connection,
                    stable_realm_uuid(ENDPOINT),
                    owner,
                    (ZulipUserTopic(7, "Concurrent", 3, old + 5),),
                    replace_all=False,
                )
                topics = (
                    (ZulipUserTopic(7, "Concurrent", 1, old - 10),)
                    if snapshot_contains_topic
                    else ()
                )
                pending = asyncio.create_task(
                    _snapshot(store, owner, catalog, topics, observed_at)
                )
                # Verify PostgreSQL has reached the contested row lock.
                for _ in range(200):
                    if await pool.fetchval(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%notification_settings_generation%' AND pid <> pg_backend_pid())"
                    ):
                        break
                    await asyncio.sleep(0.005)
                else:
                    raise AssertionError(
                        "snapshot did not wait on the realtime transaction"
                    )
                assert not pending.done()
            await asyncio.wait_for(pending, timeout=3)
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_topic_bindings"
                )
                == "follow"
            )
            assert (
                await store.store_user_topics(
                    owner,
                    "preferences",
                    (
                        ZulipUserTopic(
                            7, "Concurrent", 1, int(observed_at.timestamp()) + 1
                        ),
                    ),
                    replace_all=False,
                )
                == 1
            )
        finally:
            await pool.close()

    asyncio.run(run())


def test_snapshot_reset_and_generation_repair_are_durable():
    async def run():
        from workspace_zulip_bridge.database import prepare_database
        from workspace_zulip_bridge.models import NOTIFICATION_SETTINGS_GENERATION

        pool, store, owner, catalog = await _setup()
        try:
            await store.store_user_topics(
                owner,
                "preferences",
                (ZulipUserTopic(7, "Removed", 1, 100),),
                replace_all=False,
            )
            await pool.execute(
                "UPDATE workspace_zulip_bridge.zulip_connections SET notification_settings_generation = 1"
            )
            at = datetime.now(UTC)
            assert await _snapshot(store, owner, catalog, (), at) == 1
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == NOTIFICATION_SETTINGS_GENERATION
            )
            await prepare_database(pool)
            restarted = EventStore(pool)
            assert (
                await _snapshot(
                    restarted,
                    owner,
                    catalog,
                    (ZulipUserTopic(7, "Removed", 1, 100),),
                    at - timedelta(seconds=1),
                )
                == 0
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_topic_bindings"
                )
                == "default"
            )
            # A delayed event for a binding absent from the snapshot is fenced,
            # including a topic which has not been materialized yet.
            assert (
                await restarted.store_user_topics(
                    owner,
                    "preferences",
                    (ZulipUserTopic(7, "Unknown old topic", 1, 100),),
                    replace_all=False,
                )
                == 0
            )
            assert (
                await pool.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.zulip_topics"
                )
                == 1
            )
            # Re-registration applies new snapshots even at the current repair
            # generation. An old catalogue replay must not resurrect new topics.
            await restarted.store_chat_catalog(
                owner,
                "preferences",
                catalog,
                bootstrap_user_topics=(
                    ZulipUserTopic(7, "Stale snapshot topic", 1, 100),
                ),
                notification_snapshot_at=at - timedelta(seconds=1),
            )
            assert (
                await pool.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.zulip_topics"
                )
                == 1
            )
            fresh = datetime.now(UTC)
            await restarted.store_chat_catalog(
                owner,
                "preferences",
                catalog,
                bootstrap_user_topics=(ZulipUserTopic(7, "Removed", 3, 200),),
                notification_snapshot_at=fresh,
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_topic_bindings"
                )
                == "follow"
            )
            # Retrying the same observed snapshot is a payload no-op.
            await pool.execute("DELETE FROM workspace_zulip_bridge.workspace_outbox")
            await restarted.store_chat_catalog(
                owner,
                "preferences",
                catalog,
                bootstrap_user_topics=(ZulipUserTopic(7, "Removed", 3, 200),),
                notification_snapshot_at=fresh,
            )
            assert (
                await pool.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox"
                )
                == 0
            )
            # Equal provider seconds cannot prove whether an event precedes or
            # follows registration. Defer it to a durable source reconciliation.
            assert (
                await restarted.store_user_topics(
                    owner,
                    "preferences",
                    (ZulipUserTopic(7, "Removed", 2, int(fresh.timestamp())),),
                    replace_all=False,
                )
                == 0
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == 1
            )
            # Retrying the same bootstrap snapshot must not clear that request.
            await restarted.store_chat_catalog(
                owner,
                "preferences",
                catalog,
                bootstrap_user_topics=(ZulipUserTopic(7, "Removed", 3, 200),),
                notification_snapshot_at=fresh,
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == 1
            )
            # Nor may another repair snapshot in the same observation second.
            assert await _snapshot(restarted, owner, catalog, (), fresh) is None
        finally:
            await pool.close()

    asyncio.run(run())


def test_snapshot_aliases_use_latest_explicit_preference_once():
    async def run():
        pool, store, owner, catalog = await _setup()
        try:
            await store.store_user_topics(
                owner,
                "preferences",
                (ZulipUserTopic(7, "Current", 0, 10),),
                replace_all=False,
            )
            stream = stable_chat_uuid(ENDPOINT, "channel:7")
            topic = stable_topic_uuid(stream, "Current")
            await pool.execute(
                "INSERT INTO workspace_zulip_bridge.zulip_topic_aliases (zulip_stream_uuid, alias, topic_uuid) VALUES ($1, 'Previous', $2)",
                stream,
                topic,
            )
            assert (
                await _snapshot(
                    store,
                    owner,
                    catalog,
                    (
                        ZulipUserTopic(7, "Current", 1, 100),
                        ZulipUserTopic(7, "Previous", 3, 200),
                        ZulipUserTopic(7, "CURRENT", 2, 150),
                    ),
                    datetime.now(UTC),
                )
                == 1
            )
            rows = await pool.fetch(
                "SELECT topic_uuid, notification_mode FROM workspace_zulip_bridge.zulip_topic_bindings"
            )
            assert [tuple(row) for row in rows] == [(topic, "follow")]
        finally:
            await pool.close()

    asyncio.run(run())


def test_stale_catalog_cannot_overwrite_newer_channel_or_its_fence():
    async def run():
        from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder

        pool, store, owner, catalog = await _setup()
        try:
            old = datetime.now(UTC)
            new = old + timedelta(seconds=1)
            builder = ChatCatalogBuilder(78, "User 78", 400)
            builder.add_subscriptions(
                [{"stream_id": 7, "name": "Channel", "is_muted": True}]
            )
            muted = builder.build({"channel:7": 1})
            await store.store_chat_catalog(
                owner, "preferences", muted, catalog_observed_at=new
            )
            await store.store_chat_catalog(
                owner, "preferences", catalog, catalog_observed_at=old
            )
            row = await pool.fetchrow(
                "SELECT notification_mode, source_updated_at FROM workspace_zulip_bridge.zulip_stream_bindings"
            )
            assert tuple(row) == ("muted", new)
            # Even when the stale catalog content hash now matches the cached
            # connection hash, its timestamp must not bypass the stream fence.
            await store.store_chat_catalog(
                owner, "preferences", catalog, catalog_observed_at=old
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_stream_bindings"
                )
                == "muted"
            )
        finally:
            await pool.close()

    asyncio.run(run())


def test_equal_global_setting_fences_stale_snapshot():
    async def run():
        pool, store, owner, catalog = await _setup()
        try:
            from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder

            builder = ChatCatalogBuilder(78, "User 78", 400)
            builder.add_subscriptions(
                [{"stream_id": 7, "name": "Channel"}],
                desktop_notifications_default=False,
            )
            stale_catalog = builder.build({"channel:7": 1})
            at = datetime.now(UTC)
            assert (
                await store.store_user_notification_setting(
                    owner, "preferences", enable_stream_desktop_notifications=True
                )
                is False
            )
            await store.store_notification_snapshot(
                owner,
                "preferences",
                stale_catalog.chats,
                (),
                enable_stream_desktop_notifications=False,
                observed_at=at,
            )
            assert (
                await pool.fetchval(
                    "SELECT enable_stream_desktop_notifications FROM workspace_zulip_bridge.zulip_connections"
                )
                is True
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_stream_bindings"
                )
                == "all_messages"
            )
        finally:
            await pool.close()

    asyncio.run(run())


@pytest.mark.parametrize("bootstrap", [False, True])
@pytest.mark.parametrize("enabled", [False, True])
def test_newer_global_default_preserves_explicit_channel_preferences(
    bootstrap, enabled
):
    async def run():
        from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder

        pool, store, owner, _ = await _setup()
        try:
            channels = [(7, "Inherited"), (8, "Explicit"), (9, "Muted")]
            base = _catalog(78, channels, {})
            await store.store_chat_catalog(owner, "preferences", base)
            builder = ChatCatalogBuilder(78, "User 78", 400)
            builder.add_subscriptions(
                [
                    {"stream_id": 7, "name": "Inherited"},
                    {
                        "stream_id": 8,
                        "name": "Explicit",
                        "desktop_notifications": not enabled,
                    },
                    {"stream_id": 9, "name": "Muted", "is_muted": True},
                ],
                desktop_notifications_default=not enabled,
            )
            stale = builder.build()
            observed_at = datetime.now(UTC)
            await store.store_user_notification_setting(
                owner, "preferences", enable_stream_desktop_notifications=enabled
            )
            if bootstrap:
                await store.store_chat_catalog(
                    owner,
                    "preferences",
                    stale,
                    bootstrap_user_topics=(),
                    notification_snapshot_at=observed_at,
                    catalog_observed_at=observed_at,
                    enable_stream_desktop_notifications=not enabled,
                )
            else:
                await store.store_notification_snapshot(
                    owner,
                    "preferences",
                    stale.chats,
                    (),
                    enable_stream_desktop_notifications=not enabled,
                    observed_at=observed_at,
                )
            if not bootstrap:
                from workspace_zulip_bridge.chat_catalog import (
                    with_stream_notification_default,
                )

                # The first read began before the event requested reconciliation.
                # Retry with a later authoritative read before accepting settings.
                await store.store_notification_snapshot(
                    owner,
                    "preferences",
                    with_stream_notification_default(stale.chats, enabled),
                    (),
                    enable_stream_desktop_notifications=enabled,
                    observed_at=datetime.now(UTC) + timedelta(seconds=2),
                )
            modes = await pool.fetch(
                "SELECT stream.chat_key, binding.notification_mode FROM workspace_zulip_bridge.zulip_stream_bindings AS binding JOIN workspace_zulip_bridge.zulip_streams AS stream ON stream.uuid = binding.zulip_stream_uuid ORDER BY stream.chat_key"
            )
            assert [tuple(row) for row in modes] == [
                ("channel:7", "all_messages" if enabled else "mentions_only"),
                ("channel:8", "mentions_only" if enabled else "all_messages"),
                ("channel:9", "muted"),
            ]
        finally:
            await pool.close()

    asyncio.run(run())


def test_notification_fence_upgrade_keeps_legacy_preferences():
    async def run():
        from workspace_zulip_bridge.database import prepare_database

        pool, store, owner, _ = await _setup()
        try:
            await store.store_user_topics(
                owner,
                "preferences",
                (ZulipUserTopic(7, "Legacy", 3, 100),),
                replace_all=False,
            )
            await pool.execute(
                "ALTER TABLE workspace_zulip_bridge.zulip_connections DROP COLUMN notification_snapshot_at"
            )
            await pool.execute(
                "UPDATE workspace_zulip_bridge.zulip_connections SET notification_settings_generation = 1"
            )
            await prepare_database(pool)
            assert await pool.fetchval(
                "SELECT notification_snapshot_at FROM workspace_zulip_bridge.zulip_connections"
            ) == datetime.fromtimestamp(0, UTC)
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == 1
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_topic_bindings"
                )
                == "follow"
            )
        finally:
            await pool.close()

    asyncio.run(run())


@pytest.mark.parametrize("source_mode", [3, 1])
def test_retained_queue_same_second_event_requires_source_reconciliation(source_mode):
    async def run():
        from workspace_zulip_bridge.models import NOTIFICATION_SETTINGS_GENERATION

        pool, store, owner, catalog = await _setup()
        try:
            # This fresh registration snapshot coexists with the OLD queue.
            # The identical source second cannot prove which side of snapshot
            # observation the retained event came from.
            observed_at = datetime.now(UTC) - timedelta(seconds=2)
            version = int(observed_at.timestamp())
            await _snapshot(
                store,
                owner,
                catalog,
                (ZulipUserTopic(7, "Ambiguous", 3, version),),
                observed_at,
            )
            await pool.execute("DELETE FROM workspace_zulip_bridge.workspace_outbox")
            assert (
                await store.store_user_topics(
                    owner,
                    "preferences",
                    (ZulipUserTopic(7, "Ambiguous", 1, version),),
                    replace_all=False,
                )
                == 0
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_topic_bindings"
                )
                == "follow"
            )
            assert (
                await pool.fetchval(
                    "SELECT count(*) FROM workspace_zulip_bridge.workspace_outbox"
                )
                == 0
            )
            restarted = EventStore(pool)
            assert (
                await restarted.notification_snapshot_required(owner, "preferences")
                is True
            )
            # A later authoritative snapshot can confirm the old event was stale
            # or confirm a real new choice, without guessing by arrival order.
            await _snapshot(
                restarted,
                owner,
                catalog,
                (ZulipUserTopic(7, "Ambiguous", source_mode, version),),
                datetime.now(UTC),
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_mode FROM workspace_zulip_bridge.zulip_topic_bindings"
                )
                == {3: "follow", 1: "mute"}[source_mode]
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == NOTIFICATION_SETTINGS_GENERATION
            )
            assert (
                await restarted.notification_snapshot_required(owner, "preferences")
                is False
            )
            # A repeated retained event is now strictly older and cannot keep
            # requesting snapshots or roll the recovered value back.
            await restarted.store_user_topics(
                owner,
                "preferences",
                (ZulipUserTopic(7, "Ambiguous", 1, version),),
                replace_all=False,
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == NOTIFICATION_SETTINGS_GENERATION
            )
            assert (
                await pool.fetchval(
                    "SELECT queue_id FROM workspace_zulip_bridge.zulip_connections"
                )
                == "preferences"
            )
        finally:
            await pool.close()

    asyncio.run(run())
