# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.event_processor import ZulipEventProcessor
from workspace_zulip_bridge.models import NOTIFICATION_SETTINGS_GENERATION
from workspace_zulip_bridge.models import ZulipEvent
from workspace_zulip_bridge.tests.test_notification_state_postgres import _setup
from workspace_zulip_bridge.tests.test_notification_state_postgres import _snapshot
from workspace_zulip_bridge.tests.test_postgres import _dsn


async def _process(pool, store, owner, event):
    await store.store_events(
        owner,
        "preferences",
        (ZulipEvent(event["id"], event["type"], json.dumps(event)),),
        event["id"],
    )
    result = await ZulipEventProcessor(
        pool, store, Settings.from_env({"WZB_DATABASE_DSN": _dsn()})
    ).process_once()
    assert (result.claimed, result.applied, result.retried) == (1, 1, 0)


@pytest.mark.parametrize("stale_value, source_value", [(False, True), (True, False)])
def test_retained_old_global_event_converges_without_catalog_reload(
    stale_value, source_value
):
    async def run():
        pool, store, owner, catalog = await _setup()
        try:
            await _snapshot(store, owner, catalog, (), datetime.now(UTC))
            await pool.execute(
                "UPDATE workspace_zulip_bridge.zulip_connections SET lifecycle_status = 'active'"
            )
            await _process(
                pool,
                store,
                owner,
                {
                    "id": 1,
                    "type": "user_settings",
                    "property": "enable_stream_desktop_notifications",
                    "value": stale_value,
                },
            )
            from workspace_zulip_bridge.chat_catalog import (
                with_stream_notification_default,
            )

            # Source truth can contradict even an event equal to cached state.
            await store.store_notification_snapshot(
                owner,
                "preferences",
                with_stream_notification_default(catalog.chats, source_value),
                (),
                enable_stream_desktop_notifications=source_value,
                observed_at=datetime.now(UTC) + timedelta(seconds=2),
            )
            row = await pool.fetchrow(
                "SELECT enable_stream_desktop_notifications, lifecycle_status, notification_settings_generation, queue_id, last_event_id FROM workspace_zulip_bridge.zulip_connections"
            )
            assert tuple(row) == (
                source_value,
                "active",
                NOTIFICATION_SETTINGS_GENERATION,
                "preferences",
                1,
            )
            assert await pool.fetchval(
                "SELECT notification_mode FROM workspace_zulip_bridge.zulip_stream_bindings"
            ) == ("all_messages" if source_value else "mentions_only")
        finally:
            await pool.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "property_name", ["is_muted", "desktop_notifications", "in_home_view"]
)
def test_subscription_notification_event_requests_settings_only_repair(property_name):
    async def run():
        pool, store, owner, catalog = await _setup()
        try:
            await _snapshot(store, owner, catalog, (), datetime.now(UTC))
            await pool.execute(
                "UPDATE workspace_zulip_bridge.zulip_connections SET lifecycle_status = 'active'"
            )
            await _process(
                pool,
                store,
                owner,
                {
                    "id": 1,
                    "type": "subscription",
                    "op": "update",
                    "property": property_name,
                    "stream_id": 7,
                    "value": False,
                },
            )
            row = await pool.fetchrow(
                "SELECT lifecycle_status, streams_hash, notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
            )
            assert tuple(row) == (
                "active",
                catalog.content_hash,
                NOTIFICATION_SETTINGS_GENERATION - 1,
            )
            from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder

            builder = ChatCatalogBuilder(78, "User 78", 400)
            preference = (
                {"desktop_notifications": False}
                if property_name == "desktop_notifications"
                else {"is_muted": True}
            )
            builder.add_subscriptions(
                [{"stream_id": 7, "name": "Channel", **preference}]
            )
            await _snapshot(
                store,
                owner,
                builder.build(),
                (),
                datetime.now(UTC) + timedelta(seconds=2),
            )
            assert await pool.fetchval(
                "SELECT notification_mode FROM workspace_zulip_bridge.zulip_stream_bindings"
            ) == (
                "mentions_only" if property_name == "desktop_notifications" else "muted"
            )
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == NOTIFICATION_SETTINGS_GENERATION
            )
        finally:
            await pool.close()

    asyncio.run(run())


@pytest.mark.parametrize("request_kind", ["global", "subscription", "unchanged_global"])
def test_inflight_snapshot_does_not_lose_a_later_notification_request(request_kind):
    async def run():
        pool, store, owner, catalog = await _setup()
        try:
            await _snapshot(
                store, owner, catalog, (), datetime.now(UTC) - timedelta(seconds=2)
            )
            began = datetime.now(UTC)
            if request_kind == "subscription":
                await store.request_notification_snapshot(owner, "preferences")
            else:
                await store.store_user_notification_setting(
                    owner,
                    "preferences",
                    enable_stream_desktop_notifications=request_kind
                    == "unchanged_global",
                )
            requested = await pool.fetchval(
                "SELECT notification_refresh_requested_at FROM workspace_zulip_bridge.zulip_connections"
            )
            assert requested >= began
            await _snapshot(store, owner, catalog, (), began)
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == 1
            )
            # Simulate the next observation second, after this snapshot covered
            # the durable request. One successful pass clears the request.
            await _snapshot(store, owner, catalog, (), began + timedelta(seconds=2))
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == NOTIFICATION_SETTINGS_GENERATION
            )
            assert (
                await store.notification_snapshot_required(owner, "preferences")
                is False
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


def test_global_event_burst_coalesces_and_duplicate_replay_does_not_repair_again():
    async def run():
        pool, store, owner, catalog = await _setup()
        try:
            await _snapshot(
                store, owner, catalog, (), datetime.now(UTC) - timedelta(seconds=2)
            )
            for event_id, value in enumerate([False, True, False, False], 1):
                await _process(
                    pool,
                    store,
                    owner,
                    {
                        "id": event_id,
                        "type": "user_settings",
                        "property": "enable_stream_desktop_notifications",
                        "value": value,
                    },
                )
            assert (
                await pool.fetchval(
                    "SELECT notification_settings_generation FROM workspace_zulip_bridge.zulip_connections"
                )
                == 1
            )
            await _snapshot(store, owner, catalog, (), datetime.now(UTC))
            assert (
                await pool.fetchval(
                    "SELECT enable_stream_desktop_notifications FROM workspace_zulip_bridge.zulip_connections"
                )
                is True
            )
            old_request = await pool.fetchval(
                "SELECT notification_refresh_requested_at FROM workspace_zulip_bridge.zulip_connections"
            )
            event = {
                "id": 4,
                "type": "user_settings",
                "property": "enable_stream_desktop_notifications",
                "value": False,
            }
            stored = await store.store_events(
                owner,
                "preferences",
                (ZulipEvent(4, "user_settings", json.dumps(event)),),
                4,
            )
            assert stored[0] == 0
            stats = await ZulipEventProcessor(
                pool, store, Settings.from_env({"WZB_DATABASE_DSN": _dsn()})
            ).process_once()
            assert stats.claimed == 0
            assert (
                await pool.fetchval(
                    "SELECT notification_refresh_requested_at FROM workspace_zulip_bridge.zulip_connections"
                )
                == old_request
            )
            assert (
                await store.notification_snapshot_required(owner, "preferences")
                is False
            )
        finally:
            await pool.close()

    asyncio.run(run())


def test_refresh_request_column_upgrade_preserves_connection_state():
    async def run():
        from workspace_zulip_bridge.database import prepare_database

        pool, store, owner, catalog = await _setup()
        try:
            await _snapshot(store, owner, catalog, (), datetime.now(UTC))
            await pool.execute(
                "ALTER TABLE workspace_zulip_bridge.zulip_connections DROP COLUMN notification_refresh_requested_at"
            )
            await prepare_database(pool)
            row = await pool.fetchrow(
                "SELECT notification_settings_generation, notification_refresh_requested_at, queue_id FROM workspace_zulip_bridge.zulip_connections"
            )
            assert tuple(row) == (
                NOTIFICATION_SETTINGS_GENERATION,
                datetime.fromtimestamp(0, UTC),
                "preferences",
            )
        finally:
            await pool.close()

    asyncio.run(run())
