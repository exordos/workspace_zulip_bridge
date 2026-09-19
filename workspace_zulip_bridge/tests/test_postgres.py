# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from pathlib import Path
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
from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.models import UserDirectoryWrite
from workspace_zulip_bridge.models import ZulipAttachment
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.models import ZulipEvent
from workspace_zulip_bridge.models import ZulipMessage
from workspace_zulip_bridge.monitor import collect_snapshot
from workspace_zulip_bridge.stable_ids import stable_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_realm_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid
from workspace_zulip_bridge.workspace_events import WorkspaceEvent
from workspace_zulip_bridge.workspace_events import WorkspaceEventReceiver
from workspace_zulip_bridge.workspace_events import WorkspaceEventStore
from workspace_zulip_bridge.workspace_sync import WorkspaceBootstrapper
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker

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
        await connection.execute(
            """
            TRUNCATE workspace_zulip_bridge.sync_diffs,
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


def test_workspace_bootstrap_activates_verified_generation(
    tmp_path: Path,
) -> None:
    asyncio.run(_workspace_bootstrap_round_trip(_dsn(), tmp_path))


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
        "record": "entity",
        "type": "users",
        "uuid": str(user_uuid),
        "content_hash": "01" * 32,
        "source_updated_at": "2026-09-19T08:00:00Z",
        "data": {"display_name": "Bootstrap User"},
    }
    entity_line = json.dumps(entity, sort_keys=True, separators=(",", ":"))
    counts = {
        entity_type: 0
        for entity_type in (
            "users",
            "streams",
            "stream_bindings",
            "topics",
            "topic_bindings",
            "messages",
            "message_flags",
            "message_reactions",
        )
    }
    counts["users"] = 1
    payload = (
        "\n".join(
            (
                json.dumps(
                    {
                        "record": "meta",
                        "schema_version": 1,
                        "snapshot_uuid": str(generation),
                        "project_id": str(project_uuid),
                        "provider_uuid": str(provider_uuid),
                        "epoch_generation": str(epoch_generation),
                        "snapshot_epoch_version": 41,
                        "created_at": "2026-09-19T08:00:00Z",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                entity_line,
                json.dumps(
                    {
                        "record": "complete",
                        "snapshot_uuid": str(generation),
                        "counts": counts,
                        "sha256": hashlib.sha256(
                            (entity_line + "\n").encode()
                        ).hexdigest(),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        )
        + "\n"
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/provider/bootstrap")
        return httpx.Response(200, text=payload)

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
            await worker.plan()
            await pool.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_connections
                SET lifecycle_status = 'active'
                WHERE uuid = $1
                """,
                user_uuid,
            )
            assert await worker.process_once(client) == 0
            assert await pool.fetchval(
                """
                SELECT initial_sync_completed_at IS NOT NULL
                FROM workspace_zulip_bridge.workspace_mirror_state
                WHERE provider_uuid = $1
                """,
                provider_uuid,
            )
            await pool.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_users
                SET full_name = 'Updated User 10',
                    profile_hash = decode(repeat('02', 32), 'hex')
                WHERE uuid = $1
                """,
                user_uuid,
            )
            await worker.plan()
            assert await worker.process_once(client) == 1
        assert len(requests) == 2
        assert requests[0]["delivery_class"] == "backfill"
        assert requests[1]["delivery_class"] == "live"
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


async def _insert_user(
    connection: asyncpg.Connection,
    user_id: int,
    role: int,
    *,
    api_key: str | None = "api-key-placeholder",
    queue_id: str | None = None,
    status: str = "init",
    is_bot: bool = False,
) -> UUID:
    user_uuid = stable_user_uuid(ENDPOINT, user_id)
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
            VALUES ($1, $2, $1, $3, $4, $5, 0, $6)
            """,
            user_uuid,
            stable_realm_uuid(ENDPOINT),
            f"user-{user_id}@example.test",
            api_key,
            queue_id,
            status,
        )
    return user_uuid


def _catalog(
    own_user_id: int,
    channels: list[tuple[int, str]],
    counts: dict[str, int],
    first_visible_message_ids: dict[int, int | None] | None = None,
):
    role = {10: 100, 20: 200, 30: 200, 99: 100}.get(own_user_id, 400)
    builder = ChatCatalogBuilder(own_user_id, f"User {own_user_id}", role)
    builder.add_subscriptions(
        [{"stream_id": stream_id, "name": name} for stream_id, name in channels],
        first_visible_message_ids=first_visible_message_ids,
    )
    return builder.build(counts)


def test_directory_uses_stable_user_ids_and_keeps_bots_without_connections() -> None:
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
        assert result == UserDirectoryWrite(users=2, bots=1, changed=2)
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT zulip_user.uuid, zulip_user.login, zulip_user.full_name,
                       zulip_user.role, zulip_user.disabled, zulip_user.is_bot,
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
    finally:
        await pool.close()


def test_scheduler_uses_role_then_stable_uuid() -> None:
    asyncio.run(_scheduler_round_trip(_dsn()))


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

        owner_catalog = _catalog(
            10,
            [(7, "Shared")],
            {"channel:7": 1},
            {7: 101},
        )
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
        assert first_visible_message_id == 101

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
            assert (
                await connection.fetchval(
                    """
                    SELECT source_connection_uuid
                    FROM workspace_zulip_bridge.zulip_streams
                    WHERE chat_key = 'channel:7'
                    """
                )
                == admin_b_uuid
            )
    finally:
        await pool.close()


def test_history_ids_survive_queue_loss_and_supplier_deletion() -> None:
    asyncio.run(_history_round_trip(_dsn()))


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
                SELECT owner_user_uuid, zulip_attachment_id, source_path, name,
                       size_bytes, message_ids, metadata_hash
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
        chat_uuid = stable_chat_uuid(ENDPOINT, "channel:7")
        assert first_message_uuid == stable_message_uuid(ENDPOINT, 123)
        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT message.topic_uuid, message.content, flags.is_starred,
                       message.created_at, message.updated_at
                FROM workspace_zulip_bridge.zulip_messages AS message
                JOIN workspace_zulip_bridge.zulip_message_flags AS flags
                  ON flags.message_uuid = message.uuid
                 AND flags.zulip_user_uuid = $2
                WHERE message.uuid = $1
                """,
                first_message_uuid,
                owner_uuid,
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
            {
                "id": 5,
                "type": "update_message_flags",
                "op": "add",
                "flag": "read",
                "messages": [123],
                "all": False,
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
            ) == {"pending": 16}

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
            16,
            10,
            5,
            1,
        )
        assert stats.messages_changed == 9
        assert stats.chats_changed == 1

        async with pool.acquire() as connection:
            final_message = await connection.fetchrow(
                """
                SELECT message.content,
                       flags.is_read,
                       flags.is_starred,
                       message.reactions::text AS reactions,
                       message.created_at,
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
            chat_name = await connection.fetchval(
                """
                SELECT name
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
        assert member_read is True
        assert normalized_reactions == 3
        assert statuses == {"applied": 10, "failed": 1, "skipped": 5}
        assert skip_reasons == {
            "not_chat_supplier": 4,
            "unsupported_event_type": 1,
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
            "applied": 10,
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
