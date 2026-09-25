# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.workspace_sync import ProviderApiError
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker
from workspace_zulip_bridge.workspace_sync import _entity_dependencies
from workspace_zulip_bridge.workspace_sync import _equivalent_entity
from workspace_zulip_bridge.workspace_sync import _provider_api_error
from workspace_zulip_bridge.workspace_sync import _reaction_identity
from workspace_zulip_bridge.workspace_sync import identity_rebind_required
from workspace_zulip_bridge.workspace_sync import workspace_directory_url


def test_provider_api_error_omits_response_body() -> None:
    response = httpx.Response(
        422,
        json={
            "error": "invalid_entity",
            "message": "private message content",
            "data": {"token": "not-a-real-secret"},
        },
    )

    error = str(_provider_api_error(response))

    assert error == "Workspace Provider API returned 422 error=invalid_entity"
    assert "private message content" not in error
    assert "not-a-real-secret" not in error


def test_provider_api_error_rejects_untrusted_error_code() -> None:
    response = httpx.Response(
        500,
        json={"error": "invalid entity: private message content"},
    )

    assert str(_provider_api_error(response)) == (
        "Workspace Provider API returned 500 error=unknown"
    )


def test_provider_api_error_preserves_safe_item_index() -> None:
    response = httpx.Response(
        422,
        json={
            "error": "invalid_entity",
            "item_index": 17,
            "message": "private message content",
        },
    )

    error = _provider_api_error(response)

    assert error.item_index == 17
    assert str(error) == (
        "Workspace Provider API returned 422 error=invalid_entity item_index=17"
    )
    assert "private message content" not in str(error)


def test_moved_flag_repair_reduces_batch_after_timeout(tmp_path: Path) -> None:
    asyncio.run(_moved_flag_repair_reduces_batch_after_timeout(tmp_path))


async def _moved_flag_repair_reduces_batch_after_timeout(
    tmp_path: Path,
) -> None:
    class Transaction:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *args: object) -> bool:
            return False

    class Connection:
        def __init__(self) -> None:
            self.batch_sizes: list[int] = []

        def transaction(self) -> Transaction:
            return Transaction()

        async def fetchrow(self, query: str, *args: object) -> dict[str, object]:
            if "SELECT source_updated_at, entity_uuid" in query:
                return {"source_updated_at": None, "entity_uuid": None}
            batch_size = args[-1]
            assert isinstance(batch_size, int)
            self.batch_sizes.append(batch_size)
            if len(self.batch_sizes) == 1:
                raise TimeoutError
            return {
                "scanned": 0,
                "repaired": 0,
                "last_source_updated_at": None,
                "last_uuid": None,
                "requeued": 0,
            }

        async def execute(self, query: str, *args: object) -> str:
            return "UPDATE 1"

    class ConnectionContext:
        def __init__(self, connection: Connection) -> None:
            self.connection = connection

        async def __aenter__(self) -> Connection:
            return self.connection

        async def __aexit__(self, *args: object) -> bool:
            return False

    class Pool:
        def __init__(self) -> None:
            self.connection = Connection()

        async def execute(self, query: str, *args: object) -> str:
            return "INSERT 0 1"

        def acquire(self) -> ConnectionContext:
            return ConnectionContext(self.connection)

    token_file = tmp_path / "workspace-repair.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "ws://workspace.test/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    pool = Pool()
    worker = WorkspaceDiffWorker(pool, settings)  # type: ignore[arg-type]

    assert (
        await worker._repair_moved_message_flags(
            UUID("10000000-0000-0000-0000-000000000003"),
            UUID("10000000-0000-0000-0000-000000000004"),
        )
        == 0
    )
    assert pool.connection.batch_sizes == [10000, 5000]


def test_workspace_batch_continues_after_terminal_item_rejection(
    tmp_path: Path,
) -> None:
    asyncio.run(_workspace_batch_continues_after_terminal_item_rejection(tmp_path))


async def _workspace_batch_continues_after_terminal_item_rejection(
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "workspace-batch.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "ws://workspace.test/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    worker = WorkspaceDiffWorker(object(), settings)  # type: ignore[arg-type]
    rejected_row = {
        "entity_type": "users",
        "entity_uuid": UUID("10000000-0000-0000-0000-000000000003"),
        "claimed_at": datetime(2026, 9, 23, tzinfo=UTC),
    }
    accepted_row = {
        "entity_type": "users",
        "entity_uuid": UUID("10000000-0000-0000-0000-000000000004"),
        "claimed_at": datetime(2026, 9, 23, tzinfo=UTC),
    }
    ready = [
        (
            rejected_row,
            {"name": "rejected"},
            b"r" * 32,
            {
                "action": "upsert",
                "type": "users",
                "uuid": str(rejected_row["entity_uuid"]),
            },
        ),
        (
            accepted_row,
            {"name": "accepted"},
            b"a" * 32,
            {
                "action": "upsert",
                "type": "users",
                "uuid": str(accepted_row["entity_uuid"]),
            },
        ),
    ]
    worker._post = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            httpx.Response(
                409,
                json={"error": "provider_user_is_referenced", "item_index": 0},
            ),
            httpx.Response(200, json={"results": [{}]}),
        ]
    )
    worker._mark = AsyncMock()  # type: ignore[method-assign]
    worker._accept = AsyncMock()  # type: ignore[method-assign]

    await worker._apply_workspace_batch(object(), "backfill", ready)  # type: ignore[arg-type]

    assert worker._post.await_count == 2
    worker._mark.assert_awaited_once_with(
        [rejected_row],
        "blocked",
        "Workspace Provider API returned 409 "
        "error=provider_user_is_referenced item_index=0",
    )
    worker._accept.assert_awaited_once_with(
        [(accepted_row, {"name": "accepted"}, b"a" * 32)]
    )


def test_workspace_batch_bisects_repeated_server_failure(tmp_path: Path) -> None:
    asyncio.run(_workspace_batch_bisects_repeated_server_failure(tmp_path))


async def _workspace_batch_bisects_repeated_server_failure(
    tmp_path: Path,
) -> None:
    worker, ready = _server_failure_worker(tmp_path, 4)
    worker._post = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            httpx.Response(500, json={"error": "unknown"}),
            httpx.Response(500, json={"error": "unknown"}),
            httpx.Response(200, json={"results": [{}]}),
            httpx.Response(200, json={"results": [{}]}),
            httpx.Response(200, json={"results": [{}, {}]}),
        ]
    )
    worker._mark = AsyncMock()  # type: ignore[method-assign]
    worker._accept = AsyncMock()  # type: ignore[method-assign]

    await worker._apply_workspace_batch(object(), "backfill", ready)  # type: ignore[arg-type]

    assert worker._post.await_count == 5
    worker._mark.assert_not_awaited()
    accepted = {
        record[0]["entity_uuid"]
        for call in worker._accept.await_args_list
        for record in call.args[0]
    }
    assert accepted == {item[0]["entity_uuid"] for item in ready}


def test_workspace_batch_bounds_split_during_server_outage(tmp_path: Path) -> None:
    asyncio.run(_workspace_batch_bounds_split_during_server_outage(tmp_path))


async def _workspace_batch_bounds_split_during_server_outage(
    tmp_path: Path,
) -> None:
    worker, ready = _server_failure_worker(tmp_path, 8)
    worker._post = AsyncMock(  # type: ignore[method-assign]
        return_value=httpx.Response(500, json={"error": "unknown"})
    )
    worker._mark = AsyncMock()  # type: ignore[method-assign]
    worker._accept = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ProviderApiError, match="Provider API returned 500"):
        await worker._apply_workspace_batch(  # type: ignore[arg-type]
            object(),
            "backfill",
            ready,
        )

    assert worker._post.await_count == 7
    worker._accept.assert_not_awaited()
    marked = {
        row["entity_uuid"]
        for call in worker._mark.await_args_list
        for row in call.args[0]
    }
    assert marked == {item[0]["entity_uuid"] for item in ready}


def test_workspace_batch_falls_back_to_single_entity_put(tmp_path: Path) -> None:
    asyncio.run(_workspace_batch_falls_back_to_single_entity_put(tmp_path))


async def _workspace_batch_falls_back_to_single_entity_put(
    tmp_path: Path,
) -> None:
    worker, ready = _server_failure_worker(tmp_path, 1)
    worker._post = AsyncMock(  # type: ignore[method-assign]
        return_value=httpx.Response(500, json={"error": "unknown"})
    )
    worker._put = AsyncMock(  # type: ignore[method-assign]
        return_value=httpx.Response(200, json={"status": "created"})
    )
    worker._mark = AsyncMock()  # type: ignore[method-assign]
    worker._accept = AsyncMock()  # type: ignore[method-assign]

    await worker._apply_workspace_batch(object(), "backfill", ready)  # type: ignore[arg-type]

    worker._post.assert_awaited_once()
    worker._put.assert_awaited_once()
    assert worker._put.await_args.args[1].endswith(
        f"/provider/entities/message_flags/{ready[0][0]['entity_uuid']}"
    )
    assert worker._put.await_args.kwargs["json"] == {
        "content_hash": ready[0][3]["content_hash"],
        "source_updated_at": ready[0][3]["source_updated_at"],
        "data": ready[0][3]["data"],
    }
    worker._mark.assert_not_awaited()
    worker._accept.assert_awaited_once_with([ready[0][:3]])


def test_workspace_entity_fallback_keeps_server_failure_retryable(
    tmp_path: Path,
) -> None:
    asyncio.run(_workspace_entity_fallback_keeps_server_failure_retryable(tmp_path))


async def _workspace_entity_fallback_keeps_server_failure_retryable(
    tmp_path: Path,
) -> None:
    worker, ready = _server_failure_worker(tmp_path, 1)
    worker._post = AsyncMock(  # type: ignore[method-assign]
        return_value=httpx.Response(500, json={"error": "unknown"})
    )
    worker._put = AsyncMock(  # type: ignore[method-assign]
        return_value=httpx.Response(503, json={"error": "unavailable"})
    )
    worker._mark = AsyncMock()  # type: ignore[method-assign]
    worker._accept = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ProviderApiError, match="Provider API returned 503"):
        await worker._apply_workspace_batch(  # type: ignore[arg-type]
            object(),
            "backfill",
            ready,
        )

    worker._post.assert_awaited_once()
    worker._put.assert_awaited_once()
    worker._mark.assert_awaited_once_with(
        [ready[0][0]],
        "failed",
        "Workspace Provider API returned 503 error=unavailable",
    )
    worker._accept.assert_not_awaited()


def _server_failure_worker(
    tmp_path: Path,
    count: int,
) -> tuple[WorkspaceDiffWorker, list[tuple[dict, dict, bytes, dict]]]:
    token_file = tmp_path / "workspace-server-failure.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "ws://workspace.test/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    worker = WorkspaceDiffWorker(object(), settings)  # type: ignore[arg-type]
    ready = []
    for index in range(count):
        entity_uuid = UUID(f"10000000-0000-0000-0000-{index + 10:012d}")
        row = {
            "entity_type": "message_flags",
            "entity_uuid": entity_uuid,
            "claimed_at": datetime(2026, 9, 25, tzinfo=UTC),
            "attempt_count": 3,
        }
        ready.append(
            (
                row,
                {"read": True},
                b"f" * 32,
                {
                    "action": "upsert",
                    "type": "message_flags",
                    "uuid": str(entity_uuid),
                    "content_hash": (b"f" * 32).hex(),
                    "source_updated_at": "2026-09-25T00:00:00Z",
                    "data": {"read": True},
                },
            )
        )
    return worker, ready


def test_workspace_batch_skips_entities_outside_provider_scope(
    tmp_path: Path,
) -> None:
    asyncio.run(_workspace_batch_skips_entities_outside_provider_scope(tmp_path))


async def _workspace_batch_skips_entities_outside_provider_scope(
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "workspace-outside-scope.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "ws://workspace.test/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    worker = WorkspaceDiffWorker(object(), settings)  # type: ignore[arg-type]
    row = {
        "entity_type": "message_flags",
        "entity_uuid": UUID("10000000-0000-0000-0000-000000000003"),
        "claimed_at": datetime(2026, 9, 25, tzinfo=UTC),
    }
    ready = [
        (
            row,
            {"read": True},
            b"f" * 32,
            {
                "action": "upsert",
                "type": "message_flags",
                "uuid": str(row["entity_uuid"]),
            },
        )
    ]
    worker._post = AsyncMock(  # type: ignore[method-assign]
        return_value=httpx.Response(
            409,
            json={"error": "entity_not_provider_owned", "item_index": 0},
        )
    )
    worker._mark = AsyncMock()  # type: ignore[method-assign]
    worker._accept = AsyncMock()  # type: ignore[method-assign]

    await worker._apply_workspace_batch(object(), "backfill", ready)  # type: ignore[arg-type]

    worker._mark.assert_awaited_once_with(
        [row],
        "skipped",
        "workspace_entity_outside_provider_scope",
    )
    worker._accept.assert_not_awaited()


def test_message_dependencies_include_container_and_author() -> None:
    stream_uuid = UUID("10000000-0000-0000-0000-000000000001")
    topic_uuid = UUID("10000000-0000-0000-0000-000000000002")
    author_uuid = UUID("10000000-0000-0000-0000-000000000003")

    assert _entity_dependencies(
        "messages",
        {
            "stream_uuid": str(stream_uuid),
            "topic_uuid": str(topic_uuid),
            "author_uuid": str(author_uuid),
        },
    ) == (
        ("streams", stream_uuid),
        ("topics", topic_uuid),
        ("users", author_uuid),
    )


def test_topic_binding_waits_for_matching_stream_binding(tmp_path: Path) -> None:
    asyncio.run(_topic_binding_waits_for_matching_stream_binding(tmp_path))


async def _topic_binding_waits_for_matching_stream_binding(tmp_path: Path) -> None:
    token_file = tmp_path / "workspace-dependencies.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "ws://workspace.test/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    worker = WorkspaceDiffWorker(object(), settings)  # type: ignore[arg-type]
    topic_binding_uuid = UUID("10000000-0000-0000-0000-000000000003")
    stream_uuid = UUID("10000000-0000-0000-0000-000000000004")
    topic_uuid = UUID("10000000-0000-0000-0000-000000000005")
    user_uuid = UUID("10000000-0000-0000-0000-000000000006")
    stream_binding_uuid = UUID("10000000-0000-0000-0000-000000000007")
    row = {"entity_type": "topic_bindings", "entity_uuid": topic_binding_uuid}
    candidate = (
        row,
        {
            "stream_uuid": str(stream_uuid),
            "topic_uuid": str(topic_uuid),
            "user_uuid": str(user_uuid),
        },
        b"b" * 32,
        {"action": "upsert"},
    )
    worker._load_message_flag_binding_ids = AsyncMock(  # type: ignore[method-assign]
        return_value={}
    )
    worker._load_topic_binding_stream_binding_ids = AsyncMock(  # type: ignore[method-assign]
        return_value={topic_binding_uuid: stream_binding_uuid}
    )
    worker._load_ready_dependency_ids = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "streams": {stream_uuid},
            "topics": {topic_uuid},
            "users": {user_uuid},
            "stream_bindings": set(),
            "topic_bindings": set(),
        }
    )

    ready, deferred = await worker._partition_dependency_ready([candidate])

    assert ready == []
    assert deferred == [row]

    worker._load_ready_dependency_ids.return_value["stream_bindings"].add(  # type: ignore[attr-defined]
        stream_binding_uuid
    )
    ready, deferred = await worker._partition_dependency_ready([candidate])

    assert ready == [candidate]
    assert deferred == []


def test_dependency_deletes_do_not_wait_for_removed_source_rows(
    tmp_path: Path,
) -> None:
    asyncio.run(_dependency_deletes_do_not_wait_for_removed_source_rows(tmp_path))


async def _dependency_deletes_do_not_wait_for_removed_source_rows(
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "workspace-dependency-deletes.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "ws://workspace.test/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    worker = WorkspaceDiffWorker(object(), settings)  # type: ignore[arg-type]
    topic_binding_row = {
        "entity_type": "topic_bindings",
        "entity_uuid": UUID("10000000-0000-0000-0000-000000000003"),
    }
    message_flag_row = {
        "entity_type": "message_flags",
        "entity_uuid": UUID("10000000-0000-0000-0000-000000000004"),
    }
    candidates = [
        (topic_binding_row, {}, b"", {"action": "delete"}),
        (message_flag_row, {}, b"", {"action": "delete"}),
    ]
    worker._load_message_flag_binding_ids = AsyncMock(  # type: ignore[method-assign]
        return_value={}
    )
    worker._load_topic_binding_stream_binding_ids = AsyncMock(  # type: ignore[method-assign]
        return_value={}
    )
    worker._load_ready_dependency_ids = AsyncMock(  # type: ignore[method-assign]
        return_value={}
    )

    ready, deferred = await worker._partition_dependency_ready(candidates)

    assert ready == candidates
    assert deferred == []


def test_message_identity_alias_is_redirected_to_workspace(tmp_path: Path) -> None:
    asyncio.run(_message_identity_alias_is_redirected_to_workspace(tmp_path))


async def _message_identity_alias_is_redirected_to_workspace(
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "workspace-identity-rebind.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "ws://workspace.test/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    pool = AsyncMock()
    worker = WorkspaceDiffWorker(pool, settings)
    message_uuid = UUID("10000000-0000-0000-0000-000000000003")
    zulip_user_uuid = UUID("10000000-0000-0000-0000-000000000004")
    workspace_user_uuid = UUID("10000000-0000-0000-0000-000000000005")
    row = {"entity_type": "messages", "entity_uuid": message_uuid}
    source = {
        "stream_uuid": "10000000-0000-0000-0000-000000000006",
        "topic_uuid": "10000000-0000-0000-0000-000000000007",
        "author_uuid": str(workspace_user_uuid),
        "payload": {"kind": "markdown", "content": "same"},
        "created_at": "2026-09-24T12:00:00Z",
    }
    target = {**source, "author_uuid": str(zulip_user_uuid)}
    pool.fetch.return_value = [
        {
            "uuid": zulip_user_uuid,
            "workspace_user_uuid": workspace_user_uuid,
        }
    ]

    rebinds = await worker._message_identity_rebinds(
        [row],
        {("messages", message_uuid): source},
        {("messages", message_uuid): target},
    )

    assert rebinds == {("messages", message_uuid)}

    changed_target = {**target, "payload": {"kind": "markdown", "content": "new"}}
    rebinds = await worker._message_identity_rebinds(
        [row],
        {("messages", message_uuid): source},
        {("messages", message_uuid): changed_target},
    )

    assert rebinds == set()


def test_message_identity_redirect_is_versioned_backfill(tmp_path: Path) -> None:
    asyncio.run(_message_identity_redirect_is_versioned_backfill(tmp_path))


async def _message_identity_redirect_is_versioned_backfill(
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "workspace-identity-redirect.token"
    token_file.write_text("integration-token")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "ws://workspace.test/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    pool = AsyncMock()
    worker = WorkspaceDiffWorker(pool, settings)
    row = {
        "provider_uuid": UUID("10000000-0000-0000-0000-000000000002"),
        "entity_type": "messages",
        "entity_uuid": UUID("10000000-0000-0000-0000-000000000003"),
        "claimed_at": datetime(2026, 9, 25, tzinfo=UTC),
    }

    await worker._redirect_identity_rebind(row)

    query, *parameters = pool.execute.await_args.args
    assert "delivery_priority = 1" in query
    assert "GREATEST(" in query
    assert "+ interval '1 microsecond'" in query
    assert parameters == [
        row["provider_uuid"],
        row["entity_type"],
        row["entity_uuid"],
        row["claimed_at"],
    ]


def test_reaction_equivalence_ignores_reload_timestamp() -> None:
    source = {
        "message_uuid": "10000000-0000-0000-0000-000000000001",
        "user_uuid": "10000000-0000-0000-0000-000000000002",
        "emoji_name": "tada",
        "created_at": "2026-09-20T12:00:00Z",
    }
    target = {
        **source,
        "created_at": "2026-09-19T18:00:00.000000+00:00",
    }

    assert _equivalent_entity("message_reactions", source, target)


def test_reaction_equivalence_ignores_workspace_compatibility_metadata() -> None:
    source = {
        "message_uuid": "10000000-0000-0000-0000-000000000001",
        "user_uuid": "10000000-0000-0000-0000-000000000002",
        "emoji_name": "tada",
        "created_at": "2026-09-20T12:00:00Z",
    }
    target = {
        **source,
        "uuid": "10000000-0000-0000-0000-000000000003",
        "project_id": "10000000-0000-0000-0000-000000000004",
        "source": {"kind": "zulip", "stream_id": 0},
        "source_name": "zulip",
        "old_source": {"kind": "zulip", "stream_id": 0},
        "old_source_name": "zulip",
        "old_emoji_name": "tada",
    }

    assert _equivalent_entity("message_reactions", source, target)


def test_reaction_equivalence_still_compares_identity_fields() -> None:
    source = {
        "message_uuid": "10000000-0000-0000-0000-000000000001",
        "user_uuid": "10000000-0000-0000-0000-000000000002",
        "emoji_name": "tada",
        "created_at": "2026-09-20T12:00:00Z",
    }

    assert not _equivalent_entity(
        "message_reactions",
        source,
        {**source, "emoji_name": "heart"},
    )


def test_message_equivalence_ignores_workspace_projection_metadata() -> None:
    source = {
        "stream_uuid": "10000000-0000-0000-0000-000000000001",
        "topic_uuid": "10000000-0000-0000-0000-000000000002",
        "author_uuid": "10000000-0000-0000-0000-000000000003",
        "payload": {"kind": "markdown", "content": "round trip"},
        "created_at": "2026-09-20T21:08:40.252918+00:00",
    }
    target = {
        **source,
        "created_at": "2026-09-20T21:08:40.252918Z",
        "uuid": "10000000-0000-0000-0000-000000000004",
        "project_id": "10000000-0000-0000-0000-000000000005",
        "updated_at": "2026-09-20T21:08:41Z",
        "source": {"kind": "native"},
        "source_name": "native",
        "reactions": {},
        "reaction_users": {},
    }

    assert _equivalent_entity("messages", source, target)


def test_message_equivalence_still_compares_content() -> None:
    source = {
        "stream_uuid": "10000000-0000-0000-0000-000000000001",
        "topic_uuid": "10000000-0000-0000-0000-000000000002",
        "author_uuid": "10000000-0000-0000-0000-000000000003",
        "payload": {"kind": "markdown", "content": "before"},
        "created_at": "2026-09-20T21:08:40Z",
    }

    assert not _equivalent_entity(
        "messages",
        source,
        {**source, "payload": {"kind": "markdown", "content": "after"}},
    )


def test_stream_equivalence_maps_public_workspace_projection() -> None:
    owner_uuid = "10000000-0000-0000-0000-000000000001"
    source = {
        "name": "General",
        "description": "",
        "owner_uuid": owner_uuid,
        "invite_only": False,
        "announce": False,
        "direct_user_uuid": None,
        "private": False,
        "is_archived": False,
        "color": 0,
        "history_public_to_subscribers": False,
        "created_at": "2026-09-20T21:08:40+00:00",
    }
    target = {
        **source,
        "owner": owner_uuid,
        "owner_uuid": None,
        "description": None,
        "created_at": "2026-09-19T21:08:40Z",
        "history_public_to_subscribers": None,
        "uuid": "10000000-0000-0000-0000-000000000002",
        "project_id": "10000000-0000-0000-0000-000000000003",
        "updated_at": "2026-09-20T21:09:00Z",
        "source": {"kind": "zulip", "stream_id": 0},
        "source_name": "zulip",
        "role": "member",
        "notification_mode": "all_messages",
        "unread_count": 17,
        "active_unread_count": 17,
        "passive_unread_count": 0,
        "last_message_uuid": "10000000-0000-0000-0000-000000000004",
    }

    assert _equivalent_entity("streams", source, target)


def test_stream_equivalence_still_compares_canonical_fields() -> None:
    source = {
        "name": "General",
        "description": "before",
        "owner_uuid": "10000000-0000-0000-0000-000000000001",
        "invite_only": False,
        "announce": False,
        "direct_user_uuid": None,
        "private": False,
        "is_archived": False,
        "color": 0,
        "history_public_to_subscribers": True,
        "created_at": "2026-09-20T21:08:40Z",
    }

    assert not _equivalent_entity(
        "streams",
        source,
        {**source, "description": "after"},
    )
    assert not _equivalent_entity(
        "streams",
        source,
        {**source, "history_public_to_subscribers": False},
    )


def test_reaction_identity_matches_workspace_unique_constraint() -> None:
    assert _reaction_identity(
        {
            "message_uuid": "10000000-0000-0000-0000-000000000001",
            "user_uuid": "10000000-0000-0000-0000-000000000002",
            "emoji_name": "smile",
            "created_at": "2026-09-20T12:00:00Z",
        }
    ) == (
        "10000000-0000-0000-0000-000000000001",
        "10000000-0000-0000-0000-000000000002",
        "smile",
    )


def test_workspace_directory_uses_public_user_route() -> None:
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_API_URL": "https://workspace.test/api/workspace/v1",
        }
    )

    assert workspace_directory_url(settings) == (
        "https://workspace.test/api/workspace/v1/users/"
    )


def test_identity_rebind_is_explicit_and_limited_to_identity_fields() -> None:
    source = {
        "stream_uuid": "10000000-0000-0000-0000-000000000001",
        "user_uuid": "10000000-0000-0000-0000-000000000002",
        "role": "member",
    }

    assert identity_rebind_required(
        "stream_bindings",
        source,
        {**source, "user_uuid": "10000000-0000-0000-0000-000000000003"},
    )
    assert not identity_rebind_required(
        "stream_bindings",
        source,
        {**source, "role": "admin"},
    )
    assert not identity_rebind_required("stream_bindings", source, None)
