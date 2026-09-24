# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID

import httpx

from workspace_zulip_bridge.config import Settings
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
