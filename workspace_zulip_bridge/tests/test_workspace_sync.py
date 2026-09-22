# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from uuid import UUID

import httpx

from workspace_zulip_bridge.config import Settings
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
