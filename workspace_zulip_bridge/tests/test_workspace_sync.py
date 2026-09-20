# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.workspace_sync import _equivalent_entity
from workspace_zulip_bridge.workspace_sync import _reaction_identity
from workspace_zulip_bridge.workspace_sync import identity_rebind_required
from workspace_zulip_bridge.workspace_sync import workspace_directory_url


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
