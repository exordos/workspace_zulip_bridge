# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from importlib import resources


def _schema() -> str:
    return (
        resources.files("workspace_zulip_bridge")
        .joinpath("schema.sql")
        .read_text(encoding="utf-8")
    )


def test_schema_separates_realm_identities_and_sync_connections() -> None:
    schema = _schema()

    for table in ("zulip_realms", "zulip_users", "zulip_connections"):
        assert f"workspace_zulip_bridge.{table}" in schema
    assert "identity_key text NOT NULL UNIQUE" in schema
    assert "zulip_user_id bigint NOT NULL" in schema
    assert "is_bot boolean NOT NULL DEFAULT false" in schema
    assert "api_key text NOT NULL" in schema
    assert "lifecycle_status text NOT NULL DEFAULT 'init'" in schema
    assert "streams_hash bytea" in schema
    assert "password" not in schema


def test_schema_normalizes_workspace_like_entities_and_personal_state() -> None:
    schema = _schema()

    for table in (
        "zulip_streams",
        "zulip_stream_bindings",
        "zulip_topics",
        "zulip_topic_aliases",
        "zulip_topic_bindings",
        "zulip_messages",
        "zulip_message_flags",
        "zulip_message_reactions",
        "zulip_files",
        "zulip_message_files",
    ):
        assert f"workspace_zulip_bridge.{table}" in schema
    assert "source_connection_uuid uuid" in schema
    assert "UNIQUE (realm_uuid, chat_key)" in schema
    assert "UNIQUE (zulip_stream_uuid, zulip_user_uuid)" in schema
    assert "first_visible_message_id bigint" in schema
    assert "UNIQUE (zulip_stream_uuid, name)" in schema
    assert "reaction_users jsonb NOT NULL DEFAULT '{}'::jsonb" in schema
    assert "flags_hash bytea NOT NULL" in schema
    assert "source_path text NOT NULL" in schema
    assert "message_ids bigint[] NOT NULL DEFAULT '{}'::bigint[]" in schema
    assert "zulip_files_message_ids_idx" in schema
    assert "bytea" not in schema[
        schema.index(
            "CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_files"
        ) : schema.index(
            "CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_message_files"
        )
    ].replace("metadata_hash bytea", "")
    assert "workspace_chats" not in schema
    for table in (
        "workspace_users",
        "workspace_streams",
        "workspace_stream_bindings",
        "workspace_topics",
        "workspace_topic_bindings",
        "workspace_messages",
        "workspace_message_flags",
        "workspace_message_reactions",
    ):
        assert f"workspace_zulip_bridge.{table}" in schema
    assert "workspace_mirror_state" in schema
    assert "sync_diffs" in schema
    assert "sync_plan_cursors" in schema
    assert "partition_key uuid" in schema
    assert "zulip_messages_sync_plan_idx" in schema
    assert "zulip_message_flags_sync_plan_idx" in schema
    assert "zulip_message_reactions_sync_plan_idx" in schema


def test_schema_contains_event_retention_and_workspace_outbox_state() -> None:
    schema = _schema()

    assert "zulip_connection_uuid uuid NOT NULL" in schema
    assert "processing_status text NOT NULL DEFAULT 'pending'" in schema
    assert "attempt_count integer NOT NULL DEFAULT 0" in schema
    assert "zulip_events_pending_idx" in schema
    assert "zulip_events_terminal_retention_idx" in schema
    assert "workspace_outbox" in schema
    assert "workspace_sync_cursors" in schema
    assert "workspace_outbox_pending_entity_idx" in schema
    assert "workspace_event_cursors" in schema
    assert "workspace_events" in schema
    assert "workspace_events_pending_idx" in schema
    assert "recovery_required boolean NOT NULL DEFAULT false" in schema
