# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from importlib import resources


def test_schema_contains_user_and_event_state() -> None:
    schema = (
        resources.files("workspace_zulip_bridge")
        .joinpath("schema.sql")
        .read_text(encoding="utf-8")
    )

    assert "CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_users" in schema
    for column in (
        "uuid uuid PRIMARY KEY",
        "endpoint text NOT NULL",
        "login text NOT NULL",
        "api_key text",
        "zulip_user_id bigint",
        "full_name text",
        "role smallint",
        "disabled boolean NOT NULL DEFAULT false",
        "queue_id text",
        "last_event_id bigint",
        "status text NOT NULL DEFAULT 'init'",
        "chats_hash bytea",
        "created_at timestamptz NOT NULL",
        "updated_at timestamptz NOT NULL",
    ):
        assert column in schema

    assert "UNIQUE (endpoint, login)" in schema
    assert "password" not in schema
    assert "CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_chats" in schema
    assert "UNIQUE (endpoint, chat_key)" in schema
    assert "supplier_user_uuid uuid" in schema
    assert "history_loaded_at timestamptz" in schema
    assert "zulip_chats_supplier_idx" in schema
    assert "zulip_chats_touch_updated_at" in schema
    assert (
        "CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_chat_users" in schema
    )
    assert "available_message_count bigint NOT NULL DEFAULT 0" in schema
    assert "CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_topics" in schema
    assert "UNIQUE (zulip_chat_uuid, name)" in schema
    assert "max_message_id" not in schema
    assert "CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_messages" in schema
    assert "topic_uuid uuid" in schema
    assert "sender_user_uuid uuid NOT NULL" in schema
    assert "is_read boolean NOT NULL DEFAULT false" in schema
    assert "is_starred boolean NOT NULL DEFAULT false" in schema
    assert "reactions jsonb NOT NULL DEFAULT '[]'::jsonb" in schema
    assert "message_hash bytea NOT NULL" in schema
    assert "zulip_messages_user_chat_unread_idx" in schema
    assert "zulip_messages_topic_uuid_idx" in schema
    assert "CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_events" in schema
    assert "processing_status text NOT NULL DEFAULT 'pending'" in schema
    assert "attempt_count integer NOT NULL DEFAULT 0" in schema
    assert "processed_at timestamptz" in schema
    assert "outcome_reason text" in schema
    assert "REFERENCES workspace_zulip_bridge.zulip_users (uuid)" in schema
    assert "UNIQUE (zulip_user_uuid, queue_id, event_id)" in schema
    assert "CREATE INDEX IF NOT EXISTS zulip_events_created_at_brin" in schema
    assert "CREATE INDEX IF NOT EXISTS zulip_events_pending_idx" in schema
    assert "CREATE INDEX IF NOT EXISTS zulip_events_processing_idx" in schema
    assert "CREATE INDEX IF NOT EXISTS zulip_events_terminal_retention_idx" in schema
    assert "CREATE INDEX IF NOT EXISTS zulip_events_processed_at_brin" in schema
    assert "USING brin (created_at)" in schema
    assert "bridge_event" not in schema


def test_schema_contains_empty_workspace_entity_mirrors() -> None:
    schema = (
        resources.files("workspace_zulip_bridge")
        .joinpath("schema.sql")
        .read_text(encoding="utf-8")
    )

    for table in ("workspace_chats", "workspace_topics", "workspace_messages"):
        assert f"CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.{table}" in schema

    assert "REFERENCES workspace_zulip_bridge.workspace_chats (uuid)" in schema
    assert "REFERENCES workspace_zulip_bridge.workspace_topics (uuid)" in schema
    assert "CREATE INDEX IF NOT EXISTS workspace_chats_supplier_idx" in schema
    assert (
        "CREATE INDEX IF NOT EXISTS workspace_messages_user_chat_unread_idx" in schema
    )
    assert "CREATE INDEX IF NOT EXISTS workspace_messages_updated_at_brin" in schema
    assert "workspace_chats_touch_updated_at" in schema
    assert "workspace_topics_touch_updated_at" in schema
    assert "workspace_chats_reset_history" in schema
