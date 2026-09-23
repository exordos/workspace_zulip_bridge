-- Copyright 2026 Genesis Corporation
-- Licensed under the Apache License, Version 2.0 (the "License").

CREATE SCHEMA IF NOT EXISTS workspace_zulip_bridge;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.v4_external_accounts (
    uuid uuid PRIMARY KEY,
    owner_workspace_user_uuid uuid NOT NULL,
    desired_generation bigint NOT NULL CHECK (desired_generation >= 0),
    workspace_project_id uuid NOT NULL,
    endpoint text NOT NULL,
    login text NOT NULL,
    api_key text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS v4_external_accounts_enabled_idx
    ON workspace_zulip_bridge.v4_external_accounts (enabled, uuid);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.v4_zulip_queues (
    external_account_uuid uuid PRIMARY KEY
        REFERENCES workspace_zulip_bridge.v4_external_accounts (uuid)
        ON DELETE CASCADE,
    queue_id text,
    last_event_id bigint,
    connection_status text NOT NULL DEFAULT 'disconnected'
        CHECK (connection_status IN (
            'disconnected', 'connecting', 'connected', 'auth_required'
        )),
    connected_at timestamptz,
    disconnected_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((queue_id IS NULL) = (last_event_id IS NULL))
);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.v4_workspace_event_cursors (
    provider_uuid uuid PRIMARY KEY,
    workspace_project_id uuid NOT NULL,
    epoch_generation uuid,
    last_epoch_version bigint NOT NULL DEFAULT 0
        CHECK (last_epoch_version >= 0),
    connected_at timestamptz,
    disconnected_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
