CREATE SCHEMA IF NOT EXISTS workspace_zulip_bridge;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_realms (
    uuid uuid PRIMARY KEY,
    identity_key text NOT NULL UNIQUE,
    endpoint text NOT NULL UNIQUE,
    workspace_project_id uuid,
    workspace_provider_uuid uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_users (
    uuid uuid PRIMARY KEY,
    realm_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    zulip_user_id bigint NOT NULL,
    login text NOT NULL,
    full_name text NOT NULL,
    role smallint NOT NULL CHECK (role IN (100, 200, 300, 400, 600)),
    disabled boolean NOT NULL DEFAULT false,
    is_bot boolean NOT NULL DEFAULT false,
    avatar_url text,
    presence_status text NOT NULL DEFAULT 'offline'
        CHECK (presence_status IN ('active', 'idle', 'offline', 'do_not_disturb')),
    status_text text,
    status_emoji text,
    last_ping_at timestamptz,
    profile_hash bytea CHECK (profile_hash IS NULL OR octet_length(profile_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (realm_uuid, zulip_user_id),
    UNIQUE (realm_uuid, login)
);
ALTER TABLE workspace_zulip_bridge.zulip_users
    ADD COLUMN IF NOT EXISTS is_bot boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_connections (
    uuid uuid PRIMARY KEY,
    realm_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    zulip_user_uuid uuid NOT NULL UNIQUE
        REFERENCES workspace_zulip_bridge.zulip_users (uuid) ON DELETE CASCADE,
    login text NOT NULL,
    api_key text NOT NULL,
    sync_enabled boolean NOT NULL DEFAULT true,
    queue_id text,
    last_event_id bigint,
    lifecycle_status text NOT NULL DEFAULT 'init'
        CHECK (lifecycle_status IN (
            'init', 'streaming', 'filling', 'scheduling', 'backfilling', 'active'
        )),
    streams_hash bytea CHECK (streams_hash IS NULL OR octet_length(streams_hash) = 32),
    catalog_completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (realm_uuid, login)
);

CREATE INDEX IF NOT EXISTS zulip_connections_active_idx
    ON workspace_zulip_bridge.zulip_connections (lifecycle_status, uuid)
    WHERE sync_enabled;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_streams (
    uuid uuid PRIMARY KEY,
    realm_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    chat_type text NOT NULL CHECK (chat_type IN ('channel', 'direct', 'group_direct')),
    chat_key text NOT NULL,
    name text NOT NULL,
    description text,
    owner_user_uuid uuid
        REFERENCES workspace_zulip_bridge.zulip_users (uuid) ON DELETE SET NULL,
    invite_only boolean NOT NULL DEFAULT false,
    announce boolean NOT NULL DEFAULT false,
    direct_user_uuid uuid
        REFERENCES workspace_zulip_bridge.zulip_users (uuid) ON DELETE SET NULL,
    private boolean NOT NULL DEFAULT false,
    is_archived boolean NOT NULL DEFAULT false,
    color integer CHECK (color IS NULL OR color BETWEEN 0 AND 16777215),
    chat_parameters jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(chat_parameters) = 'object'),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    source_connection_uuid uuid
        REFERENCES workspace_zulip_bridge.zulip_connections (uuid) ON DELETE SET NULL,
    history_loaded_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (realm_uuid, chat_key)
);

CREATE INDEX IF NOT EXISTS zulip_streams_source_idx
    ON workspace_zulip_bridge.zulip_streams (source_connection_uuid, history_loaded_at)
    WHERE source_connection_uuid IS NOT NULL;
CREATE INDEX IF NOT EXISTS zulip_streams_unassigned_idx
    ON workspace_zulip_bridge.zulip_streams (realm_uuid, chat_key)
    WHERE source_connection_uuid IS NULL;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_stream_bindings (
    uuid uuid PRIMARY KEY,
    zulip_stream_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_streams (uuid) ON DELETE CASCADE,
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid) ON DELETE CASCADE,
    role text NOT NULL
        CHECK (role IN ('owner', 'administrator', 'moderator', 'member', 'guest')),
    membership_kind text NOT NULL CHECK (membership_kind IN ('subscriber', 'participant')),
    notification_mode text NOT NULL DEFAULT 'all_messages'
        CHECK (notification_mode IN ('all_messages', 'mentions_only', 'muted')),
    membership_parameters jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(membership_parameters) = 'object'),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    available_message_count bigint NOT NULL DEFAULT 0 CHECK (available_message_count >= 0),
    first_visible_message_id bigint,
    personal_state_loaded_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (zulip_stream_uuid, zulip_user_uuid)
);

ALTER TABLE workspace_zulip_bridge.zulip_stream_bindings
    ADD COLUMN IF NOT EXISTS first_visible_message_id bigint;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'zulip_stream_bindings_first_visible_message_id_check'
          AND conrelid =
              'workspace_zulip_bridge.zulip_stream_bindings'::regclass
    ) THEN
        ALTER TABLE workspace_zulip_bridge.zulip_stream_bindings
            ADD CONSTRAINT zulip_stream_bindings_first_visible_message_id_check
            CHECK (
                first_visible_message_id IS NULL
                OR first_visible_message_id >= 0
            );
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS zulip_stream_bindings_user_idx
    ON workspace_zulip_bridge.zulip_stream_bindings (zulip_user_uuid, zulip_stream_uuid);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_topics (
    uuid uuid PRIMARY KEY,
    zulip_stream_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_streams (uuid) ON DELETE CASCADE,
    name text NOT NULL,
    is_done boolean NOT NULL DEFAULT false,
    version integer NOT NULL DEFAULT 0 CHECK (version >= 0),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (zulip_stream_uuid, name)
);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_topic_aliases (
    zulip_stream_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_streams (uuid) ON DELETE CASCADE,
    alias text NOT NULL,
    topic_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_topics (uuid) ON DELETE CASCADE,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (zulip_stream_uuid, alias)
);
CREATE INDEX IF NOT EXISTS zulip_topic_aliases_topic_idx
    ON workspace_zulip_bridge.zulip_topic_aliases (topic_uuid, active);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_topic_bindings (
    uuid uuid PRIMARY KEY,
    zulip_stream_uuid uuid NOT NULL,
    topic_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_topics (uuid) ON DELETE CASCADE,
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid) ON DELETE CASCADE,
    notification_mode text NOT NULL DEFAULT 'default'
        CHECK (notification_mode IN ('default', 'mute', 'follow', 'unmute')),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (topic_uuid, zulip_user_uuid),
    FOREIGN KEY (zulip_stream_uuid, zulip_user_uuid)
        REFERENCES workspace_zulip_bridge.zulip_stream_bindings
            (zulip_stream_uuid, zulip_user_uuid) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS zulip_topic_bindings_user_idx
    ON workspace_zulip_bridge.zulip_topic_bindings (zulip_user_uuid, topic_uuid);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_messages (
    uuid uuid PRIMARY KEY,
    realm_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    source_connection_uuid uuid
        REFERENCES workspace_zulip_bridge.zulip_connections (uuid) ON DELETE CASCADE,
    zulip_stream_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_streams (uuid) ON DELETE CASCADE,
    topic_uuid uuid,
    sender_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid),
    zulip_message_id bigint NOT NULL,
    content text NOT NULL,
    reactions jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(reactions) = 'array'),
    reaction_users jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(reaction_users) = 'object'),
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    message_hash bytea NOT NULL CHECK (octet_length(message_hash) = 32),
    created_at timestamptz NOT NULL,
    source_updated_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT zulip_messages_topic_fkey FOREIGN KEY (topic_uuid)
        REFERENCES workspace_zulip_bridge.zulip_topics (uuid) ON DELETE CASCADE,
    UNIQUE (realm_uuid, zulip_message_id)
);
ALTER TABLE workspace_zulip_bridge.zulip_messages
    ADD COLUMN IF NOT EXISTS source_updated_at timestamptz;
UPDATE workspace_zulip_bridge.zulip_messages
SET source_updated_at = created_at
WHERE source_updated_at IS NULL;
ALTER TABLE workspace_zulip_bridge.zulip_messages
    ALTER COLUMN source_updated_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS zulip_messages_stream_timeline_idx
    ON workspace_zulip_bridge.zulip_messages
        (zulip_stream_uuid, created_at DESC, uuid DESC);
CREATE INDEX IF NOT EXISTS zulip_messages_topic_timeline_idx
    ON workspace_zulip_bridge.zulip_messages
        (topic_uuid, created_at DESC, uuid DESC) WHERE topic_uuid IS NOT NULL;
CREATE INDEX IF NOT EXISTS zulip_messages_provider_id_idx
    ON workspace_zulip_bridge.zulip_messages (realm_uuid, zulip_message_id);
CREATE INDEX IF NOT EXISTS zulip_messages_updated_at_brin
    ON workspace_zulip_bridge.zulip_messages USING brin (updated_at)
    WITH (pages_per_range = 32);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_message_flags (
    uuid uuid PRIMARY KEY,
    realm_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    zulip_stream_uuid uuid NOT NULL,
    message_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_messages (uuid) ON DELETE CASCADE,
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid) ON DELETE CASCADE,
    is_read boolean NOT NULL DEFAULT false,
    is_starred boolean NOT NULL DEFAULT false,
    is_collapsed boolean NOT NULL DEFAULT false,
    is_mentioned boolean NOT NULL DEFAULT false,
    is_stream_wildcard_mentioned boolean NOT NULL DEFAULT false,
    is_topic_wildcard_mentioned boolean NOT NULL DEFAULT false,
    has_alert_word boolean NOT NULL DEFAULT false,
    is_historical boolean NOT NULL DEFAULT false,
    flags_hash bytea NOT NULL CHECK (octet_length(flags_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (message_uuid, zulip_user_uuid),
    FOREIGN KEY (zulip_stream_uuid, zulip_user_uuid)
        REFERENCES workspace_zulip_bridge.zulip_stream_bindings
            (zulip_stream_uuid, zulip_user_uuid) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS zulip_message_flags_unread_user_idx
    ON workspace_zulip_bridge.zulip_message_flags
        (zulip_user_uuid, zulip_stream_uuid, message_uuid) WHERE NOT is_read;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_message_reactions (
    uuid uuid PRIMARY KEY,
    realm_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    message_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_messages (uuid) ON DELETE CASCADE,
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid) ON DELETE CASCADE,
    emoji_name text NOT NULL,
    emoji_code text NOT NULL,
    reaction_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (message_uuid, zulip_user_uuid, reaction_type, emoji_code)
);
CREATE INDEX IF NOT EXISTS zulip_message_reactions_snapshot_idx
    ON workspace_zulip_bridge.zulip_message_reactions
        (message_uuid, emoji_name, created_at, uuid) INCLUDE (zulip_user_uuid);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_files (
    uuid uuid PRIMARY KEY,
    realm_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    owner_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid),
    zulip_attachment_id bigint NOT NULL,
    source_path text NOT NULL,
    name text NOT NULL,
    content_type text,
    size_bytes bigint CHECK (size_bytes IS NULL OR size_bytes >= 0),
    source_hash text,
    source_created_at timestamptz NOT NULL,
    message_ids bigint[] NOT NULL DEFAULT '{}'::bigint[],
    metadata_hash bytea NOT NULL CHECK (octet_length(metadata_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (realm_uuid, source_path),
    UNIQUE (realm_uuid, zulip_attachment_id)
);
ALTER TABLE workspace_zulip_bridge.zulip_files
    ADD COLUMN IF NOT EXISTS message_ids bigint[] NOT NULL DEFAULT '{}'::bigint[];
CREATE INDEX IF NOT EXISTS zulip_files_message_ids_idx
    ON workspace_zulip_bridge.zulip_files USING gin (message_ids);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_message_files (
    message_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_messages (uuid) ON DELETE CASCADE,
    file_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_files (uuid) ON DELETE CASCADE,
    position integer NOT NULL CHECK (position >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (message_uuid, file_uuid)
);
CREATE INDEX IF NOT EXISTS zulip_message_files_file_idx
    ON workspace_zulip_bridge.zulip_message_files (file_uuid, message_uuid);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_events (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    zulip_connection_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_connections (uuid) ON DELETE CASCADE,
    queue_id text NOT NULL,
    event_id bigint NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    processing_status text NOT NULL DEFAULT 'pending'
        CHECK (processing_status IN ('pending', 'processing', 'applied', 'skipped', 'failed')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_at timestamptz,
    processed_at timestamptz,
    outcome_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (zulip_connection_uuid, queue_id, event_id)
);
CREATE INDEX IF NOT EXISTS zulip_events_pending_idx
    ON workspace_zulip_bridge.zulip_events (available_at, created_at, uuid)
    WHERE processing_status = 'pending';
CREATE INDEX IF NOT EXISTS zulip_events_processing_idx
    ON workspace_zulip_bridge.zulip_events (claimed_at, uuid)
    WHERE processing_status = 'processing';
CREATE INDEX IF NOT EXISTS zulip_events_terminal_retention_idx
    ON workspace_zulip_bridge.zulip_events (created_at, uuid)
    WHERE processing_status IN ('applied', 'skipped', 'failed');
CREATE INDEX IF NOT EXISTS zulip_events_created_at_brin
    ON workspace_zulip_bridge.zulip_events USING brin (created_at)
    WITH (pages_per_range = 32);
CREATE INDEX IF NOT EXISTS zulip_events_processed_at_brin
    ON workspace_zulip_bridge.zulip_events USING brin (processed_at)
    WITH (pages_per_range = 32);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_outbox (
    sequence bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    uuid uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    realm_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    workspace_project_id uuid,
    entity_type text NOT NULL CHECK (entity_type IN (
        'user', 'stream', 'stream_binding', 'topic', 'topic_binding',
        'message', 'message_flag', 'message_reaction', 'file'
    )),
    action text NOT NULL CHECK (action IN ('upsert', 'delete')),
    entity_uuid uuid NOT NULL,
    entity_hash bytea CHECK (entity_hash IS NULL OR octet_length(entity_hash) = 32),
    payload jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'),
    delivery_status text NOT NULL DEFAULT 'pending'
        CHECK (delivery_status IN ('pending', 'delivering', 'delivered', 'failed')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_at timestamptz,
    delivered_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX IF NOT EXISTS workspace_outbox_pending_entity_idx
    ON workspace_zulip_bridge.workspace_outbox
        (realm_uuid, entity_type, entity_uuid) WHERE delivery_status = 'pending';
CREATE INDEX IF NOT EXISTS workspace_outbox_claim_idx
    ON workspace_zulip_bridge.workspace_outbox (available_at, sequence)
    WHERE delivery_status IN ('pending', 'failed');

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_sync_cursors (
    realm_uuid uuid PRIMARY KEY
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    last_delivered_sequence bigint NOT NULL DEFAULT 0
        CHECK (last_delivered_sequence >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_event_cursors (
    provider_uuid uuid PRIMARY KEY,
    workspace_project_id uuid NOT NULL,
    epoch_generation uuid,
    last_epoch_version bigint NOT NULL DEFAULT 0
        CHECK (last_epoch_version >= 0),
    recovery_required boolean NOT NULL DEFAULT false,
    recovery_reason text,
    connected_at timestamptz,
    disconnected_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_events (
    sequence bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    uuid uuid NOT NULL UNIQUE,
    provider_uuid uuid NOT NULL,
    workspace_project_id uuid NOT NULL,
    epoch_generation uuid,
    epoch_version bigint NOT NULL CHECK (epoch_version >= 0),
    object_type text NOT NULL,
    action text NOT NULL,
    entity_uuid uuid,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    processing_status text NOT NULL DEFAULT 'pending'
        CHECK (processing_status IN (
            'pending', 'processing', 'applied', 'skipped', 'failed'
        )),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claimed_at timestamptz,
    processed_at timestamptz,
    last_error text,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX IF NOT EXISTS workspace_events_epoch_idx
    ON workspace_zulip_bridge.workspace_events
        (provider_uuid, epoch_generation, epoch_version)
    WHERE epoch_generation IS NOT NULL;
CREATE INDEX IF NOT EXISTS workspace_events_pending_idx
    ON workspace_zulip_bridge.workspace_events (sequence)
    WHERE processing_status = 'pending';
CREATE INDEX IF NOT EXISTS workspace_events_received_at_brin
    ON workspace_zulip_bridge.workspace_events USING brin (received_at)
    WITH (pages_per_range = 32);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_mirror_state (
    provider_uuid uuid PRIMARY KEY,
    workspace_project_id uuid NOT NULL,
    active_generation uuid,
    epoch_generation uuid,
    snapshot_epoch_version bigint NOT NULL DEFAULT 0,
    bootstrap_status text NOT NULL DEFAULT 'required'
        CHECK (bootstrap_status IN ('required', 'loading', 'ready', 'failed')),
    entity_counts jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(entity_counts) = 'object'),
    snapshot_hash bytea CHECK (
        snapshot_hash IS NULL OR octet_length(snapshot_hash) = 32
    ),
    last_error text,
    bootstrapped_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_users (
    provider_uuid uuid NOT NULL, snapshot_generation uuid NOT NULL, uuid uuid NOT NULL,
    workspace_project_id uuid NOT NULL, content_hash bytea NOT NULL,
    source_updated_at timestamptz NOT NULL, data jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_uuid, snapshot_generation, uuid)
);
CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_streams
    (LIKE workspace_zulip_bridge.workspace_users INCLUDING ALL);
CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_stream_bindings
    (LIKE workspace_zulip_bridge.workspace_users INCLUDING ALL);
CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_topics
    (LIKE workspace_zulip_bridge.workspace_users INCLUDING ALL);
CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_topic_bindings
    (LIKE workspace_zulip_bridge.workspace_users INCLUDING ALL);
CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_messages
    (LIKE workspace_zulip_bridge.workspace_users INCLUDING ALL);
CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_message_flags
    (LIKE workspace_zulip_bridge.workspace_users INCLUDING ALL);
CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_message_reactions
    (LIKE workspace_zulip_bridge.workspace_users INCLUDING ALL);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.sync_diffs (
    provider_uuid uuid NOT NULL,
    entity_type text NOT NULL CHECK (entity_type IN (
        'users', 'streams', 'stream_bindings', 'topics', 'topic_bindings',
        'messages', 'message_flags', 'message_reactions'
    )),
    entity_uuid uuid NOT NULL,
    realm_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_realms (uuid) ON DELETE CASCADE,
    direction text NOT NULL CHECK (direction IN ('to_workspace', 'to_zulip')),
    processing_status text NOT NULL DEFAULT 'pending'
        CHECK (processing_status IN (
            'pending', 'processing', 'applied', 'skipped', 'failed', 'blocked'
        )),
    source_hash bytea,
    target_hash bytea,
    source_updated_at timestamptz NOT NULL,
    target_updated_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_at timestamptz,
    processed_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_uuid, entity_type, entity_uuid)
);
CREATE INDEX IF NOT EXISTS sync_diffs_pending_idx
    ON workspace_zulip_bridge.sync_diffs
        (available_at, entity_type, source_updated_at, entity_uuid)
    WHERE processing_status IN ('pending', 'failed');
CREATE INDEX IF NOT EXISTS sync_diffs_processing_idx
    ON workspace_zulip_bridge.sync_diffs (claimed_at, entity_uuid)
    WHERE processing_status = 'processing';

CREATE OR REPLACE FUNCTION workspace_zulip_bridge.touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'zulip_realms', 'zulip_users', 'zulip_connections', 'zulip_streams',
        'zulip_stream_bindings', 'zulip_topics', 'zulip_topic_aliases',
        'zulip_topic_bindings', 'zulip_messages', 'zulip_message_flags',
        'zulip_message_reactions', 'zulip_files', 'workspace_outbox',
        'workspace_event_cursors', 'workspace_events', 'workspace_mirror_state',
        'workspace_users', 'workspace_streams', 'workspace_stream_bindings',
        'workspace_topics', 'workspace_topic_bindings', 'workspace_messages',
        'workspace_message_flags', 'workspace_message_reactions', 'sync_diffs'
    ] LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON workspace_zulip_bridge.%I',
            table_name || '_touch_updated_at', table_name
        );
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE ON workspace_zulip_bridge.%I '
            'FOR EACH ROW EXECUTE FUNCTION workspace_zulip_bridge.touch_updated_at()',
            table_name || '_touch_updated_at', table_name
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION workspace_zulip_bridge.reset_stream_history()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.source_connection_uuid IS DISTINCT FROM OLD.source_connection_uuid THEN
        NEW.history_loaded_at = NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS zulip_streams_reset_history
    ON workspace_zulip_bridge.zulip_streams;
CREATE TRIGGER zulip_streams_reset_history
BEFORE UPDATE ON workspace_zulip_bridge.zulip_streams
FOR EACH ROW EXECUTE FUNCTION workspace_zulip_bridge.reset_stream_history();
