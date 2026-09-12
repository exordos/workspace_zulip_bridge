CREATE SCHEMA IF NOT EXISTS workspace_zulip_bridge;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_users (
    uuid uuid PRIMARY KEY,
    endpoint text NOT NULL,
    login text NOT NULL,
    api_key text,
    zulip_user_id bigint,
    full_name text,
    role smallint CHECK (role IN (100, 200, 300, 400, 600)),
    disabled boolean NOT NULL DEFAULT false,
    queue_id text,
    last_event_id bigint,
    status text NOT NULL DEFAULT 'init'
        CHECK (
            status IN (
                'init',
                'streaming',
                'filling',
                'scheduling',
                'backfilling',
                'active'
            )
        ),
    chats_hash bytea CHECK (chats_hash IS NULL OR octet_length(chats_hash) = 32),
    catalog_completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (endpoint, login)
);

ALTER TABLE workspace_zulip_bridge.zulip_users
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'init';

ALTER TABLE workspace_zulip_bridge.zulip_users
    ADD COLUMN IF NOT EXISTS chats_hash bytea;

ALTER TABLE workspace_zulip_bridge.zulip_users
    ADD COLUMN IF NOT EXISTS zulip_user_id bigint;

ALTER TABLE workspace_zulip_bridge.zulip_users
    ADD COLUMN IF NOT EXISTS full_name text;

ALTER TABLE workspace_zulip_bridge.zulip_users
    ADD COLUMN IF NOT EXISTS disabled boolean NOT NULL DEFAULT false;

ALTER TABLE workspace_zulip_bridge.zulip_users
    ADD COLUMN IF NOT EXISTS role smallint;

ALTER TABLE workspace_zulip_bridge.zulip_users
    ADD COLUMN IF NOT EXISTS catalog_completed_at timestamptz;

ALTER TABLE workspace_zulip_bridge.zulip_users
    ALTER COLUMN api_key DROP NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS zulip_users_endpoint_user_id_key
    ON workspace_zulip_bridge.zulip_users (endpoint, zulip_user_id)
    WHERE zulip_user_id IS NOT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'zulip_users_status_check'
          AND conrelid = 'workspace_zulip_bridge.zulip_users'::regclass
          AND (
              pg_get_constraintdef(oid) NOT LIKE '%scheduling%'
              OR pg_get_constraintdef(oid) NOT LIKE '%backfilling%'
          )
    ) THEN
        ALTER TABLE workspace_zulip_bridge.zulip_users
            DROP CONSTRAINT zulip_users_status_check;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'zulip_users_status_check'
          AND conrelid = 'workspace_zulip_bridge.zulip_users'::regclass
    ) THEN
        ALTER TABLE workspace_zulip_bridge.zulip_users
            ADD CONSTRAINT zulip_users_status_check
            CHECK (
                status IN (
                    'init',
                    'streaming',
                    'filling',
                    'scheduling',
                    'backfilling',
                    'active'
                )
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'zulip_users_chats_hash_check'
          AND conrelid = 'workspace_zulip_bridge.zulip_users'::regclass
    ) THEN
        ALTER TABLE workspace_zulip_bridge.zulip_users
            ADD CONSTRAINT zulip_users_chats_hash_check
            CHECK (chats_hash IS NULL OR octet_length(chats_hash) = 32);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_chats (
    uuid uuid PRIMARY KEY,
    endpoint text NOT NULL,
    chat_type text NOT NULL
        CHECK (chat_type IN ('channel', 'direct', 'group_direct')),
    chat_key text NOT NULL,
    name text NOT NULL,
    chat_parameters jsonb NOT NULL,
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    supplier_user_uuid uuid
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE SET NULL,
    history_loaded_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (endpoint, chat_key)
);

CREATE INDEX IF NOT EXISTS zulip_chats_supplier_idx
    ON workspace_zulip_bridge.zulip_chats (
        supplier_user_uuid,
        history_loaded_at
    )
    WHERE supplier_user_uuid IS NOT NULL;

CREATE INDEX IF NOT EXISTS zulip_chats_unassigned_idx
    ON workspace_zulip_bridge.zulip_chats (endpoint, chat_key)
    WHERE supplier_user_uuid IS NULL;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_chat_users (
    zulip_chat_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_chats (uuid)
        ON DELETE CASCADE,
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE CASCADE,
    role text NOT NULL CHECK (role IN ('subscriber', 'participant')),
    membership_parameters jsonb NOT NULL,
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    available_message_count bigint NOT NULL DEFAULT 0
        CHECK (available_message_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (zulip_chat_uuid, zulip_user_uuid)
);

CREATE INDEX IF NOT EXISTS zulip_chat_users_user_idx
    ON workspace_zulip_bridge.zulip_chat_users (
        zulip_user_uuid,
        zulip_chat_uuid
    );

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_topics (
    uuid uuid PRIMARY KEY,
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE CASCADE,
    zulip_chat_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_chats (uuid)
        ON DELETE CASCADE,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (zulip_chat_uuid, name)
);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_messages (
    uuid uuid PRIMARY KEY,
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE CASCADE,
    zulip_chat_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_chats (uuid)
        ON DELETE CASCADE,
    topic_uuid uuid,
    sender_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE CASCADE,
    zulip_message_id bigint NOT NULL,
    content text NOT NULL,
    is_read boolean NOT NULL DEFAULT false,
    is_starred boolean NOT NULL DEFAULT false,
    is_collapsed boolean NOT NULL DEFAULT false,
    is_mentioned boolean NOT NULL DEFAULT false,
    is_stream_wildcard_mentioned boolean NOT NULL DEFAULT false,
    is_topic_wildcard_mentioned boolean NOT NULL DEFAULT false,
    has_alert_word boolean NOT NULL DEFAULT false,
    is_historical boolean NOT NULL DEFAULT false,
    reactions jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(reactions) = 'array'),
    message_hash bytea NOT NULL CHECK (octet_length(message_hash) = 32),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT zulip_messages_topic_fkey
        FOREIGN KEY (topic_uuid)
        REFERENCES workspace_zulip_bridge.zulip_topics (uuid)
        ON DELETE CASCADE,
    UNIQUE (zulip_chat_uuid, zulip_message_id)
);

CREATE INDEX IF NOT EXISTS zulip_messages_user_chat_message_idx
    ON workspace_zulip_bridge.zulip_messages (
        zulip_user_uuid,
        zulip_chat_uuid,
        zulip_message_id DESC
    );

CREATE INDEX IF NOT EXISTS zulip_messages_provider_id_idx
    ON workspace_zulip_bridge.zulip_messages (zulip_message_id);

CREATE INDEX IF NOT EXISTS zulip_messages_user_chat_unread_idx
    ON workspace_zulip_bridge.zulip_messages (
        zulip_user_uuid,
        zulip_chat_uuid,
        zulip_message_id DESC
    )
    WHERE NOT is_read;

CREATE INDEX IF NOT EXISTS zulip_messages_topic_uuid_idx
    ON workspace_zulip_bridge.zulip_messages (topic_uuid)
    WHERE topic_uuid IS NOT NULL;

CREATE INDEX IF NOT EXISTS zulip_messages_updated_at_brin
    ON workspace_zulip_bridge.zulip_messages
    USING brin (updated_at)
    WITH (pages_per_range = 32);

-- Empty destination mirrors. Their provenance columns intentionally match the
-- canonical Zulip tables; no runtime writer targets these tables yet.
CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_chats (
    uuid uuid PRIMARY KEY,
    endpoint text NOT NULL,
    chat_type text NOT NULL
        CHECK (chat_type IN ('channel', 'direct', 'group_direct')),
    chat_key text NOT NULL,
    name text NOT NULL,
    chat_parameters jsonb NOT NULL,
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    supplier_user_uuid uuid
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE SET NULL,
    history_loaded_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (endpoint, chat_key)
);

CREATE INDEX IF NOT EXISTS workspace_chats_supplier_idx
    ON workspace_zulip_bridge.workspace_chats (
        supplier_user_uuid,
        history_loaded_at
    )
    WHERE supplier_user_uuid IS NOT NULL;

CREATE INDEX IF NOT EXISTS workspace_chats_unassigned_idx
    ON workspace_zulip_bridge.workspace_chats (endpoint, chat_key)
    WHERE supplier_user_uuid IS NULL;

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_topics (
    uuid uuid PRIMARY KEY,
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE CASCADE,
    zulip_chat_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.workspace_chats (uuid)
        ON DELETE CASCADE,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (zulip_chat_uuid, name)
);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.workspace_messages (
    uuid uuid PRIMARY KEY,
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE CASCADE,
    zulip_chat_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.workspace_chats (uuid)
        ON DELETE CASCADE,
    topic_uuid uuid,
    sender_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE CASCADE,
    zulip_message_id bigint NOT NULL,
    content text NOT NULL,
    is_read boolean NOT NULL DEFAULT false,
    is_starred boolean NOT NULL DEFAULT false,
    is_collapsed boolean NOT NULL DEFAULT false,
    is_mentioned boolean NOT NULL DEFAULT false,
    is_stream_wildcard_mentioned boolean NOT NULL DEFAULT false,
    is_topic_wildcard_mentioned boolean NOT NULL DEFAULT false,
    has_alert_word boolean NOT NULL DEFAULT false,
    is_historical boolean NOT NULL DEFAULT false,
    reactions jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(reactions) = 'array'),
    message_hash bytea NOT NULL CHECK (octet_length(message_hash) = 32),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT workspace_messages_topic_fkey
        FOREIGN KEY (topic_uuid)
        REFERENCES workspace_zulip_bridge.workspace_topics (uuid)
        ON DELETE CASCADE,
    UNIQUE (zulip_chat_uuid, zulip_message_id)
);

CREATE INDEX IF NOT EXISTS workspace_messages_user_chat_message_idx
    ON workspace_zulip_bridge.workspace_messages (
        zulip_user_uuid,
        zulip_chat_uuid,
        zulip_message_id DESC
    );

CREATE INDEX IF NOT EXISTS workspace_messages_provider_id_idx
    ON workspace_zulip_bridge.workspace_messages (zulip_message_id);

CREATE INDEX IF NOT EXISTS workspace_messages_user_chat_unread_idx
    ON workspace_zulip_bridge.workspace_messages (
        zulip_user_uuid,
        zulip_chat_uuid,
        zulip_message_id DESC
    )
    WHERE NOT is_read;

CREATE INDEX IF NOT EXISTS workspace_messages_topic_uuid_idx
    ON workspace_zulip_bridge.workspace_messages (topic_uuid)
    WHERE topic_uuid IS NOT NULL;

CREATE INDEX IF NOT EXISTS workspace_messages_updated_at_brin
    ON workspace_zulip_bridge.workspace_messages
    USING brin (updated_at)
    WITH (pages_per_range = 32);

CREATE TABLE IF NOT EXISTS workspace_zulip_bridge.zulip_events (
    uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    zulip_user_uuid uuid NOT NULL
        REFERENCES workspace_zulip_bridge.zulip_users (uuid)
        ON DELETE CASCADE,
    queue_id text NOT NULL,
    event_id bigint NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    processing_status text NOT NULL DEFAULT 'pending'
        CHECK (
            processing_status IN (
                'pending',
                'processing',
                'applied',
                'skipped',
                'failed'
            )
        ),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_at timestamptz,
    processed_at timestamptz,
    outcome_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (zulip_user_uuid, queue_id, event_id)
);

ALTER TABLE workspace_zulip_bridge.zulip_events
    ADD COLUMN IF NOT EXISTS processing_status text NOT NULL DEFAULT 'pending';

ALTER TABLE workspace_zulip_bridge.zulip_events
    ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0;

ALTER TABLE workspace_zulip_bridge.zulip_events
    ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL
        DEFAULT clock_timestamp();

ALTER TABLE workspace_zulip_bridge.zulip_events
    ADD COLUMN IF NOT EXISTS claimed_at timestamptz;

ALTER TABLE workspace_zulip_bridge.zulip_events
    ADD COLUMN IF NOT EXISTS processed_at timestamptz;

ALTER TABLE workspace_zulip_bridge.zulip_events
    ADD COLUMN IF NOT EXISTS outcome_reason text;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'zulip_events_processing_status_check'
          AND conrelid = 'workspace_zulip_bridge.zulip_events'::regclass
    ) THEN
        ALTER TABLE workspace_zulip_bridge.zulip_events
            ADD CONSTRAINT zulip_events_processing_status_check
            CHECK (
                processing_status IN (
                    'pending',
                    'processing',
                    'applied',
                    'skipped',
                    'failed'
                )
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'zulip_events_attempt_count_check'
          AND conrelid = 'workspace_zulip_bridge.zulip_events'::regclass
    ) THEN
        ALTER TABLE workspace_zulip_bridge.zulip_events
            ADD CONSTRAINT zulip_events_attempt_count_check
            CHECK (attempt_count >= 0);
    END IF;
END;
$$;

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
    ON workspace_zulip_bridge.zulip_events
    USING brin (created_at)
    WITH (pages_per_range = 32);

CREATE INDEX IF NOT EXISTS zulip_events_processed_at_brin
    ON workspace_zulip_bridge.zulip_events
    USING brin (processed_at)
    WITH (pages_per_range = 32);

CREATE OR REPLACE FUNCTION workspace_zulip_bridge.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS zulip_users_touch_updated_at
    ON workspace_zulip_bridge.zulip_users;

CREATE TRIGGER zulip_users_touch_updated_at
BEFORE UPDATE ON workspace_zulip_bridge.zulip_users
FOR EACH ROW
EXECUTE FUNCTION workspace_zulip_bridge.touch_updated_at();

DROP TRIGGER IF EXISTS zulip_chats_touch_updated_at
    ON workspace_zulip_bridge.zulip_chats;

CREATE TRIGGER zulip_chats_touch_updated_at
BEFORE UPDATE ON workspace_zulip_bridge.zulip_chats
FOR EACH ROW
EXECUTE FUNCTION workspace_zulip_bridge.touch_updated_at();

DROP TRIGGER IF EXISTS zulip_topics_touch_updated_at
    ON workspace_zulip_bridge.zulip_topics;

CREATE TRIGGER zulip_topics_touch_updated_at
BEFORE UPDATE ON workspace_zulip_bridge.zulip_topics
FOR EACH ROW
EXECUTE FUNCTION workspace_zulip_bridge.touch_updated_at();

DROP TRIGGER IF EXISTS workspace_chats_touch_updated_at
    ON workspace_zulip_bridge.workspace_chats;

CREATE TRIGGER workspace_chats_touch_updated_at
BEFORE UPDATE ON workspace_zulip_bridge.workspace_chats
FOR EACH ROW
EXECUTE FUNCTION workspace_zulip_bridge.touch_updated_at();

DROP TRIGGER IF EXISTS workspace_topics_touch_updated_at
    ON workspace_zulip_bridge.workspace_topics;

CREATE TRIGGER workspace_topics_touch_updated_at
BEFORE UPDATE ON workspace_zulip_bridge.workspace_topics
FOR EACH ROW
EXECUTE FUNCTION workspace_zulip_bridge.touch_updated_at();

CREATE OR REPLACE FUNCTION workspace_zulip_bridge.reset_chat_history()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.supplier_user_uuid IS DISTINCT FROM OLD.supplier_user_uuid
    THEN
        NEW.history_loaded_at = NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS zulip_chats_reset_history
    ON workspace_zulip_bridge.zulip_chats;

CREATE TRIGGER zulip_chats_reset_history
BEFORE UPDATE ON workspace_zulip_bridge.zulip_chats
FOR EACH ROW
EXECUTE FUNCTION workspace_zulip_bridge.reset_chat_history();

DROP TRIGGER IF EXISTS workspace_chats_reset_history
    ON workspace_zulip_bridge.workspace_chats;

CREATE TRIGGER workspace_chats_reset_history
BEFORE UPDATE ON workspace_zulip_bridge.workspace_chats
FOR EACH ROW
EXECUTE FUNCTION workspace_zulip_bridge.reset_chat_history();

DROP TRIGGER IF EXISTS zulip_chat_users_touch_updated_at
    ON workspace_zulip_bridge.zulip_chat_users;

CREATE TRIGGER zulip_chat_users_touch_updated_at
BEFORE UPDATE ON workspace_zulip_bridge.zulip_chat_users
FOR EACH ROW
EXECUTE FUNCTION workspace_zulip_bridge.touch_updated_at();
