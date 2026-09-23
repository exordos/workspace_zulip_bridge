CREATE SCHEMA IF NOT EXISTS workspace_zulip_bridge;

-- The canonical schema stays a clean install definition.  These guards run
-- before it so an element update can safely reuse an older persistent volume.
DO $upgrade$
BEGIN
    IF to_regclass('workspace_zulip_bridge.zulip_realms') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_realms
            ADD COLUMN IF NOT EXISTS presence_offline_threshold_seconds integer
            NOT NULL DEFAULT 200;
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid =
                    'workspace_zulip_bridge.zulip_realms'::regclass
              AND conname = 'zulip_realms_presence_offline_threshold_check'
        ) THEN
            ALTER TABLE workspace_zulip_bridge.zulip_realms
                ADD CONSTRAINT zulip_realms_presence_offline_threshold_check
                CHECK (presence_offline_threshold_seconds > 0);
        END IF;
    END IF;

    IF to_regclass('workspace_zulip_bridge.zulip_users') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_users
            ADD COLUMN IF NOT EXISTS is_bot boolean NOT NULL DEFAULT false;
        ALTER TABLE workspace_zulip_bridge.zulip_users
            ADD COLUMN IF NOT EXISTS workspace_user_uuid uuid;
    END IF;

    IF to_regclass('workspace_zulip_bridge.zulip_connections') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS external_account_uuid uuid;
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS owner_workspace_user_uuid uuid;
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS desired_generation bigint;
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS last_event_cursor_at timestamptz;
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS reconcile_since timestamptz;
        UPDATE workspace_zulip_bridge.zulip_connections
        SET last_event_cursor_at = updated_at
        WHERE queue_id IS NOT NULL AND last_event_cursor_at IS NULL;
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid =
                    'workspace_zulip_bridge.zulip_connections'::regclass
              AND conname = 'zulip_connections_external_account_uuid_key'
        ) THEN
            ALTER TABLE workspace_zulip_bridge.zulip_connections
                ADD CONSTRAINT zulip_connections_external_account_uuid_key
                UNIQUE (external_account_uuid);
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid =
                    'workspace_zulip_bridge.zulip_connections'::regclass
              AND conname = 'zulip_connections_desired_generation_check'
        ) THEN
            ALTER TABLE workspace_zulip_bridge.zulip_connections
                ADD CONSTRAINT zulip_connections_desired_generation_check
                CHECK (desired_generation IS NULL OR desired_generation > 0);
        END IF;
    END IF;

    IF to_regclass('workspace_zulip_bridge.zulip_streams') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_streams
            ALTER COLUMN description SET DEFAULT '';
        UPDATE workspace_zulip_bridge.zulip_streams
        SET description = ''
        WHERE description IS NULL;
        ALTER TABLE workspace_zulip_bridge.zulip_streams
            ALTER COLUMN description SET NOT NULL;
    END IF;

    IF to_regclass('workspace_zulip_bridge.zulip_stream_bindings') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_stream_bindings
            ADD COLUMN IF NOT EXISTS first_visible_message_id bigint;
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid =
                    'workspace_zulip_bridge.zulip_stream_bindings'::regclass
              AND conname =
                    'zulip_stream_bindings_first_visible_message_id_check'
        ) THEN
            ALTER TABLE workspace_zulip_bridge.zulip_stream_bindings
                ADD CONSTRAINT
                    zulip_stream_bindings_first_visible_message_id_check
                CHECK (
                    first_visible_message_id IS NULL
                    OR first_visible_message_id >= 0
                );
        END IF;
    END IF;

    IF to_regclass('workspace_zulip_bridge.zulip_messages') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_messages
            ADD COLUMN IF NOT EXISTS source_updated_at timestamptz;
        UPDATE workspace_zulip_bridge.zulip_messages
        SET source_updated_at = created_at
        WHERE source_updated_at IS NULL;
        ALTER TABLE workspace_zulip_bridge.zulip_messages
            ALTER COLUMN source_updated_at SET NOT NULL;
    END IF;

    IF to_regclass('workspace_zulip_bridge.zulip_entity_links') IS NOT NULL
       AND EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'workspace_zulip_bridge'
              AND table_name = 'zulip_entity_links'
              AND column_name = 'zulip_external_id'
       )
       AND NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'workspace_zulip_bridge'
              AND table_name = 'zulip_entity_links'
              AND column_name = 'zulip_external_key'
       ) THEN
        ALTER TABLE workspace_zulip_bridge.zulip_entity_links
            RENAME COLUMN zulip_external_id TO zulip_external_key;
        ALTER TABLE workspace_zulip_bridge.zulip_entity_links
            ALTER COLUMN zulip_external_key TYPE text
            USING zulip_external_key::text;
        ALTER TABLE workspace_zulip_bridge.zulip_entity_links
            DROP CONSTRAINT IF EXISTS zulip_entity_links_entity_type_check;
        ALTER TABLE workspace_zulip_bridge.zulip_entity_links
            ADD CONSTRAINT zulip_entity_links_entity_type_check
            CHECK (entity_type IN ('stream', 'message'));
    END IF;

    IF to_regclass('workspace_zulip_bridge.zulip_files') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_files
            ADD COLUMN IF NOT EXISTS message_ids bigint[]
            NOT NULL DEFAULT '{}'::bigint[];
    END IF;

    IF to_regclass('workspace_zulip_bridge.workspace_events') IS NOT NULL
       AND NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'workspace_zulip_bridge'
              AND table_name = 'workspace_events'
              AND column_name = 'available_at'
       ) THEN
        ALTER TABLE workspace_zulip_bridge.workspace_events
            ADD COLUMN available_at timestamptz
            NOT NULL DEFAULT clock_timestamp();
        DROP INDEX IF EXISTS
            workspace_zulip_bridge.workspace_events_pending_idx;
    END IF;

    IF to_regclass('workspace_zulip_bridge.workspace_mirror_state') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.workspace_mirror_state
            ADD COLUMN IF NOT EXISTS initial_sync_completed_at timestamptz;
        ALTER TABLE workspace_zulip_bridge.workspace_mirror_state
            ADD COLUMN IF NOT EXISTS reconciliation_version smallint
            NOT NULL DEFAULT 0;
        ALTER TABLE workspace_zulip_bridge.workspace_mirror_state
            ADD COLUMN IF NOT EXISTS target_scan_generation uuid;
        UPDATE workspace_zulip_bridge.workspace_mirror_state
        SET target_scan_generation = active_generation
        WHERE initial_sync_completed_at IS NOT NULL
          AND target_scan_generation IS NULL;
    END IF;

    IF to_regclass('workspace_zulip_bridge.sync_diffs') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.sync_diffs
            ADD COLUMN IF NOT EXISTS partition_key uuid;
        ALTER TABLE workspace_zulip_bridge.sync_diffs
            ADD COLUMN IF NOT EXISTS delivery_priority smallint
            NOT NULL DEFAULT 1;
        ALTER TABLE workspace_zulip_bridge.sync_diffs
            ADD COLUMN IF NOT EXISTS dependency_wait_count integer
            NOT NULL DEFAULT 0;
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid =
                    'workspace_zulip_bridge.sync_diffs'::regclass
              AND conname = 'sync_diffs_dependency_wait_count_check'
        ) THEN
            ALTER TABLE workspace_zulip_bridge.sync_diffs
                ADD CONSTRAINT sync_diffs_dependency_wait_count_check
                CHECK (dependency_wait_count >= 0);
        END IF;
    END IF;
END;
$upgrade$;
