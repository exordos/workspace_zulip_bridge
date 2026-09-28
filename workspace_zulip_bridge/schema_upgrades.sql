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

    IF to_regclass('workspace_zulip_bridge.zulip_topics') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_topics
            DROP CONSTRAINT IF EXISTS
                zulip_topics_zulip_stream_uuid_name_key;
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid =
                    'workspace_zulip_bridge.zulip_topics'::regclass
              AND conname = 'zulip_topics_stream_name_state_key'
        ) THEN
            ALTER TABLE workspace_zulip_bridge.zulip_topics
                ADD CONSTRAINT zulip_topics_stream_name_state_key
                UNIQUE (zulip_stream_uuid, name, is_done);
        END IF;

        CREATE TABLE IF NOT EXISTS
            workspace_zulip_bridge.zulip_topic_catalog_identities (
                uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                topic_uuid uuid UNIQUE,
                zulip_stream_uuid uuid NOT NULL
                    REFERENCES workspace_zulip_bridge.zulip_streams (uuid)
                    ON DELETE CASCADE,
                catalog_topic_key text NOT NULL,
                provider_topic_id text NOT NULL,
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                UNIQUE (zulip_stream_uuid, provider_topic_id)
            );
        CREATE INDEX IF NOT EXISTS
            zulip_topic_catalog_identities_stream_topic_idx
            ON workspace_zulip_bridge.zulip_topic_catalog_identities (
                zulip_stream_uuid, topic_uuid
            );
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
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS notification_settings_generation smallint
            NOT NULL DEFAULT 0;
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS enable_stream_desktop_notifications boolean
            NOT NULL DEFAULT true;
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS notification_settings_updated_at timestamptz
            NOT NULL DEFAULT 'epoch';
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS notification_snapshot_at timestamptz
            NOT NULL DEFAULT 'epoch';
        ALTER TABLE workspace_zulip_bridge.zulip_connections
            ADD COLUMN IF NOT EXISTS notification_refresh_requested_at timestamptz
            NOT NULL DEFAULT 'epoch';
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
              AND conname =
                    'zulip_connections_notification_settings_generation_check'
        ) THEN
            ALTER TABLE workspace_zulip_bridge.zulip_connections
                ADD CONSTRAINT
                    zulip_connections_notification_settings_generation_check
                CHECK (notification_settings_generation >= 0);
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

    IF to_regclass('workspace_zulip_bridge.zulip_topic_bindings') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_topic_bindings
            ADD COLUMN IF NOT EXISTS source_updated_at timestamptz;
        UPDATE workspace_zulip_bridge.zulip_topic_bindings
        SET source_updated_at = updated_at
        WHERE source_updated_at IS NULL;
        ALTER TABLE workspace_zulip_bridge.zulip_topic_bindings
            ALTER COLUMN source_updated_at SET DEFAULT 'epoch';
        ALTER TABLE workspace_zulip_bridge.zulip_topic_bindings
            ALTER COLUMN source_updated_at SET NOT NULL;
    END IF;

    IF to_regclass('workspace_zulip_bridge.zulip_topics') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.zulip_topics
            ADD COLUMN IF NOT EXISTS source_updated_at timestamptz
            NOT NULL DEFAULT 'epoch';
        CREATE INDEX IF NOT EXISTS zulip_topics_casefold_name_idx
            ON workspace_zulip_bridge.zulip_topics (
                zulip_stream_uuid, lower(name)
            );
    END IF;

    IF to_regclass('workspace_zulip_bridge.zulip_topic_aliases') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS zulip_topic_aliases_casefold_idx
            ON workspace_zulip_bridge.zulip_topic_aliases (
                zulip_stream_uuid, lower(alias)
            ) WHERE active;
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
        ALTER TABLE workspace_zulip_bridge.zulip_stream_bindings
            ADD COLUMN IF NOT EXISTS source_updated_at timestamptz;
        UPDATE workspace_zulip_bridge.zulip_stream_bindings
        SET source_updated_at = updated_at
        WHERE source_updated_at IS NULL;
        ALTER TABLE workspace_zulip_bridge.zulip_stream_bindings
            ALTER COLUMN source_updated_at SET DEFAULT clock_timestamp();
        ALTER TABLE workspace_zulip_bridge.zulip_stream_bindings
            ALTER COLUMN source_updated_at SET NOT NULL;
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
        -- Existing rows are re-projected in bounded bridge-owned batches.  Do
        -- not rewrite the whole message table while the schema lock is held.
        ALTER TABLE workspace_zulip_bridge.zulip_messages
            ADD COLUMN IF NOT EXISTS workspace_content text;
        ALTER TABLE workspace_zulip_bridge.zulip_messages
            ADD COLUMN IF NOT EXISTS converter_version integer
            NOT NULL DEFAULT 0;
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

    IF to_regclass(
        'workspace_zulip_bridge.workspace_chat_catalog_reports'
    ) IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ADD COLUMN IF NOT EXISTS projection_revision integer
            NOT NULL DEFAULT 1;
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ADD COLUMN IF NOT EXISTS assignment_generation bigint;
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ADD COLUMN IF NOT EXISTS assignment jsonb;
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ADD COLUMN IF NOT EXISTS assignment_reconciled boolean
            NOT NULL DEFAULT false;
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ADD COLUMN IF NOT EXISTS assignment_repair_created_at timestamptz;
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ADD COLUMN IF NOT EXISTS assignment_repair_uuid uuid;
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ADD COLUMN IF NOT EXISTS source_activity_at timestamptz;
        IF to_regclass('workspace_zulip_bridge.zulip_streams') IS NOT NULL
           AND to_regclass('workspace_zulip_bridge.zulip_messages') IS NOT NULL
        THEN
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
                AS report
            SET source_activity_at = GREATEST(
                stream.created_at,
                COALESCE((
                    SELECT message.created_at
                    FROM workspace_zulip_bridge.zulip_messages AS message
                    WHERE message.zulip_stream_uuid = stream.uuid
                    ORDER BY message.created_at DESC, message.uuid DESC
                    LIMIT 1
                ), '-infinity'::timestamptz)
            )
            FROM workspace_zulip_bridge.zulip_streams AS stream
            WHERE stream.uuid = report.zulip_stream_uuid
              AND report.source_activity_at IS NULL;
        END IF;
        UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
        SET source_activity_at = created_at
        WHERE source_activity_at IS NULL;
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ALTER COLUMN source_activity_at SET DEFAULT clock_timestamp();
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ALTER COLUMN source_activity_at SET NOT NULL;
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid =
                    'workspace_zulip_bridge.workspace_chat_catalog_reports'::regclass
              AND conname =
                    'workspace_chat_catalog_reports_projection_revision_check'
        ) THEN
            ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
                ADD CONSTRAINT
                    workspace_chat_catalog_reports_projection_revision_check
                CHECK (projection_revision > 0) NOT VALID;
        END IF;
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

-- Catalog topic identities are immutable and outlive the source topic row.
-- Seed reservations from the last acknowledged assignment before any runtime
-- normalization can change a prefixed source title.
DO $topic_catalog_upgrade$
BEGIN
    IF to_regclass(
        'workspace_zulip_bridge.zulip_topic_catalog_identities'
    ) IS NULL THEN
        RETURN;
    END IF;

    IF to_regclass(
        'workspace_zulip_bridge.workspace_chat_catalog_reports'
    ) IS NOT NULL THEN
        INSERT INTO workspace_zulip_bridge.zulip_topic_catalog_identities (
            zulip_stream_uuid, catalog_topic_key, provider_topic_id
        )
        SELECT DISTINCT report.zulip_stream_uuid,
               CASE
                   WHEN left(assigned.provider_topic_id, length(prefix.value)) =
                        prefix.value
                   THEN substr(
                       assigned.provider_topic_id,
                       length(prefix.value) + 1
                   )
                   ELSE assigned.provider_topic_id
               END,
               assigned.provider_topic_id
        FROM workspace_zulip_bridge.workspace_chat_catalog_reports AS report
        JOIN workspace_zulip_bridge.zulip_streams AS stream
          ON stream.uuid = report.zulip_stream_uuid
         AND stream.chat_type = 'channel'
        CROSS JOIN LATERAL (
            SELECT regexp_replace(stream.chat_key, '^channel:', '') || ':'
                AS value
        ) AS prefix
        CROSS JOIN LATERAL (
            SELECT item ->> 'provider_topic_id' AS provider_topic_id
            FROM jsonb_array_elements(
                COALESCE(
                    report.assignment #> '{workspace_projection,topics}',
                    '[]'::jsonb
                )
            ) AS item
            WHERE item ->> 'provider_topic_id' IS NOT NULL
        ) AS assigned
        ON CONFLICT (zulip_stream_uuid, provider_topic_id) DO NOTHING;

        IF to_regclass(
            'workspace_zulip_bridge.zulip_topic_aliases'
        ) IS NOT NULL THEN
            WITH candidate_pairs AS (
                SELECT DISTINCT identity.uuid AS identity_uuid,
                       topic.uuid AS topic_uuid
                FROM workspace_zulip_bridge.zulip_topic_catalog_identities
                    AS identity
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = identity.zulip_stream_uuid
                JOIN workspace_zulip_bridge.zulip_topics AS topic
                  ON topic.zulip_stream_uuid = identity.zulip_stream_uuid
                LEFT JOIN workspace_zulip_bridge.zulip_topic_aliases AS alias
                  ON alias.zulip_stream_uuid = topic.zulip_stream_uuid
                 AND alias.topic_uuid = topic.uuid
                WHERE identity.topic_uuid IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM workspace_zulip_bridge.zulip_topic_catalog_identities
                          AS bound_identity
                      WHERE bound_identity.topic_uuid = topic.uuid
                  )
                  AND identity.provider_topic_id =
                      regexp_replace(stream.chat_key, '^channel:', '') || ':' ||
                      COALESCE(alias.alias, topic.name)
            ), counted AS (
                SELECT identity_uuid, topic_uuid,
                       count(*) OVER (PARTITION BY identity_uuid)
                           AS topics_per_identity,
                       count(*) OVER (PARTITION BY topic_uuid)
                           AS identities_per_topic
                FROM candidate_pairs
            ), unambiguous AS (
                SELECT identity_uuid, topic_uuid
                FROM counted
                WHERE topics_per_identity = 1 AND identities_per_topic = 1
            )
            UPDATE workspace_zulip_bridge.zulip_topic_catalog_identities
                AS identity
            SET topic_uuid = unambiguous.topic_uuid
            FROM unambiguous
            WHERE identity.uuid = unambiguous.identity_uuid;
        ELSE
            WITH candidate_pairs AS (
                SELECT identity.uuid AS identity_uuid, topic.uuid AS topic_uuid
                FROM workspace_zulip_bridge.zulip_topic_catalog_identities
                    AS identity
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = identity.zulip_stream_uuid
                JOIN workspace_zulip_bridge.zulip_topics AS topic
                  ON topic.zulip_stream_uuid = identity.zulip_stream_uuid
                WHERE identity.topic_uuid IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM workspace_zulip_bridge.zulip_topic_catalog_identities
                          AS bound_identity
                      WHERE bound_identity.topic_uuid = topic.uuid
                  )
                  AND identity.provider_topic_id =
                      regexp_replace(stream.chat_key, '^channel:', '') || ':' ||
                      topic.name
            ), counted AS (
                SELECT identity_uuid, topic_uuid,
                       count(*) OVER (PARTITION BY identity_uuid)
                           AS topics_per_identity,
                       count(*) OVER (PARTITION BY topic_uuid)
                           AS identities_per_topic
                FROM candidate_pairs
            ), unambiguous AS (
                SELECT identity_uuid, topic_uuid
                FROM counted
                WHERE topics_per_identity = 1 AND identities_per_topic = 1
            )
            UPDATE workspace_zulip_bridge.zulip_topic_catalog_identities
                AS identity
            SET topic_uuid = unambiguous.topic_uuid
            FROM unambiguous
            WHERE identity.uuid = unambiguous.identity_uuid;
        END IF;
    END IF;

    INSERT INTO workspace_zulip_bridge.zulip_topic_catalog_identities (
        topic_uuid, zulip_stream_uuid, catalog_topic_key, provider_topic_id
    )
    SELECT topic.uuid, topic.zulip_stream_uuid, topic.name,
           regexp_replace(stream.chat_key, '^channel:', '') || ':' || topic.name
    FROM workspace_zulip_bridge.zulip_topics AS topic
    JOIN workspace_zulip_bridge.zulip_streams AS stream
      ON stream.uuid = topic.zulip_stream_uuid
     AND stream.chat_type = 'channel'
    WHERE NOT EXISTS (
              SELECT 1
              FROM workspace_zulip_bridge.zulip_topic_catalog_identities
                  AS identity
              WHERE identity.topic_uuid = topic.uuid
          )
      AND (
          to_regclass(
              'workspace_zulip_bridge.workspace_chat_catalog_reports'
          ) IS NULL
          OR NOT EXISTS (
              SELECT 1
              FROM workspace_zulip_bridge.workspace_chat_catalog_reports AS report
              WHERE report.zulip_stream_uuid = topic.zulip_stream_uuid
                AND report.assignment IS NOT NULL
                AND topic.created_at <= COALESCE(
                    report.reported_at, 'infinity'::timestamptz
                )
          )
      )
    ON CONFLICT DO NOTHING;
END;
$topic_catalog_upgrade$;

DO $upgrade$
BEGIN
    IF to_regclass(
        'workspace_zulip_bridge.workspace_chat_catalog_reports'
    ) IS NOT NULL THEN
        EXECUTE $index$
            CREATE INDEX IF NOT EXISTS
                workspace_chat_catalog_reports_activity_pending_idx
            ON workspace_zulip_bridge.workspace_chat_catalog_reports (
                source_activity_at DESC, source_updated_at DESC, available_at,
                external_account_uuid, zulip_stream_uuid
            )
            WHERE processing_status IN ('pending', 'failed')
        $index$;
    END IF;
END;
$upgrade$;

DO $upgrade$
BEGIN
    IF to_regclass('workspace_zulip_bridge.zulip_events') IS NOT NULL THEN
        EXECUTE $index$
            CREATE INDEX IF NOT EXISTS zulip_events_pending_queue_head_idx
            ON workspace_zulip_bridge.zulip_events (
                zulip_connection_uuid, queue_id, event_id
            )
            WHERE processing_status = 'pending'
        $index$;
    END IF;
END;
$upgrade$;
DO $upgrade$
BEGIN
    IF to_regclass('workspace_zulip_bridge.workspace_events') IS NOT NULL THEN
        EXECUTE $index$
            CREATE INDEX IF NOT EXISTS
                workspace_events_realtime_priority_pending_idx
            ON workspace_zulip_bridge.workspace_events (
                (CASE object_type
                    WHEN 'message' THEN 0
                    WHEN 'message_flag' THEN 1
                    WHEN 'message_reaction' THEN 2
                    WHEN 'user' THEN 3
                    WHEN 'stream_binding' THEN 4
                    WHEN 'topic_binding' THEN 5
                    WHEN 'stream' THEN 6
                    WHEN 'topic' THEN 7
                    ELSE 8
                END),
                sequence
            )
            WHERE processing_status = 'pending'
        $index$;
    END IF;
END;
$upgrade$;
DROP INDEX IF EXISTS workspace_zulip_bridge.workspace_events_priority_pending_idx;

DO $upgrade$
BEGIN
    IF to_regclass(
        'workspace_zulip_bridge.workspace_file_projections'
    ) IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.workspace_file_projections
            ADD COLUMN IF NOT EXISTS delivery_priority smallint
            NOT NULL DEFAULT 1;
        UPDATE workspace_zulip_bridge.workspace_file_projections
        SET processing_status = 'pending', claimed_at = NULL,
            available_at = clock_timestamp(), updated_at = clock_timestamp()
        WHERE processing_status = 'processing';
    END IF;
END;
$upgrade$;

DO $$
BEGIN
    IF to_regclass('workspace_zulip_bridge.workspace_file_projections') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.workspace_file_projections
            ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz;
    END IF;
END;
$$;

DO $$
BEGIN
    IF to_regclass('workspace_zulip_bridge.workspace_chat_catalog_reports') IS NOT NULL THEN
        -- The old cursor skipped stream-matching rows before LIMIT and never
        -- checked the parent topic. Reset it once when installing this scanner.
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'workspace_zulip_bridge'
              AND table_name = 'workspace_chat_catalog_reports'
              AND column_name = 'assignment_repair_stage'
        ) THEN
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
            SET assignment_reconciled = false, assignment_repair_created_at = NULL,
                assignment_repair_uuid = NULL
            WHERE assignment IS NOT NULL;
            UPDATE workspace_zulip_bridge.workspace_mirror_state
            SET initial_sync_completed_at = NULL
            WHERE EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.zulip_realms AS realm
                WHERE realm.workspace_provider_uuid = workspace_mirror_state.provider_uuid
            );
        END IF;
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ADD COLUMN IF NOT EXISTS assignment_repair_stage smallint NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS assignment_repair_entity_uuid uuid;
    END IF;
END;
$$;

DO $$
BEGIN
    IF to_regclass('workspace_zulip_bridge.workspace_chat_catalog_reports') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.workspace_chat_catalog_reports
            ADD COLUMN IF NOT EXISTS assignment_repair_available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            ADD COLUMN IF NOT EXISTS assignment_repair_last_error text;
    END IF;
    IF to_regclass('workspace_zulip_bridge.workspace_mirror_state') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.workspace_mirror_state
            ADD COLUMN IF NOT EXISTS initial_sync_watermark_at timestamptz;
    END IF;
END;
$$;

DO $$
BEGIN
    IF to_regclass('workspace_zulip_bridge.workspace_outbox') IS NOT NULL THEN
        ALTER TABLE workspace_zulip_bridge.workspace_outbox
            ADD COLUMN IF NOT EXISTS import_required boolean NOT NULL DEFAULT false;
    END IF;
END;
$$;
