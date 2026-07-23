# Copyright 2016 Eugene Frolov <eugene@frolov.net.ru>
#
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from restalchemy.storage.sql import migrations


class MigrationStep(migrations.AbstractMigrationStep):
    def __init__(self):
        self._depends = ["0004-gate-selected-chat-messages-on-participants-23f11f.py"]

    @property
    def migration_id(self):
        return "7c52a1d8-aa30-4bbc-bc5a-4952b4841968"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        # Old builds could enqueue a live message update ahead of its history
        # topic/create dependencies. Rebuild only affected chat lanes from the
        # durable Zulip journal and history checkpoints. Provider operations use
        # stable UUIDs, so replay is idempotent even if an interrupted request
        # reached Workspace before its local result was recorded.
        session.execute(
            """
            CREATE TEMP TABLE zulip_message_dependency_rebuild
            ON COMMIT DROP AS
            SELECT DISTINCT
                delivery.account_uuid,
                delivery.record->'operation'->'provider'->>'chat_id' AS chat_key
            FROM workspace_delivery_outbox AS delivery
            WHERE delivery.sent_at IS NULL
              AND delivery.record->'operation'->>'kind' IN (
                  'message.create', 'message.update', 'message.delete',
                  'read_state.set'
              )
              AND delivery.record->'operation'->'provider'->>'chat_id' IS NOT NULL
            """
        )
        session.execute(
            """
            UPDATE zulip_provider_events AS event
            SET processing_state = 'pending',
                processing_reason = 'message_dependencies_rebuilt',
                available_at = now()
            WHERE EXISTS (
                SELECT 1
                FROM workspace_delivery_outbox AS delivery
                JOIN zulip_message_dependency_rebuild AS affected
                  ON affected.account_uuid = delivery.account_uuid
                 AND affected.chat_key =
                     delivery.record->'operation'->'provider'->>'chat_id'
                WHERE delivery.sent_at IS NULL
                  AND delivery.account_uuid = event.account_uuid
                  AND delivery.provider_queue_id = event.queue_id
                  AND delivery.provider_event_id = event.event_id
            )
            """
        )
        session.execute(
            """
            UPDATE provider_mappings AS mapping
            SET metadata = mapping.metadata
                    - 'content_sha256'
                    - 'provider_content_sha256',
                updated_at = now()
            WHERE mapping.entity_kind = 'message'
              AND NOT mapping.deleted
              AND EXISTS (
                  SELECT 1
                  FROM zulip_message_dependency_rebuild AS affected
                  WHERE affected.account_uuid = mapping.account_uuid
                    AND affected.chat_key = mapping.metadata->>'chat_key'
              )
            """
        )
        session.execute(
            """
            INSERT INTO zulip_queue_catchup_jobs (
                account_uuid, provider_chat_key,
                checkpoint_provider_message_id, state
            )
            SELECT
                affected.account_uuid,
                affected.chat_key,
                max(
                    CASE
                        WHEN mapping.provider_id ~ '^[0-9]+$'
                        THEN mapping.provider_id::bigint
                        ELSE NULL
                    END
                ),
                'pending'
            FROM zulip_message_dependency_rebuild AS affected
            LEFT JOIN provider_mappings AS mapping
              ON mapping.account_uuid = affected.account_uuid
             AND mapping.entity_kind = 'message'
             AND mapping.metadata->>'chat_key' = affected.chat_key
            GROUP BY affected.account_uuid, affected.chat_key
            ON CONFLICT (account_uuid, provider_chat_key) DO UPDATE SET
                checkpoint_provider_message_id =
                    EXCLUDED.checkpoint_provider_message_id,
                next_anchor = NULL,
                seen_provider_message_ids = '[]'::jsonb,
                page_count = 0,
                state = 'pending',
                safe_error_code = NULL,
                updated_at = now()
            """
        )
        session.execute(
            """
            UPDATE zulip_backfill_jobs AS job
            SET next_anchor = NULL,
                state = CASE
                    WHEN job.history_depth = 'new' THEN 'complete'
                    ELSE 'pending'
                END,
                available_at = now(),
                retry_count = 0,
                last_error_code = NULL,
                lease_until = NULL,
                updated_at = now()
            WHERE EXISTS (
                SELECT 1
                FROM zulip_message_dependency_rebuild AS affected
                WHERE affected.account_uuid = job.account_uuid
                  AND affected.chat_key = job.provider_chat_key
            )
            """
        )
        session.execute(
            """
            UPDATE provider_mappings AS mapping
            SET deleted = false, updated_at = now()
            WHERE mapping.entity_kind = 'topic'
              AND EXISTS (
                  SELECT 1
                  FROM zulip_message_dependency_rebuild AS affected
                  WHERE affected.account_uuid = mapping.account_uuid
                    AND affected.chat_key = mapping.metadata->>'chat_key'
              )
            """
        )
        affected_operations = """
            SELECT delivery.operation_uuid
            FROM workspace_delivery_outbox AS delivery
            JOIN zulip_message_dependency_rebuild AS affected
              ON affected.account_uuid = delivery.account_uuid
             AND affected.chat_key =
                 delivery.record->'operation'->'provider'->>'chat_id'
            WHERE delivery.sent_at IS NULL
        """
        session.execute(
            f"""
            DELETE FROM operation_idempotency
            WHERE terminal_outcome IS NULL
              AND operation_uuid IN ({affected_operations})
            """
        )
        session.execute(
            f"""
            DELETE FROM producer_operations
            WHERE operation_uuid IN ({affected_operations})
            """
        )
        session.execute(
            """
            DELETE FROM workspace_delivery_outbox AS delivery
            USING zulip_message_dependency_rebuild AS affected
            WHERE delivery.sent_at IS NULL
              AND affected.account_uuid = delivery.account_uuid
              AND affected.chat_key =
                  delivery.record->'operation'->'provider'->>'chat_id'
            """
        )
        session.execute("DROP TABLE zulip_message_dependency_rebuild")

    def downgrade(self, session):
        # Rebuilt delivery rows are source-derived and cannot be restored safely.
        return None


migration_step = MigrationStep()
