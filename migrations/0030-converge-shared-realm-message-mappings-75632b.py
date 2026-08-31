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
        self._depends = ["0029-replay-exact-provider-read-snapshots-72f1cf.py"]

    @property
    def migration_id(self):
        return "75632b76-9c40-4047-aaf4-fbe206b02992"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        # A previous history replay could leave a never-submitted reaction
        # behind a provisional account-local message mapping even though a
        # peer account in the same Zulip realm had already committed that
        # provider message. Remove only those immutable, unattempted records;
        # the bounded selected-chat replay below recreates them after runtime
        # realm convergence resolves the canonical Workspace message UUID.
        session.execute(
            """
            CREATE INDEX IF NOT EXISTS
                provider_mappings_message_provider_id_idx
            ON provider_mappings (provider_id, account_uuid)
            WHERE entity_kind = 'message' AND NOT deleted;

            CREATE TEMP TABLE shared_realm_pending_reaction_repair (
                record_uuid uuid PRIMARY KEY,
                operation_uuid uuid NOT NULL UNIQUE
            ) ON COMMIT DROP;

            INSERT INTO shared_realm_pending_reaction_repair (
                record_uuid, operation_uuid
            )
            SELECT delivery.record_uuid, delivery.operation_uuid
            FROM workspace_delivery_outbox AS delivery
            JOIN provider_mappings AS local_message
              ON local_message.account_uuid = delivery.account_uuid
             AND local_message.entity_kind = 'message'
             AND local_message.workspace_uuid = (
                    delivery.record->'operation'->'payload'
                        ->>'message_uuid'
                 )::uuid
             AND NOT local_message.deleted
             AND local_message.metadata->>'workspace_delivery_state'
                 IS DISTINCT FROM 'committed'
            JOIN zulip_event_cursors AS local_cursor
              ON local_cursor.account_uuid = local_message.account_uuid
             AND local_cursor.provider_realm_uuid IS NOT NULL
            WHERE delivery.sent_at IS NULL
              AND delivery.submission_state = 'pending'
              AND delivery.submission_attempts = 0
              AND delivery.provider_queue_id IS NULL
              AND delivery.provider_event_id IS NULL
              AND delivery.record->'operation'->>'kind' IN (
                    'reaction.upsert', 'reaction.delete'
              )
              AND EXISTS (
                  SELECT 1
                  FROM provider_mappings AS committed_message
                  JOIN zulip_event_cursors AS committed_cursor
                    ON committed_cursor.account_uuid =
                       committed_message.account_uuid
                   AND committed_cursor.provider_realm_uuid =
                       local_cursor.provider_realm_uuid
                  WHERE committed_message.account_uuid <>
                        local_message.account_uuid
                    AND committed_message.entity_kind = 'message'
                    AND committed_message.provider_id =
                        local_message.provider_id
                    AND NOT committed_message.deleted
                    AND committed_message.metadata
                            ->>'workspace_delivery_state' = 'committed'
                    AND committed_message.metadata->>'project_uuid' =
                        local_message.metadata->>'project_uuid'
                    AND committed_message.metadata->>'chat_key' =
                        local_message.metadata->>'chat_key'
              );

            DELETE FROM workspace_delivery_outbox AS delivery
            USING shared_realm_pending_reaction_repair AS repair
            WHERE delivery.record_uuid = repair.record_uuid
              AND delivery.sent_at IS NULL
              AND delivery.submission_state = 'pending'
              AND delivery.submission_attempts = 0;

            DELETE FROM operation_idempotency AS operation
            USING shared_realm_pending_reaction_repair AS repair
            WHERE operation.operation_uuid = repair.operation_uuid
              AND operation.terminal_outcome IS NULL;

            UPDATE zulip_backfill_jobs AS job
            SET next_anchor = NULL,
                state = 'pending',
                cutoff_at = CASE
                    WHEN job.history_depth = 'new'
                    THEN COALESCE(job.cutoff_at, assignment.updated_at)
                    ELSE job.cutoff_at
                END,
                available_at = now(),
                retry_count = 0,
                last_error_code = NULL,
                lease_until = NULL,
                updated_at = now()
            FROM desired_resources AS assignment
            JOIN desired_resources AS account
              ON account.resource_type = 'external_account'
             AND account.resource_uuid::text =
                 assignment.body->>'external_account_uuid'
             AND NOT account.deleted
             AND COALESCE(
                     (account.body->>'synchronization_enabled')::boolean,
                     false
                 )
            WHERE assignment.resource_type = 'external_chat_assignment'
              AND NOT assignment.deleted
              AND COALESCE((assignment.body->>'selected')::boolean, true)
              AND assignment.body->>'external_account_uuid' =
                  job.account_uuid::text
              AND assignment.body->'provider_chat'->>'provider_chat_key' =
                  job.provider_chat_key;
            """
        )

    def downgrade(self, session):
        # Replayed provider history and converged mapping identities cannot be
        # rolled back safely.
        return None


migration_step = MigrationStep()
