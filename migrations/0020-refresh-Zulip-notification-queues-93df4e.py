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
        self._depends = ["0019-fair-provider-journal-scheduling-5e3926.py"]

    @property
    def migration_id(self):
        return "93df4eb8-c837-4855-9815-a2c4036cfc6a"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        # Existing durable queues predate user_topic/user_settings capture.
        # Persist message catch-up boundaries before retiring those queues so
        # registration snapshots can converge notification settings without
        # opening a live-message gap during rollout.
        session.execute(
            """
            WITH selected_chats AS (
                SELECT
                    (assignment.body->>'external_account_uuid')::uuid
                        AS account_uuid,
                    assignment.body->'provider_chat'->>'provider_chat_key'
                        AS provider_chat_key
                FROM desired_resources AS assignment
                WHERE assignment.resource_type = 'external_chat_assignment'
                  AND NOT assignment.deleted
                  AND COALESCE(
                      (assignment.body->>'selected')::boolean, true
                  )
                UNION
                SELECT mapping.account_uuid,
                       mapping.metadata->>'chat_key'
                FROM provider_mappings AS mapping
                JOIN desired_resources AS account
                  ON account.resource_type = 'external_account'
                 AND account.resource_uuid = mapping.account_uuid
                 AND NOT account.deleted
                WHERE mapping.entity_kind = 'message'
                  AND NOT mapping.deleted
                  AND account.body->'settings'->>'selection_mode' = 'all'
                  AND mapping.metadata->>'chat_key' IS NOT NULL
            )
            INSERT INTO zulip_queue_catchup_jobs (
                account_uuid, provider_chat_key,
                checkpoint_provider_message_id, state
            )
            SELECT
                selected.account_uuid,
                selected.provider_chat_key,
                max(
                    CASE WHEN mapping.provider_id ~ '^[0-9]+$'
                         THEN mapping.provider_id::bigint ELSE NULL END
                ),
                'pending'
            FROM selected_chats AS selected
            LEFT JOIN provider_mappings AS mapping
              ON mapping.account_uuid = selected.account_uuid
             AND mapping.entity_kind = 'message'
             AND NOT mapping.deleted
             AND mapping.metadata->>'chat_key' = selected.provider_chat_key
            WHERE selected.provider_chat_key IS NOT NULL
            GROUP BY selected.account_uuid, selected.provider_chat_key
            ON CONFLICT (account_uuid, provider_chat_key) DO UPDATE SET
                checkpoint_provider_message_id =
                    EXCLUDED.checkpoint_provider_message_id,
                next_anchor = NULL,
                seen_provider_message_ids = '[]'::jsonb,
                page_count = 0,
                state = 'pending', safe_error_code = NULL,
                updated_at = now()
            """
        )
        session.execute("DELETE FROM zulip_event_cursors")

    def downgrade(self, session):
        # Retired provider queue IDs cannot be restored safely.
        return None


migration_step = MigrationStep()
