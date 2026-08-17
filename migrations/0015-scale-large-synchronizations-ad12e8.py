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
        self._depends = ["0014-bound-terminal-delivery-retention-4c61bd.py"]

    @property
    def migration_id(self):
        return "ad12e8f0-bac9-4313-8329-3b111f7c5682"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            ALTER TABLE scheduler_accounts
            ADD COLUMN IF NOT EXISTS last_participant_sync_at timestamptz;

            ALTER TABLE scheduler_accounts
            ADD COLUMN IF NOT EXISTS last_backfill_at timestamptz;

            CREATE INDEX IF NOT EXISTS
                desired_resources_assignment_chat_idx
                ON desired_resources (
                    (body->>'external_account_uuid'),
                    (body->'provider_chat'->>'provider_chat_key')
                ) INCLUDE (generation)
                WHERE resource_type = 'external_chat_assignment'
                  AND NOT deleted;

            CREATE INDEX IF NOT EXISTS
                zulip_participant_sync_account_claim_idx
                ON zulip_participant_sync (
                    account_uuid, updated_at, provider_chat_key
                ) INCLUDE (assignment_generation, state, lease_until);

            CREATE INDEX IF NOT EXISTS
                zulip_backfill_jobs_account_claim_idx
                ON zulip_backfill_jobs (
                    account_uuid, updated_at, provider_chat_key
                ) INCLUDE (
                    state, available_at, lease_until, history_depth,
                    next_anchor, cutoff_at, retry_count
                );

            INSERT INTO scheduler_accounts (account_uuid)
            SELECT account_uuid FROM zulip_participant_sync
            UNION
            SELECT account_uuid FROM zulip_backfill_jobs
            ON CONFLICT (account_uuid) DO NOTHING;
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS zulip_backfill_jobs_account_claim_idx;
            DROP INDEX IF EXISTS zulip_participant_sync_account_claim_idx;
            DROP INDEX IF EXISTS desired_resources_assignment_chat_idx;

            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS last_backfill_at;

            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS last_participant_sync_at;
            """
        )


migration_step = MigrationStep()
