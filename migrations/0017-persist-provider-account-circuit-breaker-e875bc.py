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
        self._depends = ["0016-refresh-Zulip-reactions-by-emoji-code-e76ed0.py"]

    @property
    def migration_id(self):
        return "e875bc46-8e66-432f-8bdb-5f847896d144"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            ALTER TABLE scheduler_accounts
            ADD COLUMN IF NOT EXISTS provider_generation bigint;

            ALTER TABLE scheduler_accounts
            ADD COLUMN IF NOT EXISTS provider_state text NOT NULL DEFAULT 'ready';

            ALTER TABLE scheduler_accounts
            ADD COLUMN IF NOT EXISTS provider_retry_count integer NOT NULL DEFAULT 0;

            ALTER TABLE scheduler_accounts
            ADD COLUMN IF NOT EXISTS provider_retry_after timestamptz;

            ALTER TABLE scheduler_accounts
            ADD COLUMN IF NOT EXISTS provider_error_code text;

            ALTER TABLE scheduler_accounts
            ADD COLUMN IF NOT EXISTS provider_state_updated_at timestamptz
                NOT NULL DEFAULT now();

            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS scheduler_accounts_provider_state_check;

            ALTER TABLE scheduler_accounts
            ADD CONSTRAINT scheduler_accounts_provider_state_check CHECK (
                provider_state IN ('ready', 'backoff', 'auth_required')
            );

            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS scheduler_accounts_provider_generation_check;

            ALTER TABLE scheduler_accounts
            ADD CONSTRAINT scheduler_accounts_provider_generation_check CHECK (
                provider_generation IS NULL OR provider_generation > 0
            );

            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS scheduler_accounts_provider_retry_count_check;

            ALTER TABLE scheduler_accounts
            ADD CONSTRAINT scheduler_accounts_provider_retry_count_check CHECK (
                provider_retry_count >= 0
            );

            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS scheduler_accounts_provider_retry_state_check;

            ALTER TABLE scheduler_accounts
            ADD CONSTRAINT scheduler_accounts_provider_retry_state_check CHECK (
                (provider_state = 'backoff' AND provider_retry_after IS NOT NULL)
                OR (provider_state <> 'backoff' AND provider_retry_after IS NULL)
            );

            INSERT INTO scheduler_accounts (
                account_uuid, provider_generation, provider_state,
                provider_retry_count, provider_retry_after,
                provider_error_code, provider_state_updated_at
            )
            SELECT account.resource_uuid, account.generation, 'ready',
                   0, NULL, NULL, now()
            FROM desired_resources AS account
            WHERE account.resource_type = 'external_account'
              AND NOT account.deleted
            ON CONFLICT (account_uuid) DO UPDATE SET
                provider_generation = EXCLUDED.provider_generation,
                provider_state = 'ready',
                provider_retry_count = 0,
                provider_retry_after = NULL,
                provider_error_code = NULL,
                provider_state_updated_at = now();

            UPDATE zulip_backfill_jobs
            SET state = 'pending', lease_until = NULL, available_at = now(),
                retry_count = 0, last_error_code = NULL, updated_at = now()
            WHERE state = 'failed'
              AND last_error_code IN ('unauthorized', 'unauthorized_account');

            CREATE INDEX IF NOT EXISTS scheduler_accounts_provider_ready_idx
            ON scheduler_accounts (
                provider_state, provider_retry_after, account_uuid
            ) INCLUDE (provider_generation, provider_error_code);
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS scheduler_accounts_provider_ready_idx;

            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS scheduler_accounts_provider_retry_state_check;

            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS scheduler_accounts_provider_retry_count_check;

            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS scheduler_accounts_provider_generation_check;

            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS scheduler_accounts_provider_state_check;

            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS provider_state_updated_at;

            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS provider_error_code;

            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS provider_retry_after;

            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS provider_retry_count;

            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS provider_state;

            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS provider_generation;
            """
        )


migration_step = MigrationStep()
