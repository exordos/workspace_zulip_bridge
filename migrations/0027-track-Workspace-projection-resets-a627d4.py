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
        self._depends = [
            "0026-compare-observed-report-timestamps-chronologically-00f58f.py"
        ]

    @property
    def migration_id(self):
        return "a627d460-7ce3-42a1-bdf1-bfc75454e07d"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            DO $migration$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'scheduler_accounts'
                      AND column_name = 'projection_reset_generation'
                ) THEN
                    ALTER TABLE scheduler_accounts
                    ADD COLUMN projection_reset_generation bigint
                        NOT NULL DEFAULT 0;

                    -- An older Bridge may already have persisted the Workspace
                    -- account resource and advanced its cursor while ignoring
                    -- the new reset field.  Force exactly one authoritative
                    -- snapshot after this schema upgrade so the new runtime
                    -- observes and applies every retained reset generation.
                    UPDATE bridge_metadata
                    SET control_cursor = '', blocked_batch = NULL,
                        updated_at = now()
                    WHERE singleton;
                END IF;
            END
            $migration$;

            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS
                scheduler_accounts_projection_reset_generation_check;
            ALTER TABLE scheduler_accounts
            ADD CONSTRAINT scheduler_accounts_projection_reset_generation_check
                CHECK (projection_reset_generation >= 0);
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            ALTER TABLE scheduler_accounts
            DROP CONSTRAINT IF EXISTS
                scheduler_accounts_projection_reset_generation_check;
            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS projection_reset_generation;
            """
        )


migration_step = MigrationStep()
