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
        self._depends = ["0006-index-pending-Workspace-deliveries-c143b4.py"]

    @property
    def migration_id(self):
        return "c721d997-02e6-4713-bd3d-9d715cca231f"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            ALTER TABLE zulip_event_cursors
                ADD COLUMN IF NOT EXISTS provider_realm_uuid UUID,
                ADD COLUMN IF NOT EXISTS provider_owner_user_id TEXT,
                ADD COLUMN IF NOT EXISTS provider_account_generation BIGINT;

            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname =
                        'zulip_event_cursors_provider_identity_pair_check'
                      AND conrelid = 'zulip_event_cursors'::regclass
                ) THEN
                    ALTER TABLE zulip_event_cursors
                        ADD CONSTRAINT
                            zulip_event_cursors_provider_identity_pair_check
                        CHECK (
                            (provider_realm_uuid IS NULL) =
                            (provider_owner_user_id IS NULL)
                            AND (provider_realm_uuid IS NULL) =
                                (provider_account_generation IS NULL)
                            AND (
                                provider_account_generation IS NULL
                                OR provider_account_generation > 0
                            )
                        );
                END IF;
            END
            $$;
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            ALTER TABLE zulip_event_cursors
                DROP CONSTRAINT IF EXISTS
                    zulip_event_cursors_provider_identity_pair_check,
                DROP COLUMN IF EXISTS provider_account_generation,
                DROP COLUMN IF EXISTS provider_owner_user_id,
                DROP COLUMN IF EXISTS provider_realm_uuid;
            """
        )


migration_step = MigrationStep()
