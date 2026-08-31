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
        self._depends = ["0027-track-Workspace-projection-resets-a627d4.py"]

    @property
    def migration_id(self):
        return "67084490-0e44-40ff-8a53-70bc307aebd8"

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
                      AND table_name = 'zulip_event_cursors'
                      AND column_name = 'private_catalog_scanned_generation'
                ) THEN
                    ALTER TABLE zulip_event_cursors
                    ADD COLUMN private_catalog_scanned_generation bigint;
                END IF;
            END
            $migration$;

            ALTER TABLE zulip_event_cursors
            DROP CONSTRAINT IF EXISTS
                zulip_event_cursors_private_catalog_generation_check;
            ALTER TABLE zulip_event_cursors
            ADD CONSTRAINT
                zulip_event_cursors_private_catalog_generation_check
            CHECK (
                private_catalog_scanned_generation IS NULL
                OR private_catalog_scanned_generation > 0
            );
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            ALTER TABLE zulip_event_cursors
            DROP CONSTRAINT IF EXISTS
                zulip_event_cursors_private_catalog_generation_check;
            ALTER TABLE zulip_event_cursors
            DROP COLUMN IF EXISTS private_catalog_scanned_generation;
            """
        )


migration_step = MigrationStep()
