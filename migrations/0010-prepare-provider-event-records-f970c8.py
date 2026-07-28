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
        self._depends = ["0009-index-observed-reports-d6d013.py"]

    @property
    def migration_id(self):
        return "f970c8c1-27e5-4aa5-a261-a79f1b462b2e"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            ALTER TABLE zulip_provider_events
            ADD COLUMN IF NOT EXISTS prepared_records jsonb;

            ALTER TABLE zulip_provider_events
            DROP CONSTRAINT IF EXISTS
                zulip_provider_events_prepared_records_check;

            ALTER TABLE zulip_provider_events
            ADD CONSTRAINT zulip_provider_events_prepared_records_check
            CHECK (
                prepared_records IS NULL
                OR jsonb_typeof(prepared_records) = 'array'
            );
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            ALTER TABLE zulip_provider_events
            DROP CONSTRAINT IF EXISTS
                zulip_provider_events_prepared_records_check;

            ALTER TABLE zulip_provider_events
            DROP COLUMN IF EXISTS prepared_records;
            """
        )


migration_step = MigrationStep()
