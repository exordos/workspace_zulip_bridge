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
        self._depends = ["0017-persist-provider-account-circuit-breaker-e875bc.py"]

    @property
    def migration_id(self):
        return "1c4292ab-6fef-46ef-83b4-25fa37225811"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            ALTER TABLE zulip_provider_events
            ADD COLUMN IF NOT EXISTS assignment_pending_since timestamptz;

            ALTER TABLE zulip_provider_events
            ADD COLUMN IF NOT EXISTS assignment_catalog_reported_at timestamptz;

            ALTER TABLE zulip_provider_events
            ADD COLUMN IF NOT EXISTS provider_message_context jsonb;

            ALTER TABLE zulip_provider_events
            DROP CONSTRAINT IF EXISTS
                zulip_provider_events_message_context_check;

            ALTER TABLE zulip_provider_events
            ADD CONSTRAINT zulip_provider_events_message_context_check CHECK (
                provider_message_context IS NULL
                OR jsonb_typeof(provider_message_context) = 'object'
            );
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            ALTER TABLE zulip_provider_events
            DROP CONSTRAINT IF EXISTS
                zulip_provider_events_message_context_check;

            ALTER TABLE zulip_provider_events
            DROP COLUMN IF EXISTS provider_message_context;

            ALTER TABLE zulip_provider_events
            DROP COLUMN IF EXISTS assignment_catalog_reported_at;

            ALTER TABLE zulip_provider_events
            DROP COLUMN IF EXISTS assignment_pending_since;
            """
        )


migration_step = MigrationStep()
