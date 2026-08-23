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
        self._depends = ["0018-persist-reaction-assignment-context-372258.py"]

    @property
    def migration_id(self):
        return "5e3926d5-7c57-486b-933a-5379a7075a7f"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            ALTER TABLE scheduler_accounts
            ADD COLUMN IF NOT EXISTS
                last_provider_event_dispatched_at timestamptz;

            INSERT INTO scheduler_accounts (account_uuid)
            SELECT DISTINCT account_uuid FROM zulip_provider_events
            ON CONFLICT (account_uuid) DO NOTHING;

            DROP INDEX IF EXISTS
                zulip_provider_events_account_head_idx;

            CREATE INDEX
                zulip_provider_events_account_head_idx
            ON zulip_provider_events (
                account_uuid, created_at, event_id, queue_id
            ) INCLUDE (processing_state, available_at)
            WHERE processing_state IN ('pending', 'delivering');
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS
                zulip_provider_events_account_head_idx;

            CREATE INDEX
                zulip_provider_events_account_head_idx
            ON zulip_provider_events (
                account_uuid, created_at, event_id, queue_id
            ) INCLUDE (available_at)
            WHERE processing_state = 'pending';

            ALTER TABLE scheduler_accounts
            DROP COLUMN IF EXISTS last_provider_event_dispatched_at;
            """
        )


migration_step = MigrationStep()
