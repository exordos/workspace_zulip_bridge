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
        self._depends = ["0020-refresh-Zulip-notification-queues-93df4e.py"]

    @property
    def migration_id(self):
        return "dcdd12e3-e417-46cb-b369-96ef93bc4458"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            CREATE INDEX IF NOT EXISTS
                observed_report_outbox_chat_materialization_idx
                ON observed_report_outbox (available_at)
                WHERE completed_at IS NULL
                  AND body->>'resource_type' = 'external_chat_catalog'
                  AND body->>'status' = 'ready'
                  AND COALESCE(
                      body->'catalog'->>'operation',
                      'upsert'
                  ) = 'upsert';
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS
                observed_report_outbox_chat_materialization_idx;
            """
        )


migration_step = MigrationStep()
