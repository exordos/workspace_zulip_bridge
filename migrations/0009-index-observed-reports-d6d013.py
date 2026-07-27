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
        self._depends = ["0008-refresh-Zulip-reaction-queues-c511aa.py"]

    @property
    def migration_id(self):
        return "d6d0134c-40dc-4926-9229-6be215404846"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            CREATE INDEX IF NOT EXISTS
                observed_report_outbox_resource_latest_idx
                ON observed_report_outbox (
                    ((body->>'resource_type')),
                    ((body->>'resource_uuid')),
                    created_at DESC
                );
            CREATE INDEX IF NOT EXISTS
                observed_report_outbox_pending_order_idx
                ON observed_report_outbox (created_at)
                WHERE completed_at IS NULL;
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS
                observed_report_outbox_pending_order_idx;
            DROP INDEX IF EXISTS
                observed_report_outbox_resource_latest_idx;
            """
        )


migration_step = MigrationStep()
