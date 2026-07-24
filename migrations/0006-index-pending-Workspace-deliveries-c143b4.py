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
        self._depends = ["0005-rebuild-message-topic-dependencies-7c52a1.py"]

    @property
    def migration_id(self):
        return "c143b421-08b4-4336-9aed-58a1ebddce75"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            CREATE INDEX IF NOT EXISTS
                workspace_delivery_outbox_pending_order_idx
                ON workspace_delivery_outbox (priority, created_at)
                WHERE sent_at IS NULL;
            CREATE INDEX IF NOT EXISTS
                workspace_delivery_outbox_pending_dependency_idx
                ON workspace_delivery_outbox (
                    account_uuid,
                    assignment_uuid,
                    assignment_generation,
                    assignment_project_uuid,
                    ((record->'operation'->>'kind')),
                    ((record->'operation'->>'entity_uuid'))
                )
                WHERE sent_at IS NULL;
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS
                workspace_delivery_outbox_pending_dependency_idx;
            DROP INDEX IF EXISTS workspace_delivery_outbox_pending_order_idx;
            """
        )


migration_step = MigrationStep()
