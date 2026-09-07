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
        self._depends = ["0040-rebuild-history-batches-when-sources-change-c8d0c2.py"]

    @property
    def migration_id(self):
        return "6ead9232-aef3-4d61-8a27-602ab097de33"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        # Initial history is now stored as local batches. Stop retrying its
        # obsolete unread finalizers, without cancelling message deliveries
        # also used by realtime queue recovery or rewriting accepted results.
        session.execute(
            """
            WITH retired AS (
                UPDATE workspace_delivery_outbox
                SET submission_state = 'cancelled',
                    submission_error_code = 'legacy_history_retired'
                WHERE sent_at IS NULL
                  AND submission_state NOT IN ('sent', 'cancelled')
                  AND record->'operation'->>'kind' = 'history.finalize'
                RETURNING operation_uuid
            )
            UPDATE operation_idempotency AS operation
            SET terminal_outcome = 'rejected', updated_at = now()
            FROM retired
            WHERE operation.operation_uuid = retired.operation_uuid
              AND operation.terminal_outcome IS NULL
            """
        )

    def downgrade(self, session):
        # A rollback must not resurrect retired import operations.
        return None


migration_step = MigrationStep()
