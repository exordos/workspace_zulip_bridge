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
        self._depends = ["0007-persist-Zulip-provider-identity-c721d9.py"]

    @property
    def migration_id(self):
        return "c511aaf6-5b6d-4dd0-93b7-80fc23e5192b"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        # Existing durable queues were registered before reaction events were
        # requested. Re-register them and replay the configured history window
        # so reaction snapshots converge without requiring account relinking.
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET next_anchor = NULL,
                state = CASE
                    WHEN history_depth = 'new' THEN 'complete'
                    ELSE 'pending'
                END,
                available_at = now(),
                retry_count = 0,
                last_error_code = NULL,
                lease_until = NULL,
                updated_at = now()
            """
        )
        session.execute("DELETE FROM zulip_event_cursors")

    def downgrade(self, session):
        # Retired provider queue IDs cannot be restored safely.
        return None


migration_step = MigrationStep()
