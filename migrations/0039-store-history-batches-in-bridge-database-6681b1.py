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
        self._depends = ["0038-replay-history-with-final-unread-snapshot-05224a.py"]

    @property
    def migration_id(self):
        return "6681b13e-adad-4509-8547-20fbea9dd51c"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS zulip_history_batches (
                project_uuid uuid NOT NULL,
                provider_realm_uuid uuid NOT NULL,
                from_id bigint NOT NULL CHECK (
                    from_id > 0 AND (from_id - 1) % 5000 = 0
                ),
                to_id bigint NOT NULL CHECK (to_id = from_id + 4999),
                body jsonb NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (project_uuid, provider_realm_uuid, from_id)
            )
            """
        )
        # Previously completed jobs only prove Workspace delivery, not local
        # capture. Rebuild the local snapshots, respecting the saved depth.
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET next_anchor = NULL,
                state = CASE WHEN history_depth = 'new'
                             THEN 'complete' ELSE 'pending' END,
                available_at = now(), retry_count = 0,
                last_error_code = NULL, lease_until = NULL, updated_at = now()
            WHERE state <> 'cancelled'
            """
        )

    def downgrade(self, session):
        # Preserve captured message bodies when rolling the application back.
        return None


migration_step = MigrationStep()
