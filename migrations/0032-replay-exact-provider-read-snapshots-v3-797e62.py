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
        self._depends = ["0031-replay-versioned-provider-read-snapshots-724065.py"]

    @property
    def migration_id(self):
        return "797e6238-ce4e-4699-bf55-148a9062b0a6"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        # Version 3 intentionally changes every history operation identity.
        # Earlier version-2 jobs may already be terminal after a hotpatched
        # bridge ran against a backend that still forced authored provider
        # messages read. Replaying every selected chat publishes a fresh exact
        # snapshot for every enabled account after that backend invariant is
        # removed.
        session.execute(
            """
            UPDATE zulip_backfill_jobs AS job
            SET next_anchor = NULL,
                state = 'pending',
                cutoff_at = CASE
                    WHEN job.history_depth = 'new'
                    THEN COALESCE(job.cutoff_at, assignment.updated_at)
                    ELSE job.cutoff_at
                END,
                available_at = now(),
                retry_count = 0,
                last_error_code = NULL,
                lease_until = NULL,
                updated_at = now()
            FROM desired_resources AS assignment
            JOIN desired_resources AS account
              ON account.resource_type = 'external_account'
             AND account.resource_uuid::text =
                 assignment.body->>'external_account_uuid'
             AND NOT account.deleted
             AND COALESCE(
                     (account.body->>'synchronization_enabled')::boolean,
                     false
                 )
            WHERE assignment.resource_type = 'external_chat_assignment'
              AND NOT assignment.deleted
              AND COALESCE((assignment.body->>'selected')::boolean, true)
              AND assignment.body->>'external_account_uuid' =
                  job.account_uuid::text
              AND assignment.body->'provider_chat'->>'provider_chat_key' =
                  job.provider_chat_key;
            """
        )

    def downgrade(self, session):
        # Replayed provider history cannot be rolled back safely.
        return None


migration_step = MigrationStep()
