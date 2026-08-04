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
        self._depends = [
            "0012-optimize-bridge-load-and-reconcile-stale-queues-6c9ddc.py"
        ]

    @property
    def migration_id(self):
        return "5edf7500-949a-4109-a452-e140b9d44845"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET cutoff_at = updated_at
            WHERE history_depth = 'new' AND cutoff_at IS NULL;

            CREATE INDEX IF NOT EXISTS
                zulip_provider_message_events_inflight_idx
                ON zulip_provider_events (
                    account_uuid, ((body->'message'->>'id'))
                )
                WHERE event_type = 'message'
                  AND processing_state IN ('pending', 'delivering');

            CREATE INDEX IF NOT EXISTS
                zulip_provider_message_events_local_echo_idx
                ON zulip_provider_events (
                    account_uuid, ((body->'message'->>'id')), queue_id,
                    ((body->>'local_message_id'))
                )
                WHERE event_type = 'message'
                  AND body ? 'local_message_id';

            CREATE INDEX IF NOT EXISTS
                bridge_operations_active_local_echo_idx
                ON bridge_operations (
                    account_uuid, provider_queue_id, provider_local_id
                )
                WHERE state IN ('pending', 'running', 'uncertain')
                  AND provider_local_id IS NOT NULL
                  AND record->'operation'->>'kind' = 'message.create';
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS bridge_operations_active_local_echo_idx;
            DROP INDEX IF EXISTS zulip_provider_message_events_local_echo_idx;
            DROP INDEX IF EXISTS zulip_provider_message_events_inflight_idx;

            UPDATE zulip_backfill_jobs
            SET cutoff_at = NULL
            WHERE history_depth = 'new';
            """
        )


migration_step = MigrationStep()
