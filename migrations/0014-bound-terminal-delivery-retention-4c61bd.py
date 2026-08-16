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
        self._depends = ["0013-bound-reaction-history-window-5edf75.py"]

    @property
    def migration_id(self):
        return "4c61bde5-9241-4f58-801f-25c8865ca2a9"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            CREATE INDEX IF NOT EXISTS workspace_delivery_outbox_sent_at_idx
            ON workspace_delivery_outbox (sent_at)
            WHERE sent_at IS NOT NULL
            """
        )
        session.execute(
            """
            CREATE INDEX IF NOT EXISTS zulip_provider_events_terminal_created_idx
            ON zulip_provider_events (created_at)
            WHERE processing_state IN (
                'processed', 'unsupported', 'invalid', 'ignored'
            )
            """
        )
        session.execute(
            """
            CREATE INDEX IF NOT EXISTS zulip_provider_events_account_head_idx
            ON zulip_provider_events (
                account_uuid, created_at, event_id, queue_id
            )
            INCLUDE (available_at)
            WHERE processing_state = 'pending'
            """
        )

    def downgrade(self, session):
        session.execute("DROP INDEX IF EXISTS zulip_provider_events_account_head_idx")
        session.execute(
            "DROP INDEX IF EXISTS zulip_provider_events_terminal_created_idx"
        )
        session.execute("DROP INDEX IF EXISTS workspace_delivery_outbox_sent_at_idx")


migration_step = MigrationStep()
