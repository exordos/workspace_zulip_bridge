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
            "0003-requeue-message-missing-topic-projection-ed8a5e.py"
        ]

    @property
    def migration_id(self):
        return "23f11f06-f592-4ebc-8fa3-1e2c176df627"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS zulip_participant_sync (
                account_uuid uuid NOT NULL,
                provider_chat_key text NOT NULL,
                assignment_generation bigint NOT NULL CHECK (
                    assignment_generation > 0
                ),
                state text NOT NULL DEFAULT 'pending' CHECK (
                    state IN ('pending', 'running', 'reported', 'ready')
                ),
                lease_until timestamptz,
                provider_user_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
                updated_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (account_uuid, provider_chat_key)
            )
            """
        )

    def downgrade(self, session):
        session.execute("DROP TABLE IF EXISTS zulip_participant_sync")


migration_step = MigrationStep()
