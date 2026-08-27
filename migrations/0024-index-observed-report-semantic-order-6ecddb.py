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
        self._depends = ["0023-index-reaction-provider-prefixes-dbc736.py"]

    @property
    def migration_id(self):
        return "6ecddb49-38db-43cc-9b05-a90aa48f38e8"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS
                observed_report_outbox_resource_latest_idx;
            DROP INDEX IF EXISTS
                observed_report_outbox_catalog_readiness_idx;

            CREATE INDEX IF NOT EXISTS
                observed_report_outbox_resource_observed_idx
            ON observed_report_outbox (
                (body->>'resource_type'),
                ((body->>'resource_uuid')::uuid),
                ((body->>'observed_generation')::bigint) DESC,
                (body->>'observed_at') DESC NULLS LAST,
                created_at DESC,
                report_uuid DESC
            );

            CREATE INDEX IF NOT EXISTS
                observed_report_outbox_catalog_readiness_idx
            ON observed_report_outbox (
                ((body->'catalog'->>'external_account_uuid')::uuid),
                ((body->>'resource_uuid')::uuid),
                ((body->>'observed_generation')::bigint) DESC,
                (body->>'observed_at') DESC NULLS LAST,
                created_at DESC,
                report_uuid DESC
            ) INCLUDE (result_status)
            WHERE body->>'resource_type' = 'external_chat_catalog';

            CREATE INDEX IF NOT EXISTS
                observed_report_outbox_terminal_history_idx
            ON observed_report_outbox (completed_at, report_uuid)
            WHERE completed_at IS NOT NULL;

            CREATE TABLE IF NOT EXISTS observed_report_prune_state (
                singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                last_completed_at timestamptz,
                last_report_uuid uuid,
                CHECK (
                    (last_completed_at IS NULL) =
                    (last_report_uuid IS NULL)
                )
            );

            INSERT INTO observed_report_prune_state (singleton)
            VALUES (true)
            ON CONFLICT (singleton) DO NOTHING;
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS
                observed_report_outbox_terminal_history_idx;
            DROP INDEX IF EXISTS
                observed_report_outbox_catalog_readiness_idx;
            DROP INDEX IF EXISTS
                observed_report_outbox_resource_observed_idx;
            DROP TABLE IF EXISTS observed_report_prune_state;

            CREATE INDEX IF NOT EXISTS
                observed_report_outbox_resource_latest_idx
            ON observed_report_outbox (
                ((body->>'resource_type')),
                ((body->>'resource_uuid')),
                created_at DESC
            );

            CREATE INDEX IF NOT EXISTS
                observed_report_outbox_catalog_readiness_idx
            ON observed_report_outbox (
                ((body->'catalog'->>'external_account_uuid')::uuid),
                ((body->>'observed_generation')::bigint),
                ((body->>'resource_uuid')::uuid),
                created_at DESC
            ) INCLUDE (result_status)
            WHERE body->>'resource_type' = 'external_chat_catalog';
            """
        )


migration_step = MigrationStep()
