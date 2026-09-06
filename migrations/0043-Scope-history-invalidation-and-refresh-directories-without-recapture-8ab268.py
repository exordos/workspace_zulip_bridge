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
        self._depends = ["0042-Publish-frozen-history-batches-2a4b70.py"]

    @property
    def migration_id(self):
        return "8ab26875-b5c7-4526-a48e-b62721b9c273"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute("""
            CREATE TABLE IF NOT EXISTS zulip_history_scopes (
                project_uuid uuid NOT NULL,
                provider_realm_uuid uuid NOT NULL,
                generation bigint NOT NULL,
                fingerprint char(64) NOT NULL,
                sources jsonb NOT NULL,
                directory_pending boolean NOT NULL DEFAULT false,
                directory_account_cursor integer NOT NULL DEFAULT 0,
                directory_cursor bigint NOT NULL DEFAULT 0,
                directory_users jsonb NOT NULL DEFAULT '[]'::jsonb,
                directory_retry_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY(project_uuid, provider_realm_uuid)
            );
            CREATE TABLE IF NOT EXISTS zulip_history_directory_revisions (
                provider_realm_uuid uuid PRIMARY KEY,
                generation bigint NOT NULL
            );
            CREATE INDEX IF NOT EXISTS zulip_history_scopes_refresh_idx
                ON zulip_history_scopes(directory_retry_at,project_uuid,provider_realm_uuid)
                WHERE directory_pending;
        """)

    def downgrade(self, session):
        session.execute("DROP TABLE zulip_history_scopes, zulip_history_directory_revisions")


migration_step = MigrationStep()
