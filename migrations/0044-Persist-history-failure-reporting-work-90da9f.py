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
            "0043-Scope-history-invalidation-and-refresh-directories-without-recapture-8ab268.py"
        ]

    @property
    def migration_id(self):
        return "90da9f54-4dd1-40ff-a3c3-39e668d5954c"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute("""
            CREATE TABLE IF NOT EXISTS zulip_history_failure_reports (
                project_uuid uuid NOT NULL,
                provider_realm_uuid uuid NOT NULL,
                generation bigint NOT NULL,
                safe_error_code varchar(128) NOT NULL,
                after_account uuid,
                complete boolean NOT NULL DEFAULT false,
                retry_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY(project_uuid, provider_realm_uuid),
                FOREIGN KEY(project_uuid, provider_realm_uuid)
                    REFERENCES zulip_history_scopes ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS zulip_history_failure_reports_pending_idx
                ON zulip_history_failure_reports(retry_at,project_uuid,provider_realm_uuid)
                WHERE NOT complete;
            INSERT INTO zulip_history_failure_reports
                (project_uuid,provider_realm_uuid,generation,safe_error_code)
            SELECT scope.project_uuid,scope.provider_realm_uuid,scope.generation,
                   COALESCE(failure.import_error,'history_import_failed')
            FROM zulip_history_scopes AS scope
            JOIN LATERAL (
                SELECT import_error FROM zulip_history_batches AS batch
                WHERE batch.project_uuid=scope.project_uuid
                  AND batch.provider_realm_uuid=scope.provider_realm_uuid
                  AND batch.import_status='failed'
                ORDER BY batch.from_id LIMIT 1
            ) AS failure ON true
            WHERE NOT scope.directory_pending
            ON CONFLICT DO NOTHING;
        """)

    def downgrade(self, session):
        session.execute("DROP TABLE zulip_history_failure_reports")


migration_step = MigrationStep()
