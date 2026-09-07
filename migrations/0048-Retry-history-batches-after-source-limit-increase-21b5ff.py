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
        self._depends = ["0047-Bound-directory-write-retries-436b12.py"]

    @property
    def migration_id(self):
        return "21b5ffa6-3a5b-4987-b800-84d7cf491299"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute("SET LOCAL lock_timeout = '50ms'")
        session.execute("SET LOCAL statement_timeout = '500ms'")
        session.execute("""
            UPDATE zulip_history_batches AS batch
            SET import_status='pending',import_uuid=NULL,
                import_mapping_cursor='{}'::jsonb,import_sources_hash=NULL,
                import_lease=NULL,import_lease_until=NULL,import_error=NULL,
                import_retry_at=now(),updated_at=now()
            FROM zulip_history_scopes AS scope
            WHERE batch.project_uuid=scope.project_uuid
              AND batch.provider_realm_uuid=scope.provider_realm_uuid
              AND batch.import_status='failed'
              AND batch.import_error='history_source_limit_exceeded'
              AND jsonb_array_length(scope.sources)<=512
        """)
        session.execute("""
            DELETE FROM zulip_history_failure_reports AS report
            WHERE report.safe_error_code='history_source_limit_exceeded'
              AND NOT EXISTS (
                SELECT 1 FROM zulip_history_batches AS batch
                WHERE batch.project_uuid=report.project_uuid
                  AND batch.provider_realm_uuid=report.provider_realm_uuid
                  AND batch.import_status='failed'
                  AND batch.import_error='history_source_limit_exceeded')
        """)
        session.execute("""
            DELETE FROM bridge_health AS health
            USING zulip_history_scopes AS scope
            WHERE health.component='history_publication:'
                    ||scope.project_uuid::text||':'||scope.provider_realm_uuid::text
              AND health.safe_error_code='history_source_limit_exceeded'
              AND jsonb_array_length(scope.sources)<=512
        """)

    def downgrade(self, session):
        # Recovered batches and successfully imported data remain valid.
        return None


migration_step = MigrationStep()
