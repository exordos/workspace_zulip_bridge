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
        self._depends = ["0045-Acknowledge-history-directory-revisions-0a44f0.py"]

    @property
    def migration_id(self):
        return "97e00d12-7b33-41b4-b768-8eb79c29d740"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute("""
            DO $$ BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_attribute
                  WHERE attrelid='zulip_backfill_jobs'::regclass
                    AND attname='capture_timeout_pending' AND NOT attisdropped) THEN
                ALTER TABLE zulip_backfill_jobs
                  ADD COLUMN capture_timeout_pending boolean NOT NULL DEFAULT false;
                UPDATE zulip_backfill_jobs AS job SET capture_timeout_pending=true
                WHERE job.state NOT IN ('complete','cancelled') AND (
                  job.last_error_code='history_capture_write_timeout'
                  OR EXISTS (SELECT 1 FROM bridge_health AS health
                    WHERE health.component='provider:'||job.account_uuid::text||':'||job.provider_chat_key
                      AND health.safe_error_code='history_capture_write_timeout')
                  OR EXISTS (
                    SELECT 1 FROM zulip_history_scopes AS scope
                    JOIN zulip_history_failure_reports AS report USING(project_uuid,provider_realm_uuid)
                    CROSS JOIN LATERAL jsonb_to_recordset(scope.sources)
                      AS source(account_uuid uuid,provider_chat_key text)
                    WHERE report.generation=scope.generation
                      AND report.safe_error_code='history_capture_write_timeout'
                      AND source.account_uuid=job.account_uuid
                      AND source.provider_chat_key=job.provider_chat_key));
                INSERT INTO zulip_history_failure_reports AS report
                  (project_uuid,provider_realm_uuid,generation,safe_error_code)
                SELECT scope.project_uuid,scope.provider_realm_uuid,scope.generation,
                  'history_capture_write_timeout'
                FROM zulip_history_scopes AS scope
                WHERE EXISTS (
                  SELECT 1 FROM jsonb_to_recordset(scope.sources)
                    AS source(account_uuid uuid,provider_chat_key text)
                  JOIN zulip_backfill_jobs AS job ON job.account_uuid=source.account_uuid
                    AND job.provider_chat_key=source.provider_chat_key
                  WHERE job.capture_timeout_pending)
                ON CONFLICT(project_uuid,provider_realm_uuid) DO UPDATE SET
                  generation=EXCLUDED.generation,safe_error_code=EXCLUDED.safe_error_code,
                  after_account=CASE WHEN report.safe_error_code='history_capture_write_timeout'
                    THEN report.after_account END,
                  complete=CASE WHEN report.safe_error_code='history_capture_write_timeout'
                    THEN report.complete ELSE false END,
                  retry_at=CASE WHEN report.safe_error_code='history_capture_write_timeout'
                    THEN report.retry_at ELSE now() END
                WHERE report.generation<>EXCLUDED.generation;
              END IF;
            END $$;
        """)

    def downgrade(self, session):
        # Keep the marker when rolling back application code.
        return None


migration_step = MigrationStep()
