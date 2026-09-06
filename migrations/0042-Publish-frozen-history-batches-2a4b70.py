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
        self._depends = ["0041-retire-legacy-history-finalizers-6ead92.py"]

    @property
    def migration_id(self):
        return "2a4b7011-c7ab-4028-b8d2-0722b32d9578"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute("""
            ALTER TABLE zulip_history_configuration
                ADD COLUMN IF NOT EXISTS generation bigint NOT NULL DEFAULT 1;
            UPDATE zulip_history_configuration SET fingerprint = NULL;
            ALTER TABLE zulip_history_batches
                ADD COLUMN IF NOT EXISTS import_uuid uuid,
                ADD COLUMN IF NOT EXISTS import_mapping_cursor jsonb NOT NULL DEFAULT '{}'::jsonb,
                ADD COLUMN IF NOT EXISTS import_sources_hash char(64),
                ADD COLUMN IF NOT EXISTS import_status varchar(24) NOT NULL DEFAULT 'pending',
                ADD COLUMN IF NOT EXISTS import_retry_at timestamptz NOT NULL DEFAULT now(),
                ADD COLUMN IF NOT EXISTS import_lease uuid,
                ADD COLUMN IF NOT EXISTS import_lease_until timestamptz,
                ADD COLUMN IF NOT EXISTS import_error varchar(128);
            CREATE INDEX IF NOT EXISTS zulip_history_batches_publish_idx
                ON zulip_history_batches (import_retry_at, project_uuid, provider_realm_uuid, from_id)
                WHERE import_status <> 'complete';
        """)

    def downgrade(self, session):
        # Keep captured data and generation fences across application rollback.
        return None


migration_step = MigrationStep()
