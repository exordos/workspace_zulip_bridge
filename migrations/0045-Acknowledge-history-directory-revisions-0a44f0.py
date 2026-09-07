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
        self._depends = ["0044-Persist-history-failure-reporting-work-90da9f.py"]

    @property
    def migration_id(self):
        return "0a44f0d7-3168-4eb4-b6b4-b436bd16827b"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute("""
            ALTER TABLE zulip_history_scopes
                ADD COLUMN IF NOT EXISTS directory_revision bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS directory_ack_revision bigint NOT NULL DEFAULT 0;
            UPDATE zulip_history_scopes AS scope
            SET directory_revision=revision.generation,
                directory_ack_revision=CASE WHEN scope.directory_pending
                    THEN 0 ELSE revision.generation END
            FROM zulip_history_directory_revisions AS revision
            WHERE revision.provider_realm_uuid=scope.provider_realm_uuid;
            DELETE FROM zulip_history_directory_revisions AS revision
            WHERE EXISTS (SELECT 1 FROM zulip_history_scopes AS scope
                WHERE scope.provider_realm_uuid=revision.provider_realm_uuid)
              AND NOT EXISTS (SELECT 1 FROM zulip_history_scopes AS scope
                WHERE scope.provider_realm_uuid=revision.provider_realm_uuid
                  AND (scope.directory_pending OR
                       scope.directory_ack_revision<>revision.generation));
        """)

    def downgrade(self, session):
        session.execute("""
            ALTER TABLE zulip_history_scopes DROP COLUMN directory_revision,
                DROP COLUMN directory_ack_revision;
        """)


migration_step = MigrationStep()
