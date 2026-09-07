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
        self._depends = ["0046-Persist-outstanding-capture-timeout-markers-97e00d.py"]

    @property
    def migration_id(self):
        return "436b1261-533d-4b94-ae0b-b63ac996fda1"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute("SET LOCAL lock_timeout = '50ms'")
        session.execute("SET LOCAL statement_timeout = '500ms'")
        session.execute("""ALTER TABLE zulip_history_scopes
            ADD COLUMN IF NOT EXISTS directory_write_attempts integer NOT NULL DEFAULT 0""")

    def downgrade(self, session):
        # Keep retry state during an application rollback.
        return None


migration_step = MigrationStep()
