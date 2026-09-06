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
        self._depends = ["0039-store-history-batches-in-bridge-database-6681b1.py"]

    @property
    def migration_id(self):
        return "c8d0c2ad-817b-4b44-a34a-2ed788a15f9b"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """CREATE TABLE IF NOT EXISTS zulip_history_configuration (
                singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                fingerprint text
            )"""
        )
        session.execute(
            """INSERT INTO zulip_history_configuration (singleton) VALUES (true)
               ON CONFLICT (singleton) DO NOTHING"""
        )

    def downgrade(self, session):
        return None


migration_step = MigrationStep()
