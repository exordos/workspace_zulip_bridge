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
        self._depends = ["0022-isolate-provider-journal-causal-lanes-3ae83f.py"]

    @property
    def migration_id(self):
        return "dbc7365b-e23b-4ecd-9d97-2cc589b2d009"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            CREATE INDEX IF NOT EXISTS
                provider_mappings_reaction_provider_prefix_idx
            ON provider_mappings (
                account_uuid, provider_id text_pattern_ops
            )
            WHERE entity_kind = 'reaction';
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS
                provider_mappings_reaction_provider_prefix_idx;
            """
        )


migration_step = MigrationStep()
