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


def _execute_concurrently(session, statement):
    """Build or drop an index without blocking the live bridge writer."""
    if not hasattr(session, "commit"):
        session.execute(statement)
        return
    session.commit()
    connection = session._conn
    connection.autocommit = True
    try:
        session.execute(statement)
    finally:
        connection.autocommit = False


class MigrationStep(migrations.AbstractMigrationStep):
    def __init__(self):
        self._depends = [
            "0048-Retry-history-batches-after-source-limit-increase-21b5ff.py"
        ]

    @property
    def migration_id(self):
        return "d968ea3c-3e77-4ad5-a0a6-524db5865c47"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        index_name = "bridge_operations_active_provider_lease_idx"
        _execute_concurrently(
            session,
            f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}",
        )
        _execute_concurrently(
            session,
            f"""
            CREATE INDEX CONCURRENTLY {index_name}
            ON bridge_operations (expires_at, record_uuid)
            WHERE result_sent_at IS NULL
              AND record #>> '{{transport,lease_uuid}}' IS NOT NULL
            """,
        )

    def downgrade(self, session):
        _execute_concurrently(
            session,
            """
            DROP INDEX CONCURRENTLY IF EXISTS
                bridge_operations_active_provider_lease_idx
            """,
        )


migration_step = MigrationStep()
