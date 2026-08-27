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


def _rebuild_index_concurrently(session, index_name, statement):
    """Replace invalid remnants left by an interrupted concurrent build."""
    _execute_concurrently(
        session,
        f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}",
    )
    _execute_concurrently(session, statement)


class MigrationStep(migrations.AbstractMigrationStep):
    def __init__(self):
        self._depends = ["0024-index-observed-report-semantic-order-6ecddb.py"]

    @property
    def migration_id(self):
        return "3cd8cf8a-6a67-4664-8a96-597a52447382"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        _rebuild_index_concurrently(
            session,
            "bridge_operations_result_record_uuid_idx",
            """
            CREATE INDEX CONCURRENTLY
                bridge_operations_result_record_uuid_idx
            ON bridge_operations (
                ((result_record->>'record_uuid')::uuid)
            )
            WHERE result_record IS NOT NULL
            """,
        )
        _rebuild_index_concurrently(
            session,
            "bridge_operations_pending_result_idx",
            """
            CREATE INDEX CONCURRENTLY
                bridge_operations_pending_result_idx
            ON bridge_operations (updated_at, record_uuid)
            WHERE result_record IS NOT NULL AND result_sent_at IS NULL
            """,
        )
        _rebuild_index_concurrently(
            session,
            "bridge_operations_terminal_read_retention_idx",
            """
            CREATE INDEX CONCURRENTLY
                bridge_operations_terminal_read_retention_idx
            ON bridge_operations (updated_at, record_uuid)
            WHERE state = 'committed'
              AND result_sent_at IS NOT NULL
              AND NOT manual_reconciliation_required
              AND last_error_code IS DISTINCT FROM
                  'provider_result_stale_lease'
              AND record->'operation'->>'kind' = 'read_state.set'
            """,
        )

    def downgrade(self, session):
        for index_name in (
            "bridge_operations_terminal_read_retention_idx",
            "bridge_operations_pending_result_idx",
            "bridge_operations_result_record_uuid_idx",
        ):
            _execute_concurrently(
                session,
                f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}",
            )


migration_step = MigrationStep()
