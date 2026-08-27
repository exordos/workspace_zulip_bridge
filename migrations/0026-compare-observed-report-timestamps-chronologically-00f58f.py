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

RESOURCE_INDEX = "observed_report_outbox_resource_observed_idx"
CATALOG_INDEX = "observed_report_outbox_catalog_readiness_idx"


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
        self._depends = ["0025-bound-provider-result-delivery-state-3cd8cf.py"]

    @property
    def migration_id(self):
        return "00f58f7d-d7d7-40aa-a757-a8644e1aac94"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            CREATE OR REPLACE FUNCTION
                workspace_bridge_observed_at(value text)
            RETURNS timestamptz
            LANGUAGE sql
            IMMUTABLE
            PARALLEL SAFE
            RETURNS NULL ON NULL INPUT
            AS $$ SELECT value::timestamptz $$;
            """
        )
        _rebuild_index_concurrently(
            session,
            "observed_report_outbox_resource_chronological_idx",
            """
            CREATE INDEX CONCURRENTLY
                observed_report_outbox_resource_chronological_idx
            ON observed_report_outbox (
                (body->>'resource_type'),
                ((body->>'resource_uuid')::uuid),
                ((body->>'observed_generation')::bigint) DESC,
                COALESCE(
                    workspace_bridge_observed_at(body->>'observed_at'),
                    created_at
                ) DESC,
                created_at DESC,
                report_uuid DESC
            )
            """,
        )
        _rebuild_index_concurrently(
            session,
            "observed_report_outbox_catalog_chronological_idx",
            """
            CREATE INDEX CONCURRENTLY
                observed_report_outbox_catalog_chronological_idx
            ON observed_report_outbox (
                ((body->'catalog'->>'external_account_uuid')::uuid),
                ((body->>'resource_uuid')::uuid),
                ((body->>'observed_generation')::bigint) DESC,
                COALESCE(
                    workspace_bridge_observed_at(body->>'observed_at'),
                    created_at
                ) DESC,
                created_at DESC,
                report_uuid DESC
            ) INCLUDE (result_status)
            WHERE body->>'resource_type' = 'external_chat_catalog'
            """,
        )
        for index_name in (RESOURCE_INDEX, CATALOG_INDEX):
            _execute_concurrently(
                session,
                f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}",
            )
        session.execute(
            """
            ALTER INDEX observed_report_outbox_resource_chronological_idx
            RENAME TO observed_report_outbox_resource_observed_idx;
            ALTER INDEX observed_report_outbox_catalog_chronological_idx
            RENAME TO observed_report_outbox_catalog_readiness_idx;
            """
        )

    def downgrade(self, session):
        _rebuild_index_concurrently(
            session,
            "observed_report_outbox_resource_text_idx",
            """
            CREATE INDEX CONCURRENTLY
                observed_report_outbox_resource_text_idx
            ON observed_report_outbox (
                (body->>'resource_type'),
                ((body->>'resource_uuid')::uuid),
                ((body->>'observed_generation')::bigint) DESC,
                (body->>'observed_at') DESC NULLS LAST,
                created_at DESC,
                report_uuid DESC
            )
            """,
        )
        _rebuild_index_concurrently(
            session,
            "observed_report_outbox_catalog_text_idx",
            """
            CREATE INDEX CONCURRENTLY
                observed_report_outbox_catalog_text_idx
            ON observed_report_outbox (
                ((body->'catalog'->>'external_account_uuid')::uuid),
                ((body->>'resource_uuid')::uuid),
                ((body->>'observed_generation')::bigint) DESC,
                (body->>'observed_at') DESC NULLS LAST,
                created_at DESC,
                report_uuid DESC
            ) INCLUDE (result_status)
            WHERE body->>'resource_type' = 'external_chat_catalog'
            """,
        )
        for index_name in (CATALOG_INDEX, RESOURCE_INDEX):
            _execute_concurrently(
                session,
                f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}",
            )
        session.execute(
            """
            ALTER INDEX observed_report_outbox_resource_text_idx
            RENAME TO observed_report_outbox_resource_observed_idx;
            ALTER INDEX observed_report_outbox_catalog_text_idx
            RENAME TO observed_report_outbox_catalog_readiness_idx;
            DROP FUNCTION IF EXISTS workspace_bridge_observed_at(text);
            """
        )


migration_step = MigrationStep()
