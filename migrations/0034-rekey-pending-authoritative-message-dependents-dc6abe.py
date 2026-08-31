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

from workspace_zulip_bridge import storage


class MigrationStep(migrations.AbstractMigrationStep):
    def __init__(self):
        self._depends = ["0033-replay-versioned-provider-snapshots-v4-d87fa7.py"]

    @property
    def migration_id(self):
        return "dc6abee1-ece3-4e4d-ab6d-c8f618c807f4"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        rows = session.execute(
            """
            SELECT DISTINCT delivery.account_uuid,
                   alias.workspace_uuid AS source_uuid,
                   mapping.workspace_uuid AS target_uuid
            FROM workspace_delivery_outbox AS delivery
            JOIN operation_idempotency AS idempotency
              ON idempotency.operation_uuid = delivery.operation_uuid
             AND idempotency.terminal_outcome IS NULL
            JOIN provider_mapping_aliases AS alias
              ON alias.account_uuid = delivery.account_uuid
             AND alias.entity_kind = 'message'
             AND NOT alias.deleted
             AND alias.metadata->>'workspace_delivery_state' = 'committed'
             AND (
                 (
                     delivery.record->'operation'->>'kind' IN (
                         'message.update', 'message.delete'
                     )
                     AND delivery.record->'operation'->>'entity_uuid' =
                         alias.workspace_uuid::text
                 )
                 OR delivery.record->'operation'->'payload'
                        ->>'message_uuid' = alias.workspace_uuid::text
                 OR delivery.record->'operation'->'payload'
                        ->>'reply_to_message_uuid' = alias.workspace_uuid::text
                 OR delivery.record->'operation'->'payload'
                        ->'message_uuids' ? alias.workspace_uuid::text
             )
            JOIN provider_mappings AS mapping
              ON mapping.account_uuid = alias.account_uuid
             AND mapping.entity_kind = 'message'
             AND mapping.provider_id = alias.provider_id
             AND mapping.workspace_uuid <> alias.workspace_uuid
             AND NOT mapping.deleted
             AND mapping.metadata->>'workspace_delivery_state' = 'committed'
            WHERE delivery.sent_at IS NULL
              AND delivery.submission_state = 'pending'
              AND delivery.submission_attempts = 0
            ORDER BY delivery.account_uuid, alias.workspace_uuid,
                     mapping.workspace_uuid
            """
        ).fetchall()
        for row in rows:
            storage.RestAlchemyStore._rekey_pending_workspace_message_dependents(
                session,
                str(row["account_uuid"]),
                str(row["source_uuid"]),
                str(row["target_uuid"]),
            )

    def downgrade(self, session):
        # Once a follow-up has been rebound to Workspace's canonical message,
        # routing it back to a provisional UUID would be unsafe.
        return None


migration_step = MigrationStep()
