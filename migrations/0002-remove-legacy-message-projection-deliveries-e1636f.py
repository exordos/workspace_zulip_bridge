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

LEGACY_MESSAGE_STREAM_PROJECTION_FILTER = """
    delivery.sent_at IS NULL
    AND delivery.record->'operation'->>'kind' = 'stream.upsert'
    AND delivery.provider_queue_id IS NOT NULL
    AND delivery.provider_event_id IS NOT NULL
    AND EXISTS (
        SELECT 1
        FROM workspace_delivery_outbox AS message_delivery
        WHERE message_delivery.account_uuid = delivery.account_uuid
          AND message_delivery.provider_queue_id = delivery.provider_queue_id
          AND message_delivery.provider_event_id = delivery.provider_event_id
          AND message_delivery.record->'operation'->>'kind' = 'message.create'
    )
"""


class MigrationStep(migrations.AbstractMigrationStep):
    def __init__(self):
        self._depends = ["0001-add-Zulip-provider-scheduler-state-143113.py"]

    @property
    def migration_id(self):
        return "e1636fff-9672-4ced-af46-cecbe75eff5c"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        # Older bridge releases bundled a stream projection upsert with every
        # provider message. Control owns stream materialization, so these
        # unsent records can never be accepted by the Provider API. Remove only
        # stream records paired with a message.create from the same durable
        # Zulip event; actual rename events are not matched.
        for table in ("operation_idempotency", "producer_operations"):
            session.execute(
                f"""
                DELETE FROM {table}
                WHERE operation_uuid IN (
                    SELECT delivery.operation_uuid
                    FROM workspace_delivery_outbox AS delivery
                    WHERE {LEGACY_MESSAGE_STREAM_PROJECTION_FILTER}
                )
                """
            )
        session.execute(
            f"""
            DELETE FROM workspace_delivery_outbox AS delivery
            WHERE {LEGACY_MESSAGE_STREAM_PROJECTION_FILTER}
            """
        )

    def downgrade(self, session):
        # The removed records represented invalid Provider API requests and
        # cannot be reconstructed safely.
        return None


migration_step = MigrationStep()
