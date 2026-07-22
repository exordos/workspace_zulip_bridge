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

MESSAGE_WITHOUT_TOPIC_FILTER = """
    delivery.sent_at IS NULL
    AND delivery.record->'operation'->>'kind' = 'message.create'
    AND delivery.provider_queue_id IS NOT NULL
    AND delivery.provider_event_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM workspace_delivery_outbox AS topic_delivery
        WHERE topic_delivery.account_uuid = delivery.account_uuid
          AND topic_delivery.provider_queue_id = delivery.provider_queue_id
          AND topic_delivery.provider_event_id = delivery.provider_event_id
          AND topic_delivery.record->'operation'->>'kind' = 'topic.upsert'
    )
"""


class MigrationStep(migrations.AbstractMigrationStep):
    def __init__(self):
        self._depends = ["0002-remove-legacy-message-projection-deliveries-e1636f.py"]

    @property
    def migration_id(self):
        return "ed8a5ebc-568b-4131-8f4f-c546e04fb5c0"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        # A short-lived migration removed both projection records from legacy
        # provider message batches. Requeue only durable Zulip events whose
        # unsent message no longer has its required topic upsert. Conversion
        # reuses the pending message UUID and rebuilds the correct atomic pair.
        session.execute(
            f"""
            UPDATE zulip_provider_events AS event
            SET processing_state = 'pending',
                processing_reason = 'missing_topic_projection_requeued',
                available_at = now()
            WHERE event.processing_state = 'delivering'
              AND EXISTS (
                  SELECT 1
                  FROM workspace_delivery_outbox AS delivery
                  WHERE delivery.account_uuid = event.account_uuid
                    AND delivery.provider_queue_id = event.queue_id
                    AND delivery.provider_event_id = event.event_id
                    AND {MESSAGE_WITHOUT_TOPIC_FILTER}
              )
            """
        )
        for table in ("operation_idempotency", "producer_operations"):
            session.execute(
                f"""
                DELETE FROM {table}
                WHERE operation_uuid IN (
                    SELECT delivery.operation_uuid
                    FROM workspace_delivery_outbox AS delivery
                    WHERE {MESSAGE_WITHOUT_TOPIC_FILTER}
                )
                """
            )
        session.execute(
            f"""
            DELETE FROM workspace_delivery_outbox AS delivery
            WHERE {MESSAGE_WITHOUT_TOPIC_FILTER}
            """
        )

    def downgrade(self, session):
        # The invalid pending delivery cannot be reconstructed safely; its
        # provider journal event remains available for normal conversion.
        return None


migration_step = MigrationStep()
