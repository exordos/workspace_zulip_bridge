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
        self._depends = ["0011-quarantine-rejected-provider-events-f1169c.py"]

    @property
    def migration_id(self):
        return "6c9ddc4c-d856-4363-8334-a8549a8909dd"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            ALTER TABLE workspace_delivery_outbox
            DROP CONSTRAINT IF EXISTS
                workspace_delivery_outbox_submission_state_check;

            ALTER TABLE workspace_delivery_outbox
            ADD CONSTRAINT workspace_delivery_outbox_submission_state_check
            CHECK (
                submission_state IN (
                    'pending', 'submitting', 'ambiguous', 'awaiting_result',
                    'rejected', 'cancelled', 'sent'
                )
            );

            ALTER TABLE zulip_provider_events
            DROP CONSTRAINT IF EXISTS
                zulip_provider_events_processing_state_check;

            ALTER TABLE zulip_provider_events
            ADD CONSTRAINT zulip_provider_events_processing_state_check
            CHECK (
                processing_state IN (
                    'pending', 'delivering', 'processed', 'unsupported',
                    'invalid', 'ignored'
                )
            );

            CREATE INDEX IF NOT EXISTS
                observed_report_outbox_catalog_readiness_idx
                ON observed_report_outbox (
                    ((body->'catalog'->>'external_account_uuid')::uuid),
                    ((body->>'observed_generation')::bigint),
                    ((body->>'resource_uuid')::uuid),
                    created_at DESC
                ) INCLUDE (result_status)
                WHERE body->>'resource_type' = 'external_chat_catalog';

            CREATE INDEX IF NOT EXISTS
                desired_resources_selected_assignment_account_idx
                ON desired_resources (
                    ((body->>'external_account_uuid')::uuid), generation
                )
                WHERE resource_type = 'external_chat_assignment'
                  AND NOT deleted
                  AND COALESCE((body->>'selected')::boolean, true);

            CREATE INDEX IF NOT EXISTS
                workspace_delivery_outbox_initial_backfill_idx
                ON workspace_delivery_outbox (
                    account_uuid, account_generation, operation_uuid
                )
                WHERE priority = 2;

            CREATE INDEX IF NOT EXISTS
                zulip_provider_events_pending_order_idx
                ON zulip_provider_events (available_at, created_at, event_id)
                INCLUDE (account_uuid, queue_id)
                WHERE processing_state = 'pending';

            CREATE INDEX IF NOT EXISTS
                workspace_delivery_outbox_provider_event_pending_idx
                ON workspace_delivery_outbox (
                    account_uuid, provider_queue_id, provider_event_id
                )
                WHERE sent_at IS NULL;

            UPDATE workspace_delivery_outbox AS delivery
            SET submission_state = 'cancelled',
                submission_error_code = 'account_inactive'
            WHERE delivery.sent_at IS NULL
              AND delivery.submission_state NOT IN (
                  'cancelled', 'rejected', 'sent'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM desired_resources AS account
                  WHERE account.resource_type = 'external_account'
                    AND account.resource_uuid = delivery.account_uuid
                    AND NOT account.deleted
                    AND COALESCE(
                        (account.body->>'synchronization_enabled')::boolean,
                        false
                    )
              );

            UPDATE zulip_provider_events AS event
            SET processing_state = 'ignored',
                processing_reason = 'account_inactive',
                prepared_records = NULL
            WHERE event.processing_state IN ('pending', 'delivering')
              AND NOT EXISTS (
                  SELECT 1 FROM desired_resources AS account
                  WHERE account.resource_type = 'external_account'
                    AND account.resource_uuid = event.account_uuid
                    AND NOT account.deleted
                    AND COALESCE(
                        (account.body->>'synchronization_enabled')::boolean,
                        false
                    )
              );

            UPDATE workspace_delivery_outbox AS delivery
            SET submission_state = 'ambiguous',
                next_submission_at = now(),
                submission_error_code = NULL
            WHERE delivery.sent_at IS NULL
              AND delivery.submission_state = 'awaiting_result'
              AND (
                  delivery.last_submitted_at IS NULL
                  OR delivery.next_submission_at
                      > delivery.last_submitted_at + interval '5 minutes'
              )
              AND EXISTS (
                  SELECT 1 FROM desired_resources AS account
                  WHERE account.resource_type = 'external_account'
                    AND account.resource_uuid = delivery.account_uuid
                    AND NOT account.deleted
                    AND COALESCE(
                        (account.body->>'synchronization_enabled')::boolean,
                        false
                    )
              );
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            UPDATE zulip_provider_events
            SET processing_state = 'unsupported'
            WHERE processing_state = 'ignored';

            UPDATE workspace_delivery_outbox
            SET submission_state = 'rejected'
            WHERE submission_state = 'cancelled';

            DROP INDEX IF EXISTS
                workspace_delivery_outbox_provider_event_pending_idx;
            DROP INDEX IF EXISTS zulip_provider_events_pending_order_idx;
            DROP INDEX IF EXISTS
                workspace_delivery_outbox_initial_backfill_idx;
            DROP INDEX IF EXISTS
                desired_resources_selected_assignment_account_idx;
            DROP INDEX IF EXISTS
                observed_report_outbox_catalog_readiness_idx;

            ALTER TABLE zulip_provider_events
            DROP CONSTRAINT IF EXISTS
                zulip_provider_events_processing_state_check;

            ALTER TABLE zulip_provider_events
            ADD CONSTRAINT zulip_provider_events_processing_state_check
            CHECK (
                processing_state IN (
                    'pending', 'delivering', 'processed', 'unsupported', 'invalid'
                )
            );

            ALTER TABLE workspace_delivery_outbox
            DROP CONSTRAINT IF EXISTS
                workspace_delivery_outbox_submission_state_check;

            ALTER TABLE workspace_delivery_outbox
            ADD CONSTRAINT workspace_delivery_outbox_submission_state_check
            CHECK (
                submission_state IN (
                    'pending', 'submitting', 'ambiguous', 'awaiting_result',
                    'rejected', 'sent'
                )
            );
            """
        )


migration_step = MigrationStep()
