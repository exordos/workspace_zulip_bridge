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
        self._depends = ["0010-prepare-provider-event-records-f970c8.py"]

    @property
    def migration_id(self):
        return "f1169c76-c1ea-43da-b0cc-96c5121af7fc"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            ALTER TABLE workspace_delivery_outbox
            ADD COLUMN IF NOT EXISTS submission_error_code text;

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

            ALTER TABLE workspace_delivery_outbox
            DROP CONSTRAINT IF EXISTS
                workspace_delivery_outbox_submission_error_code_check;

            ALTER TABLE workspace_delivery_outbox
            ADD CONSTRAINT workspace_delivery_outbox_submission_error_code_check
            CHECK (
                submission_error_code IS NULL
                OR length(submission_error_code) BETWEEN 1 AND 128
            );
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            UPDATE workspace_delivery_outbox
            SET submission_state = 'pending',
                submission_error_code = NULL,
                next_submission_at = now()
            WHERE submission_state = 'rejected';

            ALTER TABLE workspace_delivery_outbox
            DROP CONSTRAINT IF EXISTS
                workspace_delivery_outbox_submission_error_code_check;

            ALTER TABLE workspace_delivery_outbox
            DROP COLUMN IF EXISTS submission_error_code;

            ALTER TABLE workspace_delivery_outbox
            DROP CONSTRAINT IF EXISTS
                workspace_delivery_outbox_submission_state_check;

            ALTER TABLE workspace_delivery_outbox
            ADD CONSTRAINT workspace_delivery_outbox_submission_state_check
            CHECK (
                submission_state IN (
                    'pending', 'submitting', 'ambiguous', 'awaiting_result',
                    'sent'
                )
            );
            """
        )


migration_step = MigrationStep()
