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
        self._depends = [
            "0048-Retry-history-batches-after-source-limit-increase-21b5ff.py"
        ]

    @property
    def migration_id(self):
        return "e0a3cd71-8d87-4425-9ba8-333aabfe8e87"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute("""
            CREATE TABLE IF NOT EXISTS zulip_missing_message_recovery (
                operation_uuid uuid PRIMARY KEY,
                record_uuid uuid NOT NULL UNIQUE,
                account_uuid uuid NOT NULL,
                account_generation bigint NOT NULL,
                assignment_uuid uuid,
                assignment_generation bigint,
                project_uuid uuid NOT NULL,
                provider_realm_uuid uuid,
                provider_message_id text NOT NULL,
                provider_chat_key text NOT NULL,
                provider_topic_id text,
                state text NOT NULL DEFAULT 'pending' CHECK (
                    state IN ('pending', 'delivering', 'complete',
                              'tombstoned', 'superseded', 'blocked')
                ),
                reference jsonb,
                recovery_operations uuid[] NOT NULL DEFAULT '{}',
                lease_uuid uuid,
                lease_until timestamptz,
                available_at timestamptz NOT NULL DEFAULT now(),
                attempts integer NOT NULL DEFAULT 0,
                error_code text,
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            )
        """)
        session.execute("""
            CREATE INDEX IF NOT EXISTS zulip_missing_message_recovery_pending
            ON zulip_missing_message_recovery (available_at, operation_uuid)
            WHERE state IN ('pending', 'delivering')
        """)
        session.execute("""
            CREATE INDEX IF NOT EXISTS zulip_missing_message_recovery_references
            ON zulip_missing_message_recovery
                (account_uuid, provider_message_id)
            WHERE state = 'pending'
        """)

    def downgrade(self, session):
        raise RuntimeError(
            "Missing-base recovery requires code-only rollback; retain durable intents"
        )


migration_step = MigrationStep()
