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

UPGRADE_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS bridge_metadata (
        singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
        control_cursor text NOT NULL DEFAULT '',
        control_generation bigint NOT NULL DEFAULT 0,
        blocked_batch jsonb,
        updated_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    "ALTER TABLE bridge_metadata ADD COLUMN IF NOT EXISTS blocked_batch jsonb",
    """
    INSERT INTO bridge_metadata (singleton) VALUES (true)
    ON CONFLICT (singleton) DO NOTHING
    """,
    """
    CREATE TABLE IF NOT EXISTS desired_resources (
        resource_type text NOT NULL,
        resource_uuid uuid NOT NULL,
        generation bigint NOT NULL CHECK (generation > 0),
        body jsonb,
        deleted boolean NOT NULL DEFAULT false,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (resource_type, resource_uuid)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bridge_operations (
        record_uuid uuid PRIMARY KEY,
        operation_uuid uuid NOT NULL,
        attempt integer NOT NULL CHECK (attempt > 0),
        operation_sha256 char(64) NOT NULL,
        account_uuid uuid NOT NULL,
        project_uuid uuid NOT NULL,
        origin text NOT NULL CHECK (origin IN ('workspace', 'zulip')),
        causal_lane text NOT NULL,
        lane_sequence bigint NOT NULL CHECK (lane_sequence > 0),
        predecessor_operation_uuid uuid,
        assignment_uuid uuid,
        assignment_generation bigint,
        priority smallint NOT NULL CHECK (priority BETWEEN 0 AND 2),
        state text NOT NULL CHECK (
            state IN (
                'pending', 'running', 'uncertain', 'committed', 'rejected',
                'expired', 'cancelled'
            )
        ),
        retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
        available_at timestamptz NOT NULL DEFAULT now(),
        expires_at timestamptz,
        lease_owner text,
        lease_until timestamptz,
        record jsonb NOT NULL,
        result_record jsonb,
        result_sent_at timestamptz,
        last_error_code text,
        provider_queue_id text,
        provider_local_id text,
        provider_attempted_at timestamptz,
        reconciliation_check_count integer NOT NULL DEFAULT 0,
        reconciliation_after timestamptz,
        auto_resend_count integer NOT NULL DEFAULT 0,
        reconciliation_evidence jsonb NOT NULL DEFAULT '[]'::jsonb,
        manual_reconciliation_required boolean NOT NULL DEFAULT false,
        manual_context jsonb,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE (operation_uuid, attempt),
        UNIQUE (origin, causal_lane, lane_sequence, attempt)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS bridge_operations_ready_idx
    ON bridge_operations (priority, available_at, created_at)
    WHERE state = 'pending'
    """,
    """
    CREATE TABLE IF NOT EXISTS operation_idempotency (
        operation_uuid uuid PRIMARY KEY,
        operation_sha256 char(64) NOT NULL,
        terminal_outcome text,
        target_entity_id text,
        target_revision text,
        result_record_uuid uuid,
        manual_retry_allowed boolean NOT NULL DEFAULT false,
        updated_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    ALTER TABLE operation_idempotency
    ADD COLUMN IF NOT EXISTS manual_retry_allowed boolean NOT NULL DEFAULT false
    """,
    """
    CREATE TABLE IF NOT EXISTS causal_lane_state (
        origin text NOT NULL,
        causal_lane text NOT NULL,
        last_sequence bigint NOT NULL DEFAULT 0,
        last_operation_uuid uuid,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (origin, causal_lane)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS producer_lane_counters (
        origin text NOT NULL,
        causal_lane text NOT NULL,
        last_sequence bigint NOT NULL DEFAULT 0,
        last_operation_uuid uuid,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (origin, causal_lane)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS producer_operations (
        operation_uuid uuid PRIMARY KEY,
        origin text NOT NULL,
        causal_lane text NOT NULL,
        lane_sequence bigint NOT NULL,
        predecessor_operation_uuid uuid,
        created_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE (origin, causal_lane, lane_sequence)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS workspace_delivery_outbox (
        record_uuid uuid PRIMARY KEY,
        operation_uuid uuid NOT NULL UNIQUE,
        account_uuid uuid NOT NULL,
        account_generation bigint,
        assignment_uuid uuid,
        assignment_generation bigint,
        assignment_project_uuid uuid,
        provider_queue_id text,
        provider_event_id bigint,
        submission_state text NOT NULL DEFAULT 'pending' CHECK (
            submission_state IN (
                'pending', 'submitting', 'ambiguous', 'awaiting_result', 'sent'
            )
        ),
        submission_attempts integer NOT NULL DEFAULT 0,
        next_submission_at timestamptz NOT NULL DEFAULT now(),
        last_submitted_at timestamptz,
        priority smallint NOT NULL CHECK (priority BETWEEN 0 AND 2),
        record jsonb NOT NULL,
        sent_at timestamptz,
        created_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS account_generation bigint
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS assignment_uuid uuid
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS assignment_generation bigint
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS assignment_project_uuid uuid
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS provider_queue_id text
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS provider_event_id bigint
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS submission_state text NOT NULL DEFAULT 'pending'
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS submission_attempts integer NOT NULL DEFAULT 0
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS next_submission_at timestamptz NOT NULL DEFAULT now()
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD COLUMN IF NOT EXISTS last_submitted_at timestamptz
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    DROP CONSTRAINT IF EXISTS workspace_delivery_outbox_submission_state_check
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD CONSTRAINT workspace_delivery_outbox_submission_state_check CHECK (
        submission_state IN (
            'pending', 'submitting', 'ambiguous', 'awaiting_result', 'sent'
        )
    )
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    DROP CONSTRAINT IF EXISTS workspace_delivery_outbox_submission_attempts_check
    """,
    """
    ALTER TABLE workspace_delivery_outbox
    ADD CONSTRAINT workspace_delivery_outbox_submission_attempts_check CHECK (
        submission_attempts >= 0
    )
    """,
    "ALTER TABLE bridge_operations ADD COLUMN IF NOT EXISTS assignment_uuid uuid",
    """
    ALTER TABLE bridge_operations
    ADD COLUMN IF NOT EXISTS assignment_generation bigint
    """,
    """
    UPDATE workspace_delivery_outbox AS delivery
    SET account_generation = account.generation
    FROM desired_resources AS account
    WHERE delivery.account_generation IS NULL
      AND account.resource_type = 'external_account'
      AND account.resource_uuid = delivery.account_uuid
      AND NOT account.deleted
    """,
    """
    CREATE TABLE IF NOT EXISTS provider_mappings (
        account_uuid uuid NOT NULL,
        entity_kind text NOT NULL,
        workspace_uuid uuid NOT NULL,
        provider_id text NOT NULL,
        provider_revision text,
        metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
        deleted boolean NOT NULL DEFAULT false,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (account_uuid, entity_kind, workspace_uuid),
        UNIQUE (account_uuid, entity_kind, provider_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS provider_mapping_aliases (
        account_uuid uuid NOT NULL,
        entity_kind text NOT NULL,
        workspace_uuid uuid NOT NULL,
        provider_id text NOT NULL,
        metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
        deleted boolean NOT NULL DEFAULT false,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (account_uuid, entity_kind, workspace_uuid)
    )
    """,
    """
    ALTER TABLE provider_mappings
    ADD COLUMN IF NOT EXISTS metadata jsonb NOT NULL DEFAULT '{}'::jsonb
    """,
    """
    CREATE TABLE IF NOT EXISTS bridge_health (
        component text PRIMARY KEY,
        status text NOT NULL,
        progressed_at timestamptz NOT NULL,
        safe_error_code text,
        updated_at timestamptz NOT NULL DEFAULT now()
    )
    """,
)


DOWNGRADE_TABLES = (
    "bridge_health",
    "provider_mapping_aliases",
    "provider_mappings",
    "workspace_delivery_outbox",
    "producer_operations",
    "producer_lane_counters",
    "causal_lane_state",
    "operation_idempotency",
    "bridge_operations",
    "desired_resources",
    "bridge_metadata",
)


class MigrationStep(migrations.AbstractMigrationStep):
    def __init__(self):
        self._depends = []

    @property
    def migration_id(self):
        return "18f707af-e959-49fa-926c-1075a9307325"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        for statement in UPGRADE_STATEMENTS:
            session.execute(statement)

    def downgrade(self, session):
        for table in DOWNGRADE_TABLES:
            session.execute(f'DROP TABLE IF EXISTS "{table}"')


migration_step = MigrationStep()
