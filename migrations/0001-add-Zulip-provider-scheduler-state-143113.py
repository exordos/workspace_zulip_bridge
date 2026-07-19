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
    CREATE TABLE IF NOT EXISTS zulip_event_cursors (
        account_uuid uuid PRIMARY KEY,
        queue_id text NOT NULL,
        last_event_id bigint NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS zulip_provider_events (
        account_uuid uuid NOT NULL,
        queue_id text NOT NULL,
        event_id bigint NOT NULL,
        event_type text NOT NULL,
        body jsonb NOT NULL,
        processing_state text NOT NULL DEFAULT 'pending' CHECK (
            processing_state IN (
                'pending', 'delivering', 'processed', 'unsupported', 'invalid'
            )
        ),
        processing_reason text,
        retry_count integer NOT NULL DEFAULT 0,
        available_at timestamptz NOT NULL DEFAULT now(),
        created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (account_uuid, queue_id, event_id)
    )
    """,
    """
    ALTER TABLE zulip_provider_events
    DROP CONSTRAINT IF EXISTS zulip_provider_events_processing_state_check
    """,
    """
    ALTER TABLE zulip_provider_events
    ADD CONSTRAINT zulip_provider_events_processing_state_check CHECK (
        processing_state IN (
            'pending', 'delivering', 'processed', 'unsupported', 'invalid'
        )
    )
    """,
    """
    ALTER TABLE zulip_provider_events
    ADD COLUMN IF NOT EXISTS processing_reason text
    """,
    """
    ALTER TABLE zulip_provider_events
    ADD COLUMN IF NOT EXISTS retry_count integer NOT NULL DEFAULT 0
    """,
    """
    ALTER TABLE zulip_provider_events
    ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT now()
    """,
    """
    CREATE TABLE IF NOT EXISTS zulip_backfill_jobs (
        account_uuid uuid NOT NULL,
        provider_chat_key text NOT NULL,
        history_depth text NOT NULL CHECK (
            history_depth IN ('new', '7_days', '30_days', '90_days', 'all')
        ),
        next_anchor bigint,
        cutoff_at timestamptz,
        state text NOT NULL DEFAULT 'pending' CHECK (
            state IN ('pending', 'running', 'complete', 'cancelled', 'failed')
        ),
        lease_until timestamptz,
        available_at timestamptz NOT NULL DEFAULT now(),
        retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
        last_error_code text,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (account_uuid, provider_chat_key)
    )
    """,
    """
    ALTER TABLE zulip_backfill_jobs
    ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT now()
    """,
    """
    ALTER TABLE zulip_backfill_jobs
    ADD COLUMN IF NOT EXISTS retry_count integer NOT NULL DEFAULT 0
    """,
    """
    ALTER TABLE zulip_backfill_jobs
    ADD COLUMN IF NOT EXISTS last_error_code text
    """,
    """
    ALTER TABLE zulip_backfill_jobs
    DROP CONSTRAINT IF EXISTS zulip_backfill_jobs_retry_count_check
    """,
    """
    ALTER TABLE zulip_backfill_jobs
    ADD CONSTRAINT zulip_backfill_jobs_retry_count_check CHECK (retry_count >= 0)
    """,
    """
    ALTER TABLE zulip_backfill_jobs
    DROP CONSTRAINT IF EXISTS zulip_backfill_jobs_state_check
    """,
    """
    ALTER TABLE zulip_backfill_jobs
    ADD CONSTRAINT zulip_backfill_jobs_state_check CHECK (
        state IN ('pending', 'running', 'complete', 'cancelled', 'failed')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS zulip_queue_catchup_jobs (
        account_uuid uuid NOT NULL,
        provider_chat_key text NOT NULL,
        checkpoint_provider_message_id bigint,
        next_anchor bigint,
        seen_provider_message_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
        page_count integer NOT NULL DEFAULT 0 CHECK (page_count >= 0),
        state text NOT NULL DEFAULT 'pending' CHECK (
            state IN ('pending', 'complete', 'manual')
        ),
        safe_error_code text,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (account_uuid, provider_chat_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scheduler_accounts (
        account_uuid uuid PRIMARY KEY,
        last_dispatched_at timestamptz
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS observed_report_outbox (
        report_uuid uuid PRIMARY KEY,
        body jsonb NOT NULL,
        available_at timestamptz NOT NULL DEFAULT now(),
        attempts integer NOT NULL DEFAULT 0,
        result_status text,
        completed_at timestamptz,
        created_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS external_chat_catalog_state (
        account_uuid uuid NOT NULL,
        provider_chat_key text NOT NULL,
        participants jsonb NOT NULL DEFAULT '[]'::jsonb,
        topics jsonb NOT NULL DEFAULT '[]'::jsonb,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (account_uuid, provider_chat_key)
    )
    """,
    """
    ALTER TABLE observed_report_outbox
    ADD COLUMN IF NOT EXISTS completed_at timestamptz
    """,
    """
    ALTER TABLE observed_report_outbox
    ADD COLUMN IF NOT EXISTS result_status text
    """,
)


DOWNGRADE_TABLES = (
    "external_chat_catalog_state",
    "observed_report_outbox",
    "scheduler_accounts",
    "zulip_queue_catchup_jobs",
    "zulip_backfill_jobs",
    "zulip_provider_events",
    "zulip_event_cursors",
)


class MigrationStep(migrations.AbstractMigrationStep):
    def __init__(self):
        self._depends = ["0000-initialize-bridge-operational-state-18f707.py"]

    @property
    def migration_id(self):
        return "1431139c-e669-4be3-a10f-1fd5d3e98954"

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
