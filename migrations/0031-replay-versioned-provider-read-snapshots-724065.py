# Copyright 2016 Eugene Frolov <eugene@frolov.net.ru>
#
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License.

from restalchemy.storage.sql import migrations


class MigrationStep(migrations.AbstractMigrationStep):
    def __init__(self):
        self._depends = ["0030-converge-shared-realm-message-mappings-75632b.py"]

    @property
    def migration_id(self):
        return "7240653c-a865-4275-8954-3eb8418d6b68"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        # Backfill queue identities now include the snapshot projection
        # version. Restart every selected history window so the new identities
        # bypass terminal deliveries from the old unversioned scan and submit
        # the current provider read/unread snapshot for every account.
        session.execute(
            """
            UPDATE zulip_backfill_jobs AS job
            SET next_anchor = NULL,
                state = 'pending',
                cutoff_at = CASE
                    WHEN job.history_depth = 'new'
                    THEN COALESCE(job.cutoff_at, assignment.updated_at)
                    ELSE job.cutoff_at
                END,
                available_at = now(),
                retry_count = 0,
                last_error_code = NULL,
                lease_until = NULL,
                updated_at = now()
            FROM desired_resources AS assignment
            JOIN desired_resources AS account
              ON account.resource_type = 'external_account'
             AND account.resource_uuid::text =
                 assignment.body->>'external_account_uuid'
             AND NOT account.deleted
             AND COALESCE(
                     (account.body->>'synchronization_enabled')::boolean,
                     false
                 )
            WHERE assignment.resource_type = 'external_chat_assignment'
              AND NOT assignment.deleted
              AND COALESCE((assignment.body->>'selected')::boolean, true)
              AND assignment.body->>'external_account_uuid' =
                  job.account_uuid::text
              AND assignment.body->'provider_chat'->>'provider_chat_key' =
                  job.provider_chat_key;
            """
        )

    def downgrade(self, session):
        # Replayed provider history cannot be rolled back safely.
        return None


migration_step = MigrationStep()
