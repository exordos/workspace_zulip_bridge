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
        self._depends = ["0021-index-pending-chat-materializations-dcdd12.py"]

    @property
    def migration_id(self):
        return "3ae83f6a-8851-4cff-a5e8-9e6c9f89496a"

    @property
    def is_manual(self):
        return False

    def upgrade(self, session):
        session.execute(
            """
            ALTER TABLE zulip_provider_events
            ADD COLUMN IF NOT EXISTS causal_lane text;

            UPDATE zulip_provider_events
            SET causal_lane = CASE
                WHEN event_type = 'update_message'
                  AND body->>'stream_id' ~ '^[0-9]+$'
                  AND body->>'new_stream_id' ~ '^[0-9]+$'
                  AND (body->>'stream_id')::bigint
                      <> (body->>'new_stream_id')::bigint
                THEN NULL
                WHEN event_type = 'message'
                  AND body->'message'->>'type' = 'stream'
                  AND body->'message'->>'stream_id' ~ '^[0-9]+$'
                THEN 'channel:' || (body->'message'->>'stream_id')
                WHEN event_type = 'user_topic'
                  AND body->>'stream_id' ~ '^[0-9]+$'
                THEN 'channel:' || (body->>'stream_id')
                WHEN event_type = 'subscription'
                  AND body->>'stream_id' ~ '^[0-9]+$'
                THEN 'channel:' || (body->>'stream_id')
                WHEN event_type = 'subscription'
                THEN NULL
                WHEN event_type = 'user_settings'
                THEN 'account:user-settings'
                WHEN event_type = 'realm_user'
                  AND COALESCE(
                      body->'person'->>'user_id', body->>'user_id'
                  ) ~ '^[0-9]+$'
                THEN 'identity:' || COALESCE(
                    body->'person'->>'user_id', body->>'user_id'
                )
                WHEN event_type = 'realm_user'
                THEN 'account:identities'
                WHEN event_type = 'update_message'
                  AND body->>'stream_id' ~ '^[0-9]+$'
                THEN 'channel:' || (body->>'stream_id')
                WHEN event_type = 'update_message'
                  AND body->>'new_stream_id' ~ '^[0-9]+$'
                THEN 'channel:' || (body->>'new_stream_id')
                WHEN event_type IN (
                    'reaction', 'update_message', 'delete_message',
                    'update_message_flags'
                ) AND body->>'message_id' ~ '^[0-9]+$'
                THEN 'message:' || (body->>'message_id')
                WHEN event_type IN (
                    'update_message', 'delete_message',
                    'update_message_flags'
                ) AND jsonb_typeof(body->'message_ids') = 'array'
                  AND jsonb_array_length(body->'message_ids') = 1
                  AND body->'message_ids'->>0 ~ '^[0-9]+$'
                THEN 'message:' || (body->'message_ids'->>0)
                ELSE NULL
            END
            WHERE processing_state IN ('pending', 'delivering');

            WITH scoped_stream_events AS (
                SELECT event.account_uuid, event.queue_id, event.event_id,
                       min((candidate.stream_id)::bigint) AS stream_id
                FROM zulip_provider_events AS event
                CROSS JOIN LATERAL (
                    SELECT event.body->>'stream_id' AS stream_id
                    UNION ALL
                    SELECT stream_id.value
                    FROM jsonb_array_elements_text(
                        CASE
                            WHEN jsonb_typeof(event.body->'stream_ids') = 'array'
                            THEN event.body->'stream_ids'
                            ELSE '[]'::jsonb
                        END
                    ) AS stream_id(value)
                    UNION ALL
                    SELECT subscription.value->>'stream_id'
                    FROM jsonb_array_elements(
                        CASE
                            WHEN jsonb_typeof(event.body->'subscriptions') = 'array'
                            THEN event.body->'subscriptions'
                            ELSE '[]'::jsonb
                        END
                    ) AS subscription(value)
                ) AS candidate
                WHERE event.processing_state IN ('pending', 'delivering')
                  AND event.event_type IN ('subscription', 'user_settings')
                  AND candidate.stream_id ~ '^[0-9]+$'
                GROUP BY event.account_uuid, event.queue_id, event.event_id
                HAVING count(DISTINCT (candidate.stream_id)::bigint) = 1
            )
            UPDATE zulip_provider_events AS event
            SET causal_lane = 'channel:' || scoped.stream_id::text
            FROM scoped_stream_events AS scoped
            WHERE event.account_uuid = scoped.account_uuid
              AND event.queue_id = scoped.queue_id
              AND event.event_id = scoped.event_id;

            WITH direct_chats AS (
                SELECT event.account_uuid, event.queue_id, event.event_id,
                       CASE
                           WHEN count(*) = 2 THEN 'direct:'
                           ELSE 'group_direct:'
                       END || string_agg(
                           recipient.value->>'id', ','
                           ORDER BY (recipient.value->>'id')::bigint
                       ) AS causal_lane
                FROM zulip_provider_events AS event
                CROSS JOIN LATERAL jsonb_array_elements(
                    event.body->'message'->'display_recipient'
                ) AS recipient(value)
                WHERE event.processing_state IN ('pending', 'delivering')
                  AND event.event_type = 'message'
                  AND event.causal_lane IS NULL
                  AND jsonb_typeof(
                      event.body->'message'->'display_recipient'
                  ) = 'array'
                  AND recipient.value->>'id' ~ '^[0-9]+$'
                GROUP BY event.account_uuid, event.queue_id, event.event_id
            )
            UPDATE zulip_provider_events AS event
            SET causal_lane = direct_chat.causal_lane
            FROM direct_chats AS direct_chat
            WHERE event.account_uuid = direct_chat.account_uuid
              AND event.queue_id = direct_chat.queue_id
              AND event.event_id = direct_chat.event_id
              AND direct_chat.causal_lane IS NOT NULL;

            UPDATE zulip_provider_events AS event
            SET causal_lane = mapping.metadata->>'chat_key'
            FROM provider_mappings AS mapping
            WHERE event.processing_state IN ('pending', 'delivering')
              AND event.causal_lane LIKE 'message:%'
              AND mapping.account_uuid = event.account_uuid
              AND mapping.entity_kind = 'message'
              AND mapping.provider_id = split_part(
                  event.causal_lane, ':', 2
              )
              AND NOT mapping.deleted
              AND mapping.metadata->>'chat_key' IS NOT NULL;

            UPDATE zulip_provider_events AS event
            SET causal_lane = source.causal_lane
            FROM zulip_provider_events AS source
            WHERE event.processing_state IN ('pending', 'delivering')
              AND event.causal_lane LIKE 'message:%'
              AND source.account_uuid = event.account_uuid
              AND source.event_type = 'message'
              AND source.body->'message'->>'id' = split_part(
                  event.causal_lane, ':', 2
              )
              AND source.causal_lane IS NOT NULL;

            CREATE TABLE IF NOT EXISTS scheduler_provider_event_lanes (
                account_uuid uuid NOT NULL,
                causal_lane text NOT NULL,
                last_provider_event_dispatched_at timestamptz,
                PRIMARY KEY (account_uuid, causal_lane)
            );

            INSERT INTO scheduler_provider_event_lanes (
                account_uuid, causal_lane
            )
            SELECT DISTINCT account_uuid, causal_lane
            FROM zulip_provider_events
            WHERE causal_lane IS NOT NULL
              AND processing_state IN ('pending', 'delivering')
            ON CONFLICT (account_uuid, causal_lane) DO NOTHING;

            CREATE INDEX IF NOT EXISTS
                zulip_provider_events_lane_head_idx
            ON zulip_provider_events (
                account_uuid, causal_lane, created_at, event_id, queue_id
            ) INCLUDE (processing_state, available_at)
            WHERE causal_lane IS NOT NULL
              AND processing_state IN ('pending', 'delivering');

            CREATE INDEX IF NOT EXISTS
                zulip_provider_events_global_head_idx
            ON zulip_provider_events (
                account_uuid, created_at, event_id, queue_id
            ) INCLUDE (processing_state, available_at)
            WHERE causal_lane IS NULL
              AND processing_state IN ('pending', 'delivering');

            CREATE INDEX IF NOT EXISTS
                zulip_provider_message_context_inflight_idx
            ON zulip_provider_events (
                account_uuid, ((provider_message_context->>'id'))
            )
            WHERE provider_message_context IS NOT NULL
              AND processing_state IN ('pending', 'delivering');
            """
        )

    def downgrade(self, session):
        session.execute(
            """
            DROP INDEX IF EXISTS
                zulip_provider_message_context_inflight_idx;
            DROP INDEX IF EXISTS zulip_provider_events_global_head_idx;
            DROP INDEX IF EXISTS zulip_provider_events_lane_head_idx;
            DROP TABLE IF EXISTS scheduler_provider_event_lanes;
            ALTER TABLE zulip_provider_events
            DROP COLUMN IF EXISTS causal_lane;
            """
        )


migration_step = MigrationStep()
