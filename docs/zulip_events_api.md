# Zulip events API contract

The event collector uses the documented raw REST API. It does not reuse an
earlier bridge implementation.

Authoritative sources:

- [Register an event queue](https://zulip.com/api/register-queue)
- [Get events from an event queue](https://zulip.com/api/get-events)
- [Real-time events](https://zulip.com/api/real-time-events)
- [Get subscribed channels](https://zulip.com/api/get-subscriptions)
- [Get users](https://zulip.com/api/get-users)
- [Get messages](https://zulip.com/api/get-messages)

## Authentication and queue lifecycle

Each `zulip_users` row starts one native thread and one persistent HTTP client.
The stored login and API key are used directly as HTTP Basic credentials. The
collector never accepts, stores, or exchanges a Zulip account password.

Zulip 12 can store only an API-key hash in its `UserProfile.api_key` database
column, with the usable value kept in Zulip's protected key storage. A seeding
tool must therefore obtain the plaintext key through a supported Zulip
mechanism; copying that database column into `zulip_users.api_key` produces an
invalid credential. The bridge treats provisioning as an external operation
and never attempts to decrypt or regenerate credentials at runtime.

A new queue is registered with `POST /api/v1/register`. The collector omits
`event_types` so every event visible to the user is captured. It requests only
an empty initial state through `fetch_event_types=[]`; the restored queue does
not need to serialize the realm, users, subscriptions, or historical messages.
When no `event_queue_longpoll_timeout_seconds` is returned with that minimal
response, the configured long-poll timeout is used. An extended idle timeout
protects a queue during short network interruptions.

Authentication and account-policy failures are not retried in a loop. The
worker remains parked until its `zulip_users` connection fields change or the
row is removed. This keeps unsupported bot types and rotated keys from creating
an authentication storm while preserving the one-thread-per-row contract.

The thread repeatedly calls `GET /api/v1/events` with the persisted `queue_id`
and `last_event_id`. Event IDs increase but are not assumed to be consecutive.
A `BAD_EVENT_QUEUE_ID` response clears the persisted cursor, changes the user
status to `init`, and registers a new queue. It also invalidates the worker's
in-memory catalog-ready state. The replacement queue therefore always causes a
complete channel and direct-message history reload before long polling resumes.
The old catalog hash remains available only to suppress unchanged PostgreSQL
row writes; it never suppresses the required Zulip reread. Network and server
failures use exponential full-jitter backoff and retain the catalog-ready state
when the queue remains valid. On a daemon restart, an `active` user with a
persisted cursor refreshes only the in-memory user, subscription, and existing
chat-key maps before polling that queue. A valid queue therefore resumes
without a history scan; an expired one enters the full replacement-queue path
above. A shared registration semaphore also bounds these lightweight resume
requests and prevents a restart from registering every user simultaneously.

## Directory, chat, topic, and message fill

The queue is created before catalog loading, so events arriving during a fill
remain buffered by Zulip. The lifecycle is `init -> streaming -> filling ->
active`. A bounded semaphore limits simultaneous full-history readers. This is
backpressure for the shared Zulip HTTP/database capacity, not a correctness
lock between `user_uuid` rows.

The worker first calls `GET /api/v1/users` and upserts every human identity.
Deactivated users are retained with `disabled=true`; bots are excluded. Only a
non-disabled row with an API key owns a worker thread.

For channels, the worker calls `GET /api/v1/users/me/subscriptions` with
`include_subscribers=false`. This avoids transferring the potentially large
subscriber list. One `zulip_chats` row is written per subscribed channel. The
role is `subscriber`; channel-wide values and per-user notification or view
settings are stored in separate JSON objects.

After every user has published a catalog, one reconciliation pass selects a
single supplier for each canonical chat by realm role, then stable user UUID.
Only that supplier pages backward through the selected chat's history from
`GET /api/v1/messages`. The first request anchors at `newest`; subsequent
requests use the lowest message ID from the preceding page with the anchor
excluded. Loading stops only when Zulip returns `found_oldest=true`. Direct
conversations are keyed by sorted participant user IDs. Channel topics receive
stable UUIDs derived from the endpoint, channel ID, and topic name because
Zulip exposes topic names rather than native topic IDs. Bot-authored messages
and direct conversations containing a bot are discarded at normalization.

Each API page is normalized in the worker thread. Known personal flags become
boolean columns, reactions contain bridge user UUIDs, and SHA-256 covers the
canonical stored message state. PostgreSQL receives the page through binary
COPY into a temporary table, inserts missing topics, and performs one
hash-guarded upsert. `created_at` is the Zulip send time. `updated_at` is the
backend write time and changes only when the hash changes. A temporary seen-ID
table supports physical deletion of records absent from a completed full
reload without adding a persistent synchronization column.

Every chat has a SHA-256 hash over canonical JSON and its normalized scalar
fields. The sorted row hashes form the user's aggregate catalog hash. An equal
aggregate hash skips all chat-row writes. When it differs, one transaction
upserts only changed rows, removes stale rows, stores the aggregate hash, and
changes the user status to `active` after message reconciliation succeeds.

## PostgreSQL boundary

Every non-heartbeat event is inserted into `zulip_events`. Heartbeats are
transport keepalives, so they are not stored, but their ID still advances the
user cursor. One PostgreSQL statement locks the matching user queue, inserts
the whole returned batch with `ON CONFLICT DO NOTHING`, and advances
`last_event_id`. The polling thread performs no entity mutation. It does not
acknowledge the batch to Zulip until the inbox transaction completes, so a
retry is safe after an uncertain database result.

The uniqueness key is `(zulip_user_uuid, queue_id, event_id)`: Zulip event IDs
are queue-local and can restart after a queue is replaced.

One asynchronous processor claims bounded ordered batches with
`FOR UPDATE SKIP LOCKED`. It preloads message-to-chat and chat-to-supplier
routes for the entire batch. An event from an expired queue, an unsupported
type, or a user who is not the selected supplier for the affected chat is
completed as `skipped` before entity mutation. Accepted message, edit/move,
flag, reaction, delete, and channel-update events update the canonical tables.
Explicit deletes physically remove the canonical message. Message state hashes
include content, flags, and reactions, so replay after a crash does not move
`updated_at` unless the stored state actually changes. Stale `processing`
claims return to `pending` after a configured timeout. Consecutive messages,
flag deltas, and reaction deltas from the same supplier are normalized in event
order and flushed through one hash-guarded page write.

The processor also enforces a 24-hour default retention window for terminal
`applied`, `skipped`, and `failed` rows. It deletes oldest rows in bounded,
skip-locked batches and alternates cleanup with normal inbox processing while a
large expired tail exists. `pending` and `processing` rows are never expired;
they remain recoverable even if processing is unavailable for longer than the
retention window. PostgreSQL autovacuum makes deleted space reusable without a
blocking `VACUUM FULL` operation.

Because `event_types` is intentionally omitted, a realm-wide event is stored
once for every user queue that can see it. Keeping this raw durable inbox makes
cursor recovery inspectable. Supplier routing prevents duplicated observations
from amplifying canonical writes: one queue applies the change and the other
copies terminate as `not_chat_supplier`.
