# Workspace Zulip Bridge

A minimal, performance-oriented foundation for a bidirectional bridge between
Workspace and Zulip. The runtime is a regular systemd daemon written in Python
and backed by PostgreSQL.

This repository intentionally contains no compatibility layer or code copied
from earlier bridge implementations. The current Zulip event contract is
documented in [Zulip events API contract](docs/zulip_events_api.md).

## Design

- One asynchronous daemon process.
- A small fixed PostgreSQL connection pool using `asyncpg`.
- One native long-polling thread per Zulip user.
- Batched, idempotent event persistence with an atomic queue cursor update.
- One asynchronous database-backed event processor with crash-recoverable claims.
- Bounded queue-registration concurrency.
- Concurrent per-user chat discovery with content hashes that suppress unchanged
  writes.
- One short-lived endpoint directory cache, so concurrent workers share one
  human-directory read and PostgreSQL upsert instead of locking the same rows.
- One canonical chat row plus a user-membership row for each candidate source.
- One selected source per chat; only that source loads history and applies live
  entity changes for the chat. Duplicate observations from other users are
  completed as skipped before mutation work.
- Stable UUIDv5 identifiers derived from the Zulip endpoint and native entity ID,
  independent of which source user observed the entity.
- Canonical topic and message snapshots loaded with binary COPY staging and
  hash-guarded PostgreSQL upserts.
- Live message, edit, flag, reaction, move, delete, and channel-update events
  are applied from the durable event inbox after the polling thread advances
  its cursor.
- A daemon restart resumes a still-valid active queue after a lightweight
  runtime-state refresh; it does not reread message history.
- Non-retryable account errors park one worker until its database row changes.
- Bounded SIGINT/SIGTERM shutdown and periodic database liveness probes.
- No web framework, ORM, scheduler framework, or separate broker.

The Exordos Core image co-locates PostgreSQL with the daemon for the smallest
deployable unit and lowest database latency. PostgreSQL data lives on the
node's separate persistent disk. The database connection uses the local Unix
socket and peer authentication, so the default deployment has no database
password to distribute.

## Layout

```text
workspace_zulip_bridge/
├── config.py       # Environment-only runtime settings
├── chat_catalog.py # Canonical channel and direct-conversation catalog
├── database.py     # Pool creation and idempotent schema bootstrap
├── event_processor.py # Supplier-gated durable event processing
├── event_store.py  # Batched event and cursor persistence
├── message_history.py # Canonical message, flag, reaction, and hash builder
├── monitor.py      # Read-only counters, rates, and storage metrics
├── models.py       # User, queue, and event value objects
├── zulip_api.py    # Minimal synchronous Zulip REST client
├── zulip_worker.py # Per-user threads and their supervisor
├── service.py      # Daemon lifecycle and supervision
├── cli.py          # Console entry point and signal handling
└── schema.sql      # Initial PostgreSQL schema
etc/                # Environment example and systemd unit
exordos/            # Exordos Core build, image, and manifest files
```

## Development

Python 3.12 or newer and PostgreSQL 15 or newer are required.

```bash
tox -e develop
createdb workspace_zulip_bridge
.tox/develop/bin/workspace-zulip-bridge
```

The daemon applies the idempotent initial schema when it opens the database.
The schema contains `workspace_zulip_bridge.zulip_users`, with the Zulip
endpoint, API credentials, queue cursor, lifecycle status, and catalog hash;
canonical `workspace_zulip_bridge.zulip_chats`; candidate memberships in
`zulip_chat_users`; canonical `zulip_topics` and `zulip_messages`; and durable
inbox `workspace_zulip_bridge.zulip_events`. Event payloads remain immutable;
claim, attempt, outcome, and timing columns track processing. Empty `workspace_chats`,
`workspace_topics`, and `workspace_messages` tables mirror the three canonical
Zulip entity tables for the future Workspace-side projection. No runtime path
writes to those destination mirrors yet. The
`(endpoint, login)` pair is unique. Disabled human identities remain in
`zulip_users` for foreign-key resolution but do not own a synchronization
thread. Bots are neither inserted into the user directory nor materialized as
messages or reactions.

A user moves through `init`, `streaming`, `filling`, `scheduling`,
`backfilling`, and `active`. Queue registration establishes `streaming`.
`filling` discovers the human directory, subscribed channels, and the direct
conversations returned in Zulip's registration state while the queue buffers
new events. It does not scan message history. Zulip decides how much of the
direct-conversation list is "recent" (historically it has been based on the
1,000 most recently received direct messages), so older inactive direct chats
are intentionally deferred to a later depth-discovery pass.
`scheduling` selects one source for every canonical chat by Zulip realm role
(owner, admin, moderator, member, guest), then stable user UUID. Only the
selected user's `backfilling` phase reads that chat's
history. A successful per-chat reconciliation establishes `active` when no
assigned history remains. Existing row hashes suppress identical writes, so
their backend `updated_at` value does not move.

If Zulip reports that a queue was deleted, the worker returns the user to
`init`, removes its selected-chat snapshots, registers a new queue, rediscovers
the catalog, and backfills its assignments again. Deleting or disabling a
selected user clears the affected assignments, removes that source's messages,
and deterministically selects the next eligible user. A process restart resumes
a still-valid queue and rebuilds only its in-memory directory and chat-key maps.

Polling threads only persist non-heartbeat, non-bot events. The event processor
claims a bounded ordered batch with `FOR UPDATE SKIP LOCKED` and recovers claims
left stale by a crash. It resolves chat ownership for the whole batch before
applying data. Message, edit/move, flag, reaction, delete, and channel-update
events are applied only when the event's queue owner is the selected supplier
for the destination chat; duplicate observations from other users are marked
`skipped` with `not_chat_supplier`. Unsupported events are also completed as
skipped so they cannot block the inbox. Consecutive messages, flags, and
reactions from one supplier are normalized in event order and written as one
hash-guarded page, which removes per-event PostgreSQL round trips without
changing the final state. The same processor periodically deletes terminal
`applied`, `skipped`, and `failed` events more than 24 hours after collection.
Cleanup uses bounded batches and leaves `pending` and `processing` rows intact,
so a long outage cannot silently discard unprocessed changes. Stable UUIDs,
state hashes, set-style flag/reaction changes, and provider event keys make
retries idempotent.

Use a dedicated development database and override the DSN when necessary:

```bash
WZB_DATABASE_DSN=postgresql://localhost/workspace_zulip_bridge \
  .tox/develop/bin/workspace-zulip-bridge
```

Run the checks with:

```bash
tox -e py,ruff,mypy
```

Optional PostgreSQL integration tests run only when
`WZB_TEST_DATABASE_DSN` names a disposable database.

## Runtime configuration

All settings are environment variables. Defaults favor a local Exordos Core
deployment.

| Variable | Default | Purpose |
| --- | --- | --- |
| `WZB_DATABASE_DSN` | local Unix socket | PostgreSQL connection string |
| `WZB_DB_POOL_MIN_SIZE` | `2` | Warm connections kept open |
| `WZB_DB_POOL_MAX_SIZE` | `16` | Maximum database connections |
| `WZB_DB_COMMAND_TIMEOUT_SECONDS` | `30` | Query timeout |
| `WZB_DB_PROBE_SECONDS` | `30` | Database liveness interval |
| `WZB_USER_REFRESH_SECONDS` | `5` | User-table reconciliation interval |
| `WZB_ZULIP_CA_FILE` | system trust | Optional Zulip CA bundle |
| `WZB_ZULIP_CONNECT_TIMEOUT_SECONDS` | `10` | Zulip connection timeout |
| `WZB_ZULIP_DEFAULT_LONGPOLL_TIMEOUT_SECONDS` | `180` | Fallback long-poll timeout |
| `WZB_ZULIP_DB_ACK_TIMEOUT_SECONDS` | `120` | Database acknowledgement timeout |
| `WZB_ZULIP_RETRY_BASE_SECONDS` | `1` | Initial retry window |
| `WZB_ZULIP_RETRY_CAP_SECONDS` | `60` | Maximum retry window |
| `WZB_ZULIP_IDLE_QUEUE_TIMEOUT_SECONDS` | `3600` | Requested queue lifetime |
| `WZB_ZULIP_REGISTRATION_CONCURRENCY` | `8` | Concurrent queue registrations |
| `WZB_ZULIP_MESSAGE_SCAN_CONCURRENCY` | `32` | Concurrent in-memory message pages across all user threads |
| `WZB_ZULIP_HISTORY_CONCURRENCY` | `12` | Concurrent history sessions; must leave database-pool capacity free |
| `WZB_ZULIP_DIRECTORY_CACHE_TTL_SECONDS` | `60` | Shared endpoint directory cache lifetime |
| `WZB_ZULIP_CHAT_FILL_TIMEOUT_SECONDS` | `120` | Read timeout for a catalog API page |
| `WZB_ZULIP_MESSAGE_PAGE_SIZE` | `5000` | Combined message-history page size |
| `WZB_EVENT_PROCESSOR_BATCH_SIZE` | `1000` | Maximum events claimed per processor pass |
| `WZB_EVENT_PROCESSOR_POLL_SECONDS` | `0.05` | Idle inbox polling interval |
| `WZB_EVENT_PROCESSOR_CLAIM_TIMEOUT_SECONDS` | `60` | Stale processing-claim recovery threshold |
| `WZB_EVENT_RETENTION_SECONDS` | `86400` | Terminal event retention from collection time |
| `WZB_EVENT_CLEANUP_INTERVAL_SECONDS` | `300` | Interval between caught-up retention passes |
| `WZB_EVENT_CLEANUP_BATCH_SIZE` | `10000` | Rows deleted per short retention transaction |
| `WZB_THREAD_STOP_TIMEOUT_SECONDS` | `5` | Worker shutdown deadline |
| `WZB_LOG_LEVEL` | `INFO` | Python log level |

The `api_key` column is sensitive. It is used with `login` for Zulip HTTP Basic
authentication and is never logged. Never use real credentials in tests or
fixtures.

## Monitoring

Run the read-only monitor with the same database environment as the daemon:

```bash
workspace-zulip-bridge-monitor
```

Every five seconds it prints total and sync-enabled user, lifecycle-status,
ready-queue, canonical-chat, chat-membership, assignment, pending-history,
topic, message, and event counters;
event processing states, applied/skipped/failed rates, pending age, average and
p95 processing latency; recent message-change and ingress rates; and heap,
index, relation, and database sizes. Rolling queries use small BRIN indexes on
event timestamps and hash-guarded message `updated_at`.

Use a single exact snapshot when exact event totals and totals by type are
needed:

```bash
workspace-zulip-bridge-monitor --once --exact
```

Exact mode scans both message and event tables and counts reactions, so it
should not be used at a short interval on a large database. `--window`,
`--interval`, and `--json` customize the sampling window, cadence, and output
format.

## Exordos Core build

The element manifest requires the destination Exordos Core project UUID at
build time:

```bash
exordos build \
  --manifest-var project_id=<project-uuid> \
  --manifest-var repository=https://repo.example.com/exordos-elements \
  .
```

Install the rendered local manifest with the `exordos` CLI:

```bash
exordos elements install output/manifests/workspace_zulip_bridge.yaml
```

The image bootstrap prepares the persistent disk, starts PostgreSQL, creates a
peer-authenticated database role, and enables the daemon.
