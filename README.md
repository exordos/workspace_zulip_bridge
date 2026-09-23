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
- One provider-wide Workspace WebSocket receiver, independent of the number of
  Zulip users and realms handled by the process.
- Batched, durable Workspace event ingestion with UUID deduplication, a
  transactionally advanced resume cursor, and a PostgreSQL advisory lease for
  active/passive daemon replicas.
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
├── workspace_events.py # Provider WebSocket, cursor, and durable inbox
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
`zulip_realms` owns endpoint identity, `zulip_users` contains stable provider
identities, and `zulip_connections` contains API credentials, queue cursor,
lifecycle status, and catalog hash. The Workspace-like projection consists of
canonical `zulip_streams`, `zulip_stream_bindings`, `zulip_topics`,
`zulip_topic_aliases`, `zulip_messages`, per-user `zulip_message_flags`, and
normalized `zulip_message_reactions`. `zulip_files` and `zulip_message_files`
store only upload metadata, ownership, and message relationships: the bridge
loads each file owner's metadata from `GET /attachments` but never requests or
persists file bytes. A later Workspace delivery adapter will download a file
immediately before sending it.

The durable `zulip_events` inbox keeps immutable raw payloads plus claim,
attempt, outcome, and timing state. `workspace_outbox` is an internal,
coalescing entity-change journal; it deliberately contains no invented
Workspace API payload. `workspace_events` is the independent inbound journal
for the provider-wide Workspace socket. Disabled identities and bots remain
available for foreign-key resolution, while only enabled non-bot identities
own Zulip synchronization threads. Bot-authored messages and reactions are
still materialized through those human-owned streams, matching what users see
in Zulip.

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
`init`, registers a new queue, rediscovers the catalog, and reconciles only a
bounded recent-history window with a small cursor-overlap margin. Normal
operation is event-driven: Zulip long-poll events and the durable Workspace
journal advance the two projections without repeatedly scanning all source
tables. Deleting or disabling a
selected user clears the affected assignments, removes that source's messages,
and deterministically selects the next eligible user. A process restart resumes
a still-valid queue and rebuilds only its in-memory directory and chat-key maps.

Polling threads discard heartbeats but preserve bot-authored activity. The event processor
claims a bounded ordered batch with `FOR UPDATE SKIP LOCKED` and recovers claims
left stale by a crash. It resolves chat ownership for the whole batch before
applying data. Message, edit/move, reaction, delete, and channel-update events
are applied only when the event's queue owner is the selected supplier for the
destination stream; duplicate common-data observations from other users are
marked `skipped` with `not_chat_supplier`. Personal flag events update that
queue owner's `zulip_message_flags` row whenever the user belongs to the stream,
even when another connection supplies the common message. Unsupported events are also completed as
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

## Workspace project selection

The Workspace element owns the mutable `workspace_project_id` Values Store
variable. Set it before installing the bridge; the selected value is used for
both the bridge service-account role binding and `WZB_WORKSPACE_PROJECT_ID`.
List the variable and create its first value with the regular Exordos Values
Store commands:

```bash
exordos vs vv list --filters name=workspace_project_id --output yaml
exordos vs values add \
  --project-id INFRASTRUCTURE_PROJECT_UUID \
  --name workspace_project_id \
  --var WORKSPACE_PROJECT_VARIABLE_UUID \
  --value WORKSPACE_PROJECT_UUID
```

To select another Workspace project, update the existing value and let Element
Manager reconcile the bridge resources:

```bash
exordos vs values list --output yaml
exordos vs values update WORKSPACE_PROJECT_VALUE_UUID \
  --value WORKSPACE_PROJECT_UUID
```

The config reconciliation removes persisted Workspace access and refresh tokens
and the desired-state cursor before restarting the daemon. The next login
requests a token scoped to the new project, and the bridge takes a fresh
desired-state snapshot instead of continuing an old project's cursor. External
accounts whose `default_project_id` does not match the selected project are
reported as failed instead of remaining in `connecting`.

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
| `WZB_ZULIP_QUEUE_GAP_RECONCILIATION_SECONDS` | `86400` | Recent-history window reconciled after a deleted or expired Zulip queue |
| `WZB_ZULIP_DIRECTORY_CACHE_TTL_SECONDS` | `60` | Shared endpoint directory cache lifetime |
| `WZB_ZULIP_CHAT_FILL_TIMEOUT_SECONDS` | `120` | Read timeout for a catalog API page |
| `WZB_ZULIP_MESSAGE_PAGE_SIZE` | `5000` | Combined message-history page size |
| `WZB_EVENT_PROCESSOR_BATCH_SIZE` | `1000` | Maximum events claimed per processor pass |
| `WZB_EVENT_PROCESSOR_POLL_SECONDS` | `0.05` | Idle inbox polling interval |
| `WZB_EVENT_PROCESSOR_CLAIM_TIMEOUT_SECONDS` | `60` | Stale processing-claim recovery threshold |
| `WZB_EVENT_PROCESSOR_MAX_ATTEMPTS` | `8` | Attempts before a transient Zulip event failure becomes terminal |
| `WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS` | `0.25` | Initial transient Zulip event retry delay |
| `WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS` | `30` | Maximum transient Zulip event retry delay |
| `WZB_EVENT_RETENTION_SECONDS` | `86400` | Terminal event retention from collection time |
| `WZB_EVENT_CLEANUP_INTERVAL_SECONDS` | `300` | Interval between caught-up retention passes |
| `WZB_EVENT_CLEANUP_BATCH_SIZE` | `10000` | Rows deleted per short retention transaction |
| `WZB_WORKSPACE_WEBSOCKET_URL` | disabled | Provider event WebSocket URL; enables the receiver with the next three settings |
| `WZB_WORKSPACE_PROJECT_ID` | disabled | Workspace project served by the provider consumer |
| `WZB_WORKSPACE_PROVIDER_UUID` | disabled | Backend provider-consumer UUID used in event frames and cursors |
| `WZB_WORKSPACE_TOKEN_FILE` | disabled | Root-managed file containing the dedicated IAM bearer token |
| `WZB_WORKSPACE_CA_FILE` | system trust | Optional Workspace CA bundle |
| `WZB_WORKSPACE_EVENT_BATCH_SIZE` | `500` | Events written per inbox transaction |
| `WZB_WORKSPACE_EVENT_FLUSH_SECONDS` | `0.01` | Maximum low-volume persistence delay |
| `WZB_WORKSPACE_EVENT_MAX_ATTEMPTS` | `8` | Attempts before a transient Workspace event failure becomes terminal |
| `WZB_WORKSPACE_RETRY_BASE_SECONDS` | `1` | Initial reconnect window |
| `WZB_WORKSPACE_RETRY_CAP_SECONDS` | `60` | Maximum reconnect window |
| `WZB_WORKSPACE_LEASE_RETRY_SECONDS` | `5` | Standby receiver lease retry interval |
| `WZB_WORKSPACE_DEPENDENCY_RETRY_BASE_SECONDS` | `2` | Initial retry delay for entities whose Workspace parents are not ready |
| `WZB_WORKSPACE_DEPENDENCY_RETRY_CAP_SECONDS` | `300` | Maximum dependency retry delay |
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

The repeatable synthetic import benchmark requires an explicitly named
disposable test database and recreates only the bridge schema:

```bash
WZB_BENCHMARK_DATABASE_DSN=postgresql:///workspace_zulip_bridge_benchmark \
  .tox/py/bin/python scripts/benchmark_import.py --messages 100000
```

The Workspace inbox benchmark uses the same disposable-database guard:

```bash
WZB_BENCHMARK_DATABASE_DSN=postgresql:///workspace_zulip_bridge_benchmark \
  .tox/py/bin/python scripts/benchmark_workspace_events.py \
  --events 100000 --batch-size 500
```

## Exordos Core build

The element manifest requires the destination Exordos Core project UUID at
build time. Stage the exact tracked Git tree first so the image cannot reuse a
stale working-tree copy:

```bash
./scripts/stage_element_source.sh
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

The Workspace-side synchronization dependency and its owning source are
documented in [docs/workspace_provider_entity_api.md](docs/workspace_provider_entity_api.md).
