# Provider HTTP runtime

The bridge data plane is the private Workspace Provider API defined by
`workspace_backend/docs/workspace_provider_api_v1.yaml`. Control-plane desired
state and heartbeats remain on the separate control API. File bytes use the
private file API.

## Authentication

Control, Provider, and file clients use the enrolled bridge mTLS identity. The
Provider client validates the backend hostname and its configured CA bundle.
Missing Provider configuration is a startup error; there is no mail transport
fallback.

## Workspace to Zulip

The bridge polls:

```text
POST /api/workspace-provider/v1/operations/actions/lease
```

The request uses a client-generated request UUID, a maximum batch size, and a
300-second lease. The same request UUID is retained across an ambiguous HTTP
transport failure. Each returned operation is durably bound to its
`provider_operation_uuid` and `lease_uuid` before execution.

Terminal outcomes are reported to:

```text
POST /api/workspace-provider/v1/operation-results
```

`applied` and `duplicate` acknowledge success. `conflict`, `rejected`, and
`not_found` become local manual-reconciliation evidence. `stale_lease` is
terminal for that lease; a later lease of the same immutable operation rebinds
the durable result. No response status is retried forever.

## Zulip to Workspace

Canonical resource events are submitted to:

```text
POST /api/workspace-provider/v1/events
```

The backend applies each batch atomically. The bridge validates response order,
event UUIDs, and `applied` status before committing its local outbox. Transport
errors and invalid or non-applied responses release every claimed submission so
the idempotent event UUIDs can be retried.

### Staged synchronization

The first provider queue is registered and its cursor is persisted before any
catalog or history work. Initial channel catalog reports contain channel names
only. Once Workspace selects a channel, the bridge fetches the authoritative
Zulip subscriber list and reports it before admitting live messages or
starting the configured history backfill. The participant gate opens only when
the current Workspace assignment projection contains the same provider user
IDs.

Zulip topics are discovered from messages. The durable provider mapping table
is the local processed-topic cache: a missing topic queues an idempotent catalog
report, message delivery waits for the resulting Workspace topic mapping, and
then an idempotent `topic.upsert` precedes `message.create`.

If Zulip rejects a persisted queue, the bridge records a catch-up boundary,
invalidates only that queue cursor, and opens a replacement queue. Selected
channel participants are revalidated and configured history jobs restart from
their beginning. Stable provider mappings and operation UUIDs make the repeated
entity creation and message delivery idempotent.

## Runtime boundary

The element imports only the backend, enrollment secret, and persistent bridge
disk resources it needs. Its manifest and image contain no Workspace mail node,
mail credentials, IMAP/SMTP configuration, mail CA bootstrap, or Maildir state.

## Scheduling and retry behavior

Every active Zulip account owns one persistent long-poll thread and one
adapter/client instance. The worker durably records provider events and advances
the queue cursor before the main service thread performs history work, so a slow
history page cannot leave a gap in live event capture. Account workers are
isolated from one another and use bridge-owned durable retry/backoff after a
provider error.

Control-derived backfill jobs are reconciled in the main service thread once per
service tick. Live operations and priority-0 Provider events are always
processed first. At least once per second, one exact priority-2 history item
receives a bounded delivery quantum even while live traffic remains continuous.
Retryable history-fetch failures return the job to `pending` with a durable
`available_at`, incremented retry count, safe error code, and exponential full
jitter capped at 300 seconds. A worker restart therefore does not erase retry
deferral.
Non-retryable history errors mark only the affected account/chat job as
`failed`, retain its safe error code, and emit scoped degraded health plus an
account observed report. Other accounts continue polling and synchronizing.

`RestAlchemyStore` obtains each transaction from the RestAlchemy PostgreSQL
engine and scopes it with `session_manager()`. The engine pool may reuse
connections, but a session never crosses a store-operation or worker-thread
boundary. Concurrent account long-poll workers also own separate adapter/client
instances, so Zulip client state does not cross worker-thread boundaries.
