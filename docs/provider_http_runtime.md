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
errors and retryable responses release claimed submissions so the idempotent
event UUIDs can be retried. A record-scoped permanent rejection is isolated by
ordered batch bisection. Valid siblings still commit; the rejected record is
retained in the durable outbox with `submission_state = 'rejected'` and a safe
status code, and is not automatically resubmitted.

### Staged synchronization

The first provider queue is registered and its cursor is persisted before any
catalog or history work. Initial channel catalog reports contain channel names
only. Once Workspace selects a channel, the bridge fetches the authoritative
Zulip subscriber list and reports it before admitting live messages or
starting the configured history backfill. The participant gate opens only when
the current Workspace assignment projection contains the same provider user
IDs. Zulip `subscription` peer add/remove events immediately invalidate only
the affected selected channels, so administrator changes return through the
same catalog and desired-state handshake without a full account rescan. A
ready channel becomes eligible for a full subscriber check after one hour as a
safety net for a missed or unavailable subscription event.

Provider-backed Workspace binding changes arrive as durable `membership.add`
and `membership.remove` operations gated by `messenger.membership.write`. The
bridge resolves the mapped Zulip user and channel, then uses the official
subscription add/remove methods. Duplicate delivery, retry, removal, and re-add
therefore converge without a bridge-local membership source of truth.

Provider-backed notification changes arrive as durable
`stream.notification.update` and `topic.notification.update` operations gated
by `messenger.notification.write`. Every operation carries its source
`notification_updated_at` value. The bridge stores the newest provider-side
timestamp in mapping metadata and skips an older Workspace operation; the
backend locks the corresponding user setting and performs the symmetric check
for Zulip events. Registration snapshots seed both channel and topic settings,
and live global-setting events refresh every channel that inherits Zulip's
`enable_stream_desktop_notifications` value. Restart and queue replacement
therefore converge without treating the generic model `updated_at` value as
notification history.

Zulip topics are discovered from messages. The durable provider mapping table
is the local processed-topic cache: a missing topic queues an idempotent catalog
report, message delivery waits for the resulting Workspace topic mapping, and
then an idempotent `topic.upsert` precedes `message.create`.
If an assignment exists but its local stream or topic mapping was lost, the
bridge rematerializes that mapping from the durable assignment and immediately
wakes assignment-blocked journal events. Read-state events whose historical
message mappings point at a retired stream are omitted; message snapshots remain
the convergent read-state source for those retired projections.

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

Control-derived backfill jobs are reconciled by a profile-sized history pool.
Large profiles use eight workers so independent account/chat pages can be
fetched and converted concurrently. Idle history delivery fills the configured
Provider batch, up to 100 events, and yields for 10 milliseconds between
successful quanta instead of applying a fixed throughput delay. History catalog
and outbox writes use ten-message transactions while idle. Messages that invoke
the remote file-transfer path retain a dedicated single-message transaction.

Ready chat-catalog upserts are a strict dependency lane. The bridge drains those
reports before submitting live or historical message events, and orders chat
materialization ahead of ordinary observed status reports. This prevents an
initial or newly discovered chat from remaining unmaterialized while unrelated
message traffic continues through the data plane. A retryable catalog result
keeps the message gate closed during its control-plane backoff; `available_at`
delays only the next report submission, not the dependency itself.
The same dependency is evaluated inside the durable delivery selector, so a
catalog report committed after an earlier readiness probe is visible before any
newly committed message can be selected for Provider submission.

After the chat-materialization lane is clear, live operations and priority-0
Provider events take precedence over history. The history lane rechecks durable
live work after acquiring the shared Provider HTTP mutex; when live work is
waiting, history delivery and local conversion both fall back to one message per
transaction and at most one priority-2 delivery per second. A live event that
becomes durable after the check waits for no more than the already-started
bounded Provider request.

The 100-event batch removes the bridge-side ceiling for a 100 messages/second
history target on large profiles. The sustained end-to-end rate still depends on
Zulip history fetch latency and the Provider/backend/database round-trip.
Retryable Provider delivery backoff pauses both history submission and new page
discovery until the retry deadline, bounding the local outbox during an outage.
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
