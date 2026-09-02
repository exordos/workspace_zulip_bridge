# Provider HTTP runtime

The bridge data plane is the private Workspace Provider API defined by
`workspace_backend/docs/workspace_provider_api_v2.yaml`. Control-plane desired
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
POST /api/workspace-provider/v2/operations/actions/lease
```

The request uses a client-generated request UUID, a maximum batch size, and a
300-second lease. The same request UUID is retained across an ambiguous HTTP
transport failure. Each returned operation is durably bound to its
`provider_operation_uuid` and `lease_uuid` before execution.

Terminal outcomes are reported to:

```text
POST /api/workspace-provider/v2/operation-results
```

`applied` and `duplicate` acknowledge success. `conflict`, `rejected`, and
`not_found` become local manual-reconciliation evidence. `stale_lease` is
terminal for that lease; a later lease of the same immutable operation rebinds
the durable result. No response status is retried forever.

## Zulip to Workspace

Canonical resource commands are submitted to:

```text
POST /api/workspace-provider/v2/commands
```

The backend applies each command batch atomically. The bridge validates response
order, command keys, and `applied` status before committing its local outbox.
Transport errors and retryable responses release claimed submissions so the
idempotent event UUIDs can be retried. A record-scoped permanent rejection is isolated by
ordered batch bisection. Valid siblings still commit; the rejected record is
retained in the durable outbox with `submission_state = 'rejected'` and a safe
status code, and is not automatically resubmitted. Its operation idempotency is
terminalized immediately, so a rejected move or delete cannot become the
message context for later provider events. Unsent records are quarantined with
a dependency-specific safe code only when the rejected materialization was
causally earlier in the same producer lane, or was ordered first inside the
same prepared source event, and became terminal after the dependent record was
prepared. A later rejected operation therefore cannot invalidate an earlier
record. A rejected provisional message create also
retires its pending mapping. Before submission, a grouped read containing a
causally prior rejected message is narrowed to the still-materialized messages;
an empty read is omitted. The record identifiers and lane position stay stable,
while the prepared snapshot and digest are updated atomically. A rejected
content-only message edit does not invalidate a read because it does not change
message materialization.
Records prepared after the terminal boundary and delivery selectors both
ignore retained terminal reconciliation evidence. Once every sibling record
is terminal, the source journal event is quarantined as invalid so it cannot
remain a `delivering` predecessor and stall later causal lanes. The retained
outbox evidence can still be discarded and the source event replayed if its
Workspace assignment changes.

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
For shared messages, an account with only a provisional local mapping inherits
the committed Workspace target from another selected account in the same
verified Zulip realm, provider chat, and Workspace project. The provisional UUID
is retained as an alias, while replayed message updates, reactions, and reads use
the canonical target. A bounded selected-chat replay repairs provisional
reaction dependencies created before this convergence rule.
Backfill queue identities include a snapshot projection version. When the
message snapshot contract changes, its migration restarts every selected
history window under fresh operation identities so terminal deliveries from a
previous scan cannot suppress the new read/unread state.
If an assignment exists but its local stream or topic mapping was lost, the
bridge rematerializes that mapping from the durable assignment and immediately
wakes assignment-blocked journal events. Read-state events whose historical
message mappings point at a retired stream are omitted. A later full message
snapshot rematerializes the current projection and emits its read state as a
separate dependent command.

### ACK-confirmed history convergence

History and queue-recovery snapshots are compared with state stored in the
existing `provider_mappings.metadata` JSONB. No sidecar file, table, or second
source of truth is introduced. At startup the bridge builds account-scoped
in-memory indexes from those durable mappings; lookups include the Zulip
account UUID and are constant-time after the normal message-mapping lookup.
Control snapshot/reset changes rebuild the indexes.

A message snapshot is represented by a SHA-256 fingerprint of canonical JSON.
The canonical state contains chat type and provider chat identifier, sender ID,
the unmodified Zulip content, and edit timestamp when supplied. Channel state
also contains stream ID and normalized topic; direct-message state contains the
sorted unique participant IDs. Creation time, read flags, and reactions are not
part of the fingerprint. A committed mapping with the same fingerprint emits
no `message.create` or `message.update`; a significant change emits one update.
Backfill status alone never forces an update.

The SHA-256 fingerprint of the rendered Workspace payload is tracked alongside,
rather than folded into, that raw Zulip fingerprint. If a newest-first page
initially applies a reply or native Zulip link before its target is mapped, the
ACK records both the applied projection and that it still has a resolution
dependency. A later snapshot emits one update when the rendered target becomes
resolvable, then converges normally. Pending projections bypass the journal's
raw-fingerprint shortcut so they are always reconsidered. History-derived
message updates also forward the Zulip edit timestamp as the provider revision
so the backend freshness fence can order them against live edits.

Owner read state is stored per account, provider message ID, and Workspace
reader UUID, together with the Workspace message UUID that accepted the state.
History carries this exact boolean in the same `message.upsert` command and
persists both confirmation records in its ACK transaction. Live
`update_message_flags` events remain separate so a newer realtime flag cannot
be hidden by an older message freshness fence. Both read-to-unread and
unread-to-read transitions remain deliverable. Reactions are stored per
account, provider message ID, provider user ID, and collision-safe normalized
`reaction_type:emoji_code`. A confirmed-present add is omitted only while its
stored Workspace projection still matches the current message, stream, topic,
user, and emoji. This makes both read state and active reactions replay once if
Workspace replaces a provisional message UUID with the canonical target.

The final history page appends one `history.finalize` operation to the same
chat causal lane. Delivery selection holds that operation behind every earlier
uncommitted lane record. Its successful Workspace application schedules the
single exact stream unread snapshot and per-topic snapshots that publish the
completed import state.
Repeated removals of confirmed-absent reactions are still omitted; the opposite
transition is independent and remains deliverable.

The confirmed fingerprint or state token is written in the same PostgreSQL
transaction that accepts a successful Workspace result. The in-memory index is
updated only after that transaction commits. Transport failure, rejection, or
process interruption before the ACK therefore leaves the operation retryable.
Legacy mappings without confirmed metadata are deliberately replayed once,
including reaction mappings that predate explicit ACK state and token metadata.
This prefers a safe duplicate over losing a new state. Topic and message
dependencies remain ahead of read and reaction commands.

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

The durable provider journal preserves FIFO within each causal chat lane instead
of making one account-wide head block every unrelated chat. Message, topic,
single-channel subscription, and resolved message-derived events use their
provider chat as the lane. Account settings and identities use dedicated
account-scoped lanes. Multi-channel subscription changes, cross-channel message
moves, and unknown event shapes remain conservative account-wide barriers. A
deferred or retrying lane head can therefore hold only dependent work; another
ready lane for the same account can continue. The scheduler derives active lanes
from the durable journal, so cleanup of optional lane-fairness metadata cannot
hide pending work. It persists both account and lane fairness timestamps, and the
downstream Provider outbox retains its existing causal-lane sequencing and
idempotency boundaries.

Registration snapshots authoritatively reconcile the subscribed-channel
catalog before their synthetic notification events are recorded. Topic
notification events keep waiting while their channel remains in that durable
catalog and its Workspace assignment is still materializing. If a queue
replacement snapshot outlives a channel omitted by the latest registration,
the bridge finalizes the stale event instead of letting it occupy the channel
lane forever.

Control-derived backfill jobs are reconciled by a profile-sized history pool.
Large profiles use eight workers so independent account/chat pages can be
fetched and converted concurrently. Idle history delivery fills the configured
Provider batch, up to 100 events, and yields for 10 milliseconds between
successful quanta instead of applying a fixed throughput delay. History catalog
and outbox writes use ten-message transactions while idle. Messages that invoke
the remote file-transfer path retain a dedicated single-message transaction.

Ready chat-catalog upserts are a strict per-chat dependency lane. The bridge
orders chat materialization ahead of ordinary observed status reports and
withholds live or historical message events only when their assignment UUID
matches an incomplete catalog upsert. Independent chats and account-global
events continue through the data plane. A retryable catalog result keeps that
chat's message gate closed during its control-plane backoff; `available_at`
delays only the next report submission, not the dependency itself.
The same scoped dependency is evaluated inside the durable delivery selector,
so a matching catalog report committed after an earlier readiness probe is
visible before any newly committed message for that chat can be selected for
Provider submission.

For deliveries whose chat-materialization dependency is clear, live operations
and priority-0 Provider events take precedence over history. The history lane
rechecks durable live work after acquiring the shared Provider HTTP mutex; when
live work is waiting, history delivery and local conversion both fall back to
one message per transaction, while Provider submission uses at most one
priority-2 batch of up to ten events per second. A live event that becomes
durable after the check waits for no more than the already-started bounded
Provider request. If Provider rejects a history batch permanently, the bridge
rechecks and submits ready live events between the smaller isolation requests.

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

### Periodic operational statistics

The worker emits one INFO-level `bridge_interval_stats` JSON record every 60
seconds. It contains only aggregate, low-cardinality counters: confirmed-state
index sizes and rebuild duration; message/read/reaction cache lookups, hits,
misses, skips, and ACK updates; history and queue-catch-up pages, messages, and
provider fetch time; and backfill generated, enqueued, no-op, and suppressed
operations with per-second rates. Counters reset after each record while index
sizes remain gauges. The record contains no message text, account UUID, chat
identifier, or other per-entity label, so it is useful for capacity analysis
without turning routine imports into per-event log noise.

INFO logging is enabled only for the `workspace_zulip_bridge` logger namespace.
The root logger and the `httpx`/`httpcore` request stack remain at WARNING so
presigned file-transfer URLs and their query credentials never enter routine
request logs.

`RestAlchemyStore` obtains each transaction from the RestAlchemy PostgreSQL
engine and scopes it with `session_manager()`. The engine pool may reuse
connections, but a session never crosses a store-operation or worker-thread
boundary. Concurrent account long-poll workers also own separate adapter/client
instances, so Zulip client state does not cross worker-thread boundaries.
