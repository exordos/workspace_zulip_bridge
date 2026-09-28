# Personal notification state

Notification snapshots are observations of current state. A topic preference's
provider `last_updated` can be much older than the bridge's local row creation
or migration time. Compare a complete snapshot with the time immediately before
its source request, not only with each preference's historical change time.

Topic realtime events retain the provider's whole-second ordering. An accepted
unchanged event advances its version and local observation time, without queuing
another Workspace payload. Snapshot writes check both the source version and the
local write time so a realtime update committed after snapshot observation began
wins, even when the source event was delayed. Per-connection transactions serialize
snapshot and realtime topic writes. A realtime event in the snapshot's observation
second is ambiguous: it might be an older retained-queue event or a genuinely newer
choice. It cannot mutate the cache. Instead it durably invalidates the settings
generation, and maintenance requests a settings-only snapshot in a later second.
This also handles absent bindings. Retrying the same bootstrap snapshot cannot
clear the request. A later snapshot establishes the source truth, after which
repeated old events are strictly older and cannot create a refresh loop.

The durable `zulip_connections.notification_snapshot_at` field also fences delayed
events for omitted preferences and topics not materialized yet. Omitted explicit
preferences return to `default`; generated default rows need no mass rewrite.
Case variants and active aliases resolve to one binding, and the latest explicit
entry is applied once. Fresh generated default rows use the existing `epoch`
source timestamp and never claim to be a realtime observation.

Catalog reads carry `catalog_observed_at` captured before requesting subscriptions.
Stream binding updates, unchanged-state observations, and removals respect that
timestamp. A delayed catalog cannot overwrite a newer binding observation. If a
global desktop-notification event races a snapshot/catalog, inherited channel
modes are recalculated from the newer setting; explicit channel preferences and
muted channels keep their own modes.

Global `user_settings` and subscription-property events do not include source
versions. A retained queue can therefore deliver one after a newer snapshot.
Every global notification event, including unchanged values, and every update of
a subscription notification property requests authoritative settings reconciliation.
Global hints may provisionally update the cached default; they never count as a
completed source observation. Notification-only subscription updates read the source
settings instead of reloading the catalog/history. Structural subscription changes
continue using catalog reconciliation.

The durable `notification_refresh_requested_at` watermark prevents an in-flight
snapshot from losing a request received after observation began. A settings-only
snapshot predating the request is rejected. Bootstrap snapshot completion also
checks that watermark before acknowledging generation 2. Event bursts coalesce
into one pending generation, and ingestion deduplication prevents an already
processed event ID from re-triggering reconciliation. A fresh snapshot covers the
request and leaves no recurring work when source events stop.

## Bounded repair

`NOTIFICATION_SETTINGS_GENERATION = 2` invalidates the old personal-settings
snapshot only. On a normal worker restart, connections with an older generation
fetch a registration snapshot and subscriptions, apply settings transactionally,
and persist generation 2. An active worker checks for invalidated generations
before scheduling or starting the next historical stage; it performs the same
settings-only refresh without rebuilding the catalog or reloading history. This
does not reset the retained event queue, message history, message IDs, or catalog/history import progress. A current-generation
connection skips this repair; normal queue registration still imports its fresh
notification snapshot.

For a specifically identified connection that needs another repair, an operator
may lower only its `notification_settings_generation` to 1. The active worker's
maintenance loop notices the request; a normal worker restart also recovers it.
Scope the operation to the selected connection, retain its queue/cursor, and verify source preferences, bridge rows,
outbox/diffs and the Workspace projection afterward. Do not clear the database,
reset every connection, or reimport message history to repair preferences.

Temporary queues created only to obtain registration snapshots are immediately
removed through the supported `DELETE /api/v1/events` API, with the form field
`queue_id`. The retained realtime queue is never added to the cleanup set. Cleanup
happens before subsequent subscription/attachment/database work can fail. An
already-removed queue is accepted; other cleanup failures are retried before
allocating another temporary queue, bounding pending cleanup to one per worker.
If the process exits during failed cleanup, the source's configured idle-queue
expiry remains the fallback.

Protocol references: the owning Zulip repository's
`zerver/openapi/zulip.yaml` operation `delete-queue`,
`zproject/tornado_urls.py`, and `zerver/tornado/views.py:cleanup_event_queue`.
Subscription notification event shape is produced by
`zerver/actions/streams.py:do_change_subscription_property`.

All mutating snapshot/settings entry points lock the connection owner before
stream/topic bindings. The snapshot marker is written while that owner lock is
still held; readiness checks do not acquire binding locks. Do not introduce a
bindings-to-owner lock path when integrating other mutation handlers.

This ordering assumes the bridge and source clocks are synchronized. A test of
cache state alone does not prove Workspace delivery: acceptance must compare all
three states for a representative account, including a changed mode, removal to
default, realtime updates during a snapshot, and a restarted worker.
