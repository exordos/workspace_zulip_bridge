# History batches

Configured history jobs now capture Zulip messages into the bridge PostgreSQL
table `zulip_history_batches`. They do not create Workspace entities, transfer
files, enqueue Provider events, or publish unread snapshots. Live delivery and
queue-loss catch-up retain their existing conversion, outbox, and ACK behavior.
A separate publisher sends frozen completed batches to the backend history API.
The backend prepares S3 objects and applies short SQL parts; see the backend
`docs/history_import.md` contract and the shared golden fixtures.

## Stored format

The minimal payload below uses JSONC comments for documentation. Stored JSON
contains no comments; the example hashes are placeholders.

```jsonc
{
  "from_id": 1, // First Zulip message ID in the inclusive range.
  "to_id": 5000, // Last ID in that range; this is not a message count.
  "users": [ // Entire directory returned by Zulip, not just message participants.
    {"id": 17, "name": "Example user"} // Source user ID and display name.
  ],
  "messages": [ // Messages observed through connected, selected accounts/chats.
    {
      "id": 205, // Stable Zulip message ID within its verified realm.
      "sender_id": 17, // Author's Zulip user ID.
      "channel_id": 42, // Zulip channel ID.
      "channel_name": "Example channel", // Channel display name.
      "topic": "Import", // Zulip topic text.
      "sent_at": 1788638060, // Original creation time, Unix seconds.
      "content": "Synthetic example", // Original Markdown, apply_markdown=false.
      "reactions": [ // Complete reaction set from this message observation.
        {
          "user_id": 17, // User who reacted.
          "reaction_type": "unicode_emoji", // Zulip emoji namespace.
          "emoji_code": "1f44d", // Emoji identity within that namespace.
          "emoji_name": "+1" // Emoji display name.
        }
      ],
      "access": [ // Positive observations of which users could fetch the message.
        {"user_id": 17, "read": true, "starred": false} // Independent user flags.
      ],
      "hash": "<sha256>" // Hash of every message field except this hash.
    }
  ],
  "hash": "<sha256>" // Hash of the payload, including message hashes, except itself.
}
```

For direct messages, `channel_id`, `channel_name`, and `topic` are null and
`recipient_ids` contains the sorted Zulip participant IDs. This is the only
additional field needed to preserve direct and group-direct conversations.

## Collection and persistence

Each selected account/chat job scans newest ranges first: `10001..15000`,
`5001..10000`, `1..5000`. The official Zulip client fetches pages of at most
1000 messages within each range using numeric anchors. After a range, a
one-message backward probe with the same channel/DM narrow finds the actual
older visible message ID. The next range is aligned around that ID; the bridge
does not walk every empty 5000-ID interval back to message 1. An empty range
does not end the scan if the probe finds an older message. If it finds none,
Zulip's `found_oldest` flag confirms completion. Missing or inaccessible IDs
reduce the message count. Existing
`new`, `7_days`, `30_days`, `90_days`, and `all` history-depth settings and their saved cutoffs
remain in effect. `new` performs no historical capture.

Every stored JSON batch includes the full directory returned by `GET /users`,
including bots and inactive users, without filtering to referenced IDs.
Short-lived adapters share an account/generation-scoped directory cache for
up to five minutes. Its LRU limits are 128 account entries and 64,000 total
cached user records, including at most 1,024 historical profiles per account.
An individually larger directory is returned in full for the current batch
but is not retained in the cache. These limits bound retained records, not
the bytes of a provider response or the current batch. When many large account
directories exceed the budget, eviction causes extra directory requests.
Referenced historical identities missing from that list are fetched individually.
A definitive missing-user response retains the stable ID with the adapter's
explicit unavailable profile. Capture revision 2 rebuilds older cached batches
once to include these identities. Zulip
still controls which profiles the authenticated account can see. Catalogs from
different observations are combined by source user ID.

Rows are keyed by `(project_uuid, provider_realm_uuid, from_id)`. These scope
fields come from the current assignment and verified account cursor, outside
the JSON. Message identity is the source ID in that scope, never its hash.
Messages are combined by ID; each user's observation replaces that user's
`read` and `starred` values while retaining other users' flags. New observations
replace common content and the complete reaction set. Within an unchanged configuration, missing observations do not delete messages
or revoke access. Directory membership does not grant
message access; unlinked accounts have no inferred flags.

The row accumulates observations as jobs progress. It is not a globally atomic
Zulip snapshot or proof of current access for every server user. Job states
record collection progress separately. Publication waits until every selected
account/chat in the scope has completed capture. Attachments remain source
references in the stored JSON; the publisher transfers requested bytes separately.

Messages, users, and access entries are sorted by source ID; reactions by
`(user_id, reaction_type, emoji_code)`. SHA-256 covers compact UTF-8 JSON with
sorted object keys and unchanged Unicode text. Numbers are safe integers.
Comments, whitespace, and database metadata are excluded.

Capture reads the prior range without write locks, then merges observations,
applies the assembled directory once, hashes and serializes JSON outside any
SQL transaction. A short transaction rechecks desired state, scope generation,
directory progress and the exact capture lease before comparing the stored hash
and writing the prepared body together with the job checkpoint. A competing
capture or directory change causes a yield without advancing the checkpoint;
only that capture's lease can be released. The next quantum prepares against
the new body. Calling capture persistence from an outer transaction is rejected.
Repeating a range updates one row; it does not append a copy.

Only the prepared body INSERT/UPDATE receives a size-dependent statement budget:
`min(5000, 500 + ceil(encoded_bytes / 16384))` milliseconds. ASCII JSON encoding
makes the prepared string length equal to the transmitted byte length; encoding
and budget calculation happen before taking locks. Other SQL keeps its 500 ms
statement budget, and lock acquisition remains limited to 50 ms.
A body-write timeout rolls back the body and checkpoint, then records
`history_capture_write_timeout`, a persistent retry count, health and durable
scope reporting work in one short, generation- and lease-checked transaction.
Reporting enqueues one account per quantum, independently of capture retries.
Rejected or stale reports retain their cursor and retry after 30 seconds;
callback exceptions roll back the enqueue and cursor, then defer that work.
They do not roll back capture bookkeeping or stop the live/history lanes.
A new timeout rearms completed reporting work without resetting an unfinished
cursor. Directory refresh pauses reporting at the same generation, while a
capture-changing scope rebuild retires old work. Successful capture suppresses
pending timeout reports when no current source capture has an outstanding
timeout marker. Later provider errors and lease delays do not clear that marker.
Retries wait
5, 10, 20, 40, 80, 160 and 300 seconds; the eighth timeout marks capture failed.
Previous provider failures, CAS conflicts and lock contention do not increment
this timeout streak. A successful save clears it and its health entry. Restart
preserves the counter and backoff; a changed scope or reissued lease cannot
receive an obsolete timeout report. If bookkeeping is itself contended, the
capture lease remains for expiry instead of scheduling an immediate retry.
An existing terminal publication report takes precedence over a capture timeout
in the same scope generation, so capture recovery cannot discard that report.

Migration `0039` creates the table and restarts existing non-cancelled history
jobs once, retaining their configured depth. Already queued legacy deliveries
remain eligible for normal delivery because their format is shared with
queue-loss recovery. Migration `0041` cancels obsolete unread finalizers while
preserving message deliveries and accepted results. The old finalizer,
its dependency fences, and the unused initial-import readiness gate are removed.
Workspace delivery retries no longer fail or restart local batch jobs.
Rolling application code back preserves captured rows and does not revive
cancelled finalizers.


Migration `0043` scopes the configuration tracked since `0040` to each
`(project_uuid, provider_realm_uuid)`. Adding/removing connected accounts,
changing an account generation, or changing selected chats, projects, or history
depth rebuilds affected scopes from their current sources. Other scopes retain
their captures and publication receipts. Disabled accounts contribute no
observations. Assignment revisions that only update catalog metadata preserve
message capture. During a scope rebuild its rows may be temporarily absent or
partial; live mappings and delivery queues remain intact. Clearing job leases
and scope batch rows together prevents an older fetch from restoring removed
access. Unchanged control polls preserve completed batches.

Directory changes take a separate path. Relevant `realm_user` events (add,
remove, or a changed `full_name`) and queue re-registration mark that realm's
scopes for directory refresh. A durable revision also retains invalidations
received before the first scope exists. Each refresh quantum fetches one
account directory or rewrites one existing batch's user catalog and batch hash.
It preserves messages, personal flags, reactions, and capture job checkpoints;
it does not fetch message history again or reset completed capture jobs.
Referenced historical identities absent from current directories remain in
the catalog. Failed directory requests back off so other scopes can progress.
Directory writes use a 50 ms lock wait and a 500 ms statement budget. Only the
prepared catalog or batch JSON write receives the same size-based budget as
capture writes, capped at 5 seconds; a timeout rolls back the directory cursor.
Migration 0047 persists the directory write attempt count. Statement timeouts
retry after 5, 10, 20, 40, 80, 160 and 300 seconds. The eighth failure stops that
revision and durably reports `history_directory_write_timeout`, even while
publication is paused for the directory. Lock contention, serialization failures
and deadlocks defer one second and mark degraded health without using an attempt.
Successful directory progress clears the attempts and transient health. A new
realm directory revision or a capture-changing rebuild starts a fresh pass;
metadata-only assignment revisions preserve stopped passes and reporting cursors.

Publication pauses until the directory pass completes. Generation/cursor checks
reject stale preparation, and concurrent message merges cannot be overwritten
by an older prepared catalog. Captures saved after the directory is assembled
also use that current catalog, including new ranges behind the refresh cursor.

## Publishing completed captures

Migration `0042` adds durable publication receipts and a monotonic configuration
generation. It invalidates existing capture fingerprints once so source data is
recollected after the backend tombstone migration. Install the backend first.
The publisher probes `GET /v1/history-imports`; a backend without that capability
continues to work with local capture only.

The private mTLS envelope contains schema version 1, the stored batch, project,
verified provider realm, configuration generation and current account/chat
generations. Publication verifies the current fingerprint, verified owner and
complete selected scope. A source metadata revision creates a fresh receipt
even if the content hash is unchanged. Reconfiguration replaces local captures
and invalidates old publication leases; removing a source never deletes
canonical Workspace messages.

File descriptors must contain a valid string UUID, a relative upload path and
an observer list. Validate the complete descriptor before provider access;
malformed descriptors stop publication with durable `invalid_history_file_request`
reporting (invalid paths and observers retain their specific error codes).
File requests must name observers in the current frozen source set. Equivalent
UUID spellings are normalized before authorization and adapter lookup. Empty,
invalid, or foreign account lists stop publication with a durable
`history_file_account_not_assigned` health and account report.

An import scope may contain at most 512 selected account/chat sources. The
bridge detects an oversized scope before posting its batch, stores a terminal
`history_source_limit_exceeded` failure, and reports degraded publication health
and account status. The maximum source envelope remains below 100 KiB while the
private import endpoint allows a 50 MiB request. This bounds the source list; it
is not a limit on total messages. Other scopes remain eligible for publication.

A dedicated history lane publishes one bounded request/file operation at a
time and persists the server job UUID/status. It polls missing attachment
requests, downloads bytes through current allowed accounts and uploads them
without putting base64 content in message JSON. A definitive 404/410 or a
rejected redirect means unavailable; authentication, network and other storage
failures remain retryable.
For a shared attachment, the publisher tries each distinct authorized observer
before deferring the transfer. A successful alternate observer supplies the
bytes; any unresolved transient or authentication error prevents a later
missing-file response from incorrectly marking the file unavailable.
The realtime event journal, conversion and Provider ACK path are unchanged.

The backend's first version fills missing messages and user states. It preserves
newer live content, reactions and existing read/starred choices; hashes are
observation identities, not permission to overwrite live state.

After the server has committed a batch, the publisher consumes routing receipts
in pages of at most 500 identities/messages per selected account. These pages
contain canonical UUIDs and routing metadata without duplicating message bodies.
Mappings and their local page cursor are persisted together, preserving live
rows and tombstones. The batch is marked delivered only after all receipt pages
finish, so replies, reactions and subsequent live events retain normal routing.

History publication renews its lease across registration, receipt processing,
and slow file transfers. Definitive
missing files and transfers forbidden by the current size policy are reported
as unavailable, allowing the messages to complete with a visible unavailable
attachment marker. Network failures retain their retry behavior. Mapping
receipt writes revalidate the complete source fingerprint while briefly
locking the bridge's desired-resource table against concurrent configuration
changes; this lock never spans a provider or Workspace request.

Terminal server failures and permanent batch admission errors are stored as
`import_status = 'failed'` with a bounded safe error code and reported through
publication health and account status. They are not polled indefinitely.
Migration 0044 stores one durable reporting cursor per failed scope in the same
transaction as its terminal batch state and health. Each publication quantum
queues at most one generation-checked account report into the observed outbox,
atomically advancing its cursor, and also permits ordinary batch publication.
Reporting resumes after restart, does not expand the rejected envelope, and
discards obsolete work after scope or account configuration changes. The migration
also schedules reports for existing failed batches from the previous schema.
Disabling or emergency-suspending the provider pauses reporting without losing
its cursor; resuming checks scope and account generations before continuing.
A configuration rebuild or directory-only refresh makes affected batches
eligible for publication again. Successful routing receipt writes clear their
lease; an authority check that exits early also releases the lease for retry.

Migration 0045 records a target directory revision and its acknowledgement for
each scope, independently of capture configuration generations. A completed
revision does not force another directory request after an unrelated rebuild,
even while another project in the same realm is still refreshing. The final
acknowledgements serialize briefly against configuration and invalidation, and
remove the realm's dirty revision only after every scope has acknowledged it.
Events before bootstrap and events arriving during a fetch or rewrite remain
pending. Captures arriving after the sweep already use the assembled directory.

Migration 0046 adds `capture_timeout_pending` to each capture job, independently
of the latest provider error and retry count. Timeout bookkeeping sets it;
successful persistence clears only that job's marker, and a capture-changing
rebuild clears affected markers and retires the old reporting work. Metadata-only
assignment revisions and directory refreshes preserve the jobs and move timeout
reporting to the new publication generation without resetting its account cursor.
This also preserves reports for captures that stopped after eight timeouts.
Capture reporting locks the scope, then one outstanding job marker in stable
account/chat order, then the report cursor. The job's shared lock prevents a
successful save between marker validation and enqueue; the 50 ms lock budget
allows contention to yield without delaying other captures or live delivery.
The migration adopts existing timeout job/health records and current-generation
scope reports, preserving completed or cancelled captures. No new table is added.

After trying every allowed observer, malformed provider file URLs or lengths
become terminal publication failures with durable health/reporting work when
no observer has a recoverable failure. Credential, authentication and network
errors retain the receipt for retry and can recover after credentials change;
a successful alternative observer can supply the file immediately.
The live and queue-catchup message resolver renders a permanently invalid upload
URL as an unavailable attachment, preserving the surrounding message text; only
live delivery also falls back for a provider-origin 400, 403, 404, 410 or 422
response. Authentication, network
and retryable redirected-storage failures retain their existing retry behavior;
a redirected 404/410 becomes unavailable. Both history and realtime downloads
validate relative upload URLs before authenticated
HTTP: dot segments, encoded separators, control characters and ambiguous nested
escapes are rejected. Ordinary Unicode names, spaces and embedded dots remain
supported. A download may follow one public HTTP(S) storage redirect without
forwarding the Zulip credentials; HTTPS downgrades, non-public cross-origin
targets and a second redirect are rejected. The validated destination address
is used for direct and proxy connections. A proxy-only target that cannot be
validated through local DNS becomes unavailable instead of retrying the delivery
lane indefinitely. Before uploading downloaded bytes, history rechecks that the
current effective file limit is positive and covers the size; zero disables
transfer even for an empty file and uses the unavailable path.
