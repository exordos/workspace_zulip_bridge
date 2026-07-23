# Workspace Zulip Bridge

Standalone implementation checkout for the independently deployable
Workspace-Zulip bridge element.

The service implements the contracts maintained by the sibling
`workspace_backend` repository:

- `docs/zulip_bridge_v1_product_and_api.md`;
- `docs/zulip_bridge_control_api_v1.yaml`;
- `docs/workspace_provider_api_v1.yaml`;
- `docs/zulip_bridge_file_api_v1.yaml`.

Workspace messages and operations cross the private Provider HTTP API. The
bridge has no IMAP, SMTP, Maildir, or Workspace mail-server dependency.
The bridge VM runs its own PostgreSQL instance on the element's persistent
disk. Its local `workspace_zulip_bridge` database stores the durable scheduler,
leases, idempotency records, provider cursors, mappings, and outboxes. The
Workspace backend remains authoritative for Workspace resources and applies
Provider events in its own database transactions.

## Development

```bash
tox -e develop
.tox/develop/bin/pytest
.tox/develop/bin/ruff check .
```

Network-facing tests use fake control, Provider, file, and Zulip endpoints.

## Continuous integration

GitHub Actions runs Ruff and the Python 3.11 test suite through `tox` with the
`tox-uv` plugin. The test job provides a disposable PostgreSQL service and sets
`WORKSPACE_BRIDGE_TEST_POSTGRES_DSN`, so the PostgreSQL integration tests run
instead of being skipped.

The element workflow builds on a runner labelled `self-hosted` and `vm` with
the pinned Exordos CLI release. Every eligible build, including pull-request
builds, publishes its immutable output to the configured Exordos repository.
Repository administrators must configure `PUSH_CFG` as the base64-encoded
contents of an `exordos.push.yaml` file. The workflow marks tag builds as
`latest`; non-tag builds are published without changing `latest`.

A manual `production_release` profile provides the immutable bridge artifact
used by the Workspace PostgreSQL-canonical cutover. It refuses repository
version collisions and records the exact build and publication evidence in a
private runner-local archive configured by the
`WORKSPACE_BRIDGE_RELEASE_EVIDENCE_DIR` repository secret. See
[Production bridge release](docs/production_release_workflow.md).

## Runtime

```bash
/opt/workspace-zulip-bridge-venv/bin/workspace-zulip-bridge \
  --config /etc/workspace-zulip-bridge/bridge.conf
/opt/workspace-zulip-bridge-venv/bin/workspace-zulip-bridge-healthcheck \
  --config /etc/workspace-zulip-bridge/bridge.conf
```

The image installs the application into the isolated
`/opt/workspace-zulip-bridge-venv` virtual environment and installs two
services:

- `workspace-zulip-bridge-bootstrap.service` initializes the persistent data
  directory and applies versioned RestAlchemy migrations;
- `workspace-zulip-bridge.service` runs control polling, heartbeat, Provider
  HTTP operation leasing and event/result delivery, Zulip event ingestion, and
  the fair live/retry/backfill scheduler.

The worker also invokes the serialized bootstrap entrypoint as a `before` hook.
Repeated bootstrap invocations preserve the persistent PostgreSQL data and wait
for its local socket before starting the worker. Applied schema revisions are
tracked in `ra_migrations`; the bootstrap applies only unapplied dependency
steps through `ra-apply-migration`. Runtime transactions use the RestAlchemy
PostgreSQL engine and `session_manager()`; the bridge has no direct `psycopg`
storage layer.

## Current implementation boundary

The current implementation provides:

- mTLS enrollment, control polling, heartbeat, and certificate renewal;
- mandatory fail-closed Provider HTTP operation leasing, per-item result
  reporting, and atomic inbound event batches;
- durable exact lease binding and idempotent retry state in PostgreSQL;
- official Zulip client calls, event queues, newest-first backfill, and
  ambiguous-send reconciliation;
- durable queue registration before discovery, names-only initial channel
  discovery, and an authoritative selected-channel participant gate before
  live or historical messages are projected;
- owner-scoped projections and stable identity/chat/topic/message mappings;
- private file-plane transfers with short-lived URLs;
- bounded concurrent per-account Zulip polling (16 workers by default) with a
  fresh adapter/client per worker call;
- non-long-polling queue reads with bridge-owned retry/backoff, so an official
  client request cannot retry forever inside the live delivery loop;
- extended idle queue lifetimes on compatible Zulip servers, preserving durable
  queue cursors across quiet periods without ten-minute recovery churn;
- live/retry/backfill scheduling with hard live priority, bounded history
  delivery batches, and a dedicated single-worker history lane, so slow Zulip
  history or recovery I/O cannot block new queue events;
- exact owner read/unread projection from both Zulip message snapshots and live
  flag events, ordered after the corresponding Workspace message projection;
- automatic removal of queue-recovery jobs when their chats are deselected, so
  stale recovery state cannot keep an account in backfill forever;
- stable history cutoffs and reconciliation checkpoints across unchanged
  control-plane polls;
- durable backfill retry state with exponential full-jitter deferral for
  retryable provider failures; non-retryable failures terminate only the
  affected account/chat backfill job and produce a degraded observed report.

The Provider API request UUID is preserved across ambiguous lease transport
failures. Each leased operation retains the exact Provider operation and lease
UUID in durable state. Provider result responses are terminally recorded so
conflict, rejection, not-found, and stale-lease responses cannot create an
unbounded resend loop. A renewed lease can safely rebind the same immutable
operation. Provider event batches are released back to the outbox on transport
or response validation failure and are committed locally only after the backend
accepts the full atomic batch.

The 30-day bridge client leaf is renewed with a locally generated replacement
key during the final seven days of validity. A heartbeat can force immediate
renewal during a control-CA migration. Control, Provider, and file clients
reload the enrolled leaf and dual-trust bundle without restarting the worker.
Zulip TLS uses the system trust store plus administrator-managed custom CAs;
provider disable and emergency suspension gates are fail-closed.

The remaining release gates are explicit realm-policy enablement, live
Provider/file/Zulip conformance in both directions, live certificate rotation,
recovery and target-load scenarios, and full visible UI acceptance. Unit and
fake-endpoint tests do not claim those real-system scenarios.

See [Provider HTTP runtime](docs/provider_http_runtime.md) for the exact data
plane routes and failure semantics.

## License

Licensed under the [Apache License 2.0](LICENSE).
