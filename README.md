# Workspace Zulip Bridge v4

Protocol v4 is a deliberately small realtime message bridge. It inherits the
v3 enrollment and external-account control protocol, but it has no history,
bootstrap, snapshot, catalog, projection, or backfill path.

## Runtime

- The Workspace control client reuses the existing enrollment secret, CA,
  mTLS key/certificate, X25519 credential key, enrollment request, and IAM token
  files. It preserves the v3 enrollment and credential exchange protocol.
- v4 uses its own `desired-state-v4-cursor`. It ignores the legacy desired-state
  cursor so the first v4 start always loads a complete account snapshot into
  the empty v4 tables.
- Desired-state `external_account` resources are decrypted and written only to
  `v4_external_accounts`.
- One native thread per enabled external account authenticates to Zulip,
  registers an event queue with an empty initial-state request, and receives
  only message create, edit, and delete events through long polling.
- One separate native thread owns the Workspace `workspace.events.v1`
  WebSocket and forwards only native Workspace message events into already
  known external Zulip routes.
- The bridge applies a received Zulip message and its current user/stream/topic
  context atomically through `POST /v1/provider/v4/realtime`. Workspace remains
  authoritative and stores only normal entities with Zulip source markers.
- The bridge stores no message body or event payload. It retains only cursors
  and the minimum route/identifier links needed for edits, deletes, and echo
  suppression.
- Files, flags, reactions, history, projections, diffs, outboxes, and generic
  task processing are intentionally outside v4.

## Storage isolation

The v4 runtime creates and accesses exactly these tables:

- `workspace_zulip_bridge.v4_external_accounts`
- `workspace_zulip_bridge.v4_zulip_queues`
- `workspace_zulip_bridge.v4_workspace_event_cursors`
- `workspace_zulip_bridge.v4_stream_links`
- `workspace_zulip_bridge.v4_topic_links`
- `workspace_zulip_bridge.v4_message_links`

Legacy tables are intentionally left in the database. The v4 schema contains
no `ALTER TABLE` or `DROP TABLE`, and runtime code never reads or updates those
tables. `schema.sql` and `schema_upgrades.sql` remain as untouched legacy
definitions; `database.py` executes only `schema_v4.sql`.

## Development

Python 3.12+ and PostgreSQL 15+ are required.

```bash
tox -e develop
tox -e py,ruff,mypy
```

Optional PostgreSQL checks can use a disposable database through
`WZB_TEST_DATABASE_DSN`.

## Configuration

The deployment keeps the existing Workspace secret paths so an installed v3
instance can transition without re-enrollment. The main settings are documented
in `etc/workspace-zulip-bridge.env.example`.
