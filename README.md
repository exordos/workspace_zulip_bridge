# Workspace Zulip Bridge v4

Protocol v4 is currently a connection-only foundation. It deliberately does
not synchronize Workspace and Zulip entities yet.

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
  registers an event queue, keeps it alive with long polling, and advances only
  the queue cursor. Event payloads are discarded.
- One separate native thread owns the Workspace `workspace.events.v1`
  WebSocket. It validates event routing and advances only the epoch cursor.
  Event payloads are discarded.
- No stream, topic, user, message, file, flag, reaction, history, projection,
  diff, outbox, or task processing exists in this stage.

## Storage isolation

The v4 runtime creates and accesses exactly these tables:

- `workspace_zulip_bridge.v4_external_accounts`
- `workspace_zulip_bridge.v4_zulip_queues`
- `workspace_zulip_bridge.v4_workspace_event_cursors`

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
