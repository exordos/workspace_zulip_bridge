# Project instructions

- Repository content is written in English.
- Protocol v4 is realtime-message-only. Synchronize only message create, edit,
  and delete events received from the active Workspace WebSocket or Zulip event
  queue. Do not add snapshots, history, backfill, catalogs, projections, files,
  flags, reactions, or generic task execution.
- Never read, update, migrate, truncate, or drop legacy bridge tables. New
  persistent tables must use the `v4_` prefix. `database.py` may execute only
  `schema_v4.sql`; legacy schema files remain inert reference material.
- Reuse the existing Workspace enrollment and token file paths. Do not rotate or
  replace valid CA, mTLS, X25519, enrollment-request, access-token, or refresh-
  token material merely because the runtime version changed.
- Keep Workspace enrollment, desired-state credential exchange, token renewal,
  certificate renewal, and external-account convergence compatible with the
  established control protocol.
- Run the Workspace event WebSocket in one dedicated native thread. Validate
  routing, apply supported message events, and advance the v4 cursor only after
  delivery succeeds. Ignore events whose source is already Zulip.
- Run one native long-polling thread for each enabled external Zulip account.
  Request only message, update-message, and delete-message events with no
  initial state, and advance its v4 cursor only after delivery succeeds.
- Persist only cursors, external account connections, and the minimum stable
  stream/topic/message identifier mappings needed for realtime routing. Never
  persist message bodies or event payloads.
- Keep credentials, tokens, event bodies, private URLs, and infrastructure
  details out of logs, tests, fixtures, and repository files. Never log API keys.
- Keep the runtime small: Python, `asyncio`, `asyncpg`, PostgreSQL, and native
  worker threads. Add frameworks only after a measured need.
- The development environment is `.tox/develop`; create it with
  `tox -e develop`.
- Run `tox -e py,ruff,mypy` before handing off code changes.
- Build and deploy the element only through the `exordos` CLI.
- Do not commit, push, publish, or deploy without an explicit user request.
