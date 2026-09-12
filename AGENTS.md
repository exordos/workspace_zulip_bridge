# Project instructions

- Repository content is written in English.
- This repository is a clean implementation. Do not copy code, database models,
  migrations, or runtime conventions from earlier bridge implementations.
- Keep the runtime small: Python, `asyncio`, `asyncpg`, and PostgreSQL. Add a
  framework only after a measured need is demonstrated.
- Runtime latency is the primary engineering constraint. Prefer bounded
  concurrency, batched PostgreSQL operations, and stable connection pools.
- The current persistent model contains stable `zulip_users`, canonical
  `zulip_chats`, candidate `zulip_chat_users`, canonical `zulip_topics` and
  `zulip_messages`, append-only `zulip_events`, and empty destination mirrors
  `workspace_chats`, `workspace_topics`, and `workspace_messages`. Name database
  tables in the plural. Do not write to the Workspace mirrors until the
  Workspace API adapter contract is implemented.
- User lifecycle states are unique and ordered: `init`, `streaming`, `filling`,
  `scheduling`, `backfilling`, `active`.
- Select one source per chat before loading history. Rank candidates by Zulip
  role (owner, admin, moderator, member, guest), then stable UUID. Do not scan
  message history to rank candidates. Only the selected source may mutate that
  chat's messages.
- One native long-polling thread is owned by each row in `zulip_users`.
  Threads only persist events and advance their cursor; the asynchronous event
  processor applies chat, topic, and message mutations after a supplier gate.
  Heartbeats advance the cursor but are not persisted as domain events.
- The event processor deletes terminal `applied`, `skipped`, and `failed`
  events after the configured 24-hour default retention. Never expire pending
  or in-flight work. Keep deletes bounded and index-supported.
- Do not invent Workspace or Zulip payload contracts. Verify them in the owning
  repositories and document the source before implementing an adapter.
- Keep credentials, tokens, message bodies, private URLs, and infrastructure
  details out of logs, tests, fixtures, and repository files. In particular,
  never log or expose the `api_key` column.
- The development environment is `.tox/develop`; create it with
  `tox -e develop`.
- Run `tox -e py,ruff,mypy` before handing off code changes.
- Build and deploy the element only through the `exordos` CLI.
- Do not commit, push, publish, or deploy without an explicit user request.
