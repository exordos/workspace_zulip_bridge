# Project instructions

- Repository content is written in English.
- The development environment is `.tox/develop`; create it with `tox -e develop`.
- Keep provider credentials, enrollment secrets, presigned URLs, message bodies,
  and private infrastructure details out of logs, tests, and repository files.
- The Workspace Provider API, control API, and file API contracts are owned by
  `workspace_backend/docs`; do not silently invent incompatible fields.
- Use the official `zulip` Python client boundary. Tests use fakes and must not
  call a live Zulip or Workspace installation.
- Messages and operations use the private Provider HTTP API. The production
  runtime must not add an IMAP, SMTP, Maildir, or mail-server dependency.
- File bytes use the private file API; Provider event payloads carry only
  canonical resource metadata and references.
- PostgreSQL is persistent bridge operational state. Workspace backend owns
  Workspace resources and transaction boundaries.
- Do not create a remote repository, commit, push, or deploy without an explicit
  user request.
