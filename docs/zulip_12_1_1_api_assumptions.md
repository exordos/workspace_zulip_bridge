# Zulip 12.1.1 API assumptions

Integration target: exact `exordos/zulip` branch `dev-12.1.1`, inspected at
commit `3dc1c9498d53c40c3ade350c6857d0b46e894d4b`.

The adapter uses official Python client `zulip` 0.9.1 semantics demonstrated by
that branch's `zerver/openapi/python_examples.py` and generated OpenAPI:

- `client.register(event_types=..., apply_markdown=False,
  client_capabilities=...)` returns `queue_id` and `last_event_id`;
- `client.get_events(queue_id=..., last_event_id=..., dont_block=False)` returns
  increasing, not necessarily consecutive, event IDs;
- channel messages use `send_message` with `type=stream`, channel in `to`,
  `topic`, and `content`;
- personal and group DMs use `type=private` and recipient user IDs in `to`;
- edits use `update_message` with `message_id`, `content`, and optional
  `prev_content_sha256`;
- deletion uses `delete_message(message_id)`;
- read state uses `update_message_flags` with message IDs, `op=add/remove`, and
  `flag=read`;
- files use `upload_file` with an opened binary file object and the returned URL
  is embedded only after Workspace file-plane authorization/copy.

The bridge requests `notification_settings_null`, `bulk_message_deletion`, and
`empty_topic_name` client capabilities. It accepts `null` channel notification
settings as an instruction to inherit the user's global notification settings.
It does not assume event IDs are gapless and persists each queue's last
acknowledged event ID on the element's persistent PostgreSQL disk.

Zulip does not provide a general idempotency key for every mutation. For
outgoing messages the bridge registers an event queue and persists `queue_id`
plus `local_id=operation_uuid` before the provider call. Zulip echoes that local
ID in the queue's message event, but does not deduplicate sends by it. An
ambiguous provider outcome therefore enters explicit `uncertain` state. The
bridge first accepts a matching local-echo event. It also performs delayed
server-side reconciliation through `GET /messages` with a narrow containing
the exact conversation and current sender, `apply_markdown=false`, newest-first
results, exact raw Markdown, and a bounded timestamp window. Checks are
scheduled at 5, 15, and 30 seconds. One or more exact matches are equivalent and
commit without resending; the selected provider ID is the closest timestamp to
the first attempt, then the lowest numeric message ID. No match after all three
checks permits exactly one automatic resend. Provider unavailability or a
second ambiguous outcome requires manual reconciliation. Every check and
candidate provider message ID is persisted as sanitized evidence.
