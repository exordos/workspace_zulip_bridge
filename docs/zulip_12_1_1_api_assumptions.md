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
- reactions use `add_reaction` and `remove_reaction` with the provider message
  ID and emoji name; live `reaction` events carry `op=add/remove`,
  `message_id`, `user_id`, `emoji_name`, `emoji_code`, and `reaction_type`;
  Unicode reactions use the normalized `reaction_type`/`emoji_code` pair as
  their stable provider identity and project the decoded Unicode glyph into
  Workspace, while retaining the Zulip emoji name in provider metadata;
- files use `upload_file` with an opened binary file object and the returned URL
  is embedded only after Workspace file-plane authorization/copy.

Zulip-internal links follow the documented Markdown and URL contracts:

- raw channel, topic, and message references use `#**channel**`,
  `#**channel>topic**`, and `#**channel>topic@message-id**`;
- deep links use `#narrow/` operator/operand pairs. The bridge accepts
  `channel` and legacy `stream`, `dm` and legacy `pm`/`pm-with`, `topic`,
  `near`, and the stable topic anchor `with`;
- modern channel operands start with the numeric channel ID; any suffix is only
  a readable hint. Topic operands replace percent signs with dots after URL
  encoding, so decoding reverses dots to percent signs before URL decoding;
- `near/<message-id>` identifies a message, while `with/<message-id>` keeps a
  conversation link stable and therefore resolves to the channel/topic rather
  than the message;
- user profile links use `#user/<user-id>`, and DM operands contain provider
  user IDs.

Inbound links are stored as Workspace Markdown entity URNs (`urn:user`,
`urn:message`, `urn:stream`, and `urn:topic`). Other HTTP(S) targets are stored
as `urn:url:<absolute-url>`. Outbound entity URNs are rendered with Zulip's
native reference syntax when available, while DM and fallback links use
absolute Zulip URLs.

The bridge requests the `notification_settings_null` and
`bulk_message_deletion` client capabilities. It intentionally does not request
`empty_topic_name`. Both the empty string from an older persisted queue and
Zulip's history fallback name `general chat` identify the special empty topic;
the bridge reports that topic as the Workspace channel's default topic and
routes its messages through the backend-owned `default_topic_uuid`. It accepts
`null` channel notification settings as an instruction to inherit the user's
`enable_stream_desktop_notifications` value. The bridge requests and persists
both registration snapshots and live `user_settings` updates so inherited
channels converge when that global value changes. It does not assume event IDs
are gapless and
persists each queue's last acknowledged event ID on the element's persistent
PostgreSQL disk.

Per-channel notification synchronization maps Workspace `muted` to Zulip
`is_muted=true`, `all_messages` to `is_muted=false` plus
`desktop_notifications=true`, and `mentions_only` to `is_muted=false` plus
`desktop_notifications=false`. Per-topic modes map directly to Zulip
`visibility_policy`: `default=0`, `mute=1`, `unmute=2`, and `follow=3`.
`user_topic` events and registration snapshots provide Zulip's
`last_updated` timestamp. Zulip subscription update events do not include a
setting timestamp, so the bridge durably records the event observation time;
that observation time is the provider-side LWW timestamp for channel settings.

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
