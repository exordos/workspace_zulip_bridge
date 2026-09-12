# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import hashlib
import json
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from uuid import UUID

from workspace_zulip_bridge.models import MessagePageBuild
from workspace_zulip_bridge.models import ZulipMessage

_KNOWN_FLAGS = frozenset(
    {
        "read",
        "starred",
        "collapsed",
        "mentioned",
        "stream_wildcard_mentioned",
        "topic_wildcard_mentioned",
        "has_alert_word",
        "historical",
        "wildcard_mentioned",
    }
)


def message_state_hash(
    *,
    sender_user_uuid: UUID,
    chat_key: str,
    topic_name: str | None,
    content: str,
    sent_at: int,
    is_read: bool,
    is_starred: bool,
    is_collapsed: bool,
    is_mentioned: bool,
    is_stream_wildcard_mentioned: bool,
    is_topic_wildcard_mentioned: bool,
    has_alert_word: bool,
    is_historical: bool,
    reactions: Sequence[Mapping[str, str]],
) -> bytes:
    state = {
        "sender_user_uuid": str(sender_user_uuid),
        "chat_key": chat_key,
        "topic_name": topic_name,
        "content": content,
        "created_at": sent_at,
        "is_read": is_read,
        "is_starred": is_starred,
        "is_collapsed": is_collapsed,
        "is_mentioned": is_mentioned,
        "is_stream_wildcard_mentioned": is_stream_wildcard_mentioned,
        "is_topic_wildcard_mentioned": is_topic_wildcard_mentioned,
        "has_alert_word": has_alert_word,
        "is_historical": is_historical,
        "reactions": list(reactions),
    }
    return hashlib.sha256(
        json.dumps(
            state,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()


def build_message_page(
    raw_messages: list[Mapping[str, Any]],
    *,
    own_user_id: int,
    user_uuids: Mapping[int, UUID],
    stream_ids_by_name: Mapping[str, int],
    allowed_chat_keys: set[str],
) -> MessagePageBuild:
    messages: list[ZulipMessage] = []
    skipped_messages = 0
    skipped_reactions = 0
    unknown_flags: set[str] = set()
    for raw_message in raw_messages:
        parsed = _parse_message(
            raw_message,
            own_user_id=own_user_id,
            user_uuids=user_uuids,
            stream_ids_by_name=stream_ids_by_name,
            allowed_chat_keys=allowed_chat_keys,
        )
        if parsed is None:
            skipped_messages += 1
            continue
        message, reaction_skips, new_unknown_flags = parsed
        messages.append(message)
        skipped_reactions += reaction_skips
        unknown_flags.update(new_unknown_flags)
    return MessagePageBuild(
        messages=tuple(messages),
        skipped_messages=skipped_messages,
        skipped_reactions=skipped_reactions,
        unknown_flags=tuple(sorted(unknown_flags)),
    )


def _parse_message(
    raw_message: Mapping[str, Any],
    *,
    own_user_id: int,
    user_uuids: Mapping[int, UUID],
    stream_ids_by_name: Mapping[str, int],
    allowed_chat_keys: set[str],
) -> tuple[ZulipMessage, int, set[str]] | None:
    message_id = raw_message.get("id")
    sender_user_id = raw_message.get("sender_id")
    content = raw_message.get("content")
    sent_at = raw_message.get("timestamp")
    message_type = raw_message.get("type")
    if (
        not isinstance(message_id, int)
        or not isinstance(sender_user_id, int)
        or not isinstance(content, str)
        or not isinstance(sent_at, int)
        or message_type not in {"stream", "private"}
    ):
        raise ValueError("invalid Zulip message")
    sender_user_uuid = user_uuids.get(sender_user_id)
    if sender_user_uuid is None:
        return None

    if message_type == "stream":
        raw_stream_id = raw_message.get("stream_id")
        display_recipient = raw_message.get("display_recipient")
        stream_id = raw_stream_id if isinstance(raw_stream_id, int) else None
        if stream_id is None and isinstance(display_recipient, str):
            stream_id = stream_ids_by_name.get(display_recipient)
        topic_name = raw_message.get("subject")
        if stream_id is None or not isinstance(topic_name, str):
            raise ValueError("invalid Zulip channel message")
        chat_key = f"channel:{stream_id}"
    else:
        recipients = raw_message.get("display_recipient")
        if not isinstance(recipients, list):
            raise ValueError("invalid Zulip direct message")
        participant_ids = {own_user_id}
        for recipient in recipients:
            if not isinstance(recipient, Mapping):
                raise ValueError("invalid Zulip direct message recipient")
            recipient_user_id = recipient.get("id")
            if not isinstance(recipient_user_id, int):
                raise ValueError("invalid Zulip direct message recipient")
            participant_ids.add(recipient_user_id)
        if any(user_id not in user_uuids for user_id in participant_ids):
            return None
        chat_key = "direct:" + ",".join(str(value) for value in sorted(participant_ids))
        topic_name = None
    if chat_key not in allowed_chat_keys:
        return None

    raw_flags = raw_message.get("flags")
    if not isinstance(raw_flags, list) or not all(
        isinstance(flag, str) for flag in raw_flags
    ):
        raise ValueError("invalid Zulip message flags")
    flags = set(raw_flags)
    unknown_flags = flags - _KNOWN_FLAGS
    stream_wildcard = "stream_wildcard_mentioned" in flags
    topic_wildcard = "topic_wildcard_mentioned" in flags
    if "wildcard_mentioned" in flags and not (stream_wildcard or topic_wildcard):
        stream_wildcard = True

    raw_reactions = raw_message.get("reactions")
    if not isinstance(raw_reactions, list):
        raise ValueError("invalid Zulip message reactions")
    reactions: list[dict[str, str]] = []
    skipped_reactions = 0
    for raw_reaction in raw_reactions:
        if not isinstance(raw_reaction, Mapping):
            raise ValueError("invalid Zulip message reaction")
        reaction_user_id = raw_reaction.get("user_id")
        emoji_name = raw_reaction.get("emoji_name")
        emoji_code = raw_reaction.get("emoji_code")
        reaction_type = raw_reaction.get("reaction_type")
        if (
            not isinstance(reaction_user_id, int)
            or not isinstance(emoji_name, str)
            or not isinstance(emoji_code, str)
            or not isinstance(reaction_type, str)
        ):
            raise ValueError("invalid Zulip message reaction")
        reaction_user_uuid = user_uuids.get(reaction_user_id)
        if reaction_user_uuid is None:
            skipped_reactions += 1
            continue
        reactions.append(
            {
                "user_uuid": str(reaction_user_uuid),
                "emoji_name": emoji_name,
                "emoji_code": emoji_code,
                "reaction_type": reaction_type,
            }
        )
    reactions.sort(
        key=lambda reaction: (
            reaction["user_uuid"],
            reaction["reaction_type"],
            reaction["emoji_code"],
            reaction["emoji_name"],
        )
    )
    reactions_json = json.dumps(
        reactions,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    is_read = "read" in flags
    is_starred = "starred" in flags
    is_collapsed = "collapsed" in flags
    is_mentioned = "mentioned" in flags
    has_alert_word = "has_alert_word" in flags
    is_historical = "historical" in flags
    message_hash = message_state_hash(
        sender_user_uuid=sender_user_uuid,
        chat_key=chat_key,
        topic_name=topic_name,
        content=content,
        sent_at=sent_at,
        is_read=is_read,
        is_starred=is_starred,
        is_collapsed=is_collapsed,
        is_mentioned=is_mentioned,
        is_stream_wildcard_mentioned=stream_wildcard,
        is_topic_wildcard_mentioned=topic_wildcard,
        has_alert_word=has_alert_word,
        is_historical=is_historical,
        reactions=reactions,
    )
    return (
        ZulipMessage(
            message_id=message_id,
            chat_key=chat_key,
            topic_name=topic_name,
            sender_user_uuid=sender_user_uuid,
            content=content,
            is_read=is_read,
            is_starred=is_starred,
            is_collapsed=is_collapsed,
            is_mentioned=is_mentioned,
            is_stream_wildcard_mentioned=stream_wildcard,
            is_topic_wildcard_mentioned=topic_wildcard,
            has_alert_word=has_alert_word,
            is_historical=is_historical,
            reactions_json=reactions_json,
            message_hash=message_hash,
            sent_at=sent_at,
        ),
        skipped_reactions,
        unknown_flags,
    )
