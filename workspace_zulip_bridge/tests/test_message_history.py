# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import json
from copy import deepcopy
from uuid import UUID

from workspace_zulip_bridge.message_history import build_message_page

USER_ONE = UUID("00000000-0000-0000-0000-000000000001")
USER_TWO = UUID("00000000-0000-0000-0000-000000000002")


def _channel_message() -> dict[str, object]:
    return {
        "id": 123,
        "type": "stream",
        "stream_id": 7,
        "display_recipient": "Engineering",
        "subject": "Performance",
        "sender_id": 10,
        "content": "Raw **Markdown**",
        "timestamp": 1_700_000_000,
        "flags": [
            "starred",
            "read",
            "mentioned",
            "topic_wildcard_mentioned",
            "future_flag",
        ],
        "reactions": [
            {
                "user_id": 12,
                "emoji_name": "thumbs_up",
                "emoji_code": "1f44d",
                "reaction_type": "unicode_emoji",
            },
            {
                "user_id": 99,
                "emoji_name": "zulip",
                "emoji_code": "zulip",
                "reaction_type": "zulip_extra_emoji",
            },
        ],
    }


def test_message_fields_flags_reactions_and_hash_are_canonical() -> None:
    raw = _channel_message()
    first = build_message_page(
        [raw],
        own_user_id=10,
        user_uuids={10: USER_ONE, 12: USER_TWO},
        stream_ids_by_name={"Engineering": 7},
        allowed_chat_keys={"channel:7"},
    )

    assert first.skipped_messages == 0
    assert first.skipped_reactions == 1
    assert first.unknown_flags == ("future_flag",)
    message = first.messages[0]
    assert message.chat_key == "channel:7"
    assert message.topic_name == "Performance"
    assert message.sender_user_uuid == USER_ONE
    assert message.is_read
    assert message.is_starred
    assert message.is_mentioned
    assert message.is_topic_wildcard_mentioned
    assert not message.is_stream_wildcard_mentioned
    assert json.loads(message.reactions_json) == [
        {
            "emoji_code": "1f44d",
            "emoji_name": "thumbs_up",
            "reaction_type": "unicode_emoji",
            "user_uuid": str(USER_TWO),
        }
    ]

    reordered = deepcopy(raw)
    reordered["flags"] = list(reversed(raw["flags"]))  # type: ignore[arg-type]
    reordered["reactions"] = list(
        reversed(raw["reactions"])  # type: ignore[arg-type]
    )
    second = build_message_page(
        [reordered],
        own_user_id=10,
        user_uuids={10: USER_ONE, 12: USER_TWO},
        stream_ids_by_name={"Engineering": 7},
        allowed_chat_keys={"channel:7"},
    )
    assert second.messages[0].message_hash == message.message_hash

    changed = deepcopy(raw)
    changed["content"] = "Changed"
    third = build_message_page(
        [changed],
        own_user_id=10,
        user_uuids={10: USER_ONE, 12: USER_TWO},
        stream_ids_by_name={"Engineering": 7},
        allowed_chat_keys={"channel:7"},
    )
    assert third.messages[0].message_hash != message.message_hash


def test_direct_message_with_excluded_participant_is_skipped() -> None:
    result = build_message_page(
        [
            {
                "id": 44,
                "type": "private",
                "sender_id": 10,
                "content": "bot conversation",
                "timestamp": 1_700_000_001,
                "flags": [],
                "reactions": [],
                "display_recipient": [
                    {"id": 10, "full_name": "Human"},
                    {"id": 99, "full_name": "Bot"},
                ],
            }
        ],
        own_user_id=10,
        user_uuids={10: USER_ONE},
        stream_ids_by_name={},
        allowed_chat_keys=set(),
    )

    assert result.messages == ()
    assert result.skipped_messages == 1


def test_legacy_wildcard_flag_maps_to_stream_wildcard() -> None:
    raw = _channel_message()
    raw["flags"] = ["wildcard_mentioned"]
    result = build_message_page(
        [raw],
        own_user_id=10,
        user_uuids={10: USER_ONE},
        stream_ids_by_name={"Engineering": 7},
        allowed_chat_keys={"channel:7"},
    )

    assert result.messages[0].is_stream_wildcard_mentioned
    assert not result.messages[0].is_topic_wildcard_mentioned
