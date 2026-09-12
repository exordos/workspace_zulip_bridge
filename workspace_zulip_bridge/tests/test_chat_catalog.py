# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import json

from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder
from workspace_zulip_bridge.models import RecentPrivateConversation


def test_channel_parameters_and_membership_parameters_are_separated() -> None:
    first = ChatCatalogBuilder(10, "Current User")
    first.add_subscriptions(
        [
            {
                "stream_id": 7,
                "name": "Engineering",
                "description": "Product engineering",
                "invite_only": True,
                "color": "#123456",
                "is_muted": False,
                "desktop_notifications": 1,
            }
        ]
    )
    catalog = first.build()

    assert len(catalog.chats) == 1
    chat = catalog.chats[0]
    assert chat.chat_key == "channel:7"
    assert chat.chat_type == "channel"
    assert chat.role == "subscriber"
    assert json.loads(chat.chat_parameters_json) == {
        "description": "Product engineering",
        "invite_only": True,
        "stream_id": 7,
    }
    assert json.loads(chat.membership_parameters_json) == {
        "color": "#123456",
        "desktop_notifications": 1,
        "is_muted": False,
    }

    reordered = ChatCatalogBuilder(10, "Current User")
    reordered.add_subscriptions(
        [
            {
                "desktop_notifications": 1,
                "is_muted": False,
                "color": "#123456",
                "invite_only": True,
                "description": "Product engineering",
                "name": "Engineering",
                "stream_id": 7,
            }
        ]
    )
    assert reordered.build().content_hash == catalog.content_hash


def test_direct_conversations_are_deduplicated_by_sorted_participants() -> None:
    builder = ChatCatalogBuilder(10, "Current User")
    builder.add_direct_messages(
        [
            {
                "id": 200,
                "recipient_id": 55,
                "display_recipient": [
                    {"id": 12, "full_name": "Second User"},
                    {"id": 10, "full_name": "Current User"},
                ],
            },
            {
                "id": 150,
                "recipient_id": 55,
                "display_recipient": [
                    {"id": 10, "full_name": "Current User"},
                    {"id": 12, "full_name": "Second User"},
                ],
            },
            {
                "id": 300,
                "recipient_id": 80,
                "display_recipient": [
                    {"id": 12, "full_name": "Second User"},
                    {"id": 13, "full_name": "Third User"},
                ],
            },
        ]
    )
    chats = {chat.chat_key: chat for chat in builder.build().chats}

    direct = chats["direct:10,12"]
    assert direct.chat_type == "direct"
    assert direct.name == "Current User, Second User"
    assert direct.role == "participant"
    assert json.loads(direct.chat_parameters_json) == {
        "participant_user_ids": [10, 12],
        "recipient_id": 55,
    }

    group = chats["direct:10,12,13"]
    assert group.chat_type == "group_direct"
    assert group.name == "Current User, Second User, Third User"
    assert json.loads(group.chat_parameters_json) == {
        "participant_user_ids": [10, 12, 13],
        "recipient_id": 80,
    }


def test_recent_direct_conversations_build_catalog_without_history_scan() -> None:
    builder = ChatCatalogBuilder(10, "Current User")
    builder.add_recent_direct_conversations(
        (
            RecentPrivateConversation((12,), 200),
            RecentPrivateConversation((13, 12), 300),
            RecentPrivateConversation((99,), 400),
        ),
        user_names={
            10: "Current User",
            12: "Second User",
            13: "Third User",
        },
    )

    chats = {chat.chat_key: chat for chat in builder.build().chats}
    assert chats["direct:10,12"].name == "Current User, Second User"
    assert chats["direct:10,12,13"].chat_type == "group_direct"
    assert "direct:10,99" not in chats
