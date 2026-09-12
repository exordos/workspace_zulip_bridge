# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from typing import Any

from workspace_zulip_bridge.models import ChatRole
from workspace_zulip_bridge.models import ChatType
from workspace_zulip_bridge.models import RecentPrivateConversation
from workspace_zulip_bridge.models import ZulipChat
from workspace_zulip_bridge.models import ZulipChatCatalog

_CHANNEL_MEMBERSHIP_FIELDS = frozenset(
    {
        "audible_notifications",
        "color",
        "desktop_notifications",
        "email_notifications",
        "in_home_view",
        "is_muted",
        "pin_to_top",
        "push_notifications",
        "wildcard_mentions_notify",
    }
)


@dataclass(slots=True)
class _DirectConversation:
    participant_names: dict[int, str]
    max_message_id: int
    recipient_id: int | None


class ChatCatalogBuilder:
    def __init__(self, identity_user_id: int, identity_full_name: str) -> None:
        self._identity_user_id = identity_user_id
        self._identity_full_name = identity_full_name
        self._channels: dict[str, ZulipChat] = {}
        self._direct_conversations: dict[str, _DirectConversation] = {}

    def add_subscriptions(
        self, subscriptions: list[Mapping[str, Any]]
    ) -> tuple[ZulipChat, ...]:
        added: list[ZulipChat] = []
        for subscription in subscriptions:
            stream_id = subscription.get("stream_id")
            name = subscription.get("name")
            if not isinstance(stream_id, int) or not isinstance(name, str):
                raise ValueError("invalid Zulip subscription")
            chat_key = f"channel:{stream_id}"
            membership_parameters = {
                key: value
                for key, value in subscription.items()
                if key in _CHANNEL_MEMBERSHIP_FIELDS
            }
            chat_parameters = {
                key: value
                for key, value in subscription.items()
                if key not in _CHANNEL_MEMBERSHIP_FIELDS and key != "name"
            }
            chat = _make_chat(
                chat_type="channel",
                chat_key=chat_key,
                name=name,
                role="subscriber",
                chat_parameters=chat_parameters,
                membership_parameters=membership_parameters,
            )
            self._channels[chat_key] = chat
            added.append(chat)
        return tuple(added)

    def add_direct_messages(
        self,
        messages: list[Mapping[str, Any]],
        *,
        excluded_user_ids: frozenset[int] = frozenset(),
    ) -> tuple[ZulipChat, ...]:
        changed: dict[str, ZulipChat] = {}
        for message in messages:
            if message.get("type") == "stream":
                continue
            message_id = message.get("id")
            recipients = message.get("display_recipient")
            if not isinstance(message_id, int) or not isinstance(recipients, list):
                raise ValueError("invalid Zulip direct message")
            participant_names: dict[int, str] = {}
            for recipient in recipients:
                if not isinstance(recipient, Mapping):
                    raise ValueError("invalid Zulip direct message recipient")
                user_id = recipient.get("id")
                full_name = recipient.get("full_name")
                if not isinstance(user_id, int) or not isinstance(full_name, str):
                    raise ValueError("invalid Zulip direct message recipient")
                participant_names[user_id] = full_name
            if excluded_user_ids.intersection(participant_names):
                continue
            if self._identity_user_id not in participant_names:
                participant_names[self._identity_user_id] = self._identity_full_name
            participant_ids = sorted(participant_names)
            chat_key = "direct:" + ",".join(str(user_id) for user_id in participant_ids)
            raw_recipient_id = message.get("recipient_id")
            recipient_id = (
                raw_recipient_id if isinstance(raw_recipient_id, int) else None
            )
            existing = self._direct_conversations.get(chat_key)
            if existing is None or message_id > existing.max_message_id:
                conversation = _DirectConversation(
                    participant_names=participant_names,
                    max_message_id=message_id,
                    recipient_id=recipient_id,
                )
                self._direct_conversations[chat_key] = conversation
                changed[chat_key] = self._make_direct_chat(chat_key, conversation)
        return tuple(changed[key] for key in sorted(changed))

    def add_recent_direct_conversations(
        self,
        conversations: tuple[RecentPrivateConversation, ...],
        *,
        user_names: Mapping[int, str],
        excluded_user_ids: frozenset[int] = frozenset(),
    ) -> tuple[ZulipChat, ...]:
        changed: dict[str, ZulipChat] = {}
        for recent in conversations:
            participant_ids = sorted({self._identity_user_id, *recent.user_ids})
            if excluded_user_ids.intersection(participant_ids):
                continue
            if not all(user_id in user_names for user_id in participant_ids):
                continue
            participant_names = {
                user_id: user_names[user_id] for user_id in participant_ids
            }
            chat_key = "direct:" + ",".join(str(user_id) for user_id in participant_ids)
            existing = self._direct_conversations.get(chat_key)
            if (
                existing is not None
                and existing.max_message_id >= recent.max_message_id
            ):
                continue
            conversation = _DirectConversation(
                participant_names=participant_names,
                max_message_id=recent.max_message_id,
                recipient_id=None,
            )
            self._direct_conversations[chat_key] = conversation
            changed[chat_key] = self._make_direct_chat(chat_key, conversation)
        return tuple(changed[key] for key in sorted(changed))

    def build(
        self,
        available_message_counts: Mapping[str, int] | None = None,
    ) -> ZulipChatCatalog:
        chats = list(self._channels.values())
        for chat_key, conversation in self._direct_conversations.items():
            chats.append(self._make_direct_chat(chat_key, conversation))
        counts = available_message_counts or {}
        chats = [
            replace(chat, available_message_count=counts.get(chat.chat_key, 0))
            for chat in chats
        ]
        chats.sort(key=lambda chat: chat.chat_key)
        digest = hashlib.sha256()
        for chat in chats:
            digest.update(chat.content_hash)
            digest.update(chat.membership_hash)
            digest.update(chat.available_message_count.to_bytes(8, "big"))
        return ZulipChatCatalog(tuple(chats), digest.digest())

    def _make_direct_chat(
        self,
        chat_key: str,
        conversation: _DirectConversation,
    ) -> ZulipChat:
        participant_ids = sorted(conversation.participant_names)
        name = ", ".join(
            conversation.participant_names[user_id] for user_id in participant_ids
        )
        chat_type: ChatType = "group_direct" if len(participant_ids) > 2 else "direct"
        parameters: dict[str, object] = {
            "participant_user_ids": participant_ids,
        }
        if conversation.recipient_id is not None:
            parameters["recipient_id"] = conversation.recipient_id
        return _make_chat(
            chat_type=chat_type,
            chat_key=chat_key,
            name=name,
            role="participant",
            chat_parameters=parameters,
            membership_parameters={},
        )


def _make_chat(
    *,
    chat_type: ChatType,
    chat_key: str,
    name: str,
    role: ChatRole,
    chat_parameters: Mapping[str, object],
    membership_parameters: Mapping[str, object],
) -> ZulipChat:
    chat_parameters_json = _canonical_json(chat_parameters)
    membership_parameters_json = _canonical_json(membership_parameters)
    canonical_digest = hashlib.sha256()
    for value in (
        chat_type,
        chat_key,
        name,
        chat_parameters_json,
    ):
        canonical_digest.update(value.encode("utf-8"))
        canonical_digest.update(b"\0")
    membership_digest = hashlib.sha256()
    for value in (
        role,
        membership_parameters_json,
    ):
        membership_digest.update(value.encode("utf-8"))
        membership_digest.update(b"\0")
    return ZulipChat(
        chat_type=chat_type,
        chat_key=chat_key,
        name=name,
        role=role,
        chat_parameters_json=chat_parameters_json,
        membership_parameters_json=membership_parameters_json,
        content_hash=canonical_digest.digest(),
        membership_hash=membership_digest.digest(),
    )


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
