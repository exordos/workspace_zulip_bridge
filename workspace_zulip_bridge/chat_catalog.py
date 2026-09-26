# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from typing import Any

from workspace_zulip_bridge.models import BindingRole
from workspace_zulip_bridge.models import ChatType
from workspace_zulip_bridge.models import MembershipKind
from workspace_zulip_bridge.models import RecentPrivateConversation
from workspace_zulip_bridge.models import ZulipChat
from workspace_zulip_bridge.models import ZulipChatCatalog
from workspace_zulip_bridge.workspace_entities import validate_description

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
    def __init__(
        self,
        identity_user_id: int,
        identity_full_name: str,
        identity_role: int = 400,
    ) -> None:
        self._identity_user_id = identity_user_id
        self._identity_full_name = identity_full_name
        self._identity_role = _binding_role(identity_role)
        self._channels: dict[str, ZulipChat] = {}
        self._direct_conversations: dict[str, _DirectConversation] = {}

    def add_subscriptions(
        self,
        subscriptions: list[Mapping[str, Any]],
        *,
        first_visible_message_ids: Mapping[int, int | None] | None = None,
        desktop_notifications_default: bool = True,
    ) -> tuple[ZulipChat, ...]:
        added: list[ZulipChat] = []
        visible_message_ids = first_visible_message_ids or {}
        for subscription in subscriptions:
            stream_id = subscription.get("stream_id")
            name = subscription.get("name")
            if not isinstance(stream_id, int) or not isinstance(name, str):
                raise ValueError("invalid Zulip subscription")
            validate_description(subscription.get("description", ""))
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
            desktop_notifications = membership_parameters.get("desktop_notifications")
            if desktop_notifications is None:
                desktop_notifications = desktop_notifications_default
            elif isinstance(desktop_notifications, int) and desktop_notifications in {
                0,
                1,
            }:
                desktop_notifications = bool(desktop_notifications)
            if not isinstance(desktop_notifications, bool):
                raise ValueError("invalid Zulip subscription notification setting")
            notification_mode = (
                "muted"
                if membership_parameters.get("is_muted")
                else "all_messages"
                if desktop_notifications
                else "mentions_only"
            )
            chat = _make_chat(
                chat_type="channel",
                chat_key=chat_key,
                name=name,
                role=self._identity_role,
                membership_kind="subscriber",
                notification_mode=notification_mode,
                chat_parameters=chat_parameters,
                membership_parameters=membership_parameters,
                first_visible_message_id=visible_message_ids.get(stream_id),
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
            role=self._identity_role,
            membership_kind="participant",
            notification_mode="all_messages",
            chat_parameters=parameters,
            membership_parameters={},
        )


def _make_chat(
    *,
    chat_type: ChatType,
    chat_key: str,
    name: str,
    role: BindingRole,
    membership_kind: MembershipKind,
    notification_mode: str,
    chat_parameters: Mapping[str, object],
    membership_parameters: Mapping[str, object],
    first_visible_message_id: int | None = None,
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
        membership_kind,
        notification_mode,
        membership_parameters_json,
        str(first_visible_message_id or 0),
    ):
        membership_digest.update(value.encode("utf-8"))
        membership_digest.update(b"\0")
    return ZulipChat(
        chat_type=chat_type,
        chat_key=chat_key,
        name=name,
        role=role,
        membership_kind=membership_kind,
        notification_mode=notification_mode,
        chat_parameters_json=chat_parameters_json,
        membership_parameters_json=membership_parameters_json,
        content_hash=canonical_digest.digest(),
        membership_hash=membership_digest.digest(),
        first_visible_message_id=first_visible_message_id,
    )


def _binding_role(role: int) -> BindingRole:
    if role == 100:
        return "owner"
    if role == 200:
        return "administrator"
    if role == 300:
        return "moderator"
    if role == 400:
        return "member"
    if role == 600:
        return "guest"
    raise ValueError(f"unsupported Zulip role: {role}")


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
