# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Literal
from uuid import UUID

UserStatus = Literal[
    "init",
    "streaming",
    "filling",
    "scheduling",
    "backfilling",
    "active",
]
ChatType = Literal["channel", "direct", "group_direct"]
ChatRole = Literal["subscriber", "participant"]


@dataclass(frozen=True, slots=True)
class ZulipUser:
    uuid: UUID
    endpoint: str
    login: str
    api_key: str = field(repr=False)
    queue_id: str | None = None
    last_event_id: int | None = None
    status: UserStatus = "init"
    chats_hash: bytes | None = None
    zulip_user_id: int | None = None
    full_name: str | None = None
    role: int | None = None
    disabled: bool = False
    has_pending_history: bool = False

    def connection_signature(self) -> tuple[str, str, str]:
        return self.endpoint, self.login, self.api_key


@dataclass(frozen=True, slots=True)
class ZulipEvent:
    event_id: int
    event_type: str
    payload_json: str


@dataclass(frozen=True, slots=True)
class RegisteredQueue:
    queue_id: str
    last_event_id: int
    longpoll_timeout_seconds: float
    recent_private_conversations: tuple["RecentPrivateConversation", ...] = ()


@dataclass(frozen=True, slots=True)
class RecentPrivateConversation:
    user_ids: tuple[int, ...]
    max_message_id: int


@dataclass(frozen=True, slots=True)
class ZulipIdentity:
    user_id: int
    full_name: str
    role: int


@dataclass(frozen=True, slots=True)
class ZulipDirectoryUser:
    user_id: int
    login: str
    full_name: str
    role: int
    disabled: bool
    is_bot: bool


@dataclass(frozen=True, slots=True)
class DirectMessagePage:
    messages: list[Mapping[str, Any]]
    found_oldest: bool


@dataclass(frozen=True, slots=True)
class MessagePage:
    messages: list[Mapping[str, Any]]
    found_oldest: bool


@dataclass(frozen=True, slots=True)
class ZulipChat:
    chat_type: ChatType
    chat_key: str
    name: str
    role: ChatRole
    chat_parameters_json: str
    membership_parameters_json: str
    content_hash: bytes
    membership_hash: bytes
    available_message_count: int = 0


@dataclass(frozen=True, slots=True)
class ZulipChatCatalog:
    chats: tuple[ZulipChat, ...]
    content_hash: bytes


@dataclass(frozen=True, slots=True)
class ChatCatalogWrite:
    activated: bool
    reused: bool
    upserted: int
    deleted: int


@dataclass(frozen=True, slots=True)
class ZulipMessage:
    message_id: int
    chat_key: str
    topic_name: str | None
    sender_user_uuid: UUID
    content: str
    is_read: bool
    is_starred: bool
    is_collapsed: bool
    is_mentioned: bool
    is_stream_wildcard_mentioned: bool
    is_topic_wildcard_mentioned: bool
    has_alert_word: bool
    is_historical: bool
    reactions_json: str
    message_hash: bytes
    sent_at: int


@dataclass(frozen=True, slots=True)
class MessagePageBuild:
    messages: tuple[ZulipMessage, ...]
    skipped_messages: int
    skipped_reactions: int
    unknown_flags: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MessagePageWrite:
    received: int
    changed: int
    unchanged: int
    unassigned: int
    topics_inserted: int


@dataclass(frozen=True, slots=True)
class HistoryWrite:
    activated: bool
    messages_deleted: int
    topics_deleted: int
    schedules_loaded: int


@dataclass(frozen=True, slots=True)
class UserDirectoryWrite:
    humans: int
    changed: int


@dataclass(frozen=True, slots=True)
class LiveMessageWrite:
    messages_changed: int
    messages_unchanged: int
    messages_deleted: int
    topics_inserted: int


@dataclass(frozen=True, slots=True)
class ChatScheduleReconcile:
    invalidated: int
    assigned: int
    messages_deleted: int


@dataclass(frozen=True, slots=True)
class ScheduledChat:
    chat_key: str
    available_message_count: int
