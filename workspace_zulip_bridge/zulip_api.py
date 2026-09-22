# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import hashlib
import json
import ssl
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import Literal

import httpx

from workspace_zulip_bridge.models import MessagePage
from workspace_zulip_bridge.models import RecentPrivateConversation
from workspace_zulip_bridge.models import RegisteredQueue
from workspace_zulip_bridge.models import ZulipAttachment
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.models import ZulipIdentity
from workspace_zulip_bridge.models import ZulipUserPresence
from workspace_zulip_bridge.models import ZulipUserProfileStatus
from workspace_zulip_bridge.models import ZulipUserTopic


class ZulipApiError(Exception):
    def __init__(
        self,
        code: str,
        *,
        retryable: bool,
        status_code: int | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code


def _parse_user_topics(raw: object) -> tuple[ZulipUserTopic, ...]:
    if not isinstance(raw, list):
        raise ZulipApiError("invalid_register_response", retryable=True)
    result: list[ZulipUserTopic] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ZulipApiError("invalid_register_response", retryable=True)
        stream_id = item.get("stream_id")
        topic_name = item.get("topic_name")
        visibility_policy = item.get("visibility_policy")
        last_updated = item.get("last_updated")
        if (
            not isinstance(stream_id, int)
            or not isinstance(topic_name, str)
            or visibility_policy not in {1, 2, 3}
            or not isinstance(last_updated, int)
        ):
            raise ZulipApiError("invalid_register_response", retryable=True)
        result.append(
            ZulipUserTopic(
                stream_id=stream_id,
                topic_name=topic_name,
                visibility_policy=visibility_policy,
                last_updated=last_updated,
            )
        )
    return tuple(result)


def _parse_user_presences(
    raw: object,
    *,
    offline_threshold_seconds: int,
) -> tuple[ZulipUserPresence, ...]:
    if not isinstance(raw, Mapping):
        raise ZulipApiError("invalid_register_response", retryable=True)
    result: list[ZulipUserPresence] = []
    for raw_user_id, value in raw.items():
        if not isinstance(raw_user_id, str) or not raw_user_id.isdigit():
            continue
        if not isinstance(value, Mapping):
            raise ZulipApiError("invalid_register_response", retryable=True)
        active_timestamp = value.get("active_timestamp")
        idle_timestamp = value.get("idle_timestamp")
        active = active_timestamp if isinstance(active_timestamp, int) else None
        idle = idle_timestamp if isinstance(idle_timestamp, int) else None
        if active is None and idle is None:
            continue
        latest = max(value for value in (active, idle) if value is not None)
        status: Literal["active", "idle", "offline"]
        if latest < int(time.time()) - offline_threshold_seconds:
            status = "offline"
        elif active is not None and active >= (idle or 0):
            status = "active"
        else:
            status = "idle"
        result.append(
            ZulipUserPresence(
                user_id=int(raw_user_id),
                status=status,
                last_ping_at=latest,
            )
        )
    return tuple(result)


def _parse_user_statuses(raw: object) -> tuple[ZulipUserProfileStatus, ...]:
    if not isinstance(raw, Mapping):
        raise ZulipApiError("invalid_register_response", retryable=True)
    result: list[ZulipUserProfileStatus] = []
    for raw_user_id, value in raw.items():
        if not isinstance(raw_user_id, str) or not raw_user_id.isdigit():
            continue
        if not isinstance(value, Mapping):
            raise ZulipApiError("invalid_register_response", retryable=True)
        status_text = value.get("status_text")
        status_emoji = value.get("emoji_name")
        if status_text is not None and not isinstance(status_text, str):
            raise ZulipApiError("invalid_register_response", retryable=True)
        if status_emoji is not None and not isinstance(status_emoji, str):
            raise ZulipApiError("invalid_register_response", retryable=True)
        result.append(
            ZulipUserProfileStatus(
                user_id=int(raw_user_id),
                status_text=status_text or None,
                status_emoji=status_emoji or None,
            )
        )
    return tuple(result)


def parse_attachment(raw: Mapping[str, Any]) -> ZulipAttachment:
    attachment_id = raw.get("id")
    path_id = raw.get("path_id")
    name = raw.get("name")
    size_bytes = raw.get("size")
    created_at = raw.get("create_time")
    message_ids = raw.get("message_ids")
    if (
        not isinstance(attachment_id, int)
        or not isinstance(path_id, str)
        or not path_id
        or not isinstance(name, str)
        or not isinstance(size_bytes, int)
        or size_bytes < 0
        or not isinstance(created_at, int)
        or not isinstance(message_ids, list)
        or not all(isinstance(message_id, int) for message_id in message_ids)
    ):
        raise ZulipApiError("invalid_attachments_response", retryable=True)
    source_path = "/user_uploads/" + path_id.lstrip("/")
    metadata_hash = hashlib.sha256(
        json.dumps(
            {
                "id": attachment_id,
                "source_path": source_path,
                "name": name,
                "size": size_bytes,
                "created_at": created_at,
                "message_ids": sorted(set(message_ids)),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()
    return ZulipAttachment(
        attachment_id=attachment_id,
        source_path=source_path,
        name=name,
        size_bytes=size_bytes,
        created_at=created_at,
        message_ids=tuple(sorted(set(message_ids))),
        metadata_hash=metadata_hash,
    )


class ZulipApiClient:
    def __init__(
        self,
        endpoint: str,
        login: str,
        api_key: str,
        *,
        ca_file: Path | None,
        connect_timeout_seconds: float,
        default_longpoll_timeout_seconds: float,
        idle_queue_timeout_seconds: int,
        chat_fill_timeout_seconds: float,
        message_page_size: int,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._base_url = endpoint.rstrip("/")
        self._connect_timeout_seconds = connect_timeout_seconds
        self._default_longpoll_timeout_seconds = default_longpoll_timeout_seconds
        self._idle_queue_timeout_seconds = idle_queue_timeout_seconds
        self._chat_fill_timeout_seconds = chat_fill_timeout_seconds
        self._message_page_size = message_page_size
        self._auth = httpx.BasicAuth(login, api_key)
        verify: bool | ssl.SSLContext = True
        if ca_file is not None:
            context = ssl.create_default_context()
            context.load_verify_locations(cafile=ca_file)
            verify = context
        self._client = httpx.Client(
            verify=verify,
            follow_redirects=False,
            trust_env=False,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
            timeout=httpx.Timeout(connect_timeout_seconds),
            transport=transport,
            headers={"User-Agent": "workspace-zulip-bridge"},
        )

    def close(self) -> None:
        self._client.close()

    def register(self) -> RegisteredQueue:
        payload = self._request(
            "POST",
            "/api/v1/register",
            data={
                "fetch_event_types": json.dumps(
                    [
                        "recent_private_conversations",
                        "presence",
                        "user_status",
                        "user_topic",
                    ],
                    separators=(",", ":"),
                ),
                "slim_presence": "true",
                "client_capabilities": json.dumps(
                    {
                        "notification_settings_null": False,
                        "simplified_presence_events": True,
                    },
                    separators=(",", ":"),
                ),
                "idle_queue_timeout": str(self._idle_queue_timeout_seconds),
            },
        )
        queue_id = payload.get("queue_id")
        last_event_id = payload.get("last_event_id")
        if not isinstance(queue_id, str) or not isinstance(last_event_id, int):
            raise ZulipApiError("invalid_register_response", retryable=True)
        raw_longpoll_timeout = payload.get("event_queue_longpoll_timeout_seconds")
        longpoll_timeout = (
            float(raw_longpoll_timeout)
            if isinstance(raw_longpoll_timeout, int | float)
            and raw_longpoll_timeout > 0
            else self._default_longpoll_timeout_seconds
        )
        raw_conversations = payload.get("recent_private_conversations")
        if not isinstance(raw_conversations, list):
            raise ZulipApiError("invalid_register_response", retryable=True)
        conversations: list[RecentPrivateConversation] = []
        for conversation in raw_conversations:
            if not isinstance(conversation, Mapping):
                raise ZulipApiError("invalid_register_response", retryable=True)
            user_ids = conversation.get("user_ids")
            max_message_id = conversation.get("max_message_id")
            if (
                not isinstance(user_ids, list)
                or not all(isinstance(user_id, int) for user_id in user_ids)
                or not isinstance(max_message_id, int)
            ):
                raise ZulipApiError("invalid_register_response", retryable=True)
            conversations.append(
                RecentPrivateConversation(
                    user_ids=tuple(sorted(set(user_ids))),
                    max_message_id=max_message_id,
                )
            )
        raw_presence_threshold = payload.get(
            "server_presence_offline_threshold_seconds",
            200,
        )
        presence_threshold = (
            raw_presence_threshold
            if isinstance(raw_presence_threshold, int) and raw_presence_threshold > 0
            else 200
        )
        user_topics = _parse_user_topics(payload.get("user_topics", []))
        user_presences = _parse_user_presences(
            payload.get("presences", {}),
            offline_threshold_seconds=presence_threshold,
        )
        user_statuses = _parse_user_statuses(payload.get("user_status", {}))
        return RegisteredQueue(
            queue_id,
            last_event_id,
            longpoll_timeout,
            tuple(conversations),
            user_topics,
            user_presences,
            user_statuses,
            presence_threshold,
        )

    def get_events(
        self,
        queue_id: str,
        last_event_id: int,
        longpoll_timeout_seconds: float,
    ) -> list[Mapping[str, Any]]:
        payload = self._request(
            "GET",
            "/api/v1/events",
            params={
                "queue_id": queue_id,
                "last_event_id": str(last_event_id),
                "dont_block": "false",
            },
            timeout=httpx.Timeout(
                self._connect_timeout_seconds,
                read=longpoll_timeout_seconds,
            ),
        )
        events = payload.get("events")
        if not isinstance(events, list) or not all(
            isinstance(event, Mapping) for event in events
        ):
            raise ZulipApiError("invalid_events_response", retryable=True)
        return events

    def get_own_user(self) -> ZulipIdentity:
        payload = self._request("GET", "/api/v1/users/me")
        user_id = payload.get("user_id")
        full_name = payload.get("full_name")
        role = payload.get("role")
        if (
            not isinstance(user_id, int)
            or not isinstance(full_name, str)
            or role not in {100, 200, 300, 400, 600}
        ):
            raise ZulipApiError("invalid_own_user_response", retryable=True)
        return ZulipIdentity(user_id=user_id, full_name=full_name, role=role)

    def get_subscriptions(self) -> list[Mapping[str, Any]]:
        payload = self._request(
            "GET",
            "/api/v1/users/me/subscriptions",
            params={"include_subscribers": "false"},
            timeout=self._chat_timeout(),
        )
        subscriptions = payload.get("subscriptions")
        if not isinstance(subscriptions, list) or not all(
            isinstance(subscription, Mapping) for subscription in subscriptions
        ):
            raise ZulipApiError("invalid_subscriptions_response", retryable=True)
        return subscriptions

    def get_users(self) -> list[ZulipDirectoryUser]:
        payload = self._request(
            "GET",
            "/api/v1/users",
            params={
                "client_gravatar": "true",
                "include_custom_profile_fields": "false",
            },
            timeout=self._chat_timeout(),
        )
        members = payload.get("members")
        if not isinstance(members, list):
            raise ZulipApiError("invalid_users_response", retryable=True)
        users: list[ZulipDirectoryUser] = []
        for member in members:
            if not isinstance(member, Mapping):
                raise ZulipApiError("invalid_users_response", retryable=True)
            user_id = member.get("user_id")
            login = member.get("email")
            full_name = member.get("full_name")
            is_active = member.get("is_active")
            is_bot = member.get("is_bot")
            role = member.get("role")
            raw_avatar_url = member.get("avatar_url")
            avatar_url = raw_avatar_url if isinstance(raw_avatar_url, str) else None
            if (
                not isinstance(user_id, int)
                or not isinstance(login, str)
                or not isinstance(full_name, str)
                or not isinstance(is_active, bool)
                or not isinstance(is_bot, bool)
                or role not in {100, 200, 300, 400, 600}
            ):
                raise ZulipApiError("invalid_users_response", retryable=True)
            users.append(
                ZulipDirectoryUser(
                    user_id=user_id,
                    login=login,
                    full_name=full_name,
                    role=role,
                    disabled=not is_active,
                    is_bot=is_bot,
                    avatar_url=avatar_url,
                )
            )
        return users

    def get_attachments(self) -> list[ZulipAttachment]:
        payload = self._request(
            "GET",
            "/api/v1/attachments",
            timeout=self._chat_timeout(),
        )
        raw_attachments = payload.get("attachments")
        if not isinstance(raw_attachments, list):
            raise ZulipApiError("invalid_attachments_response", retryable=True)
        attachments: list[ZulipAttachment] = []
        for raw in raw_attachments:
            if not isinstance(raw, Mapping):
                raise ZulipApiError("invalid_attachments_response", retryable=True)
            attachments.append(parse_attachment(raw))
        return attachments

    def get_messages_page(
        self,
        anchor: str | int,
        *,
        include_anchor: bool,
        narrow: list[Mapping[str, object]] | None = None,
    ) -> MessagePage:
        params = {
            "anchor": str(anchor),
            "include_anchor": json.dumps(include_anchor),
            "num_before": str(self._message_page_size),
            "num_after": "0",
            "apply_markdown": "false",
            "allow_empty_topic_name": "true",
        }
        if narrow is not None:
            params["narrow"] = json.dumps(narrow, separators=(",", ":"))
        payload = self._request(
            "GET",
            "/api/v1/messages",
            params=params,
            timeout=self._chat_timeout(),
        )
        messages = payload.get("messages")
        found_oldest = payload.get("found_oldest")
        if not isinstance(messages, list) or not all(
            isinstance(message, dict) for message in messages
        ):
            raise ZulipApiError("invalid_messages_response", retryable=True)
        if not isinstance(found_oldest, bool):
            raise ZulipApiError("invalid_messages_response", retryable=True)
        return MessagePage(messages=messages, found_oldest=found_oldest)

    def get_chat_messages_page(
        self,
        chat_key: str,
        own_user_id: int,
        anchor: str | int,
        *,
        include_anchor: bool,
    ) -> MessagePage:
        if chat_key.startswith("channel:"):
            try:
                channel_id = int(chat_key.removeprefix("channel:"))
            except ValueError as exc:
                raise ValueError("invalid channel chat key") from exc
            narrow: list[Mapping[str, object]] = [
                {"operator": "channel", "operand": channel_id}
            ]
        elif chat_key.startswith("direct:"):
            try:
                participant_ids = [
                    int(value) for value in chat_key.removeprefix("direct:").split(",")
                ]
            except ValueError as exc:
                raise ValueError("invalid direct chat key") from exc
            if own_user_id not in participant_ids or not participant_ids:
                raise ValueError("direct chat key does not include current user")
            other_user_ids = [
                user_id for user_id in participant_ids if user_id != own_user_id
            ]
            operand = other_user_ids or [own_user_id]
            narrow = [{"operator": "dm", "operand": operand}]
        else:
            raise ValueError("unsupported chat key")
        return self.get_messages_page(
            anchor,
            include_anchor=include_anchor,
            narrow=narrow,
        )

    def get_first_accessible_channel_message_id(self, stream_id: int) -> int | None:
        payload = self._request(
            "GET",
            "/api/v1/messages",
            params={
                "anchor": "oldest",
                "include_anchor": "true",
                "num_before": "0",
                "num_after": "1",
                "apply_markdown": "false",
                "allow_empty_topic_name": "true",
                "narrow": json.dumps(
                    [{"operator": "channel", "operand": stream_id}],
                    separators=(",", ":"),
                ),
            },
            timeout=self._chat_timeout(),
        )
        messages = payload.get("messages")
        found_oldest = payload.get("found_oldest")
        if not isinstance(messages, list) or not all(
            isinstance(message, dict) for message in messages
        ):
            raise ZulipApiError("invalid_messages_response", retryable=True)
        if found_oldest is not True:
            raise ZulipApiError("invalid_messages_response", retryable=True)
        if not messages:
            return None
        message_id = messages[0].get("id")
        if not isinstance(message_id, int):
            raise ZulipApiError("invalid_messages_response", retryable=True)
        return message_id

    def send_message(
        self,
        chat_key: str,
        own_user_id: int,
        content: str,
        *,
        topic: str | None,
        queue_id: str,
        local_id: str,
    ) -> int:
        if chat_key.startswith("channel:"):
            try:
                recipient: str = str(int(chat_key.removeprefix("channel:")))
            except ValueError as exc:
                raise ValueError("invalid channel chat key") from exc
            data = {
                "type": "stream",
                "to": recipient,
                "topic": topic or "General",
                "content": content,
            }
        elif chat_key.startswith("direct:"):
            try:
                participant_ids = {
                    int(value) for value in chat_key.removeprefix("direct:").split(",")
                }
            except ValueError as exc:
                raise ValueError("invalid direct chat key") from exc
            if own_user_id not in participant_ids:
                raise ValueError("direct chat key does not include current user")
            recipients = sorted(participant_ids - {own_user_id}) or [own_user_id]
            data = {
                "type": "private",
                "to": json.dumps(recipients, separators=(",", ":")),
                "content": content,
            }
        else:
            raise ValueError("unsupported chat key")
        data.update(
            {
                "queue_id": queue_id,
                "local_id": local_id,
                "read_by_sender": "true",
            }
        )
        payload = self._request("POST", "/api/v1/messages", data=data)
        message_id = payload.get("id")
        if not isinstance(message_id, int):
            raise ZulipApiError("invalid_send_message_response", retryable=True)
        return message_id

    def update_message(
        self,
        message_id: int,
        *,
        content: str | None = None,
        topic: str | None = None,
        propagate_mode: str = "change_one",
    ) -> None:
        data: dict[str, str] = {}
        if content is not None:
            data["content"] = content
        if topic is not None:
            data["topic"] = topic
            data["propagate_mode"] = propagate_mode
        if not data:
            return
        self._request("PATCH", f"/api/v1/messages/{message_id}", data=data)

    def delete_message(self, message_id: int) -> None:
        self._request("DELETE", f"/api/v1/messages/{message_id}")

    def update_message_flag(
        self,
        message_id: int,
        flag: str,
        enabled: bool,
    ) -> None:
        if flag not in {"read", "starred", "collapsed"}:
            raise ValueError("unsupported writable Zulip message flag")
        self._request(
            "POST",
            "/api/v1/messages/flags",
            data={
                "messages": json.dumps([message_id], separators=(",", ":")),
                "op": "add" if enabled else "remove",
                "flag": flag,
            },
        )

    def update_reaction(
        self,
        message_id: int,
        emoji_name: str,
        *,
        enabled: bool,
    ) -> None:
        self._request(
            "POST" if enabled else "DELETE",
            f"/api/v1/messages/{message_id}/reactions",
            data={"emoji_name": emoji_name},
        )

    def update_stream(
        self,
        stream_id: int,
        *,
        name: str | None = None,
        description: str | None = None,
        is_archived: bool | None = None,
    ) -> None:
        data: dict[str, str] = {}
        if name is not None:
            data["new_name"] = name
        if description is not None:
            data["description"] = description
        if is_archived is not None:
            data["is_archived"] = json.dumps(is_archived)
        if data:
            self._request("PATCH", f"/api/v1/streams/{stream_id}", data=data)

    def update_subscription(self, stream_name: str, *, enabled: bool) -> None:
        subscriptions: object = [{"name": stream_name}] if enabled else [stream_name]
        self._request(
            "POST" if enabled else "DELETE",
            "/api/v1/users/me/subscriptions",
            data={
                "subscriptions": json.dumps(
                    subscriptions,
                    separators=(",", ":"),
                )
            },
        )

    def create_stream(
        self,
        name: str,
        *,
        description: str | None,
        invite_only: bool,
    ) -> int:
        self._request(
            "POST",
            "/api/v1/users/me/subscriptions",
            data={
                "subscriptions": json.dumps(
                    [{"name": name, "description": description or ""}],
                    separators=(",", ":"),
                ),
                "invite_only": json.dumps(invite_only),
            },
        )
        payload = self._request(
            "GET",
            "/api/v1/get_stream_id",
            params={"stream": name},
        )
        stream_id = payload.get("stream_id")
        if not isinstance(stream_id, int):
            raise ZulipApiError("invalid_get_stream_id_response", retryable=True)
        return stream_id

    def update_subscription_property(
        self,
        stream_id: int,
        property_name: str,
        value: bool | str,
    ) -> None:
        self._request(
            "PATCH",
            f"/api/v1/users/me/subscriptions/{stream_id}",
            data={
                "property": property_name,
                "value": json.dumps(value) if isinstance(value, bool) else value,
            },
        )

    def update_topic_notification(
        self,
        stream_id: int,
        topic: str,
        *,
        visibility_policy: int,
    ) -> None:
        self._request(
            "POST",
            "/api/v1/user_topics",
            data={
                "stream_id": str(stream_id),
                "topic": topic,
                "visibility_policy": str(visibility_policy),
            },
        )

    def _chat_timeout(self) -> httpx.Timeout:
        return httpx.Timeout(
            self._connect_timeout_seconds,
            read=self._chat_fill_timeout_seconds,
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        data: Mapping[str, str] | None = None,
        params: Mapping[str, str] | None = None,
        timeout: httpx.Timeout | None = None,
    ) -> Mapping[str, Any]:
        response = self._client.request(
            method,
            f"{self._base_url}{path}",
            data=data,
            params=params,
            auth=self._auth,
            timeout=timeout,
        )
        return self._successful_payload(response)

    @staticmethod
    def _successful_payload(response: httpx.Response) -> Mapping[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise ZulipApiError(
                f"http_{response.status_code}_invalid_json",
                retryable=response.status_code >= 500,
                status_code=response.status_code,
            ) from exc
        if not isinstance(payload, Mapping):
            raise ZulipApiError("invalid_json_shape", retryable=True)
        if response.status_code >= 400 or payload.get("result") != "success":
            raw_code = payload.get("code")
            code = (
                raw_code
                if isinstance(raw_code, str)
                else f"http_{response.status_code}"
            )
            retryable = (
                response.status_code == 429
                or response.status_code >= 500
                or code == "BAD_EVENT_QUEUE_ID"
            )
            raise ZulipApiError(
                code,
                retryable=retryable,
                status_code=response.status_code,
            )
        return payload
