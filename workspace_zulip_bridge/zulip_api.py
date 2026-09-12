# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx

from workspace_zulip_bridge.models import DirectMessagePage
from workspace_zulip_bridge.models import MessagePage
from workspace_zulip_bridge.models import RecentPrivateConversation
from workspace_zulip_bridge.models import RegisteredQueue
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.models import ZulipIdentity


class ZulipApiError(Exception):
    def __init__(self, code: str, *, retryable: bool) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


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
        self._client = httpx.Client(
            verify=str(ca_file) if ca_file is not None else True,
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
                    ["recent_private_conversations"],
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
        return RegisteredQueue(
            queue_id,
            last_event_id,
            longpoll_timeout,
            tuple(conversations),
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
                )
            )
        return users

    def get_direct_messages_page(
        self,
        anchor: str | int,
        *,
        include_anchor: bool,
    ) -> DirectMessagePage:
        payload = self._request(
            "GET",
            "/api/v1/messages",
            params={
                "anchor": str(anchor),
                "include_anchor": json.dumps(include_anchor),
                "num_before": str(self._message_page_size),
                "num_after": "0",
                "apply_markdown": "false",
                "narrow": json.dumps(
                    [{"operator": "is", "operand": "dm"}],
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
        if not isinstance(found_oldest, bool):
            raise ZulipApiError("invalid_messages_response", retryable=True)
        return DirectMessagePage(messages=messages, found_oldest=found_oldest)

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

    def get_messages_by_ids(
        self,
        message_ids: list[int],
    ) -> list[Mapping[str, Any]]:
        if not message_ids:
            return []
        payload = self._request(
            "GET",
            "/api/v1/messages",
            params={
                "message_ids": json.dumps(message_ids, separators=(",", ":")),
                "apply_markdown": "false",
                "allow_empty_topic_name": "true",
            },
            timeout=self._chat_timeout(),
        )
        messages = payload.get("messages")
        if not isinstance(messages, list) or not all(
            isinstance(message, dict) for message in messages
        ):
            raise ZulipApiError("invalid_messages_response", retryable=True)
        return messages

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
            raise ZulipApiError(code, retryable=retryable)
        return payload
