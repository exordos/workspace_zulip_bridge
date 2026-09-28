# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import json
import ssl
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx

from workspace_zulip_bridge.models import RegisteredQueue
from workspace_zulip_bridge.models import ZulipIdentity

_SAFE_REMOTE_ERROR_CODES = frozenset(
    {
        "BAD_EVENT_QUEUE_ID",
        "BAD_REQUEST",
        "INVALID_API_KEY",
        "RATE_LIMIT_HIT",
        "REQUEST_VARIABLE_MISSING",
        "UNAUTHORIZED",
    }
)


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


class ZulipApiClient:
    """The Zulip transport needed to own and drain one event queue."""

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
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._base_url = endpoint.rstrip("/")
        self._connect_timeout_seconds = connect_timeout_seconds
        self._default_longpoll_timeout_seconds = default_longpoll_timeout_seconds
        self._idle_queue_timeout_seconds = idle_queue_timeout_seconds
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
            headers={"User-Agent": "workspace-zulip-bridge-v4"},
        )

    def close(self) -> None:
        self._client.close()

    def check_auth(self) -> None:
        self._request("GET", "/api/v1/users/me")

    def own_user_id(self) -> int:
        return self.own_user().user_id

    def own_user(self) -> ZulipIdentity:
        payload = self._request("GET", "/api/v1/users/me")
        user_id = payload.get("user_id")
        email = payload.get("email")
        full_name = payload.get("full_name")
        if (
            not isinstance(user_id, int)
            or not isinstance(email, str)
            or not email
            or not isinstance(full_name, str)
            or not full_name
        ):
            raise ZulipApiError("invalid_own_user_response", retryable=True)
        return ZulipIdentity(user_id, email, full_name)

    def register(self) -> RegisteredQueue:
        payload = self._request(
            "POST",
            "/api/v1/register",
            data={
                "event_types": json.dumps(
                    ["message", "update_message", "delete_message"],
                    separators=(",", ":"),
                ),
                "fetch_event_types": "[]",
                "apply_markdown": "false",
                "slim_presence": "true",
                "client_capabilities": json.dumps(
                    {
                        "bulk_message_deletion": True,
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
        raw_timeout = payload.get("event_queue_longpoll_timeout_seconds")
        longpoll_timeout = (
            float(raw_timeout)
            if isinstance(raw_timeout, int | float) and raw_timeout > 0
            else self._default_longpoll_timeout_seconds
        )
        return RegisteredQueue(queue_id, last_event_id, longpoll_timeout)

    def poll(
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
        if not isinstance(events, list):
            raise ZulipApiError("invalid_events_response", retryable=True)
        for event in events:
            if (
                not isinstance(event, Mapping)
                or not isinstance(event.get("id"), int)
                or not isinstance(event.get("type"), str)
            ):
                raise ZulipApiError("invalid_event", retryable=True)
        return events

    def get_message(self, message_id: int) -> Mapping[str, Any]:
        payload = self._request(
            "GET",
            f"/api/v1/messages/{message_id}",
            params={
                "apply_markdown": "false",
                "allow_empty_topic_name": "true",
            },
        )
        message = payload.get("message")
        if not isinstance(message, Mapping):
            raise ZulipApiError("invalid_message_response", retryable=True)
        return message

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
                recipient = str(int(chat_key.removeprefix("channel:")))
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
        data.update({"queue_id": queue_id, "local_id": local_id})
        payload = self._request("POST", "/api/v1/messages", data=data)
        message_id = payload.get("id")
        if not isinstance(message_id, int):
            raise ZulipApiError("invalid_send_message_response", retryable=True)
        return message_id

    def update_message(
        self,
        message_id: int,
        *,
        content: str,
        topic: str | None,
    ) -> None:
        data = {"content": content}
        if topic is not None:
            data.update({"topic": topic, "propagate_mode": "change_one"})
        self._request("PATCH", f"/api/v1/messages/{message_id}", data=data)

    def delete_message(self, message_id: int) -> None:
        self._request("DELETE", f"/api/v1/messages/{message_id}")

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
                if isinstance(raw_code, str) and raw_code in _SAFE_REMOTE_ERROR_CODES
                else f"http_{response.status_code}_zulip_error"
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
