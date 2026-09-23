# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import json
import ssl
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx

from workspace_zulip_bridge.models import RegisteredQueue

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

    def register(self) -> RegisteredQueue:
        payload = self._request(
            "POST",
            "/api/v1/register",
            data={
                # Initial state is intentionally empty. Omitting event_types keeps
                # the queue subscribed to all live event types.
                "fetch_event_types": "[]",
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
    ) -> int:
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
        next_event_id = last_event_id
        for event in events:
            if not isinstance(event, Mapping) or not isinstance(event.get("id"), int):
                raise ZulipApiError("invalid_event", retryable=True)
            next_event_id = max(next_event_id, event["id"])
        return next_event_id

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
