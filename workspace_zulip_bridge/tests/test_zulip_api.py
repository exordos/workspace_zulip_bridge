# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import json
from urllib.parse import parse_qs

import httpx
import pytest

from workspace_zulip_bridge.zulip_api import ZulipApiClient
from workspace_zulip_bridge.zulip_api import ZulipApiError


def _client(handler: object) -> ZulipApiClient:
    return ZulipApiClient(
        "https://zulip.example.test",
        "user@example.test",
        "not-a-real-key",
        ca_file=None,
        connect_timeout_seconds=2,
        default_longpoll_timeout_seconds=180,
        idle_queue_timeout_seconds=3600,
        transport=httpx.MockTransport(handler),  # type: ignore[arg-type]
    )


def test_registers_only_live_message_events_without_initial_state() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/register"):
            form = parse_qs(request.content.decode())
            assert json.loads(form["fetch_event_types"][0]) == []
            assert json.loads(form["event_types"][0]) == [
                "message",
                "update_message",
                "delete_message",
            ]
            assert form["apply_markdown"] == ["false"]
            return httpx.Response(
                200,
                json={
                    "result": "success",
                    "queue_id": "queue-1",
                    "last_event_id": -1,
                    "event_queue_longpoll_timeout_seconds": 90,
                },
            )
        return httpx.Response(
            200,
            json={
                "result": "success",
                "events": [
                    {"id": 4, "type": "message", "sensitive": "discarded"},
                    {"id": 5, "type": "heartbeat"},
                ],
            },
        )

    client = _client(handler)
    try:
        queue = client.register()
        assert queue.queue_id == "queue-1"
        assert client.poll(queue.queue_id, queue.last_event_id, 90) == [
            {"id": 4, "type": "message", "sensitive": "discarded"},
            {"id": 5, "type": "heartbeat"},
        ]
    finally:
        client.close()
    assert len(requests) == 2


def test_expired_queue_is_retryable() -> None:
    client = _client(
        lambda _request: httpx.Response(
            400,
            json={"result": "error", "code": "BAD_EVENT_QUEUE_ID"},
        )
    )
    try:
        with pytest.raises(ZulipApiError) as error:
            client.poll("expired", 3, 90)
    finally:
        client.close()

    assert error.value.code == "BAD_EVENT_QUEUE_ID"
    assert error.value.retryable


def test_auth_check_discards_profile_payload() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "result": "success",
                "user_id": 42,
                "full_name": "Discarded profile",
            },
        )

    client = _client(handler)
    try:
        client.check_auth()
    finally:
        client.close()

    assert [request.url.path for request in requests] == ["/api/v1/users/me"]


def test_realtime_message_methods_use_only_point_operations() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "result": "success",
                    "message": {"id": 71, "content": "edited"},
                },
            )
        if request.method == "POST":
            return httpx.Response(200, json={"result": "success", "id": 71})
        return httpx.Response(200, json={"result": "success"})

    client = _client(handler)
    try:
        assert client.get_message(71)["content"] == "edited"
        assert (
            client.send_message(
                "channel:9",
                42,
                "hello",
                topic="Realtime",
                queue_id="queue-1",
                local_id="local-1",
            )
            == 71
        )
        client.update_message(71, content="edited", topic="Realtime")
        client.delete_message(71)
    finally:
        client.close()

    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/messages/71"),
        ("POST", "/api/v1/messages"),
        ("PATCH", "/api/v1/messages/71"),
        ("DELETE", "/api/v1/messages/71"),
    ]
    send_form = parse_qs(requests[1].content.decode())
    assert send_form["queue_id"] == ["queue-1"]
    assert send_form["local_id"] == ["local-1"]
