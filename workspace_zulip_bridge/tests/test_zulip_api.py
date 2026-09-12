# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import json
from urllib.parse import parse_qs

import httpx
import pytest

from workspace_zulip_bridge.zulip_api import ZulipApiClient
from workspace_zulip_bridge.zulip_api import ZulipApiError


def test_register_and_get_events() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/register"):
            assert request.headers["authorization"].startswith("Basic ")
            form = parse_qs(request.content.decode())
            assert json.loads(form["fetch_event_types"][0]) == [
                "recent_private_conversations"
            ]
            assert form["idle_queue_timeout"] == ["3600"]
            return httpx.Response(
                200,
                json={
                    "result": "success",
                    "msg": "",
                    "queue_id": "queue-1",
                    "last_event_id": -1,
                    "event_queue_longpoll_timeout_seconds": 90,
                    "recent_private_conversations": [
                        {"user_ids": [12, 11], "max_message_id": 42}
                    ],
                },
            )
        assert request.url.path.endswith("/events")
        assert request.url.params["queue_id"] == "queue-1"
        assert request.url.params["last_event_id"] == "-1"
        return httpx.Response(
            200,
            json={
                "result": "success",
                "msg": "",
                "events": [{"id": 1, "type": "heartbeat"}],
            },
        )

    client = _client(handler)
    try:
        queue = client.register()
        assert queue.queue_id == "queue-1"
        assert queue.last_event_id == -1
        assert queue.longpoll_timeout_seconds == 90
        assert queue.recent_private_conversations[0].user_ids == (11, 12)
        assert client.get_events("queue-1", -1, 90) == [{"id": 1, "type": "heartbeat"}]
    finally:
        client.close()

    assert len(requests) == 2


def test_register_uses_default_longpoll_timeout_when_initial_state_is_empty() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "result": "success",
                "msg": "",
                "queue_id": "queue-1",
                "last_event_id": -1,
                "recent_private_conversations": [],
            },
        )

    client = _client(handler)
    try:
        assert client.register().longpoll_timeout_seconds == 180
    finally:
        client.close()


def test_bad_event_queue_id_is_classified() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={
                "result": "error",
                "msg": "queue expired",
                "code": "BAD_EVENT_QUEUE_ID",
            },
        )

    client = _client(handler)
    try:
        with pytest.raises(ZulipApiError) as error:
            client.get_events("expired", 12, 90)
        assert error.value.code == "BAD_EVENT_QUEUE_ID"
        assert error.value.retryable
    finally:
        client.close()


def test_chat_catalog_endpoints_and_direct_message_pagination() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/users/me/subscriptions"):
            assert request.url.params["include_subscribers"] == "false"
            return httpx.Response(
                200,
                json={
                    "result": "success",
                    "msg": "",
                    "subscriptions": [{"stream_id": 7, "name": "General"}],
                },
            )
        if request.url.path.endswith("/users/me"):
            return httpx.Response(
                200,
                json={
                    "result": "success",
                    "msg": "",
                    "user_id": 42,
                    "full_name": "Test User",
                    "role": 400,
                },
            )
        assert request.url.path.endswith("/messages")
        assert request.url.params["anchor"] == "123"
        assert request.url.params["include_anchor"] == "false"
        assert request.url.params["num_before"] == "5000"
        assert request.url.params["num_after"] == "0"
        assert request.url.params["apply_markdown"] == "false"
        assert json.loads(request.url.params["narrow"]) == [
            {"operator": "is", "operand": "dm"}
        ]
        return httpx.Response(
            200,
            json={
                "result": "success",
                "msg": "",
                "found_oldest": True,
                "messages": [{"id": 122, "display_recipient": []}],
            },
        )

    client = _client(handler)
    try:
        assert client.get_own_user().user_id == 42
        assert client.get_subscriptions() == [{"stream_id": 7, "name": "General"}]
        page = client.get_direct_messages_page(123, include_anchor=False)
        assert page.found_oldest
        assert page.messages == [{"id": 122, "display_recipient": []}]
    finally:
        client.close()

    assert len(requests) == 3


def test_user_directory_and_full_message_history_endpoints() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/users"):
            assert request.url.params["client_gravatar"] == "true"
            assert request.url.params["include_custom_profile_fields"] == "false"
            return httpx.Response(
                200,
                json={
                    "result": "success",
                    "msg": "",
                    "members": [
                        {
                            "user_id": 10,
                            "email": "human@example.test",
                            "full_name": "Human",
                            "is_active": True,
                            "is_bot": False,
                            "role": 400,
                        },
                        {
                            "user_id": 99,
                            "email": "bot@example.test",
                            "full_name": "Bot",
                            "is_active": False,
                            "is_bot": True,
                            "role": 600,
                        },
                    ],
                },
            )
        assert request.url.path.endswith("/messages")
        assert request.url.params["anchor"] == "newest"
        assert request.url.params["include_anchor"] == "true"
        assert request.url.params["num_before"] == "5000"
        assert request.url.params["allow_empty_topic_name"] == "true"
        assert "narrow" not in request.url.params
        return httpx.Response(
            200,
            json={
                "result": "success",
                "msg": "",
                "found_oldest": True,
                "messages": [{"id": 123}],
            },
        )

    client = _client(handler)
    try:
        users = client.get_users()
        page = client.get_messages_page("newest", include_anchor=True)
    finally:
        client.close()

    assert [(user.user_id, user.disabled, user.is_bot) for user in users] == [
        (10, False, False),
        (99, True, True),
    ]
    assert page.messages == [{"id": 123}]
    assert page.found_oldest
    assert len(requests) == 2


def test_messages_can_be_refetched_by_id_for_live_updates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/messages")
        assert json.loads(request.url.params["message_ids"]) == [4, 8, 15]
        assert request.url.params["apply_markdown"] == "false"
        assert request.url.params["allow_empty_topic_name"] == "true"
        assert "anchor" not in request.url.params
        return httpx.Response(
            200,
            json={
                "result": "success",
                "msg": "",
                "messages": [{"id": 4}, {"id": 15}],
            },
        )

    client = _client(handler)
    try:
        messages = client.get_messages_by_ids([4, 8, 15])
    finally:
        client.close()

    assert messages == [{"id": 4}, {"id": 15}]


@pytest.mark.parametrize(
    ("chat_key", "own_user_id", "expected_narrow"),
    [
        ("channel:7", 10, [{"operator": "channel", "operand": 7}]),
        (
            "direct:10,12,13",
            10,
            [{"operator": "dm", "operand": [12, 13]}],
        ),
    ],
)
def test_history_pages_are_narrowed_to_one_scheduled_chat(
    chat_key: str,
    own_user_id: int,
    expected_narrow: list[dict[str, object]],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/messages")
        assert json.loads(request.url.params["narrow"]) == expected_narrow
        return httpx.Response(
            200,
            json={
                "result": "success",
                "msg": "",
                "found_oldest": True,
                "messages": [],
            },
        )

    client = _client(handler)
    try:
        page = client.get_chat_messages_page(
            chat_key,
            own_user_id,
            "newest",
            include_anchor=True,
        )
    finally:
        client.close()
    assert page.found_oldest


def _client(handler: object) -> ZulipApiClient:
    return ZulipApiClient(
        "https://zulip.example.test",
        "user@example.test",
        "not-a-real-api-key",
        ca_file=None,
        connect_timeout_seconds=2,
        default_longpoll_timeout_seconds=180,
        idle_queue_timeout_seconds=3600,
        chat_fill_timeout_seconds=120,
        message_page_size=5000,
        transport=httpx.MockTransport(handler),  # type: ignore[arg-type]
    )
