# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from uuid import uuid4

import pytest

from workspace_zulip_bridge.stable_ids import canonical_endpoint
from workspace_zulip_bridge.stable_ids import stable_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid


def test_endpoint_and_provider_entities_have_stable_uuid5_values() -> None:
    endpoint = "HTTPS://Zulip.Example.Test:443/"
    canonical = "https://zulip.example.test"

    assert canonical_endpoint(endpoint) == canonical
    assert stable_user_uuid(endpoint, 42) == stable_user_uuid(canonical, 42)
    assert stable_chat_uuid(endpoint, "channel:7") == stable_chat_uuid(
        canonical, "channel:7"
    )
    assert stable_message_uuid(endpoint, 123) == stable_message_uuid(canonical, 123)
    assert stable_user_uuid(endpoint, 42).version == 5
    assert stable_chat_uuid(endpoint, "channel:7").version == 5
    assert stable_message_uuid(endpoint, 123).version == 5


def test_entity_namespaces_and_provider_keys_do_not_collide() -> None:
    endpoint = "https://zulip.example.test"

    assert stable_user_uuid(endpoint, 7) != stable_message_uuid(endpoint, 7)
    assert stable_user_uuid(endpoint, 7) != stable_user_uuid(endpoint, 8)
    assert stable_chat_uuid(endpoint, "channel:7") != stable_chat_uuid(
        endpoint, "channel:8"
    )
    assert stable_chat_uuid(endpoint, "channel:7") != stable_chat_uuid(
        "https://other.example.test", "channel:7"
    )


def test_topic_ids_are_stable_within_their_chat() -> None:
    chat_uuid = stable_chat_uuid("https://zulip.example.test", "channel:7")

    assert stable_topic_uuid(chat_uuid, "Performance") == stable_topic_uuid(
        chat_uuid, "Performance"
    )
    assert stable_topic_uuid(chat_uuid, "Performance").version == 5
    assert stable_topic_uuid(chat_uuid, "Performance") != stable_topic_uuid(
        chat_uuid, "Operations"
    )
    assert stable_topic_uuid(chat_uuid, "Performance") != stable_topic_uuid(
        uuid4(), "Performance"
    )


@pytest.mark.parametrize(
    "endpoint",
    ["", "zulip.example.test", "ftp://zulip.example.test", "https:///missing-host"],
)
def test_invalid_endpoints_are_rejected(endpoint: str) -> None:
    with pytest.raises(ValueError):
        canonical_endpoint(endpoint)
