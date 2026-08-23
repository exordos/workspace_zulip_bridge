import datetime

import pytest
import requests
import zulip

from workspace_zulip_bridge import converter, zulip_adapter

OWNER_UUID = "10000000-0000-4000-8000-000000000001"
AUTHOR_UUID = OWNER_UUID
STREAM_UUID = "10000000-0000-4000-8000-000000000002"
TOPIC_UUID = "10000000-0000-4000-8000-000000000003"
MESSAGE_UUID = "10000000-0000-4000-8000-000000000004"
USER_2_UUID = "10000000-0000-4000-8000-000000000005"
USER_3_UUID = "10000000-0000-4000-8000-000000000006"
EXTERNAL_CHAT_UUID = "10000000-0000-4000-8000-000000000007"
DIRECT_STREAM_UUID = "10000000-0000-4000-8000-000000000008"
DIRECT_TOPIC_UUID = "10000000-0000-4000-8000-000000000009"
SELF_STREAM_UUID = "10000000-0000-4000-8000-000000000010"


@pytest.mark.parametrize(
    "response",
    [
        {"result": "error", "code": "BAD_API_KEY"},
        {
            "result": "error",
            "code": "BAD_REQUEST",
            "msg": "Invalid API key",
        },
    ],
)
def test_provider_authentication_failures_use_account_quarantine_code(response):
    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        zulip_adapter._successful(response)

    assert captured.value.code == "unauthorized_account"
    assert not captured.value.retryable


class FakeClient:
    def __init__(self):
        self.base_url = "https://zulip.example.invalid/api/"
        self.feature_level = 500
        self.sent = []
        self.updated = []
        self.flags = []
        self.subscription_settings = []
        self.deleted = []
        self.fail_send = False
        self.messages = []
        self.event_requests = []
        self.endpoint_requests = []
        self.stream_updates = []
        self.read_streams = []
        self.read_topics = []
        self.added_reactions = []
        self.removed_reactions = []
        self.added_subscriptions = []
        self.removed_subscriptions = []
        self.add_reaction_result = {"result": "success"}
        self.remove_reaction_result = {"result": "success"}
        self.uploads = []
        self.registration_request = None
        self.subscriptions_request = None
        self.users_request = None
        self.subscriptions_requests = 0
        self.users_requests = 0
        self.profile_requests = 0
        self.user_requests = []
        self.subscriptions = [
            {
                "stream_id": 42,
                "name": "Engineering",
                "subscribers": [1, 2],
                "is_muted": False,
                "desktop_notifications": None,
            }
        ]
        self.members = [
            {
                "user_id": 1,
                "full_name": "Owner",
                "email": "owner@example.invalid",
            },
            {
                "user_id": 2,
                "full_name": "Other User",
                "email": "other@example.invalid",
            },
        ]

    def register(self, **kwargs):
        self.registration_request = kwargs
        return {
            "result": "success",
            "queue_id": "queue-1",
            "last_event_id": 7,
            "user_id": 1,
            "realm_uuid": "00000000-0000-4000-8000-000000000001",
            "user_settings": {"enable_stream_desktop_notifications": True},
        }

    def get_subscriptions(self, request=None):
        self.subscriptions_request = request
        self.subscriptions_requests += 1
        return {
            "result": "success",
            "subscriptions": self.subscriptions,
        }

    def get_events(self, **kwargs):
        self.event_requests.append(kwargs)
        return {"result": "success", "events": []}

    def call_endpoint(
        self,
        *,
        url,
        method,
        request,
        longpolling=False,
        timeout=None,
    ):
        self.endpoint_requests.append(
            {
                "url": url,
                "method": method,
                "request": request,
                "longpolling": longpolling,
                "timeout": timeout,
            }
        )
        if url == "events":
            return self.get_events(**request)
        if url == "user_topics":
            return {"result": "success"}
        raise AssertionError(f"Unexpected endpoint: {url}")

    def get_profile(self):
        self.profile_requests += 1
        return {"result": "success", "user_id": 1}

    def get_users(self, request=None):
        self.users_request = request
        self.users_requests += 1
        return {"result": "success", "members": self.members}

    def get_user_by_id(self, user_id, **request):
        self.user_requests.append((user_id, request))
        member = next(
            (member for member in self.members if member["user_id"] == user_id),
            None,
        )
        if member is None:
            return {"result": "error", "code": "BAD_REQUEST"}
        return {"result": "success", "user": member}

    def get_messages(self, request):
        self.last_get_messages = request
        return {"result": "success", "messages": self.messages}

    def send_message(self, request):
        self.sent.append(request)
        if self.fail_send:
            raise requests.Timeout("lost response")
        return {"result": "success", "id": 99}

    def update_message(self, request):
        self.updated.append(request)
        return {"result": "success"}

    def update_stream(self, request):
        self.stream_updates.append(request)
        return {"result": "success"}

    def delete_message(self, message_id):
        self.deleted.append(message_id)
        return {"result": "success"}

    def update_message_flags(self, request):
        self.flags.append(request)
        return {"result": "success"}

    def update_subscription_settings(self, subscription_data):
        self.subscription_settings.append(subscription_data)
        return {"result": "success"}

    def mark_stream_as_read(self, stream_id):
        self.read_streams.append(stream_id)
        return {"result": "success"}

    def mark_topic_as_read(self, stream_id, topic_name):
        self.read_topics.append((stream_id, topic_name))
        return {"result": "success"}

    def add_reaction(self, request):
        self.added_reactions.append(request)
        return self.add_reaction_result

    def remove_reaction(self, request):
        self.removed_reactions.append(request)
        return self.remove_reaction_result

    def add_subscriptions(self, streams, **kwargs):
        self.added_subscriptions.append((streams, kwargs))
        return {"result": "success"}

    def remove_subscriptions(self, streams, principals=None):
        self.removed_subscriptions.append((streams, principals))
        return {"result": "success"}

    def upload_file(self, file):
        self.uploads.append((file.name, file.read()))
        return {"result": "success", "uri": "/user_uploads/file"}


class FakeRouting:
    streams = {
        "channel:42": {
            "metadata": {
                "chat_type": "channel",
                "name": "engineering",
                "participants": [],
            }
        },
        "direct:2": {
            "metadata": {
                "chat_type": "direct",
                "name": "Direct message",
                "participants": [OWNER_UUID, USER_2_UUID],
            }
        },
        "group_direct:2,3": {
            "metadata": {
                "chat_type": "group_direct",
                "name": "Group direct message",
                "participants": [OWNER_UUID, USER_2_UUID, USER_3_UUID],
            }
        },
    }
    workspace = {
        ("stream", STREAM_UUID): {
            "provider_id": "channel:42",
            "metadata": {
                "chat_type": "channel",
                "name": "engineering",
                "participants": [],
            },
        },
        ("topic", TOPIC_UUID): {
            "provider_id": "42:bridge",
            "metadata": {"stream_uuid": STREAM_UUID},
        },
        ("message", MESSAGE_UUID): {
            "provider_id": "99",
            "metadata": {
                "chat_key": "channel:42",
                "stream_uuid": STREAM_UUID,
                "topic_uuid": TOPIC_UUID,
                "subject": "bridge",
            },
        },
        ("stream", DIRECT_STREAM_UUID): {
            "provider_id": "direct:2",
            "metadata": {
                "chat_type": "direct",
                "name": "Direct message",
                "participants": [OWNER_UUID, USER_2_UUID],
            },
        },
        ("topic", DIRECT_TOPIC_UUID): {
            "provider_id": "direct:1,2:default",
            "metadata": {
                "stream_uuid": DIRECT_STREAM_UUID,
                "chat_key": "direct:1,2",
            },
        },
        ("stream", SELF_STREAM_UUID): {
            "provider_id": "direct:1",
            "metadata": {
                "chat_type": "direct",
                "name": "Direct message",
                "participants": [OWNER_UUID],
            },
        },
        ("identity", OWNER_UUID): {
            "provider_id": "1",
            "metadata": {"display_name": "Owner"},
        },
        ("identity", USER_2_UUID): {
            "provider_id": "2",
            "metadata": {"display_name": "Other User"},
        },
        ("identity", USER_3_UUID): {
            "provider_id": "3",
            "metadata": {"display_name": "Third User"},
        },
    }

    def provider_mapping(self, entity_kind, provider_id):
        if entity_kind != "stream":
            return None
        return self.streams.get(provider_id)

    def workspace_mapping(self, entity_kind, workspace_uuid):
        return self.workspace.get((entity_kind, workspace_uuid))

    def topic_message_mapping(self, topic_uuid):
        if topic_uuid != TOPIC_UUID:
            return None
        return {"provider_id": "99", "metadata": {"topic_uuid": TOPIC_UUID}}

    def external_chat_uuid(self, provider_chat_key):
        return EXTERNAL_CHAT_UUID


def _operation(chat_kind="channel"):
    chat_key = {
        "channel": "channel:42",
        "personal_dm": "direct:2",
        "group_dm": "group_direct:2,3",
    }[chat_kind]
    return {
        "kind": "message.create",
        "provider": {
            "kind": "zulip",
            "chat_id": chat_key,
            "entity_id": None,
            "revision": None,
        },
        "payload": {
            "stream_uuid": STREAM_UUID,
            "topic_uuid": TOPIC_UUID,
            "author_uuid": AUTHOR_UUID,
            "payload": {"kind": "markdown", "content": "hello"},
            "reply_to_message_uuid": None,
        },
    }


def _adapter(client, routing=None):
    adapter = zulip_adapter.OfficialZulipAdapter(
        client=client,
        routing=FakeRouting() if routing is None else routing,
        owner_user_uuid=OWNER_UUID,
    )
    adapter.restore_queue("queue-1", 7)
    return adapter


def test_outbound_prepare_never_registers_or_replaces_the_live_queue():
    client = FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(
        client=client, routing=FakeRouting(), owner_user_uuid=OWNER_UUID
    )

    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter.prepare(_operation(), "operation-1")

    assert error.value.code == "provider_unavailable"
    assert error.value.retryable
    assert client.registration_request is None


def test_selected_channel_catalog_reads_authoritative_subscribers_and_users():
    client = FakeClient()
    adapter = _adapter(client)

    catalog = adapter.channel_catalog("channel:42")

    assert client.subscriptions_request == {"include_subscribers": True}
    assert client.users_request == {"include_deactivated": True}
    assert catalog == {
        "subscriptions": [
            {
                "stream_id": 42,
                "name": "Engineering",
                "subscribers": [1, 2],
                "is_muted": False,
                "desktop_notifications": None,
            }
        ],
        "realm_users": client.members,
        "user_id": 1,
    }


def test_selected_channel_catalog_rejects_unknown_stream():
    client = FakeClient()
    adapter = _adapter(client)

    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter.channel_catalog("channel:404")

    assert error.value.code == "invalid_record"
    assert not error.value.retryable


def test_selected_channel_catalog_batches_account_snapshot_requests():
    client = FakeClient()
    client.subscriptions.append(
        {
            "stream_id": 43,
            "name": "Operations",
            "subscribers": [1],
        }
    )
    adapter = _adapter(client)

    catalog = adapter.channel_catalogs(["channel:42", "channel:43"])

    assert [subscription["stream_id"] for subscription in catalog["subscriptions"]] == [
        42,
        43,
    ]
    assert client.subscriptions_requests == 1
    assert client.users_requests == 1
    assert client.profile_requests == 1


@pytest.mark.parametrize("chat_kind", ["channel", "personal_dm", "group_dm"])
def test_zb_msg_001_message_mapping_uses_official_client_semantics(chat_kind):
    client = FakeClient()
    adapter = _adapter(client)
    correlation = adapter.prepare(_operation(chat_kind), "operation-1")
    assert correlation is not None
    assert adapter.apply(_operation(chat_kind), correlation) == ("99", None)
    request = client.sent[0]
    assert request["queue_id"] == "queue-1"
    assert request["local_id"] == "operation-1"
    assert request["type"] == ("stream" if chat_kind == "channel" else "private")
    if chat_kind == "channel":
        assert request["to"] == "engineering"
        assert request["topic"] == "bridge"
    else:
        assert request["to"] == ([2] if chat_kind == "personal_dm" else [2, 3])


def test_outbound_mentions_and_attachments_use_provider_formats_without_raw_urns():
    class FileClient:
        def __init__(self):
            self.exports = []

        def export_file(self, *args, **kwargs):
            self.exports.append((args, kwargs))
            return "report.pdf", "application/pdf", b"pdf-bytes"

    client = FakeClient()
    file_client = FileClient()
    adapter = zulip_adapter.OfficialZulipAdapter(
        client=client,
        routing=FakeRouting(),
        owner_user_uuid=OWNER_UUID,
        account_uuid=OWNER_UUID,
        file_client=file_client,
        file_limit=lambda: 1024,
    )
    adapter.restore_queue("queue-1", 7)
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        f"[Other User](urn:user:{USER_2_UUID}) "
        "[report.pdf](urn:file:10000000-0000-4000-8000-000000000008)"
    )
    correlation = adapter.prepare(operation, MESSAGE_UUID)

    assert adapter.apply(operation, correlation) == ("99", None)
    assert client.sent[0]["content"] == (
        "@**Other User|2** [report.pdf](/user_uploads/file)"
    )
    assert "urn:" not in client.sent[0]["content"]
    assert client.uploads == [("report.pdf", b"pdf-bytes")]
    export_args = file_client.exports[0][0]
    assert str(export_args[2]) == OWNER_UUID
    assert str(export_args[3]) == EXTERNAL_CHAT_UUID


def test_outbound_workspace_links_use_zulip_native_and_url_formats():
    client = FakeClient()
    adapter = _adapter(client)
    operation = _operation()
    operation["payload"]["payload"]["content"] = " ".join(
        (
            f"[Workspace alias](urn:user:{USER_2_UUID})",
            f"[channel](urn:stream:{STREAM_UUID})",
            f"[topic](urn:topic:{TOPIC_UUID})",
            f"[message](urn:message:{MESSAGE_UUID})",
            f"[dm](urn:stream:{DIRECT_STREAM_UUID})",
            f"[dm topic](urn:topic:{DIRECT_TOPIC_UUID})",
            f"[self dm](urn:stream:{SELF_STREAM_UUID})",
            "[site](urn:url:https://example.com/a?x=1#section)",
        )
    )

    correlation = adapter.prepare(operation, "operation-1")

    assert correlation is not None
    assert correlation.provider_rendered_content == " ".join(
        (
            "@**Other User|2**",
            "#**engineering**",
            "#**engineering>bridge**",
            "#**engineering>bridge@99**",
            "[dm](https://zulip.example.invalid/#narrow/dm/2-user)",
            "[dm topic](https://zulip.example.invalid/#narrow/dm/2-user)",
            "[self dm](https://zulip.example.invalid/#narrow/dm/1-user)",
            "[site](https://example.com/a?x=1#section)",
        )
    )
    assert "urn:" not in correlation.provider_rendered_content


def test_reply_uses_zulip_native_quote_and_reply_semantics():
    client = FakeClient()
    client.messages = [
        {
            "id": 99,
            "sender_id": 2,
            "sender_full_name": "Other User",
            "content": "original text",
        }
    ]
    operation = _operation()
    operation["payload"]["reply_to_message_uuid"] = MESSAGE_UUID
    adapter = _adapter(client)
    correlation = adapter.prepare(operation, "operation-1")

    assert correlation.provider_rendered_content == (
        "@_**Other User|2** "
        "[said](https://zulip.example.invalid/#narrow/near/99):\n"
        "```quote\noriginal text\n```\n\nhello"
    )
    assert client.last_get_messages["narrow"] == [{"operator": "id", "operand": 99}]
    assert adapter.apply(operation, correlation) == ("99", None)
    assert client.sent[0]["content"] == correlation.provider_rendered_content


def test_canonical_workspace_quote_uses_zulip_native_reply_semantics():
    client = FakeClient()
    client.messages = [
        {
            "id": 99,
            "sender_id": 2,
            "sender_full_name": "Other User",
            "content": "original text\n```text\nnested code\n```",
        }
    ]
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        f"[Other User](urn:quote:{MESSAGE_UUID})\n\nreply text"
    )
    adapter = _adapter(client)

    correlation = adapter.prepare(operation, "operation-1")

    assert correlation.provider_rendered_content == (
        "@_**Other User|2** "
        "[said](https://zulip.example.invalid/#narrow/near/99):\n"
        "````quote\noriginal text\n```text\nnested code\n```\n````\n\n"
        "reply text"
    )
    assert "urn:quote:" not in correlation.provider_rendered_content


def test_canonical_workspace_selected_quote_preserves_exact_selected_text():
    client = FakeClient()
    client.messages = [
        {
            "id": 99,
            "sender_id": 2,
            "sender_full_name": "Other User",
            "content": "full original text",
        }
    ]
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        f"[Other User](urn:quote:{MESSAGE_UUID}"
        "?text=%20Selected%20text%0Asecond%20line%20%26%20%23%20)\n\nreply"
    )
    adapter = _adapter(client)

    correlation = adapter.prepare(operation, "operation-1")

    assert "```quote\n Selected text\nsecond line & # \n```" in (
        correlation.provider_rendered_content
    )
    assert "full original text" not in correlation.provider_rendered_content
    assert "urn:quote:" not in correlation.provider_rendered_content


def test_canonical_quote_deduplicates_matching_reply_payload_reference():
    client = FakeClient()
    client.messages = [
        {
            "id": 99,
            "sender_id": 2,
            "sender_full_name": "Other User",
            "content": "original text",
        }
    ]
    operation = _operation()
    operation["payload"]["reply_to_message_uuid"] = MESSAGE_UUID
    operation["payload"]["payload"]["content"] = (
        f"[Other User](urn:quote:{MESSAGE_UUID})\n\nreply"
    )
    adapter = _adapter(client)

    correlation = adapter.prepare(operation, "operation-1")

    assert correlation.provider_rendered_content.count("[said](") == 1
    assert correlation.provider_rendered_content.endswith("\n\nreply")


@pytest.mark.parametrize(
    "query",
    (
        "unknown=value",
        "text=first&text=second",
        "text=%E0%A4%A",
    ),
)
def test_canonical_quote_with_invalid_query_fails_without_leaking_raw_urn(query):
    client = FakeClient()
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        f"[Other User](urn:quote:{MESSAGE_UUID}?{query})\n\nreply"
    )
    adapter = _adapter(client)

    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter.prepare(operation, "operation-1")

    assert error.value.code == "invalid_record"
    assert not error.value.retryable
    assert client.sent == []


def test_canonical_quote_inside_fenced_code_remains_literal():
    client = FakeClient()
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        f"````md\n[Other User](urn:quote:{MESSAGE_UUID})\n```text\nnested\n```\n````"
    )
    adapter = _adapter(client)

    correlation = adapter.prepare(operation, "operation-1")

    assert (
        correlation.provider_rendered_content
        == operation["payload"]["payload"]["content"]
    )
    assert not hasattr(client, "last_get_messages")


def test_canonical_quote_without_provider_mapping_fails_not_found():
    client = FakeClient()
    routing = FakeRouting()
    routing.workspace = dict(routing.workspace)
    routing.workspace.pop(("message", MESSAGE_UUID))
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        f"[Other User](urn:quote:{MESSAGE_UUID})\n\nreply"
    )
    adapter = _adapter(client, routing)

    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter.prepare(operation, "operation-1")

    assert error.value.code == "not_found"
    assert not error.value.retryable
    assert not hasattr(client, "last_get_messages")


def test_workspace_links_inside_code_remain_literal():
    client = FakeClient()
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        f"`[user](urn:user:{USER_2_UUID}) "
        "[docs](urn:url:https://example.com/inline)`\n"
        "```markdown\n"
        f"[message](urn:message:{MESSAGE_UUID})\n"
        "[docs](urn:url:https://example.com/fenced)\n"
        "```\n\n"
        "    [docs](urn:url:https://example.com/indented)"
    )
    adapter = _adapter(client)

    correlation = adapter.prepare(operation, "operation-1")

    assert (
        correlation.provider_rendered_content
        == (operation["payload"]["payload"]["content"])
    )


def test_external_link_title_round_trips_between_zulip_and_workspace():
    zulip_content = '[docs](https://example.com/reference "Documentation")'
    workspace_content, lossy = converter.convert_markdown(
        zulip_content,
        {},
        "https://zulip.example.invalid/#narrow/near/99",
    )
    client = FakeClient()
    operation = _operation()
    operation["payload"]["payload"]["content"] = workspace_content
    adapter = _adapter(client)

    correlation = adapter.prepare(operation, "operation-1")

    assert not lossy
    assert correlation.provider_rendered_content == zulip_content


def test_workspace_reference_link_uses_zulip_url_and_drops_definition():
    client = FakeClient()
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        "[docs][reference]\n\n[reference]: urn:url:https://example.com/reference"
    )
    adapter = _adapter(client)

    correlation = adapter.prepare(operation, "operation-1")

    assert correlation.provider_rendered_content == (
        "[docs](https://example.com/reference)\n\n"
    )
    assert "urn:" not in correlation.provider_rendered_content


@pytest.mark.parametrize(
    "literal_code",
    (
        "`[docs][reference]`",
        "```markdown\n[docs][reference]\n```",
    ),
)
def test_workspace_reference_definition_is_kept_for_literal_code(
    literal_code,
):
    content = (
        "[docs][reference]\n\n"
        f"{literal_code}\n\n"
        "[reference]: urn:url:https://example.com/reference"
    )
    client = FakeClient()
    operation = _operation()
    operation["payload"]["payload"]["content"] = content
    adapter = _adapter(client)

    correlation = adapter.prepare(operation, "operation-1")

    assert correlation.provider_rendered_content == content.replace(
        "[docs][reference]",
        "[docs](https://example.com/reference)",
        1,
    )


def test_nested_workspace_image_and_link_destinations_are_both_converted():
    client = FakeClient()
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        "[![diagram](urn:url:https://example.com/image.png)]"
        "(urn:url:https://example.com/page)"
    )
    adapter = _adapter(client)

    correlation = adapter.prepare(operation, "operation-1")

    assert correlation.provider_rendered_content == (
        "[![diagram](https://example.com/image.png)](https://example.com/page)"
    )
    assert "urn:" not in correlation.provider_rendered_content


def test_canonical_workspace_quote_is_converted_during_message_update():
    client = FakeClient()
    client.messages = [
        {
            "id": 99,
            "sender_id": 2,
            "sender_full_name": "Other User",
            "content": "original text",
        }
    ]
    operation = _operation()
    operation["kind"] = "message.update"
    operation["provider"]["entity_id"] = "99"
    operation["payload"]["payload"]["content"] = (
        f"[Other User](urn:quote:{MESSAGE_UUID})\n\nedited reply"
    )
    adapter = _adapter(client)

    adapter.prepare(operation, "operation-1")
    assert adapter.apply(operation) == ("99", None)

    assert client.updated[0]["content"] == (
        "@_**Other User|2** "
        "[said](https://zulip.example.invalid/#narrow/near/99):\n"
        "```quote\noriginal text\n```\n\nedited reply"
    )
    assert "urn:quote:" not in client.updated[0]["content"]


def test_reconciliation_uses_persisted_exact_provider_rendering_without_reupload():
    class FileClient:
        def export_file(self, *args, **kwargs):
            return "report.pdf", "application/pdf", b"pdf-bytes"

    client = FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(
        client=client,
        routing=FakeRouting(),
        owner_user_uuid=OWNER_UUID,
        account_uuid=OWNER_UUID,
        file_client=FileClient(),
        file_limit=lambda: 1024,
    )
    adapter.restore_queue("queue-1", 7)
    operation = _operation()
    operation["payload"]["payload"]["content"] = (
        f"[Other User](urn:user:{USER_2_UUID}) "
        "[report.pdf](urn:file:10000000-0000-4000-8000-000000000008)"
    )
    correlation = adapter.prepare(operation, MESSAGE_UUID)
    assert len(client.uploads) == 1
    attempted = datetime.datetime.now(datetime.UTC)
    client.messages = [
        {
            "id": 101,
            "content": correlation.provider_rendered_content,
            "sender_id": 1,
            "timestamp": attempted.timestamp(),
        }
    ]

    evidence = adapter.reconcile_message(
        operation, attempted, correlation.provider_rendered_content
    )

    assert evidence.selected_provider_id == "101"
    assert len(client.uploads) == 1


def test_outbound_update_attachment_uses_real_external_chat_uuid():
    class FileClient:
        def __init__(self):
            self.chat_uuid = None

        def export_file(
            self,
            transfer_uuid,
            operation_uuid,
            account_uuid,
            chat_uuid,
            *args,
            **kwargs,
        ):
            self.chat_uuid = chat_uuid
            return "report.pdf", "application/pdf", b"pdf-bytes"

    client = FakeClient()
    file_client = FileClient()
    adapter = zulip_adapter.OfficialZulipAdapter(
        client=client,
        routing=FakeRouting(),
        owner_user_uuid=OWNER_UUID,
        account_uuid=OWNER_UUID,
        file_client=file_client,
        file_limit=lambda: 1024,
    )
    operation = _operation()
    operation["kind"] = "message.update"
    operation["provider"]["entity_id"] = "99"
    operation["payload"]["payload"]["content"] = (
        "[report.pdf](urn:file:10000000-0000-4000-8000-000000000008)"
    )

    adapter.prepare(operation, MESSAGE_UUID)
    assert adapter.apply(operation) == ("99", None)
    assert str(file_client.chat_uuid) == EXTERNAL_CHAT_UUID
    assert client.updated[0]["content"] == "[report.pdf](/user_uploads/file)"


@pytest.mark.parametrize("kind", ["message.update", "message.delete"])
def test_message_mutation_resolves_provider_id_after_create(kind):
    client = FakeClient()
    adapter = _adapter(client)
    operation = _operation()
    operation["kind"] = kind
    operation["entity_uuid"] = MESSAGE_UUID

    assert adapter.apply(operation) == ("99", None)
    if kind == "message.update":
        assert client.updated == [{"message_id": 99, "content": "hello"}]
        assert client.deleted == []
    else:
        assert client.updated == []
        assert client.deleted == [99]


def test_zb_msg_003_lost_send_response_is_ambiguous_not_retryable():
    client = FakeClient()
    client.fail_send = True
    adapter = _adapter(client)
    operation = _operation()
    correlation = adapter.prepare(operation, "operation-1")
    with pytest.raises(zulip_adapter.ZulipAmbiguousOutcome):
        adapter.apply(operation, correlation)


def test_zb_msg_003_reconciliation_selects_closest_then_lowest_id():
    attempted = datetime.datetime.now(datetime.UTC)
    client = FakeClient()
    client.messages = [
        {
            "id": 100,
            "content": "hello",
            "sender_id": 1,
            "timestamp": attempted.timestamp() + 2,
        },
        {
            "id": 99,
            "content": "hello",
            "sender_id": 1,
            "timestamp": attempted.timestamp() + 2,
        },
        {
            "id": 98,
            "content": "different",
            "sender_id": 1,
            "timestamp": attempted.timestamp(),
        },
    ]
    adapter = _adapter(client)
    evidence = adapter.reconcile_message(_operation(), attempted)
    assert evidence.exact_match_count == 2
    assert evidence.candidate_ids == ("99", "100")
    assert evidence.selected_provider_id == "99"
    assert client.last_get_messages["apply_markdown"] is False
    assert client.last_get_messages["anchor"] == "newest"


def test_read_state_resolves_canonical_workspace_message_uuid():
    client = FakeClient()
    adapter = _adapter(client)
    operation = {
        "kind": "read_state.set",
        "provider": {
            "kind": "zulip",
            "chat_id": "channel:42",
            "entity_id": None,
            "revision": None,
        },
        "payload": {
            "stream_uuid": STREAM_UUID,
            "topic_uuid": TOPIC_UUID,
            "reader_uuid": OWNER_UUID,
            "through_message_uuid": MESSAGE_UUID,
            "read": True,
        },
    }

    assert adapter.apply(operation) == ("99", None)
    assert client.flags == [{"messages": [99], "op": "add", "flag": "read"}]


def test_read_state_boundary_expands_all_mapped_messages_in_scope():
    class Routing(FakeRouting):
        def workspace_message_mappings_through(
            self, stream_uuid, topic_uuid, through_workspace_uuid
        ):
            assert (stream_uuid, topic_uuid, through_workspace_uuid) == (
                STREAM_UUID,
                TOPIC_UUID,
                MESSAGE_UUID,
            )
            return [{"provider_id": value} for value in (97, 98, 99)]

    client = FakeClient()
    adapter = _adapter(client, routing=Routing())
    operation = {
        "kind": "read_state.set",
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "stream_uuid": STREAM_UUID,
            "topic_uuid": TOPIC_UUID,
            "reader_uuid": OWNER_UUID,
            "through_message_uuid": MESSAGE_UUID,
            "read": True,
        },
    }
    assert adapter.apply(operation) == ("99", None)
    assert client.flags == [{"messages": [97, 98, 99], "op": "add", "flag": "read"}]


def test_exact_read_state_updates_only_listed_messages():
    client = FakeClient()
    adapter = _adapter(client)
    operation = {
        "kind": "read_state.set",
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "stream_uuid": STREAM_UUID,
            "topic_uuid": TOPIC_UUID,
            "reader_uuid": OWNER_UUID,
            "message_uuids": [MESSAGE_UUID],
            "read": False,
        },
    }
    assert adapter.apply(operation) == ("99", None)
    assert client.flags == [{"messages": [99], "op": "remove", "flag": "read"}]


def test_reaction_create_update_and_delete_use_official_client_semantics():
    client = FakeClient()
    adapter = _adapter(client)
    create = {
        "kind": "reaction.create",
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "message_uuid": MESSAGE_UUID,
            "user_uuid": OWNER_UUID,
            "emoji_name": "thumbs_up",
        },
    }

    assert adapter.apply(create) == ("99:1:thumbs_up", None)
    assert client.added_reactions == [{"message_id": 99, "emoji_name": "thumbs_up"}]

    update = {
        "kind": "reaction.update",
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "message_uuid": MESSAGE_UUID,
            "user_uuid": OWNER_UUID,
            "emoji_name": "heart",
            "previous_message_uuid": MESSAGE_UUID,
            "previous_emoji_name": "thumbs_up",
            "provider": {
                "emoji_code": "1f44d",
                "reaction_type": "unicode_emoji",
            },
        },
    }

    assert adapter.apply(update) == ("99:1:heart", None)
    assert client.removed_reactions == [
        {
            "message_id": 99,
            "emoji_name": "thumbs_up",
            "emoji_code": "1f44d",
            "reaction_type": "unicode_emoji",
        }
    ]
    assert client.added_reactions[-1] == {"message_id": 99, "emoji_name": "heart"}

    delete = {
        "kind": "reaction.delete",
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "message_uuid": MESSAGE_UUID,
            "user_uuid": OWNER_UUID,
            "emoji_name": "heart",
        },
    }

    assert adapter.apply(delete) == ("99:1:heart", None)
    assert client.removed_reactions[-1] == {
        "message_id": 99,
        "emoji_name": "heart",
    }


def test_workspace_unicode_reaction_uses_zulip_server_emoji_identity(monkeypatch):
    class EmojiResponse:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {
                "code_to_names": {
                    "1f44d": ["+1", "thumbs_up", "like"],
                    "2764": ["heart", "love"],
                }
            }

    requests = []

    def get(url, **kwargs):
        requests.append((url, kwargs))
        return EmojiResponse()

    monkeypatch.setattr(zulip_adapter.requests, "get", get)
    zulip_adapter._zulip_unicode_emoji_names.cache_clear()
    client = FakeClient()
    client.base_url = "https://zulip.example.test/api/"
    client.tls_verification = "/tmp/test-ca.pem"
    adapter = _adapter(client)

    create = {
        "kind": "reaction.create",
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "message_uuid": MESSAGE_UUID,
            "user_uuid": OWNER_UUID,
            "emoji_name": "👍",
        },
    }
    delete = {
        "kind": "reaction.delete",
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "message_uuid": MESSAGE_UUID,
            "user_uuid": OWNER_UUID,
            "emoji_name": "👍",
        },
    }

    assert adapter.apply(create) == ("99:1:unicode_emoji:1f44d", None)
    assert adapter.apply(delete) == ("99:1:unicode_emoji:1f44d", None)
    assert client.added_reactions[-1] == {
        "message_id": 99,
        "emoji_name": "+1",
        "emoji_code": "1f44d",
        "reaction_type": "unicode_emoji",
    }
    assert client.removed_reactions[-1] == client.added_reactions[-1]
    assert requests == [
        (
            "https://zulip.example.test/static/generated/emoji/emoji_api.json",
            {"verify": "/tmp/test-ca.pem", "timeout": 60.0},
        )
    ]
    zulip_adapter._zulip_unicode_emoji_names.cache_clear()


def test_reaction_retries_converge_after_ambiguous_provider_commit():
    client = FakeClient()
    client.add_reaction_result = {
        "result": "error",
        "code": "REACTION_ALREADY_EXISTS",
    }
    client.remove_reaction_result = {
        "result": "error",
        "code": "REACTION_DOES_NOT_EXIST",
    }
    adapter = _adapter(client)

    assert adapter.apply(
        {
            "kind": "reaction.create",
            "provider": {"kind": "zulip", "chat_id": "channel:42"},
            "payload": {
                "message_uuid": MESSAGE_UUID,
                "user_uuid": OWNER_UUID,
                "emoji_name": "eyes",
            },
        }
    ) == ("99:1:eyes", None)
    assert adapter.apply(
        {
            "kind": "reaction.delete",
            "provider": {"kind": "zulip", "chat_id": "channel:42"},
            "payload": {
                "message_uuid": MESSAGE_UUID,
                "user_uuid": OWNER_UUID,
                "emoji_name": "eyes",
            },
        }
    ) == ("99:1:eyes", None)


def test_exact_read_state_does_not_reinterpret_workspace_order_as_provider_boundary():
    first_workspace_uuid = "10000000-0000-0000-0000-000000000010"
    boundary_workspace_uuid = "20000000-0000-0000-0000-000000000020"

    class Routing(FakeRouting):
        workspace = {
            **FakeRouting.workspace,
            ("message", first_workspace_uuid): {
                "provider_id": "9002",
                "metadata": {},
            },
            ("message", boundary_workspace_uuid): {
                "provider_id": "1001",
                "metadata": {},
            },
        }

    client = FakeClient()
    adapter = _adapter(client, routing=Routing())
    operation = {
        "kind": "read_state.set",
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "stream_uuid": STREAM_UUID,
            "topic_uuid": TOPIC_UUID,
            "reader_uuid": OWNER_UUID,
            "message_uuids": [first_workspace_uuid, boundary_workspace_uuid],
            "read": True,
        },
    }

    assert adapter.apply(operation) == ("9002", None)
    assert client.flags == [{"messages": [9002, 1001], "op": "add", "flag": "read"}]


def test_exact_read_state_applies_mapped_prefix_when_terminal_message_is_unmapped():
    mapped_workspace_uuid = "10000000-0000-0000-0000-000000000010"
    terminal_unmapped_uuid = "20000000-0000-0000-0000-000000000020"

    class Routing(FakeRouting):
        workspace = {
            **FakeRouting.workspace,
            ("message", mapped_workspace_uuid): {
                "provider_id": "9002",
                "metadata": {},
            },
        }

    client = FakeClient()
    adapter = _adapter(client, routing=Routing())
    operation = {
        "kind": "read_state.set",
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "stream_uuid": STREAM_UUID,
            "topic_uuid": TOPIC_UUID,
            "reader_uuid": OWNER_UUID,
            "message_uuids": [mapped_workspace_uuid, terminal_unmapped_uuid],
            "read": True,
        },
    }

    assert adapter.apply(operation) == ("9002", None)
    assert client.flags == [{"messages": [9002], "op": "add", "flag": "read"}]


def test_official_unrecoverable_network_error_is_retryable():
    class Client(FakeClient):
        def get_events(self, **kwargs):
            raise zulip.UnrecoverableNetworkError("offline")

    adapter = _adapter(Client())
    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        adapter.events("queue", 1)
    assert captured.value.code == "provider_unavailable"
    assert captured.value.retryable is True


def test_topic_read_state_without_boundary_uses_canonical_topic_mapping():
    client = FakeClient()
    adapter = _adapter(client)
    operation = {
        "kind": "read_state.set",
        "provider": {
            "kind": "zulip",
            "chat_id": "channel:42",
            "entity_id": None,
            "revision": None,
        },
        "payload": {
            "stream_uuid": STREAM_UUID,
            "topic_uuid": TOPIC_UUID,
            "reader_uuid": OWNER_UUID,
            "through_message_uuid": None,
            "read": True,
        },
    }

    assert adapter.apply(operation) == ("42", None)
    assert client.read_topics == [(42, "bridge")]


def test_stream_rename_uses_canonical_provider_chat_id():
    client = FakeClient()
    adapter = _adapter(client)
    operation = {
        "kind": "stream.upsert",
        "provider": {
            "kind": "zulip",
            "chat_id": "channel:42",
            "entity_id": "channel:42",
            "revision": None,
        },
        "payload": {
            "name": "renamed engineering",
            "description": "",
            "private": True,
            "chat_kind": "channel",
            "participant_uuids": [],
            "default_topic_uuid": None,
        },
    }

    assert adapter.apply(operation) == ("42", None)
    assert client.stream_updates == [
        {"stream_id": 42, "new_name": "renamed engineering"}
    ]


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (
            "all_messages",
            [
                {
                    "stream_id": 42,
                    "property": "desktop_notifications",
                    "value": True,
                },
                {"stream_id": 42, "property": "is_muted", "value": False},
            ],
        ),
        (
            "mentions_only",
            [
                {
                    "stream_id": 42,
                    "property": "desktop_notifications",
                    "value": False,
                },
                {"stream_id": 42, "property": "is_muted", "value": False},
            ],
        ),
        (
            "muted",
            [{"stream_id": 42, "property": "is_muted", "value": True}],
        ),
    ],
)
def test_stream_notification_modes_use_official_subscription_api(mode, expected):
    client = FakeClient()
    adapter = _adapter(client)
    operation = {
        "kind": "stream.notification.update",
        "entity_uuid": STREAM_UUID,
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "uuid": STREAM_UUID,
            "stream_uuid": STREAM_UUID,
            "user_uuid": OWNER_UUID,
            "notification_mode": mode,
            "notification_updated_at": "2026-08-23T12:30:00Z",
        },
    }

    assert adapter.apply(operation) == ("channel:42", None)
    assert client.subscription_settings == [expected]


def test_topic_notification_mode_uses_user_topic_endpoint():
    client = FakeClient()
    adapter = _adapter(client)
    operation = {
        "kind": "topic.notification.update",
        "entity_uuid": TOPIC_UUID,
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "uuid": TOPIC_UUID,
            "stream_uuid": STREAM_UUID,
            "user_uuid": OWNER_UUID,
            "notification_mode": "follow",
            "notification_updated_at": "2026-08-23T12:31:00Z",
        },
    }

    assert adapter.apply(operation) == ("42:bridge", None)
    assert client.endpoint_requests == [
        {
            "url": "user_topics",
            "method": "POST",
            "request": {
                "stream_id": 42,
                "topic": "bridge",
                "visibility_policy": 3,
            },
            "longpolling": False,
            "timeout": None,
        }
    ]


def test_newer_provider_notification_observation_skips_stale_workspace_write():
    class Routing(FakeRouting):
        workspace = {
            **FakeRouting.workspace,
            ("stream", STREAM_UUID): {
                **FakeRouting.workspace[("stream", STREAM_UUID)],
                "metadata": {
                    **FakeRouting.workspace[("stream", STREAM_UUID)]["metadata"],
                    "notification_updated_at": "2026-08-23T12:32:00Z",
                },
            },
        }

    client = FakeClient()
    adapter = _adapter(client, routing=Routing())
    operation = {
        "kind": "stream.notification.update",
        "entity_uuid": STREAM_UUID,
        "provider": {"kind": "zulip", "chat_id": "channel:42"},
        "payload": {
            "uuid": STREAM_UUID,
            "stream_uuid": STREAM_UUID,
            "user_uuid": OWNER_UUID,
            "notification_mode": "all_messages",
            "notification_updated_at": "2026-08-23T12:31:00Z",
        },
    }

    assert adapter.apply(operation) == ("channel:42", None)
    assert client.subscription_settings == []


@pytest.mark.parametrize("kind", ["membership.add", "membership.remove"])
def test_membership_write_uses_official_subscription_api(kind):
    client = FakeClient()
    adapter = _adapter(client)
    operation = {
        "kind": kind,
        "entity_uuid": "10000000-0000-4000-8000-000000000011",
        "provider": {
            "kind": "zulip",
            "chat_id": "channel:42",
            "entity_id": "2",
            "revision": None,
        },
        "payload": {
            "stream_uuid": STREAM_UUID,
            "user_uuid": USER_2_UUID,
            "role": "member",
        },
    }

    assert adapter.apply(operation) == ("2", None)
    if kind == "membership.add":
        assert client.added_subscriptions == [
            ([{"name": "engineering"}], {"principals": [2]})
        ]
        assert client.removed_subscriptions == []
    else:
        assert client.added_subscriptions == []
        assert client.removed_subscriptions == [(["engineering"], [2])]


def test_membership_write_converges_across_retry_remove_and_readd():
    class MembershipClient(FakeClient):
        def __init__(self):
            super().__init__()
            self.subscribers = set()

        def add_subscriptions(self, streams, **kwargs):
            super().add_subscriptions(streams, **kwargs)
            self.subscribers.update(kwargs["principals"])
            return {"result": "success"}

        def remove_subscriptions(self, streams, principals=None):
            super().remove_subscriptions(streams, principals)
            self.subscribers.difference_update(principals or [])
            return {"result": "success"}

    client = MembershipClient()
    adapter = _adapter(client)
    operation = {
        "kind": "membership.add",
        "entity_uuid": "10000000-0000-4000-8000-000000000011",
        "provider": {
            "kind": "zulip",
            "chat_id": "channel:42",
            "entity_id": "2",
            "revision": None,
        },
        "payload": {
            "stream_uuid": STREAM_UUID,
            "user_uuid": USER_2_UUID,
            "role": "member",
        },
    }

    assert adapter.apply(operation) == ("2", None)
    assert adapter.apply(operation) == ("2", None)
    assert client.subscribers == {2}

    operation["kind"] = "membership.remove"
    assert adapter.apply(operation) == ("2", None)
    assert adapter.apply(operation) == ("2", None)
    assert client.subscribers == set()

    operation["kind"] = "membership.add"
    assert adapter.apply(operation) == ("2", None)
    assert client.subscribers == {2}


def test_topic_rename_uses_canonical_topic_uuid_mapping():
    client = FakeClient()
    adapter = _adapter(client)
    operation = {
        "kind": "topic.upsert",
        "entity_uuid": TOPIC_UUID,
        "provider": {
            "kind": "zulip",
            "chat_id": "channel:42",
            "entity_id": "42:bridge",
            "revision": None,
        },
        "payload": {"stream_uuid": STREAM_UUID, "name": "renamed topic"},
    }

    assert adapter.apply(operation) == ("99", None)
    assert client.updated == [
        {"message_id": 99, "topic": "renamed topic", "propagate_mode": "change_all"}
    ]


def test_backfill_history_is_raw_and_newest_first():
    client = FakeClient()
    client.messages = [
        {"id": 10, "timestamp": 100},
        {"id": 12, "timestamp": 101},
        {"id": 11, "timestamp": 101},
    ]
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)
    messages = adapter.message_history("channel:42")
    assert [message["id"] for message in messages] == [12, 11, 10]
    assert client.last_get_messages["num_before"] == zulip_adapter.HISTORY_PAGE_SIZE
    assert client.last_get_messages["apply_markdown"] is False
    assert client.last_get_messages["narrow"] == [
        {"operator": "channel", "operand": 42}
    ]


def test_message_by_id_returns_exact_raw_provider_message():
    client = FakeClient()
    client.messages = [
        {"id": 600, "type": "stream", "stream_id": 41},
        {"id": 601, "type": "stream", "stream_id": 42},
    ]
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.message_by_id(601) == {
        "id": 601,
        "type": "stream",
        "stream_id": 42,
    }
    assert client.last_get_messages == {
        "anchor": 601,
        "num_before": 0,
        "num_after": 0,
        "apply_markdown": False,
        "narrow": [{"operator": "id", "operand": 601}],
    }


def test_message_by_id_returns_none_when_provider_message_is_absent():
    client = FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.message_by_id(601) is None


def test_message_by_id_preserves_retryable_provider_failures():
    class FailingClient(FakeClient):
        def get_messages(self, request):
            raise requests.Timeout("provider unavailable")

    adapter = zulip_adapter.OfficialZulipAdapter(client=FailingClient())

    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter.message_by_id(601)

    assert error.value.code == "provider_unavailable"
    assert error.value.retryable is True


def test_provider_event_poll_uses_bounded_nonblocking_boundary():
    client = FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.events("queue-1", 7) == []
    assert client.event_requests == [
        {"queue_id": "queue-1", "last_event_id": 7, "dont_block": True}
    ]
    assert client.endpoint_requests == [
        {
            "url": "events",
            "method": "GET",
            "request": {
                "queue_id": "queue-1",
                "last_event_id": 7,
                "dont_block": True,
            },
            "longpolling": False,
            "timeout": None,
        }
    ]


def test_provider_event_poll_can_use_one_blocking_request_per_account():
    client = FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.events("queue-1", 7, long_polling=True) == []
    assert client.event_requests == [
        {"queue_id": "queue-1", "last_event_id": 7, "dont_block": False}
    ]
    assert client.endpoint_requests == [
        {
            "url": "events",
            "method": "GET",
            "request": {
                "queue_id": "queue-1",
                "last_event_id": 7,
                "dont_block": False,
            },
            "longpolling": True,
            "timeout": None,
        }
    ]


def test_official_client_disables_inline_retries(monkeypatch):
    calls = []

    class Client(FakeClient):
        def __init__(self, **kwargs):
            super().__init__()
            calls.append(kwargs)

    monkeypatch.setattr(zulip_adapter.zulip, "Client", Client)
    credentials = zulip_adapter.ZulipCredentials(
        site="https://zulip.example.invalid",
        email="owner@example.invalid",
        api_key="secret",
    )

    zulip_adapter.OfficialZulipAdapter(credentials=credentials)

    assert calls == [
        {
            "email": "owner@example.invalid",
            "api_key": "secret",
            "site": "https://zulip.example.invalid",
            "client": "workspace-zulip-bridge/0.1",
            "cert_bundle": None,
            "retry_on_errors": False,
        }
    ]


def test_invalid_server_settings_are_retryable_and_account_scoped(monkeypatch):
    def invalid_client(**kwargs):
        raise AssertionError("zulip_version is missing")

    monkeypatch.setattr(zulip_adapter.zulip, "Client", invalid_client)
    credentials = zulip_adapter.ZulipCredentials(
        site="https://zulip.example.invalid",
        email="owner@example.invalid",
        api_key="secret",
    )

    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        zulip_adapter.OfficialZulipAdapter(credentials=credentials)

    assert error.value.code == "provider_unavailable"
    assert error.value.retryable is True


def test_registration_requests_and_retains_catalog_snapshot_fields():
    client = FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.ensure_queue() == ("queue-1", 7)
    snapshot = adapter.take_registration_snapshot()
    assert snapshot is not None
    assert snapshot["user_id"] == 1
    assert snapshot["subscriptions"] == [
        {
            "stream_id": 42,
            "name": "Engineering",
            "subscribers": [1, 2],
            "is_muted": False,
            "desktop_notifications": None,
        }
    ]
    assert snapshot["user_settings"] == {"enable_stream_desktop_notifications": True}
    assert client.subscriptions_request == {"include_subscribers": True}
    assert client.users_request == {"include_deactivated": True}
    assert client.registration_request["fetch_event_types"] == [
        "message",
        "subscription",
        "user_topic",
        "user_settings",
        "realm_user",
        "realm",
        "recent_private_conversations",
    ]
    assert "reaction" in client.registration_request["event_types"]
    assert "user_settings" in client.registration_request["event_types"]
    assert client.registration_request["client_capabilities"] == {
        "notification_settings_null": True,
        "bulk_message_deletion": True,
    }
    assert client.registration_request["idle_queue_timeout"] == (
        zulip_adapter.PROVIDER_QUEUE_IDLE_TIMEOUT_SECONDS
    )
    assert adapter.take_registration_snapshot() is None


def test_notification_subscriptions_validate_current_overrides():
    client = FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.notification_subscriptions() == client.subscriptions
    assert client.subscriptions_request == {"include_subscribers": False}

    client.subscriptions[0]["is_muted"] = "false"
    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter.notification_subscriptions()
    assert error.value.code == "invalid_record"
    assert not error.value.retryable


def test_registration_hydrates_referenced_user_missing_from_bulk_directory():
    client = FakeClient()
    client.members.append(
        {
            "user_id": 3,
            "full_name": "Historical User",
            "email": "historical@example.invalid",
        }
    )
    original_get_users = client.get_users

    def get_users(request=None):
        result = original_get_users(request)
        return {**result, "members": result["members"][:2]}

    client.get_users = get_users
    original_get_subscriptions = client.get_subscriptions

    def get_subscriptions(request=None):
        result = original_get_subscriptions(request)
        result["subscriptions"][0]["subscribers"].append(3)
        return result

    client.get_subscriptions = get_subscriptions
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.ensure_queue() == ("queue-1", 7)
    snapshot = adapter.take_registration_snapshot()

    assert snapshot is not None
    assert [user["user_id"] for user in snapshot["realm_users"]] == [1, 2, 3]
    assert client.user_requests == [(3, {})]


def test_registration_skips_referenced_user_absent_from_provider_directory():
    client = FakeClient()
    original_get_subscriptions = client.get_subscriptions

    def get_subscriptions(request=None):
        result = original_get_subscriptions(request)
        result["subscriptions"][0]["subscribers"].append(3)
        return result

    client.get_subscriptions = get_subscriptions
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.ensure_queue() == ("queue-1", 7)
    snapshot = adapter.take_registration_snapshot()

    assert snapshot is not None
    assert [user["user_id"] for user in snapshot["realm_users"]] == [1, 2]
    assert client.user_requests == [(3, {})]


def test_registration_retries_failed_referenced_user_hydration():
    client = FakeClient()
    original_get_subscriptions = client.get_subscriptions

    def get_subscriptions(request=None):
        result = original_get_subscriptions(request)
        result["subscriptions"][0]["subscribers"].append(3)
        return result

    def get_user_by_id(user_id, **request):
        raise requests.Timeout("provider unavailable")

    client.get_subscriptions = get_subscriptions
    client.get_user_by_id = get_user_by_id
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter.ensure_queue()

    assert error.value.code == "provider_unavailable"
    assert error.value.retryable


def test_legacy_registration_omits_unsupported_idle_queue_timeout():
    client = FakeClient()
    client.feature_level = 480
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.ensure_queue() == ("queue-1", 7)
    assert "idle_queue_timeout" not in client.registration_request


def test_provider_file_download_streams_with_a_strict_effective_limit(monkeypatch):
    class Response:
        headers = {"Content-Length": "9", "Content-Type": "application/octet-stream"}
        closed = False

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            yield b"12345"
            yield b"6789"

        def close(self):
            self.closed = True

    response = Response()
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)
    client = FakeClient()
    client.email = "owner@example.test"
    client.api_key = "secret"
    client.base_url = "https://zulip.example.test/api/"
    client.tls_verification = True
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter.download_file("/user_uploads/file.bin", max_bytes=8)
    assert error.value.code == "provider_file_too_large"
    assert response.closed


def test_provider_file_http_error_always_closes_streamed_response(monkeypatch):
    class Response:
        headers = {}
        closed = False

        def raise_for_status(self):
            raise requests.HTTPError("not found")

        def close(self):
            self.closed = True

    response = Response()
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)
    client = FakeClient()
    client.email = "owner@example.test"
    client.api_key = "secret"
    client.base_url = "https://zulip.example.test/api/"
    client.tls_verification = True
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter.download_file("/user_uploads/missing.bin")
    assert error.value.code == "provider_file_unavailable"
    assert response.closed


@pytest.mark.parametrize("body_error", [False, True])
def test_provider_file_success_and_body_error_close_response(monkeypatch, body_error):
    class Response:
        headers = {"Content-Length": "4", "Content-Type": "text/plain"}
        closed = False

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            if body_error:
                raise requests.ConnectionError("connection reset")
            yield b"data"

        def close(self):
            self.closed = True

    response = Response()
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)
    client = FakeClient()
    client.email = "owner@example.test"
    client.api_key = "secret"
    client.base_url = "https://zulip.example.test/api/"
    client.tls_verification = True
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    if body_error:
        with pytest.raises(zulip_adapter.ZulipOperationError) as error:
            adapter.download_file("/user_uploads/file.txt")
        assert error.value.code == "provider_file_unavailable"
    else:
        assert adapter.download_file("/user_uploads/file.txt").content == b"data"
    assert response.closed
