import datetime

import pytest
import requests
import zulip

from workspace_zulip_bridge import zulip_adapter

OWNER_UUID = "10000000-0000-4000-8000-000000000001"
AUTHOR_UUID = OWNER_UUID
STREAM_UUID = "10000000-0000-4000-8000-000000000002"
TOPIC_UUID = "10000000-0000-4000-8000-000000000003"
MESSAGE_UUID = "10000000-0000-4000-8000-000000000004"
USER_2_UUID = "10000000-0000-4000-8000-000000000005"
USER_3_UUID = "10000000-0000-4000-8000-000000000006"
EXTERNAL_CHAT_UUID = "10000000-0000-4000-8000-000000000007"


class FakeClient:
    def __init__(self):
        self.base_url = "https://zulip.example.invalid/api/"
        self.sent = []
        self.updated = []
        self.flags = []
        self.deleted = []
        self.fail_send = False
        self.messages = []
        self.event_requests = []
        self.stream_updates = []
        self.read_streams = []
        self.read_topics = []
        self.uploads = []
        self.registration_request = None

    def register(self, **kwargs):
        self.registration_request = kwargs
        return {
            "result": "success",
            "queue_id": "queue-1",
            "last_event_id": 7,
            "user_id": 1,
        }

    def get_events(self, **kwargs):
        self.event_requests.append(kwargs)
        return {"result": "success", "events": []}

    def get_profile(self):
        return {"result": "success", "user_id": 1}

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

    def mark_stream_as_read(self, stream_id):
        self.read_streams.append(stream_id)
        return {"result": "success"}

    def mark_topic_as_read(self, stream_id, topic_name):
        self.read_topics.append((stream_id, topic_name))
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
        ("topic", TOPIC_UUID): {"provider_id": "42:bridge", "metadata": {}},
        ("message", MESSAGE_UUID): {"provider_id": "99", "metadata": {}},
        ("identity", USER_2_UUID): {"provider_id": "2", "metadata": {}},
        ("identity", USER_3_UUID): {"provider_id": "3", "metadata": {}},
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
    assert client.last_get_messages["apply_markdown"] is False
    assert client.last_get_messages["narrow"] == [
        {"operator": "channel", "operand": 42}
    ]


def test_provider_event_poll_is_nonblocking():
    client = FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.events("queue-1", 7) == []
    assert client.event_requests == [
        {"queue_id": "queue-1", "last_event_id": 7, "dont_block": True}
    ]


def test_registration_requests_and_retains_catalog_snapshot_fields():
    client = FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)

    assert adapter.ensure_queue() == ("queue-1", 7)
    snapshot = adapter.take_registration_snapshot()
    assert snapshot is not None
    assert snapshot["user_id"] == 1
    assert client.registration_request["fetch_event_types"] == [
        "message",
        "subscription",
        "realm_user",
        "recent_private_conversations",
    ]
    assert client.registration_request["client_capabilities"] == {
        "notification_settings_null": True,
        "bulk_message_deletion": True,
        "empty_topic_name": True,
    }
    assert adapter.take_registration_snapshot() is None


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
