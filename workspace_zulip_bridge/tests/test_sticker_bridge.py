"""Sticker media reuses file transfer and restores only verified markers."""

import datetime
import hashlib
import types
import uuid

import httpx
import pytest
import requests

from workspace_zulip_bridge import (
    converter,
    file_api,
    scheduler,
    service,
    zulip_adapter,
)
from workspace_zulip_bridge.tests import test_converter as conversion
from workspace_zulip_bridge.tests import test_file_api as files
from workspace_zulip_bridge.tests import test_scheduler as scheduling
from workspace_zulip_bridge.tests import test_zulip_adapter as adapters

STICKER_UUID = "550e8400-e29b-41d4-a716-446655440000"
LABEL = f"workspace-sticker:v1:{STICKER_UUID}"
CONTENT = b"synthetic sticker bytes"


@pytest.mark.parametrize("image_marker", ["", "!"])
def test_outbound_sticker_reuses_upload_and_preserves_image_syntax(image_marker):
    client = adapters.FakeClient()
    exports = []

    def export_file(*args, **kwargs):
        exports.append(args)
        return "sticker.webp", "image/webp", CONTENT

    adapter = zulip_adapter.OfficialZulipAdapter(
        client=client,
        routing=adapters.FakeRouting(),
        account_uuid=adapters.OWNER_UUID,
        file_client=types.SimpleNamespace(export_file=export_file),
        file_limit=lambda: 1024,
    )
    original = f"{image_marker}[sticker](urn:sticker:{STICKER_UUID})"
    content = f"before {original} after ` {original} `"
    converted = adapter._convert_workspace_markdown(
        content, str(uuid.uuid4()), "channel:42"
    )
    assert converted == (
        f"before {image_marker}[{LABEL}](/user_uploads/file) after ` {original} `"
    )
    assert client.uploads == [("sticker.webp", CONTENT)]
    assert len(exports) == 1 and exports[0][4] == f"urn:sticker:{STICKER_UUID}"


def _resolve(metadata, failure=None):
    instance = object.__new__(service.BridgeService)
    instance.store = conversion.FakeStore()
    instance.store.effective_file_limit = lambda _: 1024
    calls = []

    def resolve_sticker(*args):
        calls.append(("resolve", args))
        if failure is not None:
            raise failure
        return metadata

    def import_file(*args, **kwargs):
        calls.append(("import", args))
        return "urn:image:00000000-0000-4000-8000-000000000001"

    instance.file_client = types.SimpleNamespace(
        resolve_sticker=resolve_sticker, import_file=import_file
    )
    adapter = types.SimpleNamespace(
        download_file=lambda *args, **kwargs: types.SimpleNamespace(
            name="sticker.webp",
            content_type="application/octet-stream",
            content=CONTENT,
        )
    )
    resolver = instance._file_resolver(
        adapter, conversion.ACCOUNT_UUID, str(uuid.uuid4())
    )
    return resolver, calls


def _metadata():
    return {
        "uuid": STICKER_UUID,
        "sha256": hashlib.sha256(CONTENT).hexdigest(),
        "size_bytes": len(CONTENT),
        "content_type": "image/webp",
    }


@pytest.mark.parametrize("delivery_class", ["live", "backfill"])
def test_verified_marker_restores_sticker_through_event_conversion(delivery_class):
    resolver, calls = _resolve(_metadata())
    original = f"[{LABEL}](/user_uploads/file)"
    records = converter.event_records(
        conversion.FakeStore(),
        conversion.ACCOUNT_UUID,
        "queue",
        {
            "id": 10,
            "type": "message",
            "message": {
                **conversion._dm_message(),
                "content": f"before {original} after `{original}`",
            },
        },
        delivery_class,
        original_url="https://chat.example.invalid",
        file_resolver=resolver,
    )
    created = next(
        op for op in conversion._operations(records) if op["kind"] == "message.create"
    )
    assert created["payload"]["payload"]["content"] == (
        f"before ![sticker](urn:sticker:{STICKER_UUID}) after `{original}`"
    )
    assert [call[0] for call in calls] == ["resolve"]


@pytest.mark.parametrize("case", ["plain", "version", "invalid", "hash", "size"])
def test_unverified_marker_uses_ordinary_import(case):
    metadata = _metadata()
    label = LABEL
    if case == "plain":
        label = "sticker.webp"
    elif case == "version":
        label = LABEL.replace(":v1:", ":v2:")
    elif case == "invalid":
        label = "workspace-sticker:v1:invalid"
    elif case == "hash":
        metadata["sha256"] = "0" * 64
    elif case == "size":
        metadata["size_bytes"] += 1
    resolver, calls = _resolve(metadata)
    assert resolver("/user_uploads/file", label).startswith("urn:image:")
    assert calls[-1][0] == "import"
    assert len(calls) == (1 if case in {"plain", "version", "invalid"} else 2)


def test_deleted_sticker_marker_uses_placeholder_without_import():
    resolver, calls = _resolve(None)
    content, lossy = converter.convert_markdown(
        f"before [{LABEL}](/user_uploads/file) after",
        {},
        "https://chat.example.invalid",
        file_resolver=resolver,
    )
    assert lossy
    assert content == (
        f"before **{converter.UNAVAILABLE_STICKER_MARKER}** after\n\n"
        "[Open original](urn:url:https://chat.example.invalid)"
    )
    assert "urn:sticker:" not in content
    assert [call[0] for call in calls] == ["resolve"]


@pytest.mark.parametrize(
    "status,retryable,code",
    [
        (403, False, "workspace_file_import_unavailable"),
        (422, False, "invalid_record"),
        (429, True, "workspace_file_import_unavailable"),
        (503, True, "workspace_file_import_unavailable"),
    ],
)
def test_lookup_failure_does_not_silently_import(status, retryable, code):
    response = httpx.Response(
        status, request=httpx.Request("GET", "https://test.invalid")
    )
    failure = httpx.HTTPStatusError(
        "synthetic", request=response.request, response=response
    )
    resolver, calls = _resolve(None, failure)
    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        resolver("/user_uploads/file", LABEL)
    assert error.value.code == code
    assert error.value.retryable is retryable
    assert [call[0] for call in calls] == ["resolve"]


@pytest.mark.parametrize("status", [200, 404, 403, 503])
def test_metadata_lookup_is_scoped_read_only_request(status):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, json=_metadata())

    client = file_api.FileApiClient(
        files._settings(),
        httpx.Client(
            base_url="https://control.invalid", transport=httpx.MockTransport(handler)
        ),
    )
    account, chat = uuid.uuid4(), uuid.uuid4()
    if status in {403, 503}:
        with pytest.raises(httpx.HTTPStatusError):
            client.resolve_sticker(uuid.UUID(STICKER_UUID), account, chat)
    else:
        assert client.resolve_sticker(uuid.UUID(STICKER_UUID), account, chat) == (
            _metadata() if status == 200 else None
        )
    request = requests[0]
    assert request.method == "GET"
    assert request.headers["Content-Length"] == "0"
    assert request.url.path == f"/v1/stickers/{STICKER_UUID}"
    assert dict(request.url.params) == {
        "external_account_uuid": str(account),
        "external_chat_uuid": str(chat),
    }
    client.close()


def test_lookup_transport_failure_retries_without_import():
    resolver, calls = _resolve(None, httpx.ConnectError("synthetic"))
    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        resolver("/user_uploads/file", LABEL)
    assert error.value.retryable
    assert [call[0] for call in calls] == ["resolve"]


def test_same_upload_can_be_sticker_and_plain_image_in_one_message():
    resolver, calls = _resolve(_metadata())
    assert resolver("/user_uploads/file", LABEL) == f"urn:sticker:{STICKER_UUID}"
    assert resolver("/user_uploads/file", "ordinary image").startswith("urn:image:")
    assert [call[0] for call in calls] == ["resolve", "import"]


def test_reconcile_sticker_send_uses_persisted_rendering_without_reupload():
    client = adapters.FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(
        client=client,
        routing=adapters.FakeRouting(),
        owner_user_uuid=adapters.OWNER_UUID,
        account_uuid=adapters.OWNER_UUID,
        file_client=types.SimpleNamespace(
            export_file=lambda *args, **kwargs: ("sticker.webp", "image/webp", CONTENT)
        ),
        file_limit=lambda: 1024,
    )
    adapter.restore_queue("queue-1", 7)
    operation = adapters._operation()
    operation["payload"]["payload"]["content"] = (
        f"![sticker](urn:sticker:{STICKER_UUID})"
    )
    correlation = adapter.prepare(operation, adapters.MESSAGE_UUID)
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
    assert correlation.provider_rendered_content == f"![{LABEL}](/user_uploads/file)"


@pytest.mark.parametrize("change", ["text", "remove", "unmark", "replace"])
def test_incoming_edit_keeps_sticker_only_while_matching_attachment_remains(change):
    store = conversion.FakeStore()
    resolver, calls = _resolve(_metadata())
    original = f"[{LABEL}](/user_uploads/file)"
    converter.event_records(
        store,
        conversion.ACCOUNT_UUID,
        "queue",
        {
            "id": 1,
            "type": "message",
            "message": {**conversion._dm_message(), "content": original},
        },
        file_resolver=resolver,
    )
    if change == "text":
        content = f"edited {original}"
        expected = f"edited ![sticker](urn:sticker:{STICKER_UUID})"
    elif change == "remove":
        content = expected = "removed image"
    elif change == "unmark":
        content = "[ordinary](/user_uploads/file)"
        expected = "[ordinary](urn:image:00000000-0000-4000-8000-000000000001)"
    else:
        resolver, calls = _resolve({**_metadata(), "sha256": "0" * 64})
        content = f"[{LABEL}](/user_uploads/replacement)"
        expected = f"[{LABEL}](urn:image:00000000-0000-4000-8000-000000000001)"
    records = converter.event_records(
        store,
        conversion.ACCOUNT_UUID,
        "queue",
        {
            "id": 2,
            "type": "update_message",
            "message_id": 501,
            "message_ids": [501],
            "content": content,
            "edit_timestamp": 1_700_000_010,
        },
        file_resolver=resolver,
    )
    updated = next(
        op for op in conversion._operations(records) if op["kind"] == "message.update"
    )
    assert updated["payload"]["payload"]["content"] == expected


@pytest.mark.parametrize(
    "label, expected", [(LABEL, "sticker"), ("photo.png", "photo.png")]
)
def test_unavailable_attachment_has_readable_label(label, expected):
    content, lossy = converter.convert_markdown(
        f"[{label}](/user_uploads/missing)",
        {},
        "https://chat.example.invalid",
        file_resolver=lambda *_: None,
    )
    assert lossy
    marker = (
        f"**{converter.UNAVAILABLE_STICKER_MARKER}**"
        if label == LABEL
        else f"**File unavailable:** {expected}"
    )
    assert content == (
        f"{marker}\n\n[Open original](urn:url:https://chat.example.invalid)"
    )


@pytest.mark.parametrize(
    "sticker_urn",
    ["urn:sticker:invalid", f"urn:sticker:{STICKER_UUID}?v=1"],
)
def test_invalid_outbound_sticker_urn_uses_placeholder_without_export(sticker_urn):
    client = adapters.FakeClient()
    adapter = zulip_adapter.OfficialZulipAdapter(
        client=client,
        routing=adapters.FakeRouting(),
    )
    content = adapter._convert_workspace_markdown(
        f"before ![sticker]({sticker_urn}) after",
        str(uuid.uuid4()),
        "channel:42",
    )
    assert content == (f"before **{converter.UNAVAILABLE_STICKER_MARKER}** after")
    assert "urn:sticker:" not in content


@pytest.mark.parametrize("stage", ["authorization", "download"])
@pytest.mark.parametrize("kind", ["sticker", "image"])
@pytest.mark.parametrize(
    "status, retryable, code",
    [
        (400, False, "invalid_record"),
        (401, False, "permission_denied"),
        (403, False, "permission_denied"),
        (404, False, "not_found"),
        (422, False, "invalid_record"),
        (408, True, "workspace_unavailable"),
        (425, True, "workspace_unavailable"),
        (429, True, "rate_limited"),
        (503, True, "workspace_unavailable"),
        (None, True, "workspace_unavailable"),
        ("oversize", False, "invalid_record"),
        ("length", False, "invalid_record"),
        ("digest", False, "invalid_record"),
    ],
)
def test_export_failure_is_handled_by_scheduler(
    operation_record, kind, stage, status, retryable, code
):
    requests = []

    def respond(request):
        requests.append(request)
        if request.method == "PUT" and (stage == "download" or isinstance(status, str)):
            return httpx.Response(
                200,
                json={
                    "name": "sticker.webp",
                    "content_type": "image/webp",
                    "size_bytes": 2048 if status == "oversize" else len(CONTENT),
                    "sha256": hashlib.sha256(CONTENT).hexdigest(),
                    "download": {
                        "method": "GET",
                        "url": "https://object.invalid/media",
                    },
                },
            )
        if status == "length":
            return httpx.Response(200, content=b"short")
        if status == "digest":
            return httpx.Response(200, content=b"x" * len(CONTENT))
        if status is None:
            raise httpx.ConnectError("Unavailable", request=request)
        return httpx.Response(status)

    client = adapters.FakeClient()
    operation = operation_record["operation"]
    operation["provider"]["chat_id"] = "channel:42"
    operation["payload"]["stream_uuid"] = adapters.STREAM_UUID
    operation["payload"]["topic_uuid"] = adapters.TOPIC_UUID
    operation["payload"]["payload"]["content"] = f"![media](urn:{kind}:{STICKER_UUID})"
    with httpx.Client(
        base_url="https://workspace.example.invalid",
        transport=httpx.MockTransport(respond),
    ) as http_client:
        adapter = zulip_adapter.OfficialZulipAdapter(
            client=client,
            routing=adapters.FakeRouting(),
            account_uuid=operation_record["account_uuid"],
            owner_user_uuid=operation["actor_uuid"],
            file_client=file_api.FileApiClient(
                files._settings(), http_client, http_client
            ),
            file_limit=lambda: 1024,
        )
        store = scheduling.FakeStore(operation_record)
        worker = scheduler.Scheduler(store, lambda _: adapter, "worker")
        assert worker.run_once()
    assert requests[0].method == "PUT"
    if stage == "download" and status != "oversize":
        assert requests[1].method == "GET"
    assert not client.uploads
    assert not store.uncertain
    if kind == "sticker" and status == 404:
        assert not store.retries
        assert store.correlations
        assert store.completed[0][2] == "committed"
        assert client.sent[0]["content"] == (
            f"**{converter.UNAVAILABLE_STICKER_MARKER}**"
        )
        assert "urn:sticker:" not in client.sent[0]["content"]
        return
    assert not store.correlations
    if retryable:
        assert store.retries[0][2] == code
        assert not store.completed
    else:
        assert not store.retries
        assert store.completed[0][2] == "rejected"
        assert store.completed[0][1]["result"]["safe_error"]["code"] == code


@pytest.mark.parametrize("kind", ["sticker", "image"])
def test_provider_upload_transport_failure_is_retried_by_scheduler(
    operation_record, kind
):
    client = adapters.FakeClient()

    def fail_upload(_stream):
        raise requests.ConnectionError("synthetic provider transport failure")

    client.upload_file = fail_upload
    operation = operation_record["operation"]
    operation["provider"]["chat_id"] = "channel:42"
    operation["payload"]["stream_uuid"] = adapters.STREAM_UUID
    operation["payload"]["topic_uuid"] = adapters.TOPIC_UUID
    operation["payload"]["payload"]["content"] = f"![media](urn:{kind}:{STICKER_UUID})"
    adapter = zulip_adapter.OfficialZulipAdapter(
        client=client,
        routing=adapters.FakeRouting(),
        account_uuid=operation_record["account_uuid"],
        owner_user_uuid=operation["actor_uuid"],
        file_client=types.SimpleNamespace(
            export_file=lambda *args, **kwargs: (
                "sticker.webp",
                "image/webp",
                CONTENT,
            )
        ),
        file_limit=lambda: 1024,
    )
    store = scheduling.FakeStore(operation_record)
    worker = scheduler.Scheduler(store, lambda _: adapter, "worker")

    assert worker.run_once()

    assert store.retries[0][2] == "provider_unavailable"
    assert not store.correlations
    assert not store.completed
    assert not store.uncertain
    assert not client.sent
