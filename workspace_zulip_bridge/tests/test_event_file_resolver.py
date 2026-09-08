"""Shared upload validation preserves live and queue-catchup message text."""

import logging
import types
import uuid

import pytest
import requests

from workspace_zulip_bridge import converter, service, zulip_adapter
from workspace_zulip_bridge.tests import test_converter as conversion
from workspace_zulip_bridge.tests import test_upload_download_paths as uploads

upload_adapter = uploads.upload_adapter


def convert(adapter, delivery_class, path, imported):
    instance = object.__new__(service.BridgeService)
    instance.store = conversion.FakeStore()
    instance.store.effective_file_limit = lambda _: 1024

    def import_file(*args, **kwargs):
        imported.append(args)
        return "urn:file:00000000-0000-4000-8000-000000000001"

    instance.file_client = types.SimpleNamespace(import_file=import_file)
    message = {
        **conversion._dm_message(),
        "content": f"Keep this text [archive.txt]({path}) and this text.",
    }
    records = instance._event_records_with_file_fallback(
        adapter,
        conversion.ACCOUNT_UUID,
        str(uuid.uuid4()),
        "synthetic-queue",
        {"id": 1, "type": "message", "message": message},
        delivery_class,
    )
    operations = conversion._operations(records)
    created = next(op for op in operations if op["kind"] == "message.create")
    return created["payload"]["payload"]["content"]


@pytest.mark.parametrize("delivery_class", ["live", "backfill"])
@pytest.mark.parametrize(
    "path",
    [
        "/user_uploads/../../api/v1/users",
        "/user_uploads/%2e%2e/api/v1/users",
        "/user_uploads/1/a%2Fb",
        "/user_uploads/1/a%5Cb",
        "/user_uploads/1/%00file",
    ],
)
def test_invalid_attachment_retains_message_through_actual_converter_and_adapter(
    upload_adapter, delivery_class, path
):
    adapter, transport = upload_adapter
    imported = []
    content = convert(adapter, delivery_class, path, imported)
    assert "Keep this text" in content and "and this text." in content
    assert "**File unavailable:** archive.txt" in content
    assert "/user_uploads/" not in content
    assert not transport.sent and not imported


@pytest.mark.parametrize("delivery_class", ["live", "backfill"])
def test_valid_attachment_still_downloads_and_imports(upload_adapter, delivery_class):
    adapter, transport = upload_adapter
    imported = []
    content = convert(adapter, delivery_class, "/user_uploads/1/report..txt", imported)
    assert "urn:file:" in content and converter.UNAVAILABLE_FILE_MARKER not in content
    assert len(transport.sent) == len(imported) == 1


@pytest.mark.parametrize("delivery_class", ["live", "backfill"])
@pytest.mark.parametrize("failure", ["auth", "network", "credentials"])
def test_provider_failures_keep_retry_semantics_through_event_converter(
    upload_adapter, monkeypatch, delivery_class, failure
):
    adapter, transport = upload_adapter
    if failure == "credentials":
        adapter.client.api_key = None
    else:

        def get(*args, **kwargs):
            if failure == "network":
                raise requests.ConnectionError("Synthetic provider failure")
            response = requests.Response()
            response.status_code = 401
            response.url = "https://zulip.example.test/user_uploads/1/file"
            response._content = b""
            response._content_consumed = True
            return response

        monkeypatch.setattr(zulip_adapter.requests, "get", get)
    imported = []
    with pytest.raises(zulip_adapter.ZulipOperationError) as failed:
        convert(adapter, delivery_class, "/user_uploads/1/file", imported)
    assert failed.value.code == (
        "provider_file_credentials_unavailable"
        if failure == "credentials"
        else "provider_file_unavailable"
    )
    assert failed.value.retryable is (failure != "credentials")
    assert not transport.sent and not imported


@pytest.mark.parametrize("delivery_class", ["live", "backfill"])
def test_forbidden_file_falls_back_only_for_live_message(
    upload_adapter, monkeypatch, caplog, delivery_class
):
    adapter, _ = upload_adapter

    def forbidden(*args, **kwargs):
        response = requests.Response()
        response.status_code = 403
        response.url = "https://zulip.example.test/user_uploads/1/file"
        response._content = b""
        response._content_consumed = True
        return response

    monkeypatch.setattr(zulip_adapter.requests, "get", forbidden)
    imported = []
    with caplog.at_level(logging.WARNING):
        if delivery_class == "live":
            assert converter.UNAVAILABLE_FILE_MARKER in convert(
                adapter, delivery_class, "/user_uploads/1/file", imported
            )
        else:
            with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
                convert(adapter, delivery_class, "/user_uploads/1/file", imported)
            assert captured.value.retryable
            assert captured.value.http_status == 403
    assert not imported
    assert any(
        "provider_file_fallback" in message
        and conversion.ACCOUNT_UUID in message
        and "http_status=403" in message
        for message in caplog.messages
    ) is (delivery_class == "live")


def test_redirected_storage_failure_remains_retryable_for_live_message(
    upload_adapter, monkeypatch
):
    adapter, transport = upload_adapter

    def redirected_failure(*args, **kwargs):
        raise zulip_adapter.ZulipOperationError(
            "provider_file_unavailable",
            True,
            http_status=403,
            provider_response=False,
        )

    monkeypatch.setattr(adapter, "download_file", redirected_failure)
    imported = []
    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        convert(adapter, "live", "/user_uploads/1/file", imported)

    assert captured.value.retryable
    assert not transport.sent and not imported


@pytest.mark.parametrize("delivery_class", ["live", "backfill"])
def test_nonretryable_missing_file_falls_back_for_every_delivery_class(
    upload_adapter, monkeypatch, delivery_class
):
    adapter, transport = upload_adapter

    def missing(*args, **kwargs):
        raise zulip_adapter.ZulipOperationError(
            "provider_file_unavailable",
            False,
            http_status=404,
        )

    monkeypatch.setattr(adapter, "download_file", missing)
    imported = []
    assert converter.UNAVAILABLE_FILE_MARKER in convert(
        adapter,
        delivery_class,
        "/user_uploads/1/file",
        imported,
    )
    assert not transport.sent and not imported
