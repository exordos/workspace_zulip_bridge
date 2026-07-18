import json
import pathlib
import unittest.mock

import httpx
import pytest

from workspace_zulip_bridge import config, control


def _settings() -> config.ControlConfig:
    path = pathlib.Path("/nonexistent")
    return config.ControlConfig(
        base_url="https://control.invalid",
        bootstrap_url="http://control.invalid:21085",
        hostname="control.invalid",
        ca_file=path,
        certificate_file=path,
        private_key_file=path,
        credential_private_key_file=path,
    )


def test_zb_control_001_typed_cursor_expiry():
    client = httpx.Client(
        base_url="https://control.invalid",
        transport=httpx.MockTransport(lambda request: httpx.Response(410)),
    )
    api = control.ControlClient(_settings(), client)
    with pytest.raises(control.ControlCursorExpired):
        api.desired_changes("cursor-1")


def test_zb_control_002_heartbeat_reports_exact_capabilities():
    captured = {}

    def handler(request):
        body = json.loads(request.content)
        captured.update(body)
        return httpx.Response(200, json={"heartbeat_uuid": body["heartbeat_uuid"]})

    client = httpx.Client(
        base_url="https://control.invalid", transport=httpx.MockTransport(handler)
    )
    control.ControlClient(_settings(), client).heartbeat()
    assert captured["provider_kind"] == "zulip"
    assert set(captured["capabilities"]) == set(control.CAPABILITIES)
    assert captured["capabilities"]["messenger.file.transfer"]["limits"] == {
        "max_file_bytes": 52_428_800
    }


def test_production_control_client_uses_loaded_mtls_context():
    context = unittest.mock.Mock()
    with (
        unittest.mock.patch.object(
            control.mtls, "client_context", return_value=context
        ) as client_context,
        unittest.mock.patch.object(control.httpx, "Client") as client_class,
    ):
        control.ControlClient(_settings())

    client_context.assert_called_once_with(
        pathlib.Path("/nonexistent"),
        pathlib.Path("/nonexistent"),
        pathlib.Path("/nonexistent"),
    )
    client_class.assert_called_once_with(
        base_url="https://control.invalid",
        verify=context,
        timeout=10.0,
        follow_redirects=False,
        headers={"Accept": "application/json"},
    )


def test_zb_control_001_cursor_and_scope_are_sent():
    def handler(request):
        assert request.headers.get_list("Content-Length") == ["0"]
        assert request.url.params["cursor"] == "cursor-1"
        assert request.url.params["resource_types"] == ",".join(control.RESOURCE_TYPES)
        return httpx.Response(
            200,
            json={
                "control_schema_version": "v1",
                "current_cursor": "cursor-1",
                "next_cursor": "cursor-2",
                "changes": [],
            },
        )

    client = httpx.Client(
        base_url="https://control.invalid", transport=httpx.MockTransport(handler)
    )
    assert (
        control.ControlClient(_settings(), client).desired_changes("cursor-1")[
            "next_cursor"
        ]
        == "cursor-2"
    )


def test_retryable_status_exposes_retry_after_without_masking_authentication():
    responses = iter(
        (
            httpx.Response(503, headers={"Retry-After": "17"}),
            httpx.Response(401),
        )
    )
    client = httpx.Client(
        base_url="https://control.invalid",
        transport=httpx.MockTransport(lambda request: next(responses)),
    )
    api = control.ControlClient(_settings(), client)

    with pytest.raises(control.ControlRetryableError) as retryable:
        api.heartbeat()
    assert retryable.value.status_code == 503
    assert retryable.value.retry_after_seconds == 17.0

    with pytest.raises(httpx.HTTPStatusError) as authentication:
        api.heartbeat()
    assert authentication.value.response.status_code == 401


def test_non_retry_after_server_error_uses_exponential_backoff_only():
    client = httpx.Client(
        base_url="https://control.invalid",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(500, headers={"Retry-After": "120"})
        ),
    )

    with pytest.raises(control.ControlRetryableError) as retryable:
        control.ControlClient(_settings(), client).heartbeat()

    assert retryable.value.status_code == 500
    assert retryable.value.retry_after_seconds is None
