import json
import pathlib
import uuid

import httpx
import pytest

from workspace_zulip_bridge import config, provider_api


def _settings():
    return config.ProviderApiConfig(
        "https://provider.invalid",
        pathlib.Path("ca.crt"),
        pathlib.Path("bridge.crt"),
        pathlib.Path("bridge.key"),
    )


def test_provider_api_uses_exact_private_v1_routes_and_envelopes():
    seen = []

    def handle(request):
        seen.append((request.method, request.url.path, request.read()))
        if request.url.path.endswith("/lease"):
            request_uuid = json.loads(request.read())["request_uuid"]
            return httpx.Response(
                200,
                json={"request_uuid": request_uuid, "operations": []},
            )
        if request.url.path.endswith("/operation-results"):
            return httpx.Response(
                200,
                json={"results": [{"result_uuid": "result", "status": "applied"}]},
            )
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "provider_event_uuid": "event",
                        "status": "applied",
                        "target_uuid": None,
                        "safe_error": None,
                        "duplicate": False,
                    }
                ]
            },
        )

    client = provider_api.ProviderApiClient(
        _settings(),
        httpx.Client(
            base_url="https://provider.invalid",
            transport=httpx.MockTransport(handle),
        ),
    )
    request_uuid = uuid.uuid4()

    assert client.lease_operations(request_uuid)["request_uuid"] == str(request_uuid)
    assert client.report_results([{"result_uuid": "result"}])["results"]
    assert client.apply_events([{"provider_event_uuid": "event"}])["results"]
    assert [path for _method, path, _body in seen] == [
        "/api/workspace-provider/v1/operations/actions/lease",
        "/api/workspace-provider/v1/operation-results",
        "/api/workspace-provider/v1/events",
    ]


def test_provider_api_keeps_retryable_conflict_separate_from_bad_request():
    responses = iter((httpx.Response(409), httpx.Response(400)))
    client = provider_api.ProviderApiClient(
        _settings(),
        httpx.Client(
            base_url="https://provider.invalid",
            transport=httpx.MockTransport(lambda _request: next(responses)),
        ),
    )

    with pytest.raises(provider_api.ProviderApiRetryableError):
        client.lease_operations(uuid.uuid4())
    with pytest.raises(httpx.HTTPStatusError):
        client.lease_operations(uuid.uuid4())
