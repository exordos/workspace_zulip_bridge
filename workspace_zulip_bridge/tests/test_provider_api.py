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


def test_provider_api_uses_exact_private_v2_routes_and_envelopes():
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
                        "provider_event_key": "event",
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
    assert client.apply_commands([{"provider_event_key": "event"}])["results"]
    assert [path for _method, path, _body in seen] == [
        "/api/workspace-provider/v2/operations/actions/lease",
        "/api/workspace-provider/v2/operation-results",
        "/api/workspace-provider/v2/commands",
    ]


def test_provider_http_runtime_documents_the_v2_contract_and_routes():
    repository_root = pathlib.Path(__file__).parents[2]
    readme = (repository_root / "README.md").read_text()
    runtime = (repository_root / "docs/provider_http_runtime.md").read_text()

    assert "docs/workspace_provider_api_v2.yaml" in readme
    assert "workspace_backend/docs/workspace_provider_api_v2.yaml" in runtime
    assert "/api/workspace-provider/v2/operations/actions/lease" in runtime
    assert "/api/workspace-provider/v2/operation-results" in runtime
    assert "/api/workspace-provider/v2/commands" in runtime
    assert "/api/workspace-provider/v1" not in runtime


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


def test_provider_event_validation_rejection_has_a_terminal_error_type():
    client = provider_api.ProviderApiClient(
        _settings(),
        httpx.Client(
            base_url="https://provider.invalid",
            transport=httpx.MockTransport(lambda _request: httpx.Response(422)),
        ),
    )

    with pytest.raises(provider_api.ProviderEventRejectedError) as exc_info:
        client.apply_commands([{"provider_event_key": str(uuid.uuid4())}])

    assert exc_info.value.status_code == 422
