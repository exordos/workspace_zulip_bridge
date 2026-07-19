import hashlib
import pathlib
import unittest.mock
import uuid

import httpx
import pytest

from workspace_zulip_bridge import config, file_api


def _settings():
    path = pathlib.Path("/nonexistent")
    return config.FileApiConfig("https://control.invalid", path, path, path)


def test_zb_file_001_import_uses_allocation_put_and_finalize():
    content = b"image bytes"
    file_uuid = uuid.uuid4()
    requests = []

    def control_handler(request):
        requests.append(request.url.path)
        if request.method == "PUT":
            return httpx.Response(
                200,
                json={
                    "status": "allocated",
                    "allocation_generation": 3,
                    "upload": {"method": "PUT", "url": "https://object.invalid/o"},
                },
            )
        return httpx.Response(200, json={"file_urn": f"urn:image:{file_uuid}"})

    objects = httpx.MockTransport(lambda request: httpx.Response(200))
    client = file_api.FileApiClient(
        _settings(),
        httpx.Client(
            base_url="https://control.invalid",
            transport=httpx.MockTransport(control_handler),
        ),
        httpx.Client(transport=objects),
    )
    urn = client.import_file(
        uuid.uuid4(),
        uuid.uuid4(),
        uuid.uuid4(),
        file_api.IncomingFile(file_uuid, "a.png", "image/png", content),
    )
    assert urn == f"urn:image:{file_uuid}"
    assert requests[-1].endswith("/actions/finalize")


def test_zb_file_002_export_digest_mismatch_fails_closed():
    def control_handler(request):
        return httpx.Response(
            200,
            json={
                "name": "a.bin",
                "content_type": "application/octet-stream",
                "size_bytes": len(b"tampered"),
                "sha256": hashlib.sha256(b"expected").hexdigest(),
                "download": {"method": "GET", "url": "https://object.invalid/o"},
            },
        )

    client = file_api.FileApiClient(
        _settings(),
        httpx.Client(
            base_url="https://control.invalid",
            transport=httpx.MockTransport(control_handler),
        ),
        httpx.Client(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=b"tampered")
            )
        ),
    )
    with pytest.raises(ValueError, match="digest"):
        client.export_file(
            uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), "urn:file:x"
        )


def test_zb_file_003_rejects_declared_outgoing_size_before_object_download():
    object_requests = []
    client = file_api.FileApiClient(
        _settings(),
        httpx.Client(
            base_url="https://control.invalid",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    json={
                        "name": "large.bin",
                        "content_type": "application/octet-stream",
                        "size_bytes": 9,
                        "sha256": hashlib.sha256(b"123456789").hexdigest(),
                        "download": {
                            "method": "GET",
                            "url": "https://object.invalid/o",
                        },
                    },
                )
            ),
        ),
        httpx.Client(
            transport=httpx.MockTransport(
                lambda request: object_requests.append(request) or httpx.Response(200)
            )
        ),
    )

    with pytest.raises(ValueError, match="effective file limit"):
        client.export_file(
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            "urn:file:x",
            max_bytes=8,
        )
    assert object_requests == []


def test_production_private_client_uses_bridge_mtls_material():
    object_client = unittest.mock.Mock()
    context = unittest.mock.Mock()
    with (
        unittest.mock.patch.object(
            file_api.mtls, "client_context", return_value=context
        ) as client_context,
        unittest.mock.patch.object(file_api.httpx, "Client") as client_class,
    ):
        file_api.FileApiClient(_settings(), object_client=object_client)

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


def test_private_object_put_and_get_reuse_mtls_client_and_frame_get():
    content = b"private bytes"
    file_uuid = uuid.uuid4()
    private_requests = []
    external_requests = []

    def private_handler(request):
        private_requests.append(request)
        if request.url.path == f"/v1/file-transfers/incoming/{file_uuid}":
            return httpx.Response(
                200,
                json={
                    "status": "allocated",
                    "allocation_generation": 1,
                    "upload": {
                        "method": "PUT",
                        "url": f"https://control.invalid/v1/file-objects/{file_uuid}",
                    },
                },
            )
        if request.url.path.endswith("/actions/finalize"):
            return httpx.Response(200, json={"file_urn": f"urn:file:{file_uuid}"})
        if request.url.path == f"/v1/file-objects/{file_uuid}":
            if request.method == "GET":
                assert "Content-Length" not in request.headers
                return httpx.Response(200, content=content)
            return httpx.Response(200)
        return httpx.Response(
            200,
            json={
                "name": "private.bin",
                "content_type": "application/octet-stream",
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "download": {
                    "method": "GET",
                    "url": f"https://control.invalid/v1/file-objects/{file_uuid}",
                },
            },
        )

    client = file_api.FileApiClient(
        _settings(),
        httpx.Client(
            base_url="https://control.invalid",
            transport=httpx.MockTransport(private_handler),
        ),
        httpx.Client(
            transport=httpx.MockTransport(
                lambda request: external_requests.append(request) or httpx.Response(500)
            )
        ),
    )

    assert (
        client.import_file(
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            file_api.IncomingFile(
                file_uuid, "private.bin", "application/octet-stream", content
            ),
        )
        == f"urn:file:{file_uuid}"
    )
    assert client.export_file(
        uuid.uuid4(),
        uuid.uuid4(),
        uuid.uuid4(),
        uuid.uuid4(),
        f"urn:file:{file_uuid}",
    ) == ("private.bin", "application/octet-stream", content)
    assert external_requests == []
    assert [request.method for request in private_requests].count("GET") == 1
