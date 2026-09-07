"""Exercise authenticated upload downloads through real requests preparation."""

import io
import types
import urllib.parse

import pytest
import requests

from workspace_zulip_bridge import zulip_adapter
from workspace_zulip_bridge.tests import test_history_delivery as delivery

INVALID_UPLOAD_PATHS = [
    "/user_uploads/../../api/v1/users",
    "/user_uploads/./file",
    "/user_uploads/1/..",
    "/user_uploads/%2e%2e/api/v1/users",
    "/user_uploads/.%2E/api/v1/users",
    "/user_uploads/%2e./api/v1/users",
    "/user_uploads/%252e%252e/api/v1/users",
    "/user_uploads/%25%32%65%25%32%65/api/v1/users",
    "/user_uploads/..%2f..%2fapi/v1/users",
    "/user_uploads/1/a%2Fb",
    "/user_uploads/1/a%252fb",
    "/user_uploads/1/a%25252fb",
    "/user_uploads/..\\..\\api\\v1\\users",
    "/user_uploads/1/a%5Cb",
    "/user_uploads/1/a%255cb",
    "/user_uploads/1/%00file",
    "/user_uploads/1/%250afile",
    "/user_uploads/1/\tfile",
    "/user_uploads/1/file\n",
    "/user_uploads/1/%ff",
    "/user_uploads/1/%" + "25" * 10 + "2e",
    "https://zulip.example.test/user_uploads/1/file",
    "//zulip.example.test/user_uploads/1/file",
    "/api/v1/users",
    None,
]


@pytest.fixture
def upload_adapter(monkeypatch):
    class Transport(requests.adapters.BaseAdapter):
        def __init__(self):
            self.sent = []
            self.content = b"fixture"
            self.on_send = lambda: None

        def send(self, request, **kwargs):
            self.sent.append(request)
            self.on_send()
            response = requests.Response()
            response.status_code = 200
            response.headers = {
                "Content-Length": str(len(self.content)),
                "Content-Type": "text/plain",
            }
            response.raw = io.BytesIO(self.content)
            response.request = request
            response.url = request.url
            return response

        def close(self):
            pass

    transport = Transport()
    session = requests.Session()
    session.trust_env = False
    # Every scheme available to requests is intercepted: even a regression must
    # never reach a real provider endpoint during an adversarial-path test.
    session.mount("https://", transport)
    session.mount("http://", transport)
    monkeypatch.setattr(zulip_adapter.requests, "get", session.get)
    adapter = zulip_adapter.OfficialZulipAdapter(
        client=types.SimpleNamespace(
            base_url="https://zulip.example.test/api/",
            email="synthetic@example.test",
            api_key="synthetic-file-key",
            tls_verification=True,
        )
    )
    try:
        yield adapter, transport
    finally:
        session.close()


@pytest.mark.parametrize("path", INVALID_UPLOAD_PATHS)
def test_invalid_upload_path_never_reaches_authenticated_transport(
    upload_adapter, path
):
    adapter, transport = upload_adapter
    with pytest.raises(zulip_adapter.ZulipOperationError) as caught:
        adapter.download_file(path)
    assert (
        caught.value.code == "invalid_provider_file_url" and not caught.value.retryable
    )
    assert transport.sent == []


@pytest.mark.parametrize(
    "path",
    [
        "/user_uploads/1/token/report..txt",
        "/user_uploads/1/token/report with spaces.txt",
        "/user_uploads/1/token/Отчёт été.txt",
        "/user_uploads/1/token/report%20name.txt",
        "/user_uploads/1/token/report%2520name.txt",
        "/user_uploads/1/token/100%25done.txt",
        "/user_uploads/1/token/plot%23one.svg",
        "/user_uploads/1/token/file.txt?download=1",
    ],
)
def test_valid_upload_names_keep_real_adapter_download_behavior(upload_adapter, path):
    adapter, transport = upload_adapter
    assert adapter.download_file(path).content == b"fixture"
    assert len(transport.sent) == 1
    request = transport.sent[0]
    parsed = urllib.parse.urlsplit(request.url)
    assert parsed.scheme == "https" and parsed.netloc == "zulip.example.test"
    assert parsed.path.startswith("/user_uploads/")
    assert urllib.parse.unquote(parsed.path) == urllib.parse.unquote(
        urllib.parse.urlsplit(path).path
    )
    assert "Authorization" in request.headers


@pytest.mark.parametrize(
    "content,final_limit,method",
    [
        (b"", 0, "POST"),
        (b"fixture", 0, "POST"),
        (b"", 1, "PUT"),
        (b"fixture", 7, "PUT"),
        (b"fixture", 6, "POST"),
    ],
)
def test_history_rechecks_positive_limit_after_actual_adapter_download(
    monkeypatch, upload_adapter, content, final_limit, method
):
    adapter, transport = upload_adapter
    transport.content = content
    limit = [1024]
    transport.on_send = lambda: limit.__setitem__(0, final_limit)
    publisher, sent, releases = delivery.waiting_file_publisher(
        monkeypatch, adapter.download_file, limit=lambda _: limit[0]
    )
    assert publisher.run_once()
    assert len(transport.sent) == 1
    assert [request.method for request in sent] == ["GET", method]
    if method == "POST":
        assert sent[-1].url.path.endswith("/unavailable")
    else:
        assert sent[-1].content == content
    assert len(releases) == 1
