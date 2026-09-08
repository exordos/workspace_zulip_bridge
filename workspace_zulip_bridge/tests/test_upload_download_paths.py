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
            self.status_code = 200
            self.redirect_to = None
            self.redirect_responses = 1
            self.redirect_addresses = []
            self.on_send = lambda: None

        def send(self, request, **kwargs):
            self.sent.append(request)
            self.on_send()
            response = requests.Response()
            if (
                self.redirect_to is not None
                and len(self.sent) <= self.redirect_responses
            ):
                response.status_code = 302
                response.headers = {"Location": self.redirect_to}
                response.raw = io.BytesIO()
                response.request = request
                response.url = request.url
                return response
            response.status_code = self.status_code
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

    def request_file_redirect(redirect, address, *, auth, verify):
        transport.redirect_addresses.append(str(address))
        response = session.get(
            redirect.url,
            auth=auth,
            verify=verify,
            timeout=60.0,
            allow_redirects=False,
            stream=True,
        )
        return response, types.SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(
        zulip_adapter,
        "_request_file_redirect",
        request_file_redirect,
    )
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


def test_provider_file_download_follows_public_redirect_without_credentials(
    upload_adapter, monkeypatch
):
    adapter, transport = upload_adapter
    transport.redirect_to = "https://redirect-target.example.test/private"
    monkeypatch.setattr(
        zulip_adapter.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (
                zulip_adapter.socket.AF_INET,
                zulip_adapter.socket.SOCK_STREAM,
                6,
                "",
                ("93.184.216.34", 443),
            )
        ],
    )

    assert adapter.download_file("/user_uploads/1/file.txt").content == b"fixture"

    assert len(transport.sent) == 2
    assert transport.redirect_addresses == ["93.184.216.34"]
    assert "Authorization" in transport.sent[0].headers
    assert "Authorization" not in transport.sent[1].headers


def test_provider_file_download_keeps_credentials_on_safe_same_origin_redirect(
    upload_adapter, monkeypatch
):
    adapter, transport = upload_adapter
    transport.redirect_to = (
        "https://zulip.example.test/user_uploads/1/redirected-file.txt"
    )
    monkeypatch.setattr(
        zulip_adapter.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (
                zulip_adapter.socket.AF_INET,
                zulip_adapter.socket.SOCK_STREAM,
                6,
                "",
                ("93.184.216.34", 443),
            )
        ],
    )

    assert adapter.download_file("/user_uploads/1/file.txt").content == b"fixture"

    assert len(transport.sent) == 2
    assert "Authorization" in transport.sent[0].headers
    assert "Authorization" in transport.sent[1].headers


def test_provider_file_download_rejects_same_origin_redirect_outside_uploads(
    upload_adapter,
):
    adapter, transport = upload_adapter
    transport.redirect_to = "https://zulip.example.test/api/v1/users"

    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        adapter.download_file("/user_uploads/1/file.txt")

    assert not captured.value.retryable
    assert len(transport.sent) == 1


@pytest.mark.parametrize(
    "target",
    [
        "http://169.254.169.254/latest/meta-data/",
        "https://127.0.0.1/private",
        "https://[::1]/private",
        "http://redirect-target.example.test/private",
    ],
)
def test_provider_file_download_rejects_unsafe_redirect_target(
    upload_adapter, target
):
    adapter, transport = upload_adapter
    transport.redirect_to = target

    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        adapter.download_file("/user_uploads/1/file.txt")

    assert captured.value.code == "provider_file_unavailable"
    assert not captured.value.retryable
    assert captured.value.http_status == 302
    assert len(transport.sent) == 1


def test_provider_file_download_rejects_redirect_hostname_resolving_private(
    upload_adapter, monkeypatch
):
    adapter, transport = upload_adapter
    transport.redirect_to = "https://redirect-target.example.test/private"
    monkeypatch.setattr(
        zulip_adapter.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (
                zulip_adapter.socket.AF_INET,
                zulip_adapter.socket.SOCK_STREAM,
                6,
                "",
                ("10.0.0.1", 443),
            )
        ],
    )

    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        adapter.download_file("/user_uploads/1/file.txt")

    assert not captured.value.retryable
    assert len(transport.sent) == 1


@pytest.mark.parametrize(
    "target",
    [
        "https://" + "a" * 64 + ".example.test/private",
        "https://storage..example.test/private",
    ],
)
def test_provider_file_download_rejects_invalid_redirect_hostname(
    upload_adapter, target
):
    adapter, transport = upload_adapter
    transport.redirect_to = target

    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        adapter.download_file("/user_uploads/1/file.txt")

    assert captured.value.code == "provider_file_unavailable"
    assert not captured.value.retryable
    assert captured.value.http_status == 302
    assert len(transport.sent) == 1


@pytest.mark.parametrize(
    ("proxies", "retryable"),
    [
        ({}, True),
        ({"http": "http://proxy.example.test:8080"}, True),
        ({"https": "http://proxy.example.test:8080"}, False),
    ],
)
def test_redirect_dns_failure_does_not_retry_forever_behind_proxy(
    upload_adapter, monkeypatch, proxies, retryable
):
    adapter, transport = upload_adapter
    transport.redirect_to = "https://storage.example.test/private"

    def fail_dns(*args, **kwargs):
        raise OSError("synthetic DNS failure")

    monkeypatch.setattr(zulip_adapter.socket, "getaddrinfo", fail_dns)
    monkeypatch.setattr(
        zulip_adapter.requests.utils,
        "get_environ_proxies",
        lambda url: proxies,
    )

    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        adapter.download_file("/user_uploads/1/file.txt")

    assert captured.value.code == "provider_file_unavailable"
    assert captured.value.retryable is retryable
    assert captured.value.http_status == 302
    assert len(transport.sent) == 1


@pytest.mark.parametrize(
    ("status", "retryable"),
    [(403, True), (404, False), (410, False)],
)
def test_redirected_storage_http_failure_classification(
    upload_adapter, monkeypatch, status, retryable
):
    adapter, transport = upload_adapter
    transport.redirect_to = "https://redirect-target.example.test/private"
    transport.status_code = status
    monkeypatch.setattr(
        zulip_adapter.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (
                zulip_adapter.socket.AF_INET,
                zulip_adapter.socket.SOCK_STREAM,
                6,
                "",
                ("93.184.216.34", 443),
            )
        ],
    )

    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        adapter.download_file("/user_uploads/1/file.txt")

    assert captured.value.retryable is retryable
    assert not captured.value.provider_response
    assert captured.value.http_status == status
    assert len(transport.sent) == 2
    assert "Authorization" not in transport.sent[1].headers


def test_provider_file_download_rejects_a_second_redirect(
    upload_adapter, monkeypatch
):
    adapter, transport = upload_adapter
    transport.redirect_to = "https://redirect-target.example.test/private"
    transport.redirect_responses = 2
    monkeypatch.setattr(
        zulip_adapter.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (
                zulip_adapter.socket.AF_INET,
                zulip_adapter.socket.SOCK_STREAM,
                6,
                "",
                ("93.184.216.34", 443),
            )
        ],
    )

    with pytest.raises(zulip_adapter.ZulipOperationError) as captured:
        adapter.download_file("/user_uploads/1/file.txt")

    assert not captured.value.retryable
    assert captured.value.http_status == 302
    assert len(transport.sent) == 2


def test_pinned_redirect_adapter_connects_to_validated_address(monkeypatch):
    adapter = zulip_adapter._PinnedAddressAdapter(
        "storage.example.test",
        zulip_adapter.ipaddress.ip_address("93.184.216.34"),
    )
    captured = {}
    expected_pool = object()

    def connection_from_host(**kwargs):
        captured.update(kwargs)
        return expected_pool

    monkeypatch.setattr(
        adapter.poolmanager,
        "connection_from_host",
        connection_from_host,
    )
    request = requests.Request(
        "GET", "https://storage.example.test/object"
    ).prepare()

    assert adapter.get_connection_with_tls_context(request, True) is expected_pool
    adapter.add_headers(request)
    assert captured["host"] == "93.184.216.34"
    assert captured["pool_kwargs"]["server_hostname"] == "storage.example.test"
    assert captured["pool_kwargs"]["assert_hostname"] == "storage.example.test"
    assert request.headers["Host"] == "storage.example.test"


def test_pinned_redirect_adapter_pins_https_tunnel_through_proxy(monkeypatch):
    adapter = zulip_adapter._PinnedAddressAdapter(
        "storage.example.test",
        zulip_adapter.ipaddress.ip_address("93.184.216.34"),
    )
    captured = {}
    expected_pool = object()

    class ProxyManager:
        def connection_from_host(self, **kwargs):
            captured.update(kwargs)
            return expected_pool

    monkeypatch.setattr(
        adapter,
        "proxy_manager_for",
        lambda proxy: ProxyManager(),
    )
    request = requests.Request(
        "GET", "https://storage.example.test/object"
    ).prepare()

    assert (
        adapter.get_connection_with_tls_context(
            request,
            True,
            proxies={"https": "http://proxy.example.test:8080"},
        )
        is expected_pool
    )
    assert captured["host"] == "93.184.216.34"
    assert captured["pool_kwargs"]["server_hostname"] == "storage.example.test"
    assert captured["pool_kwargs"]["assert_hostname"] == "storage.example.test"


def test_pinned_redirect_adapter_uses_validated_address_for_http_proxy():
    adapter = zulip_adapter._PinnedAddressAdapter(
        "storage.example.test",
        zulip_adapter.ipaddress.ip_address("93.184.216.34"),
    )
    request = requests.Request(
        "GET", "http://storage.example.test:8080/object?download=1"
    ).prepare()

    assert adapter.request_url(
        request,
        {"http": "http://proxy.example.test:3128"},
    ) == "http://93.184.216.34:8080/object?download=1"
    adapter.add_headers(request)
    assert request.headers["Host"] == "storage.example.test:8080"


def test_redirect_request_keeps_environment_proxies_without_netrc_auth(monkeypatch):
    calls = []

    class Session:
        trust_env = True

        def mount(self, *args):
            pass

        def get(self, url, **kwargs):
            calls.append((self.trust_env, url, kwargs))
            return requests.Response()

        def close(self):
            pass

    monkeypatch.setattr(zulip_adapter.requests, "Session", Session)
    redirect = zulip_adapter._FileRedirect(
        "https://storage.example.test/object",
        "storage.example.test",
        (zulip_adapter.ipaddress.ip_address("93.184.216.34"),),
        False,
    )

    response, session = zulip_adapter._request_file_redirect(
        redirect,
        redirect.addresses[0],
        auth=None,
        verify=True,
    )

    assert response is not None
    assert session is not None
    assert calls[0][0] is True
    assert calls[0][2]["auth"] is zulip_adapter.NO_REDIRECT_AUTH


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
