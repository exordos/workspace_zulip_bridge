import datetime
import email.utils
import typing
import uuid

import httpx

from workspace_zulip_bridge import __version__, config, mtls

CAPABILITIES: dict[str, dict[str, object]] = {
    "messenger.chat_catalog": {"revision": 1, "limits": {}},
    "messenger.message.send": {"revision": 1, "limits": {}},
    "messenger.message.edit": {"revision": 1, "limits": {}},
    "messenger.message.delete": {"revision": 1, "limits": {}},
    "messenger.message.read": {"revision": 1, "limits": {}},
    "messenger.stream.rename": {"revision": 1, "limits": {}},
    "messenger.topic.rename": {"revision": 1, "limits": {}},
    "messenger.file.transfer": {
        "revision": 1,
        "limits": {"max_file_bytes": 52_428_800},
    },
}
RESOURCE_TYPES = (
    "custom_ca_bundle",
    "external_account",
    "external_chat_assignment",
    "external_provider_policy",
)


class ControlCursorExpired(RuntimeError):
    pass


class ControlRetryableError(RuntimeError):
    def __init__(self, status_code: int, retry_after_seconds: float | None):
        super().__init__(f"Retryable control response: HTTP {status_code}")
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


class ControlClient:
    def __init__(
        self,
        settings: config.ControlConfig,
        client: httpx.Client | None = None,
    ):
        self.settings = settings
        self._owns_client = client is None
        self.client = client or self._new_client()

    def _new_client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.settings.base_url,
            verify=mtls.client_context(
                self.settings.ca_file,
                self.settings.certificate_file,
                self.settings.private_key_file,
            ),
            timeout=10.0,
            follow_redirects=False,
            headers={"Accept": "application/json"},
        )

    def reload_tls(self) -> None:
        if not self._owns_client:
            return
        self.client.close()
        self.client = self._new_client()

    def close(self) -> None:
        self.client.close()

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                retry_at = email.utils.parsedate_to_datetime(value)
            except (TypeError, ValueError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=datetime.UTC)
            return max(
                0.0,
                (retry_at - datetime.datetime.now(datetime.UTC)).total_seconds(),
            )

    @classmethod
    def _raise_for_status(cls, response: httpx.Response) -> None:
        if response.status_code == 429 or 500 <= response.status_code < 600:
            raise ControlRetryableError(
                response.status_code,
                (
                    cls._retry_after_seconds(response)
                    if response.status_code in {429, 503}
                    else None
                ),
            )
        response.raise_for_status()

    def heartbeat(
        self, blocked_batch: dict[str, object] | None = None
    ) -> dict[str, object]:
        heartbeat_uuid = str(uuid.uuid4())
        response = self.client.put(
            "/v1/bridge-instances/self/heartbeat",
            json={
                "heartbeat_uuid": heartbeat_uuid,
                "client_timestamp": datetime.datetime.now(datetime.UTC)
                .isoformat()
                .replace("+00:00", "Z"),
                "image_version": __version__,
                "provider_kind": "zulip",
                "capabilities": CAPABILITIES,
                "blocked_batch": blocked_batch,
            },
        )
        self._raise_for_status(response)
        value = typing.cast(dict[str, object], response.json())
        if value["heartbeat_uuid"] != heartbeat_uuid:
            raise ValueError("Heartbeat response UUID mismatch")
        return value

    def desired_changes(self, cursor: str) -> dict[str, object]:
        response = self.client.get(
            "/v1/desired-state/changes",
            headers={"Content-Length": "0"},
            params={
                "cursor": cursor,
                "resource_types": ",".join(RESOURCE_TYPES),
                "page_limit": "200",
            },
        )
        if response.status_code == 410:
            raise ControlCursorExpired("Desired-state cursor requires a snapshot")
        self._raise_for_status(response)
        value = typing.cast(dict[str, object], response.json())
        if value["control_schema_version"] != "v1":
            raise ValueError("Unsupported control schema")
        if value["current_cursor"] != cursor:
            raise ValueError("Control cursor response mismatch")
        return value

    def create_snapshot(self) -> dict[str, object]:
        response = self.client.post(
            "/v1/desired-state/snapshots",
            json={
                "request_uuid": str(uuid.uuid4()),
                "resource_types": list(RESOURCE_TYPES),
            },
        )
        self._raise_for_status(response)
        return typing.cast(dict[str, object], response.json())

    def snapshot_page(
        self, snapshot_token: str, page_cursor: str | None
    ) -> dict[str, object]:
        parameters = {"page_limit": "200"}
        if page_cursor is not None:
            parameters["page_cursor"] = page_cursor
        response = self.client.get(
            f"/v1/desired-state/snapshots/{snapshot_token}/pages",
            headers={"Content-Length": "0"},
            params=parameters,
        )
        self._raise_for_status(response)
        return typing.cast(dict[str, object], response.json())

    def observed_reports(self, reports: list[dict[str, object]]) -> dict[str, object]:
        if len(reports) > 500:
            raise ValueError("Observed report batch exceeds 500 items")
        response = self.client.post(
            "/v1/observed-state/reports",
            json={"reports": reports},
        )
        self._raise_for_status(response)
        return typing.cast(dict[str, object], response.json())
