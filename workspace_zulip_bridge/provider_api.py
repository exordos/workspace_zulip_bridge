import datetime
import email.utils
import typing
import uuid

import httpx

from workspace_zulip_bridge import config, mtls


class ProviderApiRetryableError(RuntimeError):
    def __init__(self, status_code: int, retry_after_seconds: float | None = None):
        super().__init__(f"Retryable Provider API response: HTTP {status_code}")
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


class ProviderApiClient:
    """mTLS client for the private Workspace Provider Data API v1."""

    API_ROOT = "/api/workspace-provider/v1"

    def __init__(
        self,
        settings: config.ProviderApiConfig,
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
            timeout=self.settings.timeout_seconds,
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
        if response.status_code in {409, 429} or response.status_code >= 500:
            raise ProviderApiRetryableError(
                response.status_code,
                cls._retry_after_seconds(response),
            )
        response.raise_for_status()

    def lease_operations(
        self,
        request_uuid: uuid.UUID,
        *,
        limit: int = 20,
        lease_seconds: int = 300,
    ) -> dict[str, object]:
        response = self.client.post(
            f"{self.API_ROOT}/operations/actions/lease",
            json={
                "request_uuid": str(request_uuid),
                "limit": limit,
                "lease_seconds": lease_seconds,
            },
        )
        self._raise_for_status(response)
        value = typing.cast(dict[str, object], response.json())
        if value["request_uuid"] != str(request_uuid):
            raise ValueError("Provider operation lease response UUID mismatch")
        return value

    def report_results(self, results: list[dict[str, object]]) -> dict[str, object]:
        if not 1 <= len(results) <= 500:
            raise ValueError("Provider result batch size is invalid")
        response = self.client.post(
            f"{self.API_ROOT}/operation-results",
            json={"results": results},
        )
        self._raise_for_status(response)
        return typing.cast(dict[str, object], response.json())

    def apply_events(self, events: list[dict[str, object]]) -> dict[str, object]:
        if not 1 <= len(events) <= 500:
            raise ValueError("Provider event batch size is invalid")
        response = self.client.post(
            f"{self.API_ROOT}/events",
            json={"events": events},
        )
        self._raise_for_status(response)
        return typing.cast(dict[str, object], response.json())
