import dataclasses
import hashlib
import io
import typing
import uuid

import httpx

from workspace_zulip_bridge import config, mtls

MAX_FILE_BYTES = 52_428_800


@dataclasses.dataclass(frozen=True)
class IncomingFile:
    file_uuid: uuid.UUID
    name: str
    content_type: str
    content: bytes


class FileApiClient:
    def __init__(
        self,
        settings: config.FileApiConfig,
        client: httpx.Client | None = None,
        object_client: httpx.Client | None = None,
    ):
        self.settings = settings
        self._owns_client = client is None
        self.client = client or self._new_client()
        self.object_client = object_client or httpx.Client(
            timeout=60.0,
            follow_redirects=False,
        )

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
        self.object_client.close()

    def _transfer_client(self, url: str) -> httpx.Client:
        target = httpx.URL(url)
        private = httpx.URL(self.settings.base_url)
        if (
            target.scheme == private.scheme
            and target.host == private.host
            and target.port == private.port
        ):
            return self.client
        return self.object_client

    def import_file(
        self,
        operation_uuid: uuid.UUID,
        account_uuid: uuid.UUID,
        chat_uuid: uuid.UUID,
        incoming: IncomingFile,
        max_bytes: int = MAX_FILE_BYTES,
    ) -> str:
        if max_bytes <= 0 or len(incoming.content) > min(max_bytes, MAX_FILE_BYTES):
            raise ValueError("Incoming file exceeds the effective file limit")
        digest = hashlib.sha256(incoming.content).hexdigest()
        descriptor = {
            "operation_uuid": str(operation_uuid),
            "external_account_uuid": str(account_uuid),
            "external_chat_uuid": str(chat_uuid),
            "name": incoming.name,
            "size_bytes": len(incoming.content),
            "content_type": incoming.content_type,
            "sha256": digest,
        }
        response = self.client.put(
            f"/v1/file-transfers/incoming/{incoming.file_uuid}",
            json=descriptor,
        )
        response.raise_for_status()
        state = typing.cast(dict[str, object], response.json())
        if state["status"] == "finalized":
            return str(state["file_urn"])
        upload = typing.cast(dict[str, object], state["upload"])
        if upload["method"] != "PUT":
            raise ValueError("Unexpected presigned upload method")
        upload_response = self._transfer_client(str(upload["url"])).put(
            str(upload["url"]),
            content=io.BytesIO(incoming.content),
            headers=typing.cast(dict[str, str], upload.get("headers", {})),
        )
        upload_response.raise_for_status()
        finalize = self.client.post(
            f"/v1/file-transfers/incoming/{incoming.file_uuid}/actions/finalize",
            json={
                "operation_uuid": str(operation_uuid),
                "allocation_generation": state["allocation_generation"],
                "size_bytes": len(incoming.content),
                "content_type": incoming.content_type,
                "sha256": digest,
            },
        )
        finalize.raise_for_status()
        finalized = typing.cast(dict[str, object], finalize.json())
        return str(finalized["file_urn"])

    def export_file(
        self,
        transfer_uuid: uuid.UUID,
        operation_uuid: uuid.UUID,
        account_uuid: uuid.UUID,
        chat_uuid: uuid.UUID,
        file_urn: str,
        max_bytes: int = MAX_FILE_BYTES,
    ) -> tuple[str, str, bytes]:
        response = self.client.put(
            f"/v1/file-transfers/outgoing/{transfer_uuid}",
            json={
                "operation_uuid": str(operation_uuid),
                "external_account_uuid": str(account_uuid),
                "external_chat_uuid": str(chat_uuid),
                "file_urn": file_urn,
            },
        )
        response.raise_for_status()
        authorization = typing.cast(dict[str, object], response.json())
        download = typing.cast(dict[str, object], authorization["download"])
        if download["method"] != "GET":
            raise ValueError("Unexpected presigned download method")
        expected_size = authorization.get("size_bytes")
        effective_limit = min(max_bytes, MAX_FILE_BYTES)
        if (
            effective_limit <= 0
            or not isinstance(expected_size, int)
            or isinstance(expected_size, bool)
            or expected_size < 0
            or expected_size > effective_limit
        ):
            raise ValueError("Outgoing file exceeds the effective file limit")
        content = bytearray()
        download_headers = dict(
            typing.cast(dict[str, str], download.get("headers", {}))
        )
        download_headers["Content-Length"] = "0"
        with self._transfer_client(str(download["url"])).stream(
            "GET",
            str(download["url"]),
            headers=download_headers,
        ) as object_response:
            object_response.raise_for_status()
            declared_length = object_response.headers.get("Content-Length")
            if declared_length is not None and int(declared_length) != expected_size:
                raise ValueError("Downloaded file length mismatch")
            for chunk in object_response.iter_bytes(64 * 1024):
                if len(content) + len(chunk) > effective_limit:
                    raise ValueError("Downloaded file exceeds the effective file limit")
                content.extend(chunk)
        if len(content) != expected_size:
            raise ValueError("Downloaded file length mismatch")
        if hashlib.sha256(content).hexdigest() != authorization["sha256"]:
            raise ValueError("Downloaded file digest mismatch")
        return (
            str(authorization["name"]),
            str(authorization["content_type"]),
            bytes(content),
        )
