import dataclasses
import datetime
import hashlib
import io
import pathlib
import re
import typing
import urllib.parse
import uuid

import requests
import zulip

from workspace_zulip_bridge import file_api

MAX_PROVIDER_FILE_BYTES = 52_428_800
TRANSFER_NAMESPACE = uuid.UUID("8aa58582-d782-4e98-bfc3-7b5ee96e3bd6")
WORKSPACE_FILE_RE = re.compile(
    r"(?P<image>!?)\[(?P<name>[^\]]+)\]\("
    r"(?P<urn>urn:(?:file|image|video):[0-9a-f-]+(?:\?[^)]*)?)\)"
)
WORKSPACE_MENTION_RE = re.compile(
    r"\[(?P<name>[^\]]+)\]\(urn:user:(?P<uuid>[0-9a-f-]+)\)"
)
PROVIDER_NETWORK_ERRORS = (
    requests.RequestException,
    zulip.UnrecoverableNetworkError,
    zulip.ZulipError,
)


class ZulipClient(typing.Protocol):
    def register(self, **kwargs: object) -> dict[str, object]: ...

    def get_events(self, **kwargs: object) -> dict[str, object]: ...

    def get_messages(self, request: dict[str, object]) -> dict[str, object]: ...

    def get_profile(self) -> dict[str, object]: ...

    def send_message(self, request: dict[str, object]) -> dict[str, object]: ...

    def update_message(self, request: dict[str, object]) -> dict[str, object]: ...

    def update_stream(self, request: dict[str, object]) -> dict[str, object]: ...

    def delete_message(self, message_id: int) -> dict[str, object]: ...

    def update_message_flags(self, request: dict[str, object]) -> dict[str, object]: ...

    def mark_stream_as_read(self, stream_id: int) -> dict[str, object]: ...

    def mark_topic_as_read(
        self, stream_id: int, topic_name: str
    ) -> dict[str, object]: ...

    def upload_file(self, file: typing.BinaryIO) -> dict[str, object]: ...


class ZulipRoutingMappings(typing.Protocol):
    def provider_mapping(
        self, entity_kind: str, provider_id: str
    ) -> dict[str, object] | None: ...

    def workspace_mapping(
        self, entity_kind: str, workspace_uuid: str
    ) -> dict[str, object] | None: ...

    def topic_message_mapping(self, topic_uuid: str) -> dict[str, object] | None: ...

    def workspace_message_mappings_through(
        self, stream_uuid: str, topic_uuid: str | None, through_workspace_uuid: str
    ) -> list[dict[str, object]]: ...

    def external_chat_uuid(self, provider_chat_key: str) -> str: ...


@dataclasses.dataclass(frozen=True)
class ZulipCredentials:
    site: str
    email: str
    api_key: str
    cert_bundle: str | None = None


class ZulipOperationError(RuntimeError):
    def __init__(self, code: str, retryable: bool):
        super().__init__(code)
        self.code = code
        self.retryable = retryable


class ZulipAmbiguousOutcome(RuntimeError):
    """The provider may have committed a message but the response was lost."""


@dataclasses.dataclass(frozen=True)
class SendCorrelation:
    queue_id: str
    local_id: str
    last_event_id: int
    provider_rendered_content: str


@dataclasses.dataclass(frozen=True)
class ReconciliationEvidence:
    checked_at: str
    candidate_ids: tuple[str, ...]
    exact_match_count: int
    selected_provider_id: str | None


@dataclasses.dataclass(frozen=True)
class ProviderFile:
    name: str
    content_type: str
    content: bytes


def _successful(result: dict[str, object]) -> dict[str, object]:
    if result.get("result") == "success":
        return result
    code = str(result.get("code", "provider_error")).lower()
    retryable = code in {
        "rate_limit_hit",
        "request_timeout",
        "server_error",
        "provider_error",
    }
    raise ZulipOperationError(code, retryable)


class OfficialZulipAdapter:
    """Boundary for the official Python client used by Zulip 12.1.1."""

    def __init__(
        self,
        credentials: ZulipCredentials | None = None,
        client: ZulipClient | None = None,
        routing: ZulipRoutingMappings | None = None,
        owner_user_uuid: str | None = None,
        account_uuid: str | None = None,
        file_client: file_api.FileApiClient | None = None,
        file_limit: typing.Callable[[], int] | None = None,
    ):
        if client is None:
            if credentials is None:
                raise ValueError("Zulip credentials are required")
            try:
                client = zulip.Client(
                    email=credentials.email,
                    api_key=credentials.api_key,
                    site=credentials.site,
                    client="workspace-zulip-bridge/0.1",
                    cert_bundle=credentials.cert_bundle,
                )
            except PROVIDER_NETWORK_ERRORS as exc:
                raise ZulipOperationError("provider_unavailable", True) from exc
        self.client = client
        self.credentials = credentials
        self.routing = routing
        self.owner_user_uuid = owner_user_uuid
        self.account_uuid = account_uuid
        self.file_client = file_client
        self.file_limit = file_limit
        self._queue_id: str | None = None
        self._last_event_id: int | None = None
        self._user_id: int | None = None
        self._registration_snapshot: dict[str, object] | None = None
        self._prepared_operation_uuid: str | None = None

    def _provider_mapping(
        self, entity_kind: str, provider_id: object
    ) -> dict[str, object]:
        if self.routing is None:
            raise ZulipOperationError("not_found", False)
        mapping = self.routing.provider_mapping(entity_kind, str(provider_id))
        if mapping is None:
            raise ZulipOperationError("not_found", False)
        return mapping

    def _workspace_mapping(
        self, entity_kind: str, workspace_uuid: object
    ) -> dict[str, object]:
        if self.routing is None:
            raise ZulipOperationError("not_found", False)
        mapping = self.routing.workspace_mapping(entity_kind, str(workspace_uuid))
        if mapping is None:
            raise ZulipOperationError("not_found", False)
        return mapping

    def _topic_message_mapping(self, topic_uuid: object) -> dict[str, object]:
        if self.routing is None:
            raise ZulipOperationError("not_found", False)
        mapping = self.routing.topic_message_mapping(str(topic_uuid))
        if mapping is None:
            raise ZulipOperationError("not_found", False)
        return mapping

    @staticmethod
    def _channel_id(chat_key: object) -> int:
        if not isinstance(chat_key, str) or not chat_key.startswith("channel:"):
            raise ZulipOperationError("unsupported_operation", False)
        try:
            return int(chat_key.removeprefix("channel:"))
        except ValueError as exc:
            raise ZulipOperationError("not_found", False) from exc

    def _topic_name(self, chat_key: str, topic_uuid: object) -> str:
        topic_mapping = self._workspace_mapping("topic", topic_uuid)
        topic_provider_id = str(topic_mapping["provider_id"])
        prefix = f"{self._channel_id(chat_key)}:"
        if not topic_provider_id.startswith(prefix):
            raise ZulipOperationError("not_found", False)
        return topic_provider_id[len(prefix) :]

    def _message_target(
        self, operation: dict[str, object]
    ) -> tuple[dict[str, object], str]:
        payload = typing.cast(dict[str, object], operation["payload"])
        provider = typing.cast(dict[str, object], operation["provider"])
        chat_key = provider.get("chat_id")
        if not isinstance(chat_key, str):
            raise ZulipOperationError("not_found", False)
        stream_mapping = self._provider_mapping("stream", chat_key)
        metadata = stream_mapping.get("metadata")
        if not isinstance(metadata, dict):
            raise ZulipOperationError("not_found", False)
        chat_type = metadata.get("chat_type")
        if chat_type == "channel":
            channel_name = metadata.get("name")
            if not isinstance(channel_name, str) or not channel_name:
                raise ZulipOperationError("not_found", False)
            return {
                "type": "stream",
                "to": channel_name,
                "topic": self._topic_name(chat_key, payload["topic_uuid"]),
            }, chat_key
        if chat_type not in {"direct", "group_direct"}:
            raise ZulipOperationError("unsupported_operation", False)
        participants = metadata.get("participants")
        if not isinstance(participants, list):
            raise ZulipOperationError("not_found", False)
        recipient_ids = []
        for participant_uuid in participants:
            if participant_uuid == self.owner_user_uuid:
                continue
            identity = self._workspace_mapping("identity", participant_uuid)
            try:
                recipient_ids.append(int(str(identity["provider_id"])))
            except (KeyError, TypeError, ValueError) as exc:
                raise ZulipOperationError("not_found", False) from exc
        if not recipient_ids:
            raise ZulipOperationError("not_found", False)
        return {"type": "private", "to": recipient_ids}, chat_key

    @property
    def server_url(self) -> str:
        if self.credentials is not None:
            return self.credentials.site.rstrip("/")
        base_url = str(getattr(self.client, "base_url", ""))
        return base_url.removesuffix("/api/").removesuffix("/api")

    def download_file(
        self, provider_url: str, max_bytes: int = MAX_PROVIDER_FILE_BYTES
    ) -> ProviderFile:
        if not provider_url.startswith("/user_uploads/"):
            raise ZulipOperationError("invalid_provider_file_url", False)
        if max_bytes <= 0 or max_bytes > MAX_PROVIDER_FILE_BYTES:
            raise ZulipOperationError("provider_file_transfer_disabled", False)
        email = getattr(self.client, "email", None)
        api_key = getattr(self.client, "api_key", None)
        if not isinstance(email, str) or not isinstance(api_key, str):
            raise ZulipOperationError("provider_file_credentials_unavailable", False)
        response: requests.Response | None = None
        try:
            response = requests.get(
                urllib.parse.urljoin(self.server_url + "/", provider_url.lstrip("/")),
                auth=(email, api_key),
                verify=getattr(self.client, "tls_verification", True),
                timeout=60.0,
                allow_redirects=False,
                stream=True,
            )
            response.raise_for_status()
            declared_length = response.headers.get("Content-Length")
            if declared_length is not None:
                try:
                    declared_bytes = int(declared_length)
                except ValueError as exc:
                    raise ZulipOperationError(
                        "invalid_provider_file_length", False
                    ) from exc
                if declared_bytes < 0 or declared_bytes > max_bytes:
                    raise ZulipOperationError("provider_file_too_large", False)
            content = bytearray()
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                if len(content) + len(chunk) > max_bytes:
                    raise ZulipOperationError("provider_file_too_large", False)
                content.extend(chunk)
        except PROVIDER_NETWORK_ERRORS as exc:
            raise ZulipOperationError("provider_file_unavailable", True) from exc
        finally:
            if response is not None:
                response.close()
        assert response is not None
        name = (
            pathlib.PurePosixPath(urllib.parse.urlparse(provider_url).path).name
            or "zulip-file"
        )
        content_type = response.headers.get(
            "Content-Type", "application/octet-stream"
        ).split(";", 1)[0]
        return ProviderFile(name, content_type, bytes(content))

    def message_history(
        self,
        provider_chat_key: str,
        anchor: int | str = "newest",
        page_size: int = 100,
    ) -> list[dict[str, object]]:
        chat_type, _, identifiers = provider_chat_key.partition(":")
        if chat_type == "channel":
            narrow: list[dict[str, object]] = [
                {"operator": "channel", "operand": int(identifiers)}
            ]
        elif chat_type in {"direct", "group_direct"}:
            narrow = [
                {
                    "operator": "dm",
                    "operand": [int(value) for value in identifiers.split(",")],
                }
            ]
        else:
            raise ZulipOperationError("invalid_provider_chat_key", False)
        try:
            result = _successful(
                self.client.get_messages(
                    {
                        "anchor": anchor,
                        "num_before": page_size,
                        "num_after": 0,
                        "apply_markdown": False,
                        "narrow": narrow,
                    }
                )
            )
        except PROVIDER_NETWORK_ERRORS as exc:
            raise ZulipOperationError("provider_unavailable", True) from exc
        return sorted(
            typing.cast(list[dict[str, object]], result["messages"]),
            key=lambda message: (float(message["timestamp"]), int(message["id"])),
            reverse=True,
        )

    def restore_queue(self, queue_id: str, last_event_id: int) -> None:
        self._queue_id = queue_id
        self._last_event_id = last_event_id

    def ensure_queue(self) -> tuple[str, int]:
        if self._queue_id is None:
            (
                self._queue_id,
                self._last_event_id,
                self._registration_snapshot,
            ) = self.register_queue()
        assert self._last_event_id is not None
        return self._queue_id, self._last_event_id

    def take_registration_snapshot(self) -> dict[str, object] | None:
        snapshot = self._registration_snapshot
        self._registration_snapshot = None
        return snapshot

    def invalidate_queue(self) -> None:
        self._queue_id = None
        self._last_event_id = None

    def prepare(
        self,
        operation: dict[str, object],
        operation_uuid: str,
        provider_rendered_content: str | None = None,
    ) -> SendCorrelation | None:
        self._prepared_operation_uuid = operation_uuid
        if operation["kind"] != "message.create":
            return None
        if self._queue_id is None or self._last_event_id is None:
            # The long-lived provider poller owns queue registration and its
            # durable cursor. A one-shot outbound adapter must never replace it.
            raise ZulipOperationError("provider_unavailable", True)
        queue_id, last_event_id = self._queue_id, self._last_event_id
        rendered = provider_rendered_content or self._provider_message_content(
            operation, operation_uuid
        )
        return SendCorrelation(queue_id, operation_uuid, last_event_id, rendered)

    def register_queue(self) -> tuple[str, int, dict[str, object]]:
        try:
            result = _successful(
                self.client.register(
                    event_types=[
                        "message",
                        "update_message",
                        "delete_message",
                        "update_message_flags",
                        "subscription",
                        "realm_user",
                    ],
                    fetch_event_types=[
                        "message",
                        "subscription",
                        "realm_user",
                        "recent_private_conversations",
                    ],
                    apply_markdown=False,
                    client_capabilities={
                        "notification_settings_null": True,
                        "bulk_message_deletion": True,
                        "empty_topic_name": True,
                    },
                )
            )
        except PROVIDER_NETWORK_ERRORS as exc:
            raise ZulipOperationError("provider_unavailable", True) from exc
        if result.get("user_id") is not None:
            self._user_id = int(result["user_id"])
        return (
            str(result["queue_id"]),
            int(result["last_event_id"]),
            result,
        )

    def _external_chat_uuid(self, provider_chat_key: str) -> uuid.UUID:
        if self.routing is None:
            raise ZulipOperationError("not_found", False)
        return uuid.UUID(self.routing.external_chat_uuid(provider_chat_key))

    def _convert_workspace_markdown(
        self,
        content: str,
        operation_uuid: str | None,
        provider_chat_key: str,
    ) -> str:
        def mention(match: re.Match[str]) -> str:
            if self.routing is None:
                return f"@{match.group('name')}"
            mapping = self.routing.workspace_mapping("identity", match.group("uuid"))
            if mapping is None:
                return f"@{match.group('name')}"
            return f"@**{match.group('name')}|{mapping['provider_id']}**"

        converted = WORKSPACE_MENTION_RE.sub(mention, content)

        def attachment(match: re.Match[str]) -> str:
            if (
                self.file_client is None
                or self.file_limit is None
                or self.account_uuid is None
                or operation_uuid is None
            ):
                raise ZulipOperationError("provider_file_transfer_disabled", False)
            file_urn = match.group("urn")
            transfer_uuid = uuid.uuid5(
                TRANSFER_NAMESPACE,
                f"{operation_uuid}:{file_urn}",
            )
            name, _content_type, content_bytes = self.file_client.export_file(
                transfer_uuid,
                uuid.UUID(operation_uuid),
                uuid.UUID(self.account_uuid),
                self._external_chat_uuid(provider_chat_key),
                file_urn,
                max_bytes=self.file_limit(),
            )
            stream = io.BytesIO(content_bytes)
            stream.name = name  # type: ignore[attr-defined]
            uploaded = _successful(self.client.upload_file(stream))
            provider_uri = uploaded.get("uri")
            if not isinstance(provider_uri, str) or not provider_uri:
                raise ZulipOperationError("provider_file_unavailable", True)
            return f"{match.group('image')}[{match.group('name')}]({provider_uri})"

        return WORKSPACE_FILE_RE.sub(attachment, converted)

    def _reply_quote(self, reply_to_message_uuid: object) -> str:
        mapping = self._workspace_mapping("message", reply_to_message_uuid)
        provider_message_id = int(str(mapping["provider_id"]))
        try:
            response = _successful(
                self.client.get_messages(
                    {
                        "anchor": provider_message_id,
                        "num_before": 0,
                        "num_after": 0,
                        "apply_markdown": False,
                        "narrow": [{"operator": "id", "operand": provider_message_id}],
                    }
                )
            )
        except PROVIDER_NETWORK_ERRORS as exc:
            raise ZulipOperationError("provider_unavailable", True) from exc
        message = next(
            (
                candidate
                for candidate in typing.cast(
                    list[dict[str, object]], response.get("messages", [])
                )
                if int(candidate.get("id", -1)) == provider_message_id
            ),
            None,
        )
        if message is None:
            raise ZulipOperationError("not_found", False)
        sender_id = int(message["sender_id"])
        sender_name = str(message["sender_full_name"])
        provider_site = self.server_url.rstrip("/")
        link = (
            f"{provider_site}/#narrow/near/{provider_message_id}"
            if provider_site
            else f"#narrow/near/{provider_message_id}"
        )
        return (
            f"@_**{sender_name}|{sender_id}** [said]({link}):\n"
            f"```quote\n{message['content']}\n```\n\n"
        )

    def _provider_message_content(
        self, operation: dict[str, object], operation_uuid: str | None
    ) -> str:
        payload = typing.cast(dict[str, object], operation["payload"])
        provider = typing.cast(dict[str, object], operation["provider"])
        chat_key = provider.get("chat_id")
        if not isinstance(chat_key, str):
            raise ZulipOperationError("invalid_record", False)
        message = typing.cast(dict[str, object], payload["payload"])
        content = self._convert_workspace_markdown(
            str(message["content"]), operation_uuid, chat_key
        )
        reply_to = payload.get("reply_to_message_uuid")
        if reply_to is not None:
            content = self._reply_quote(reply_to) + content
        return content

    def events(self, queue_id: str, last_event_id: int) -> list[dict[str, object]]:
        try:
            result = _successful(
                self.client.get_events(
                    queue_id=queue_id,
                    last_event_id=last_event_id,
                    dont_block=True,
                )
            )
        except PROVIDER_NETWORK_ERRORS as exc:
            raise ZulipOperationError("provider_unavailable", True) from exc
        return typing.cast(list[dict[str, object]], result["events"])

    def reconcile_message(
        self,
        operation: dict[str, object],
        attempted_at: datetime.datetime,
        provider_rendered_content: str | None = None,
    ) -> ReconciliationEvidence:
        if operation["kind"] != "message.create":
            raise ZulipOperationError("unsupported_reconciliation", False)
        if self._user_id is None:
            try:
                profile = _successful(self.client.get_profile())
            except PROVIDER_NETWORK_ERRORS as exc:
                raise ZulipOperationError("provider_unavailable", True) from exc
            self._user_id = int(profile["user_id"])
        target, _ = self._message_target(operation)
        narrow: list[dict[str, object]] = [
            {"operator": "sender", "operand": self._user_id}
        ]
        if target["type"] == "stream":
            narrow.extend(
                [
                    {"operator": "channel", "operand": target["to"]},
                    {"operator": "topic", "operand": target["topic"]},
                ]
            )
        else:
            narrow.append({"operator": "dm", "operand": target["to"]})
        try:
            result = _successful(
                self.client.get_messages(
                    {
                        "anchor": "newest",
                        "num_before": 100,
                        "num_after": 0,
                        "apply_markdown": False,
                        "narrow": narrow,
                    }
                )
            )
        except PROVIDER_NETWORK_ERRORS as exc:
            raise ZulipOperationError("provider_unavailable", True) from exc
        expected = provider_rendered_content or self._provider_message_content(
            operation, None
        )
        attempted_timestamp = attempted_at.timestamp()
        lower_bound = attempted_timestamp - 5.0
        upper_bound = attempted_timestamp + 60.0
        matches: list[tuple[float, int, str]] = []
        for message in typing.cast(list[dict[str, object]], result["messages"]):
            if message.get("content") != expected:
                continue
            if int(message.get("sender_id", -1)) != self._user_id:
                continue
            timestamp = float(message.get("timestamp", 0))
            if not lower_bound <= timestamp <= upper_bound:
                continue
            provider_id = str(message["id"])
            matches.append(
                (abs(timestamp - attempted_timestamp), int(provider_id), provider_id)
            )
        matches.sort(key=lambda item: (item[0], item[1]))
        candidate_ids = tuple(item[2] for item in matches)
        checked_at = datetime.datetime.now(datetime.UTC).isoformat()
        selected = candidate_ids[0] if candidate_ids else None
        return ReconciliationEvidence(
            checked_at, candidate_ids, len(candidate_ids), selected
        )

    def apply(
        self,
        operation: dict[str, object],
        correlation: SendCorrelation | None = None,
        operation_uuid: str | None = None,
    ) -> tuple[str | None, str | None]:
        try:
            return self._apply(operation, correlation, operation_uuid)
        except PROVIDER_NETWORK_ERRORS as exc:
            raise ZulipOperationError("provider_unavailable", True) from exc

    def _apply(
        self,
        operation: dict[str, object],
        correlation: SendCorrelation | None = None,
        operation_uuid: str | None = None,
    ) -> tuple[str | None, str | None]:
        operation_uuid = operation_uuid or self._prepared_operation_uuid
        kind = str(operation["kind"])
        payload = typing.cast(dict[str, object], operation["payload"])
        provider = typing.cast(dict[str, object], operation["provider"])
        if kind == "message.create":
            if correlation is None:
                raise ZulipOperationError("missing_send_correlation", False)
            target, _ = self._message_target(operation)
            request: dict[str, object] = {
                "content": correlation.provider_rendered_content,
                "queue_id": correlation.queue_id,
                "local_id": correlation.local_id,
                **target,
            }
            try:
                result = _successful(self.client.send_message(request))
            except PROVIDER_NETWORK_ERRORS as exc:
                raise ZulipAmbiguousOutcome("provider_send_outcome_unknown") from exc
            return str(result["id"]), None
        if kind == "message.update":
            message = typing.cast(dict[str, object], payload["payload"])
            chat_key = provider.get("chat_id")
            if not isinstance(chat_key, str):
                raise ZulipOperationError("invalid_record", False)
            request = {
                "message_id": int(str(provider["entity_id"])),
                "content": self._convert_workspace_markdown(
                    str(message["content"]), operation_uuid, chat_key
                ),
            }
            previous = payload.get("previous_content")
            if isinstance(previous, str):
                request["prev_content_sha256"] = hashlib.sha256(
                    previous.encode("utf-8")
                ).hexdigest()
            _successful(self.client.update_message(request))
            return str(provider["entity_id"]), None
        if kind == "message.delete":
            _successful(self.client.delete_message(int(str(provider["entity_id"]))))
            return str(provider["entity_id"]), None
        if kind == "read_state.set":
            exact_uuids = typing.cast(list[object] | None, payload.get("message_uuids"))
            through_uuid = payload.get("through_message_uuid")
            if exact_uuids is not None:
                if self.routing is None:
                    raise ZulipOperationError("not_found", False)
                provider_ids = []
                for value in exact_uuids:
                    mapping = self.routing.workspace_mapping("message", str(value))
                    if mapping is not None:
                        provider_ids.append(int(str(mapping["provider_id"])))
                if not provider_ids:
                    return None, None
                _successful(
                    self.client.update_message_flags(
                        {
                            "messages": provider_ids,
                            "op": "add" if payload["read"] else "remove",
                            "flag": "read",
                        }
                    )
                )
                return str(max(provider_ids)), None
            if through_uuid is None:
                if not payload["read"]:
                    raise ZulipOperationError("unsupported_operation", False)
                chat_key = provider.get("chat_id")
                stream_id = self._channel_id(chat_key)
                topic_uuid = payload["topic_uuid"]
                if topic_uuid is None:
                    _successful(self.client.mark_stream_as_read(stream_id))
                    return str(stream_id), None
                assert isinstance(chat_key, str)
                topic_name = self._topic_name(chat_key, topic_uuid)
                _successful(self.client.mark_topic_as_read(stream_id, topic_name))
                return str(stream_id), None
            if self.routing is None:
                raise ZulipOperationError("not_found", False)
            if hasattr(self.routing, "workspace_message_mappings_through"):
                mappings = self.routing.workspace_message_mappings_through(
                    str(payload["stream_uuid"]),
                    (
                        None
                        if payload["topic_uuid"] is None
                        else str(payload["topic_uuid"])
                    ),
                    str(through_uuid),
                )
            else:
                mappings = [self._workspace_mapping("message", through_uuid)]
            provider_ids = [int(str(mapping["provider_id"])) for mapping in mappings]
            if not provider_ids:
                raise ZulipOperationError("not_found", False)
            request = {
                "messages": provider_ids,
                "op": "add" if payload["read"] else "remove",
                "flag": "read",
            }
            _successful(self.client.update_message_flags(request))
            return str(max(provider_ids)), None
        if kind == "stream.upsert":
            chat_key = provider.get("chat_id")
            stream_id = self._channel_id(chat_key)
            _successful(
                self.client.update_stream(
                    {"stream_id": stream_id, "new_name": payload["name"]}
                )
            )
            return str(stream_id), None
        if kind == "topic.upsert":
            message = self._topic_message_mapping(operation["entity_uuid"])
            request = {"message_id": int(str(message["provider_id"]))}
            request["topic"] = payload["name"]
            request["propagate_mode"] = "change_all"
            _successful(self.client.update_message(request))
            return str(message["provider_id"]), None
        raise ZulipOperationError("unsupported_operation", False)
