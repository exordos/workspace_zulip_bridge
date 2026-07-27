import dataclasses
import datetime
import functools
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
PROVIDER_QUEUE_IDLE_TIMEOUT_SECONDS = 43_200
# History runs in the main service thread. Keep each provider quantum small so
# already captured live events return to delivery promptly between pages.
# Zulip's message endpoint is paginated server-side. A production backfill
# should amortize one request across a useful batch while the delivery outbox
# still drains it in bounded Provider API quanta.
HISTORY_PAGE_SIZE = 100
ZULIP_EMOJI_DATA_PATH = "/static/generated/emoji/emoji_api.json"
TRANSFER_NAMESPACE = uuid.UUID("8aa58582-d782-4e98-bfc3-7b5ee96e3bd6")
WORKSPACE_FILE_RE = re.compile(
    r"(?P<image>!?)\[(?P<name>[^\]]+)\]\("
    r"(?P<urn>urn:(?:file|image|video):[0-9a-f-]+(?:\?[^)]*)?)\)"
)
WORKSPACE_MENTION_RE = re.compile(
    r"\[(?P<name>[^\]]+)\]\(urn:user:(?P<uuid>[0-9a-f-]+)\)"
)
WORKSPACE_ENTITY_LINK_RE = re.compile(
    r"\[(?P<label>[^\]]+)\]\("
    r"urn:(?P<kind>message|stream|topic):(?P<uuid>[0-9a-f-]+)"
    r"\)"
)
WORKSPACE_URL_LINK_RE = re.compile(
    r"(?P<image>!?)\[(?P<label>[^\]]+)\]\("
    r"urn:url:(?P<url>https?://[^\s)]+)"
    r"\)"
)
PROVIDER_NETWORK_ERRORS = (
    requests.RequestException,
    zulip.UnrecoverableNetworkError,
    zulip.ZulipError,
)


class ZulipClient(typing.Protocol):
    def register(self, **kwargs: object) -> dict[str, object]: ...

    def get_subscriptions(
        self, request: dict[str, object] | None = None
    ) -> dict[str, object]: ...

    def get_users(self) -> dict[str, object]: ...

    def get_events(self, **kwargs: object) -> dict[str, object]: ...

    def get_messages(self, request: dict[str, object]) -> dict[str, object]: ...

    def get_profile(self) -> dict[str, object]: ...

    def send_message(self, request: dict[str, object]) -> dict[str, object]: ...

    def update_message(self, request: dict[str, object]) -> dict[str, object]: ...

    def update_stream(self, request: dict[str, object]) -> dict[str, object]: ...

    def delete_message(self, message_id: int) -> dict[str, object]: ...

    def update_message_flags(self, request: dict[str, object]) -> dict[str, object]: ...

    def add_reaction(self, request: dict[str, object]) -> dict[str, object]: ...

    def remove_reaction(self, request: dict[str, object]) -> dict[str, object]: ...

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


def _unicode_emoji_code(emoji: str) -> str:
    return "-".join(
        f"{ord(character):04x}"
        for character in emoji
        if character not in {"\ufe0e", "\ufe0f"}
    )


@functools.lru_cache(maxsize=32)
def _zulip_unicode_emoji_names(
    server_url: str,
    tls_verification: bool | str,
) -> dict[str, tuple[str, ...]]:
    try:
        response = requests.get(
            urllib.parse.urljoin(
                server_url.rstrip("/") + "/",
                ZULIP_EMOJI_DATA_PATH.lstrip("/"),
            ),
            verify=tls_verification,
            timeout=60.0,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        raise ZulipOperationError("provider_unavailable", True) from exc
    except ValueError as exc:
        raise ZulipOperationError("invalid_record", False) from exc
    if not isinstance(payload, dict):
        raise ZulipOperationError("invalid_record", False)
    code_to_names = payload.get("code_to_names")
    if not isinstance(code_to_names, dict):
        raise ZulipOperationError("invalid_record", False)
    names_by_code: dict[str, tuple[str, ...]] = {}
    for code, names in code_to_names.items():
        if not isinstance(code, str) or not isinstance(names, list):
            continue
        valid_names = tuple(name for name in names if isinstance(name, str) and name)
        if valid_names:
            names_by_code[code] = valid_names
    return names_by_code


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
                    # The bridge owns durable retry/backoff state. The official
                    # client otherwise retries failed non-long-poll requests
                    # inline for minutes instead of returning control to the
                    # dedicated account worker.
                    retry_on_errors=False,
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
        page_size: int = HISTORY_PAGE_SIZE,
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
        register_request: dict[str, object] = {
            "event_types": [
                "message",
                "update_message",
                "delete_message",
                "update_message_flags",
                "reaction",
                "subscription",
                "realm_user",
            ],
            "fetch_event_types": [
                "message",
                "subscription",
                "realm_user",
                "realm",
                "recent_private_conversations",
            ],
            "apply_markdown": False,
            # Topic mappings require a non-empty provider name. Omitting the
            # empty_topic_name capability makes Zulip use its "general chat"
            # fallback for the special empty topic on live events, matching
            # the representation returned by message history.
            "client_capabilities": {
                "notification_settings_null": True,
                "bulk_message_deletion": True,
            },
        }
        if int(getattr(self.client, "feature_level", 0)) >= 481:
            # Ask modern Zulip servers for a 12-hour idle lifetime as a safety
            # margin around the dedicated account long-poll worker. Use an integer:
            # this official client forwards free-form kwargs without JSON-
            # encoding string values, while the endpoint parses this field as
            # JSON.
            register_request["idle_queue_timeout"] = (
                PROVIDER_QUEUE_IDLE_TIMEOUT_SECONDS
            )
        try:
            result = _successful(self.client.register(**register_request))
            subscriptions = _successful(
                self.client.get_subscriptions({"include_subscribers": True})
            ).get("subscriptions")
        except PROVIDER_NETWORK_ERRORS as exc:
            raise ZulipOperationError("provider_unavailable", True) from exc
        if not isinstance(subscriptions, list) or not all(
            isinstance(subscription, dict)
            and isinstance(subscription.get("stream_id"), int)
            and isinstance(subscription.get("name"), str)
            and isinstance(subscription.get("subscribers"), list)
            and all(
                isinstance(user_id, int)
                for user_id in typing.cast(
                    list[object], subscription.get("subscribers")
                )
            )
            for subscription in subscriptions
        ):
            raise ZulipOperationError("invalid_record", False)
        result["subscriptions"] = subscriptions
        if result.get("user_id") is not None:
            self._user_id = int(result["user_id"])
        return (
            str(result["queue_id"]),
            int(result["last_event_id"]),
            result,
        )

    def channel_catalog(self, provider_chat_key: str) -> dict[str, object]:
        stream_id = self._channel_id(provider_chat_key)
        try:
            subscriptions = _successful(
                self.client.get_subscriptions({"include_subscribers": True})
            ).get("subscriptions")
            members = _successful(self.client.get_users()).get("members")
            profile = _successful(self.client.get_profile())
        except PROVIDER_NETWORK_ERRORS as exc:
            raise ZulipOperationError("provider_unavailable", True) from exc
        if (
            not isinstance(subscriptions, list)
            or not isinstance(members, list)
            or not all(
                isinstance(member, dict)
                and isinstance(member.get("user_id"), int)
                for member in members
            )
            or not isinstance(profile.get("user_id"), int)
        ):
            raise ZulipOperationError("invalid_record", False)
        subscription = next(
            (
                item
                for item in subscriptions
                if isinstance(item, dict) and item.get("stream_id") == stream_id
            ),
            None,
        )
        if (
            subscription is None
            or not isinstance(subscription.get("name"), str)
            or not isinstance(subscription.get("subscribers"), list)
            or not all(
                isinstance(user_id, int)
                for user_id in typing.cast(
                    list[object], subscription.get("subscribers")
                )
            )
        ):
            raise ZulipOperationError("invalid_record", False)
        return {
            "subscriptions": [subscription],
            "realm_users": members,
            "user_id": profile["user_id"],
        }

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
            metadata = mapping.get("metadata")
            metadata = metadata if isinstance(metadata, dict) else {}
            provider_name = metadata.get("display_name")
            if not isinstance(provider_name, str) or not provider_name:
                provider_name = match.group("name")
            return f"@**{provider_name}|{mapping['provider_id']}**"

        converted = WORKSPACE_MENTION_RE.sub(mention, content)

        def entity_link(match: re.Match[str]) -> str:
            if self.routing is None:
                return match.group("label")
            mapping = self.routing.workspace_mapping(
                match.group("kind"), match.group("uuid")
            )
            if mapping is None:
                return match.group("label")
            return self._provider_entity_link(
                match.group("kind"),
                match.group("label"),
                mapping,
            )

        converted = WORKSPACE_ENTITY_LINK_RE.sub(entity_link, converted)

        def external_link(match: re.Match[str]) -> str:
            return (
                f"{match.group('image')}[{match.group('label')}]({match.group('url')})"
            )

        converted = WORKSPACE_URL_LINK_RE.sub(external_link, converted)

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

    @staticmethod
    def _encode_hash_component(value: str) -> str:
        encoded = urllib.parse.quote(value, safe="")
        replacements = {"%": ".", "(": ".28", ")": ".29", ".": ".2E"}
        return "".join(replacements.get(character, character) for character in encoded)

    @staticmethod
    def _native_link_safe(value: str) -> bool:
        return "*" not in value and "\n" not in value and "\r" not in value

    def _provider_url(self, fragment: str) -> str:
        provider_site = self.server_url.rstrip("/")
        return f"{provider_site}/{fragment}" if provider_site else fragment

    def _channel_mapping(self, provider_channel_id: str) -> dict[str, object] | None:
        if self.routing is None:
            return None
        return self.routing.provider_mapping("stream", f"channel:{provider_channel_id}")

    @staticmethod
    def _channel_name(mapping: dict[str, object] | None) -> str | None:
        if mapping is None:
            return None
        metadata = mapping.get("metadata")
        if not isinstance(metadata, dict):
            return None
        name = metadata.get("name")
        return name if isinstance(name, str) and name else None

    def _channel_link(
        self,
        label: str,
        provider_channel_id: str,
        channel_name: str,
        topic_name: str | None = None,
        provider_message_id: str | None = None,
    ) -> str:
        if self._native_link_safe(channel_name) and (
            topic_name is None or self._native_link_safe(topic_name)
        ):
            reference = channel_name
            if topic_name is not None:
                reference += f">{topic_name}"
            if provider_message_id is not None:
                reference += f"@{provider_message_id}"
            return f"#**{reference}**"
        fragment = f"#narrow/channel/{provider_channel_id}"
        if topic_name is not None:
            fragment += f"/topic/{self._encode_hash_component(topic_name)}"
        if provider_message_id is not None:
            fragment += f"/near/{provider_message_id}"
        return f"[{label}]({self._provider_url(fragment)})"

    def _direct_stream_link(self, label: str, provider_id: str) -> str:
        _chat_type, separator, raw_ids = provider_id.partition(":")
        if not separator:
            return label
        try:
            provider_user_ids = {int(value) for value in raw_ids.split(",") if value}
        except ValueError:
            return label
        original_provider_user_ids = provider_user_ids.copy()
        if self.owner_user_uuid is not None and self.routing is not None:
            owner = self.routing.workspace_mapping("identity", self.owner_user_uuid)
            if owner is not None:
                try:
                    provider_user_ids.discard(int(str(owner["provider_id"])))
                except (KeyError, TypeError, ValueError):
                    return label
        if not provider_user_ids:
            provider_user_ids = original_provider_user_ids
        if not provider_user_ids:
            return label
        sorted_ids = sorted(provider_user_ids)
        slug = ",".join(str(value) for value in sorted_ids)
        slug += "-user" if len(sorted_ids) == 1 else "-group"
        return f"[{label}]({self._provider_url(f'#narrow/dm/{slug}')})"

    def _provider_entity_link(
        self,
        entity_kind: str,
        label: str,
        mapping: dict[str, object],
    ) -> str:
        provider_id = str(mapping.get("provider_id", ""))
        metadata = mapping.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        if entity_kind == "stream":
            if provider_id.startswith("channel:"):
                provider_channel_id = provider_id.removeprefix("channel:")
                channel_name = self._channel_name(mapping)
                if channel_name is None:
                    return label
                return self._channel_link(label, provider_channel_id, channel_name)
            if provider_id.startswith(("direct:", "group_direct:")):
                return self._direct_stream_link(label, provider_id)
            return label

        if entity_kind == "topic":
            chat_key = metadata.get("chat_key")
            if isinstance(chat_key, str) and chat_key.startswith(
                ("direct:", "group_direct:")
            ):
                return self._direct_stream_link(label, chat_key)
            provider_channel_id, separator, topic_name = provider_id.partition(":")
            if not separator:
                return label
            channel_mapping = self._channel_mapping(provider_channel_id)
            channel_name = self._channel_name(channel_mapping)
            if channel_name is None:
                return label
            return self._channel_link(
                label,
                provider_channel_id,
                channel_name,
                topic_name,
            )

        if entity_kind == "message":
            if not provider_id.isdigit():
                return label
            chat_key = metadata.get("chat_key")
            if isinstance(chat_key, str) and chat_key.startswith("channel:"):
                provider_channel_id = chat_key.removeprefix("channel:")
                channel_mapping = self._channel_mapping(provider_channel_id)
                channel_name = self._channel_name(channel_mapping)
                topic_name = metadata.get("subject")
                if channel_name is not None and isinstance(topic_name, str):
                    return self._channel_link(
                        label,
                        provider_channel_id,
                        channel_name,
                        topic_name,
                        provider_id,
                    )
            return f"[{label}]({self._provider_url(f'#narrow/near/{provider_id}')})"

        return label

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
            call_endpoint = getattr(self.client, "call_endpoint", None)
            if callable(call_endpoint):
                response = call_endpoint(
                    url="events",
                    method="GET",
                    request={
                        "queue_id": queue_id,
                        "last_event_id": last_event_id,
                    },
                    longpolling=True,
                )
            else:
                # Small test doubles and compatible client implementations may
                # expose only the generated endpoint method.
                response = self.client.get_events(
                    queue_id=queue_id,
                    last_event_id=last_event_id,
                )
            result = _successful(response)
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

    @staticmethod
    def _reaction_result(
        result: dict[str, object],
        already_converged_code: str,
    ) -> None:
        try:
            _successful(result)
        except ZulipOperationError as exc:
            if exc.code != already_converged_code:
                raise

    def _reaction_request(
        self,
        payload: dict[str, object],
        message_uuid: object,
        emoji_name: object,
        *,
        include_provider_identity: bool,
    ) -> dict[str, object]:
        message = self._workspace_mapping("message", message_uuid)
        provider_emoji_name, emoji_code, reaction_type = (
            self._provider_reaction_identity(emoji_name)
        )
        request: dict[str, object] = {
            "message_id": int(str(message["provider_id"])),
            "emoji_name": provider_emoji_name,
        }
        if emoji_code is not None and reaction_type is not None:
            request.update(
                {
                    "emoji_code": emoji_code,
                    "reaction_type": reaction_type,
                }
            )
        provider = payload.get("provider")
        if (
            include_provider_identity
            and emoji_code is None
            and isinstance(provider, dict)
        ):
            emoji_code = provider.get("emoji_code")
            reaction_type = provider.get("reaction_type")
            if isinstance(emoji_code, str) and isinstance(reaction_type, str):
                request.update(
                    {
                        "emoji_code": emoji_code,
                        "reaction_type": reaction_type,
                    }
                )
        return request

    def _provider_reaction_identity(
        self,
        emoji_name: object,
    ) -> tuple[str, str | None, str | None]:
        value = str(emoji_name)
        if value.isascii():
            return value, None, None
        emoji_code = _unicode_emoji_code(value)
        if not emoji_code:
            raise ZulipOperationError("invalid_record", False)
        names = _zulip_unicode_emoji_names(
            self.server_url,
            typing.cast(
                bool | str,
                getattr(self.client, "tls_verification", True),
            ),
        ).get(emoji_code)
        if not names:
            raise ZulipOperationError("unsupported_operation", False)
        return names[0], emoji_code, "unicode_emoji"

    def _add_reaction(
        self,
        payload: dict[str, object],
        message_uuid: object,
        emoji_name: object,
    ) -> str:
        request = self._reaction_request(
            payload,
            message_uuid,
            emoji_name,
            include_provider_identity=False,
        )
        self._reaction_result(
            self.client.add_reaction(request),
            "reaction_already_exists",
        )
        return self._reaction_provider_id(payload, request)

    def _remove_reaction(
        self,
        payload: dict[str, object],
        message_uuid: object,
        emoji_name: object,
    ) -> str:
        request = self._reaction_request(
            payload,
            message_uuid,
            emoji_name,
            include_provider_identity=True,
        )
        self._reaction_result(
            self.client.remove_reaction(request),
            "reaction_does_not_exist",
        )
        return self._reaction_provider_id(payload, request)

    def _reaction_provider_id(
        self,
        payload: dict[str, object],
        request: dict[str, object],
    ) -> str:
        if self.routing is None:
            raise ZulipOperationError("not_found", False)
        identity = self.routing.workspace_mapping(
            "identity",
            str(payload["user_uuid"]),
        )
        if identity is None:
            raise ZulipOperationError("not_found", False)
        return ":".join(
            (
                str(int(str(request["message_id"]))),
                str(int(str(identity["provider_id"]))),
                str(request["emoji_name"]),
            )
        )

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
        if kind == "reaction.create":
            entity_id = self._add_reaction(
                payload,
                payload["message_uuid"],
                payload["emoji_name"],
            )
            return entity_id, None
        if kind == "reaction.update":
            previous_message_uuid = payload.get("previous_message_uuid")
            previous_emoji_name = payload.get("previous_emoji_name")
            if previous_message_uuid is None or previous_emoji_name is None:
                raise ZulipOperationError("invalid_record", False)
            self._remove_reaction(
                payload,
                previous_message_uuid,
                previous_emoji_name,
            )
            entity_id = self._add_reaction(
                payload,
                payload["message_uuid"],
                payload["emoji_name"],
            )
            return entity_id, None
        if kind == "reaction.delete":
            entity_id = self._remove_reaction(
                payload,
                payload["message_uuid"],
                payload["emoji_name"],
            )
            return entity_id, None
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
