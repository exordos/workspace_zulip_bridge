import copy
import datetime
import hashlib
import re
import typing
import urllib.parse
import uuid

from workspace_zulip_bridge import canonical, markdown_conversion

OPERATION_NAMESPACE = uuid.UUID("9d8b6952-b2de-4c80-a9c7-9619aaf5f35d")
ENTITY_NAMESPACE = uuid.UUID("9a1d0e75-50a5-413c-b3e8-d070232ef57f")
MENTION_RE = re.compile(
    r"@_?\*\*(?:"
    r"(?P<name_with_id>[^*|]+)\|(?P<user_id>[0-9]+)"
    r"|\|(?P<user_id_only>[0-9]+)"
    r"|(?P<name_only>[^*]+)"
    r")\*\*"
)
REPLY_FRAGMENT_RE = re.compile(r"^narrow/(?:.*/)?near/(?P<id>[0-9]+)$")
ZULIP_NATIVE_LINK_RE = re.compile(r"#\*\*(?P<reference>[^*]+)\*\*")
ANGLE_URL_RE = re.compile(r"<(?P<url>https?://[^>\s]+)>")
BARE_URL_RE = re.compile(r"(?<!urn:url:)(?P<url>https?://[^\s<]+|www\.[^\s<]+)")
SCHEMELESS_WEB_TARGET_RE = re.compile(
    r"(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?::[0-9]+)?(?:[/?#].*)?"
)
ZULIP_EMPTY_TOPIC_FALLBACK_NAME = "general chat"
ZULIP_DIRECT_TOPIC_NAME = "Zulip"
# Keep provider message projections within Workspace MarkdownPayload.content.
WORKSPACE_MARKDOWN_MAX_LENGTH = 40_000
WORKSPACE_MARKDOWN_TRUNCATION_MARKER = "\n\n[Message truncated]"


def is_empty_channel_topic(subject: str) -> bool:
    return subject == "" or subject.casefold() == ZULIP_EMPTY_TOPIC_FALLBACK_NAME


def channel_topic_name(subject: str) -> str:
    if is_empty_channel_topic(subject):
        return ZULIP_EMPTY_TOPIC_FALLBACK_NAME
    return subject


def channel_topic_provider_id(stream_id: object, subject: str) -> str:
    return f"{stream_id}:{channel_topic_name(subject)}"


class ConversionStore(typing.Protocol):
    def account_resource(self, account_uuid: str) -> dict[str, object] | None: ...

    def account_settings(self, account_uuid: str) -> dict[str, object] | None: ...

    def assignment_for_provider_chat(
        self, account_uuid: str, provider_chat_key: str
    ) -> dict[str, object] | None: ...

    def producer_lane_position(
        self, operation_uuid: str, origin: str, causal_lane: str
    ) -> tuple[int, str | None]: ...

    def provider_mapping(
        self, account_uuid: str, entity_kind: str, provider_id: str
    ) -> dict[str, object] | None: ...

    def provider_mapping_by_name(
        self, account_uuid: str, entity_kind: str, name: str
    ) -> dict[str, object] | None: ...

    def workspace_mapping(
        self, account_uuid: str, entity_kind: str, workspace_uuid: str
    ) -> dict[str, object] | None: ...

    def accepted_provider_message_context(
        self, account_uuid: str, queue_id: str, event_id: int
    ) -> dict[str, object] | None: ...

    def remember_provider_mapping(
        self,
        account_uuid: str,
        entity_kind: str,
        provider_id: str,
        workspace_uuid: str,
        metadata: dict[str, object],
        provider_revision: str | None = None,
    ) -> None: ...

    def rename_provider_mapping(
        self,
        account_uuid: str,
        entity_kind: str,
        old_provider_id: str,
        new_provider_id: str,
        metadata: dict[str, object],
        provider_revision: str | None = None,
    ) -> dict[str, object] | None: ...


FileResolver = typing.Callable[[str, str], str]


class ZulipLinkResolver:
    def __init__(
        self,
        store: ConversionStore,
        account_uuid: str,
        owner_uuid: str,
    ):
        self.store = store
        self.account_uuid = account_uuid
        self.owner_uuid = owner_uuid
        self._channels_by_name: dict[str, dict[str, object] | None] = {}
        self._owner_provider_id_loaded = False
        self._owner_provider_id: int | None = None

    @staticmethod
    def _urn(entity_kind: str, mapping: dict[str, object] | None) -> str | None:
        if mapping is None:
            return None
        urn_kind = {
            "identity": "user",
            "message": "message",
            "stream": "stream",
            "topic": "topic",
        }.get(entity_kind)
        if urn_kind is None:
            return None
        return f"urn:{urn_kind}:{mapping['workspace_uuid']}"

    def provider_urn(self, entity_kind: str, provider_id: str) -> str | None:
        return self._urn(
            entity_kind,
            self.store.provider_mapping(self.account_uuid, entity_kind, provider_id),
        )

    def channel_mapping(self, channel_name: str) -> dict[str, object] | None:
        cache_key = channel_name.casefold()
        if cache_key in self._channels_by_name:
            return self._channels_by_name[cache_key]
        mapping = self.store.provider_mapping_by_name(
            self.account_uuid, "stream", channel_name
        )
        if mapping is None:
            self._channels_by_name[cache_key] = None
            return None
        metadata = mapping.get("metadata")
        if (
            not isinstance(metadata, dict)
            or metadata.get("chat_type") != "channel"
            or not str(mapping.get("provider_id", "")).startswith("channel:")
        ):
            self._channels_by_name[cache_key] = None
            return None
        self._channels_by_name[cache_key] = mapping
        return mapping

    def channel_urn(self, channel_name: str) -> str | None:
        return self._urn("stream", self.channel_mapping(channel_name))

    def topic_urn(self, channel_name: str, topic_name: str) -> str | None:
        channel = self.channel_mapping(channel_name)
        if channel is None:
            return None
        provider_id = str(channel["provider_id"]).removeprefix("channel:")
        return self.provider_urn("topic", f"{provider_id}:{topic_name}")

    def direct_stream_urn(self, provider_user_ids: list[int]) -> str | None:
        ids = {int(value) for value in provider_user_ids}
        if not self._owner_provider_id_loaded:
            owner = self.store.workspace_mapping(
                self.account_uuid, "identity", self.owner_uuid
            )
            try:
                self._owner_provider_id = (
                    None if owner is None else int(str(owner["provider_id"]))
                )
            except (KeyError, TypeError, ValueError):
                self._owner_provider_id = None
            self._owner_provider_id_loaded = True
        if self._owner_provider_id is None:
            return None
        ids.add(self._owner_provider_id)
        if not ids:
            return None
        chat_type = "direct" if len(ids) <= 2 else "group_direct"
        provider_id = f"{chat_type}:{','.join(str(value) for value in sorted(ids))}"
        return self.provider_urn("stream", provider_id)


def stable_entity_uuid(account_uuid: str, kind: str, provider_id: str) -> str:
    return str(
        uuid.uuid5(
            ENTITY_NAMESPACE,
            f"zulip:{uuid.UUID(account_uuid)}:{kind}:{provider_id}",
        )
    )


def provider_chat_reference(message: dict[str, object]) -> tuple[str, str]:
    if message["type"] == "stream":
        return "channel", f"channel:{int(message['stream_id'])}"
    recipients = typing.cast(list[dict[str, object]], message["display_recipient"])
    participant_ids = sorted(int(recipient["id"]) for recipient in recipients)
    chat_type = "direct" if len(participant_ids) == 2 else "group_direct"
    return chat_type, f"{chat_type}:{','.join(map(str, participant_ids))}"


def provider_chat_assignment(
    store: ConversionStore, account_uuid: str, provider_chat_key: str
) -> tuple[str, bool]:
    assignment = store.assignment_for_provider_chat(account_uuid, provider_chat_key)
    if assignment is not None:
        if not bool(assignment.get("selected", True)):
            raise ValueError("provider_chat_not_selected")
        return str(assignment["project_id"]), True
    settings = store.account_settings(account_uuid)
    if settings is None or settings["selection_mode"] != "all":
        raise ValueError("provider_chat_not_selected")
    raise ValueError("provider_chat_assignment_pending")


def _reconcile_assignment_projection(
    store: ConversionStore,
    account_uuid: str,
    provider_chat_key: str,
) -> bool:
    reconcile = getattr(store, "reconcile_assignment_projection", None)
    return bool(
        callable(reconcile) and reconcile(account_uuid, provider_chat_key)
    )


def operation_uuid_for(
    account_uuid: str, queue_id: str, event_id: int, subindex: int
) -> str:
    return str(
        uuid.uuid5(
            OPERATION_NAMESPACE,
            f"{account_uuid}:{queue_id}:{event_id}:{subindex}",
        )
    )


def _accepted_live_replay_records(
    account_uuid: str,
    event: dict[str, object],
    accepted_context: dict[str, object],
) -> list[dict[str, object]] | None:
    accepted_records = accepted_context.get("accepted_records")
    if not isinstance(accepted_records, list) or not accepted_records:
        return None
    if accepted_context.get("accepted_records_complete") is not True:
        raise ValueError("provider_event_replay_incomplete")
    provider_message_id = str(
        typing.cast(dict[str, object], event["message"])["id"]
    )
    records_by_uuid: dict[str, dict[str, object]] = {}
    for record in accepted_records:
        if not isinstance(record, dict):
            return None
        operation_uuid = record.get("operation_uuid")
        if not isinstance(operation_uuid, str) or operation_uuid in records_by_uuid:
            return None
        records_by_uuid[operation_uuid] = record
    record_sources = [f"provider-message:{provider_message_id}"]
    accepted_project_uuid = accepted_context.get("project_uuid")
    if isinstance(accepted_project_uuid, str):
        record_sources.insert(
            0,
            f"provider-message:{provider_message_id}:project:{accepted_project_uuid}",
        )
    for record_source in record_sources:
        remaining = dict(records_by_uuid)
        ordered: list[dict[str, object]] = []
        for subindex in range(len(remaining)):
            operation_uuid = operation_uuid_for(
                account_uuid,
                record_source,
                int(event["id"]),
                subindex,
            )
            record = remaining.pop(operation_uuid, None)
            if record is None:
                break
            ordered.append(copy.deepcopy(record))
        if not remaining:
            return ordered
    return None


def _record(
    store: ConversionStore,
    account_uuid: str,
    project_uuid: str,
    queue_id: str,
    event_id: int,
    subindex: int,
    operation: dict[str, object],
    causal_lane: str,
    created_at: datetime.datetime,
    delivery_class: str,
) -> dict[str, object]:
    operation_uuid = operation_uuid_for(account_uuid, queue_id, event_id, subindex)
    sequence, predecessor = store.producer_lane_position(
        operation_uuid, "zulip", causal_lane
    )
    record: dict[str, object] = {
        "schema": "workspace.provider",
        "schema_version": 1,
        "record_kind": "operation",
        "record_uuid": str(uuid.uuid5(OPERATION_NAMESPACE, operation_uuid + ":record")),
        "operation_uuid": operation_uuid,
        "attempt": 1,
        "operation_sha256": "",
        "account_uuid": str(uuid.UUID(account_uuid)),
        "project_uuid": str(uuid.UUID(project_uuid)),
        "origin": "zulip",
        "causal_lane": causal_lane,
        "sequence": sequence,
        "predecessor_operation_uuid": predecessor,
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
        "expires_at": None,
        "operation": operation,
    }
    extensions = typing.cast(dict[str, object], operation.setdefault("extensions", {}))
    extensions["delivery_class"] = delivery_class
    record["operation_sha256"] = canonical.operation_digest(record)
    return record


def convert_markdown(
    content: str,
    mention_uuids: dict[str, str],
    original_url: str,
    file_resolver: FileResolver | None = None,
    link_resolver: ZulipLinkResolver | None = None,
) -> tuple[str, bool]:
    """Convert raw Zulip Markdown without leaking provider-only file URLs."""
    converted, lossy = _convert_zulip_links(
        content,
        original_url,
        link_resolver,
        mention_uuids=mention_uuids,
        file_resolver=file_resolver,
    )
    if lossy and original_url and original_url not in converted:
        converted = f"{converted}\n\n[Open original](urn:url:{original_url})"
    if len(converted) > WORKSPACE_MARKDOWN_MAX_LENGTH:
        marker = WORKSPACE_MARKDOWN_TRUNCATION_MARKER
        if _provider_site(original_url):
            linked_marker = (
                f"\n\n[Message truncated; open original](urn:url:{original_url})"
            )
            if len(linked_marker) < WORKSPACE_MARKDOWN_MAX_LENGTH:
                marker = linked_marker
        converted = (
            converted[: WORKSPACE_MARKDOWN_MAX_LENGTH - len(marker)] + marker
        )
        lossy = True
    return converted, lossy


def _provider_site(original_url: str) -> str:
    parsed = urllib.parse.urlsplit(original_url)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def _decode_hash_component(value: str) -> str:
    return urllib.parse.unquote(value.replace(".", "%"))


def _channel_id_from_slug(slug: str) -> str | None:
    candidate = slug.split("-", 1)[0]
    return candidate if candidate.isdigit() else None


def _dm_user_ids_from_slug(slug: str) -> list[int] | None:
    candidate = slug.split("-", 1)[0]
    if not candidate:
        return None
    parts = candidate.split(",")
    if not all(part.isdigit() for part in parts):
        return None
    return [int(part) for part in parts]


def _same_provider_url(target: str, provider_site: str) -> bool:
    if target.startswith("#") or (
        target.startswith("/") and not target.startswith("//")
    ):
        return True
    parsed = urllib.parse.urlsplit(target)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    if not provider_site:
        return False
    provider = urllib.parse.urlsplit(provider_site)
    return (
        parsed.scheme.lower(),
        parsed.netloc.lower(),
    ) == (
        provider.scheme.lower(),
        provider.netloc.lower(),
    )


def _zulip_url_urn(
    target: str,
    provider_site: str,
    resolver: ZulipLinkResolver | None,
) -> str | None:
    if resolver is None or not _same_provider_url(target, provider_site):
        return None
    parsed = urllib.parse.urlsplit(target)
    fragment = parsed.fragment
    if fragment.startswith("user/"):
        provider_user_id = fragment.removeprefix("user/").split("/", 1)[0]
        if provider_user_id.isdigit():
            return resolver.provider_urn("identity", provider_user_id)
        return None
    if not fragment.startswith("narrow/"):
        return None

    parts = fragment.split("/")
    terms: list[tuple[str, str]] = []
    for index in range(1, len(parts), 2):
        operator = _decode_hash_component(parts[index]).lower().removeprefix("-")
        operator = {
            "stream": "channel",
            "pm": "dm",
            "pm-with": "dm",
        }.get(operator, operator)
        operand = parts[index + 1] if index + 1 < len(parts) else ""
        terms.append((operator, operand))

    near = next(
        (
            operand
            for operator, operand in terms
            if operator == "near" and operand.isdigit()
        ),
        None,
    )
    if near is not None:
        return resolver.provider_urn("message", near)

    channel_operand = next(
        (operand for operator, operand in terms if operator == "channel"),
        None,
    )
    if channel_operand is not None:
        channel_id = _channel_id_from_slug(channel_operand)
        channel_mapping = (
            resolver.store.provider_mapping(
                resolver.account_uuid, "stream", f"channel:{channel_id}"
            )
            if channel_id is not None
            else None
        )
        if channel_mapping is None and channel_id is None:
            legacy_name = _decode_hash_component(channel_operand)
            channel_mapping = resolver.channel_mapping(legacy_name)
            if channel_mapping is None and "-" in legacy_name:
                channel_mapping = resolver.channel_mapping(
                    legacy_name.replace("-", " ")
                )
        if channel_mapping is None:
            return None
        topic_operand = next(
            (operand for operator, operand in terms if operator == "topic"),
            None,
        )
        if topic_operand is None:
            return resolver._urn("stream", channel_mapping)
        provider_channel_id = str(channel_mapping["provider_id"]).removeprefix(
            "channel:"
        )
        return resolver.provider_urn(
            "topic",
            f"{provider_channel_id}:{_decode_hash_component(topic_operand)}",
        )

    dm_operand = next(
        (operand for operator, operand in terms if operator == "dm"),
        None,
    )
    if dm_operand is not None:
        provider_user_ids = _dm_user_ids_from_slug(dm_operand)
        if provider_user_ids is not None:
            return resolver.direct_stream_urn(provider_user_ids)
    return None


def _absolute_provider_url(target: str, provider_site: str) -> str | None:
    if target.startswith(("#", "/")):
        if not provider_site:
            return None
        return urllib.parse.urljoin(provider_site.rstrip("/") + "/", target)
    parsed = urllib.parse.urlsplit(target)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return target
    if SCHEMELESS_WEB_TARGET_RE.fullmatch(target):
        return f"https://{target}"
    return None


def _workspace_link_target(
    target: str,
    provider_site: str,
    resolver: ZulipLinkResolver | None,
) -> str:
    if target.startswith("urn:"):
        return target
    entity_urn = _zulip_url_urn(target, provider_site, resolver)
    if entity_urn is not None:
        return entity_urn
    absolute_url = _absolute_provider_url(target, provider_site)
    if absolute_url is not None:
        return f"urn:url:{absolute_url}"
    return target


def _native_zulip_link(
    match: re.Match[str],
    resolver: ZulipLinkResolver | None,
) -> tuple[str, bool]:
    reference = match.group("reference")
    if resolver is None:
        return match.group(0), True
    if ">" not in reference:
        urn = resolver.channel_urn(reference)
        if urn is None:
            return match.group(0), True
        return f"[#{reference}]({urn})", False

    channel_name, topic_reference = reference.split(">", 1)
    message_match = re.fullmatch(
        r"(?P<topic>.*)@(?P<message_id>[0-9]+)", topic_reference
    )
    if message_match is not None:
        urn = resolver.provider_urn("message", message_match.group("message_id"))
        if urn is None:
            return match.group(0), True
        topic_name = message_match.group("topic")
        return f"[#{channel_name} > {topic_name} @ 💬]({urn})", False

    urn = resolver.topic_urn(channel_name, topic_reference)
    if urn is None:
        return match.group(0), True
    return f"[#{channel_name} > {topic_reference}]({urn})", False


def _trim_bare_url(value: str) -> tuple[str, str]:
    url = value
    suffix = ""
    while url and url[-1] in ".,;:!?":
        suffix = url[-1] + suffix
        url = url[:-1]
    while url.endswith(")") and url.count("(") < url.count(")"):
        suffix = ")" + suffix
        url = url[:-1]
    return url, suffix


def _semantic_reply_provider_id(content: str) -> str | None:
    for link in markdown_conversion.semantic_quote_links(content):
        target = urllib.parse.urlsplit(link.destination)
        if target.scheme and target.scheme.casefold() not in {"http", "https"}:
            continue
        match = REPLY_FRAGMENT_RE.fullmatch(target.fragment)
        if match is not None:
            return match.group("id")
    return None


def _convert_zulip_links(
    content: str,
    original_url: str,
    resolver: ZulipLinkResolver | None,
    *,
    mention_uuids: dict[str, str] | None = None,
    file_resolver: FileResolver | None = None,
) -> tuple[str, bool]:
    provider_site = _provider_site(original_url)
    lossy = False
    mention_uuids = mention_uuids or {}

    def transform_text(segment: str) -> str:
        nonlocal lossy
        converted: list[str] = []
        cursor = 0
        patterns = (
            ("mention", MENTION_RE),
            ("native", ZULIP_NATIVE_LINK_RE),
            ("angle", ANGLE_URL_RE),
            ("bare", BARE_URL_RE),
        )
        while cursor < len(segment):
            candidates: list[tuple[int, int, str, re.Match[str] | None]] = []
            for priority, (kind, pattern) in enumerate(patterns):
                match = pattern.search(segment, cursor)
                if match is not None:
                    candidates.append((match.start(), priority, kind, match))
            upload_index = segment.find("/user_uploads/", cursor)
            if upload_index >= 0:
                candidates.append((upload_index, len(patterns), "upload", None))
            if not candidates:
                converted.append(segment[cursor:])
                break

            start, _priority, kind, match = min(candidates)
            converted.append(segment[cursor:start])
            if kind == "upload":
                lossy = True
                converted.append(original_url + "#file-")
                cursor = start + len("/user_uploads/")
                continue
            if match is None:
                raise AssertionError("text token match is required")
            cursor = match.end()

            if kind == "mention":
                provider_user_id = (
                    match.group("user_id") or match.group("user_id_only")
                )
                name = (
                    match.group("name_with_id")
                    or match.group("name_only")
                    or provider_user_id
                    or "User"
                )
                user_uuid = (
                    mention_uuids.get(f"id:{provider_user_id}")
                    if provider_user_id is not None
                    else None
                ) or mention_uuids.get(name)
                if user_uuid is None:
                    lossy = True
                    converted.append(f"@{name}")
                else:
                    converted.append(f"[{name}](urn:user:{user_uuid})")
                continue
            if kind == "native":
                replacement, replacement_lossy = _native_zulip_link(
                    match, resolver
                )
                lossy = lossy or replacement_lossy
                converted.append(replacement)
                continue
            if kind == "angle":
                url = match.group("url")
                target = _workspace_link_target(url, provider_site, resolver)
                converted.append(f"[{url}]({target})")
                continue
            if kind == "bare":
                raw_url, suffix = _trim_bare_url(match.group("url"))
                target = (
                    raw_url
                    if raw_url.startswith(("http://", "https://"))
                    else f"https://{raw_url}"
                )
                workspace_target = _workspace_link_target(
                    target, provider_site, resolver
                )
                converted.append(
                    f"[{raw_url}]({workspace_target}){suffix}"
                )
                continue
            raise AssertionError(f"unknown text token kind: {kind}")
        return "".join(converted)

    def transform_link(link: markdown_conversion.MarkdownLink) -> str:
        nonlocal lossy
        if link.destination.startswith("/user_uploads/"):
            if file_resolver is None:
                lossy = True
                return link.with_destination(original_url)
            return link.with_destination(
                file_resolver(link.destination, link.label)
            )
        target = _workspace_link_target(
            link.destination,
            provider_site,
            resolver,
        )
        return (
            link.raw
            if target == link.destination
            else link.with_destination(target)
        )

    return (
        markdown_conversion.transform_markdown(
            content,
            text_transform=transform_text,
            link_transform=transform_link,
            convert_semantic_quotes=True,
        ),
        lossy,
    )


def _provider(
    chat_key: str, entity_id: str | None, revision: str | None = None
) -> dict[str, object]:
    return {
        "kind": "zulip",
        "chat_id": chat_key,
        "entity_id": entity_id,
        "revision": revision,
    }


def _reaction_provider_id(
    provider_message_id: object,
    provider_user_id: object,
    emoji_name: object,
) -> str:
    return ":".join(
        (
            str(int(str(provider_message_id))),
            str(int(str(provider_user_id))),
            str(emoji_name),
        )
    )


def _reaction_operations(
    store: ConversionStore,
    account_uuid: str,
    owner_uuid: str,
    project_uuid: str,
    stream_uuid: str,
    topic_uuid: str,
    chat_key: str,
    provider_message_id: object,
    message_uuid: str,
    reaction: dict[str, object],
    kind: str,
    occurred_at: str,
) -> list[dict[str, object]]:
    provider_user_id = str(int(str(reaction["user_id"])))
    emoji_name = str(reaction["emoji_name"])
    emoji_code = str(reaction["emoji_code"])
    reaction_type = str(reaction["reaction_type"])
    identity = store.provider_mapping(
        account_uuid,
        "identity",
        provider_user_id,
    )
    operations: list[dict[str, object]] = []
    if identity is None:
        identity_uuid = stable_entity_uuid(
            account_uuid,
            "identity",
            provider_user_id,
        )
        identity_metadata = {
            "display_name": f"Zulip user {provider_user_id}",
            "email": None,
            "avatar_urn": None,
            "active": True,
        }
        store.remember_provider_mapping(
            account_uuid,
            "identity",
            provider_user_id,
            identity_uuid,
            identity_metadata,
        )
        operations.append(
            {
                "kind": "identity.upsert",
                "entity_uuid": identity_uuid,
                "actor_uuid": owner_uuid,
                "occurred_at": occurred_at,
                "provider": _provider(chat_key, provider_user_id),
                "payload": identity_metadata,
                "extensions": {"provider_badge": "zulip"},
            }
        )
    else:
        identity_uuid = str(identity["workspace_uuid"])
    provider_reaction_id = _reaction_provider_id(
        provider_message_id,
        provider_user_id,
        emoji_name,
    )
    existing = store.provider_mapping(
        account_uuid,
        "reaction",
        provider_reaction_id,
    )
    reaction_uuid = (
        str(existing["workspace_uuid"])
        if existing is not None
        else stable_entity_uuid(account_uuid, "reaction", provider_reaction_id)
    )
    reaction_metadata = {
        "project_uuid": project_uuid,
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "message_uuid": message_uuid,
        "user_uuid": identity_uuid,
        "chat_key": chat_key,
        "emoji_name": emoji_name,
        "emoji_code": emoji_code,
        "reaction_type": reaction_type,
    }
    store.remember_provider_mapping(
        account_uuid,
        "reaction",
        provider_reaction_id,
        reaction_uuid,
        reaction_metadata,
    )
    operations.append(
        {
            "kind": kind,
            "entity_uuid": reaction_uuid,
            "actor_uuid": identity_uuid,
            "occurred_at": occurred_at,
            "provider": _provider(chat_key, provider_reaction_id),
            "payload": {
                "stream_uuid": stream_uuid,
                "topic_uuid": topic_uuid,
                "message_uuid": message_uuid,
                "user_uuid": identity_uuid,
                "emoji_name": emoji_name,
            },
            "extensions": {
                "provider_badge": "zulip",
                "emoji_code": emoji_code,
                "reaction_type": reaction_type,
            },
        }
    )
    return operations


def _identity_operations(
    store: ConversionStore,
    account_uuid: str,
    owner_uuid: str,
    message: dict[str, object],
    chat_key: str,
    occurred_at: str,
) -> tuple[
    list[dict[str, object]],
    dict[int, str],
    dict[str, str],
    dict[int, dict[str, object]],
]:
    recipients = (
        typing.cast(list[dict[str, object]], message["display_recipient"])
        if message["type"] != "stream"
        else []
    )
    provider_users: dict[int, dict[str, object]] = {
        int(user["id"]): user for user in recipients
    }
    sender_id = int(message["sender_id"])
    provider_users.setdefault(
        sender_id,
        {
            "id": sender_id,
            "full_name": message["sender_full_name"],
            "email": message.get("sender_email"),
            "avatar_url": message.get("avatar_url"),
            "is_me": bool(message.get("is_me_message", False)),
        },
    )
    for match in MENTION_RE.finditer(str(message.get("content", ""))):
        provider_user_id_raw = match.group("user_id") or match.group("user_id_only")
        if provider_user_id_raw is None:
            continue
        provider_user_id = int(provider_user_id_raw)
        provider_users.setdefault(
            provider_user_id,
            {
                "id": provider_user_id,
                "full_name": match.group("name_with_id") or provider_user_id_raw,
                "email": None,
                "avatar_url": None,
                "is_me": False,
            },
        )
    identities: dict[int, str] = {}
    mentions: dict[str, str] = {}
    operations: list[dict[str, object]] = []
    for provider_user_id, user in sorted(provider_users.items()):
        is_owner = bool(user.get("is_me")) or (
            provider_user_id == sender_id and bool(message.get("is_me_message"))
        )
        existing = store.provider_mapping(
            account_uuid, "identity", str(provider_user_id)
        )
        if is_owner:
            identity_uuid = owner_uuid
        elif existing is None:
            identity_uuid = stable_entity_uuid(
                account_uuid,
                "identity",
                str(provider_user_id),
            )
        else:
            identity_uuid = str(existing["workspace_uuid"])
        identities[provider_user_id] = identity_uuid
        display_name = str(user.get("full_name", message.get("sender_full_name", "")))
        mentions[display_name] = identity_uuid
        mentions[f"id:{provider_user_id}"] = identity_uuid
        if identity_uuid == owner_uuid or existing is not None:
            continue
        operations.append(
            {
                "kind": "identity.upsert",
                "entity_uuid": identity_uuid,
                "actor_uuid": owner_uuid,
                "occurred_at": occurred_at,
                "provider": _provider(chat_key, str(provider_user_id)),
                "payload": {
                    "display_name": display_name,
                    "email": user.get("email"),
                    "avatar_urn": None,
                    "active": True,
                },
                "extensions": {
                    "provider_badge": "zulip",
                    "provider_avatar_url": user.get("avatar_url"),
                },
            }
        )
    return operations, identities, mentions, provider_users


def _update_mention_operations(
    store: ConversionStore,
    account_uuid: str,
    owner_uuid: str,
    chat_key: str,
    content: str,
    occurred_at: str,
) -> tuple[list[dict[str, object]], dict[str, str]]:
    operations: list[dict[str, object]] = []
    mention_uuids: dict[str, str] = {}
    seen: set[str] = set()
    for match in MENTION_RE.finditer(content):
        provider_user_id = match.group("user_id") or match.group("user_id_only")
        if provider_user_id is None or provider_user_id in seen:
            continue
        seen.add(provider_user_id)
        display_name = match.group("name_with_id") or provider_user_id
        mapping = store.provider_mapping(account_uuid, "identity", provider_user_id)
        identity_uuid = (
            stable_entity_uuid(account_uuid, "identity", provider_user_id)
            if mapping is None
            else str(mapping["workspace_uuid"])
        )
        mention_uuids[f"id:{provider_user_id}"] = identity_uuid
        mention_uuids[display_name] = identity_uuid
        metadata = {
            "display_name": display_name,
            "email": None,
            "avatar_urn": None,
            "active": True,
        }
        if mapping is not None:
            metadata = {
                **metadata,
                **typing.cast(dict[str, object], mapping.get("metadata", {})),
            }
        store.remember_provider_mapping(
            account_uuid,
            "identity",
            provider_user_id,
            identity_uuid,
            metadata,
        )
        if identity_uuid == owner_uuid:
            continue
        operations.append(
            {
                "kind": "identity.upsert",
                "entity_uuid": identity_uuid,
                "actor_uuid": owner_uuid,
                "occurred_at": occurred_at,
                "provider": _provider(chat_key, provider_user_id),
                "payload": metadata,
                "extensions": {"provider_badge": "zulip"},
            }
        )
    return operations, mention_uuids


def _message_context(
    store: ConversionStore,
    account_uuid: str,
    message: dict[str, object],
) -> tuple[str, str, str, str, str]:
    chat_type, chat_key = provider_chat_reference(message)
    project_uuid, _assignment_exists = provider_chat_assignment(
        store, account_uuid, chat_key
    )
    stream_mapping = store.provider_mapping(account_uuid, "stream", chat_key)
    if stream_mapping is None and _reconcile_assignment_projection(
        store, account_uuid, chat_key
    ):
        stream_mapping = store.provider_mapping(account_uuid, "stream", chat_key)
    if stream_mapping is None:
        raise ValueError("provider_chat_assignment_pending")
    stream_uuid = str(stream_mapping["workspace_uuid"])
    subject = str(message["subject"])
    topic_provider_id = (
        channel_topic_provider_id(message["stream_id"], subject)
        if chat_type == "channel"
        else f"{chat_key}:default"
    )
    stream_metadata = typing.cast(
        dict[str, object], stream_mapping.get("metadata", {})
    )
    if chat_type == "channel" and is_empty_channel_topic(subject):
        default_topic_uuid = stream_metadata.get("default_topic_uuid")
        if default_topic_uuid is None:
            raise ValueError("provider_chat_assignment_pending")
        return (
            chat_type,
            chat_key,
            project_uuid,
            stream_uuid,
            str(default_topic_uuid),
        )
    topic_mapping = store.provider_mapping(account_uuid, "topic", topic_provider_id)
    if topic_mapping is None and _reconcile_assignment_projection(
        store, account_uuid, chat_key
    ):
        topic_mapping = store.provider_mapping(
            account_uuid, "topic", topic_provider_id
        )
    if topic_mapping is None:
        raise ValueError("provider_chat_assignment_pending")
    topic_uuid = str(topic_mapping["workspace_uuid"])
    return chat_type, chat_key, project_uuid, stream_uuid, topic_uuid


def message_event_records(
    store: ConversionStore,
    account_uuid: str,
    queue_id: str,
    event: dict[str, object],
    delivery_class: str = "live",
    original_url: str = "",
    file_resolver: FileResolver | None = None,
) -> list[dict[str, object]]:
    if event.get("local_message_id") is not None:
        return []
    message = typing.cast(dict[str, object], event["message"])
    account = store.account_resource(account_uuid)
    if account is None:
        raise ValueError("unknown_external_account")
    owner_uuid = str(account["owner_user_uuid"])
    provider_message_id = str(message["id"])
    existing_message = store.provider_mapping(
        account_uuid, "message", provider_message_id
    )
    existing_message_metadata = (
        typing.cast(dict[str, object], existing_message.get("metadata", {}))
        if existing_message is not None
        else {}
    )
    event_chat_type, event_chat_key = provider_chat_reference(message)
    stored_provider_event_id = existing_message_metadata.get("provider_event_id")
    accepted_context_lookup = getattr(
        store, "accepted_provider_message_context", None
    )
    accepted_context = (
        accepted_context_lookup(account_uuid, queue_id, int(event["id"]))
        if callable(accepted_context_lookup)
        else None
    )
    replay_context = (
        accepted_context
        if accepted_context is not None
        else existing_message_metadata
    )
    same_live_event_replay = (
        delivery_class == "live"
        and existing_message is not None
        and (
            accepted_context is not None
            or existing_message_metadata.get("mapping_origin") == "zulip"
        )
        and replay_context.get("chat_key") == event_chat_key
        and (
            accepted_context is not None
            or stored_provider_event_id is None
            or int(stored_provider_event_id) == int(event["id"])
        )
        and all(
            replay_context.get(name) is not None
            for name in ("project_uuid", "stream_uuid", "topic_uuid", "chat_key")
        )
    )
    if same_live_event_replay and accepted_context is not None:
        accepted_records = _accepted_live_replay_records(
            account_uuid,
            event,
            accepted_context,
        )
        if accepted_records is not None:
            # An exact live-event replay is an immutable journal replay, not a
            # fresh conversion. Reuse the complete accepted sequence so a
            # newly remembered identity or another mutable mapping cannot
            # remove an operation and shift every later deterministic UUID.
            return accepted_records
    if same_live_event_replay:
        chat_type = event_chat_type
        chat_key = event_chat_key
        project_uuid = str(replay_context["project_uuid"])
        stream_uuid = str(replay_context["stream_uuid"])
        topic_uuid = str(replay_context["topic_uuid"])
    else:
        (
            chat_type,
            chat_key,
            project_uuid,
            stream_uuid,
            topic_uuid,
        ) = _message_context(store, account_uuid, message)
    message_uuid = (
        str(replay_context.get("message_uuid", existing_message["workspace_uuid"]))
        if existing_message is not None
        else stable_entity_uuid(account_uuid, "message", provider_message_id)
    )
    workspace_delivery_committed = existing_message is not None and (
        existing_message.get("convergent_alias") is True
        or existing_message_metadata.get("mapping_origin") == "workspace"
        or existing_message_metadata.get("workspace_delivery_state") == "committed"
    )
    occurred_at_dt = datetime.datetime.fromtimestamp(
        float(message["timestamp"]), datetime.UTC
    )
    occurred_at = occurred_at_dt.isoformat().replace("+00:00", "Z")
    lane = f"chat:{account_uuid}:{stream_uuid}"
    (
        identity_operations,
        identity_uuids,
        mention_uuids,
        provider_users,
    ) = _identity_operations(
        store, account_uuid, owner_uuid, message, chat_key, occurred_at
    )
    recipients = (
        typing.cast(list[dict[str, object]], message["display_recipient"])
        if chat_type != "channel"
        else []
    )
    sender_id = int(message["sender_id"])
    author_uuid = identity_uuids.get(sender_id)
    if author_uuid is None:
        raise ValueError("provider_chat_assignment_pending")
    if same_live_event_replay and replay_context.get("author_uuid") is not None:
        author_uuid = str(replay_context["author_uuid"])
    existing_stream = store.provider_mapping(account_uuid, "stream", chat_key)
    existing_stream_metadata = (
        typing.cast(dict[str, object], existing_stream["metadata"])
        if existing_stream is not None
        else {}
    )
    existing_participants = (
        typing.cast(
            list[str],
            existing_stream_metadata.get("participants", []),
        )
        if existing_stream is not None
        else []
    )
    participants = sorted(existing_participants)
    expected_participants = {owner_uuid}
    if chat_type != "channel":
        expected_participants.update(
            {
                author_uuid,
                *(identity_uuids[int(user["id"])] for user in recipients),
            }
        )
    if not expected_participants.issubset(participants):
        raise ValueError("provider_chat_assignment_pending")
    if chat_type == "direct" and len(participants) != 2:
        raise ValueError("invalid_personal_dm_membership")
    stream_name = str(
        existing_stream_metadata.get(
            "name",
            message["display_recipient"]
            if chat_type == "channel"
            else message.get("recipient_display_name", "Direct message"),
        )
    )
    stream_description = str(existing_stream_metadata.get("description", ""))
    stream_private = bool(existing_stream_metadata.get("private", True))
    default_topic_uuid = existing_stream_metadata.get("default_topic_uuid")
    if chat_type != "channel" and default_topic_uuid is None:
        default_topic_uuid = topic_uuid
    provider_site = original_url.rstrip("/")
    message_url = (
        f"{provider_site}/#narrow/near/{provider_message_id}"
        if provider_site
        else f"#narrow/near/{provider_message_id}"
    )
    markdown, lossy = convert_markdown(
        str(message["content"]),
        mention_uuids,
        message_url,
        file_resolver,
        ZulipLinkResolver(store, account_uuid, owner_uuid),
    )
    reply_provider_id = _semantic_reply_provider_id(str(message["content"]))
    reply_mapping = (
        store.provider_mapping(account_uuid, "message", reply_provider_id)
        if reply_provider_id is not None
        else None
    )
    reply_to_message_uuid = (
        str(reply_mapping["workspace_uuid"]) if reply_mapping is not None else None
    )
    content_sha256 = hashlib.sha256(markdown.encode("utf-8")).hexdigest()
    provider_content_sha256 = hashlib.sha256(
        str(message["content"]).encode("utf-8")
    ).hexdigest()
    channel_subject = channel_topic_name(str(message.get("subject", "")))
    topic_provider_id = (
        channel_topic_provider_id(message["stream_id"], channel_subject)
        if chat_type == "channel"
        else f"{chat_key}:default"
    )
    flags = message.get("flags", event.get("flags"))
    message_payload = {
        "stream_uuid": stream_uuid,
        "topic_uuid": topic_uuid,
        "author_uuid": author_uuid,
        "payload": {"kind": "markdown", "content": markdown},
        "reply_to_message_uuid": reply_to_message_uuid,
    }
    if isinstance(flags, list):
        # History catch-up can race ahead of an earlier live read-state event.
        # Carry the snapshot value on the message projection as the convergent
        # source of truth instead of relying only on a separate flag operation.
        message_payload["read"] = "read" in flags
    message_operation = {
        "kind": (
            "message.update"
            if delivery_class == "backfill" and workspace_delivery_committed
            else "message.create"
        ),
        "entity_uuid": message_uuid,
        "actor_uuid": author_uuid,
        "occurred_at": occurred_at,
        "provider": _provider(chat_key, provider_message_id),
        "payload": message_payload,
        "extensions": {
            "provider_badge": "zulip",
            "provider_original_url": message_url,
            "lossy_conversion": lossy,
            "unresolved_reply_provider_id": (
                reply_provider_id
                if reply_provider_id is not None and reply_mapping is None
                else None
            ),
        },
    }
    accepted_message_operation = replay_context.get("message_operation")
    if same_live_event_replay and isinstance(accepted_message_operation, dict):
        # Exact live-event replay must reuse the accepted rendering as well as
        # its structural UUIDs. Native Zulip links resolve through mutable
        # topic mappings, so rendering them again after recanonicalization can
        # otherwise change the digest of the same deterministic operation UUID.
        message_operation = copy.deepcopy(accepted_message_operation)
    # Control-plane assignments materialize the stream projection. Provider
    # messages still need a topic upsert because the backend-owned topic UUID
    # mapping can precede materialization of that topic in Messenger storage.
    operations = [
        *identity_operations,
        {
            "kind": "topic.upsert",
            "entity_uuid": topic_uuid,
            "actor_uuid": owner_uuid,
            "occurred_at": occurred_at,
            "provider": _provider(chat_key, topic_provider_id),
            "payload": {
                "stream_uuid": stream_uuid,
                "name": (
                    channel_subject
                    if chat_type == "channel"
                    else ZULIP_DIRECT_TOPIC_NAME
                ),
            },
            "extensions": {"provider_badge": "zulip"},
        },
    ]
    if (
        not workspace_delivery_committed
        or delivery_class == "backfill"
        or same_live_event_replay
    ):
        operations.append(message_operation)
    if isinstance(flags, list) and delivery_class != "backfill":
        operations.append(
            {
                "kind": "read_state.set",
                "entity_uuid": stream_uuid,
                "actor_uuid": owner_uuid,
                "occurred_at": occurred_at,
                "provider": _provider(chat_key, None),
                "payload": {
                    "stream_uuid": stream_uuid,
                    "topic_uuid": topic_uuid,
                    "reader_uuid": owner_uuid,
                    "message_uuids": [message_uuid],
                    "read": "read" in flags,
                },
                "extensions": {"provider_badge": "zulip"},
            }
        )
    store.remember_provider_mapping(
        account_uuid,
        "stream",
        chat_key,
        stream_uuid,
        {
            "chat_type": chat_type,
            "project_uuid": project_uuid,
            "participants": participants,
            "name": stream_name,
            "description": stream_description,
            "private": stream_private,
            "default_topic_uuid": default_topic_uuid,
        },
    )
    store.remember_provider_mapping(
        account_uuid,
        "topic",
        topic_provider_id,
        topic_uuid,
        {"stream_uuid": stream_uuid, "chat_key": chat_key},
    )
    store.remember_provider_mapping(
        account_uuid,
        "message",
        provider_message_id,
        message_uuid,
        {
            **existing_message_metadata,
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
            "chat_key": chat_key,
            "project_uuid": project_uuid,
            "provider_timestamp": float(message["timestamp"]),
            "content_sha256": content_sha256,
            "provider_content_sha256": provider_content_sha256,
            "subject": channel_subject if chat_type == "channel" else "",
            "mapping_origin": existing_message_metadata.get("mapping_origin", "zulip"),
            "provider_event_id": existing_message_metadata.get(
                "provider_event_id", int(event["id"])
            ),
            "workspace_delivery_state": (
                "committed"
                if workspace_delivery_committed
                else existing_message_metadata.get(
                    "workspace_delivery_state", "pending"
                )
            ),
        },
    )
    for provider_user_id, identity_uuid in identity_uuids.items():
        user = provider_users[provider_user_id]
        payload = {
            "display_name": str(user.get("full_name", provider_user_id)),
            "email": user.get("email"),
            "avatar_urn": None,
            "active": True,
        }
        store.remember_provider_mapping(
            account_uuid,
            "identity",
            str(provider_user_id),
            identity_uuid,
            payload,
        )
    reactions = message.get("reactions")
    if isinstance(reactions, list):
        for reaction in reactions:
            if not isinstance(reaction, dict):
                raise ValueError("Provider message reaction is invalid")
            operations.extend(
                _reaction_operations(
                    store,
                    account_uuid,
                    owner_uuid,
                    project_uuid,
                    stream_uuid,
                    topic_uuid,
                    chat_key,
                    provider_message_id,
                    message_uuid,
                    reaction,
                    "reaction.upsert",
                    occurred_at,
                )
            )
    record_source = f"provider-message:{provider_message_id}"
    if delivery_class == "live":
        # A live project move changes the target and therefore the operation
        # digests. Scope these deterministic UUIDs to that target so a
        # retained submitted prefix from the old project cannot collide with
        # the journal replay required for the new project. Backfill keeps its
        # established source to preserve upgrade-time page idempotency.
        record_source = f"{record_source}:project:{project_uuid}"
    elif delivery_class == "backfill":
        record_source = (
            f"{record_source}:reconcile-generation:{int(account['generation'])}"
        )
    records = []
    for index, operation in enumerate(operations):
        operation_lane = (
            f"identity:{account_uuid}:{operation['entity_uuid']}"
            if operation["kind"] == "identity.upsert"
            else lane
        )
        records.append(
            _record(
                store,
                account_uuid,
                project_uuid,
                record_source,
                int(event["id"]),
                index,
                operation,
                operation_lane,
                occurred_at_dt,
                delivery_class,
            )
        )
    return records


def _reaction_event_records(
    store: ConversionStore,
    account_uuid: str,
    queue_id: str,
    event: dict[str, object],
    delivery_class: str,
) -> list[dict[str, object]]:
    account = store.account_resource(account_uuid)
    if account is None:
        return []
    message = store.provider_mapping(
        account_uuid,
        "message",
        str(event["message_id"]),
    )
    if message is None:
        raise ValueError("provider_chat_assignment_pending")
    metadata = typing.cast(dict[str, object], message["metadata"])
    event_time = _event_time(event)
    reaction_op = str(event["op"])
    if reaction_op not in {"add", "remove"}:
        raise ValueError("Provider reaction operation is invalid")
    operations = _reaction_operations(
        store,
        account_uuid,
        str(account["owner_user_uuid"]),
        str(metadata["project_uuid"]),
        str(metadata["stream_uuid"]),
        str(metadata["topic_uuid"]),
        str(metadata["chat_key"]),
        event["message_id"],
        str(message["workspace_uuid"]),
        event,
        "reaction.upsert" if reaction_op == "add" else "reaction.delete",
        event_time.isoformat().replace("+00:00", "Z"),
    )
    return [
        _record(
            store,
            account_uuid,
            str(metadata["project_uuid"]),
            queue_id,
            int(event["id"]),
            index,
            operation,
            (
                f"identity:{account_uuid}:{operation['entity_uuid']}"
                if operation["kind"] == "identity.upsert"
                else f"chat:{account_uuid}:{metadata['stream_uuid']}"
            ),
            event_time,
            delivery_class,
        )
        for index, operation in enumerate(operations)
    ]


def _mapped_event_records(
    store: ConversionStore,
    account_uuid: str,
    queue_id: str,
    event: dict[str, object],
    delivery_class: str,
    original_url: str,
    file_resolver: FileResolver | None,
) -> list[dict[str, object]]:
    event_type = str(event["type"])
    event_time = _event_time(event)
    account = store.account_resource(account_uuid)
    if account is None:
        return []
    owner_uuid = str(account["owner_user_uuid"])
    message_ids = event.get("message_ids")
    if message_ids is None and event_type == "update_message_flags":
        message_ids = event.get("messages")
    if message_ids is None and event.get("message_id") is not None:
        message_ids = [event["message_id"]]
    records: list[dict[str, object]] = []
    next_subindex = 0
    if (
        event_type == "update_message"
        and event.get("orig_subject") is not None
        and event.get("subject") is not None
        and event.get("stream_id") is not None
        and channel_topic_name(str(event["orig_subject"]))
        != channel_topic_name(str(event["subject"]))
    ):
        old_topic_name = channel_topic_name(str(event["orig_subject"]))
        new_topic_name = channel_topic_name(str(event["subject"]))
        old_provider_id = channel_topic_provider_id(
            event["stream_id"], old_topic_name
        )
        old_topic = store.provider_mapping(account_uuid, "topic", old_provider_id)
        if old_topic is not None:
            topic_metadata = typing.cast(dict[str, object], old_topic["metadata"])
            new_provider_id = channel_topic_provider_id(
                event["stream_id"], new_topic_name
            )
            renamed = store.rename_provider_mapping(
                account_uuid,
                "topic",
                old_provider_id,
                new_provider_id,
                topic_metadata,
                str(event.get("edit_timestamp")),
            )
            if renamed is not None:
                stream_uuid = str(topic_metadata["stream_uuid"])
                stream_mapping = store.provider_mapping(
                    account_uuid, "stream", str(topic_metadata["chat_key"])
                )
                if stream_mapping is not None:
                    stream_metadata = typing.cast(
                        dict[str, object], stream_mapping["metadata"]
                    )
                    project_uuid = str(stream_metadata["project_uuid"])
                    operation = {
                        "kind": "topic.upsert",
                        "entity_uuid": str(renamed["workspace_uuid"]),
                        "actor_uuid": owner_uuid,
                        "occurred_at": event_time.isoformat().replace("+00:00", "Z"),
                        "provider": _provider(
                            str(topic_metadata["chat_key"]),
                            new_provider_id,
                            str(event.get("edit_timestamp")),
                        ),
                        "payload": {
                            "stream_uuid": stream_uuid,
                            "name": new_topic_name,
                        },
                        "extensions": {"provider_badge": "zulip"},
                    }
                    records.append(
                        _record(
                            store,
                            account_uuid,
                            project_uuid,
                            queue_id,
                            int(event["id"]),
                            next_subindex,
                            operation,
                            f"chat:{account_uuid}:{stream_uuid}",
                            event_time,
                            delivery_class,
                        )
                    )
                    next_subindex += 1
    if event_type == "update_message_flags" and event.get("flag") == "read":
        grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
        for provider_message_id_raw in typing.cast(list[object], message_ids or []):
            mapping = store.provider_mapping(
                account_uuid, "message", str(provider_message_id_raw)
            )
            if mapping is None:
                continue
            metadata = typing.cast(dict[str, object], mapping["metadata"])
            chat_key = str(metadata["chat_key"])
            assignment = store.assignment_for_provider_chat(
                account_uuid, chat_key
            )
            stream_mapping = store.provider_mapping(
                account_uuid, "stream", chat_key
            )
            if stream_mapping is None and _reconcile_assignment_projection(
                store, account_uuid, chat_key
            ):
                stream_mapping = store.provider_mapping(
                    account_uuid, "stream", chat_key
                )
            if (
                assignment is None
                or stream_mapping is None
                or str(metadata["project_uuid"])
                != str(assignment["project_id"])
                or str(metadata["stream_uuid"])
                != str(stream_mapping["workspace_uuid"])
            ):
                # Recanonicalization can leave historical message mappings
                # pointing at a retired stream. The message snapshot remains
                # the convergent read-state source; do not submit a permanently
                # invalid read event against the retired projection.
                continue
            key = (
                str(metadata["project_uuid"]),
                str(metadata["stream_uuid"]),
                str(metadata["topic_uuid"]),
                chat_key,
            )
            grouped.setdefault(key, []).append(mapping)
        for (project_uuid, stream_uuid, topic_uuid, chat_key), mappings in sorted(
            grouped.items()
        ):
            message_uuids = sorted(
                str(mapping["workspace_uuid"]) for mapping in mappings
            )
            operation = {
                "kind": "read_state.set",
                "entity_uuid": stream_uuid,
                "actor_uuid": owner_uuid,
                "occurred_at": event_time.isoformat().replace("+00:00", "Z"),
                "provider": _provider(chat_key, None),
                "payload": {
                    "stream_uuid": stream_uuid,
                    "topic_uuid": topic_uuid,
                    "reader_uuid": owner_uuid,
                    "message_uuids": message_uuids,
                    "read": event.get("op") == "add",
                },
                "extensions": {"provider_badge": "zulip"},
            }
            records.append(
                _record(
                    store,
                    account_uuid,
                    project_uuid,
                    queue_id,
                    int(event["id"]),
                    next_subindex,
                    operation,
                    f"chat:{account_uuid}:{stream_uuid}",
                    event_time,
                    delivery_class,
                )
            )
            next_subindex += 1
        return records
    for provider_message_id_raw in typing.cast(list[object], message_ids or []):
        provider_message_id = str(provider_message_id_raw)
        mapping = store.provider_mapping(account_uuid, "message", provider_message_id)
        if mapping is None:
            continue
        metadata = typing.cast(dict[str, object], mapping["metadata"])
        project_uuid = str(metadata["project_uuid"])
        stream_uuid = str(metadata["stream_uuid"])
        topic_uuid = str(metadata["topic_uuid"])
        author_uuid = str(metadata["author_uuid"])
        chat_key = str(metadata["chat_key"])
        operation: dict[str, object]
        record_source = queue_id
        if event_type == "delete_message":
            record_source = f"provider-message-delete:{provider_message_id}"
            operation = {
                "kind": "message.delete",
                "entity_uuid": str(mapping["workspace_uuid"]),
                "actor_uuid": author_uuid,
                "occurred_at": event_time.isoformat().replace("+00:00", "Z"),
                "provider": _provider(chat_key, provider_message_id),
                "payload": {
                    "stream_uuid": stream_uuid,
                    "topic_uuid": topic_uuid,
                    "author_uuid": author_uuid,
                },
                "extensions": {"provider_badge": "zulip"},
            }
        elif event_type == "update_message" and event.get("content") is not None:
            provider_content_sha256 = hashlib.sha256(
                str(event["content"]).encode("utf-8")
            ).hexdigest()
            record_source = (
                f"provider-message-update:{provider_message_id}:"
                f"{provider_content_sha256}"
            )
            message_url = (
                f"{original_url.rstrip('/')}/#narrow/near/{provider_message_id}"
            )
            occurred_at = event_time.isoformat().replace("+00:00", "Z")
            mention_operations, mention_uuids = _update_mention_operations(
                store,
                account_uuid,
                owner_uuid,
                chat_key,
                str(event["content"]),
                occurred_at,
            )
            for mention_operation in mention_operations:
                records.append(
                    _record(
                        store,
                        account_uuid,
                        project_uuid,
                        record_source,
                        int(event["id"]),
                        next_subindex,
                        mention_operation,
                        f"identity:{account_uuid}:{mention_operation['entity_uuid']}",
                        event_time,
                        delivery_class,
                    )
                )
                next_subindex += 1
            markdown, lossy = convert_markdown(
                str(event["content"]),
                mention_uuids,
                message_url,
                file_resolver,
                ZulipLinkResolver(store, account_uuid, owner_uuid),
            )
            topic_name = str(event.get("subject", metadata.get("subject", "")))
            if chat_key.startswith("channel:"):
                topic_name = channel_topic_name(topic_name)
            else:
                topic_name = ZULIP_DIRECT_TOPIC_NAME
            topic_provider_id = (
                channel_topic_provider_id(
                    chat_key.removeprefix("channel:"), topic_name
                )
                if chat_key.startswith("channel:")
                else f"{chat_key}:default"
            )
            if not any(
                record["operation"]["kind"] == "topic.upsert"
                and record["operation"]["entity_uuid"] == topic_uuid
                for record in records
            ):
                topic_operation = {
                    "kind": "topic.upsert",
                    "entity_uuid": topic_uuid,
                    "actor_uuid": owner_uuid,
                    "occurred_at": occurred_at,
                    "provider": _provider(
                        chat_key,
                        topic_provider_id,
                        str(event.get("edit_timestamp")),
                    ),
                    "payload": {
                        "stream_uuid": stream_uuid,
                        "name": topic_name,
                    },
                    "extensions": {"provider_badge": "zulip"},
                }
                records.append(
                    _record(
                        store,
                        account_uuid,
                        project_uuid,
                        record_source,
                        int(event["id"]),
                        next_subindex,
                        topic_operation,
                        f"chat:{account_uuid}:{stream_uuid}",
                        event_time,
                        delivery_class,
                    )
                )
                next_subindex += 1
            operation = {
                "kind": "message.update",
                "entity_uuid": str(mapping["workspace_uuid"]),
                "actor_uuid": author_uuid,
                "occurred_at": event_time.isoformat().replace("+00:00", "Z"),
                "provider": _provider(
                    chat_key, provider_message_id, str(event.get("edit_timestamp"))
                ),
                "payload": {
                    "stream_uuid": stream_uuid,
                    "topic_uuid": topic_uuid,
                    "author_uuid": author_uuid,
                    "payload": {"kind": "markdown", "content": markdown},
                },
                "extensions": {
                    "provider_badge": "zulip",
                    "provider_original_url": message_url,
                    "lossy_conversion": lossy,
                    "content_sha256": hashlib.sha256(
                        markdown.encode("utf-8")
                    ).hexdigest(),
                    "provider_content_sha256": provider_content_sha256,
                    "subject": (topic_name if chat_key.startswith("channel:") else ""),
                },
            }
        else:
            continue
        records.append(
            _record(
                store,
                account_uuid,
                project_uuid,
                record_source,
                int(event["id"]),
                next_subindex,
                operation,
                f"chat:{account_uuid}:{stream_uuid}",
                event_time,
                delivery_class,
            )
        )
        next_subindex += 1
    return records


def _event_time(event: dict[str, object]) -> datetime.datetime:
    supplied = event.get("edit_timestamp", event.get("timestamp"))
    if supplied is not None:
        return datetime.datetime.fromtimestamp(float(supplied), datetime.UTC)
    return datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC) + datetime.timedelta(
        microseconds=int(event["id"])
    )


def _subscription_records(
    store: ConversionStore,
    account_uuid: str,
    queue_id: str,
    event: dict[str, object],
    delivery_class: str,
) -> list[dict[str, object]]:
    account = store.account_resource(account_uuid)
    if account is None:
        return []
    owner_uuid = str(account["owner_user_uuid"])
    event_time = _event_time(event)
    subscriptions: list[dict[str, object]] = []
    if event.get("op") in {"add", "remove"}:
        subscriptions = typing.cast(list[dict[str, object]], event["subscriptions"])
    elif event.get("op") == "update" and event.get("property") == "name":
        subscriptions = [{"stream_id": event["stream_id"], "name": event["value"]}]
    records: list[dict[str, object]] = []
    for index, subscription in enumerate(subscriptions):
        chat_key = f"channel:{int(subscription['stream_id'])}"
        try:
            project_uuid, assignment_exists = provider_chat_assignment(
                store, account_uuid, chat_key
            )
        except ValueError as exc:
            if str(exc) == "provider_chat_assignment_pending":
                raise
            continue
        mapping = store.provider_mapping(account_uuid, "stream", chat_key)
        if mapping is None:
            raise ValueError("provider_chat_assignment_pending")
        stream_uuid = str(mapping["workspace_uuid"])
        old_metadata = (
            typing.cast(dict[str, object], mapping["metadata"])
            if mapping is not None
            else {}
        )
        if event.get("op") == "remove":
            operation = {
                "kind": "stream.delete",
                "entity_uuid": stream_uuid,
                "actor_uuid": owner_uuid,
                "occurred_at": event_time.isoformat().replace("+00:00", "Z"),
                "provider": _provider(chat_key, chat_key),
                "payload": {"stream_uuid": stream_uuid},
                "extensions": {"provider_badge": "zulip"},
            }
        else:
            participants = typing.cast(list[str], old_metadata.get("participants", []))
            if not participants:
                raise ValueError("provider_chat_assignment_pending")
            stream_payload = {
                "name": subscription.get("name", old_metadata.get("name", "")),
                "description": subscription.get(
                    "description", old_metadata.get("description", "")
                ),
                "private": bool(old_metadata.get("private", True)),
                "chat_kind": "channel",
                "participant_uuids": participants,
                "default_topic_uuid": old_metadata.get("default_topic_uuid"),
            }
            operation = {
                "kind": "stream.upsert",
                "entity_uuid": stream_uuid,
                "actor_uuid": owner_uuid,
                "occurred_at": event_time.isoformat().replace("+00:00", "Z"),
                "provider": _provider(chat_key, chat_key),
                "payload": stream_payload,
                "extensions": {
                    "provider_badge": "zulip",
                    "assignment_materialized": assignment_exists,
                },
            }
            store.remember_provider_mapping(
                account_uuid,
                "stream",
                chat_key,
                stream_uuid,
                {
                    **stream_payload,
                    "chat_type": "channel",
                    "project_uuid": project_uuid,
                    "participants": participants,
                },
            )
        records.append(
            _record(
                store,
                account_uuid,
                project_uuid,
                queue_id,
                int(event["id"]),
                index,
                operation,
                f"chat:{account_uuid}:{stream_uuid}",
                event_time,
                delivery_class,
            )
        )
    return records


def _realm_user_records(
    store: ConversionStore,
    account_uuid: str,
    queue_id: str,
    event: dict[str, object],
    delivery_class: str,
) -> list[dict[str, object]]:
    account = store.account_resource(account_uuid)
    if account is None:
        return []
    owner_uuid = str(account["owner_user_uuid"])
    person = typing.cast(dict[str, object], event["person"])
    provider_user_id = str(person.get("user_id", person.get("id")))
    mapping = store.provider_mapping(account_uuid, "identity", provider_user_id)
    identity_uuid = (
        str(mapping["workspace_uuid"])
        if mapping is not None
        else stable_entity_uuid(account_uuid, "identity", provider_user_id)
    )
    previous = (
        typing.cast(dict[str, object], mapping["metadata"])
        if mapping is not None
        else {}
    )
    email = person.get("new_email", person.get("email", previous.get("email")))
    payload = {
        "display_name": person.get(
            "full_name", previous.get("display_name", provider_user_id)
        ),
        "email": email,
        "avatar_urn": previous.get("avatar_urn"),
        "active": False
        if event.get("op") == "remove"
        else bool(person.get("is_active", previous.get("active", True))),
    }
    store.remember_provider_mapping(
        account_uuid, "identity", provider_user_id, identity_uuid, payload
    )
    settings = typing.cast(dict[str, object], account["settings"])
    project_uuid = str(settings["default_project_id"])
    event_time = _event_time(event)
    operation = {
        "kind": "identity.upsert",
        "entity_uuid": identity_uuid,
        "actor_uuid": owner_uuid,
        "occurred_at": event_time.isoformat().replace("+00:00", "Z"),
        "provider": _provider("account", provider_user_id),
        "payload": payload,
        "extensions": {"provider_badge": "zulip"},
    }
    return [
        _record(
            store,
            account_uuid,
            project_uuid,
            queue_id,
            int(event["id"]),
            0,
            operation,
            f"identity:{account_uuid}:{identity_uuid}",
            event_time,
            delivery_class,
        )
    ]


def event_records(
    store: ConversionStore,
    account_uuid: str,
    queue_id: str,
    event: dict[str, object],
    delivery_class: str = "live",
    original_url: str = "",
    file_resolver: FileResolver | None = None,
) -> list[dict[str, object]]:
    event_type = str(event["type"])
    if event_type == "message":
        return message_event_records(
            store,
            account_uuid,
            queue_id,
            event,
            delivery_class,
            original_url,
            file_resolver,
        )
    if event_type in {"update_message", "delete_message", "update_message_flags"}:
        return _mapped_event_records(
            store,
            account_uuid,
            queue_id,
            event,
            delivery_class,
            original_url,
            file_resolver,
        )
    if event_type == "reaction":
        return _reaction_event_records(
            store,
            account_uuid,
            queue_id,
            event,
            delivery_class,
        )
    if event_type == "subscription":
        return _subscription_records(
            store, account_uuid, queue_id, event, delivery_class
        )
    if event_type == "realm_user":
        return _realm_user_records(store, account_uuid, queue_id, event, delivery_class)
    return []


def newest_first(messages: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        messages,
        key=lambda message: (float(message["timestamp"]), int(message["id"])),
        reverse=True,
    )
