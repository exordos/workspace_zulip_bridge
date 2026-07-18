import datetime
import hashlib
import re
import typing
import uuid

from workspace_zulip_bridge import canonical

OPERATION_NAMESPACE = uuid.UUID("9d8b6952-b2de-4c80-a9c7-9619aaf5f35d")
ENTITY_NAMESPACE = uuid.UUID("9a1d0e75-50a5-413c-b3e8-d070232ef57f")
MENTION_RE = re.compile(
    r"@_?\*\*(?:"
    r"(?P<name_with_id>[^*|]+)\|(?P<user_id>[0-9]+)"
    r"|\|(?P<user_id_only>[0-9]+)"
    r"|(?P<name_only>[^*]+)"
    r")\*\*"
)
UPLOAD_RE = re.compile(r"\[(?P<name>[^\]]+)\]\((?P<url>/user_uploads/[^)]+)\)")
QUOTE_RE = re.compile(r"~~~ quote\n(?P<body>.*?)\n~~~", re.DOTALL)
REPLY_LINK_RE = re.compile(
    r"\]\((?:https?://[^)]+)?/?#narrow/(?:[^)]+/)?near/(?P<id>[0-9]+)\)"
)


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


def _assignment(
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


def operation_uuid_for(
    account_uuid: str, queue_id: str, event_id: int, subindex: int
) -> str:
    return str(
        uuid.uuid5(
            OPERATION_NAMESPACE,
            f"{account_uuid}:{queue_id}:{event_id}:{subindex}",
        )
    )


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
) -> tuple[str, bool]:
    """Convert raw Zulip Markdown without leaking provider-only file URLs."""
    lossy = False

    def quote(match: re.Match[str]) -> str:
        return "\n".join(f"> {line}" for line in match.group("body").splitlines())

    converted = QUOTE_RE.sub(quote, content)

    def mention(match: re.Match[str]) -> str:
        nonlocal lossy
        provider_user_id = match.group("user_id") or match.group("user_id_only")
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
            return f"@{name}"
        return f"[{name}](urn:user:{user_uuid})"

    converted = MENTION_RE.sub(mention, converted)

    def upload(match: re.Match[str]) -> str:
        nonlocal lossy
        if file_resolver is None:
            lossy = True
            return f"[{match.group('name')}]({original_url})"
        return f"[{match.group('name')}]({file_resolver(match.group('url'), match.group('name'))})"

    converted = UPLOAD_RE.sub(upload, converted)
    if "/user_uploads/" in converted:
        lossy = True
        converted = converted.replace("/user_uploads/", original_url + "#file-")
    if lossy and original_url not in converted:
        converted = f"{converted}\n\n[Open original]({original_url})"
    return converted, lossy


def _provider(
    chat_key: str, entity_id: str | None, revision: str | None = None
) -> dict[str, object]:
    return {
        "kind": "zulip",
        "chat_id": chat_key,
        "entity_id": entity_id,
        "revision": revision,
    }


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
            raise ValueError("provider_chat_assignment_pending")
        else:
            identity_uuid = str(existing["workspace_uuid"])
        identities[provider_user_id] = identity_uuid
        display_name = str(user.get("full_name", message.get("sender_full_name", "")))
        mentions[display_name] = identity_uuid
        mentions[f"id:{provider_user_id}"] = identity_uuid
        if identity_uuid == owner_uuid:
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
        if mapping is None:
            raise ValueError("provider_chat_assignment_pending")
        identity_uuid = str(mapping["workspace_uuid"])
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
) -> tuple[str, str, str, str, str, bool]:
    chat_type, chat_key = provider_chat_reference(message)
    project_uuid, assignment_exists = _assignment(store, account_uuid, chat_key)
    stream_mapping = store.provider_mapping(account_uuid, "stream", chat_key)
    if stream_mapping is None:
        raise ValueError("provider_chat_assignment_pending")
    stream_uuid = str(stream_mapping["workspace_uuid"])
    topic_provider_id = (
        f"{message['stream_id']}:{message['subject']}"
        if chat_type == "channel"
        else f"{chat_key}:default"
    )
    topic_mapping = store.provider_mapping(account_uuid, "topic", topic_provider_id)
    if topic_mapping is None:
        raise ValueError("provider_chat_assignment_pending")
    topic_uuid = str(topic_mapping["workspace_uuid"])
    return chat_type, chat_key, project_uuid, stream_uuid, topic_uuid, assignment_exists


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
    (
        chat_type,
        chat_key,
        project_uuid,
        stream_uuid,
        topic_uuid,
        assignment_exists,
    ) = _message_context(store, account_uuid, message)
    provider_message_id = str(message["id"])
    existing_message = store.provider_mapping(
        account_uuid, "message", provider_message_id
    )
    existing_message_metadata = (
        typing.cast(dict[str, object], existing_message.get("metadata", {}))
        if existing_message is not None
        else {}
    )
    message_uuid = (
        str(existing_message["workspace_uuid"])
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
    expected_participants = {
        owner_uuid,
        author_uuid,
        *(identity_uuids[int(user["id"])] for user in recipients),
    }
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
        str(message["content"]), mention_uuids, message_url, file_resolver
    )
    reply_match = REPLY_LINK_RE.search(str(message["content"]))
    reply_provider_id = reply_match.group("id") if reply_match is not None else None
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
    topic_provider_id = (
        f"{message['stream_id']}:{message['subject']}"
        if chat_type == "channel"
        else f"{chat_key}:default"
    )
    message_operation = {
        "kind": "message.create",
        "entity_uuid": message_uuid,
        "actor_uuid": author_uuid,
        "occurred_at": occurred_at,
        "provider": _provider(chat_key, provider_message_id),
        "payload": {
            "stream_uuid": stream_uuid,
            "topic_uuid": topic_uuid,
            "author_uuid": author_uuid,
            "payload": {"kind": "markdown", "content": markdown},
            "reply_to_message_uuid": reply_to_message_uuid,
        },
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
    operations = [
        *identity_operations,
        {
            "kind": "stream.upsert",
            "entity_uuid": stream_uuid,
            "actor_uuid": owner_uuid,
            "occurred_at": occurred_at,
            "provider": _provider(chat_key, chat_key),
            "payload": {
                "name": stream_name,
                "description": stream_description,
                "private": stream_private,
                "chat_kind": {
                    "channel": "channel",
                    "direct": "personal_dm",
                    "group_direct": "group_dm",
                }[chat_type],
                "participant_uuids": participants,
                "default_topic_uuid": default_topic_uuid,
            },
            "extensions": {
                "assignment_materialized": assignment_exists,
                "provider_badge": "zulip",
            },
        },
        {
            "kind": "topic.upsert",
            "entity_uuid": topic_uuid,
            "actor_uuid": owner_uuid,
            "occurred_at": occurred_at,
            "provider": _provider(chat_key, topic_provider_id),
            "payload": {
                "stream_uuid": stream_uuid,
                "name": message["subject"] if chat_type == "channel" else "default",
            },
            "extensions": {"provider_badge": "zulip"},
        },
    ]
    if not workspace_delivery_committed:
        operations.append(message_operation)
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
            "subject": str(message.get("subject", "")),
            "mapping_origin": existing_message_metadata.get("mapping_origin", "zulip"),
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
                f"provider-message:{provider_message_id}",
                int(event["id"]),
                index,
                operation,
                operation_lane,
                occurred_at_dt,
                delivery_class,
            )
        )
    return records


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
        and event["orig_subject"] != event["subject"]
    ):
        old_provider_id = f"{event['stream_id']}:{event['orig_subject']}"
        old_topic = store.provider_mapping(account_uuid, "topic", old_provider_id)
        if old_topic is not None:
            topic_metadata = typing.cast(dict[str, object], old_topic["metadata"])
            new_provider_id = f"{event['stream_id']}:{event['subject']}"
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
                            "name": str(event["subject"]),
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
            key = (
                str(metadata["project_uuid"]),
                str(metadata["stream_uuid"]),
                str(metadata["topic_uuid"]),
                str(metadata["chat_key"]),
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
                str(event["content"]), mention_uuids, message_url, file_resolver
            )
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
                },
            }
            store.remember_provider_mapping(
                account_uuid,
                "message",
                provider_message_id,
                str(mapping["workspace_uuid"]),
                {
                    **metadata,
                    "content_sha256": hashlib.sha256(
                        markdown.encode("utf-8")
                    ).hexdigest(),
                    "provider_content_sha256": provider_content_sha256,
                    "subject": str(event.get("subject", metadata.get("subject", ""))),
                },
                str(event.get("edit_timestamp")),
            )
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
            project_uuid, assignment_exists = _assignment(store, account_uuid, chat_key)
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
