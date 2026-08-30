import datetime
import hashlib
import json
import typing
import uuid

from workspace_zulip_bridge import canonical, converter

_OUTBOUND_KIND = {
    "message.create": "message.create",
    "message.update": "message.update",
    "message.delete": "message.delete",
    "reaction.create": "reaction.create",
    "reaction.update": "reaction.update",
    "reaction.delete": "reaction.delete",
    "read_state.set": "read_state.set",
    "membership.add": "membership.add",
    "membership.remove": "membership.remove",
    "stream.notification.update": "stream.notification.update",
    "topic.notification.update": "topic.notification.update",
    "stream.delete": "stream.delete",
    "topic.create": "topic.create",
    "stream.update": "stream.upsert",
    "topic.update": "topic.upsert",
    "topic.delete": "topic.delete",
}

_INBOUND_KIND = {
    "identity.upsert": "identity.upsert",
    "stream.upsert": "stream.upsert",
    "stream.delete": "stream.delete",
    "topic.upsert": "topic.upsert",
    "topic.delete": "topic.delete",
    "message.create": "message.upsert",
    "message.update": "message.upsert",
    "message.delete": "message.delete",
    "reaction.upsert": "reaction.upsert",
    "reaction.delete": "reaction.delete",
    "read_state.set": "read_state.set",
    "stream.notification.update": "stream.notification.update",
    "topic.notification.update": "topic.notification.update",
}

_SERVER_OWNED_RESOURCE_FIELDS = {
    "external_account_uuid",
    "external_chat_uuid",
    "message_uuid",
    "message_uuids",
    "project_id",
    "provider_external_id",
    "provider_uuid",
    "reader_uuid",
    "stream_uuid",
    "topic_uuid",
    "user_uuid",
    "uuid",
}


def _provider_mapping(store, account_uuid: str, kind: str, workspace_uuid: object):
    mapping = store.workspace_mapping(account_uuid, kind, str(workspace_uuid))
    if mapping is None:
        raise ValueError(f"Missing Zulip {kind} mapping")
    return mapping


def _chat_key(store, account_uuid: str, kind: str, payload: dict[str, object]):
    if kind.startswith("stream."):
        stream_uuid = payload["uuid"]
    elif (
        kind.startswith(("topic.", "message.", "membership."))
        or kind == "read_state.set"
    ):
        stream_uuid = payload["stream_uuid"]
    elif kind.startswith("reaction."):
        message = _provider_mapping(
            store, account_uuid, "message", payload["message_uuid"]
        )
        metadata = typing.cast(dict[str, object], message["metadata"])
        return str(metadata["chat_key"])
    else:
        raise ValueError("Unsupported Provider operation kind")
    return str(
        _provider_mapping(store, account_uuid, "stream", stream_uuid)["provider_id"]
    )


def _message_created_at(
    store,
    account_uuid: str,
    provider: dict[str, object],
    operation: dict[str, object],
    record: dict[str, object],
) -> str:
    provider_id = provider.get("entity_id")
    lookup = getattr(store, "provider_mapping", None)
    if provider_id is not None and callable(lookup):
        mapping = lookup(account_uuid, "message", str(provider_id))
        if mapping is not None:
            metadata = typing.cast(dict[str, object], mapping.get("metadata", {}))
            provider_timestamp = metadata.get("provider_timestamp")
            if isinstance(provider_timestamp, (int, float, str)) and not isinstance(
                provider_timestamp, bool
            ):
                try:
                    return (
                        datetime.datetime.fromtimestamp(
                            float(provider_timestamp), datetime.UTC
                        )
                        .isoformat()
                        .replace("+00:00", "Z")
                    )
                except (OverflowError, TypeError, ValueError):
                    pass
    value = operation.get("occurred_at") or record["created_at"]
    if isinstance(value, datetime.datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("Provider message creation time is invalid")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.UTC)
    return parsed.astimezone(datetime.UTC).isoformat().replace("+00:00", "Z")


def leased_operation_record(store, leased: dict[str, object]) -> dict[str, object]:
    """Adapt the exact Provider API lease envelope to the durable scheduler record."""
    kind = str(leased["operation_kind"])
    bridge_kind = _OUTBOUND_KIND[kind]
    account_uuid = str(uuid.UUID(str(leased["external_account_uuid"])))
    project_uuid = str(uuid.UUID(str(leased["project_id"])))
    payload = typing.cast(dict[str, object], leased["payload"])
    entity_kind = kind.split(".", 1)[0]
    if kind == "read_state.set":
        exact_message_uuids = payload.get("message_uuids")
        if not isinstance(exact_message_uuids, list) or not exact_message_uuids:
            raise ValueError("Provider read state requires exact message UUIDs")
        entity_uuid = str(uuid.UUID(str(exact_message_uuids[-1])))
    else:
        entity_uuid = str(uuid.UUID(str(payload["uuid"])))
    chat_key = _chat_key(store, account_uuid, kind, payload)
    entity_id = None
    if kind.startswith("membership."):
        identity = _provider_mapping(
            store,
            account_uuid,
            "identity",
            payload["user_uuid"],
        )
        entity_id = str(identity["provider_id"])
    elif kind.startswith("reaction."):
        message = _provider_mapping(
            store,
            account_uuid,
            "message",
            payload["message_uuid"],
        )
        entity_id = str(message["provider_id"])
    elif kind in {"message.update", "message.delete"}:
        # A create and its mutation can arrive in the same lease batch. Keep
        # the mutation durable; its causal predecessor installs this mapping.
        mapping = store.workspace_mapping(
            account_uuid,
            entity_kind,
            entity_uuid,
        )
        if mapping is not None:
            entity_id = str(mapping["provider_id"])
    elif kind in {
        "topic.create",
        "topic.update",
        "topic.delete",
        "topic.notification.update",
    }:
        # A topic create and its first mutation can be leased together. The
        # causal predecessor installs the mapping before the mutation runs.
        mapping = store.workspace_mapping(
            account_uuid,
            entity_kind,
            entity_uuid,
        )
        if mapping is not None:
            entity_id = str(mapping["provider_id"])
    elif kind not in {"message.create", "read_state.set"}:
        mapping = _provider_mapping(store, account_uuid, entity_kind, entity_uuid)
        entity_id = str(mapping["provider_id"])
    occurred_at = (
        payload.get("notification_updated_at")
        or payload.get("updated_at")
        or payload.get("created_at")
    )
    if occurred_at is None:
        # Some server-owned operations have no domain timestamp. Their stable
        # operation UUID still identifies the same desired change across lease
        # renewal, so a stable sentinel is required to keep the operation digest
        # replay-safe.
        occurred_at = "1970-01-01T00:00:00Z"
    operation = {
        "kind": bridge_kind,
        "entity_uuid": entity_uuid,
        "actor_uuid": str(
            payload.get("reader_uuid")
            or payload.get("who_uuid")
            or payload.get("user_uuid")
            or payload.get("author_uuid")
            or uuid.UUID(int=0)
        ),
        "occurred_at": str(occurred_at),
        "provider": {
            "kind": "zulip",
            "chat_id": chat_key,
            "entity_id": entity_id,
            "revision": None,
        },
        "payload": payload,
        "extensions": {},
    }
    # The provider record is the durable local unit. Its UUID remains stable
    # across lease retries even when an older backend exposes a public read
    # operation UUID in the legacy external field.
    local_operation_uuid = str(
        uuid.UUID(
            str(
                leased[
                    "provider_operation_uuid"
                    if kind == "read_state.set"
                    else "external_operation_uuid"
                ]
            )
        )
    )
    record: dict[str, object] = {
        "schema": "workspace.provider",
        "schema_version": 1,
        "record_kind": "operation",
        "record_uuid": str(uuid.UUID(str(leased["provider_operation_uuid"]))),
        "operation_uuid": local_operation_uuid,
        "attempt": 1,
        "operation_sha256": "",
        "account_uuid": account_uuid,
        "project_uuid": project_uuid,
        "origin": "workspace",
        "causal_lane": f"chat:{account_uuid}:{chat_key}",
        "sequence": 0,
        "predecessor_operation_uuid": None,
        "created_at": datetime.datetime.now(datetime.UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "expires_at": str(leased["lease_expires_at"]),
        "transport": {
            "provider_operation_uuid": str(leased["provider_operation_uuid"]),
            "lease_uuid": str(leased["lease_uuid"]),
            "required_capability": leased.get("required_capability"),
            "provider_attempt": leased["attempt"],
        },
        "operation": operation,
    }
    if hasattr(store, "producer_lane_position"):
        sequence, predecessor = store.producer_lane_position(
            local_operation_uuid,
            "workspace",
            str(record["causal_lane"]),
        )
        if sequence:
            record["sequence"] = sequence
            record["predecessor_operation_uuid"] = predecessor
    record["operation_sha256"] = canonical.operation_digest(record)
    return record


def result_payload(result: dict[str, object]) -> dict[str, object]:
    transport = typing.cast(dict[str, object], result["transport"])
    body = typing.cast(dict[str, object], result["result"])
    outcome = str(body["outcome"])
    safe_error = body.get("safe_error")
    status = {
        "committed": "succeeded",
        "manual_reconciliation_required": "manual_reconciliation_required",
    }.get(outcome, "failed")
    payload: dict[str, object] = {
        "result_uuid": str(result["record_uuid"]),
        "provider_operation_uuid": str(transport["provider_operation_uuid"]),
        "lease_uuid": str(transport["lease_uuid"]),
        "status": status,
        "safe_error": (
            None
            if not isinstance(safe_error, dict)
            else str(safe_error.get("code", "provider operation failed"))
        ),
    }
    if status == "manual_reconciliation_required":
        payload["reconciliation"] = body["reconciliation"]
    return payload


def _event_identity_mapping(
    store,
    account_uuid: str,
    workspace_uuid: str,
) -> dict[str, object] | None:
    mapping = store.workspace_mapping(account_uuid, "identity", workspace_uuid)
    if mapping is not None:
        return mapping
    tombstone_reader = getattr(store, "tombstoned_workspace_mapping", None)
    if not callable(tombstone_reader):
        return None
    return tombstone_reader(account_uuid, "identity", workspace_uuid)


def event_payload(store, record: dict[str, object]) -> dict[str, object] | None:
    operation = typing.cast(dict[str, object], record["operation"])
    operation_kind = str(operation["kind"])
    kind = _INBOUND_KIND.get(operation_kind)
    if kind is None:
        raise ValueError(f"Unsupported Provider event operation kind: {operation_kind}")
    account_uuid = str(record["account_uuid"])
    project_uuid = str(record["project_uuid"])
    provider = typing.cast(dict[str, object], operation["provider"])
    chat_key = str(provider["chat_id"])
    external_chat_uuid = _AccountRouting(store, account_uuid).external_chat_uuid(
        chat_key
    )
    payload = dict(typing.cast(dict[str, object], operation["payload"]))
    resource: dict[str, object] = {
        "uuid": str(operation["entity_uuid"]),
        "provider_external_id": str(
            provider.get("entity_id")
            or provider.get("chat_id")
            or operation["entity_uuid"]
        ),
        "provider_metadata": {
            "chat_key": chat_key,
            "provider_revision": provider.get("revision"),
            **typing.cast(dict[str, object], operation.get("extensions", {})),
        },
    }
    if kind.startswith("reaction."):
        resource.update(payload)
    elif not kind.endswith(".delete"):
        resource.update(payload)
        if "author_uuid" in resource:
            resource["user_uuid"] = resource.pop("author_uuid")
        if kind == "message.upsert":
            resource["created_at"] = _message_created_at(
                store,
                account_uuid,
                provider,
                operation,
                record,
            )
        if kind == "message.upsert" and "user_uuid" in resource:
            author = _event_identity_mapping(
                store,
                account_uuid,
                str(resource["user_uuid"]),
            )
            if author is not None:
                author_metadata = typing.cast(
                    dict[str, object],
                    author.get("metadata", {}),
                )
                resource["author_identity"] = {
                    "provider_external_id": str(author["provider_id"]),
                    "display_name": str(
                        author_metadata.get("display_name", author["provider_id"])
                    ),
                    "email": author_metadata.get("email"),
                    "avatar_urn": author_metadata.get("avatar_urn"),
                    "active": bool(author_metadata.get("active", True)),
                }
    else:
        for relation in ("stream_uuid", "topic_uuid", "message_uuid"):
            if relation in payload:
                resource[relation] = payload[relation]
    if kind.startswith("reaction.") and "user_uuid" in resource:
        actor = _event_identity_mapping(
            store,
            account_uuid,
            str(resource["user_uuid"]),
        )
        if actor is not None:
            actor_metadata = typing.cast(
                dict[str, object],
                actor.get("metadata", {}),
            )
            resource["user_identity"] = {
                "provider_external_id": str(actor["provider_id"]),
                "display_name": str(
                    actor_metadata.get("display_name", actor["provider_id"])
                ),
                "email": actor_metadata.get("email"),
                "avatar_urn": actor_metadata.get("avatar_urn"),
                "active": bool(actor_metadata.get("active", True)),
            }
    return {
        "provider_event_uuid": str(record["operation_uuid"]),
        "external_account_uuid": account_uuid,
        "external_chat_uuid": external_chat_uuid,
        "project_id": project_uuid,
        "provider_sequence": str(record["sequence"]),
        "kind": kind,
        "payload": {"resource": resource},
    }


def direct_conversation_key(
    store,
    account_uuid: str,
    provider_chat_key: str,
) -> str:
    """Return the realm-global v1 direct-conversation serialization."""
    if provider_chat_key.startswith("channel:") or provider_chat_key == "account":
        return provider_chat_key
    if provider_chat_key.startswith("direct-conversation:v1:"):
        count, separator, identifiers = provider_chat_key.removeprefix(
            "direct-conversation:v1:"
        ).partition(":")
        values = identifiers.split(",") if separator else []
        if str(len(values)) != count:
            raise ValueError("Provider direct-conversation key is invalid")
    elif provider_chat_key.startswith(("direct:", "group_direct:")):
        values = provider_chat_key.split(":", 1)[1].split(",")
    else:
        raise ValueError("Provider chat key is invalid")
    participant_ids = {int(value) for value in values if value}
    cursor_reader = getattr(store, "provider_event_cursor", None)
    cursor = cursor_reader(account_uuid) if callable(cursor_reader) else None
    owner_id = (
        cursor.get("provider_owner_user_id") if isinstance(cursor, dict) else None
    )
    if owner_id is not None:
        participant_ids.add(int(str(owner_id)))
    if not participant_ids:
        raise ValueError("Provider direct-conversation participants are missing")
    identifiers = ",".join(str(value) for value in sorted(participant_ids))
    return f"direct-conversation:v1:{len(participant_ids)}:{identifiers}"


def _workspace_provider_id(
    store,
    account_uuid: str,
    kind: str,
    workspace_uuid: object,
) -> str:
    return str(
        _provider_mapping(store, account_uuid, kind, workspace_uuid)["provider_id"]
    )


def provider_event_key(
    kind: str,
    provider_chat_key: str,
    provider_object: dict[str, str],
    provider_references: dict[str, object],
    payload: dict[str, object],
) -> str:
    """Return the account-independent semantic key for a desired provider state."""
    object_kind = str(provider_object["kind"])
    object_id = str(provider_object["id"])
    state = {
        "provider_chat_key": provider_chat_key,
        "provider_object": provider_object,
        "provider_references": provider_references,
        "payload": payload,
    }
    normalized = json.dumps(
        state,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(normalized).hexdigest()
    object_id_length = len(object_id.encode("utf-8"))
    return (
        f"provider-event:v1:{kind}:{object_kind}:"
        f"{object_id_length}:{object_id}:{digest}"
    )


def command_payload(store, record: dict[str, object]) -> dict[str, object] | None:
    """Adapt one durable record to the server-owned Provider Data API v2."""
    event = event_payload(store, record)
    if event is None:
        return None
    account_uuid = str(record["account_uuid"])
    operation = typing.cast(dict[str, object], record["operation"])
    provider = typing.cast(dict[str, object], operation["provider"])
    resource = dict(typing.cast(dict[str, object], event["payload"])["resource"])
    kind = str(event["kind"])
    provider_chat_key = direct_conversation_key(
        store,
        account_uuid,
        str(provider["chat_id"]),
    )
    object_kind = {
        "identity": "user",
        "stream": "channel"
        if provider_chat_key.startswith("channel:")
        else "conversation",
        "topic": "topic",
        "message": "message",
        "reaction": "reaction",
        "read_state": "read-state",
    }[kind.split(".", 1)[0]]
    object_id = str(resource["provider_external_id"])
    references: dict[str, object] = {}
    if kind.startswith("topic."):
        references["topic"] = object_id
    elif kind.startswith("message."):
        topic_uuid = resource.get("topic_uuid")
        if topic_uuid is not None:
            references["topic"] = _workspace_provider_id(
                store, account_uuid, "topic", topic_uuid
            )
        user_uuid = resource.get("user_uuid")
        if user_uuid is not None:
            references["user"] = _workspace_provider_id(
                store, account_uuid, "identity", user_uuid
            )
    elif kind.startswith("reaction."):
        references["message"] = _workspace_provider_id(
            store,
            account_uuid,
            "message",
            resource["message_uuid"],
        )
        references["user"] = _workspace_provider_id(
            store,
            account_uuid,
            "identity",
            resource["user_uuid"],
        )
    elif kind == "read_state.set":
        references["messages"] = [
            _workspace_provider_id(store, account_uuid, "message", message_uuid)
            for message_uuid in typing.cast(list[object], resource["message_uuids"])
        ]
        references["reader"] = _workspace_provider_id(
            store,
            account_uuid,
            "identity",
            resource["reader_uuid"],
        )
        topic_uuid = resource.get("topic_uuid")
        if topic_uuid is not None:
            references["topic"] = _workspace_provider_id(
                store, account_uuid, "topic", topic_uuid
            )
    elif kind == "topic.notification.update":
        references["topic"] = _workspace_provider_id(
            store,
            account_uuid,
            "topic",
            resource["uuid"],
        )
    provider_metadata = resource.get("provider_metadata")
    if isinstance(provider_metadata, dict):
        resource["provider_metadata"] = {
            key: value
            for key, value in provider_metadata.items()
            if key
            not in {
                "account_uuid",
                "chat_key",
                "delivery_class",
                "external_id",
                "provider_event_uuid",
            }
        }
    payload = {
        key: value
        for key, value in resource.items()
        if key not in _SERVER_OWNED_RESOURCE_FIELDS
    }
    provider_object = {"kind": object_kind, "id": object_id}
    command_chat_key = "account" if kind == "identity.upsert" else provider_chat_key
    return {
        "provider_event_key": provider_event_key(
            kind,
            command_chat_key,
            provider_object,
            references,
            payload,
        ),
        "delivery_uuid": str(uuid.UUID(str(record["operation_uuid"]))),
        "external_account_uuid": account_uuid,
        "provider_chat_key": command_chat_key,
        "provider_sequence": provider.get("revision"),
        "kind": kind,
        "provider_object": provider_object,
        "provider_references": references,
        "payload": payload,
    }


class _AccountRouting:
    def __init__(self, store, account_uuid: str):
        self.store = store
        self.account_uuid = account_uuid

    def external_chat_uuid(self, provider_chat_key: str) -> str:
        assignment = self.store.assignment_for_provider_chat(
            self.account_uuid, provider_chat_key
        )
        if assignment is not None:
            return str(assignment["uuid"])
        return converter.stable_entity_uuid(
            self.account_uuid, "external_chat", provider_chat_key
        )
