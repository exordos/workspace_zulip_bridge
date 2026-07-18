import datetime
import typing
import uuid

from workspace_zulip_bridge import canonical, converter

_OUTBOUND_KIND = {
    "message.create": "message.create",
    "message.update": "message.update",
    "message.delete": "message.delete",
    "read_state.set": "read_state.set",
    "stream.update": "stream.upsert",
    "topic.update": "topic.upsert",
}

_INBOUND_KIND = {
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
}

_NON_EVENT_KINDS = frozenset({"identity.upsert"})


def _provider_mapping(store, account_uuid: str, kind: str, workspace_uuid: object):
    mapping = store.workspace_mapping(account_uuid, kind, str(workspace_uuid))
    if mapping is None:
        raise ValueError(f"Missing Zulip {kind} mapping")
    return mapping


def _chat_key(store, account_uuid: str, kind: str, payload: dict[str, object]):
    if kind.startswith("stream."):
        stream_uuid = payload["uuid"]
    elif kind.startswith(("topic.", "message.")) or kind == "read_state.set":
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
    if kind not in {"message.create", "read_state.set"}:
        mapping = _provider_mapping(store, account_uuid, entity_kind, entity_uuid)
        entity_id = str(mapping["provider_id"])
    operation = {
        "kind": bridge_kind,
        "entity_uuid": entity_uuid,
        "actor_uuid": str(
            payload.get("reader_uuid")
            or payload.get("user_uuid")
            or payload.get("author_uuid")
            or uuid.UUID(int=0)
        ),
        "occurred_at": str(
            payload.get("updated_at")
            or payload.get("created_at")
            or datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
        ),
        "provider": {
            "kind": "zulip",
            "chat_id": chat_key,
            "entity_id": entity_id,
            "revision": None,
        },
        "payload": payload,
        "extensions": {},
    }
    local_operation_uuid = str(uuid.UUID(str(leased["external_operation_uuid"])))
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


def event_payload(store, record: dict[str, object]) -> dict[str, object] | None:
    operation = typing.cast(dict[str, object], record["operation"])
    operation_kind = str(operation["kind"])
    kind = _INBOUND_KIND.get(operation_kind)
    if kind is None:
        if operation_kind in _NON_EVENT_KINDS:
            return None
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
    if not kind.endswith(".delete"):
        resource.update(payload)
        if "author_uuid" in resource:
            resource["user_uuid"] = resource.pop("author_uuid")
    else:
        for relation in ("stream_uuid", "topic_uuid", "message_uuid"):
            if relation in payload:
                resource[relation] = payload[relation]
    return {
        "provider_event_uuid": str(record["operation_uuid"]),
        "external_account_uuid": account_uuid,
        "external_chat_uuid": external_chat_uuid,
        "project_id": project_uuid,
        "provider_sequence": str(record["sequence"]),
        "kind": kind,
        "payload": {"resource": resource},
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
