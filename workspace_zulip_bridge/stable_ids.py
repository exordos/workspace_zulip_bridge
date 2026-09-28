# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from uuid import NAMESPACE_URL
from uuid import UUID
from uuid import uuid5

_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "https://exordos.com/workspace-zulip-bridge/entities/v4",
)


def stable_user_uuid(account_uuid: UUID, user_id: int) -> UUID:
    return _stable_uuid(account_uuid, "user", str(user_id))


def stable_stream_uuid(account_uuid: UUID, chat_key: str) -> UUID:
    return _stable_uuid(account_uuid, "stream", chat_key)


def stable_topic_uuid(stream_uuid: UUID, topic_name: str) -> UUID:
    return uuid5(stream_uuid, f"topic\0{topic_name}")


def stable_message_uuid(account_uuid: UUID, message_id: int) -> UUID:
    return _stable_uuid(account_uuid, "message", str(message_id))


def stable_stream_binding_uuid(stream_uuid: UUID, user_uuid: UUID) -> UUID:
    return uuid5(stream_uuid, f"binding\0{user_uuid}")


def stable_topic_binding_uuid(topic_uuid: UUID, user_uuid: UUID) -> UUID:
    return uuid5(topic_uuid, f"binding\0{user_uuid}")


def _stable_uuid(
    account_uuid: UUID,
    entity_type: str,
    provider_key: str,
) -> UUID:
    name = "\0".join((str(account_uuid), entity_type, provider_key))
    return uuid5(_NAMESPACE, name)
