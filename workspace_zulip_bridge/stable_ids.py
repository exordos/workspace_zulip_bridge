# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from urllib.parse import SplitResult
from urllib.parse import urlsplit
from urllib.parse import urlunsplit
from uuid import NAMESPACE_URL
from uuid import UUID
from uuid import uuid5

_BRIDGE_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "https://exordos.com/workspace-zulip-bridge/entities/v1",
)


def canonical_endpoint(endpoint: str) -> str:
    parsed = urlsplit(endpoint.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Zulip endpoint must be an absolute HTTP(S) URL")
    host = parsed.hostname
    if host is None:
        raise ValueError("Zulip endpoint must contain a host")
    scheme = parsed.scheme.lower()
    default_port = 443 if scheme == "https" else 80
    port = parsed.port
    netloc = host.lower()
    if port is not None and port != default_port:
        netloc = f"{netloc}:{port}"
    path = parsed.path.rstrip("/")
    return urlunsplit(SplitResult(scheme, netloc, path, "", ""))


def stable_user_uuid(endpoint: str, zulip_user_id: int) -> UUID:
    return _stable_uuid(endpoint, "user", str(zulip_user_id))


def stable_realm_uuid(endpoint: str) -> UUID:
    return _stable_uuid(endpoint, "realm", canonical_endpoint(endpoint))


def stable_chat_uuid(endpoint: str, chat_key: str) -> UUID:
    return _stable_uuid(endpoint, "chat", chat_key)


def stable_topic_uuid(chat_uuid: UUID, topic_name: str) -> UUID:
    return uuid5(chat_uuid, f"topic\0{topic_name}")


def stable_message_uuid(endpoint: str, zulip_message_id: int) -> UUID:
    return _stable_uuid(endpoint, "message", str(zulip_message_id))


def stable_stream_binding_uuid(stream_uuid: UUID, user_uuid: UUID) -> UUID:
    return uuid5(stream_uuid, f"binding\0{user_uuid}")


def stable_topic_binding_uuid(topic_uuid: UUID, user_uuid: UUID) -> UUID:
    return uuid5(topic_uuid, f"binding\0{user_uuid}")


def stable_message_flag_uuid(message_uuid: UUID, user_uuid: UUID) -> UUID:
    return uuid5(message_uuid, f"flags\0{user_uuid}")


def stable_reaction_uuid(
    message_uuid: UUID,
    user_uuid: UUID,
    reaction_type: str,
    emoji_code: str,
) -> UUID:
    return uuid5(
        message_uuid,
        "\0".join(("reaction", str(user_uuid), reaction_type, emoji_code)),
    )


def stable_file_uuid(endpoint: str, source_path: str) -> UUID:
    return _stable_uuid(endpoint, "file", source_path)


def _stable_uuid(endpoint: str, entity_type: str, provider_key: str) -> UUID:
    name = "\0".join((canonical_endpoint(endpoint), entity_type, provider_key))
    return uuid5(_BRIDGE_NAMESPACE, name)
