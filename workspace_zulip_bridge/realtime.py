# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import datetime
import hashlib
import json
import logging
import ssl
from collections.abc import Mapping
from typing import Any
from uuid import UUID

import httpx

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.models import ExternalAccount
from workspace_zulip_bridge.models import ZulipIdentity
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_stream_binding_uuid
from workspace_zulip_bridge.stable_ids import stable_stream_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_binding_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid
from workspace_zulip_bridge.v4_store import V4Store
from workspace_zulip_bridge.workspace_auth import WorkspaceTokenManager
from workspace_zulip_bridge.zulip_api import ZulipApiClient
from workspace_zulip_bridge.zulip_api import ZulipApiError

LOG = logging.getLogger(__name__)
_REALTIME_PATH = "/provider/v4/realtime"


class ZulipQueueUnavailableError(RuntimeError):
    """The mapped account has not yet established its live event queue."""


def _timestamp(value: int | float) -> str:
    return (
        datetime.datetime.fromtimestamp(value, datetime.UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _hash(data: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            data,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _operation(
    resource: str,
    entity_uuid: UUID,
    data: Mapping[str, Any],
    source_updated_at: str,
) -> dict[str, Any]:
    value = dict(data)
    return {
        "action": "upsert",
        "type": resource,
        "uuid": str(entity_uuid),
        "content_hash": _hash(value),
        "source_updated_at": source_updated_at,
        "data": value,
    }


class WorkspaceRealtimeClient:
    def __init__(
        self,
        settings: Settings,
        tokens: WorkspaceTokenManager | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if settings.workspace_api_url is None:
            raise ValueError("Workspace API URL is required for realtime delivery")
        self._url = settings.workspace_api_url.rstrip("/") + _REALTIME_PATH
        self._tokens = tokens or WorkspaceTokenManager(settings)
        self._verify: bool | ssl.SSLContext = True
        if settings.workspace_ca_file is not None:
            context = ssl.create_default_context()
            context.load_verify_locations(cafile=settings.workspace_ca_file)
            self._verify = context
        self._timeout = settings.workspace_request_timeout_seconds
        self._transport = transport

    async def apply(self, operations: list[dict[str, Any]]) -> None:
        if not operations:
            return
        token = await self._tokens.access_token()
        response = await self._request(token, operations)
        if response.status_code == 401:
            token = await self._tokens.access_token(force_refresh=True)
            response = await self._request(token, operations)
        if response.is_error:
            raise RuntimeError(
                f"Workspace realtime API returned HTTP {response.status_code}"
            )
        payload = response.json()
        if not isinstance(payload, Mapping) or not isinstance(
            payload.get("results"), list
        ):
            raise RuntimeError("Workspace realtime API returned invalid JSON")

    async def _request(
        self,
        token: str,
        operations: list[dict[str, Any]],
    ) -> httpx.Response:
        async with httpx.AsyncClient(
            verify=self._verify,
            timeout=httpx.Timeout(self._timeout),
            transport=self._transport,
            follow_redirects=False,
            trust_env=False,
        ) as client:
            return await client.post(
                self._url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                json={"operations": operations},
            )


class ZulipRealtimeProcessor:
    """Apply only messages delivered by the active Zulip event queue."""

    def __init__(self, store: V4Store, workspace: WorkspaceRealtimeClient) -> None:
        self._store = store
        self._workspace = workspace

    async def apply(
        self,
        account: ExternalAccount,
        identity: ZulipIdentity,
        event: Mapping[str, Any],
    ) -> None:
        event_type = event.get("type")
        if event_type in {"message", "update_message"}:
            message = event.get("message")
            if not isinstance(message, Mapping):
                raise ValueError("Zulip message event omitted message")
            await self._upsert(account, identity, event, message)
            return
        if event_type == "delete_message":
            await self._delete(account, event)

    async def _upsert(
        self,
        account: ExternalAccount,
        identity: ZulipIdentity,
        event: Mapping[str, Any],
        message: Mapping[str, Any],
    ) -> None:
        message_id = message.get("id")
        sender_id = message.get("sender_id")
        content = message.get("content")
        sent_at = message.get("timestamp")
        if (
            not isinstance(message_id, int)
            or not isinstance(sender_id, int)
            or not isinstance(content, str)
            or not isinstance(sent_at, int | float)
        ):
            raise ValueError("Zulip message event is incomplete")

        chat_key, stream_name, topic_name, peer_id = self._route(
            identity.user_id,
            message,
        )
        stream_uuid = stable_stream_uuid(account.uuid, chat_key)
        topic_uuid = stable_topic_uuid(stream_uuid, topic_name)
        author_uuid = (
            account.owner_workspace_user_uuid
            if sender_id == identity.user_id
            else stable_user_uuid(account.uuid, sender_id)
        )
        local_message_uuid: UUID | None = None
        local_id = event.get("local_message_id") or message.get("local_message_id")
        try:
            local_message_uuid = UUID(str(local_id))
        except (TypeError, ValueError):
            pass
        if local_message_uuid is not None:
            local_link = await self._store.message_link(local_message_uuid)
            if local_link is not None and local_link.account.uuid == account.uuid:
                await self._store.upsert_realtime_links(
                    account.uuid,
                    local_link.workspace_stream_uuid,
                    chat_key,
                    local_link.workspace_topic_uuid,
                    topic_name,
                    local_message_uuid,
                    message_id,
                )
                return
        message_uuid = await self._store.workspace_message_uuid(
            account.uuid,
            message_id,
        )
        if message_uuid is None:
            message_uuid = stable_message_uuid(account.uuid, message_id)

        created_at = _timestamp(sent_at)
        edit_timestamp = event.get("edit_timestamp")
        updated_at = (
            edit_timestamp if isinstance(edit_timestamp, int | float) else sent_at
        )
        source_updated_at = _timestamp(updated_at)
        operations: list[dict[str, Any]] = [
            _operation(
                "users",
                account.owner_workspace_user_uuid,
                {
                    "username": identity.email,
                    "display_name": identity.full_name,
                    "email": identity.email,
                    "created_at": created_at,
                },
                source_updated_at,
            )
        ]
        external_users: dict[int, tuple[object, object]] = {}
        if message.get("type") == "private" and peer_id is not None:
            recipients = message.get("display_recipient")
            if isinstance(recipients, list):
                for recipient in recipients:
                    if not isinstance(recipient, Mapping):
                        continue
                    recipient_id = recipient.get("id")
                    if recipient_id == peer_id:
                        external_users[recipient_id] = (
                            recipient.get("email"),
                            recipient.get("full_name"),
                        )
        if sender_id != identity.user_id:
            external_users[sender_id] = (
                message.get("sender_email"),
                message.get("sender_full_name"),
            )
        for external_user_id, (email, full_name) in sorted(external_users.items()):
            user_data: dict[str, Any] = {
                "username": (
                    email
                    if isinstance(email, str) and email
                    else f"zulip-{external_user_id}"
                ),
                "display_name": (
                    full_name
                    if isinstance(full_name, str) and full_name.strip()
                    else f"Zulip user {external_user_id}"
                ),
                "created_at": created_at,
            }
            if isinstance(email, str) and email:
                user_data["email"] = email
            operations.append(
                _operation(
                    "users",
                    stable_user_uuid(account.uuid, external_user_id),
                    user_data,
                    source_updated_at,
                )
            )

        stream_data: dict[str, Any] = {
            "name": stream_name[:255],
            "description": "",
            "owner_uuid": str(account.owner_workspace_user_uuid),
            "default_topic_uuid": str(topic_uuid),
            "private": chat_key.startswith("direct:"),
            "history_public_to_subscribers": False,
            "created_at": created_at,
        }
        if peer_id is not None:
            stream_data["direct_user_uuid"] = str(
                account.owner_workspace_user_uuid
                if peer_id == identity.user_id
                else stable_user_uuid(account.uuid, peer_id)
            )
        operations.extend(
            (
                _operation("streams", stream_uuid, stream_data, source_updated_at),
                _operation(
                    "stream_bindings",
                    stable_stream_binding_uuid(
                        stream_uuid,
                        account.owner_workspace_user_uuid,
                    ),
                    {
                        "stream_uuid": str(stream_uuid),
                        "user_uuid": str(account.owner_workspace_user_uuid),
                        "role": "owner",
                        "created_at": created_at,
                    },
                    source_updated_at,
                ),
                _operation(
                    "topics",
                    topic_uuid,
                    {
                        "stream_uuid": str(stream_uuid),
                        "name": topic_name,
                        "created_at": created_at,
                    },
                    source_updated_at,
                ),
                _operation(
                    "topic_bindings",
                    stable_topic_binding_uuid(
                        topic_uuid,
                        account.owner_workspace_user_uuid,
                    ),
                    {
                        "stream_uuid": str(stream_uuid),
                        "topic_uuid": str(topic_uuid),
                        "user_uuid": str(account.owner_workspace_user_uuid),
                        "created_at": created_at,
                    },
                    source_updated_at,
                ),
                _operation(
                    "messages",
                    message_uuid,
                    {
                        "stream_uuid": str(stream_uuid),
                        "topic_uuid": str(topic_uuid),
                        "author_uuid": str(author_uuid),
                        "payload": {"kind": "markdown", "content": content},
                        "created_at": created_at,
                    },
                    source_updated_at,
                ),
            )
        )
        await self._workspace.apply(operations)
        await self._store.upsert_realtime_links(
            account.uuid,
            stream_uuid,
            chat_key,
            topic_uuid,
            topic_name,
            message_uuid,
            message_id,
        )

    async def _delete(
        self,
        account: ExternalAccount,
        event: Mapping[str, Any],
    ) -> None:
        raw_ids = event.get("message_ids")
        if not isinstance(raw_ids, list):
            raw_id = event.get("message_id")
            raw_ids = [raw_id] if isinstance(raw_id, int) else []
        message_ids = [value for value in raw_ids if isinstance(value, int)]
        operations = []
        message_uuids = []
        for message_id in message_ids:
            message_uuid = await self._store.workspace_message_uuid(
                account.uuid,
                message_id,
            ) or stable_message_uuid(account.uuid, message_id)
            message_uuids.append(message_uuid)
            operations.append(
                {
                    "action": "delete",
                    "type": "messages",
                    "uuid": str(message_uuid),
                }
            )
        await self._workspace.apply(operations)
        for message_uuid in message_uuids:
            await self._store.delete_message_link(message_uuid)

    @staticmethod
    def _route(
        own_user_id: int,
        message: Mapping[str, Any],
    ) -> tuple[str, str, str, int | None]:
        if message.get("type") == "stream":
            stream_id = message.get("stream_id")
            stream_name = message.get("display_recipient")
            topic = message.get("subject", message.get("topic", ""))
            if not isinstance(stream_id, int) or not isinstance(stream_name, str):
                raise ValueError("Zulip channel message route is incomplete")
            return (
                f"channel:{stream_id}",
                stream_name,
                topic if isinstance(topic, str) and topic else "General",
                None,
            )
        if message.get("type") != "private":
            raise ValueError("unsupported Zulip message type")
        recipients = message.get("display_recipient")
        people = (
            [item for item in recipients if isinstance(item, Mapping)]
            if isinstance(recipients, list)
            else []
        )
        user_ids = {
            value for item in people if isinstance((value := item.get("id")), int)
        }
        sender_id = message.get("sender_id")
        if isinstance(sender_id, int):
            user_ids.add(sender_id)
        user_ids.add(own_user_id)
        names = [
            value
            for item in people
            if isinstance((value := item.get("full_name")), str) and value
        ]
        peer_ids = sorted(user_ids - {own_user_id})
        return (
            "direct:" + ",".join(str(value) for value in sorted(user_ids)),
            ", ".join(names) or "Zulip direct message",
            "Direct messages",
            peer_ids[0] if len(peer_ids) == 1 else None,
        )


class WorkspaceRealtimeForwarder:
    """Forward Workspace message events directly to their Zulip account."""

    def __init__(self, store: V4Store, settings: Settings) -> None:
        self._store = store
        self._settings = settings

    async def apply(self, frame: Mapping[str, Any]) -> None:
        if frame.get("object_type") != "message":
            return
        payload = frame.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("Workspace message event omitted payload")
        if payload.get("source_name") == "zulip":
            return
        message_uuid = UUID(str(payload["uuid"]))
        if frame.get("action") == "deleted":
            await self._delete(message_uuid)
            return
        await self._upsert(frame, message_uuid, payload)

    async def _upsert(
        self,
        frame: Mapping[str, Any],
        message_uuid: UUID,
        payload: Mapping[str, Any],
    ) -> None:
        stream_uuid = UUID(str(payload["stream_uuid"]))
        topic_uuid = UUID(str(payload["topic_uuid"]))
        body = payload.get("payload")
        if not isinstance(body, Mapping) or not isinstance(body.get("content"), str):
            raise ValueError("Workspace message payload is not Markdown")
        content = body["content"]
        existing = await self._store.message_link(message_uuid)
        route = await self._store.stream_link(stream_uuid)
        if route is None:
            LOG.warning(
                "Skipping unmapped realtime Workspace stream stream_uuid=%s",
                stream_uuid,
            )
            return
        if route.account.queue_id is None:
            raise ZulipQueueUnavailableError(
                "an active Zulip event queue is required for delivery"
            )
        topic_name = await self._store.topic_name(topic_uuid)
        if topic_name is None:
            candidate = payload.get("topic_name")
            if isinstance(candidate, str) and candidate.strip():
                topic_name = candidate
            elif route.chat_key.startswith("direct:"):
                topic_name = "Direct messages"
            else:
                raise ValueError("Workspace message event omitted topic_name")
        topic = None if route.chat_key.startswith("direct:") else topic_name
        client = self._client(route.account)
        try:
            if existing is None or existing.zulip_message_id is None:
                await self._store.upsert_realtime_links(
                    route.account.uuid,
                    stream_uuid,
                    route.chat_key,
                    topic_uuid,
                    topic_name,
                    message_uuid,
                    None,
                )
                own_user_id = await asyncio.to_thread(client.own_user_id)
                zulip_message_id = await asyncio.to_thread(
                    client.send_message,
                    route.chat_key,
                    own_user_id,
                    content,
                    topic=topic,
                    queue_id=route.account.queue_id,
                    local_id=str(message_uuid),
                )
            else:
                await asyncio.to_thread(
                    client.update_message,
                    existing.zulip_message_id,
                    content=content,
                    topic=topic,
                )
                zulip_message_id = existing.zulip_message_id
        finally:
            client.close()
        await self._store.upsert_realtime_links(
            route.account.uuid,
            stream_uuid,
            route.chat_key,
            topic_uuid,
            topic_name,
            message_uuid,
            zulip_message_id,
        )

    async def _delete(self, message_uuid: UUID) -> None:
        link = await self._store.message_link(message_uuid)
        if link is None:
            return
        if link.zulip_message_id is None:
            await self._store.delete_message_link(message_uuid)
            return
        client = self._client(link.account)
        try:
            try:
                await asyncio.to_thread(
                    client.delete_message,
                    link.zulip_message_id,
                )
            except ZulipApiError as error:
                if error.status_code != 404:
                    raise
        finally:
            client.close()
        await self._store.delete_message_link(message_uuid)

    def _client(self, account: ExternalAccount) -> ZulipApiClient:
        return ZulipApiClient(
            account.endpoint,
            account.login,
            account.api_key,
            ca_file=self._settings.effective_zulip_ca_file,
            connect_timeout_seconds=self._settings.zulip_connect_timeout_seconds,
            default_longpoll_timeout_seconds=(
                self._settings.zulip_default_longpoll_timeout_seconds
            ),
            idle_queue_timeout_seconds=self._settings.zulip_idle_queue_timeout_seconds,
        )
