# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import json
import logging
import random
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit
from uuid import UUID

import asyncpg
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed
from websockets.typing import Subprotocol

from workspace_zulip_bridge.config import Settings

LOG = logging.getLogger(__name__)
WORKSPACE_EVENTS_PROTOCOL = "workspace.events.v1"


class WorkspaceCursorGapError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class WorkspaceEventCursor:
    epoch_generation: UUID | None
    last_epoch_version: int
    recovery_required: bool
    recovery_reason: str | None


@dataclass(frozen=True, slots=True)
class WorkspaceEvent:
    uuid: UUID
    epoch_version: int
    object_type: str
    action: str
    entity_uuid: UUID | None
    frame: dict[str, Any]


class WorkspaceEventStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def cursor(
        self,
        provider_uuid: UUID,
        project_uuid: UUID,
    ) -> WorkspaceEventCursor:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_event_cursors (
                    provider_uuid, workspace_project_id
                ) VALUES ($1, $2)
                ON CONFLICT (provider_uuid) DO NOTHING
                """,
                provider_uuid,
                project_uuid,
            )
            row = await connection.fetchrow(
                """
                SELECT workspace_project_id, epoch_generation,
                       last_epoch_version, recovery_required, recovery_reason
                FROM workspace_zulip_bridge.workspace_event_cursors
                WHERE provider_uuid = $1
                """,
                provider_uuid,
            )
        if row is None or row["workspace_project_id"] != project_uuid:
            raise RuntimeError("Workspace provider cursor belongs to another project")
        return WorkspaceEventCursor(
            epoch_generation=row["epoch_generation"],
            last_epoch_version=row["last_epoch_version"],
            recovery_required=row["recovery_required"],
            recovery_reason=row["recovery_reason"],
        )

    async def persist(
        self,
        provider_uuid: UUID,
        project_uuid: UUID,
        epoch_generation: UUID | None,
        events: list[WorkspaceEvent],
    ) -> int:
        if not events:
            return 0
        async with self._pool.acquire() as connection, connection.transaction():
            inserted = await connection.fetchval(
                """
                WITH input AS (
                    SELECT *
                    FROM unnest(
                        $1::uuid[], $2::bigint[], $3::text[], $4::text[],
                        $5::uuid[], $6::text[]
                    ) AS item(
                        uuid, epoch_version, object_type, action,
                        entity_uuid, payload
                    )
                ), inserted AS (
                    INSERT INTO workspace_zulip_bridge.workspace_events (
                        uuid, provider_uuid, workspace_project_id,
                        epoch_generation, epoch_version, object_type, action,
                        entity_uuid, payload
                    )
                    SELECT uuid, $7, $8, $9, epoch_version, object_type,
                           action, entity_uuid, payload::jsonb
                    FROM input
                    ON CONFLICT (uuid) DO NOTHING
                    RETURNING 1
                )
                SELECT count(*) FROM inserted
                """,
                [event.uuid for event in events],
                [event.epoch_version for event in events],
                [event.object_type for event in events],
                [event.action for event in events],
                [event.entity_uuid for event in events],
                [
                    json.dumps(event.frame, separators=(",", ":"), sort_keys=True)
                    for event in events
                ],
                provider_uuid,
                project_uuid,
                epoch_generation,
            )
            if epoch_generation is not None:
                await connection.execute(
                    """
                    UPDATE workspace_zulip_bridge.workspace_event_cursors
                    SET epoch_generation = $3,
                        last_epoch_version = GREATEST(last_epoch_version, $4),
                        updated_at = clock_timestamp()
                    WHERE provider_uuid = $1 AND workspace_project_id = $2
                    """,
                    provider_uuid,
                    project_uuid,
                    epoch_generation,
                    max(event.epoch_version for event in events),
                )
        return int(inserted or 0)

    async def mark_ready(
        self,
        provider_uuid: UUID,
        project_uuid: UUID,
        epoch_generation: UUID,
        epoch_version: int,
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_events
                SET epoch_generation = $3,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1 AND workspace_project_id = $2
                  AND epoch_generation IS NULL
                  AND epoch_version <= $4
                """,
                provider_uuid,
                project_uuid,
                epoch_generation,
                epoch_version,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_event_cursors
                SET epoch_generation = $3,
                    last_epoch_version = GREATEST(last_epoch_version, $4),
                    recovery_required = false,
                    recovery_reason = NULL,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1 AND workspace_project_id = $2
                """,
                provider_uuid,
                project_uuid,
                epoch_generation,
                epoch_version,
            )

    async def mark_connected(self, provider_uuid: UUID) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_event_cursors
            SET connected_at = clock_timestamp(),
                disconnected_at = NULL,
                updated_at = clock_timestamp()
            WHERE provider_uuid = $1
            """,
            provider_uuid,
        )

    async def mark_disconnected(self, provider_uuid: UUID) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_event_cursors
            SET disconnected_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE provider_uuid = $1
            """,
            provider_uuid,
        )

    async def mark_recovery_required(
        self,
        provider_uuid: UUID,
        reason: str,
    ) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_event_cursors
            SET recovery_required = true,
                recovery_reason = $2,
                disconnected_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE provider_uuid = $1
            """,
            provider_uuid,
            reason[:2048],
        )


class WorkspaceEventReceiver:
    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        if not settings.workspace_events_enabled:
            raise ValueError("Workspace event receiver is not configured")
        assert settings.workspace_websocket_url is not None
        assert settings.workspace_project_id is not None
        assert settings.workspace_provider_uuid is not None
        assert settings.workspace_token_file is not None
        self._pool = pool
        self._settings = settings
        self._url = settings.workspace_websocket_url
        self._project_uuid = settings.workspace_project_id
        self._provider_uuid = settings.workspace_provider_uuid
        self._token_file = settings.workspace_token_file
        self._store = WorkspaceEventStore(pool)

    async def run(self) -> None:
        while True:
            async with self._pool.acquire() as lease:
                acquired = await lease.fetchval(
                    """
                    SELECT pg_try_advisory_lock(
                        hashtextextended('workspace_zulip_bridge:workspace-events:'
                            || $1::text, 0)
                    )
                    """,
                    str(self._provider_uuid),
                )
                if acquired:
                    LOG.info("Workspace event receiver lease acquired")
                    try:
                        await self._run_leader()
                    finally:
                        await lease.execute(
                            """
                            SELECT pg_advisory_unlock(
                                hashtextextended(
                                    'workspace_zulip_bridge:workspace-events:'
                                    || $1::text,
                                    0
                                )
                            )
                            """,
                            str(self._provider_uuid),
                        )
            await asyncio.sleep(self._settings.workspace_lease_retry_seconds)

    async def _run_leader(self) -> None:
        retry_seconds = self._settings.workspace_retry_base_seconds
        while True:
            cursor = await self._store.cursor(
                self._provider_uuid,
                self._project_uuid,
            )
            if cursor.recovery_required:
                LOG.error(
                    "Workspace event cursor requires snapshot recovery: %s",
                    cursor.recovery_reason,
                )
                await asyncio.sleep(self._settings.workspace_lease_retry_seconds)
                continue
            try:
                await self._receive(cursor)
                retry_seconds = self._settings.workspace_retry_base_seconds
            except asyncio.CancelledError:
                raise
            except WorkspaceCursorGapError as exc:
                await self._store.mark_recovery_required(
                    self._provider_uuid,
                    str(exc),
                )
            except Exception:
                LOG.exception("Workspace event websocket disconnected")
                await self._store.mark_disconnected(self._provider_uuid)
                delay = random.uniform(retry_seconds * 0.5, retry_seconds)
                await asyncio.sleep(delay)
                retry_seconds = min(
                    retry_seconds * 2,
                    self._settings.workspace_retry_cap_seconds,
                )

    async def _receive(self, cursor: WorkspaceEventCursor) -> None:
        token = await asyncio.to_thread(_read_token, self._token_file)
        url = _cursor_url(self._url, cursor)
        ssl_context = _ssl_context(self._url, self._settings.workspace_ca_file)
        async with connect(
            url,
            subprotocols=[
                Subprotocol(WORKSPACE_EVENTS_PROTOCOL),
                Subprotocol(f"bearer.{token}"),
            ],
            open_timeout=self._settings.zulip_connect_timeout_seconds,
            ping_interval=30,
            ping_timeout=30,
            max_queue=self._settings.workspace_event_batch_size * 2,
            ssl=ssl_context,
            proxy=None,
        ) as websocket:
            if websocket.subprotocol != WORKSPACE_EVENTS_PROTOCOL:
                raise RuntimeError("Workspace websocket subprotocol was not selected")
            await self._store.mark_connected(self._provider_uuid)
            LOG.info(
                "Workspace event websocket connected at epoch %d",
                cursor.last_epoch_version,
            )
            try:
                await self._consume(websocket, cursor)
            except ConnectionClosed as exc:
                close_code = None if exc.rcvd is None else exc.rcvd.code
                close_reason = None if exc.rcvd is None else exc.rcvd.reason
                if close_code == 4410:
                    raise WorkspaceCursorGapError(
                        f"Workspace event cursor expired: {close_reason}"
                    ) from exc
                raise
            finally:
                await self._store.mark_disconnected(self._provider_uuid)

    async def _consume(
        self,
        websocket: Any,
        cursor: WorkspaceEventCursor,
    ) -> None:
        generation = cursor.epoch_generation
        batch: list[WorkspaceEvent] = []
        try:
            while True:
                timeout = (
                    self._settings.workspace_event_flush_seconds if batch else None
                )
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except TimeoutError:
                    await self._flush(batch, generation)
                    batch.clear()
                    continue
                frame = _decode_frame(raw)
                if frame.get("error") == "epoch_pruned" or frame.get("code") == 410:
                    await self._flush(batch, generation)
                    batch.clear()
                    raise WorkspaceCursorGapError(
                        f"{frame.get('reason', 'epoch_pruned')}: "
                        f"minimum={frame.get('minimum_epoch_version')}"
                    )
                if frame.get("type") == "ready":
                    await self._flush(batch, generation)
                    batch.clear()
                    generation = UUID(str(frame["epoch_generation"]))
                    await self._store.mark_ready(
                        self._provider_uuid,
                        self._project_uuid,
                        generation,
                        _nonnegative_int(frame["epoch_version"], "epoch_version"),
                    )
                    continue
                batch.append(
                    _parse_event(frame, self._project_uuid, self._provider_uuid)
                )
                if len(batch) >= self._settings.workspace_event_batch_size:
                    await self._flush(batch, generation)
                    batch.clear()
        finally:
            if batch:
                await self._flush(batch, generation)

    async def _flush(
        self,
        batch: list[WorkspaceEvent],
        generation: UUID | None,
    ) -> None:
        if not batch:
            return
        await self._store.persist(
            self._provider_uuid,
            self._project_uuid,
            generation,
            batch,
        )


def _read_token(path: Path) -> str:
    token = path.read_text(encoding="utf-8").strip()
    if not token or any(character.isspace() for character in token):
        raise ValueError("Workspace token file must contain one bearer token")
    return token


def _cursor_url(url: str, cursor: WorkspaceEventCursor) -> str:
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["last_epoch_version"] = str(cursor.last_epoch_version)
    if cursor.epoch_generation is None:
        query.pop("epoch_generation", None)
    else:
        query["epoch_generation"] = str(cursor.epoch_generation)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), ""))


def _ssl_context(url: str, ca_file: Path | None) -> ssl.SSLContext | None:
    if urlsplit(url).scheme != "wss":
        return None
    return ssl.create_default_context(cafile=None if ca_file is None else str(ca_file))


def _decode_frame(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    frame = json.loads(raw)
    if not isinstance(frame, dict):
        raise ValueError("Workspace websocket frame must be an object")
    return frame


def _nonnegative_int(value: object, field: str) -> int:
    result = int(str(value))
    if result < 0:
        raise ValueError(f"{field} must be non-negative")
    return result


def _parse_event(
    frame: dict[str, Any],
    project_uuid: UUID,
    provider_uuid: UUID,
) -> WorkspaceEvent:
    if UUID(str(frame["project_id"])) != project_uuid:
        raise ValueError("Workspace event belongs to another project")
    if UUID(str(frame["user_uuid"])) != provider_uuid:
        raise ValueError("Workspace event belongs to another provider consumer")
    payload = frame.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("Workspace event payload must be an object")
    entity_uuid = None
    try:
        entity_uuid = UUID(str(payload["uuid"]))
    except (KeyError, TypeError, ValueError):
        pass
    return WorkspaceEvent(
        uuid=UUID(str(frame["uuid"])),
        epoch_version=_nonnegative_int(frame["epoch_version"], "epoch_version"),
        object_type=str(frame["object_type"]),
        action=str(frame["action"]),
        entity_uuid=entity_uuid,
        frame=frame,
    )
