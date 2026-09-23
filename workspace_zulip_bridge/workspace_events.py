# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import json
import logging
import random
import ssl
import threading
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit
from uuid import UUID

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed
from websockets.typing import Subprotocol

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.database import open_pool
from workspace_zulip_bridge.models import WorkspaceEventCursor
from workspace_zulip_bridge.v4_store import V4Store
from workspace_zulip_bridge.workspace_auth import WorkspaceTokenManager

LOG = logging.getLogger(__name__)
WORKSPACE_EVENTS_PROTOCOL = "workspace.events.v1"


class WorkspaceCursorGapError(RuntimeError):
    def __init__(self, minimum_epoch_version: int = 0) -> None:
        super().__init__("workspace_cursor_expired")
        self.minimum_epoch_version = minimum_epoch_version


class WorkspaceEventReceiver:
    """Maintain the Workspace socket and discard frames after cursoring them."""

    def __init__(
        self,
        store: V4Store,
        settings: Settings,
        tokens: WorkspaceTokenManager | None = None,
    ) -> None:
        if not settings.workspace_events_enabled:
            raise ValueError("Workspace event receiver is not configured")
        assert settings.workspace_websocket_url is not None
        assert settings.workspace_project_id is not None
        assert settings.workspace_provider_uuid is not None
        self._store = store
        self._settings = settings
        self._url = settings.workspace_websocket_url
        self._project_uuid = settings.workspace_project_id
        self._provider_uuid = settings.workspace_provider_uuid
        self._tokens = tokens or WorkspaceTokenManager(settings)

    async def run(self) -> None:
        retry_seconds = self._settings.workspace_retry_base_seconds
        while True:
            cursor = await self._store.workspace_cursor(
                self._provider_uuid,
                self._project_uuid,
            )
            try:
                await self._receive(cursor)
                retry_seconds = self._settings.workspace_retry_base_seconds
            except asyncio.CancelledError:
                raise
            except WorkspaceCursorGapError as exc:
                await self._store.reset_workspace_cursor(
                    self._provider_uuid,
                    self._project_uuid,
                    max(0, exc.minimum_epoch_version - 1),
                )
            except Exception as exc:
                LOG.warning(
                    "Workspace event websocket reconnecting: error=%s",
                    type(exc).__name__,
                )
                await self._store.mark_workspace_disconnected(
                    self._provider_uuid,
                    type(exc).__name__,
                )
            delay = random.uniform(retry_seconds * 0.5, retry_seconds)
            await asyncio.sleep(delay)
            retry_seconds = min(
                retry_seconds * 2,
                self._settings.workspace_retry_cap_seconds,
            )

    async def _receive(self, cursor: WorkspaceEventCursor) -> None:
        token = await self._tokens.access_token()
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
            LOG.info(
                "Workspace event websocket connected at epoch %d",
                cursor.last_epoch_version,
            )
            try:
                await self._consume(websocket, cursor)
            except ConnectionClosed as exc:
                close_code = None if exc.rcvd is None else exc.rcvd.code
                if close_code == 4410:
                    raise WorkspaceCursorGapError from exc
                raise
            finally:
                await self._store.mark_workspace_disconnected(
                    self._provider_uuid,
                    None,
                )

    async def _consume(self, websocket: Any, cursor: WorkspaceEventCursor) -> None:
        generation = cursor.epoch_generation
        pending_version = cursor.last_epoch_version
        pending_count = 0
        try:
            while True:
                timeout = (
                    self._settings.workspace_event_flush_seconds
                    if pending_count
                    else None
                )
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except TimeoutError:
                    if generation is not None:
                        await self._store.advance_workspace_cursor(
                            self._provider_uuid,
                            self._project_uuid,
                            generation,
                            pending_version,
                        )
                    pending_count = 0
                    continue
                frame = _decode_frame(raw)
                if frame.get("error") == "epoch_pruned" or frame.get("code") == 410:
                    minimum = _nonnegative_int(
                        frame.get("minimum_epoch_version", 0),
                        "minimum_epoch_version",
                    )
                    raise WorkspaceCursorGapError(minimum)
                if frame.get("type") == "ready":
                    generation = UUID(str(frame["epoch_generation"]))
                    pending_version = _nonnegative_int(
                        frame["epoch_version"],
                        "epoch_version",
                    )
                    await self._store.advance_workspace_cursor(
                        self._provider_uuid,
                        self._project_uuid,
                        generation,
                        pending_version,
                    )
                    pending_count = 0
                    continue
                _validate_event_route(frame, self._project_uuid, self._provider_uuid)
                pending_version = max(
                    pending_version,
                    _nonnegative_int(frame["epoch_version"], "epoch_version"),
                )
                pending_count += 1
                if pending_count < self._settings.workspace_event_batch_size:
                    continue
                if generation is None:
                    raise ValueError("Workspace event arrived before ready frame")
                await self._store.advance_workspace_cursor(
                    self._provider_uuid,
                    self._project_uuid,
                    generation,
                    pending_version,
                )
                pending_count = 0
        finally:
            if pending_count:
                if generation is None:
                    raise ValueError("Workspace event arrived before ready frame")
                await self._store.advance_workspace_cursor(
                    self._provider_uuid,
                    self._project_uuid,
                    generation,
                    pending_version,
                )


class WorkspaceEventThread(threading.Thread):
    """Run the Workspace WebSocket on an event loop isolated in one OS thread."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(name="workspace-events", daemon=True)
        self._settings = settings
        self._state_lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_requested = threading.Event()
        self.error: BaseException | None = None

    def stop(self) -> None:
        self._stop_requested.set()
        with self._state_lock:
            loop = self._loop
            task = self._task
        if loop is not None and task is not None:
            loop.call_soon_threadsafe(task.cancel)

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        task = loop.create_task(self._run(), name="workspace-event-receiver")
        with self._state_lock:
            self._loop = loop
            self._task = task
        if self._stop_requested.is_set():
            task.cancel()
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
        except BaseException as exc:
            self.error = exc
            LOG.exception("Workspace event thread failed")
        finally:
            with self._state_lock:
                self._loop = None
                self._task = None
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    async def _run(self) -> None:
        pool = await open_pool(self._settings)
        try:
            receiver = WorkspaceEventReceiver(
                V4Store(pool),
                self._settings,
                WorkspaceTokenManager(self._settings),
            )
            await receiver.run()
        finally:
            await pool.close()


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


def _validate_event_route(
    frame: dict[str, Any],
    project_uuid: UUID,
    provider_uuid: UUID,
) -> None:
    if UUID(str(frame["project_id"])) != project_uuid:
        raise ValueError("Workspace event belongs to another project")
    if UUID(str(frame["user_uuid"])) != provider_uuid:
        raise ValueError("Workspace event belongs to another provider consumer")


def _nonnegative_int(value: object, field: str) -> int:
    result = int(str(value))
    if result < 0:
        raise ValueError(f"{field} must be non-negative")
    return result
