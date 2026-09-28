# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import json
from pathlib import Path
from uuid import UUID

import pytest

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.models import WorkspaceEventCursor
from workspace_zulip_bridge.realtime import ZulipQueueUnavailableError
from workspace_zulip_bridge.workspace_events import WorkspaceEventReceiver
from workspace_zulip_bridge.workspace_events import _cursor_url
from workspace_zulip_bridge.zulip_api import ZulipApiError

PROJECT_UUID = UUID("10000000-0000-0000-0000-000000000001")
PROVIDER_UUID = UUID("10000000-0000-0000-0000-000000000002")
GENERATION = UUID("10000000-0000-0000-0000-000000000003")


class FakeWebsocket:
    def __init__(self, frames: list[dict[str, object]]) -> None:
        self._frames = [json.dumps(frame) for frame in frames]

    async def recv(self) -> str:
        if not self._frames:
            raise StopAsyncIteration
        return self._frames.pop(0)


class FakeStore:
    def __init__(self) -> None:
        self.cursors: list[tuple[UUID, int]] = []
        self.events: list[dict[str, object]] = []

    async def apply(self, frame: dict[str, object]) -> None:
        self.events.append(frame)

    async def advance_workspace_cursor(
        self,
        provider_uuid: UUID,
        project_uuid: UUID,
        generation: UUID,
        version: int,
    ) -> None:
        assert provider_uuid == PROVIDER_UUID
        assert project_uuid == PROJECT_UUID
        self.cursors.append((generation, version))


def _receiver(tmp_path: Path) -> tuple[WorkspaceEventReceiver, FakeStore]:
    token = tmp_path / "workspace.token"
    token.write_text("header.payload.signature")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws",
            "WZB_WORKSPACE_API_URL": "https://workspace.example/api",
            "WZB_WORKSPACE_PROJECT_ID": str(PROJECT_UUID),
            "WZB_WORKSPACE_PROVIDER_UUID": str(PROVIDER_UUID),
            "WZB_WORKSPACE_TOKEN_FILE": str(token),
            "WZB_WORKSPACE_EVENT_BATCH_SIZE": "1",
        }
    )
    store = FakeStore()
    return (
        WorkspaceEventReceiver(  # type: ignore[arg-type]
            store,
            settings,
            store.apply,  # type: ignore[arg-type]
        ),
        store,
    )


def _receiver_with_batch_size(
    tmp_path: Path,
    batch_size: int,
) -> tuple[WorkspaceEventReceiver, FakeStore]:
    token = tmp_path / "workspace.token"
    token.write_text("header.payload.signature")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws",
            "WZB_WORKSPACE_API_URL": "https://workspace.example/api",
            "WZB_WORKSPACE_PROJECT_ID": str(PROJECT_UUID),
            "WZB_WORKSPACE_PROVIDER_UUID": str(PROVIDER_UUID),
            "WZB_WORKSPACE_TOKEN_FILE": str(token),
            "WZB_WORKSPACE_EVENT_BATCH_SIZE": str(batch_size),
        }
    )
    store = FakeStore()
    return (
        WorkspaceEventReceiver(  # type: ignore[arg-type]
            store,
            settings,
            store.apply,  # type: ignore[arg-type]
        ),
        store,
    )


def test_cursor_url_contains_only_resume_state() -> None:
    result = _cursor_url(
        "wss://workspace.example/events/ws?existing=yes",
        WorkspaceEventCursor(GENERATION, 91),
    )

    assert "existing=yes" in result
    assert "last_epoch_version=91" in result
    assert f"epoch_generation={GENERATION}" in result


def test_receiver_applies_event_before_advancing_cursor(
    tmp_path: Path,
) -> None:
    receiver, store = _receiver(tmp_path)
    frames = [
        {
            "type": "ready",
            "epoch_generation": str(GENERATION),
            "epoch_version": 3,
        },
        {
            "epoch_version": 4,
            "project_id": str(PROJECT_UUID),
            "user_uuid": str(PROVIDER_UUID),
            "payload": {"secret": "must-not-be-stored"},
        },
    ]

    with pytest.raises(StopAsyncIteration):
        asyncio.run(
            receiver._consume(
                FakeWebsocket(frames),
                WorkspaceEventCursor(None, 0),
            )
        )

    assert store.cursors == [(GENERATION, 3), (GENERATION, 4)]
    assert store.events == [frames[1]]


def test_receiver_advances_each_realtime_event_without_batching(
    tmp_path: Path,
) -> None:
    receiver, store = _receiver_with_batch_size(tmp_path, 100)
    frames = [
        {
            "type": "ready",
            "epoch_generation": str(GENERATION),
            "epoch_version": 3,
        },
        {
            "epoch_version": 4,
            "project_id": str(PROJECT_UUID),
            "user_uuid": str(PROVIDER_UUID),
            "payload": {"secret": "must-not-be-stored"},
        },
    ]

    with pytest.raises(StopAsyncIteration):
        asyncio.run(
            receiver._consume(
                FakeWebsocket(frames),
                WorkspaceEventCursor(None, 0),
            )
        )

    assert store.cursors == [(GENERATION, 3), (GENERATION, 4)]
    assert store.events == [frames[1]]


def test_receiver_skips_a_permanent_failure_without_blocking_later_events(
    tmp_path: Path,
) -> None:
    receiver, store = _receiver(tmp_path)
    frames = [
        {
            "type": "ready",
            "epoch_generation": str(GENERATION),
            "epoch_version": 3,
        },
        {
            "epoch_version": 4,
            "project_id": str(PROJECT_UUID),
            "user_uuid": str(PROVIDER_UUID),
            "payload": {"content": "invalid"},
        },
        {
            "epoch_version": 5,
            "project_id": str(PROJECT_UUID),
            "user_uuid": str(PROVIDER_UUID),
            "payload": {"content": "valid"},
        },
    ]
    handled: list[int] = []

    async def handle(frame: dict[str, object]) -> None:
        version = int(frame["epoch_version"])
        handled.append(version)
        if version == 4:
            raise ValueError("permanent event failure")

    receiver._event_handler = handle  # type: ignore[assignment]

    with pytest.raises(StopAsyncIteration):
        asyncio.run(
            receiver._consume(
                FakeWebsocket(frames),
                WorkspaceEventCursor(None, 0),
            )
        )

    assert handled == [4, 5]
    assert store.cursors == [(GENERATION, 3), (GENERATION, 4), (GENERATION, 5)]


@pytest.mark.parametrize(
    "error",
    [
        ZulipApiError("RATE_LIMIT_HIT", retryable=True, status_code=429),
        ZulipQueueUnavailableError("queue unavailable"),
    ],
)
def test_receiver_replays_a_retryable_forwarder_failure(
    tmp_path: Path,
    error: Exception,
) -> None:
    receiver, store = _receiver(tmp_path)
    frames = [
        {
            "type": "ready",
            "epoch_generation": str(GENERATION),
            "epoch_version": 3,
        },
        {
            "epoch_version": 4,
            "project_id": str(PROJECT_UUID),
            "user_uuid": str(PROVIDER_UUID),
            "payload": {"content": "retry"},
        },
    ]

    async def handle(_frame: dict[str, object]) -> None:
        raise error

    receiver._event_handler = handle  # type: ignore[assignment]

    with pytest.raises(type(error), match=str(error)):
        asyncio.run(
            receiver._consume(
                FakeWebsocket(frames),
                WorkspaceEventCursor(None, 0),
            )
        )

    assert store.cursors == [(GENERATION, 3)]
