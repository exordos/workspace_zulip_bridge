# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import json
from pathlib import Path
from uuid import UUID

import pytest

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.models import WorkspaceEventCursor
from workspace_zulip_bridge.workspace_events import WorkspaceEventReceiver
from workspace_zulip_bridge.workspace_events import _cursor_url

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
            "WZB_WORKSPACE_PROJECT_ID": str(PROJECT_UUID),
            "WZB_WORKSPACE_PROVIDER_UUID": str(PROVIDER_UUID),
            "WZB_WORKSPACE_TOKEN_FILE": str(token),
            "WZB_WORKSPACE_EVENT_BATCH_SIZE": "1",
        }
    )
    store = FakeStore()
    return WorkspaceEventReceiver(store, settings), store  # type: ignore[arg-type]


def _receiver_with_batch_size(
    tmp_path: Path,
    batch_size: int,
) -> tuple[WorkspaceEventReceiver, FakeStore]:
    token = tmp_path / "workspace.token"
    token.write_text("header.payload.signature")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws",
            "WZB_WORKSPACE_PROJECT_ID": str(PROJECT_UUID),
            "WZB_WORKSPACE_PROVIDER_UUID": str(PROVIDER_UUID),
            "WZB_WORKSPACE_TOKEN_FILE": str(token),
            "WZB_WORKSPACE_EVENT_BATCH_SIZE": str(batch_size),
        }
    )
    store = FakeStore()
    return WorkspaceEventReceiver(store, settings), store  # type: ignore[arg-type]


def test_cursor_url_contains_only_resume_state() -> None:
    result = _cursor_url(
        "wss://workspace.example/events/ws?existing=yes",
        WorkspaceEventCursor(GENERATION, 91),
    )

    assert "existing=yes" in result
    assert "last_epoch_version=91" in result
    assert f"epoch_generation={GENERATION}" in result


def test_receiver_discards_event_payload_and_advances_only_cursor(
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


def test_receiver_flushes_cursor_when_socket_closes_between_batches(
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
