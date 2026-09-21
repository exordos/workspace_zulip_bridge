# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import json
from pathlib import Path
from uuid import UUID
from uuid import uuid4

import pytest

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.workspace_events import WorkspaceCursorGapError
from workspace_zulip_bridge.workspace_events import WorkspaceEventCursor
from workspace_zulip_bridge.workspace_events import WorkspaceEventReceiver
from workspace_zulip_bridge.workspace_events import _cursor_url
from workspace_zulip_bridge.workspace_events import _parse_event

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
        self.persisted: list[tuple[UUID | None, list[int]]] = []
        self.ready: list[tuple[UUID, int]] = []

    async def persist(
        self,
        provider_uuid: UUID,
        project_uuid: UUID,
        generation: UUID | None,
        events: list[object],
    ) -> int:
        assert provider_uuid == PROVIDER_UUID
        assert project_uuid == PROJECT_UUID
        versions = [event.epoch_version for event in events]  # type: ignore[attr-defined]
        self.persisted.append((generation, versions))
        return len(events)

    async def mark_ready(
        self,
        provider_uuid: UUID,
        project_uuid: UUID,
        generation: UUID,
        epoch_version: int,
    ) -> None:
        assert provider_uuid == PROVIDER_UUID
        assert project_uuid == PROJECT_UUID
        self.ready.append((generation, epoch_version))


class LeaseProbe(Exception):
    pass


class FakeLease:
    async def fetchval(self, query: str, provider_uuid: object) -> bool:
        assert "pg_try_advisory_lock" in query
        assert provider_uuid == str(PROVIDER_UUID)
        assert isinstance(provider_uuid, str)
        raise LeaseProbe


class FakeAcquire:
    async def __aenter__(self) -> FakeLease:
        return FakeLease()

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakePool:
    def acquire(self) -> FakeAcquire:
        return FakeAcquire()


def _frame(epoch_version: int) -> dict[str, object]:
    return {
        "schema_version": 1,
        "uuid": str(uuid4()),
        "epoch_version": epoch_version,
        "project_id": str(PROJECT_UUID),
        "user_uuid": str(PROVIDER_UUID),
        "object_type": "message",
        "action": "updated",
        "payload": {"kind": "message.updated", "uuid": str(uuid4())},
    }


def _receiver(tmp_path: Path, batch_size: int = 2) -> WorkspaceEventReceiver:
    token_file = tmp_path / "workspace.token"
    token_file.write_text("header.payload.signature")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws",
            "WZB_WORKSPACE_PROJECT_ID": str(PROJECT_UUID),
            "WZB_WORKSPACE_PROVIDER_UUID": str(PROVIDER_UUID),
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            "WZB_WORKSPACE_EVENT_BATCH_SIZE": str(batch_size),
        }
    )
    return WorkspaceEventReceiver(object(), settings)  # type: ignore[arg-type]


def test_cursor_url_preserves_query_and_adds_resume_state() -> None:
    cursor = WorkspaceEventCursor(GENERATION, 91, False, None)

    result = _cursor_url("wss://workspace.example/events/ws?existing=yes", cursor)

    assert "existing=yes" in result
    assert "last_epoch_version=91" in result
    assert f"epoch_generation={GENERATION}" in result


def test_provider_frames_are_batched_across_ready_boundary(tmp_path: Path) -> None:
    receiver = _receiver(tmp_path)
    store = FakeStore()
    receiver._store = store  # type: ignore[assignment]
    frames = [
        _frame(1),
        _frame(2),
        {
            "type": "ready",
            "epoch_generation": str(GENERATION),
            "epoch_version": 2,
        },
        _frame(5),
    ]

    with pytest.raises(StopAsyncIteration):
        asyncio.run(
            receiver._consume(
                FakeWebsocket(frames),
                WorkspaceEventCursor(None, 0, False, None),
            )
        )

    assert store.persisted == [(None, [1, 2]), (GENERATION, [5])]
    assert store.ready == [(GENERATION, 2)]


def test_advisory_lock_uses_a_text_parameter(tmp_path: Path) -> None:
    receiver = _receiver(tmp_path)
    receiver._pool = FakePool()  # type: ignore[assignment]

    with pytest.raises(LeaseProbe):
        asyncio.run(receiver.run())


def test_cursor_gap_is_terminal_and_preserves_prior_events(tmp_path: Path) -> None:
    receiver = _receiver(tmp_path, batch_size=100)
    store = FakeStore()
    receiver._store = store  # type: ignore[assignment]
    frames = [
        _frame(7),
        {
            "type": "EventsCursorExpiredError",
            "code": 410,
            "error": "epoch_pruned",
            "reason": "https://private.example/internal",
            "minimum_epoch_version": 9,
        },
    ]

    with pytest.raises(
        WorkspaceCursorGapError,
        match="workspace_cursor_expired:minimum_epoch_version=9",
    ) as error:
        asyncio.run(
            receiver._consume(
                FakeWebsocket(frames),
                WorkspaceEventCursor(GENERATION, 6, False, None),
            )
        )

    assert "private.example" not in str(error.value)
    assert store.persisted == [(GENERATION, [7])]


def test_event_validation_rejects_wrong_provider() -> None:
    frame = _frame(1)
    frame["user_uuid"] = str(uuid4())

    with pytest.raises(ValueError, match="another provider"):
        _parse_event(frame, PROJECT_UUID, PROVIDER_UUID)
