# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from pathlib import Path
from typing import Any

import workspace_zulip_bridge.service as service_module
from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.service import BridgeService
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker


class FakePool:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class FakeSupervisor:
    stop: asyncio.Event
    calls: list[str]

    def __init__(self, store: object, loop: object, settings: Settings) -> None:
        self.calls.append("supervisor-init")

    async def run(self) -> None:
        self.calls.append("supervisor-run")
        self.stop.set()
        await asyncio.Future()


class FakeEventProcessor:
    calls: list[str]

    def __init__(self, pool: object, store: object, settings: Settings) -> None:
        self.calls.append("event-processor-init")

    async def run(self) -> None:
        self.calls.append("event-processor-run")
        await asyncio.Future()


class FakeWorkspaceEventReceiver:
    calls: list[str]

    def __init__(self, pool: object, settings: Settings) -> None:
        self.calls.append("workspace-receiver-init")

    async def run(self) -> None:
        self.calls.append("workspace-receiver-run")
        await asyncio.Future()


class FakeWorkspaceBootstrapper:
    calls: list[str]

    def __init__(self, pool: object, settings: Settings) -> None:
        self.calls.append("workspace-bootstrap-init")

    async def ensure(self) -> bool:
        self.calls.append("workspace-bootstrap-ensure")
        return True


class FakeWorkspaceWorker:
    calls: list[str]
    label = "workspace-worker"

    def __init__(self, pool: object, settings: Settings) -> None:
        self.calls.append(f"{self.label}-init")

    async def run(self) -> None:
        self.calls.append(f"{self.label}-run")
        await asyncio.Future()


class FakeWorkspaceEventProcessor(FakeWorkspaceWorker):
    label = "workspace-event-processor"


class FakeWorkspaceDiffWorker(FakeWorkspaceWorker):
    label = "workspace-diff-worker"


def test_workspace_diff_worker_plans_once_before_draining(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    asyncio.run(_run_workspace_diff_worker_drain_test(monkeypatch, tmp_path))


async def _run_workspace_diff_worker_drain_test(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "workspace.token"
    token_file.write_text("token")
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws",
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )
    worker = WorkspaceDiffWorker(object(), settings)  # type: ignore[arg-type]
    calls: list[str] = []
    batches = iter((100, 100, 0))

    async def fake_plan() -> int:
        calls.append("plan")
        return 200

    async def fake_process_once(client: object) -> int:
        calls.append("process")
        return next(batches)

    monkeypatch.setattr(worker, "plan", fake_plan)
    monkeypatch.setattr(worker, "process_once", fake_process_once)

    assert await worker._plan_and_drain(object()) == 200  # type: ignore[arg-type]
    assert calls == ["plan", "process", "process", "process"]


def test_daemon_prepares_probes_and_closes_database(monkeypatch: object) -> None:
    asyncio.run(_run_daemon_lifecycle_test(monkeypatch))


async def _run_daemon_lifecycle_test(monkeypatch: object) -> None:
    calls: list[str] = []
    pool = FakePool()
    stop = asyncio.Event()

    async def fake_open_pool(settings: Settings) -> FakePool:
        calls.append("open")
        return pool

    async def fake_prepare_database(candidate: FakePool) -> None:
        assert candidate is pool
        calls.append("prepare")

    async def fake_probe_database(candidate: FakePool) -> None:
        assert candidate is pool
        calls.append("probe")

    monkeypatch.setattr(service_module, "open_pool", fake_open_pool)  # type: ignore[attr-defined]
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "prepare_database", fake_prepare_database
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "probe_database", fake_probe_database
    )
    FakeSupervisor.stop = stop
    FakeSupervisor.calls = calls
    FakeEventProcessor.calls = calls
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "ZulipThreadSupervisor", FakeSupervisor
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "ZulipEventProcessor", FakeEventProcessor
    )

    await BridgeService(Settings.from_env({})).run(stop)

    assert calls == [
        "open",
        "prepare",
        "probe",
        "supervisor-init",
        "event-processor-init",
        "supervisor-run",
        "event-processor-run",
    ]
    assert pool.closed


def test_daemon_starts_workspace_receiver_when_configured(
    monkeypatch: object,
    tmp_path: Path,
) -> None:
    asyncio.run(_run_workspace_receiver_test(monkeypatch, tmp_path))


async def _run_workspace_receiver_test(
    monkeypatch: object,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    pool = FakePool()
    stop = asyncio.Event()
    token_file = tmp_path / "workspace.token"
    token_file.write_text("token")

    async def fake_open_pool(settings: Settings) -> FakePool:
        return pool

    async def fake_prepare_database(candidate: FakePool) -> None:
        return None

    async def fake_probe_database(candidate: FakePool) -> None:
        return None

    monkeypatch.setattr(service_module, "open_pool", fake_open_pool)  # type: ignore[attr-defined]
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "prepare_database", fake_prepare_database
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "probe_database", fake_probe_database
    )
    FakeSupervisor.stop = stop
    FakeSupervisor.calls = calls
    FakeEventProcessor.calls = calls
    FakeWorkspaceEventReceiver.calls = calls
    FakeWorkspaceBootstrapper.calls = calls
    FakeWorkspaceEventProcessor.calls = calls
    FakeWorkspaceDiffWorker.calls = calls
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "ZulipThreadSupervisor", FakeSupervisor
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "ZulipEventProcessor", FakeEventProcessor
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "WorkspaceEventReceiver", FakeWorkspaceEventReceiver
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "WorkspaceBootstrapper", FakeWorkspaceBootstrapper
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "WorkspaceEventProcessor", FakeWorkspaceEventProcessor
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "WorkspaceDiffWorker", FakeWorkspaceDiffWorker
    )
    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws",
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
        }
    )

    await BridgeService(settings).run(stop)

    assert "workspace-receiver-init" in calls
    assert "workspace-receiver-run" in calls
    assert "workspace-bootstrap-ensure" in calls
    assert "workspace-event-processor-run" in calls
    assert "workspace-diff-worker-run" in calls
    assert pool.closed
