# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from pathlib import Path

import workspace_zulip_bridge.service as service_module
from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.service import BridgeService


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
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "ZulipThreadSupervisor", FakeSupervisor
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "ZulipEventProcessor", FakeEventProcessor
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        service_module, "WorkspaceEventReceiver", FakeWorkspaceEventReceiver
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
    assert pool.closed
