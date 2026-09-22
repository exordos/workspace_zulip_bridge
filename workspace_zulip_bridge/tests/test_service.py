# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

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

    def __init__(
        self, pool: object, settings: Settings, tokens: object | None = None
    ) -> None:
        self.calls.append("workspace-receiver-init")

    async def run(self) -> None:
        self.calls.append("workspace-receiver-run")
        await asyncio.Future()


class FakeWorkspaceBootstrapper:
    calls: list[str]

    def __init__(
        self, pool: object, settings: Settings, tokens: object | None = None
    ) -> None:
        self.calls.append("workspace-bootstrap-init")

    async def ensure(self) -> bool:
        self.calls.append("workspace-bootstrap-ensure")
        return True


class FakeWorkspaceWorker:
    calls: list[str]
    label = "workspace-worker"

    def __init__(
        self,
        pool: object,
        settings: Settings,
        *,
        plan_enabled: bool = True,
        partition: int = 0,
        partition_count: int = 1,
        tokens: object | None = None,
    ) -> None:
        self.calls.append(
            f"{self.label}-init-{plan_enabled}-{partition}/{partition_count}"
        )

    async def run(self) -> None:
        self.calls.append(f"{self.label}-run")
        await asyncio.Future()


class FakeWorkspaceEventProcessor(FakeWorkspaceWorker):
    label = "workspace-event-processor"


class FakeWorkspaceDiffWorker(FakeWorkspaceWorker):
    label = "workspace-diff-worker"


def test_workspace_bootstrap_retries_transient_failure() -> None:
    asyncio.run(_run_workspace_bootstrap_retry_test())


async def _run_workspace_bootstrap_retry_test() -> None:
    class FlakyBootstrapper:
        def __init__(self) -> None:
            self.calls = 0

        async def ensure(self) -> bool:
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("temporary Workspace outage")
            return True

    settings = Settings.from_env({"WZB_WORKSPACE_LEASE_RETRY_SECONDS": "0.01"})
    bootstrapper = FlakyBootstrapper()

    assert await BridgeService(settings)._ensure_bootstrap(
        bootstrapper,
        asyncio.Event(),
    )
    assert bootstrapper.calls == 2


def test_workspace_bootstrap_retry_stops_cleanly() -> None:
    asyncio.run(_run_workspace_bootstrap_stop_test())


async def _run_workspace_bootstrap_stop_test() -> None:
    stop = asyncio.Event()

    class StoppingBootstrapper:
        async def ensure(self) -> bool:
            stop.set()
            raise TimeoutError("Workspace is stopping")

    assert not await BridgeService(Settings.from_env({}))._ensure_bootstrap(
        StoppingBootstrapper(),
        stop,
    )


def test_workspace_bootstrap_in_progress_stops_cleanly() -> None:
    asyncio.run(_run_workspace_bootstrap_in_progress_stop_test())


async def _run_workspace_bootstrap_in_progress_stop_test() -> None:
    stop = asyncio.Event()
    started = asyncio.Event()

    class BlockingBootstrapper:
        cancelled = False

        async def ensure(self) -> bool:
            started.set()
            try:
                future: asyncio.Future[bool] = asyncio.Future()
                return await future
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    bootstrapper = BlockingBootstrapper()
    task = asyncio.create_task(
        BridgeService(Settings.from_env({}))._ensure_bootstrap(
            bootstrapper,
            stop,
        )
    )
    await started.wait()
    stop.set()

    assert not await asyncio.wait_for(task, timeout=0.1)
    assert bootstrapper.cancelled


def test_workspace_diff_worker_plans_once_before_draining(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    asyncio.run(_run_workspace_diff_worker_drain_test(monkeypatch, tmp_path))


def test_workspace_diff_worker_rejects_invalid_partition(tmp_path: Path) -> None:
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

    with pytest.raises(ValueError, match="partition"):
        WorkspaceDiffWorker(
            object(),  # type: ignore[arg-type]
            settings,
            partition=2,
            partition_count=2,
        )


def test_workspace_diff_worker_plans_every_entity_without_starvation(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    asyncio.run(_run_workspace_diff_worker_fair_plan_test(monkeypatch, tmp_path))


async def _run_workspace_diff_worker_fair_plan_test(
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

    class PlanningPool:
        async def fetchrow(self, query: str, *args: object) -> dict[str, object]:
            assert "active_generation" in query
            return {
                "active_generation": UUID("20000000-0000-0000-0000-000000000001"),
                "reconciliation_version": 2,
            }

    worker = WorkspaceDiffWorker(PlanningPool(), settings)  # type: ignore[arg-type]
    calls: list[str] = []

    async def fake_link_realm() -> UUID:
        return UUID("30000000-0000-0000-0000-000000000001")

    async def fake_ensure_direct_topics(realm_uuid: UUID) -> None:
        assert realm_uuid == UUID("30000000-0000-0000-0000-000000000001")

    async def fake_ensure_topic_bindings(realm_uuid: UUID) -> None:
        assert realm_uuid == UUID("30000000-0000-0000-0000-000000000001")

    async def fake_plan_entity(
        entity_type: str,
        source: object,
        realm_uuid: UUID,
        generation: UUID,
    ) -> int:
        del source, realm_uuid, generation
        calls.append(entity_type)
        return 1 if entity_type == "users" else 0

    async def fake_repair(realm_uuid: UUID, generation: UUID) -> int:
        del realm_uuid, generation
        return 0

    monkeypatch.setattr(worker, "_link_realm", fake_link_realm)
    monkeypatch.setattr(worker, "_ensure_direct_topics", fake_ensure_direct_topics)
    monkeypatch.setattr(worker, "_ensure_topic_bindings", fake_ensure_topic_bindings)
    monkeypatch.setattr(worker, "_plan_entity", fake_plan_entity)
    monkeypatch.setattr(worker, "_repair_missing_source_diffs", fake_repair)

    assert await worker.plan() == 1
    assert calls == [
        "users",
        "streams",
        "stream_bindings",
        "topics",
        "topic_bindings",
        "messages",
        "message_flags",
        "message_reactions",
    ]


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

    async def fake_plan() -> int:
        calls.append("plan")
        return 200

    async def fake_process_once(client: object) -> int:
        calls.append("process")
        return 100

    async def fake_complete() -> bool:
        calls.append("complete")
        return True

    monkeypatch.setattr(worker, "plan", fake_plan)
    monkeypatch.setattr(worker, "process_once", fake_process_once)
    monkeypatch.setattr(worker, "_complete_initial_sync", fake_complete)

    assert await worker._plan_and_drain(object()) == 100  # type: ignore[arg-type]
    assert calls == ["plan", "process"]


def test_workspace_diff_worker_completes_only_after_empty_plan(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    asyncio.run(_run_workspace_diff_worker_completion_test(monkeypatch, tmp_path))


async def _run_workspace_diff_worker_completion_test(
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

    async def fake_plan() -> int:
        calls.append("plan")
        return 0

    async def fake_process_once(client: object) -> int:
        calls.append("process")
        return 0

    async def fake_complete() -> bool:
        calls.append("complete")
        return True

    monkeypatch.setattr(worker, "plan", fake_plan)
    monkeypatch.setattr(worker, "process_once", fake_process_once)
    monkeypatch.setattr(worker, "_complete_initial_sync", fake_complete)

    assert await worker._plan_and_drain(object()) == 0  # type: ignore[arg-type]
    assert calls == ["plan", "process", "complete"]


def test_workspace_diff_worker_waits_for_control_realm(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    asyncio.run(_run_workspace_diff_worker_realm_wait_test(monkeypatch, tmp_path))


def test_workspace_diff_worker_accepts_missing_control_realm(tmp_path: Path) -> None:
    asyncio.run(_run_workspace_diff_worker_missing_realm_test(tmp_path))


async def _run_workspace_diff_worker_missing_realm_test(tmp_path: Path) -> None:
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

    class EmptyRealmPool:
        async def fetch(self, query: str, *args: object) -> list[object]:
            assert "zulip_realms" in query
            return []

    worker = WorkspaceDiffWorker(EmptyRealmPool(), settings)  # type: ignore[arg-type]

    assert await worker._link_realm() is None


async def _run_workspace_diff_worker_realm_wait_test(
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

    async def fake_plan() -> None:
        calls.append("plan")
        return None

    async def fail_process_once(client: object) -> int:
        raise AssertionError("diff processing started before realm readiness")

    async def fail_complete() -> bool:
        raise AssertionError("initial sync completed before realm readiness")

    monkeypatch.setattr(worker, "plan", fake_plan)
    monkeypatch.setattr(worker, "process_once", fail_process_once)
    monkeypatch.setattr(worker, "_complete_initial_sync", fail_complete)

    assert await worker._plan_and_drain(object()) == 0  # type: ignore[arg-type]
    assert calls == ["plan"]


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
    assert calls.count("workspace-diff-worker-run") == 2
    assert "workspace-diff-worker-init-True-0/2" in calls
    assert "workspace-diff-worker-init-False-1/2" in calls
    assert pool.closed
