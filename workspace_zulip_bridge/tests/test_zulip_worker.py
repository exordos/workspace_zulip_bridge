# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import threading
from uuid import UUID

import pytest

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.models import ExternalAccount
from workspace_zulip_bridge.models import RegisteredQueue
from workspace_zulip_bridge.zulip_api import ZulipApiError
from workspace_zulip_bridge.zulip_worker import ZulipEventThread
from workspace_zulip_bridge.zulip_worker import ZulipThreadSupervisor

ACCOUNT_UUID = UUID("10000000-0000-0000-0000-000000000001")
OWNER_UUID = UUID("10000000-0000-0000-0000-000000000002")
PROJECT_UUID = UUID("10000000-0000-0000-0000-000000000003")


class FakeStore:
    def __init__(self) -> None:
        self.queues: list[tuple[str, int]] = []
        self.cursors: list[tuple[str, int]] = []
        self.cleared: list[str] = []

    async def set_zulip_queue(
        self, account_uuid: UUID, queue_id: str, event_id: int
    ) -> bool:
        assert account_uuid == ACCOUNT_UUID
        self.queues.append((queue_id, event_id))
        return True

    async def advance_zulip_cursor(
        self, account_uuid: UUID, queue_id: str, event_id: int
    ) -> bool:
        assert account_uuid == ACCOUNT_UUID
        self.cursors.append((queue_id, event_id))
        return True

    async def clear_zulip_queue(self, account_uuid: UUID, queue_id: str) -> bool:
        assert account_uuid == ACCOUNT_UUID
        self.cleared.append(queue_id)
        return True


class FakeClient:
    def __init__(self) -> None:
        self.worker: ZulipEventThread | None = None

    def register(self) -> RegisteredQueue:
        return RegisteredQueue("queue-1", 0, 90)

    def poll(self, queue_id: str, event_id: int, timeout: float) -> int:
        assert (queue_id, event_id, timeout) == ("queue-1", 0, 90)
        assert self.worker is not None
        self.worker._stop_requested.set()
        return 7


def test_worker_registers_queue_and_only_advances_cursor() -> None:
    asyncio.run(_run_worker_test())


async def _run_worker_test() -> None:
    account = ExternalAccount(
        ACCOUNT_UUID,
        OWNER_UUID,
        1,
        PROJECT_UUID,
        "https://zulip.example.test",
        "user@example.test",
        "private-key",
    )
    store = FakeStore()
    client = FakeClient()
    worker = ZulipEventThread(
        account,
        store,  # type: ignore[arg-type]
        asyncio.get_running_loop(),
        Settings(),
        threading.BoundedSemaphore(1),
    )
    client.worker = worker

    await asyncio.to_thread(worker._poll, client)  # type: ignore[arg-type]

    assert store.queues == [("queue-1", 0)]
    assert store.cursors == [("queue-1", 7)]


class ExpiringClient:
    def register(self) -> RegisteredQueue:
        return RegisteredQueue("queue-b", 20, 90)

    def poll(self, queue_id: str, event_id: int, timeout: float) -> int:
        if queue_id == "queue-a":
            assert (event_id, timeout) == (10, 180)
            raise ZulipApiError("BAD_EVENT_QUEUE_ID", retryable=True)
        assert (queue_id, event_id, timeout) == ("queue-b", 20, 90)
        raise ZulipApiError("RATE_LIMIT_HIT", retryable=True)


class ResumingClient:
    def __init__(self, worker: ZulipEventThread) -> None:
        self._worker = worker

    def register(self) -> RegisteredQueue:
        raise AssertionError("the replacement queue must be resumed")

    def poll(self, queue_id: str, event_id: int, timeout: float) -> int:
        assert (queue_id, event_id, timeout) == ("queue-b", 20, 90)
        self._worker._stop_requested.set()
        return 21


def test_worker_resumes_replacement_queue_after_retryable_failure() -> None:
    asyncio.run(_run_reconnect_test())


async def _run_reconnect_test() -> None:
    account = ExternalAccount(
        ACCOUNT_UUID,
        OWNER_UUID,
        1,
        PROJECT_UUID,
        "https://zulip.example.test",
        "user@example.test",
        "private-key",
        queue_id="queue-a",
        last_event_id=10,
    )
    store = FakeStore()
    worker = ZulipEventThread(
        account,
        store,  # type: ignore[arg-type]
        asyncio.get_running_loop(),
        Settings(),
        threading.BoundedSemaphore(1),
    )

    with pytest.raises(ZulipApiError, match="RATE_LIMIT_HIT"):
        await asyncio.to_thread(worker._poll, ExpiringClient())  # type: ignore[arg-type]
    await asyncio.to_thread(
        worker._poll,
        ResumingClient(worker),  # type: ignore[arg-type]
    )

    assert store.cleared == ["queue-a"]
    assert store.queues == [("queue-b", 20)]
    assert store.cursors == [("queue-b", 21)]


class SupervisorStore:
    def __init__(self, account: ExternalAccount) -> None:
        self.account = account

    async def list_external_accounts(self) -> list[ExternalAccount]:
        return [self.account]


class SupervisorWorker:
    def __init__(self, account: ExternalAccount) -> None:
        self.account = account
        self.alive = False
        self.stop_calls = 0

    def start(self) -> None:
        self.alive = True

    def stop(self) -> None:
        self.stop_calls += 1

    def is_alive(self) -> bool:
        return self.alive


def test_supervisor_does_not_replace_worker_that_timed_out_stopping() -> None:
    asyncio.run(_run_stuck_worker_test())


async def _run_stuck_worker_test() -> None:
    account = ExternalAccount(
        ACCOUNT_UUID,
        OWNER_UUID,
        1,
        PROJECT_UUID,
        "https://zulip.example.test",
        "user@example.test",
        "private-key",
    )
    store = SupervisorStore(account)
    workers: list[SupervisorWorker] = []

    def worker_factory(
        worker_account: ExternalAccount,
        registration_gate: threading.Semaphore,
    ) -> SupervisorWorker:
        del registration_gate
        worker = SupervisorWorker(worker_account)
        workers.append(worker)
        return worker

    supervisor = ZulipThreadSupervisor(
        store,  # type: ignore[arg-type]
        asyncio.get_running_loop(),
        Settings(thread_stop_timeout_seconds=0),
        worker_factory,
    )
    await supervisor.reconcile()
    first_worker = workers[0]

    store.account = ExternalAccount(
        ACCOUNT_UUID,
        OWNER_UUID,
        2,
        PROJECT_UUID,
        "https://zulip.example.test",
        "user@example.test",
        "rotated-key",
    )
    await supervisor.reconcile()

    assert first_worker.stop_calls == 1
    assert workers == [first_worker]
    assert supervisor._workers[ACCOUNT_UUID][1] is first_worker

    first_worker.alive = False
    await supervisor.reconcile()

    assert len(workers) == 2
    assert supervisor._workers[ACCOUNT_UUID][1] is workers[1]
