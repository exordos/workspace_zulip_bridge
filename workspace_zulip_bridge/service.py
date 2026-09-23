# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import logging
import random
import typing

import asyncpg

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.database import open_pool
from workspace_zulip_bridge.database import prepare_database
from workspace_zulip_bridge.database import probe_database
from workspace_zulip_bridge.event_processor import ZulipEventProcessor
from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.workspace_auth import WorkspaceTokenManager
from workspace_zulip_bridge.workspace_control import WorkspaceControlWorker
from workspace_zulip_bridge.workspace_events import WorkspaceEventReceiver
from workspace_zulip_bridge.workspace_sync import WorkspaceBootstrapper
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker
from workspace_zulip_bridge.workspace_sync import WorkspaceEventProcessor
from workspace_zulip_bridge.zulip_worker import ZulipThreadSupervisor

LOG = logging.getLogger(__name__)


class _Bootstrapper(typing.Protocol):
    async def ensure(self) -> bool: ...


class BridgeService:
    CONTENT_PARTITION_COUNT = 2

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @classmethod
    def content_worker_partitions(cls, worker_count: int) -> list[tuple[int, int]]:
        partition_count = min(worker_count, cls.CONTENT_PARTITION_COUNT)
        return [
            (index % partition_count, partition_count) for index in range(worker_count)
        ]

    @staticmethod
    def unpartitioned_worker_count(worker_count: int) -> int:
        return min(2, max(1, worker_count // 2))

    async def run(self, stop: asyncio.Event) -> None:
        pool = await open_pool(self._settings)
        supervised_tasks: list[asyncio.Task[typing.Any]] = []
        try:
            await prepare_database(pool)
            await probe_database(pool)
            store = EventStore(pool)
            supervisor = ZulipThreadSupervisor(
                store,
                asyncio.get_running_loop(),
                self._settings,
            )
            event_processor = ZulipEventProcessor(pool, store, self._settings)
            probe_task = asyncio.create_task(
                self._probe_loop(pool),
                name="database-probe",
            )
            supervisor_task = asyncio.create_task(
                supervisor.run(),
                name="zulip-thread-supervisor",
            )
            event_processor_task = asyncio.create_task(
                event_processor.run(),
                name="zulip-event-processor",
            )
            stop_task = asyncio.create_task(stop.wait(), name="stop-signal")
            supervised_tasks = [
                probe_task,
                supervisor_task,
                event_processor_task,
                stop_task,
            ]
            if self._settings.workspace_control_enabled:
                control_worker = WorkspaceControlWorker(pool, self._settings)
                supervised_tasks.append(
                    asyncio.create_task(
                        control_worker.run(),
                        name="workspace-control",
                    )
                )
            if self._settings.workspace_events_enabled:
                content_worker_partitions = self.content_worker_partitions(
                    self._settings.workspace_sync_workers
                )
                content_partition_count = content_worker_partitions[0][1]
                tokens = WorkspaceTokenManager(self._settings)
                bootstrapper = WorkspaceBootstrapper(pool, self._settings, tokens)
                if await self._ensure_bootstrap(bootstrapper, stop):
                    receiver = WorkspaceEventReceiver(pool, self._settings, tokens)
                    workspace_event_processor = WorkspaceEventProcessor(
                        pool, self._settings
                    )
                    workspace_diff_planner = WorkspaceDiffWorker(
                        pool,
                        self._settings,
                        plan_enabled=True,
                        partition=0,
                        partition_count=content_partition_count,
                        scope="unpartitioned",
                        tokens=tokens,
                    )
                    workspace_diff_workers = [
                        WorkspaceDiffWorker(
                            pool,
                            self._settings,
                            plan_enabled=False,
                            partition=partition,
                            partition_count=partition_count,
                            scope="partitioned",
                            delivery_priority=1,
                            tokens=tokens,
                        )
                        for partition, partition_count in content_worker_partitions
                    ]
                    workspace_unpartitioned_drainers = [
                        WorkspaceDiffWorker(
                            pool,
                            self._settings,
                            plan_enabled=False,
                            partition=0,
                            partition_count=content_partition_count,
                            scope="unpartitioned",
                            delivery_priority=1,
                            tokens=tokens,
                        )
                        for _ in range(
                            self.unpartitioned_worker_count(
                                self._settings.workspace_sync_workers
                            )
                        )
                    ]
                    workspace_realtime_drainer = WorkspaceDiffWorker(
                        pool,
                        self._settings,
                        plan_enabled=False,
                        partition=0,
                        partition_count=1,
                        scope="both",
                        delivery_priority=0,
                        tokens=tokens,
                    )
                    supervised_tasks.extend(
                        (
                            asyncio.create_task(
                                self._bootstrap_loop(bootstrapper),
                                name="workspace-bootstrap",
                            ),
                            asyncio.create_task(
                                receiver.run(),
                                name="workspace-event-receiver",
                            ),
                            asyncio.create_task(
                                workspace_event_processor.run(),
                                name="workspace-event-processor",
                            ),
                        )
                    )
                    supervised_tasks.append(
                        asyncio.create_task(
                            workspace_diff_planner.run(),
                            name="workspace-diff-planner",
                        )
                    )
                    supervised_tasks.extend(
                        asyncio.create_task(
                            worker.run(),
                            name=f"workspace-diff-worker-{index}",
                        )
                        for index, worker in enumerate(workspace_diff_workers)
                    )
                    supervised_tasks.extend(
                        asyncio.create_task(
                            worker.run(),
                            name=f"workspace-diff-unpartitioned-{index}",
                        )
                        for index, worker in enumerate(workspace_unpartitioned_drainers)
                    )
                    supervised_tasks.append(
                        asyncio.create_task(
                            workspace_realtime_drainer.run(),
                            name="workspace-diff-realtime",
                        )
                    )
            LOG.info("bridge daemon is ready")
            try:
                completed, _ = await asyncio.wait(
                    supervised_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task not in completed:
                    failed_task = next(
                        task for task in completed if task is not stop_task
                    )
                    try:
                        failed_task.result()
                    except asyncio.CancelledError as error:
                        raise RuntimeError(
                            f"bridge task {failed_task.get_name()} was cancelled"
                        ) from error
                    raise RuntimeError(
                        f"bridge task {failed_task.get_name()} stopped unexpectedly"
                    )
            finally:
                await self._cancel_tasks(supervised_tasks)
        finally:
            try:
                await asyncio.wait_for(
                    pool.close(),
                    timeout=self._settings.thread_stop_timeout_seconds,
                )
            except TimeoutError:
                LOG.error("Database pool shutdown timed out; terminating pool")
                pool.terminate()
            LOG.info("bridge daemon stopped")

    async def _cancel_tasks(
        self,
        tasks: list[asyncio.Task[typing.Any]],
    ) -> None:
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        done, pending = await asyncio.wait(
            tasks,
            timeout=self._settings.thread_stop_timeout_seconds,
        )
        if done:
            await asyncio.gather(*done, return_exceptions=True)
        if pending:
            LOG.error(
                "Bridge task shutdown timed out tasks=%s",
                ",".join(sorted(task.get_name() for task in pending)),
            )

    async def _probe_loop(self, pool: asyncpg.Pool) -> None:
        while True:
            await asyncio.sleep(self._settings.db_probe_seconds)
            await probe_database(pool)

    async def _bootstrap_loop(self, bootstrapper: _Bootstrapper) -> None:
        attempt = 0
        while True:
            try:
                await bootstrapper.ensure()
                attempt = 0
                delay = self._settings.workspace_lease_retry_seconds
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("Workspace bootstrap refresh failed; retrying")
                delay = self._bootstrap_retry_delay(attempt)
                attempt += 1
            await asyncio.sleep(delay)

    async def _ensure_bootstrap(
        self,
        bootstrapper: _Bootstrapper,
        stop: asyncio.Event,
    ) -> bool:
        attempt = 0
        while not stop.is_set():
            bootstrap_task = asyncio.create_task(bootstrapper.ensure())
            stop_task = asyncio.create_task(stop.wait())
            try:
                completed, _ = await asyncio.wait(
                    (bootstrap_task, stop_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if bootstrap_task in completed:
                    bootstrap_task.result()
                    return True
                if stop_task in completed:
                    bootstrap_task.cancel()
                    await asyncio.gather(bootstrap_task, return_exceptions=True)
                    return False
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("Workspace bootstrap failed; retrying")
            finally:
                for task in (bootstrap_task, stop_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(
                    bootstrap_task,
                    stop_task,
                    return_exceptions=True,
                )
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self._bootstrap_retry_delay(attempt),
                )
            except TimeoutError:
                pass
            attempt += 1
        return False

    def _bootstrap_retry_delay(self, attempt: int) -> float:
        maximum = min(
            self._settings.workspace_retry_cap_seconds,
            self._settings.workspace_retry_base_seconds * (2 ** min(attempt, 16)),
        )
        return random.uniform(maximum * 0.5, maximum)
