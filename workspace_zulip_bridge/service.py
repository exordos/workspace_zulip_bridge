# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import logging

import asyncpg

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.database import open_pool
from workspace_zulip_bridge.database import prepare_database
from workspace_zulip_bridge.database import probe_database
from workspace_zulip_bridge.event_processor import ZulipEventProcessor
from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.workspace_events import WorkspaceEventReceiver
from workspace_zulip_bridge.workspace_sync import WorkspaceBootstrapper
from workspace_zulip_bridge.workspace_sync import WorkspaceDiffWorker
from workspace_zulip_bridge.workspace_sync import WorkspaceEventProcessor
from workspace_zulip_bridge.zulip_worker import ZulipThreadSupervisor

LOG = logging.getLogger(__name__)


class BridgeService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def run(self, stop: asyncio.Event) -> None:
        pool = await open_pool(self._settings)
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
            if self._settings.workspace_events_enabled:
                bootstrapper = WorkspaceBootstrapper(pool, self._settings)
                await bootstrapper.ensure()
                receiver = WorkspaceEventReceiver(pool, self._settings)
                workspace_event_processor = WorkspaceEventProcessor(
                    pool, self._settings
                )
                workspace_diff_workers = [
                    WorkspaceDiffWorker(
                        pool,
                        self._settings,
                        plan_enabled=index == 0,
                        partition=index,
                        partition_count=self._settings.workspace_sync_workers,
                    )
                    for index in range(self._settings.workspace_sync_workers)
                ]
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
                supervised_tasks.extend(
                    asyncio.create_task(
                        worker.run(),
                        name=f"workspace-diff-worker-{index}",
                    )
                    for index, worker in enumerate(workspace_diff_workers)
                )
            LOG.info("bridge daemon is ready")
            try:
                completed, _ = await asyncio.wait(
                    supervised_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task not in completed:
                    failed_task = next(iter(completed))
                    failed_task.result()
            finally:
                for task in supervised_tasks:
                    task.cancel()
                await asyncio.gather(*supervised_tasks, return_exceptions=True)
        finally:
            await pool.close()
            LOG.info("bridge daemon stopped")

    async def _probe_loop(self, pool: asyncpg.Pool) -> None:
        while True:
            await asyncio.sleep(self._settings.db_probe_seconds)
            await probe_database(pool)

    async def _bootstrap_loop(self, bootstrapper: WorkspaceBootstrapper) -> None:
        while True:
            await asyncio.sleep(self._settings.workspace_lease_retry_seconds)
            await bootstrapper.ensure()
