# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import logging

import asyncpg

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.database import open_pool
from workspace_zulip_bridge.database import prepare_database
from workspace_zulip_bridge.database import probe_database
from workspace_zulip_bridge.v4_store import V4Store
from workspace_zulip_bridge.workspace_control import WorkspaceControlWorker
from workspace_zulip_bridge.workspace_events import WorkspaceEventThread
from workspace_zulip_bridge.zulip_worker import ZulipThreadSupervisor

LOG = logging.getLogger(__name__)


class BridgeService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def run(self, stop: asyncio.Event) -> None:
        pool = await open_pool(self._settings)
        workspace_thread: WorkspaceEventThread | None = None
        tasks: list[asyncio.Task[object]] = []
        try:
            await prepare_database(pool)
            await probe_database(pool)
            store = V4Store(pool)
            supervisor = ZulipThreadSupervisor(
                store,
                asyncio.get_running_loop(),
                self._settings,
            )
            tasks.extend(
                (
                    asyncio.create_task(self._probe_loop(pool), name="database-probe"),
                    asyncio.create_task(
                        supervisor.run(),
                        name="zulip-thread-supervisor",
                    ),
                    asyncio.create_task(stop.wait(), name="stop-signal"),
                )
            )
            if self._settings.workspace_control_enabled:
                tasks.append(
                    asyncio.create_task(
                        WorkspaceControlWorker(pool, self._settings).run(),
                        name="workspace-control",
                    )
                )
            if self._settings.workspace_events_enabled:
                workspace_thread = WorkspaceEventThread(self._settings)
                workspace_thread.start()
                tasks.append(
                    asyncio.create_task(
                        self._watch_workspace_thread(workspace_thread),
                        name="workspace-thread-watch",
                    )
                )
            LOG.info("v4 connection daemon is ready")
            completed, _ = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            stop_task = next(task for task in tasks if task.get_name() == "stop-signal")
            if stop_task not in completed:
                next(iter(completed)).result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if workspace_thread is not None:
                workspace_thread.stop()
                await asyncio.to_thread(
                    workspace_thread.join,
                    self._settings.thread_stop_timeout_seconds,
                )
                if workspace_thread.is_alive():
                    LOG.warning("Workspace event thread did not stop")
            await pool.close()
            LOG.info("v4 connection daemon stopped")

    async def _probe_loop(self, pool: asyncpg.Pool) -> None:
        while True:
            await asyncio.sleep(self._settings.db_probe_seconds)
            await probe_database(pool)

    @staticmethod
    async def _watch_workspace_thread(worker: WorkspaceEventThread) -> None:
        await asyncio.to_thread(worker.join)
        if worker.error is not None:
            raise RuntimeError("Workspace event thread failed") from worker.error
        raise RuntimeError("Workspace event thread stopped unexpectedly")
