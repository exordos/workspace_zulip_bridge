# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import concurrent.futures
import logging
import random
import threading
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any
from typing import Protocol
from uuid import UUID

import httpx

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.models import ExternalAccount
from workspace_zulip_bridge.v4_store import V4Store
from workspace_zulip_bridge.zulip_api import ZulipApiClient
from workspace_zulip_bridge.zulip_api import ZulipApiError

LOG = logging.getLogger(__name__)


class Worker(Protocol):
    account: ExternalAccount

    def start(self) -> None: ...

    def stop(self) -> None: ...

    def is_alive(self) -> bool: ...


ApiFactory = Callable[[ExternalAccount], ZulipApiClient]
WorkerFactory = Callable[[ExternalAccount, threading.Semaphore], Worker]


class ZulipEventThread(threading.Thread):
    """Own one Zulip queue and discard every received event after cursoring it."""

    def __init__(
        self,
        account: ExternalAccount,
        store: V4Store,
        loop: asyncio.AbstractEventLoop,
        settings: Settings,
        registration_gate: threading.Semaphore,
        api_factory: ApiFactory | None = None,
    ) -> None:
        super().__init__(name=f"zulip-account-{account.uuid}", daemon=True)
        self.account = account
        self._store = store
        self._loop = loop
        self._settings = settings
        self._registration_gate = registration_gate
        self._api_factory = api_factory or self._default_api_factory
        self._stop_requested = threading.Event()
        self._client_lock = threading.Lock()
        self._client: ZulipApiClient | None = None
        self._queue_id = account.queue_id
        self._last_event_id = account.last_event_id
        self._longpoll_timeout_seconds = settings.zulip_default_longpoll_timeout_seconds

    def stop(self) -> None:
        self._stop_requested.set()
        with self._client_lock:
            client = self._client
            self._client = None
        if client is not None:
            try:
                client.close()
            except Exception:
                LOG.debug("Zulip client close failed", exc_info=True)

    def run(self) -> None:
        attempt = 0
        while not self._stop_requested.is_set():
            client = self._api_factory(self.account)
            with self._client_lock:
                self._client = client
            try:
                self._submit(
                    self._store.mark_zulip_status(self.account.uuid, "connecting")
                )
                self._poll(client)
                attempt = 0
            except ZulipApiError as exc:
                if self._stop_requested.is_set():
                    return
                if not exc.retryable:
                    self._submit(
                        self._store.mark_zulip_status(
                            self.account.uuid,
                            "auth_required",
                            exc.code,
                        )
                    )
                    LOG.warning(
                        "Zulip worker paused account_uuid=%s code=%s",
                        self.account.uuid,
                        exc.code,
                    )
                    self._stop_requested.wait()
                    return
                self._mark_disconnected(exc.code)
            except httpx.TransportError as exc:
                if self._stop_requested.is_set():
                    return
                self._mark_disconnected(type(exc).__name__)
            except Exception as exc:
                if self._stop_requested.is_set():
                    return
                LOG.exception("Zulip worker failed account_uuid=%s", self.account.uuid)
                self._mark_disconnected(type(exc).__name__)
            finally:
                with self._client_lock:
                    owns_client = self._client is client
                    if owns_client:
                        self._client = None
                if owns_client:
                    client.close()
            self._wait_before_retry(attempt)
            attempt += 1

    def _poll(self, client: ZulipApiClient) -> None:
        while not self._stop_requested.is_set():
            if self._queue_id is None or self._last_event_id is None:
                with self._registration_gate:
                    if self._stop_requested.is_set():
                        return
                    registered = client.register()
                if not self._submit(
                    self._store.set_zulip_queue(
                        self.account.uuid,
                        registered.queue_id,
                        registered.last_event_id,
                    )
                ):
                    return
                self._queue_id = registered.queue_id
                self._last_event_id = registered.last_event_id
                self._longpoll_timeout_seconds = registered.longpoll_timeout_seconds
                LOG.info("Zulip queue registered account_uuid=%s", self.account.uuid)
            queue_id = self._queue_id
            last_event_id = self._last_event_id
            try:
                next_event_id = client.poll(
                    queue_id,
                    last_event_id,
                    self._longpoll_timeout_seconds,
                )
            except ZulipApiError as exc:
                if exc.code != "BAD_EVENT_QUEUE_ID":
                    raise
                if not self._submit(
                    self._store.clear_zulip_queue(self.account.uuid, queue_id)
                ):
                    return
                LOG.info("Zulip queue expired account_uuid=%s", self.account.uuid)
                self._queue_id = None
                self._last_event_id = None
                self._longpoll_timeout_seconds = (
                    self._settings.zulip_default_longpoll_timeout_seconds
                )
                continue
            if next_event_id == last_event_id:
                continue
            if not self._submit(
                self._store.advance_zulip_cursor(
                    self.account.uuid,
                    queue_id,
                    next_event_id,
                )
            ):
                return
            self._last_event_id = next_event_id

    def _mark_disconnected(self, error: str) -> None:
        LOG.warning(
            "Zulip worker reconnecting account_uuid=%s error=%s",
            self.account.uuid,
            error,
        )
        self._submit(
            self._store.mark_zulip_status(
                self.account.uuid,
                "disconnected",
                error,
            )
        )

    def _submit(self, coroutine: Coroutine[Any, Any, Any]) -> Any:
        try:
            future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        except Exception:
            coroutine.close()
            raise
        try:
            return future.result(timeout=self._settings.zulip_db_ack_timeout_seconds)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise

    def _wait_before_retry(self, attempt: int) -> None:
        maximum = min(
            self._settings.zulip_retry_cap_seconds,
            self._settings.zulip_retry_base_seconds * (2 ** min(attempt, 16)),
        )
        self._stop_requested.wait(random.uniform(maximum * 0.5, maximum))

    def _default_api_factory(self, account: ExternalAccount) -> ZulipApiClient:
        return ZulipApiClient(
            account.endpoint,
            account.login,
            account.api_key,
            ca_file=self._settings.effective_zulip_ca_file,
            connect_timeout_seconds=self._settings.zulip_connect_timeout_seconds,
            default_longpoll_timeout_seconds=(
                self._settings.zulip_default_longpoll_timeout_seconds
            ),
            idle_queue_timeout_seconds=(
                self._settings.zulip_idle_queue_timeout_seconds
            ),
        )


class ZulipThreadSupervisor:
    def __init__(
        self,
        store: V4Store,
        loop: asyncio.AbstractEventLoop,
        settings: Settings,
        worker_factory: WorkerFactory | None = None,
    ) -> None:
        self._store = store
        self._loop = loop
        self._settings = settings
        self._registration_gate = threading.BoundedSemaphore(
            settings.zulip_registration_concurrency
        )
        self._worker_factory = worker_factory or self._new_worker
        self._workers: dict[UUID, tuple[tuple[object, ...], Worker]] = {}

    async def run(self) -> None:
        try:
            while True:
                await self.reconcile()
                await asyncio.sleep(self._settings.account_refresh_seconds)
        finally:
            await self._stop_workers([worker for _, worker in self._workers.values()])
            self._workers.clear()

    async def reconcile(self) -> None:
        accounts = {
            account.uuid: account
            for account in await self._store.list_external_accounts()
        }
        stale_ids = [
            account_uuid
            for account_uuid, (signature, worker) in self._workers.items()
            if account_uuid not in accounts
            or signature != accounts[account_uuid].connection_signature()
            or not worker.is_alive()
        ]
        stale_workers = [self._workers[account_uuid][1] for account_uuid in stale_ids]
        await self._stop_workers(stale_workers)
        for account_uuid in stale_ids:
            worker = self._workers[account_uuid][1]
            if not worker.is_alive():
                self._workers.pop(account_uuid)
        for account_uuid, account in accounts.items():
            if account_uuid in self._workers:
                continue
            worker = self._worker_factory(account, self._registration_gate)
            worker.start()
            self._workers[account_uuid] = (account.connection_signature(), worker)
            LOG.info("Zulip worker started account_uuid=%s", account_uuid)

    async def _stop_workers(self, workers: list[Worker]) -> None:
        for worker in workers:
            worker.stop()
        deadline = self._loop.time() + self._settings.thread_stop_timeout_seconds
        while any(worker.is_alive() for worker in workers):
            if self._loop.time() >= deadline:
                LOG.warning(
                    "Zulip workers did not stop count=%s",
                    sum(worker.is_alive() for worker in workers),
                )
                return
            await asyncio.sleep(0.05)

    def _new_worker(
        self,
        account: ExternalAccount,
        registration_gate: threading.Semaphore,
    ) -> Worker:
        return ZulipEventThread(
            account,
            self._store,
            self._loop,
            self._settings,
            registration_gate,
        )
