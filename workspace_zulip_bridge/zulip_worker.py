# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import random
import threading
import time
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import Protocol
from uuid import UUID

import httpx

from workspace_zulip_bridge.chat_catalog import ChatCatalogBuilder
from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.message_history import build_message_page
from workspace_zulip_bridge.models import RecentPrivateConversation
from workspace_zulip_bridge.models import ScheduledChat
from workspace_zulip_bridge.models import UserDirectoryWrite
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.models import ZulipEvent
from workspace_zulip_bridge.models import ZulipIdentity
from workspace_zulip_bridge.models import ZulipUser
from workspace_zulip_bridge.stable_ids import stable_user_uuid
from workspace_zulip_bridge.zulip_api import ZulipApiClient
from workspace_zulip_bridge.zulip_api import ZulipApiError

LOG = logging.getLogger(__name__)


class Worker(Protocol):
    user: ZulipUser

    def start(self) -> None: ...

    def stop(self) -> None: ...

    def is_alive(self) -> bool: ...


ApiFactory = Callable[[ZulipUser], ZulipApiClient]
WorkerFactory = Callable[[ZulipUser, threading.Semaphore], Worker]
DirectoryLoader = Callable[
    [],
    tuple[list[ZulipDirectoryUser], UserDirectoryWrite],
]


@dataclass(frozen=True, slots=True)
class _DirectoryCacheEntry:
    users: tuple[ZulipDirectoryUser, ...]
    loaded_at: float


class EndpointDirectoryCache:
    def __init__(self, ttl_seconds: float) -> None:
        self._ttl_seconds = ttl_seconds
        self._condition = threading.Condition()
        self._entries: dict[str, _DirectoryCacheEntry] = {}
        self._loading: set[str] = set()

    def get_or_load(
        self,
        endpoint: str,
        loader: DirectoryLoader,
    ) -> tuple[tuple[ZulipDirectoryUser, ...], UserDirectoryWrite]:
        while True:
            with self._condition:
                entry = self._entries.get(endpoint)
                now = time.monotonic()
                if entry is not None and now - entry.loaded_at < self._ttl_seconds:
                    return entry.users, UserDirectoryWrite(len(entry.users), 0)
                if endpoint not in self._loading:
                    self._loading.add(endpoint)
                    break
                self._condition.wait(timeout=1.0)
        try:
            users, result = loader()
            entry = _DirectoryCacheEntry(tuple(users), time.monotonic())
        except BaseException:
            with self._condition:
                self._loading.discard(endpoint)
                self._condition.notify_all()
            raise
        with self._condition:
            self._entries[endpoint] = entry
            self._loading.discard(endpoint)
            self._condition.notify_all()
        return entry.users, result


class ZulipEventThread(threading.Thread):
    def __init__(
        self,
        user: ZulipUser,
        store: EventStore,
        loop: asyncio.AbstractEventLoop,
        settings: Settings,
        registration_gate: threading.Semaphore,
        directory_cache: EndpointDirectoryCache | None = None,
        catalog_write_gate: threading.Semaphore | None = None,
        message_scan_gate: threading.Semaphore | None = None,
        history_gate: threading.Semaphore | None = None,
        api_factory: ApiFactory | None = None,
    ) -> None:
        super().__init__(
            name=f"zulip-user-{user.uuid}",
            daemon=True,
        )
        self.user = user
        self._store = store
        self._loop = loop
        self._settings = settings
        self._registration_gate = registration_gate
        self._directory_cache = directory_cache or EndpointDirectoryCache(
            settings.zulip_directory_cache_ttl_seconds
        )
        self._catalog_write_gate = catalog_write_gate or threading.BoundedSemaphore(1)
        self._message_scan_gate = message_scan_gate or threading.BoundedSemaphore(
            settings.zulip_message_scan_concurrency
        )
        self._history_gate = history_gate or threading.BoundedSemaphore(
            settings.zulip_history_concurrency
        )
        self._api_factory = api_factory or self._default_api_factory
        self._stop_requested = threading.Event()
        self._client_lock = threading.Lock()
        self._client: ZulipApiClient | None = None
        self._queue_id = user.queue_id
        self._last_event_id = user.last_event_id
        self._catalog_filled = False
        self._resume_existing_queue = (
            user.status in {"scheduling", "backfilling", "active"}
            and user.queue_id is not None
            and user.last_event_id is not None
        )
        self._identity: ZulipIdentity | None = None
        self._user_uuids: dict[int, UUID] = {}
        self._bot_user_ids: frozenset[int] = frozenset()
        self._stream_ids_by_name: dict[str, int] = {}
        self._allowed_chat_keys: set[str] = set()
        self._catalog_builder: ChatCatalogBuilder | None = None
        self._recent_private_conversations: tuple[RecentPrivateConversation, ...] = ()

    def stop(self) -> None:
        self._stop_requested.set()
        with self._client_lock:
            client = self._client
            self._client = None
        if client is not None:
            threading.Thread(
                target=self._close_client,
                args=(client,),
                name=f"zulip-close-{self.user.uuid}",
                daemon=True,
            ).start()

    def run(self) -> None:
        attempt = 0
        while not self._stop_requested.is_set():
            pause_until_configuration_changes = False
            client = self._api_factory(self.user)
            with self._client_lock:
                self._client = client
            try:
                self._poll(client)
                return
            except ZulipApiError as exc:
                if self._stop_requested.is_set():
                    return
                if not exc.retryable:
                    LOG.warning(
                        "Zulip worker paused user_uuid=%s code=%s",
                        self.user.uuid,
                        exc.code,
                    )
                    pause_until_configuration_changes = True
                else:
                    LOG.warning(
                        "Zulip worker retry user_uuid=%s code=%s",
                        self.user.uuid,
                        exc.code,
                    )
            except httpx.TransportError as exc:
                if self._stop_requested.is_set():
                    return
                LOG.warning(
                    "Zulip worker retry user_uuid=%s code=%s",
                    self.user.uuid,
                    type(exc).__name__,
                )
            except Exception:
                if self._stop_requested.is_set():
                    return
                LOG.exception(
                    "Zulip worker unexpected failure user_uuid=%s",
                    self.user.uuid,
                )
            finally:
                with self._client_lock:
                    owns_client = self._client is client
                    if owns_client:
                        self._client = None
                if owns_client:
                    client.close()
            if pause_until_configuration_changes:
                self._stop_requested.wait()
                return
            self._wait_before_retry(attempt)
            attempt += 1

    def _poll(self, client: ZulipApiClient) -> None:
        queue_id = self._queue_id
        last_event_id = self._last_event_id
        longpoll_timeout = self._settings.zulip_default_longpoll_timeout_seconds
        attempt = 0
        while not self._stop_requested.is_set():
            registered_now = False
            if queue_id is None or last_event_id is None:
                with self._registration_gate:
                    if self._stop_requested.is_set():
                        return
                    registered = client.register()
                if not self._submit(
                    self._store.set_queue(
                        self.user.uuid,
                        registered.queue_id,
                        registered.last_event_id,
                    )
                ):
                    return
                queue_id = registered.queue_id
                last_event_id = registered.last_event_id
                self._queue_id = queue_id
                self._last_event_id = last_event_id
                self._recent_private_conversations = (
                    registered.recent_private_conversations
                )
                longpoll_timeout = registered.longpoll_timeout_seconds
                registered_now = True
                LOG.info(
                    "Zulip queue registered user_uuid=%s",
                    self.user.uuid,
                )

            if not self._catalog_filled and self._resume_existing_queue:
                with self._registration_gate:
                    if self._stop_requested.is_set():
                        return
                    if not self._restore_runtime_state(client):
                        return
                self._catalog_filled = True
                self._resume_existing_queue = False

            if not self._catalog_filled:
                if not registered_now:
                    if not self._submit(
                        self._store.set_user_status(
                            self.user.uuid,
                            queue_id,
                            "streaming",
                        )
                    ):
                        return
                if not self._fill_chat_catalog(client, queue_id):
                    return
                self._catalog_filled = True

            user_status = self._submit(
                self._store.get_user_status(self.user.uuid, queue_id)
            )
            if user_status is None:
                return
            if user_status == "scheduling":
                self._stop_requested.wait(self._settings.user_refresh_seconds)
                continue

            pending_history = self._submit(
                self._store.list_pending_history_chats(self.user.uuid, queue_id)
            )
            if pending_history:
                with self._history_gate:
                    if self._stop_requested.is_set():
                        return
                    if not self._load_scheduled_history(
                        client,
                        queue_id,
                        pending_history,
                    ):
                        return

            try:
                raw_events = client.get_events(
                    queue_id,
                    last_event_id,
                    longpoll_timeout,
                )
            except ZulipApiError as exc:
                if exc.code != "BAD_EVENT_QUEUE_ID":
                    raise
                if not self._submit(self._store.clear_queue(self.user.uuid, queue_id)):
                    return
                LOG.info(
                    "Zulip queue lost; catalog reload required user_uuid=%s",
                    self.user.uuid,
                )
                queue_id = None
                last_event_id = None
                self._queue_id = None
                self._last_event_id = None
                self._catalog_filled = False
                self._resume_existing_queue = False
                self._identity = None
                self._user_uuids.clear()
                self._bot_user_ids = frozenset()
                self._stream_ids_by_name.clear()
                self._allowed_chat_keys.clear()
                self._catalog_builder = None
                self._recent_private_conversations = ()
                self._wait_before_retry(attempt)
                attempt += 1
                continue

            if self._stop_requested.is_set():
                return
            if not raw_events:
                attempt = 0
                continue
            events, next_event_id = self._prepare_events(raw_events, last_event_id)
            _, cursor_updated = self._submit(
                self._store.store_events(
                    self.user.uuid,
                    queue_id,
                    events,
                    next_event_id,
                )
            )
            if not cursor_updated:
                return
            last_event_id = next_event_id
            self._last_event_id = next_event_id
            attempt = 0

    def _fill_chat_catalog(self, client: ZulipApiClient, queue_id: str) -> bool:
        return self._load_chat_catalog(client, queue_id)

    def _load_chat_catalog(self, client: ZulipApiClient, queue_id: str) -> bool:
        started_at = time.monotonic()
        if not self._submit(self._store.begin_catalog_fill(self.user.uuid, queue_id)):
            return False
        identity = client.get_own_user()
        directory, directory_result = self._load_directory(client)
        if not self._submit(
            self._store.set_user_identity(
                self.user.uuid,
                self.user.endpoint,
                identity.user_id,
                identity.full_name,
                identity.role,
            )
        ):
            return False
        user_uuids = {
            user.user_id: stable_user_uuid(self.user.endpoint, user.user_id)
            for user in directory
            if not user.is_bot
        }
        if identity.user_id not in user_uuids:
            raise RuntimeError("current Zulip user is missing from user directory")
        bot_user_ids = frozenset(user.user_id for user in directory if user.is_bot)
        subscriptions = client.get_subscriptions()
        builder = ChatCatalogBuilder(identity.user_id, identity.full_name)
        channel_chats = builder.add_subscriptions(subscriptions)
        direct_chats = builder.add_recent_direct_conversations(
            self._recent_private_conversations,
            user_names={
                user.user_id: user.full_name for user in directory if not user.is_bot
            },
            excluded_user_ids=bot_user_ids,
        )
        stream_ids_by_name: dict[str, int] = {}
        for subscription in subscriptions:
            stream_id = subscription.get("stream_id")
            name = subscription.get("name")
            if isinstance(stream_id, int) and isinstance(name, str):
                stream_ids_by_name[name] = stream_id

        allowed_chat_keys = {chat.chat_key for chat in (*channel_chats, *direct_chats)}
        catalog = builder.build()
        skipped_direct_chats = len(self._recent_private_conversations) - len(
            direct_chats
        )
        with self._catalog_write_gate:
            if self._stop_requested.is_set():
                return False
            result = self._submit(
                self._store.store_chat_catalog(
                    self.user.uuid,
                    queue_id,
                    catalog,
                )
            )
        if not result.activated:
            return False
        elapsed = time.monotonic() - started_at
        self._identity = identity
        self._user_uuids = user_uuids
        self._bot_user_ids = bot_user_ids
        self._stream_ids_by_name = stream_ids_by_name
        self._allowed_chat_keys = allowed_chat_keys
        self._catalog_builder = builder
        LOG.info(
            "Zulip catalog ready user_uuid=%s users=%s user_changes=%s "
            "chats=%s chat_upserts=%s chat_deletes=%s "
            "skipped_direct_chats=%s elapsed_seconds=%.3f",
            self.user.uuid,
            directory_result.humans,
            directory_result.changed,
            len(catalog.chats),
            result.upserted,
            result.deleted,
            skipped_direct_chats,
            elapsed,
        )
        return True

    def _load_scheduled_history(
        self,
        client: ZulipApiClient,
        queue_id: str,
        scheduled_chats: list[ScheduledChat],
    ) -> bool:
        if self._identity is None:
            return False
        started_at = time.monotonic()
        history = self._submit(self._store.begin_history(self.user.uuid, queue_id))
        pages = 0
        source_message_count = 0
        stored_message_count = 0
        changed_message_count = 0
        unchanged_message_count = 0
        unassigned_message_count = 0
        skipped_message_count = 0
        skipped_reaction_count = 0
        topics_inserted = 0
        unknown_flags: set[str] = set()
        try:
            schedules_loaded = 0
            messages_deleted = 0
            topics_deleted = 0
            for scheduled_chat in scheduled_chats:
                if self._stop_requested.is_set():
                    return False
                anchor: str | int = "newest"
                include_anchor = True
                while not self._stop_requested.is_set():
                    with self._message_scan_gate:
                        page = client.get_chat_messages_page(
                            scheduled_chat.chat_key,
                            self._identity.user_id,
                            anchor,
                            include_anchor=include_anchor,
                        )
                        source_message_count += len(page.messages)
                        built_page = build_message_page(
                            page.messages,
                            own_user_id=self._identity.user_id,
                            user_uuids=self._user_uuids,
                            stream_ids_by_name=self._stream_ids_by_name,
                            allowed_chat_keys={scheduled_chat.chat_key},
                        )
                        page_write = self._submit(
                            history.store_page(built_page.messages)
                        )
                        pages += 1
                        stored_message_count += page_write.received
                        changed_message_count += page_write.changed
                        unchanged_message_count += page_write.unchanged
                        unassigned_message_count += page_write.unassigned
                        skipped_message_count += built_page.skipped_messages
                        skipped_reaction_count += built_page.skipped_reactions
                        topics_inserted += page_write.topics_inserted
                        unknown_flags.update(built_page.unknown_flags)
                        found_oldest = page.found_oldest
                        next_anchor = (
                            anchor
                            if found_oldest
                            else self._next_history_anchor(page.messages, anchor)
                        )
                        del built_page
                        del page
                    if found_oldest:
                        break
                    anchor = next_anchor
                    include_anchor = False
                chat_write = self._submit(history.finish([scheduled_chat.chat_key]))
                if not chat_write.activated:
                    return False
                schedules_loaded += chat_write.schedules_loaded
                messages_deleted += chat_write.messages_deleted
                topics_deleted += chat_write.topics_deleted
            if self._stop_requested.is_set():
                return False
        finally:
            self._submit(history.close())

        elapsed = time.monotonic() - started_at
        rate = stored_message_count / elapsed if elapsed else 0.0
        if unknown_flags:
            LOG.warning(
                "Unknown Zulip message flags user_uuid=%s flags=%s",
                self.user.uuid,
                ",".join(sorted(unknown_flags)),
            )
        LOG.info(
            "Zulip scheduled history ready user_uuid=%s chats=%s pages=%s "
            "source_messages=%s stored_messages=%s message_changes=%s "
            "message_unchanged=%s unassigned_messages=%s skipped_messages=%s "
            "skipped_reactions=%s topic_inserts=%s message_deletes=%s "
            "topic_deletes=%s schedules_loaded=%s elapsed_seconds=%.3f "
            "messages_per_second=%.3f",
            self.user.uuid,
            len(scheduled_chats),
            pages,
            source_message_count,
            stored_message_count,
            changed_message_count,
            unchanged_message_count,
            unassigned_message_count,
            skipped_message_count,
            skipped_reaction_count,
            topics_inserted,
            messages_deleted,
            topics_deleted,
            schedules_loaded,
            elapsed,
            rate,
        )
        return True

    def _restore_runtime_state(self, client: ZulipApiClient) -> bool:
        identity = client.get_own_user()
        directory, _ = self._load_directory(client)
        if not self._submit(
            self._store.set_user_identity(
                self.user.uuid,
                self.user.endpoint,
                identity.user_id,
                identity.full_name,
                identity.role,
            )
        ):
            return False
        user_uuids = {
            user.user_id: stable_user_uuid(self.user.endpoint, user.user_id)
            for user in directory
            if not user.is_bot
        }
        if identity.user_id not in user_uuids:
            raise RuntimeError("current Zulip user is missing from user directory")
        bot_user_ids = frozenset(user.user_id for user in directory if user.is_bot)
        subscriptions = client.get_subscriptions()
        builder = ChatCatalogBuilder(identity.user_id, identity.full_name)
        channel_chats = builder.add_subscriptions(subscriptions)
        allowed_chat_keys = self._submit(
            self._store.list_user_chat_keys(self.user.uuid)
        )
        allowed_chat_keys.update(chat.chat_key for chat in channel_chats)
        self._identity = identity
        self._user_uuids = user_uuids
        self._bot_user_ids = bot_user_ids
        self._stream_ids_by_name = {
            name: stream_id
            for subscription in subscriptions
            if isinstance((stream_id := subscription.get("stream_id")), int)
            and isinstance((name := subscription.get("name")), str)
        }
        self._allowed_chat_keys = allowed_chat_keys
        self._catalog_builder = builder
        LOG.info("Zulip queue resumed user_uuid=%s", self.user.uuid)
        return True

    def _load_directory(
        self,
        client: ZulipApiClient,
    ) -> tuple[tuple[ZulipDirectoryUser, ...], UserDirectoryWrite]:
        def load() -> tuple[list[ZulipDirectoryUser], UserDirectoryWrite]:
            users = client.get_users()
            result = self._submit(
                self._store.store_user_directory(self.user.endpoint, users)
            )
            return users, result

        return self._directory_cache.get_or_load(self.user.endpoint, load)

    @staticmethod
    def _next_history_anchor(
        messages: list[Mapping[str, Any]],
        current_anchor: str | int,
    ) -> int:
        message_ids = [
            message_id
            for message in messages
            if isinstance((message_id := message.get("id")), int)
        ]
        if not message_ids:
            raise ZulipApiError("invalid_messages_pagination", retryable=True)
        next_anchor = min(message_ids)
        if isinstance(current_anchor, int) and next_anchor >= current_anchor:
            raise ZulipApiError("invalid_messages_pagination", retryable=True)
        return next_anchor

    def _prepare_events(
        self,
        raw_events: list[Mapping[str, Any]],
        last_event_id: int,
    ) -> tuple[list[ZulipEvent], int]:
        events: list[ZulipEvent] = []
        next_event_id = last_event_id
        for raw_event in raw_events:
            event_id = raw_event.get("id")
            event_type = raw_event.get("type")
            if not isinstance(event_id, int) or not isinstance(event_type, str):
                raise ZulipApiError("invalid_event", retryable=True)
            next_event_id = max(next_event_id, event_id)
            if event_type == "heartbeat" or self._event_is_bot_related(raw_event):
                continue
            events.append(
                ZulipEvent(
                    event_id=event_id,
                    event_type=event_type,
                    payload_json=json.dumps(
                        raw_event,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                )
            )
        return events, next_event_id

    def _event_is_bot_related(self, event: Mapping[str, Any]) -> bool:
        user_id = event.get("user_id")
        if isinstance(user_id, int) and user_id in self._bot_user_ids:
            return True
        message = event.get("message")
        if isinstance(message, Mapping):
            sender_id = message.get("sender_id")
            if isinstance(sender_id, int) and sender_id in self._bot_user_ids:
                return True
        person = event.get("person")
        if isinstance(person, Mapping):
            person_user_id = person.get("user_id")
            if isinstance(person_user_id, int) and person_user_id in self._bot_user_ids:
                return True
        return False

    def _submit(self, coroutine: Coroutine[Any, Any, Any]) -> Any:
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
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
        self._stop_requested.wait(random.uniform(0.0, maximum))

    def _default_api_factory(self, user: ZulipUser) -> ZulipApiClient:
        return ZulipApiClient(
            user.endpoint,
            user.login,
            user.api_key,
            ca_file=self._settings.zulip_ca_file,
            connect_timeout_seconds=self._settings.zulip_connect_timeout_seconds,
            default_longpoll_timeout_seconds=(
                self._settings.zulip_default_longpoll_timeout_seconds
            ),
            idle_queue_timeout_seconds=(
                self._settings.zulip_idle_queue_timeout_seconds
            ),
            chat_fill_timeout_seconds=(self._settings.zulip_chat_fill_timeout_seconds),
            message_page_size=self._settings.zulip_message_page_size,
        )

    @staticmethod
    def _close_client(client: ZulipApiClient) -> None:
        try:
            client.close()
        except Exception:
            LOG.debug("Zulip client close failed", exc_info=True)


class ZulipThreadSupervisor:
    def __init__(
        self,
        store: EventStore,
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
        self._directory_cache = EndpointDirectoryCache(
            settings.zulip_directory_cache_ttl_seconds
        )
        self._catalog_write_gate = threading.BoundedSemaphore(1)
        self._message_scan_gate = threading.BoundedSemaphore(
            settings.zulip_message_scan_concurrency
        )
        self._history_gate = threading.BoundedSemaphore(
            settings.zulip_history_concurrency
        )
        self._worker_factory = worker_factory or self._new_worker
        self._workers: dict[UUID, tuple[bytes, Worker]] = {}

    async def run(self) -> None:
        try:
            while True:
                await self.reconcile()
                await asyncio.sleep(self._settings.user_refresh_seconds)
        finally:
            await self._stop_workers([worker for _, worker in self._workers.values()])
            self._workers.clear()

    async def reconcile(self) -> None:
        schedule = await self._store.reconcile_chat_schedules()
        if schedule.invalidated or schedule.assigned or schedule.messages_deleted:
            LOG.info(
                "Zulip chat schedules reconciled invalidated=%s assigned=%s "
                "messages_deleted=%s",
                schedule.invalidated,
                schedule.assigned,
                schedule.messages_deleted,
            )
        users = {user.uuid: user for user in await self._store.list_users()}
        stale_ids = [
            user_uuid
            for user_uuid, (signature, worker) in self._workers.items()
            if user_uuid not in users
            or signature != self._signature(users[user_uuid])
            or not worker.is_alive()
        ]
        stale_workers = [self._workers.pop(user_uuid)[1] for user_uuid in stale_ids]
        await self._stop_workers(stale_workers)

        for user_uuid, user in users.items():
            if user_uuid in self._workers:
                continue
            worker = self._worker_factory(
                user,
                self._registration_gate,
            )
            worker.start()
            self._workers[user_uuid] = (self._signature(user), worker)
            LOG.info("Zulip worker started user_uuid=%s", user_uuid)

    async def _stop_workers(self, workers: list[Worker]) -> None:
        if not workers:
            return
        for worker in workers:
            worker.stop()
        deadline = self._loop.time() + self._settings.thread_stop_timeout_seconds
        while any(worker.is_alive() for worker in workers):
            if self._loop.time() >= deadline:
                alive = sum(worker.is_alive() for worker in workers)
                LOG.warning("Zulip workers did not stop count=%s", alive)
                return
            await asyncio.sleep(0.05)

    def _new_worker(
        self,
        user: ZulipUser,
        registration_gate: threading.Semaphore,
    ) -> Worker:
        return ZulipEventThread(
            user,
            self._store,
            self._loop,
            self._settings,
            registration_gate,
            self._directory_cache,
            self._catalog_write_gate,
            self._message_scan_gate,
            self._history_gate,
        )

    @staticmethod
    def _signature(user: ZulipUser) -> bytes:
        digest = hashlib.blake2b(digest_size=16)
        for value in user.connection_signature():
            digest.update(value.encode("utf-8"))
            digest.update(b"\0")
        return digest.digest()
