# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import concurrent.futures
import threading
import time
from uuid import UUID

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.models import ChatCatalogWrite
from workspace_zulip_bridge.models import ChatScheduleReconcile
from workspace_zulip_bridge.models import DirectMessagePage
from workspace_zulip_bridge.models import HistoryWrite
from workspace_zulip_bridge.models import LiveMessageWrite
from workspace_zulip_bridge.models import MessagePage
from workspace_zulip_bridge.models import MessagePageWrite
from workspace_zulip_bridge.models import RecentPrivateConversation
from workspace_zulip_bridge.models import RegisteredQueue
from workspace_zulip_bridge.models import UserDirectoryWrite
from workspace_zulip_bridge.models import UserStatus
from workspace_zulip_bridge.models import ZulipChatCatalog
from workspace_zulip_bridge.models import ZulipDirectoryUser
from workspace_zulip_bridge.models import ZulipEvent
from workspace_zulip_bridge.models import ZulipIdentity
from workspace_zulip_bridge.models import ZulipUser
from workspace_zulip_bridge.zulip_api import ZulipApiError
from workspace_zulip_bridge.zulip_worker import EndpointDirectoryCache
from workspace_zulip_bridge.zulip_worker import ZulipEventThread
from workspace_zulip_bridge.zulip_worker import ZulipThreadSupervisor

USER_ONE = ZulipUser(
    UUID("00000000-0000-0000-0000-000000000001"),
    "https://zulip.example.test",
    "one@example.test",
    "not-a-real-api-key",
)
USER_TWO = ZulipUser(
    UUID("00000000-0000-0000-0000-000000000002"),
    "https://zulip.example.test",
    "two@example.test",
    "not-a-real-api-key",
)


def test_user_representation_hides_api_key() -> None:
    assert "not-a-real-api-key" not in repr(USER_ONE)


def test_directory_cache_coalesces_concurrent_endpoint_loads() -> None:
    cache = EndpointDirectoryCache(60.0)
    calls = 0
    lock = threading.Lock()
    directory = [
        ZulipDirectoryUser(10, "one@example.test", "Current User", 400, False, False)
    ]

    def load() -> tuple[list[ZulipDirectoryUser], UserDirectoryWrite]:
        nonlocal calls
        with lock:
            calls += 1
        time.sleep(0.02)
        return directory, UserDirectoryWrite(1, 1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(lambda _: cache.get_or_load(USER_ONE.endpoint, load), range(8))
        )

    assert calls == 1
    assert all(result[0] == tuple(directory) for result in results)
    assert sorted(result[1].changed for result in results) == [0] * 7 + [1]


class FakeStore:
    def __init__(self) -> None:
        self.users = [USER_ONE, USER_TWO]
        self.queues: list[tuple[UUID, str, int]] = []
        self.cleared: list[tuple[UUID, str]] = []
        self.batches: list[tuple[UUID, str, list[ZulipEvent], int]] = []
        self.statuses: list[tuple[UUID, UserStatus]] = []
        self.catalogs: list[tuple[UUID, str, ZulipChatCatalog]] = []
        self.live_message_counts: list[int] = []
        self.history_begins = 0
        self.stored = asyncio.Event()

    async def set_user_identity(
        self,
        user_uuid: UUID,
        endpoint: str,
        zulip_user_id: int,
        full_name: str,
        role: int,
    ) -> bool:
        return True

    async def begin_catalog_fill(self, user_uuid: UUID, queue_id: str) -> bool:
        self.statuses.append((user_uuid, "filling"))
        return True

    async def list_pending_history_chats(
        self, user_uuid: UUID, queue_id: str
    ) -> list[object]:
        return []

    async def reconcile_chat_schedules(self) -> ChatScheduleReconcile:
        return ChatScheduleReconcile(0, 0, 0)

    async def store_user_directory(
        self, endpoint: str, users: list[ZulipDirectoryUser]
    ) -> UserDirectoryWrite:
        return UserDirectoryWrite(humans=2, changed=2)

    async def list_user_chat_keys(self, user_uuid: UUID) -> set[str]:
        return {"channel:7", "direct:10,12"}

    async def begin_history(self, user_uuid: UUID, queue_id: str) -> "FakeHistory":
        self.history_begins += 1
        return FakeHistory()

    async def apply_live_messages(
        self,
        user_uuid: UUID,
        queue_id: str,
        chats: object,
        messages: object,
        deleted_message_ids: object,
    ) -> LiveMessageWrite:
        self.live_message_counts.append(len(messages))  # type: ignore[arg-type]
        return LiveMessageWrite(len(messages), 0, 0, 0)  # type: ignore[arg-type]

    async def list_users(self) -> list[ZulipUser]:
        return self.users

    async def set_queue(
        self, user_uuid: UUID, queue_id: str, last_event_id: int
    ) -> bool:
        self.queues.append((user_uuid, queue_id, last_event_id))
        self.statuses.append((user_uuid, "streaming"))
        return True

    async def clear_queue(self, user_uuid: UUID, queue_id: str) -> bool:
        self.cleared.append((user_uuid, queue_id))
        self.statuses.append((user_uuid, "init"))
        return True

    async def set_user_status(
        self, user_uuid: UUID, queue_id: str, status: UserStatus
    ) -> bool:
        self.statuses.append((user_uuid, status))
        return True

    async def get_user_status(
        self, user_uuid: UUID, queue_id: str
    ) -> UserStatus | None:
        return "active"

    async def store_chat_catalog(
        self,
        user_uuid: UUID,
        queue_id: str,
        catalog: ZulipChatCatalog,
    ) -> ChatCatalogWrite:
        self.catalogs.append((user_uuid, queue_id, catalog))
        self.statuses.append((user_uuid, "scheduling"))
        return ChatCatalogWrite(True, False, len(catalog.chats), 0)

    async def store_events(
        self,
        user_uuid: UUID,
        queue_id: str,
        events: list[ZulipEvent],
        last_event_id: int,
    ) -> tuple[int, bool]:
        self.batches.append((user_uuid, queue_id, events, last_event_id))
        self.stored.set()
        return len(events), True


class FakeApi:
    def __init__(self) -> None:
        self.closed = threading.Event()
        self.polls = 0

    def register(self) -> RegisteredQueue:
        return RegisteredQueue(
            "queue-1",
            -1,
            90,
            (RecentPrivateConversation((12,), 100),),
        )

    def get_own_user(self) -> ZulipIdentity:
        return ZulipIdentity(10, "Current User", 400)

    def get_subscriptions(self) -> list[dict[str, object]]:
        return [{"stream_id": 7, "name": "Engineering"}]

    def get_users(self) -> list[ZulipDirectoryUser]:
        return [
            ZulipDirectoryUser(
                10, "one@example.test", "Current User", 400, False, False
            ),
            ZulipDirectoryUser(
                12, "two@example.test", "Second User", 400, False, False
            ),
        ]

    def get_direct_messages_page(
        self, anchor: str | int, *, include_anchor: bool
    ) -> DirectMessagePage:
        return DirectMessagePage(
            messages=[
                {
                    "id": 100,
                    "display_recipient": [
                        {"id": 10, "full_name": "Current User"},
                        {"id": 12, "full_name": "Second User"},
                    ],
                }
            ],
            found_oldest=True,
        )

    def get_messages_page(
        self,
        anchor: str | int,
        *,
        include_anchor: bool,
        narrow: object = None,
    ) -> MessagePage:
        return MessagePage(
            messages=[
                {
                    "id": 100,
                    "type": "private",
                    "sender_id": 10,
                    "content": "hello",
                    "timestamp": 1_700_000_000,
                    "flags": ["read"],
                    "reactions": [],
                    "display_recipient": [
                        {"id": 10, "full_name": "Current User"},
                        {"id": 12, "full_name": "Second User"},
                    ],
                }
            ],
            found_oldest=True,
        )

    def get_messages_by_ids(self, message_ids: list[int]) -> list[dict[str, object]]:
        return [
            {
                "id": message_id,
                "type": "private",
                "sender_id": 12,
                "content": "live",
                "timestamp": 1_700_000_001,
                "flags": [],
                "reactions": [],
                "display_recipient": [
                    {"id": 10, "full_name": "Current User"},
                    {"id": 12, "full_name": "Second User"},
                ],
            }
            for message_id in message_ids
        ]

    def get_events(
        self, queue_id: str, last_event_id: int, timeout: float
    ) -> list[dict[str, object]]:
        self.polls += 1
        if self.polls == 1:
            return [
                {"id": 1, "type": "heartbeat"},
                {"id": 2, "type": "message", "message": {"id": 42}},
            ]
        self.closed.wait(5)
        return []

    def close(self) -> None:
        self.closed.set()


class FakeHistory:
    async def store_chats(self, chats: object) -> int:
        return 0

    async def store_page(self, messages: object) -> MessagePageWrite:
        return MessagePageWrite(1, 1, 0, 0, 0)

    async def finish(self, chat_keys: object) -> HistoryWrite:
        return HistoryWrite(True, 0, 0, 1)

    async def close(self) -> None:
        return None


def test_one_thread_persists_a_batch_and_advances_past_heartbeat() -> None:
    asyncio.run(_thread_test())


async def _thread_test() -> None:
    store = FakeStore()
    api = FakeApi()
    worker = ZulipEventThread(
        USER_ONE,
        store,  # type: ignore[arg-type]
        asyncio.get_running_loop(),
        Settings(database_dsn="postgresql:///test"),
        threading.BoundedSemaphore(1),
        api_factory=lambda user: api,  # type: ignore[arg-type]
    )

    worker.start()
    await asyncio.wait_for(store.stored.wait(), timeout=2)
    worker.stop()
    await asyncio.to_thread(worker.join, 2)

    assert not worker.is_alive()
    assert store.queues == [(USER_ONE.uuid, "queue-1", -1)]
    assert [status for _, status in store.statuses] == [
        "streaming",
        "filling",
        "scheduling",
    ]
    assert len(store.catalogs) == 1
    assert [chat.chat_key for chat in store.catalogs[0][2].chats] == [
        "channel:7",
        "direct:10,12",
    ]
    assert len(store.batches) == 1
    assert store.live_message_counts == []
    user_uuid, queue_id, events, last_event_id = store.batches[0]
    assert user_uuid == USER_ONE.uuid
    assert queue_id == "queue-1"
    assert [(event.event_id, event.event_type) for event in events] == [(2, "message")]
    assert last_event_id == 2


def test_active_user_resumes_valid_queue_without_reloading_history() -> None:
    asyncio.run(_resume_queue_test())


async def _resume_queue_test() -> None:
    store = FakeStore()
    api = FakeApi()
    active_user = ZulipUser(
        uuid=USER_ONE.uuid,
        endpoint=USER_ONE.endpoint,
        login=USER_ONE.login,
        api_key=USER_ONE.api_key,
        queue_id="queue-1",
        last_event_id=-1,
        status="active",
    )
    worker = ZulipEventThread(
        active_user,
        store,  # type: ignore[arg-type]
        asyncio.get_running_loop(),
        Settings(database_dsn="postgresql:///test"),
        threading.BoundedSemaphore(1),
        api_factory=lambda user: api,  # type: ignore[arg-type]
    )

    worker.start()
    await asyncio.wait_for(store.stored.wait(), timeout=2)
    worker.stop()
    await asyncio.to_thread(worker.join, 2)

    assert not worker.is_alive()
    assert store.queues == []
    assert store.statuses == []
    assert store.history_begins == 0
    assert store.catalogs == []
    assert len(store.batches) == 1
    assert store.live_message_counts == []


class SlowCloseApi(FakeApi):
    def __init__(self) -> None:
        super().__init__()
        self.polling = threading.Event()
        self.release = threading.Event()

    def get_events(
        self, queue_id: str, last_event_id: int, timeout: float
    ) -> list[dict[str, object]]:
        self.polling.set()
        self.release.wait(5)
        return []

    def close(self) -> None:
        self.release.wait(5)


def test_stop_does_not_block_on_an_active_http_request() -> None:
    asyncio.run(_nonblocking_stop_test())


async def _nonblocking_stop_test() -> None:
    store = FakeStore()
    api = SlowCloseApi()
    worker = ZulipEventThread(
        USER_ONE,
        store,  # type: ignore[arg-type]
        asyncio.get_running_loop(),
        Settings(database_dsn="postgresql:///test"),
        threading.BoundedSemaphore(1),
        api_factory=lambda user: api,  # type: ignore[arg-type]
    )

    worker.start()
    assert await asyncio.to_thread(api.polling.wait, 2)
    started = time.monotonic()
    worker.stop()
    elapsed = time.monotonic() - started
    api.release.set()
    await asyncio.to_thread(worker.join, 2)

    assert elapsed < 0.2
    assert not worker.is_alive()


class ExpiredQueueApi(FakeApi):
    def __init__(self) -> None:
        super().__init__()
        self.catalog_reads = 0

    def register(self) -> RegisteredQueue:
        return RegisteredQueue("fresh-queue", -1, 90)

    def get_own_user(self) -> ZulipIdentity:
        self.catalog_reads += 1
        return super().get_own_user()

    def get_events(
        self, queue_id: str, last_event_id: int, timeout: float
    ) -> list[dict[str, object]]:
        if queue_id == "expired-queue":
            raise ZulipApiError("BAD_EVENT_QUEUE_ID", retryable=True)
        if self.polls == 0:
            self.polls += 1
            return [{"id": 0, "type": "message", "message": {"id": 43}}]
        self.closed.wait(5)
        return []


def test_expired_queue_is_cleared_and_registered_again() -> None:
    asyncio.run(_expired_queue_test())


async def _expired_queue_test() -> None:
    store = FakeStore()
    api = ExpiredQueueApi()
    user = ZulipUser(
        USER_ONE.uuid,
        USER_ONE.endpoint,
        USER_ONE.login,
        USER_ONE.api_key,
        "expired-queue",
        7,
    )
    worker = ZulipEventThread(
        user,
        store,  # type: ignore[arg-type]
        asyncio.get_running_loop(),
        Settings(
            database_dsn="postgresql:///test",
            zulip_retry_base_seconds=0.001,
            zulip_retry_cap_seconds=0.001,
        ),
        threading.BoundedSemaphore(1),
        api_factory=lambda selected_user: api,  # type: ignore[arg-type]
    )

    worker.start()
    await asyncio.wait_for(store.stored.wait(), timeout=2)
    worker.stop()
    await asyncio.to_thread(worker.join, 2)

    assert store.cleared == [(user.uuid, "expired-queue")]
    assert store.queues == [(user.uuid, "fresh-queue", -1)]
    assert api.catalog_reads == 2
    assert [queue_id for _, queue_id, _ in store.catalogs] == [
        "expired-queue",
        "fresh-queue",
    ]
    assert [status for _, status in store.statuses] == [
        "streaming",
        "filling",
        "scheduling",
        "init",
        "streaming",
        "filling",
        "scheduling",
    ]
    assert store.batches[0][1] == "fresh-queue"
    assert store.batches[0][3] == 0


class RejectedApi(FakeApi):
    def __init__(self) -> None:
        super().__init__()
        self.attempted = threading.Event()
        self.attempts = 0

    def register(self) -> RegisteredQueue:
        self.attempts += 1
        self.attempted.set()
        raise ZulipApiError("UNAUTHORIZED", retryable=False)


def test_nonretryable_api_error_parks_worker_until_configuration_changes() -> None:
    asyncio.run(_rejected_api_test())


async def _rejected_api_test() -> None:
    store = FakeStore()
    api = RejectedApi()
    worker = ZulipEventThread(
        USER_ONE,
        store,  # type: ignore[arg-type]
        asyncio.get_running_loop(),
        Settings(database_dsn="postgresql:///test"),
        threading.BoundedSemaphore(1),
        api_factory=lambda user: api,  # type: ignore[arg-type]
    )

    worker.start()
    assert await asyncio.to_thread(api.attempted.wait, 2)
    await asyncio.sleep(0.05)

    assert worker.is_alive()
    assert api.attempts == 1

    worker.stop()
    await asyncio.to_thread(worker.join, 2)
    assert not worker.is_alive()


class FakeWorker:
    def __init__(self, user: ZulipUser) -> None:
        self.user = user
        self.alive = False
        self.stopped = False

    def start(self) -> None:
        self.alive = True

    def stop(self) -> None:
        self.alive = False
        self.stopped = True

    def is_alive(self) -> bool:
        return self.alive


def test_supervisor_owns_exactly_one_thread_per_user() -> None:
    asyncio.run(_supervisor_test())


async def _supervisor_test() -> None:
    store = FakeStore()
    workers: list[FakeWorker] = []

    def worker_factory(
        user: ZulipUser,
        registration_gate: threading.Semaphore,
    ) -> FakeWorker:
        worker = FakeWorker(user)
        workers.append(worker)
        return worker

    supervisor = ZulipThreadSupervisor(
        store,  # type: ignore[arg-type]
        asyncio.get_running_loop(),
        Settings(database_dsn="postgresql:///test"),
        worker_factory=worker_factory,  # type: ignore[arg-type]
    )
    await supervisor.reconcile()
    await supervisor.reconcile()

    assert len(workers) == 2
    assert all(worker.alive for worker in workers)

    store.users = [USER_TWO]
    await supervisor.reconcile()

    assert workers[0].stopped
    assert workers[1].alive
