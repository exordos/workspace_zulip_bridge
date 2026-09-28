# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from uuid import UUID

from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.event_store import _complete_attachment_file_uuids


def test_direct_chat_writes_do_not_wait_for_unrelated_catalogs() -> None:
    asyncio.run(_direct_chat_writes_do_not_wait_for_unrelated_catalogs())


async def _direct_chat_writes_do_not_wait_for_unrelated_catalogs() -> None:
    store = EventStore(cast(Any, object()))
    first_entered = asyncio.Event()
    release = asyncio.Event()
    active = 0
    maximum_active = 0

    async def fake_store(*_args: object) -> bool:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        first_entered.set()
        await release.wait()
        active -= 1
        return True

    store._store_direct_message_chat = fake_store  # type: ignore[method-assign]
    first = asyncio.create_task(store.store_direct_message_chat(*_direct_chat_args()))
    await first_entered.wait()
    second = asyncio.create_task(store.store_direct_message_chat(*_direct_chat_args()))
    await asyncio.sleep(0)

    assert maximum_active == 2
    release.set()
    assert await asyncio.gather(first, second) == [True, True]
    assert maximum_active == 2


def _direct_chat_args() -> tuple[UUID, str, dict[str, object]]:
    return UUID("10000000-0000-0000-0000-000000000001"), "queue", {}


def test_attachment_metadata_recovers_a_concurrent_upsert() -> None:
    asyncio.run(_attachment_metadata_recovers_a_concurrent_upsert())


async def _attachment_metadata_recovers_a_concurrent_upsert() -> None:
    realm_uuid = UUID("10000000-0000-0000-0000-000000000001")
    first_uuid = UUID("10000000-0000-0000-0000-000000000002")
    concurrent_uuid = UUID("10000000-0000-0000-0000-000000000003")
    connection = AsyncMock()
    connection.fetch.return_value = [
        {"uuid": concurrent_uuid, "source_path": "/user_uploads/b.png"}
    ]

    resolved = await _complete_attachment_file_uuids(
        connection,
        realm_uuid,
        ("/user_uploads/a.png", "/user_uploads/b.png"),
        {"/user_uploads/a.png": first_uuid},
    )

    assert resolved == {
        "/user_uploads/a.png": first_uuid,
        "/user_uploads/b.png": concurrent_uuid,
    }
    connection.fetch.assert_awaited_once()
    assert connection.fetch.await_args.args[1:] == (
        realm_uuid,
        ["/user_uploads/b.png"],
    )
