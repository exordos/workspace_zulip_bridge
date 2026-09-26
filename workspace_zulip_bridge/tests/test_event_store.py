# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from typing import Any
from typing import cast
from uuid import UUID

from workspace_zulip_bridge.event_store import EventStore


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
