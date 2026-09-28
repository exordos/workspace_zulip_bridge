# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID

import httpx

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.workspace_chat_catalog import WorkspaceChatCatalogWorker


def test_catalog_report_waits_for_shared_control_capacity(tmp_path: Path) -> None:
    asyncio.run(_catalog_report_waits_for_shared_control_capacity(tmp_path))


async def _catalog_report_waits_for_shared_control_capacity(tmp_path: Path) -> None:
    semaphore = asyncio.Semaphore(1)
    await semaphore.acquire()
    settings = Settings(
        database_dsn="postgresql:///unused",
        workspace_control_url="https://127.0.0.1",
        workspace_bridge_instance_uuid=UUID("10000000-0000-4000-8000-000000000001"),
        workspace_control_state_dir=tmp_path,
    )
    worker = WorkspaceChatCatalogWorker(
        object(),  # type: ignore[arg-type]
        settings,
        control_semaphore=semaphore,
    )
    report_uuid = "10000000-0000-4000-8000-000000000002"
    response = httpx.Response(
        200,
        json={"results": [{"report_uuid": report_uuid, "status": "applied"}]},
    )
    client = AsyncMock()
    client.post.return_value = response
    context = AsyncMock()
    context.__aenter__.return_value = client
    worker._client = lambda: context  # type: ignore[method-assign]

    task = asyncio.create_task(worker._send_report({"report_uuid": report_uuid}))
    await asyncio.sleep(0)
    client.post.assert_not_awaited()
    semaphore.release()

    assert await task == "applied"
    client.post.assert_awaited_once()


def test_non_coordinator_only_delivers_catalog_reports(tmp_path: Path) -> None:
    asyncio.run(_non_coordinator_only_delivers_catalog_reports(tmp_path))


async def _non_coordinator_only_delivers_catalog_reports(tmp_path: Path) -> None:
    settings = Settings(
        database_dsn="postgresql:///unused",
        workspace_control_url="https://127.0.0.1",
        workspace_bridge_instance_uuid=UUID("10000000-0000-4000-8000-000000000001"),
        workspace_control_state_dir=tmp_path,
    )
    worker = WorkspaceChatCatalogWorker(
        object(),  # type: ignore[arg-type]
        settings,
        coordinate=False,
    )
    worker._refresh_catalogs = AsyncMock(  # type: ignore[method-assign]
        side_effect=AssertionError("non-coordinator refreshed catalogs")
    )
    worker._retire_unneeded_reports = AsyncMock(  # type: ignore[method-assign]
        side_effect=AssertionError("non-coordinator retired reports")
    )
    worker._claim_report = AsyncMock(return_value=None)  # type: ignore[method-assign]

    assert await worker.process_once() == 0
    worker._claim_report.assert_awaited_once()


def test_coordinator_refreshes_priority_catalogs_before_pending_reports(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _coordinator_refreshes_priority_catalogs_before_pending_reports(tmp_path)
    )


async def _coordinator_refreshes_priority_catalogs_before_pending_reports(
    tmp_path: Path,
) -> None:
    settings = Settings(
        database_dsn="postgresql:///unused",
        workspace_control_url="https://127.0.0.1",
        workspace_bridge_instance_uuid=UUID("10000000-0000-4000-8000-000000000001"),
        workspace_control_state_dir=tmp_path,
    )
    worker = WorkspaceChatCatalogWorker(
        object(),  # type: ignore[arg-type]
        settings,
    )
    report_uuid = UUID("10000000-0000-4000-8000-000000000002")
    worker._refresh_catalogs = AsyncMock(return_value=20)  # type: ignore[method-assign]
    worker._claim_report = AsyncMock(  # type: ignore[method-assign]
        return_value={"report_uuid": report_uuid, "report": {}}
    )
    worker._send_report = AsyncMock(return_value="applied")  # type: ignore[method-assign]
    worker._finish_report = AsyncMock()  # type: ignore[method-assign]

    assert await worker.process_once() == 21
    worker._refresh_catalogs.assert_awaited_once_with(priority_only=True)
    worker._claim_report.assert_awaited_once()
    worker._send_report.assert_awaited_once_with({})
    worker._finish_report.assert_awaited_once_with(report_uuid, "applied")
