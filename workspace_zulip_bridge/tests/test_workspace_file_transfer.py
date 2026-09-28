# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import hashlib
import io
from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import UUID

import httpx
import pytest

from workspace_zulip_bridge import workspace_file_transfer
from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.stable_ids import stable_external_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_outgoing_file_transfer_uuid
from workspace_zulip_bridge.workspace_file_transfer import WorkspaceFileTransferWorker
from workspace_zulip_bridge.workspace_file_transfer import WorkspaceOutgoingFileReader
from workspace_zulip_bridge.workspace_file_transfer import replace_source_file_urn
from workspace_zulip_bridge.workspace_file_transfer import workspace_file_name

SOURCE_UUID = UUID("10000000-0000-0000-0000-000000000001")
TARGET_UUID = UUID("20000000-0000-0000-0000-000000000002")


class SeedPool:
    def __init__(self, *, has_reserve: bool) -> None:
        self.has_reserve = has_reserve
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    async def __aenter__(self) -> "SeedPool":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def acquire(self) -> "SeedPool":
        return self

    def transaction(self) -> "SeedPool":
        return self

    async def fetch(self, query: str, *args: object) -> list[object]:
        self.fetch_calls.append((query, args))
        return []

    async def fetchval(self, query: str, *args: object) -> bool:
        self.fetchval_calls.append((query, args))
        return self.has_reserve

    async def execute(self, _query: str, *_args: object) -> str:
        return "UPDATE 0"


class ClaimPool(SeedPool):
    def __init__(self) -> None:
        super().__init__(has_reserve=False)
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query: str, *args: object) -> None:
        self.fetchrow_calls.append((query, args))
        return None


class FinalizeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def __aenter__(self) -> "FinalizeConnection":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def transaction(self) -> "FinalizeConnection":
        return self

    async def fetchrow(self, _query: str, *_args: object) -> dict[str, object]:
        return {
            "file_uuid": SOURCE_UUID,
            "zulip_stream_uuid": TARGET_UUID,
            "processing_status": "processing",
            "claimed_at": None,
            "workspace_urn": None,
        }

    async def fetch(self, query: str, *_args: object) -> list[dict[str, object]]:
        if "SELECT message.uuid\n" in query:
            return [{"uuid": SOURCE_UUID}]
        if "SELECT link.message_uuid, link.file_uuid" in query:
            return []
        if "SELECT message_uuid, file_uuid" in query:
            return [{"message_uuid": SOURCE_UUID, "file_uuid": SOURCE_UUID}]
        return [
            {
                "uuid": SOURCE_UUID,
                "workspace_content": (
                    f"![native](urn:file:{SOURCE_UUID}?name=native.png)"
                ),
                "sender_user_uuid": TARGET_UUID,
                "chat_key": "channel:42",
                "topic_name": "general",
                "created_at": datetime(2026, 9, 27, tzinfo=UTC),
            }
        ]

    async def executemany(
        self,
        query: str,
        args: list[object] | list[tuple[object, ...]],
    ) -> None:
        self.executed.append((query, (args,)))

    async def execute(self, query: str, *args: object) -> str:
        self.executed.append((query, args))
        return "UPDATE 1"


class FinalizePool:
    def __init__(self) -> None:
        self.connection = FinalizeConnection()

    def acquire(self) -> FinalizeConnection:
        return self.connection


def _worker(pool: SeedPool) -> WorkspaceFileTransferWorker:
    return WorkspaceFileTransferWorker(
        pool,  # type: ignore[arg-type]
        Settings(
            database_dsn="postgresql:///unused",
            workspace_control_url="https://control.example.invalid",
        ),
    )


def test_external_chat_identity_stays_compatible_with_existing_catalogs() -> None:
    assert stable_external_chat_uuid(
        UUID("10000000-0000-0000-0000-000000000003"),
        "channel:42",
    ) == UUID("2a239a52-7e3f-5db9-9631-996c5d7581c4")


def test_outgoing_workspace_file_is_authorized_and_verified() -> None:
    account_uuid = UUID("10000000-0000-0000-0000-000000000003")
    chat_uuid = UUID("10000000-0000-0000-0000-000000000004")
    content = b"native workspace file"
    file_urn = f"urn:file:{SOURCE_UUID}"
    transfer_uuid = stable_outgoing_file_transfer_uuid(
        SOURCE_UUID,
        account_uuid,
        chat_uuid,
    )
    requests: list[httpx.Request] = []

    def control_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "PUT"
        assert request.url.path.endswith(f"/v1/file-transfers/outgoing/{transfer_uuid}")
        return httpx.Response(
            200,
            json={
                "transfer_uuid": str(transfer_uuid),
                "operation_uuid": str(transfer_uuid),
                "status": "ready",
                "authorization_generation": 1,
                "file_uuid": str(SOURCE_UUID),
                "file_urn": file_urn,
                "name": "test.txt",
                "size_bytes": len(content),
                "content_type": "text/plain",
                "sha256": hashlib.sha256(content).hexdigest(),
                "download": {
                    "method": "GET",
                    "url": "https://files.example.invalid/object",
                    "headers": {"x-test-token": "signed"},
                },
            },
        )

    def download_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.headers["x-test-token"] == "signed"
        return httpx.Response(200, content=content)

    async def run() -> None:
        reader = WorkspaceOutgoingFileReader(
            Settings(
                database_dsn="postgresql:///unused",
                workspace_control_url="https://control.example.invalid",
            ),
            control_transport=httpx.MockTransport(control_handler),
            download_transport=httpx.MockTransport(download_handler),
        )
        try:
            assert await reader.read(file_urn, account_uuid, chat_uuid) == (
                "test.txt",
                "text/plain",
                content,
            )
        finally:
            await reader.close()

    asyncio.run(run())
    assert len(requests) == 2


@pytest.mark.parametrize("kind", ("file", "image", "video"))
def test_source_placeholder_is_replaced_with_native_workspace_urn(kind: str) -> None:
    content = f"before ![asset](urn:file:{SOURCE_UUID}?name=asset.png) after"
    target = f"urn:{kind}:{TARGET_UUID}"

    assert replace_source_file_urn(content, SOURCE_UUID, target) == (
        f"before ![asset]({target}?name=asset.png) after"
    )


def test_unrelated_file_reference_is_unchanged() -> None:
    content = f"[asset](urn:file:{TARGET_UUID})"
    assert (
        replace_source_file_urn(
            content,
            SOURCE_UUID,
            f"urn:file:{TARGET_UUID}",
        )
        == content
    )


def test_invalid_native_urn_is_rejected() -> None:
    with pytest.raises(ValueError, match="invalid Workspace file URN"):
        replace_source_file_urn("content", SOURCE_UUID, "https://example.invalid")


def test_workspace_file_name_is_normalized_and_bounded() -> None:
    assert workspace_file_name("  a/b\\c\x00e\u0301.png  ") == "a_b_c_é.png"
    assert len(workspace_file_name("я" * 200).encode("utf-8")) <= 255
    assert workspace_file_name("\x00/\\") == "___"


def test_file_backfill_skips_reseed_while_runnable_reserve_is_full() -> None:
    pool = SeedPool(has_reserve=True)

    assert asyncio.run(_worker(pool)._seed_projection_jobs()) == 0

    assert len(pool.fetch_calls) == 1
    assert "sync_diffs" in pool.fetch_calls[0][0]
    assert len(pool.fetchval_calls) == 1
    reserve_query, reserve_args = pool.fetchval_calls[0]
    assert reserve_args == (
        1_000,
        workspace_file_transfer.CATALOG_PROJECTION_REVISION,
    )
    assert "projection.available_at <= clock_timestamp()" in reserve_query
    assert "connection.sync_enabled" in reserve_query
    assert "catalog.processing_status = 'reported'" in reserve_query
    assert "catalog.projection_revision >= $2" in reserve_query


def test_file_backfill_seeds_a_large_newest_first_candidate_page() -> None:
    pool = SeedPool(has_reserve=False)

    assert asyncio.run(_worker(pool)._seed_projection_jobs()) == 0

    assert len(pool.fetch_calls) == 2
    candidate_query, candidate_args = pool.fetch_calls[1]
    assert candidate_args == (20_000,)
    assert "candidate_files AS MATERIALIZED" in candidate_query
    assert "ORDER BY outbox.sequence" in candidate_query
    assert "LIMIT $1" in candidate_query


def test_file_claim_waits_for_current_catalog_and_uses_ready_index_order() -> None:
    pool = ClaimPool()

    assert asyncio.run(_worker(pool)._claim_job()) is None

    query, args = pool.fetchrow_calls[0]
    assert args == (workspace_file_transfer.CATALOG_PROJECTION_REVISION,)
    assert "catalog.processing_status = 'reported'" in query
    assert "catalog.projection_revision >= $1" in query
    assert "projection.available_at, projection.created_at, projection.uuid" in query


def test_staged_source_is_downloaded_once_and_reused_for_upload() -> None:
    asyncio.run(_staged_source_is_downloaded_once_and_reused_for_upload())


async def _staged_source_is_downloaded_once_and_reused_for_upload() -> None:
    payload = b"native workspace file bytes"
    source_calls = 0
    uploaded: list[bytes] = []

    async def source_handler(request: httpx.Request) -> httpx.Response:
        nonlocal source_calls
        source_calls += 1
        return httpx.Response(
            200,
            headers={"Content-Type": "image/png"},
            content=payload,
            request=request,
        )

    async def upload_handler(request: httpx.Request) -> httpx.Response:
        uploaded.append(await request.aread())
        return httpx.Response(200, request=request)

    worker = _worker(SeedPool(has_reserve=False))
    worker._source_http = httpx.AsyncClient(
        transport=httpx.MockTransport(source_handler)
    )
    worker._upload_http = httpx.AsyncClient(
        transport=httpx.MockTransport(upload_handler)
    )
    job = workspace_file_transfer._Job(
        projection_uuid=SOURCE_UUID,
        operation_uuid=SOURCE_UUID,
        file_uuid=SOURCE_UUID,
        stream_uuid=TARGET_UUID,
        realm_uuid=TARGET_UUID,
        external_account_uuid=TARGET_UUID,
        external_chat_uuid=TARGET_UUID,
        endpoint="https://source.example.invalid",
        login="test@example.invalid",
        api_key="test-key",
        source_path="user_uploads/native.png",
        name="native.png",
    )
    staged = await worker._stage_source(job)
    try:
        assert staged.descriptor.size_bytes == len(payload)
        assert staged.descriptor.sha256 == hashlib.sha256(payload).hexdigest()
        await worker._upload(
            staged,
            {
                "method": "PUT",
                "url": "https://upload.example.invalid/native.png",
                "headers": {
                    "Content-Length": str(len(payload)),
                    "Content-Type": "image/png",
                },
            },
        )
    finally:
        staged.content.close()
        await worker._close_clients()

    assert source_calls == 1
    assert uploaded == [payload]


def test_finalized_live_file_wakes_its_waiting_message() -> None:
    asyncio.run(_finalized_live_file_wakes_its_waiting_message())


async def _finalized_live_file_wakes_its_waiting_message() -> None:
    pool = FinalizePool()
    worker = WorkspaceFileTransferWorker(
        pool,  # type: ignore[arg-type]
        Settings(
            database_dsn="postgresql:///unused",
            workspace_control_url="https://127.0.0.1",
        ),
    )
    job = workspace_file_transfer._Job(
        projection_uuid=SOURCE_UUID,
        operation_uuid=SOURCE_UUID,
        file_uuid=SOURCE_UUID,
        stream_uuid=TARGET_UUID,
        realm_uuid=TARGET_UUID,
        external_account_uuid=TARGET_UUID,
        external_chat_uuid=TARGET_UUID,
        endpoint="https://127.0.0.1",
        login="test@example.invalid",
        api_key="test-key",
        source_path="user_uploads/native.png",
        name="native.png",
    )
    descriptor = workspace_file_transfer._Descriptor(
        size_bytes=7,
        content_type="image/png",
        sha256=hashlib.sha256(b"content").hexdigest(),
    )

    await worker._finalize_job(job, descriptor, f"urn:image:{TARGET_UUID}")

    wake_queries = [
        (query, args)
        for query, args in pool.connection.executed
        if "UPDATE workspace_zulip_bridge.sync_diffs" in query
    ]
    assert len(wake_queries) == 1
    assert wake_queries[0][1] == ([SOURCE_UUID],)
    assert "dependency_wait_count = 0" in wake_queries[0][0]
    assert "delivery_priority = 0" in wake_queries[0][0]


def test_control_calls_share_one_gate_across_file_workers() -> None:
    asyncio.run(_control_calls_share_one_gate_across_file_workers())


async def _control_calls_share_one_gate_across_file_workers() -> None:
    active = 0
    maximum_active = 0

    async def control_handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return httpx.Response(
            200,
            json={
                "status": "finalized",
                "file_urn": f"urn:file:{TARGET_UUID}",
                "size_bytes": 0,
                "content_type": "application/octet-stream",
                "sha256": hashlib.sha256(b"").hexdigest(),
            },
            request=request,
        )

    control_semaphore = asyncio.Semaphore(2)
    workers = [
        WorkspaceFileTransferWorker(
            object(),  # type: ignore[arg-type]
            Settings(
                database_dsn="postgresql:///unused",
                workspace_control_url="https://control.example.invalid",
            ),
            control_semaphore=control_semaphore,
        )
        for _index in range(4)
    ]
    for worker in workers:
        worker._control_http = httpx.AsyncClient(
            base_url="https://control.example.invalid",
            transport=httpx.MockTransport(control_handler),
        )
    job = workspace_file_transfer._Job(
        projection_uuid=SOURCE_UUID,
        operation_uuid=SOURCE_UUID,
        file_uuid=SOURCE_UUID,
        stream_uuid=TARGET_UUID,
        realm_uuid=TARGET_UUID,
        external_account_uuid=TARGET_UUID,
        external_chat_uuid=TARGET_UUID,
        endpoint="https://source.example.invalid",
        login="test@example.invalid",
        api_key="test-key",
        source_path="user_uploads/native.png",
        name="native.png",
    )
    descriptor = workspace_file_transfer._Descriptor(
        size_bytes=0,
        content_type="application/octet-stream",
        sha256=hashlib.sha256(b"").hexdigest(),
    )
    try:
        await asyncio.gather(
            *(
                worker._transfer(
                    job,
                    workspace_file_transfer._StagedSource(
                        descriptor,
                        io.BytesIO(),
                    ),
                )
                for worker in workers
            )
        )
    finally:
        await asyncio.gather(*(worker._close_clients() for worker in workers))

    assert maximum_active == 2


def test_live_file_is_confirmed_after_finalize_before_message_release() -> None:
    asyncio.run(_live_file_is_confirmed_after_finalize_before_message_release())


async def _live_file_is_confirmed_after_finalize_before_message_release() -> None:
    payload = b"live image"
    descriptor = workspace_file_transfer._Descriptor(
        size_bytes=len(payload),
        content_type="image/png",
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    control_calls: list[str] = []

    async def control_handler(request: httpx.Request) -> httpx.Response:
        control_calls.append(f"{request.method} {request.url.path}")
        response = {
            "status": "finalized",
            "file_urn": f"urn:image:{TARGET_UUID}",
            "size_bytes": descriptor.size_bytes,
            "content_type": descriptor.content_type,
            "sha256": descriptor.sha256,
        }
        if len(control_calls) == 1:
            response = {
                "status": "allocated",
                "allocation_generation": 1,
                "upload": {
                    "method": "PUT",
                    "url": "https://upload.example.invalid/live.png",
                    "headers": {
                        "Content-Length": str(descriptor.size_bytes),
                        "Content-Type": descriptor.content_type,
                    },
                },
            }
        return httpx.Response(200, json=response, request=request)

    async def upload_handler(request: httpx.Request) -> httpx.Response:
        assert await request.aread() == payload
        return httpx.Response(200, request=request)

    worker = _worker(SeedPool(has_reserve=False))
    worker._control_http = httpx.AsyncClient(
        base_url="https://control.example.invalid",
        transport=httpx.MockTransport(control_handler),
    )
    worker._upload_http = httpx.AsyncClient(
        transport=httpx.MockTransport(upload_handler),
    )
    job = workspace_file_transfer._Job(
        projection_uuid=SOURCE_UUID,
        operation_uuid=SOURCE_UUID,
        file_uuid=SOURCE_UUID,
        stream_uuid=TARGET_UUID,
        realm_uuid=TARGET_UUID,
        external_account_uuid=TARGET_UUID,
        external_chat_uuid=TARGET_UUID,
        endpoint="https://source.example.invalid",
        login="test@example.invalid",
        api_key="test-key",
        source_path="user_uploads/live.png",
        name="live.png",
        delivery_priority=0,
    )
    try:
        urn = await worker._transfer(
            job,
            workspace_file_transfer._StagedSource(
                descriptor,
                io.BytesIO(payload),
            ),
        )
    finally:
        await worker._close_clients()

    assert urn == f"urn:image:{TARGET_UUID}"
    assert control_calls == [
        f"PUT /v1/file-transfers/incoming/{SOURCE_UUID}",
        f"POST /v1/file-transfers/incoming/{SOURCE_UUID}/actions/finalize",
        f"PUT /v1/file-transfers/incoming/{SOURCE_UUID}",
    ]


def test_control_client_loads_bridge_identity_into_tls_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = tmp_path / "control"
    state.mkdir()
    for name in ("control-ca.pem", "bridge.crt", "bridge.key"):
        (state / name).write_text("placeholder", encoding="utf-8")

    class FakeContext:
        loaded_chain: tuple[str, str] | None = None

        def load_cert_chain(self, certificate: str, key: str) -> None:
            self.loaded_chain = (certificate, key)

    context = FakeContext()
    captured: dict[str, object] = {}
    client = object()

    def create_default_context(*, cafile: str) -> FakeContext:
        captured["cafile"] = cafile
        return context

    def async_client(**options: object) -> object:
        captured.update(options)
        return client

    monkeypatch.setattr(
        workspace_file_transfer.ssl,
        "create_default_context",
        create_default_context,
    )
    monkeypatch.setattr(workspace_file_transfer.httpx, "AsyncClient", async_client)

    worker = WorkspaceFileTransferWorker(
        object(),  # type: ignore[arg-type]
        Settings(
            database_dsn="postgresql:///unused",
            workspace_control_url="https://control.example.invalid",
            workspace_control_state_dir=state,
        ),
    )

    assert worker._control_client() is client
    assert captured["cafile"] == str(state / "control-ca.pem")
    assert captured["verify"] is context
    assert "cert" not in captured
    assert captured["headers"] == {"Accept": "application/json"}
    limits = captured["limits"]
    assert isinstance(limits, httpx.Limits)
    assert limits.max_connections == 2
    assert limits.max_keepalive_connections == 2
    assert context.loaded_chain == (
        str(state / "bridge.crt"),
        str(state / "bridge.key"),
    )
    assert isinstance(captured["timeout"], httpx.Timeout)
