# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Bridge-owned transfer of Zulip uploads into native Workspace files."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import re
import ssl
import unicodedata
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import httpx

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.message_history import message_content_hash
from workspace_zulip_bridge.stable_ids import stable_external_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_file_projection_uuid

LOG = logging.getLogger(__name__)
MAX_FILE_BYTES = 50 * 1024 * 1024
_CONTENT_TYPE = re.compile(r"^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$")
_WORKSPACE_URN = re.compile(r"^urn:(?:file|image|video):[0-9a-f-]{36}$")


class FileTransferError(RuntimeError):
    """A safe file-transfer error that never includes signed URLs."""

    def __init__(self, code: str, *, retryable: bool = True) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class _Job:
    projection_uuid: UUID
    operation_uuid: UUID
    file_uuid: UUID
    stream_uuid: UUID
    realm_uuid: UUID
    external_account_uuid: UUID
    external_chat_uuid: UUID
    endpoint: str
    login: str
    api_key: str
    source_path: str
    name: str


@dataclass(frozen=True, slots=True)
class _Descriptor:
    size_bytes: int
    content_type: str
    sha256: str


def replace_source_file_urn(content: str, source_uuid: UUID, target_urn: str) -> str:
    """Replace only the bridge's unresolved source-file placeholder."""

    if _WORKSPACE_URN.fullmatch(target_urn) is None:
        raise ValueError("invalid Workspace file URN")
    return content.replace(f"urn:file:{source_uuid}", target_urn)


def workspace_file_name(name: str) -> str:
    """Normalize a provider file name to the private file API contract."""

    value = unicodedata.normalize("NFC", name)
    value = "".join(
        "_" if ord(character) < 32 or character in "/\\" else character
        for character in value
    ).strip()
    value = value or "file"
    encoded = value.encode("utf-8")
    if len(encoded) <= 255:
        return value
    encoded = encoded[:255]
    while True:
        try:
            return encoded.decode("utf-8")
        except UnicodeDecodeError:
            encoded = encoded[:-1]


class WorkspaceFileTransferWorker:
    """Move provider bytes directly into Workspace object storage."""

    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        if settings.workspace_control_url is None:
            raise ValueError("Workspace control must be configured")
        self._pool = pool
        self._settings = settings
        self._control_url = settings.workspace_control_url.rstrip("/")
        self._state = settings.workspace_control_state_dir

    async def run(self) -> None:
        while True:
            try:
                changed = await self.process_once()
            except asyncio.CancelledError:
                raise
            except (TimeoutError, asyncpg.PostgresError, httpx.HTTPError) as error:
                LOG.warning(
                    "Workspace file transfer pass failed error=%s",
                    type(error).__name__,
                )
                changed = 0
            await asyncio.sleep(
                0.0 if changed else self._settings.workspace_sync_poll_seconds
            )

    async def process_once(self) -> int:
        seeded = await self._seed_projection_jobs()
        await self._complete_file_outbox()
        job = await self._claim_job()
        if job is None:
            return seeded
        try:
            descriptor = await self._probe_source(job)
            file_urn = await self._transfer(job, descriptor)
            await self._finalize_job(job, descriptor, file_urn)
        except asyncio.CancelledError:
            raise
        except FileTransferError as error:
            await self._fail_job(job, error.code, retryable=error.retryable)
        except (TimeoutError, httpx.HTTPError) as error:
            await self._fail_job(job, type(error).__name__, retryable=True)
        await self._complete_file_outbox(job.file_uuid)
        return seeded + 1

    async def _seed_projection_jobs(self) -> int:
        live_rows = await self._pool.fetch(
            """
            SELECT DISTINCT file.uuid AS file_uuid,
                   message.zulip_stream_uuid AS stream_uuid,
                   0::smallint AS delivery_priority
            FROM workspace_zulip_bridge.sync_diffs AS diff
            JOIN workspace_zulip_bridge.zulip_messages AS message
              ON message.uuid = diff.entity_uuid
            JOIN workspace_zulip_bridge.zulip_message_files AS link
              ON link.message_uuid = message.uuid
            JOIN workspace_zulip_bridge.zulip_files AS file
              ON file.uuid = link.file_uuid
            LEFT JOIN workspace_zulip_bridge.workspace_file_projections
                AS projection
              ON projection.file_uuid = file.uuid
             AND projection.zulip_stream_uuid = message.zulip_stream_uuid
            WHERE diff.entity_type = 'messages'
              AND diff.delivery_priority = 0
              AND diff.direction = 'to_workspace'
              AND diff.processing_status IN (
                  'pending', 'processing', 'failed', 'blocked'
              )
              AND message.workspace_content IS NOT NULL
              AND (
                  projection.uuid IS NULL OR projection.delivery_priority > 0
              )
            ORDER BY file.uuid, message.zulip_stream_uuid
            LIMIT 100
            """
        )
        remaining = 100 - len(live_rows)
        backfill_rows = []
        if remaining:
            backfill_rows = await self._pool.fetch(
                """
            WITH candidate_files AS MATERIALIZED (
                SELECT entity_uuid
                FROM workspace_zulip_bridge.workspace_outbox
                WHERE entity_type = 'file'
                  AND action = 'upsert'
                  AND delivery_status IN ('pending', 'failed')
                  AND available_at <= clock_timestamp()
                ORDER BY sequence
                LIMIT 500
            )
            SELECT DISTINCT file.uuid AS file_uuid,
                   message.zulip_stream_uuid AS stream_uuid,
                   1::smallint AS delivery_priority
            FROM candidate_files AS candidate
            JOIN workspace_zulip_bridge.zulip_files AS file
              ON file.uuid = candidate.entity_uuid
            JOIN workspace_zulip_bridge.zulip_message_files AS link
              ON link.file_uuid = file.uuid
            JOIN workspace_zulip_bridge.zulip_messages AS message
              ON message.uuid = link.message_uuid
            WHERE message.workspace_content IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.workspace_file_projections
                      AS projection
                  WHERE projection.file_uuid = file.uuid
                    AND projection.zulip_stream_uuid =
                        message.zulip_stream_uuid
            )
            ORDER BY file.uuid, message.zulip_stream_uuid
            LIMIT $1
                """,
                remaining,
            )
        rows_by_key: dict[tuple[UUID, UUID], asyncpg.Record] = {}
        for row in (*live_rows, *backfill_rows):
            key = (UUID(str(row["file_uuid"])), UUID(str(row["stream_uuid"])))
            rows_by_key.setdefault(key, row)
        rows = list(rows_by_key.values())
        if not rows:
            await self._pool.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_outbox
                SET delivery_status = 'delivered', delivered_at = clock_timestamp(),
                    last_error = 'source_delete_retained',
                    updated_at = clock_timestamp()
                WHERE entity_type = 'file' AND action = 'delete'
                  AND delivery_status IN ('pending', 'failed')
                """
            )
            return 0
        records = []
        for row in rows:
            file_uuid = UUID(str(row["file_uuid"]))
            stream_uuid = UUID(str(row["stream_uuid"]))
            projection_uuid = stable_file_projection_uuid(file_uuid, stream_uuid)
            records.append(
                (
                    projection_uuid,
                    file_uuid,
                    stream_uuid,
                    projection_uuid,
                    int(row["delivery_priority"]),
                )
            )
        async with self._pool.acquire() as connection, connection.transaction():
            inserted = await connection.fetch(
                """
                INSERT INTO workspace_zulip_bridge.workspace_file_projections (
                    uuid, file_uuid, zulip_stream_uuid, operation_uuid,
                    delivery_priority
                )
                SELECT * FROM unnest(
                    $1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[],
                    $5::smallint[]
                )
                ON CONFLICT (file_uuid, zulip_stream_uuid) DO UPDATE
                SET delivery_priority = LEAST(
                        workspace_file_projections.delivery_priority,
                        EXCLUDED.delivery_priority
                    ),
                    available_at = CASE
                        WHEN workspace_file_projections.delivery_priority
                             > EXCLUDED.delivery_priority
                        THEN clock_timestamp()
                        ELSE workspace_file_projections.available_at
                    END,
                    updated_at = clock_timestamp()
                WHERE workspace_file_projections.delivery_priority
                      > EXCLUDED.delivery_priority
                RETURNING uuid
                """,
                [record[0] for record in records],
                [record[1] for record in records],
                [record[2] for record in records],
                [record[3] for record in records],
                [record[4] for record in records],
            )
        return len(inserted)

    async def _claim_job(self) -> _Job | None:
        row = await self._pool.fetchrow(
            """
            WITH expired AS (
                UPDATE workspace_zulip_bridge.workspace_file_projections
                SET processing_status = 'failed', claimed_at = NULL,
                    available_at = clock_timestamp(), last_error = 'claim_expired',
                    updated_at = clock_timestamp()
                WHERE processing_status = 'processing'
                  AND claimed_at < clock_timestamp() - interval '10 minutes'
            ), candidate AS (
                SELECT projection.uuid
                FROM workspace_zulip_bridge.workspace_file_projections AS projection
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = projection.zulip_stream_uuid
                JOIN workspace_zulip_bridge.zulip_connections AS connection
                  ON connection.uuid = stream.source_connection_uuid
                WHERE projection.processing_status IN ('pending', 'failed')
                  AND projection.available_at <= clock_timestamp()
                  AND connection.external_account_uuid IS NOT NULL
                  AND connection.sync_enabled
                ORDER BY projection.delivery_priority, projection.available_at,
                         projection.created_at, projection.uuid
                LIMIT 1 FOR UPDATE SKIP LOCKED
            ), claimed AS (
                UPDATE workspace_zulip_bridge.workspace_file_projections AS projection
                SET processing_status = 'processing',
                    claimed_at = clock_timestamp(), attempt_count = attempt_count + 1,
                    last_error = NULL, updated_at = clock_timestamp()
                FROM candidate WHERE projection.uuid = candidate.uuid
                RETURNING projection.*
            )
            SELECT claimed.uuid AS projection_uuid, claimed.operation_uuid,
                   file.uuid AS file_uuid, stream.uuid AS stream_uuid,
                   file.realm_uuid, connection.external_account_uuid,
                   stream.chat_key,
                   realm.endpoint, connection.login, connection.api_key,
                   file.source_path, file.name
            FROM claimed
            JOIN workspace_zulip_bridge.zulip_files AS file
              ON file.uuid = claimed.file_uuid
            JOIN workspace_zulip_bridge.zulip_streams AS stream
              ON stream.uuid = claimed.zulip_stream_uuid
            JOIN workspace_zulip_bridge.zulip_connections AS connection
              ON connection.uuid = stream.source_connection_uuid
            JOIN workspace_zulip_bridge.zulip_realms AS realm
              ON realm.uuid = file.realm_uuid
            WHERE connection.external_account_uuid IS NOT NULL
              AND connection.sync_enabled
            """
        )
        if row is None:
            return None
        return _Job(
            projection_uuid=UUID(str(row["projection_uuid"])),
            operation_uuid=UUID(str(row["operation_uuid"])),
            file_uuid=UUID(str(row["file_uuid"])),
            stream_uuid=UUID(str(row["stream_uuid"])),
            realm_uuid=UUID(str(row["realm_uuid"])),
            external_account_uuid=UUID(str(row["external_account_uuid"])),
            external_chat_uuid=stable_external_chat_uuid(
                UUID(str(row["external_account_uuid"])),
                str(row["chat_key"]),
            ),
            endpoint=str(row["endpoint"]),
            login=str(row["login"]),
            api_key=str(row["api_key"]),
            source_path=str(row["source_path"]),
            name=str(row["name"]),
        )

    def _source_verify(self) -> bool | ssl.SSLContext:
        ca_file = self._settings.effective_zulip_ca_file
        if ca_file is None:
            return True
        context = ssl.create_default_context()
        context.load_verify_locations(cafile=ca_file)
        return context

    def _source_client(self, job: _Job) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=job.endpoint.rstrip("/"),
            auth=httpx.BasicAuth(job.login, job.api_key),
            verify=self._source_verify(),
            follow_redirects=True,
            trust_env=False,
            timeout=httpx.Timeout(self._settings.workspace_request_timeout_seconds),
            headers={"User-Agent": "workspace-zulip-bridge"},
        )

    async def _probe_source(self, job: _Job) -> _Descriptor:
        digest = hashlib.sha256()
        size = 0
        async with self._source_client(job) as client:
            async with client.stream("GET", job.source_path) as response:
                if response.is_error:
                    raise FileTransferError(
                        f"zulip_download_http_{response.status_code}",
                        retryable=response.status_code == 429
                        or response.status_code >= 500,
                    )
                content_type = self._content_type(response, job.name)
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MAX_FILE_BYTES:
                        raise FileTransferError("file_too_large", retryable=False)
                    digest.update(chunk)
        return _Descriptor(size, content_type, digest.hexdigest())

    @staticmethod
    def _content_type(response: httpx.Response, name: str) -> str:
        value = response.headers.get("Content-Type", "").partition(";")[0].strip()
        if _CONTENT_TYPE.fullmatch(value) is not None:
            return str(value)
        guessed = mimetypes.guess_type(name)[0] or "application/octet-stream"
        if _CONTENT_TYPE.fullmatch(guessed) is None:
            return "application/octet-stream"
        return guessed

    def _control_client(self) -> httpx.AsyncClient:
        ca = self._state / "control-ca.pem"
        certificate = self._state / "bridge.crt"
        key = self._state / "bridge.key"
        for path in (ca, certificate, key):
            if not path.is_file():
                raise FileTransferError("bridge_identity_not_ready")
        context = ssl.create_default_context(cafile=str(ca))
        context.load_cert_chain(str(certificate), str(key))
        return httpx.AsyncClient(
            base_url=self._control_url,
            verify=context,
            follow_redirects=False,
            trust_env=False,
            timeout=httpx.Timeout(self._settings.workspace_request_timeout_seconds),
            headers={"Accept": "application/json"},
        )

    async def _transfer(self, job: _Job, descriptor: _Descriptor) -> str:
        request = {
            "operation_uuid": str(job.operation_uuid),
            "external_account_uuid": str(job.external_account_uuid),
            "external_chat_uuid": str(job.external_chat_uuid),
            "name": workspace_file_name(job.name),
            "size_bytes": descriptor.size_bytes,
            "content_type": descriptor.content_type,
            "sha256": descriptor.sha256,
        }
        async with self._control_client() as control:
            allocation_response = await control.put(
                f"/v1/file-transfers/incoming/{job.projection_uuid}",
                json=request,
            )
            allocation = self._response(allocation_response)
            if allocation.get("status") == "finalized":
                return self._finalized_urn(allocation, descriptor)
            upload = allocation.get("upload")
            generation = allocation.get("allocation_generation")
            if not isinstance(upload, dict) or not isinstance(generation, int):
                raise FileTransferError("invalid_allocation_response")
            await self._upload(job, descriptor, upload)
            finalize = await control.post(
                f"/v1/file-transfers/incoming/{job.projection_uuid}/actions/finalize",
                json={
                    "operation_uuid": str(job.operation_uuid),
                    "allocation_generation": generation,
                    "size_bytes": descriptor.size_bytes,
                    "content_type": descriptor.content_type,
                    "sha256": descriptor.sha256,
                },
            )
            return self._finalized_urn(self._response(finalize), descriptor)

    @staticmethod
    def _response(response: httpx.Response) -> dict[str, Any]:
        if response.is_error:
            retryable = response.status_code not in {400, 413, 415, 422}
            raise FileTransferError(
                f"workspace_file_http_{response.status_code}",
                retryable=retryable,
            )
        try:
            value = response.json()
        except ValueError as error:
            raise FileTransferError("invalid_workspace_file_response") from error
        if not isinstance(value, dict):
            raise FileTransferError("invalid_workspace_file_response")
        return value

    @staticmethod
    def _finalized_urn(response: dict[str, Any], descriptor: _Descriptor) -> str:
        urn = response.get("file_urn")
        if (
            response.get("status") != "finalized"
            or not isinstance(urn, str)
            or _WORKSPACE_URN.fullmatch(urn) is None
            or response.get("size_bytes") != descriptor.size_bytes
            or response.get("content_type") != descriptor.content_type
            or response.get("sha256") != descriptor.sha256
        ):
            raise FileTransferError("invalid_workspace_file_response")
        return urn

    async def _upload(
        self,
        job: _Job,
        descriptor: _Descriptor,
        upload: dict[str, Any],
    ) -> None:
        url = upload.get("url")
        headers = upload.get("headers")
        if upload.get("method") != "PUT" or not isinstance(url, str):
            raise FileTransferError("invalid_allocation_response")
        if not isinstance(headers, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in headers.items()
        ):
            raise FileTransferError("invalid_allocation_response")
        if (
            headers.get("Content-Length") != str(descriptor.size_bytes)
            or headers.get("Content-Type") != descriptor.content_type
        ):
            raise FileTransferError("invalid_allocation_response")
        digest = hashlib.sha256()
        size = 0
        async with self._source_client(job) as source:
            async with source.stream("GET", job.source_path) as response:
                if response.is_error:
                    raise FileTransferError(
                        f"zulip_download_http_{response.status_code}",
                        retryable=response.status_code == 429
                        or response.status_code >= 500,
                    )

                async def chunks() -> AsyncIterator[bytes]:
                    nonlocal size
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        digest.update(chunk)
                        yield chunk

                async with httpx.AsyncClient(
                    follow_redirects=False,
                    trust_env=False,
                    timeout=httpx.Timeout(
                        max(300.0, self._settings.workspace_request_timeout_seconds)
                    ),
                ) as uploader:
                    result = await uploader.put(url, headers=headers, content=chunks())
                if result.is_error:
                    raise FileTransferError(
                        f"object_upload_http_{result.status_code}",
                        retryable=result.status_code == 429
                        or result.status_code >= 500,
                    )
        if size != descriptor.size_bytes or digest.hexdigest() != descriptor.sha256:
            raise FileTransferError("source_changed_during_upload")

    async def _finalize_job(
        self,
        job: _Job,
        descriptor: _Descriptor,
        file_urn: str,
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                SELECT message.uuid, message.workspace_content,
                       message.sender_user_uuid, stream.chat_key,
                       topic.name AS topic_name, message.created_at
                FROM workspace_zulip_bridge.zulip_message_files AS link
            JOIN workspace_zulip_bridge.zulip_messages AS message
              ON message.uuid = link.message_uuid
             AND message.workspace_content IS NOT NULL
                 AND message.zulip_stream_uuid = $2
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = message.zulip_stream_uuid
                LEFT JOIN workspace_zulip_bridge.zulip_topics AS topic
                  ON topic.uuid = message.topic_uuid
                WHERE link.file_uuid = $1
                ORDER BY message.uuid
                """,
                job.file_uuid,
                job.stream_uuid,
            )
            changed: list[tuple[UUID, str, bytes]] = []
            for row in rows:
                current = str(row["workspace_content"] or "")
                projected = replace_source_file_urn(
                    current,
                    job.file_uuid,
                    file_urn,
                )
                if projected == current:
                    continue
                content_hash = message_content_hash(
                    sender_user_uuid=UUID(str(row["sender_user_uuid"])),
                    chat_key=str(row["chat_key"]),
                    topic_name=(
                        None if row["topic_name"] is None else str(row["topic_name"])
                    ),
                    content=projected,
                    sent_at=int(row["created_at"].timestamp()),
                )
                changed.append((UUID(str(row["uuid"])), projected, content_hash))
            if changed:
                await connection.executemany(
                    """
                    UPDATE workspace_zulip_bridge.zulip_messages
                    SET workspace_content = $2, content_hash = $3,
                        source_updated_at = GREATEST(
                            source_updated_at, clock_timestamp()
                        ), updated_at = clock_timestamp()
                    WHERE uuid = $1
                    """,
                    changed,
                )
                await connection.executemany(
                    """
                    INSERT INTO workspace_zulip_bridge.workspace_outbox (
                        realm_uuid, entity_type, action, entity_uuid
                    ) VALUES ($1, 'message', 'upsert', $2)
                    ON CONFLICT (realm_uuid, entity_type, entity_uuid)
                        WHERE delivery_status = 'pending'
                    DO UPDATE SET action = 'upsert',
                                  available_at = clock_timestamp(),
                                  updated_at = clock_timestamp()
                    """,
                    [(job.realm_uuid, message_uuid) for message_uuid, _, _ in changed],
                )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_file_projections
                SET processing_status = 'finalized', workspace_urn = $2,
                    content_type = $3, size_bytes = $4, sha256 = $5,
                    finalized_at = clock_timestamp(), claimed_at = NULL,
                    last_error = NULL, updated_at = clock_timestamp()
                WHERE uuid = $1
                """,
                job.projection_uuid,
                file_urn,
                descriptor.content_type,
                descriptor.size_bytes,
                descriptor.sha256,
            )

    async def _fail_job(self, job: _Job, reason: str, *, retryable: bool) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_file_projections
            SET processing_status = CASE WHEN $3 THEN 'failed' ELSE 'blocked' END,
                available_at = CASE WHEN $3 THEN
                    clock_timestamp() + make_interval(
                        secs => LEAST(300::double precision,
                            power(2::double precision, LEAST(attempt_count, 8)))
                    )
                    ELSE available_at END,
                claimed_at = NULL, last_error = $2,
                updated_at = clock_timestamp()
            WHERE uuid = $1
            """,
            job.projection_uuid,
            reason[:128],
            retryable,
        )
        LOG.warning(
            "Workspace file transfer deferred projection=%s reason=%s retryable=%s",
            job.projection_uuid,
            reason,
            retryable,
        )

    async def _complete_file_outbox(self, file_uuid: UUID | None = None) -> None:
        await self._pool.execute(
            """
            WITH candidates AS (
                SELECT sequence
                FROM workspace_zulip_bridge.workspace_outbox
                WHERE entity_type = 'file' AND action = 'upsert'
                  AND delivery_status IN ('pending', 'failed')
                  AND ($1::uuid IS NULL OR entity_uuid = $1)
                ORDER BY sequence
                LIMIT 100
            )
            UPDATE workspace_zulip_bridge.workspace_outbox AS outbox
            SET delivery_status = 'delivered', delivered_at = clock_timestamp(),
                claimed_at = NULL, last_error = NULL,
                updated_at = clock_timestamp()
            WHERE outbox.entity_type = 'file'
              AND outbox.action = 'upsert'
              AND outbox.delivery_status IN ('pending', 'failed')
              AND outbox.sequence IN (SELECT sequence FROM candidates)
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.zulip_message_files AS link
                  JOIN workspace_zulip_bridge.zulip_messages AS message
                    ON message.uuid = link.message_uuid
                  LEFT JOIN workspace_zulip_bridge.workspace_file_projections
                    AS projection
                    ON projection.file_uuid = link.file_uuid
                   AND projection.zulip_stream_uuid = message.zulip_stream_uuid
                   AND projection.processing_status = 'finalized'
                  WHERE link.file_uuid = outbox.entity_uuid
                    AND projection.uuid IS NULL
              )
            """,
            file_uuid,
        )
