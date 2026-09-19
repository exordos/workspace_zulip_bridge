# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Bootstrap and converge the local Zulip and Workspace entity graphs."""

import asyncio
import hashlib
import json
import logging
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit
from urllib.parse import urlunsplit
from uuid import UUID

import asyncpg
import httpx

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.workspace_events import _read_token

LOG = logging.getLogger(__name__)

ENTITY_TYPES = (
    "users",
    "streams",
    "stream_bindings",
    "topics",
    "topic_bindings",
    "messages",
    "message_flags",
    "message_reactions",
)
PRIORITY = {entity_type: index for index, entity_type in enumerate(ENTITY_TYPES)}


def canonical_hash(data: Mapping[str, Any]) -> bytes:
    return hashlib.sha256(
        json.dumps(
            data,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()


def _timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() != UTC.utcoffset(result):
        raise ValueError("Workspace timestamps must use UTC")
    return result.astimezone(UTC)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def workspace_api_url(settings: Settings) -> str:
    if settings.workspace_api_url is not None:
        return settings.workspace_api_url.rstrip("/")
    assert settings.workspace_websocket_url is not None
    parsed = urlsplit(settings.workspace_websocket_url)
    scheme = "https" if parsed.scheme == "wss" else "http"
    suffix = "/events/ws"
    path = parsed.path
    if not path.endswith(suffix):
        raise ValueError("Workspace websocket URL must end with /events/ws")
    return urlunsplit((scheme, parsed.netloc, path[: -len(suffix)], "", "")).rstrip("/")


class WorkspaceBootstrapper:
    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        assert settings.workspace_provider_uuid is not None
        assert settings.workspace_project_id is not None
        assert settings.workspace_token_file is not None
        self._pool = pool
        self._settings = settings
        self._provider_uuid = settings.workspace_provider_uuid
        self._project_uuid = settings.workspace_project_id
        self._token_file = settings.workspace_token_file

    async def ensure(self) -> bool:
        row = await self._pool.fetchrow(
            """
            SELECT mirror.bootstrap_status, mirror.active_generation,
                   cursor.recovery_required
            FROM workspace_zulip_bridge.workspace_mirror_state AS mirror
            LEFT JOIN workspace_zulip_bridge.workspace_event_cursors AS cursor
              ON cursor.provider_uuid = mirror.provider_uuid
            WHERE mirror.provider_uuid = $1
            """,
            self._provider_uuid,
        )
        if (
            row is not None
            and row["bootstrap_status"] == "ready"
            and row["active_generation"] is not None
            and not row["recovery_required"]
        ):
            return False
        await self.bootstrap()
        return True

    async def bootstrap(self, client: httpx.AsyncClient | None = None) -> None:
        await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_mirror_state (
                provider_uuid, workspace_project_id, bootstrap_status
            ) VALUES ($1, $2, 'loading')
            ON CONFLICT (provider_uuid) DO UPDATE
            SET workspace_project_id = EXCLUDED.workspace_project_id,
                bootstrap_status = 'loading', last_error = NULL,
                updated_at = clock_timestamp()
            """,
            self._provider_uuid,
            self._project_uuid,
        )
        try:
            await self._load_snapshot(client)
        except BaseException as exc:
            await self._pool.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_mirror_state
                SET bootstrap_status = 'failed', last_error = $2,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1
                """,
                self._provider_uuid,
                str(exc)[:2048],
            )
            raise

    async def _load_snapshot(self, client: httpx.AsyncClient | None) -> None:
        if client is None:
            token = await asyncio.to_thread(_read_token, self._token_file)
            verify: bool | str = (
                True
                if self._settings.workspace_ca_file is None
                else str(self._settings.workspace_ca_file)
            )
            async with httpx.AsyncClient(
                headers={"Authorization": f"Bearer {token}"},
                verify=verify,
                timeout=httpx.Timeout(
                    self._settings.workspace_bootstrap_timeout_seconds
                ),
            ) as owned_client:
                await self._load_snapshot(owned_client)
            return
        buffers: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
        counts = {entity_type: 0 for entity_type in ENTITY_TYPES}
        digest = hashlib.sha256()
        meta: dict[str, Any] | None = None
        complete: dict[str, Any] | None = None
        async with client.stream(
            "GET", f"{workspace_api_url(self._settings)}/provider/bootstrap"
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line:
                    continue
                record = json.loads(line)
                kind = record.get("record")
                if kind == "meta":
                    if meta is not None:
                        raise ValueError("duplicate Workspace snapshot meta")
                    meta = record
                    continue
                if kind == "complete":
                    complete = record
                    continue
                if kind != "entity" or meta is None or complete is not None:
                    raise ValueError("invalid Workspace snapshot framing")
                entity_type = str(record["type"])
                if entity_type not in ENTITY_TYPES:
                    raise ValueError("unsupported Workspace snapshot entity")
                digest.update((line + "\n").encode("utf-8"))
                counts[entity_type] += 1
                buffers[entity_type].append(
                    (
                        self._provider_uuid,
                        UUID(str(meta["snapshot_uuid"])),
                        UUID(str(record["uuid"])),
                        self._project_uuid,
                        bytes.fromhex(str(record["content_hash"])),
                        _timestamp(str(record["source_updated_at"])),
                        json.dumps(record["data"], separators=(",", ":")),
                    )
                )
                if len(buffers[entity_type]) >= 5000:
                    await self._copy(entity_type, buffers[entity_type])
                    buffers[entity_type].clear()
        if meta is None or complete is None:
            raise ValueError("incomplete Workspace snapshot")
        generation = UUID(str(meta["snapshot_uuid"]))
        for entity_type, records in buffers.items():
            await self._copy(entity_type, records)
        if complete.get("counts") != counts:
            raise ValueError("Workspace snapshot entity counts do not match")
        if complete.get("sha256") != digest.hexdigest():
            raise ValueError("Workspace snapshot checksum does not match")
        epoch_generation = UUID(str(meta["epoch_generation"]))
        epoch_version = int(meta["snapshot_epoch_version"])
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                DELETE FROM workspace_zulip_bridge.workspace_events
                WHERE provider_uuid = $1 AND epoch_version <= $2
                """,
                self._provider_uuid,
                epoch_version,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.workspace_event_cursors (
                    provider_uuid, workspace_project_id, epoch_generation,
                    last_epoch_version
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT (provider_uuid) DO UPDATE SET
                    workspace_project_id = EXCLUDED.workspace_project_id,
                    epoch_generation = EXCLUDED.epoch_generation,
                    last_epoch_version = EXCLUDED.last_epoch_version,
                    recovery_required = false, recovery_reason = NULL,
                    updated_at = clock_timestamp()
                """,
                self._provider_uuid,
                self._project_uuid,
                epoch_generation,
                epoch_version,
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_mirror_state
                SET active_generation = $3, epoch_generation = $4,
                    snapshot_epoch_version = $5, bootstrap_status = 'ready',
                    entity_counts = $6::jsonb, snapshot_hash = $7,
                    last_error = NULL, bootstrapped_at = clock_timestamp(),
                    initial_sync_completed_at = NULL,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1 AND workspace_project_id = $2
                """,
                self._provider_uuid,
                self._project_uuid,
                generation,
                epoch_generation,
                epoch_version,
                json.dumps(counts, separators=(",", ":")),
                digest.digest(),
            )
        for entity_type in ENTITY_TYPES:
            await self._pool.execute(
                f"DELETE FROM workspace_zulip_bridge.workspace_{entity_type} "
                "WHERE provider_uuid = $1 AND snapshot_generation <> $2",
                self._provider_uuid,
                generation,
            )
        LOG.info("Workspace bootstrap activated: counts=%s", counts)

    async def _copy(self, entity_type: str, records: list[tuple[Any, ...]]) -> None:
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.copy_records_to_table(
                f"workspace_{entity_type}",
                schema_name="workspace_zulip_bridge",
                records=records,
                columns=(
                    "provider_uuid",
                    "snapshot_generation",
                    "uuid",
                    "workspace_project_id",
                    "content_hash",
                    "source_updated_at",
                    "data",
                ),
            )


class WorkspaceEventProcessor:
    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        assert settings.workspace_provider_uuid is not None
        self._pool = pool
        self._settings = settings
        self._provider_uuid = settings.workspace_provider_uuid

    async def run(self) -> None:
        while True:
            changed = await self.process_once()
            if not changed:
                await asyncio.sleep(self._settings.workspace_sync_poll_seconds)

    async def process_once(self) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                WITH claim AS (
                    SELECT sequence
                    FROM workspace_zulip_bridge.workspace_events
                    WHERE provider_uuid = $1 AND processing_status = 'pending'
                    ORDER BY sequence
                    FOR UPDATE SKIP LOCKED
                    LIMIT $2
                )
                UPDATE workspace_zulip_bridge.workspace_events AS event
                SET processing_status = 'processing', claimed_at = clock_timestamp(),
                    attempt_count = attempt_count + 1
                FROM claim WHERE event.sequence = claim.sequence
                RETURNING event.*
                """,
                self._provider_uuid,
                self._settings.workspace_event_batch_size,
            )
        if not rows:
            return 0
        for row in rows:
            try:
                applied = await self._apply(row)
                status = "applied" if applied else "skipped"
                error = None
            except Exception as exc:
                LOG.exception("Workspace mirror event failed")
                status = "failed"
                error = str(exc)[:2048]
            await self._pool.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_events
                SET processing_status = $2, processed_at = clock_timestamp(),
                    last_error = $3, updated_at = clock_timestamp()
                WHERE sequence = $1
                """,
                row["sequence"],
                status,
                error,
            )
        return len(rows)

    async def _apply(self, row: asyncpg.Record) -> bool:
        frame = _json_object(row["payload"])
        payload = frame.get("payload")
        if not isinstance(payload, dict):
            return False
        entity_type = _event_entity_type(str(row["object_type"]))
        if entity_type is None:
            return False
        state = await self._pool.fetchrow(
            """
            SELECT active_generation FROM workspace_zulip_bridge.workspace_mirror_state
            WHERE provider_uuid = $1 AND bootstrap_status = 'ready'
            """,
            self._provider_uuid,
        )
        if state is None or state["active_generation"] is None:
            raise RuntimeError("Workspace mirror is not bootstrapped")
        generation = state["active_generation"]
        items = payload.get("items")
        values = items if isinstance(items, list) else [payload]
        applied = False
        for value in values:
            if not isinstance(value, dict):
                continue
            raw_uuid = value.get("uuid") or row["entity_uuid"]
            if raw_uuid is None:
                continue
            entity_uuid = UUID(str(raw_uuid))
            if row["action"] == "deleted":
                await self._pool.execute(
                    f"DELETE FROM workspace_zulip_bridge.workspace_{entity_type} "
                    "WHERE provider_uuid = $1 AND snapshot_generation = $2 "
                    "AND uuid = $3",
                    self._provider_uuid,
                    generation,
                    entity_uuid,
                )
                target_hash = None
                source_updated_at = _timestamp(str(frame["updated_at"]))
            else:
                data = {key: item for key, item in value.items() if key != "kind"}
                source_updated_at = _timestamp(
                    str(value.get("updated_at") or frame["updated_at"])
                )
                target_hash = canonical_hash(data)
                await self._pool.execute(
                    f"""
                    INSERT INTO workspace_zulip_bridge.workspace_{entity_type} (
                        provider_uuid, snapshot_generation, uuid,
                        workspace_project_id, content_hash, source_updated_at, data
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                    ON CONFLICT (provider_uuid, snapshot_generation, uuid)
                    DO UPDATE SET content_hash = EXCLUDED.content_hash,
                        source_updated_at = EXCLUDED.source_updated_at,
                        data = EXCLUDED.data, updated_at = clock_timestamp()
                    """,
                    self._provider_uuid,
                    generation,
                    entity_uuid,
                    row["workspace_project_id"],
                    target_hash,
                    source_updated_at,
                    json.dumps(data, separators=(",", ":")),
                )
            await self._refresh_existing_diff(
                entity_type,
                entity_uuid,
                target_hash,
                source_updated_at,
            )
            applied = True
        return applied

    async def _refresh_existing_diff(
        self,
        entity_type: str,
        entity_uuid: UUID,
        target_hash: bytes | None,
        target_updated_at: datetime,
    ) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET direction = CASE
                    WHEN $5 > source_updated_at THEN 'to_zulip'
                    ELSE 'to_workspace'
                END,
                processing_status = CASE
                    WHEN target_hash IS DISTINCT FROM $4
                      OR target_updated_at IS DISTINCT FROM $5
                    THEN 'pending' ELSE processing_status END,
                available_at = CASE
                    WHEN target_hash IS DISTINCT FROM $4
                      OR target_updated_at IS DISTINCT FROM $5
                    THEN clock_timestamp() ELSE available_at END,
                last_error = CASE
                    WHEN target_hash IS DISTINCT FROM $4
                      OR target_updated_at IS DISTINCT FROM $5
                    THEN NULL ELSE last_error END,
                target_hash = $4, target_updated_at = $5,
                updated_at = clock_timestamp()
            WHERE provider_uuid = $1 AND entity_type = $2
              AND entity_uuid = $3
            """,
            self._provider_uuid,
            entity_type,
            entity_uuid,
            target_hash,
            target_updated_at,
        )


class WorkspaceDiffWorker:
    def __init__(
        self,
        pool: asyncpg.Pool,
        settings: Settings,
        *,
        plan_enabled: bool = True,
        partition: int = 0,
        partition_count: int = 1,
    ) -> None:
        assert settings.workspace_provider_uuid is not None
        assert settings.workspace_project_id is not None
        assert settings.workspace_token_file is not None
        if partition_count < 1 or not 0 <= partition < partition_count:
            raise ValueError("invalid Workspace sync partition")
        self._pool = pool
        self._settings = settings
        self._provider_uuid = settings.workspace_provider_uuid
        self._project_uuid = settings.workspace_project_id
        self._token_file = settings.workspace_token_file
        self._plan_enabled = plan_enabled
        self._partition = partition
        self._partition_count = partition_count

    async def run(self) -> None:
        token = await asyncio.to_thread(_read_token, self._token_file)
        verify: bool | str = (
            True
            if self._settings.workspace_ca_file is None
            else str(self._settings.workspace_ca_file)
        )
        async with httpx.AsyncClient(
            headers={"Authorization": f"Bearer {token}"},
            verify=verify,
            timeout=httpx.Timeout(self._settings.workspace_request_timeout_seconds),
        ) as client:
            while True:
                try:
                    if self._plan_enabled:
                        await self._plan_and_drain(client)
                    else:
                        await self._drain(client)
                except (
                    TimeoutError,
                    asyncpg.PostgresError,
                    httpx.HTTPError,
                    RuntimeError,
                ):
                    LOG.warning("Workspace diff pass failed; retrying", exc_info=True)
                    await asyncio.sleep(self._settings.workspace_retry_base_seconds)
                    continue
                await asyncio.sleep(self._settings.workspace_sync_poll_seconds)

    async def _plan_and_drain(self, client: httpx.AsyncClient) -> int:
        planned = await self.plan()
        processed = await self._drain(client)
        if planned == 0 and processed == 0:
            await self._complete_initial_sync()
        return processed

    async def _drain(self, client: httpx.AsyncClient) -> int:
        processed = 0
        while changed := await self.process_once(client):
            processed += changed
        return processed

    async def plan(self) -> int:
        realm_uuid = await self._link_realm()
        await self._ensure_direct_topics(realm_uuid)
        state = await self._pool.fetchrow(
            """
            SELECT active_generation
            FROM workspace_zulip_bridge.workspace_mirror_state
            WHERE provider_uuid = $1 AND bootstrap_status = 'ready'
            """,
            self._provider_uuid,
        )
        if state is None or state["active_generation"] is None:
            return 0
        generation = state["active_generation"]
        total = 0
        for entity_type, source in _SOURCE_TABLES.items():
            planned = await self._plan_entity(
                entity_type,
                source,
                realm_uuid,
                generation,
            )
            total += planned
            if planned:
                break
        return total

    async def _plan_entity(
        self,
        entity_type: str,
        source: Mapping[str, str],
        realm_uuid: UUID,
        generation: UUID,
    ) -> int:
        timestamp_column = (
            "source.source_updated_at"
            if entity_type == "messages"
            else "source.updated_at"
        )
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_plan_cursors (
                    provider_uuid, entity_type, snapshot_generation
                ) VALUES ($1, $2, $3)
                ON CONFLICT (provider_uuid, entity_type) DO UPDATE
                SET snapshot_generation = EXCLUDED.snapshot_generation,
                    source_updated_at = CASE
                        WHEN sync_plan_cursors.snapshot_generation
                             IS DISTINCT FROM EXCLUDED.snapshot_generation
                        THEN NULL ELSE sync_plan_cursors.source_updated_at END,
                    entity_uuid = CASE
                        WHEN sync_plan_cursors.snapshot_generation
                             IS DISTINCT FROM EXCLUDED.snapshot_generation
                        THEN NULL ELSE sync_plan_cursors.entity_uuid END,
                    updated_at = clock_timestamp()
                """,
                self._provider_uuid,
                entity_type,
                generation,
            )
            cursor = await connection.fetchrow(
                """
                SELECT source_updated_at, entity_uuid
                FROM workspace_zulip_bridge.sync_plan_cursors
                WHERE provider_uuid = $1 AND entity_type = $2
                FOR UPDATE
                """,
                self._provider_uuid,
                entity_type,
            )
            assert cursor is not None
            rows = await connection.fetch(
                f"""
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    partition_key,
                    direction, source_hash, target_hash,
                    source_updated_at, target_updated_at
                )
                SELECT $1, $2, source.uuid, $3, {source["partition"]},
                       CASE WHEN target.source_updated_at > {timestamp_column}
                            THEN 'to_zulip' ELSE 'to_workspace' END,
                       {source["hash"]}, target.content_hash,
                       {timestamp_column}, target.source_updated_at
                FROM {source["from"]} AS source
                {source["joins"]}
                LEFT JOIN workspace_zulip_bridge.workspace_{entity_type} AS target
                  ON target.provider_uuid = $1
                 AND target.snapshot_generation = $4
                 AND target.uuid = source.uuid
                WHERE {source["where"]}
                  AND (
                      $5::timestamptz IS NULL
                      OR ({timestamp_column}, source.uuid) > ($5, $6)
                  )
                ORDER BY {timestamp_column}, source.uuid
                LIMIT $7
                ON CONFLICT (provider_uuid, entity_type, entity_uuid)
                DO UPDATE SET
                    direction = EXCLUDED.direction,
                    partition_key = EXCLUDED.partition_key,
                    source_hash = EXCLUDED.source_hash,
                    target_hash = EXCLUDED.target_hash,
                    source_updated_at = EXCLUDED.source_updated_at,
                    target_updated_at = EXCLUDED.target_updated_at,
                    processing_status = CASE
                        WHEN sync_diffs.source_hash IS DISTINCT FROM EXCLUDED.source_hash
                          OR sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                          OR sync_diffs.source_updated_at
                             IS DISTINCT FROM EXCLUDED.source_updated_at
                          OR sync_diffs.target_updated_at
                             IS DISTINCT FROM EXCLUDED.target_updated_at
                          OR sync_diffs.direction IS DISTINCT FROM EXCLUDED.direction
                        THEN 'pending' ELSE sync_diffs.processing_status END,
                    available_at = CASE
                        WHEN sync_diffs.source_hash IS DISTINCT FROM EXCLUDED.source_hash
                          OR sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                          OR sync_diffs.source_updated_at
                             IS DISTINCT FROM EXCLUDED.source_updated_at
                          OR sync_diffs.target_updated_at
                             IS DISTINCT FROM EXCLUDED.target_updated_at
                          OR sync_diffs.direction IS DISTINCT FROM EXCLUDED.direction
                        THEN clock_timestamp() ELSE sync_diffs.available_at END,
                    updated_at = clock_timestamp()
                RETURNING entity_uuid, source_updated_at
                """,
                self._provider_uuid,
                entity_type,
                realm_uuid,
                generation,
                cursor["source_updated_at"],
                cursor["entity_uuid"],
                self._settings.workspace_sync_plan_batch_size,
            )
            if not rows:
                return 0
            last = max(
                rows,
                key=lambda row: (row["source_updated_at"], row["entity_uuid"]),
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.sync_plan_cursors
                SET source_updated_at = $3, entity_uuid = $4,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1 AND entity_type = $2
                """,
                self._provider_uuid,
                entity_type,
                last["source_updated_at"],
                last["entity_uuid"],
            )
            return len(rows)

    async def process_once(self, client: httpx.AsyncClient) -> int:
        mirror = await self._pool.fetchrow(
            """
            SELECT initial_sync_completed_at
            FROM workspace_zulip_bridge.workspace_mirror_state
            WHERE provider_uuid = $1
            """,
            self._provider_uuid,
        )
        delivery_class = (
            "live"
            if mirror is not None and mirror["initial_sync_completed_at"] is not None
            else "backfill"
        )
        async with self._pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                WITH claim AS (
                    SELECT provider_uuid, entity_type, entity_uuid
                    FROM workspace_zulip_bridge.sync_diffs
                    WHERE provider_uuid = $1
                      AND processing_status IN ('pending', 'failed')
                      AND available_at <= clock_timestamp()
                      AND (
                          ($3 = 0 AND entity_type NOT IN (
                              'messages', 'message_flags', 'message_reactions'
                          ))
                          OR (
                              entity_type IN (
                                  'messages', 'message_flags',
                                  'message_reactions'
                              )
                              AND (
                                  (
                                      hashtextextended(
                                          COALESCE(
                                              partition_key, entity_uuid
                                          )::text,
                                          0
                                      ) % $4 + $4
                                  ) % $4
                              ) = $3
                          )
                      )
                    ORDER BY CASE entity_type
                        WHEN 'users' THEN 0 WHEN 'streams' THEN 1
                        WHEN 'stream_bindings' THEN 2 WHEN 'topics' THEN 3
                        WHEN 'topic_bindings' THEN 4 WHEN 'messages' THEN 5
                        WHEN 'message_flags' THEN 6 ELSE 7 END,
                        source_updated_at, entity_uuid
                    FOR UPDATE SKIP LOCKED LIMIT $2
                )
                UPDATE workspace_zulip_bridge.sync_diffs AS diff
                SET processing_status = 'processing', claimed_at = clock_timestamp(),
                    attempt_count = attempt_count + 1
                FROM claim
                WHERE (diff.provider_uuid, diff.entity_type, diff.entity_uuid) =
                      (claim.provider_uuid, claim.entity_type, claim.entity_uuid)
                RETURNING diff.*
                """,
                self._provider_uuid,
                self._settings.workspace_sync_batch_size,
                self._partition,
                self._partition_count,
            )
        if not rows:
            return 0
        to_workspace = [row for row in rows if row["direction"] == "to_workspace"]
        to_zulip = [row for row in rows if row["direction"] == "to_zulip"]
        if to_zulip:
            await self._mark(
                to_zulip,
                "blocked",
                "Workspace-to-Zulip writer is intentionally not enabled yet",
            )
        if not to_workspace:
            return len(rows)
        operations = []
        records: list[tuple[asyncpg.Record, dict[str, Any], bytes]] = []
        loaded: dict[tuple[str, UUID], dict[str, Any]] = {}
        grouped: dict[str, list[UUID]] = defaultdict(list)
        for row in to_workspace:
            grouped[row["entity_type"]].append(row["entity_uuid"])
        for entity_type, entity_uuids in grouped.items():
            loaded.update(await self._load_zulip_entities(entity_type, entity_uuids))
        for row in to_workspace:
            data = loaded.get((row["entity_type"], row["entity_uuid"]))
            if data is None:
                operations.append(
                    {
                        "action": "delete",
                        "type": row["entity_type"],
                        "uuid": str(row["entity_uuid"]),
                    }
                )
                records.append((row, {}, b""))
                continue
            content_hash = canonical_hash(data)
            operations.append(
                {
                    "action": "upsert",
                    "type": row["entity_type"],
                    "uuid": str(row["entity_uuid"]),
                    "content_hash": content_hash.hex(),
                    "source_updated_at": row["source_updated_at"]
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "data": data,
                }
            )
            records.append((row, data, content_hash))
        try:
            response = await client.post(
                f"{workspace_api_url(self._settings)}/provider/entities/actions/apply/invoke",
                json={
                    "delivery_class": delivery_class,
                    "operations": operations,
                },
            )
            if response.is_error:
                raise RuntimeError(
                    f"Workspace Provider API returned {response.status_code}: "
                    f"{response.text[:2048]}"
                )
            results = response.json()["results"]
            if len(results) != len(records):
                raise RuntimeError("Workspace batch result length mismatch")
            await self._accept(records)
        except Exception as exc:
            await self._mark(to_workspace, "failed", str(exc)[:2048])
            raise
        return len(rows)

    async def _complete_initial_sync(self) -> bool:
        result = await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_mirror_state AS mirror
            SET initial_sync_completed_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE mirror.provider_uuid = $1
              AND mirror.initial_sync_completed_at IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.zulip_connections AS connection
                  JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                    ON zulip_user.uuid = connection.zulip_user_uuid
                  WHERE connection.sync_enabled
                    AND NOT zulip_user.disabled AND NOT zulip_user.is_bot
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.zulip_connections AS connection
                  JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                    ON zulip_user.uuid = connection.zulip_user_uuid
                  WHERE connection.sync_enabled
                    AND NOT zulip_user.disabled AND NOT zulip_user.is_bot
                    AND connection.lifecycle_status <> 'active'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM workspace_zulip_bridge.sync_diffs AS diff
                  WHERE diff.provider_uuid = mirror.provider_uuid
                    AND diff.direction = 'to_workspace'
                    AND diff.processing_status IN (
                        'pending', 'processing', 'failed'
                    )
              )
            """,
            self._provider_uuid,
        )
        return result == "UPDATE 1"

    async def _accept(
        self, records: list[tuple[asyncpg.Record, dict[str, Any], bytes]]
    ) -> None:
        state = await self._pool.fetchrow(
            "SELECT active_generation FROM workspace_zulip_bridge.workspace_mirror_state "
            "WHERE provider_uuid = $1",
            self._provider_uuid,
        )
        if state is None or state["active_generation"] is None:
            raise RuntimeError("Workspace mirror generation disappeared")
        generation = state["active_generation"]
        async with self._pool.acquire() as connection, connection.transaction():
            grouped: dict[str, list[tuple[asyncpg.Record, dict[str, Any], bytes]]] = (
                defaultdict(list)
            )
            for record in records:
                grouped[record[0]["entity_type"]].append(record)
            for entity_type, items in grouped.items():
                upserts = [item for item in items if item[1]]
                deletes = [item for item in items if not item[1]]
                if upserts:
                    await connection.executemany(
                        f"""
                        INSERT INTO workspace_zulip_bridge.workspace_{entity_type} (
                            provider_uuid, snapshot_generation, uuid,
                            workspace_project_id, content_hash,
                            source_updated_at, data
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                        ON CONFLICT (provider_uuid, snapshot_generation, uuid)
                        DO UPDATE SET content_hash = EXCLUDED.content_hash,
                            source_updated_at = EXCLUDED.source_updated_at,
                            data = EXCLUDED.data, updated_at = clock_timestamp()
                        """,
                        [
                            (
                                self._provider_uuid,
                                generation,
                                row["entity_uuid"],
                                self._project_uuid,
                                content_hash,
                                row["source_updated_at"],
                                json.dumps(data, separators=(",", ":")),
                            )
                            for row, data, content_hash in upserts
                        ],
                    )
                if deletes:
                    await connection.execute(
                        f"""
                        DELETE FROM workspace_zulip_bridge.workspace_{entity_type}
                        WHERE provider_uuid = $1 AND snapshot_generation = $2
                          AND uuid = ANY($3::uuid[])
                        """,
                        self._provider_uuid,
                        generation,
                        [item[0]["entity_uuid"] for item in deletes],
                    )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.sync_diffs AS diff
                SET processing_status = 'applied', processed_at = clock_timestamp(),
                    target_hash = input.target_hash,
                    target_updated_at = diff.source_updated_at,
                    last_error = NULL, updated_at = clock_timestamp()
                FROM unnest($2::text[], $3::uuid[], $4::bytea[])
                    AS input(entity_type, entity_uuid, target_hash)
                WHERE diff.provider_uuid = $1
                  AND diff.entity_type = input.entity_type
                  AND diff.entity_uuid = input.entity_uuid
                """,
                self._provider_uuid,
                [record[0]["entity_type"] for record in records],
                [record[0]["entity_uuid"] for record in records],
                [record[2] or None for record in records],
            )

    async def _mark(self, rows: list[asyncpg.Record], status: str, error: str) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.executemany(
                """
                UPDATE workspace_zulip_bridge.sync_diffs
                SET processing_status = $4, last_error = $5,
                    available_at = CASE WHEN $4 = 'failed'
                        THEN clock_timestamp() + interval '1 second'
                        ELSE available_at END,
                    processed_at = CASE WHEN $4 IN ('blocked', 'skipped')
                        THEN clock_timestamp() ELSE processed_at END,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1 AND entity_type = $2 AND entity_uuid = $3
                """,
                [
                    (
                        row["provider_uuid"],
                        row["entity_type"],
                        row["entity_uuid"],
                        status,
                        error,
                    )
                    for row in rows
                ],
            )

    async def _link_realm(self) -> UUID:
        rows = await self._pool.fetch(
            """
            SELECT uuid, workspace_provider_uuid
            FROM workspace_zulip_bridge.zulip_realms
            WHERE workspace_provider_uuid = $1
               OR (workspace_provider_uuid IS NULL
                   AND (workspace_project_id IS NULL OR workspace_project_id = $2))
            ORDER BY uuid
            """,
            self._provider_uuid,
            self._project_uuid,
        )
        exact = [row for row in rows if row["workspace_provider_uuid"] is not None]
        if len(exact) == 1:
            return UUID(str(exact[0]["uuid"]))
        if len(rows) != 1:
            raise RuntimeError("Workspace provider must map to exactly one Zulip realm")
        realm_uuid = UUID(str(rows[0]["uuid"]))
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_realms
            SET workspace_project_id = $2, workspace_provider_uuid = $3
            WHERE uuid = $1 AND workspace_provider_uuid IS NULL
            """,
            realm_uuid,
            self._project_uuid,
            self._provider_uuid,
        )
        return realm_uuid

    async def _ensure_direct_topics(self, realm_uuid: UUID) -> None:
        rows = await self._pool.fetch(
            """
            SELECT stream.uuid
            FROM workspace_zulip_bridge.zulip_streams AS stream
            WHERE stream.realm_uuid = $1 AND stream.chat_type <> 'channel'
              AND NOT EXISTS (
                  SELECT 1 FROM workspace_zulip_bridge.zulip_topics AS topic
                  WHERE topic.zulip_stream_uuid = stream.uuid
              )
            """,
            realm_uuid,
        )
        values = [
            (
                stable_topic_uuid(row["uuid"], "General"),
                row["uuid"],
                hashlib.sha256(b"General").digest(),
            )
            for row in rows
        ]
        topic_uuids = [value[0] for value in values]
        stream_uuids = [value[1] for value in values]
        content_hashes = [value[2] for value in values]
        async with self._pool.acquire() as connection, connection.transaction():
            if values:
                await connection.execute(
                    """
                    INSERT INTO workspace_zulip_bridge.zulip_topics
                        (uuid, zulip_stream_uuid, name, content_hash)
                    SELECT input.topic_uuid, input.stream_uuid, 'General',
                           input.content_hash
                    FROM unnest($1::uuid[], $2::uuid[], $3::bytea[])
                        AS input(topic_uuid, stream_uuid, content_hash)
                    ON CONFLICT (uuid) DO NOTHING
                    """,
                    topic_uuids,
                    stream_uuids,
                    content_hashes,
                )
            await connection.execute(
                """
                WITH pending AS MATERIALIZED (
                    SELECT message.ctid, topic.uuid AS topic_uuid
                    FROM workspace_zulip_bridge.zulip_messages AS message
                    JOIN workspace_zulip_bridge.zulip_streams AS stream
                      ON stream.uuid = message.zulip_stream_uuid
                    JOIN workspace_zulip_bridge.zulip_topics AS topic
                      ON topic.zulip_stream_uuid = stream.uuid
                     AND topic.name = 'General'
                    WHERE stream.realm_uuid = $1
                      AND stream.chat_type <> 'channel'
                      AND message.topic_uuid IS NULL
                    LIMIT 10000
                    FOR UPDATE OF message SKIP LOCKED
                )
                UPDATE workspace_zulip_bridge.zulip_messages AS message
                SET topic_uuid = pending.topic_uuid
                FROM pending WHERE message.ctid = pending.ctid
                """,
                realm_uuid,
            )

    async def _load_zulip_entities(
        self, entity_type: str, entity_uuids: list[UUID]
    ) -> dict[tuple[str, UUID], dict[str, Any]]:
        rows = await self._pool.fetch(_ENTITY_QUERIES[entity_type], entity_uuids)
        return {
            (entity_type, UUID(str(row["entity_uuid"]))): _json_object(row["data"])
            for row in rows
        }


def _event_entity_type(object_type: str) -> str | None:
    return {
        "user": "users",
        "stream": "streams",
        "stream_binding": "stream_bindings",
        "topic": "topics",
        "stream_topic": "topics",
        "topic_binding": "topic_bindings",
        "message": "messages",
        "message_flag": "message_flags",
        "message_reaction": "message_reactions",
    }.get(object_type)


_SOURCE_TABLES = {
    "users": {
        "from": "workspace_zulip_bridge.zulip_users",
        "joins": "",
        "where": "source.realm_uuid = $3",
        "hash": "source.profile_hash",
        "partition": "NULL::uuid",
    },
    "streams": {
        "from": "workspace_zulip_bridge.zulip_streams",
        "joins": """
            LEFT JOIN workspace_zulip_bridge.zulip_connections AS supplier
              ON supplier.uuid = source.source_connection_uuid
        """,
        "where": """
            source.realm_uuid = $3 AND source.source_connection_uuid IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_users AS owner
                WHERE owner.provider_uuid = $1
                  AND owner.snapshot_generation = $4
                  AND owner.uuid = COALESCE(
                      source.owner_user_uuid, supplier.zulip_user_uuid
                  )
            )
            AND (
                source.direct_user_uuid IS NULL OR EXISTS (
                    SELECT 1
                    FROM workspace_zulip_bridge.workspace_users AS direct_user
                    WHERE direct_user.provider_uuid = $1
                      AND direct_user.snapshot_generation = $4
                      AND direct_user.uuid = source.direct_user_uuid
                )
            )
        """,
        "hash": "source.content_hash",
        "partition": "NULL::uuid",
    },
    "stream_bindings": {
        "from": "workspace_zulip_bridge.zulip_stream_bindings",
        "joins": "JOIN workspace_zulip_bridge.zulip_streams AS parent ON parent.uuid = source.zulip_stream_uuid",
        "where": """
            parent.realm_uuid = $3 AND parent.source_connection_uuid IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_streams AS target_parent
                WHERE target_parent.provider_uuid = $1
                  AND target_parent.snapshot_generation = $4
                  AND target_parent.uuid = source.zulip_stream_uuid
            )
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_users AS target_user
                WHERE target_user.provider_uuid = $1
                  AND target_user.snapshot_generation = $4
                  AND target_user.uuid = source.zulip_user_uuid
            )
        """,
        "hash": "source.content_hash",
        "partition": "NULL::uuid",
    },
    "topics": {
        "from": "workspace_zulip_bridge.zulip_topics",
        "joins": "JOIN workspace_zulip_bridge.zulip_streams AS parent ON parent.uuid = source.zulip_stream_uuid",
        "where": """
            parent.realm_uuid = $3 AND parent.source_connection_uuid IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_streams AS target_parent
                WHERE target_parent.provider_uuid = $1
                  AND target_parent.snapshot_generation = $4
                  AND target_parent.uuid = source.zulip_stream_uuid
            )
        """,
        "hash": "source.content_hash",
        "partition": "NULL::uuid",
    },
    "topic_bindings": {
        "from": "workspace_zulip_bridge.zulip_topic_bindings",
        "joins": "JOIN workspace_zulip_bridge.zulip_streams AS parent ON parent.uuid = source.zulip_stream_uuid",
        "where": """
            parent.realm_uuid = $3 AND parent.source_connection_uuid IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_streams AS target_stream
                WHERE target_stream.provider_uuid = $1
                  AND target_stream.snapshot_generation = $4
                  AND target_stream.uuid = source.zulip_stream_uuid
            )
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_topics AS target_topic
                WHERE target_topic.provider_uuid = $1
                  AND target_topic.snapshot_generation = $4
                  AND target_topic.uuid = source.topic_uuid
            )
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_users AS target_user
                WHERE target_user.provider_uuid = $1
                  AND target_user.snapshot_generation = $4
                  AND target_user.uuid = source.zulip_user_uuid
            )
        """,
        "hash": "source.content_hash",
        "partition": "NULL::uuid",
    },
    "messages": {
        "from": "workspace_zulip_bridge.zulip_messages",
        "joins": "",
        "where": """
            source.realm_uuid = $3
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_streams AS target_stream
                WHERE target_stream.provider_uuid = $1
                  AND target_stream.snapshot_generation = $4
                  AND target_stream.uuid = source.zulip_stream_uuid
            )
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_topics AS target_topic
                WHERE target_topic.provider_uuid = $1
                  AND target_topic.snapshot_generation = $4
                  AND target_topic.uuid = source.topic_uuid
            )
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_users AS target_user
                WHERE target_user.provider_uuid = $1
                  AND target_user.snapshot_generation = $4
                  AND target_user.uuid = source.sender_user_uuid
            )
        """,
        "hash": "source.content_hash",
        "partition": "source.zulip_stream_uuid",
    },
    "message_flags": {
        "from": "workspace_zulip_bridge.zulip_message_flags",
        "joins": "",
        "where": """
            source.realm_uuid = $3
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_messages AS target_message
                WHERE target_message.provider_uuid = $1
                  AND target_message.snapshot_generation = $4
                  AND target_message.uuid = source.message_uuid
            )
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_users AS target_user
                WHERE target_user.provider_uuid = $1
                  AND target_user.snapshot_generation = $4
                  AND target_user.uuid = source.zulip_user_uuid
            )
        """,
        "hash": "source.flags_hash",
        "partition": "source.zulip_stream_uuid",
    },
    "message_reactions": {
        "from": "workspace_zulip_bridge.zulip_message_reactions",
        "joins": "JOIN workspace_zulip_bridge.zulip_messages AS message ON message.uuid = source.message_uuid",
        "where": """
            source.realm_uuid = $3
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_messages AS target_message
                WHERE target_message.provider_uuid = $1
                  AND target_message.snapshot_generation = $4
                  AND target_message.uuid = source.message_uuid
            )
            AND EXISTS (
                SELECT 1 FROM workspace_zulip_bridge.workspace_users AS target_user
                WHERE target_user.provider_uuid = $1
                  AND target_user.snapshot_generation = $4
                  AND target_user.uuid = source.zulip_user_uuid
            )
        """,
        "hash": "NULL::bytea",
        "partition": "message.zulip_stream_uuid",
    },
}


_ENTITY_QUERIES = {
    "users": """
        SELECT zulip_user.uuid AS entity_uuid, jsonb_build_object(
            'username', zulip_user.login,
            'display_name', zulip_user.full_name,
            'email', zulip_user.login,
            'status', zulip_user.presence_status,
            'disabled', zulip_user.disabled,
            'is_bot', zulip_user.is_bot,
            'avatar', CASE
                WHEN zulip_user.avatar_url IS NULL OR zulip_user.avatar_url = ''
                    THEN NULL
                WHEN zulip_user.avatar_url LIKE 'urn:%'
                    THEN zulip_user.avatar_url
                WHEN zulip_user.avatar_url LIKE 'http://%'
                     OR zulip_user.avatar_url LIKE 'https://%'
                    THEN 'urn:url:' || zulip_user.avatar_url
                WHEN zulip_user.avatar_url LIKE '/%'
                    THEN 'urn:url:' || rtrim(realm.endpoint, '/')
                         || zulip_user.avatar_url
                ELSE NULL
            END,
            'last_ping_at', zulip_user.last_ping_at,
            'status_text', zulip_user.status_text,
            'status_emoji', zulip_user.status_emoji,
            'created_at', zulip_user.created_at
        ) AS data
        FROM workspace_zulip_bridge.zulip_users AS zulip_user
        JOIN workspace_zulip_bridge.zulip_realms AS realm
          ON realm.uuid = zulip_user.realm_uuid
        WHERE zulip_user.uuid = ANY($1::uuid[])
    """,
    "streams": """
        SELECT stream.uuid AS entity_uuid, jsonb_build_object(
            'name', stream.name, 'description', stream.description,
            'owner_uuid', COALESCE(stream.owner_user_uuid, connection.zulip_user_uuid),
            'invite_only', stream.invite_only, 'announce', stream.announce,
            'direct_user_uuid', stream.direct_user_uuid,
            'private', stream.private, 'is_archived', stream.is_archived,
            'color', COALESCE(stream.color, 0),
            'history_public_to_subscribers',
                COALESCE((stream.chat_parameters ->> 'history_public_to_subscribers')::boolean, true),
            'created_at', stream.created_at
        ) AS data
        FROM workspace_zulip_bridge.zulip_streams AS stream
        LEFT JOIN workspace_zulip_bridge.zulip_connections AS connection
          ON connection.uuid = stream.source_connection_uuid
        WHERE stream.uuid = ANY($1::uuid[])
    """,
    "stream_bindings": """
        SELECT binding.uuid AS entity_uuid, jsonb_build_object(
            'stream_uuid', binding.zulip_stream_uuid,
            'user_uuid', binding.zulip_user_uuid,
            'who_uuid', COALESCE(stream.owner_user_uuid, connection.zulip_user_uuid),
            'role', binding.role,
            'notification_mode', binding.notification_mode,
            'created_at', binding.created_at
        ) AS data
        FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
        JOIN workspace_zulip_bridge.zulip_streams AS stream
          ON stream.uuid = binding.zulip_stream_uuid
        LEFT JOIN workspace_zulip_bridge.zulip_connections AS connection
          ON connection.uuid = stream.source_connection_uuid
        WHERE binding.uuid = ANY($1::uuid[])
    """,
    "topics": """
        SELECT uuid AS entity_uuid, jsonb_build_object(
            'stream_uuid', zulip_stream_uuid, 'name', name,
            'is_done', is_done, 'version', version, 'created_at', created_at
        ) AS data FROM workspace_zulip_bridge.zulip_topics
        WHERE uuid = ANY($1::uuid[])
    """,
    "topic_bindings": """
        SELECT uuid AS entity_uuid, jsonb_build_object(
            'stream_uuid', zulip_stream_uuid, 'topic_uuid', topic_uuid,
            'user_uuid', zulip_user_uuid, 'notification_mode', notification_mode,
            'created_at', created_at
        ) AS data FROM workspace_zulip_bridge.zulip_topic_bindings
        WHERE uuid = ANY($1::uuid[])
    """,
    "messages": """
        SELECT uuid AS entity_uuid, jsonb_build_object(
            'stream_uuid', zulip_stream_uuid, 'topic_uuid', topic_uuid,
            'author_uuid', sender_user_uuid,
            'payload', jsonb_build_object('kind', 'markdown', 'content', content),
            'created_at', created_at
        ) AS data FROM workspace_zulip_bridge.zulip_messages
        WHERE uuid = ANY($1::uuid[])
    """,
    "message_flags": """
        SELECT uuid AS entity_uuid, jsonb_build_object(
            'stream_uuid', zulip_stream_uuid, 'message_uuid', message_uuid,
            'user_uuid', zulip_user_uuid, 'read', is_read,
            'pinned', false, 'starred', is_starred, 'mentioned', is_mentioned
        ) AS data FROM workspace_zulip_bridge.zulip_message_flags
        WHERE uuid = ANY($1::uuid[])
    """,
    "message_reactions": """
        SELECT uuid AS entity_uuid, jsonb_build_object(
            'message_uuid', message_uuid, 'user_uuid', zulip_user_uuid,
            'emoji_name', emoji_name, 'created_at', created_at
        ) AS data FROM workspace_zulip_bridge.zulip_message_reactions
        WHERE uuid = ANY($1::uuid[])
    """,
}
