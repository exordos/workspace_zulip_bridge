# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Publish bridge-owned Zulip chat catalogs to Workspace control state."""

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import logging
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID
from uuid import uuid5

import asyncpg
import httpx
from asyncpg.pool import PoolConnectionProxy

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.stable_ids import stable_external_chat_uuid
from workspace_zulip_bridge.topic_state import topic_display_name
from workspace_zulip_bridge.workspace_file_transfer import CATALOG_PROJECTION_REVISION
from workspace_zulip_bridge.workspace_file_transfer import MAX_FILE_BYTES

LOG = logging.getLogger(__name__)

_CHANNEL_CAPABILITIES = frozenset(
    {
        "messenger.membership.write",
        "messenger.notification.write",
        "messenger.stream.rename",
        "messenger.topic.rename",
    }
)
_COMMON_CAPABILITIES = frozenset(
    {
        "messenger.chat_catalog",
        "messenger.file.transfer",
        "messenger.message.delete",
        "messenger.message.edit",
        "messenger.message.read",
        "messenger.message.read.paging",
        "messenger.message.send",
        "messenger.reaction.write",
    }
)


def _legacy_catalog_topic_provider_id(chat_key: str, display_name: str) -> str:
    if not chat_key.startswith("channel:"):
        raise ValueError("catalog topic identities only apply to channels")
    return f"{chat_key.removeprefix('channel:')}:{display_name}"


def _opaque_catalog_topic_provider_id(topic_uuid: UUID) -> str:
    return f"topic-uuid:{topic_uuid}"


class CatalogReportError(RuntimeError):
    """A catalog publication failure safe to expose in bridge diagnostics."""

    def __init__(self, code: str, *, retryable: bool = True) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class _CatalogSource:
    account_uuid: UUID
    stream_uuid: UUID
    desired_generation: int
    owner_workspace_user_uuid: UUID
    project_uuid: UUID
    realm_uuid: UUID
    owner_zulip_user_uuid: UUID
    owner_zulip_user_id: int
    chat_type: str
    chat_key: str
    display_name: str
    description: str
    chat_parameters: Mapping[str, object]
    source_activity_at: datetime.datetime
    source_updated_at: datetime.datetime


class WorkspaceChatCatalogWorker:
    """Keep Workspace control assignments aligned with bridge chat state."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        settings: Settings,
        *,
        coordinate: bool = True,
        control_semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        if settings.workspace_control_url is None:
            raise ValueError("Workspace control must be configured")
        if settings.workspace_bridge_instance_uuid is None:
            raise ValueError("Workspace bridge identity must be configured")
        self._pool = pool
        self._settings = settings
        self._control_url = settings.workspace_control_url.rstrip("/")
        self._bridge_uuid = settings.workspace_bridge_instance_uuid
        self._state = settings.workspace_control_state_dir
        self._coordinate = coordinate
        self._assignment_repair_key: tuple[UUID, str] | None = None
        self._control_semaphore = control_semaphore or asyncio.Semaphore(
            settings.workspace_file_control_concurrency
        )

    async def run(self) -> None:
        while True:
            changed = 0
            try:
                changed = await self.process_once()
            except asyncio.CancelledError:
                raise
            except (TimeoutError, asyncpg.PostgresError, httpx.HTTPError) as error:
                LOG.warning(
                    "Workspace chat catalog pass failed: error=%s",
                    type(error).__name__,
                )
            except (CatalogReportError, ValueError) as error:
                LOG.warning(
                    "Workspace chat catalog data is not ready: error=%s",
                    type(error).__name__,
                )
            # A catalog report is already serialized through the shared control
            # semaphore.  Drain ready work without an artificial two-second gap;
            # only the idle path needs polling backoff.
            await asyncio.sleep(
                0 if changed else self._settings.workspace_control_poll_seconds
            )

    async def process_once(self) -> int:
        refreshed = 0
        retired = 0
        if self._coordinate:
            # Do not let an old report backlog hide newly discovered chats or
            # a projection-format upgrade.  Only the coordinator performs this
            # bounded, newest-first refresh; the remaining workers keep
            # delivering reports in parallel.
            refreshed = await self._refresh_catalogs(priority_only=True)
        reports = await self._claim_reports()
        if not reports and self._coordinate:
            refreshed += await self._refresh_catalogs()
            retired = await self._retire_unneeded_reports()
            reports = await self._claim_reports()
        if not reports:
            return refreshed + retired
        try:
            outcomes = await self._send_reports(
                [_object(report["report"]) for report in reports]
            )
        except asyncio.CancelledError:
            raise
        except CatalogReportError as error:
            for report in reports:
                await self._fail_report(
                    UUID(str(report["report_uuid"])),
                    error.code,
                    retryable=error.retryable,
                )
        except (TimeoutError, httpx.HTTPError) as error:
            for report in reports:
                await self._fail_report(
                    UUID(str(report["report_uuid"])),
                    type(error).__name__,
                    retryable=True,
                )
        else:
            for report in reports:
                report_uuid = UUID(str(report["report_uuid"]))
                await self._finish_report(report_uuid, outcomes[report_uuid])
        return refreshed + retired + len(reports)

    async def run_assignment_repairs(self) -> None:
        """Repair persisted assignments independently of catalog publication."""
        while True:
            changed = 0
            try:
                changed = await self._requeue_assignment_messages()
            except (TimeoutError, asyncpg.PostgresError, ValueError):
                LOG.warning("Workspace assignment repair deferred")
            await asyncio.sleep(
                0 if changed else self._settings.workspace_control_poll_seconds
            )

    async def _requeue_assignment_messages(self) -> int:
        self._assignment_repair_key = None
        try:
            return await self._repair_assignment_page()
        except (TimeoutError, asyncpg.PostgresError, ValueError) as error:
            if self._assignment_repair_key is not None:
                report_uuid, assignment = self._assignment_repair_key
                await self._pool.execute(
                    """
                    UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
                    SET assignment_repair_available_at = clock_timestamp() + interval '1 minute',
                        assignment_repair_last_error = $3, updated_at = clock_timestamp()
                    WHERE report_uuid = $1 AND assignment = $2::jsonb
                    """,
                    report_uuid,
                    assignment,
                    type(error).__name__,
                )
            raise

    async def _repair_assignment_page(self) -> int:
        """Scan one index-supported page, advancing past already-correct rows."""
        from workspace_zulip_bridge.workspace_sync import _catalog_projection_topic_uuid
        from workspace_zulip_bridge.workspace_sync import _catalog_topic_provider_id

        stages = (
            ("stream", "zulip_streams", "uuid"),
            ("stream_binding", "zulip_stream_bindings", "zulip_stream_uuid"),
            ("topic", "zulip_topics", "zulip_stream_uuid"),
            ("topic_binding", "zulip_topic_bindings", "zulip_stream_uuid"),
            ("message", "zulip_messages", "zulip_stream_uuid"),
            ("message_flag", "zulip_message_flags", "zulip_stream_uuid"),
        )
        async with self._pool.acquire() as connection, connection.transaction():
            report = await connection.fetchrow(
                """
                SELECT report.external_account_uuid, report.zulip_stream_uuid,
                       report.report_uuid, report.assignment::text, report.catalog,
                       report.resource_uuid, stream.chat_type, stream.chat_key,
                       report.assignment_repair_stage,
                       report.assignment_repair_entity_uuid,
                       report.assignment_repair_created_at,
                       report.assignment_repair_uuid,
                       realm.uuid AS realm_uuid, realm.workspace_provider_uuid,
                       mirror.active_generation,
                       (report.assignment #>>
                           '{workspace_projection,stream,uuid}')
                           AS projection_stream_uuid
                FROM workspace_zulip_bridge.workspace_chat_catalog_reports AS report
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = report.zulip_stream_uuid
                JOIN workspace_zulip_bridge.zulip_connections AS supplier
                  ON supplier.uuid = stream.source_connection_uuid
                 AND supplier.external_account_uuid = report.external_account_uuid
                 AND supplier.sync_enabled
                JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = stream.realm_uuid
                JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
                  ON mirror.provider_uuid = realm.workspace_provider_uuid
                 AND mirror.active_generation IS NOT NULL
                WHERE report.processing_status = 'reported'
                  AND report.projection_revision >= $1
                  AND report.assignment IS NOT NULL
                  AND report.assignment #>> '{workspace_projection,stream,uuid}' IS NOT NULL
                  AND NOT report.assignment_reconciled
                  AND report.assignment_repair_available_at <= clock_timestamp()
                ORDER BY report.updated_at, report.external_account_uuid,
                         report.zulip_stream_uuid
                LIMIT 1 FOR UPDATE OF report SKIP LOCKED
                """,
                CATALOG_PROJECTION_REVISION,
            )
            if report is None:
                return 0
            self._assignment_repair_key = (report["report_uuid"], report["assignment"])
            projection_stream_uuid = UUID(report["projection_stream_uuid"])
            assignment = _object(report["assignment"])
            stage = int(report["assignment_repair_stage"])
            entity_type, table, stream_column = stages[stage]
            # SQL identifiers below come exclusively from the fixed stage list.
            # Scan before testing target state; otherwise a matching prefix can
            # force an unbounded read or continually hide later mismatches.
            if entity_type == "message":
                rows = await connection.fetch(
                    """
                    WITH page AS MATERIALIZED (
                        SELECT uuid, created_at, topic_uuid
                        FROM workspace_zulip_bridge.zulip_messages
                        WHERE zulip_stream_uuid = $1
                          AND ($2::timestamptz IS NULL OR (created_at, uuid) < ($2, $3::uuid))
                        ORDER BY created_at DESC, uuid DESC LIMIT 1000
                    )
                    SELECT page.uuid, page.created_at, identity.provider_topic_id,
                           target.data ->> 'topic_uuid' AS target_topic_uuid,
                           (target.uuid IS NULL OR
                            (target.data ->> 'stream_uuid')::uuid IS DISTINCT FROM $6::uuid)
                               AS needs_repair
                    FROM page
                    LEFT JOIN workspace_zulip_bridge.zulip_topic_catalog_identities AS identity
                      ON identity.topic_uuid = page.topic_uuid
                     AND identity.zulip_stream_uuid = $1
                    LEFT JOIN workspace_zulip_bridge.workspace_messages AS target
                      ON target.provider_uuid = $4 AND target.snapshot_generation = $5
                     AND target.uuid = page.uuid
                    ORDER BY page.created_at DESC, page.uuid DESC
                    """,
                    report["zulip_stream_uuid"],
                    report["assignment_repair_created_at"],
                    report["assignment_repair_uuid"],
                    report["workspace_provider_uuid"],
                    report["active_generation"],
                    projection_stream_uuid,
                )
            else:
                rows = await connection.fetch(
                    f"""
                    SELECT uuid, true AS needs_repair
                    FROM workspace_zulip_bridge.{table}
                    WHERE {stream_column} = $1
                      AND ($2::uuid IS NULL OR uuid > $2)
                    ORDER BY uuid LIMIT 1000
                    """,
                    report["zulip_stream_uuid"],
                    report["assignment_repair_entity_uuid"],
                )
            entity_uuids = []
            topic_projection_uuids: dict[str, UUID] = {}
            for row in rows:
                needs_repair = row["needs_repair"]
                if entity_type == "message":
                    provider_topic_id = _catalog_topic_provider_id(
                        report["chat_type"],
                        report["chat_key"],
                        row["provider_topic_id"],
                    )
                    if provider_topic_id not in topic_projection_uuids:
                        topic_projection_uuids[provider_topic_id] = (
                            _catalog_projection_topic_uuid(
                                _object(report["catalog"]),
                                report["resource_uuid"],
                                provider_topic_id,
                                assignment,
                            )
                        )
                    topic_uuid = topic_projection_uuids[provider_topic_id]
                    needs_repair = needs_repair or row["target_topic_uuid"] != str(
                        topic_uuid
                    )
                if needs_repair:
                    entity_uuids.append(row["uuid"])
            if entity_uuids:
                await connection.execute(
                    """
                    INSERT INTO workspace_zulip_bridge.workspace_outbox (
                        realm_uuid, entity_type, action, entity_uuid, import_required
                    )
                    SELECT $1, $2, 'upsert', uuid, true FROM unnest($3::uuid[]) AS uuid
                    ON CONFLICT (realm_uuid, entity_type, entity_uuid)
                        WHERE delivery_status = 'pending'
                    DO UPDATE SET action = 'upsert', entity_hash = NULL,
                        import_required = true, available_at = clock_timestamp(), updated_at = clock_timestamp()
                    """,
                    report["realm_uuid"],
                    entity_type,
                    entity_uuids,
                )
            finished_stage = len(rows) < 1000
            next_stage = min(stage + int(finished_stage), len(stages) - 1)
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
                SET assignment_repair_stage = $3,
                    assignment_repair_entity_uuid = $4,
                    assignment_repair_created_at = $5,
                    assignment_repair_uuid = $6,
                    assignment_reconciled = $7,
                    assignment_repair_last_error = NULL,
                    updated_at = clock_timestamp()
                WHERE external_account_uuid = $1 AND zulip_stream_uuid = $2
                """,
                report["external_account_uuid"],
                report["zulip_stream_uuid"],
                next_stage,
                rows[-1]["uuid"] if rows and not finished_stage else None,
                rows[-1]["created_at"]
                if rows and entity_type == "message"
                else report["assignment_repair_created_at"],
                rows[-1]["uuid"]
                if rows and entity_type == "message"
                else report["assignment_repair_uuid"],
                finished_stage and stage == len(stages) - 1,
            )
            return max(1, len(rows))

    async def _refresh_catalogs(self, *, priority_only: bool = False) -> int:
        changed = await self._reactivate_needed_reports()
        # Newly discovered and reassigned chats must not wait for the complete
        # historical catalog comparison. Select that small realtime lane first,
        # then calculate the more expensive aggregate timestamp only for the
        # selected chats. The historical comparison below remains the fallback
        # once this lane is empty.
        rows = await self._pool.fetch(
            """
            WITH candidates AS MATERIALIZED (
                SELECT connection.external_account_uuid AS account_uuid,
                       stream.uuid AS stream_uuid,
                       connection.desired_generation,
                       connection.owner_workspace_user_uuid,
                       realm.workspace_project_id AS project_uuid,
                       realm.uuid AS realm_uuid,
                       owner.uuid AS owner_zulip_user_uuid,
                       owner.zulip_user_id AS owner_zulip_user_id,
                       stream.chat_type, stream.chat_key,
                       stream.name AS display_name, stream.description,
                       stream.chat_parameters,
                       stream.created_at AS stream_created_at,
                       stream.updated_at AS stream_updated_at,
                       owner_binding.updated_at AS owner_binding_updated_at,
                       owner.updated_at AS owner_updated_at
                FROM workspace_zulip_bridge.zulip_connections AS connection
                JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = connection.realm_uuid
                JOIN workspace_zulip_bridge.zulip_users AS owner
                  ON owner.uuid = connection.zulip_user_uuid
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.source_connection_uuid = connection.uuid
                JOIN workspace_zulip_bridge.zulip_stream_bindings AS owner_binding
                  ON owner_binding.zulip_user_uuid = owner.uuid
                 AND owner_binding.zulip_stream_uuid = stream.uuid
                LEFT JOIN workspace_zulip_bridge.workspace_chat_catalog_reports
                    AS report
                  ON report.external_account_uuid =
                         connection.external_account_uuid
                 AND report.zulip_stream_uuid = stream.uuid
                WHERE connection.sync_enabled
                  AND connection.external_account_uuid IS NOT NULL
                  AND connection.owner_workspace_user_uuid IS NOT NULL
                  AND connection.desired_generation IS NOT NULL
                  AND realm.workspace_project_id IS NOT NULL
                  AND NOT owner.disabled
                  AND (
                      report.external_account_uuid IS NULL
                      OR report.observed_generation <>
                         connection.desired_generation
                      OR report.projection_revision <> $1
                )
                ORDER BY (report.external_account_uuid IS NULL) DESC,
                         COALESCE(
                             report.source_activity_at,
                             stream.created_at
                         ) DESC,
                         stream.updated_at DESC,
                         connection.external_account_uuid, stream.uuid
                LIMIT 20
            )
            SELECT candidate.account_uuid, candidate.stream_uuid,
                   candidate.desired_generation,
                   candidate.owner_workspace_user_uuid,
                   candidate.project_uuid, candidate.realm_uuid,
                   candidate.owner_zulip_user_uuid,
                   candidate.owner_zulip_user_id,
                   candidate.chat_type, candidate.chat_key,
                   candidate.display_name, candidate.description,
                   candidate.chat_parameters,
                   GREATEST(
                       candidate.stream_created_at,
                       COALESCE((
                           SELECT message.created_at
                           FROM workspace_zulip_bridge.zulip_messages AS message
                           WHERE message.zulip_stream_uuid = candidate.stream_uuid
                           ORDER BY message.created_at DESC, message.uuid DESC
                           LIMIT 1
                       ), '-infinity'::timestamptz)
                   ) AS source_activity_at,
                   EXISTS (
                       SELECT 1
                       FROM workspace_zulip_bridge.workspace_file_projections
                           AS live_projection
                       WHERE live_projection.zulip_stream_uuid =
                                 candidate.stream_uuid
                         AND live_projection.delivery_priority = 0
                         AND live_projection.processing_status IN (
                             'pending', 'processing', 'failed'
                         )
                   ) AS has_live_file_work,
                   (
                       SELECT max(live_file.source_created_at)
                       FROM workspace_zulip_bridge.workspace_file_projections
                           AS live_projection
                       JOIN workspace_zulip_bridge.zulip_files AS live_file
                         ON live_file.uuid = live_projection.file_uuid
                       WHERE live_projection.zulip_stream_uuid =
                                 candidate.stream_uuid
                         AND live_projection.delivery_priority = 0
                         AND live_projection.processing_status IN (
                             'pending', 'processing', 'failed'
                         )
                   ) AS newest_live_file_at,
                   GREATEST(
                       candidate.stream_updated_at,
                       candidate.owner_binding_updated_at,
                       candidate.owner_updated_at,
                       COALESCE((
                           SELECT max(topic.updated_at)
                           FROM workspace_zulip_bridge.zulip_topics AS topic
                           WHERE topic.zulip_stream_uuid = candidate.stream_uuid
                       ), '-infinity'::timestamptz),
                       COALESCE((
                           SELECT max(GREATEST(binding.updated_at, member.updated_at))
                           FROM workspace_zulip_bridge.zulip_stream_bindings
                               AS binding
                           JOIN workspace_zulip_bridge.zulip_users AS member
                             ON member.uuid = binding.zulip_user_uuid
                           WHERE binding.zulip_stream_uuid =
                                 candidate.stream_uuid
                       ), '-infinity'::timestamptz)
                   ) AS source_updated_at
            FROM candidates AS candidate
            ORDER BY candidate.stream_updated_at DESC,
                     candidate.account_uuid, candidate.stream_uuid
            """,
            CATALOG_PROJECTION_REVISION,
        )
        if not rows and not priority_only:
            rows = await self._pool.fetch(
                """
                SELECT connection.external_account_uuid AS account_uuid,
                   stream.uuid AS stream_uuid,
                   connection.desired_generation,
                   connection.owner_workspace_user_uuid,
                   realm.workspace_project_id AS project_uuid,
                   realm.uuid AS realm_uuid,
                   owner.uuid AS owner_zulip_user_uuid,
                   owner.zulip_user_id AS owner_zulip_user_id,
                   stream.chat_type, stream.chat_key,
                   stream.name AS display_name, stream.description,
                   stream.chat_parameters,
                   GREATEST(
                       stream.created_at,
                       COALESCE((
                           SELECT message.created_at
                           FROM workspace_zulip_bridge.zulip_messages AS message
                           WHERE message.zulip_stream_uuid = stream.uuid
                           ORDER BY message.created_at DESC, message.uuid DESC
                           LIMIT 1
                       ), '-infinity'::timestamptz)
                   ) AS source_activity_at,
                   EXISTS (
                       SELECT 1
                       FROM workspace_zulip_bridge.workspace_file_projections
                           AS live_projection
                       WHERE live_projection.zulip_stream_uuid = stream.uuid
                         AND live_projection.delivery_priority = 0
                         AND live_projection.processing_status IN (
                             'pending', 'processing', 'failed'
                         )
                   ) AS has_live_file_work,
                   (
                       SELECT max(live_file.source_created_at)
                       FROM workspace_zulip_bridge.workspace_file_projections
                           AS live_projection
                       JOIN workspace_zulip_bridge.zulip_files AS live_file
                         ON live_file.uuid = live_projection.file_uuid
                       WHERE live_projection.zulip_stream_uuid = stream.uuid
                         AND live_projection.delivery_priority = 0
                         AND live_projection.processing_status IN (
                             'pending', 'processing', 'failed'
                         )
                   ) AS newest_live_file_at,
                   GREATEST(
                       stream.updated_at,
                       owner_binding.updated_at,
                       owner.updated_at,
                       COALESCE((
                           SELECT max(topic.updated_at)
                           FROM workspace_zulip_bridge.zulip_topics AS topic
                           WHERE topic.zulip_stream_uuid = stream.uuid
                       ), '-infinity'::timestamptz),
                       COALESCE((
                           SELECT max(GREATEST(binding.updated_at, member.updated_at))
                           FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
                           JOIN workspace_zulip_bridge.zulip_users AS member
                             ON member.uuid = binding.zulip_user_uuid
                           WHERE binding.zulip_stream_uuid = stream.uuid
                       ), '-infinity'::timestamptz)
                   ) AS source_updated_at
            FROM workspace_zulip_bridge.zulip_connections AS connection
            JOIN workspace_zulip_bridge.zulip_realms AS realm
              ON realm.uuid = connection.realm_uuid
            JOIN workspace_zulip_bridge.zulip_users AS owner
              ON owner.uuid = connection.zulip_user_uuid
            JOIN workspace_zulip_bridge.zulip_streams AS stream
              ON stream.source_connection_uuid = connection.uuid
            JOIN workspace_zulip_bridge.zulip_stream_bindings AS owner_binding
              ON owner_binding.zulip_user_uuid = owner.uuid
             AND owner_binding.zulip_stream_uuid = stream.uuid
            LEFT JOIN workspace_zulip_bridge.workspace_chat_catalog_reports
                AS report
              ON report.external_account_uuid = connection.external_account_uuid
             AND report.zulip_stream_uuid = stream.uuid
            WHERE connection.sync_enabled
              AND connection.external_account_uuid IS NOT NULL
              AND connection.owner_workspace_user_uuid IS NOT NULL
              AND connection.desired_generation IS NOT NULL
              AND realm.workspace_project_id IS NOT NULL
              AND NOT owner.disabled
              AND (
                  report.external_account_uuid IS NULL
                  OR report.observed_generation <> connection.desired_generation
                  OR report.projection_revision <> $1
                  OR report.source_updated_at < GREATEST(
                       stream.updated_at,
                       owner_binding.updated_at,
                       owner.updated_at,
                       COALESCE((
                           SELECT max(topic.updated_at)
                           FROM workspace_zulip_bridge.zulip_topics AS topic
                           WHERE topic.zulip_stream_uuid = stream.uuid
                       ), '-infinity'::timestamptz),
                       COALESCE((
                           SELECT max(GREATEST(binding.updated_at, member.updated_at))
                           FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
                           JOIN workspace_zulip_bridge.zulip_users AS member
                             ON member.uuid = binding.zulip_user_uuid
                           WHERE binding.zulip_stream_uuid = stream.uuid
                       ), '-infinity'::timestamptz)
                  )
              )
            ORDER BY source_updated_at DESC,
                     has_live_file_work DESC,
                     newest_live_file_at DESC NULLS LAST,
                     connection.external_account_uuid, stream.uuid
                LIMIT 20
                """,
                CATALOG_PROJECTION_REVISION,
            )
        for row in rows:
            source = _CatalogSource(
                account_uuid=UUID(str(row["account_uuid"])),
                stream_uuid=UUID(str(row["stream_uuid"])),
                desired_generation=int(row["desired_generation"]),
                owner_workspace_user_uuid=UUID(str(row["owner_workspace_user_uuid"])),
                project_uuid=UUID(str(row["project_uuid"])),
                realm_uuid=UUID(str(row["realm_uuid"])),
                owner_zulip_user_uuid=UUID(str(row["owner_zulip_user_uuid"])),
                owner_zulip_user_id=int(row["owner_zulip_user_id"]),
                chat_type=str(row["chat_type"]),
                chat_key=str(row["chat_key"]),
                display_name=str(row["display_name"]),
                description=str(row["description"]),
                chat_parameters=_object(row["chat_parameters"]),
                source_activity_at=row["source_activity_at"],
                source_updated_at=row["source_updated_at"],
            )
            try:
                catalog = await self._build_catalog(source)
                changed += await self._queue_catalog(source, catalog)
            except CatalogReportError as error:
                LOG.warning(
                    "Workspace chat catalog entry is not ready: error=%s",
                    error.code,
                )
        return changed

    async def _reactivate_needed_reports(self) -> int:
        result = await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports AS report
            SET processing_status = 'pending', attempt_count = 0,
                available_at = clock_timestamp(), claimed_at = NULL,
                last_error = NULL, updated_at = clock_timestamp()
            WHERE report.processing_status = 'blocked'
              AND report.last_error IN (
                  'catalog_not_required_for_file_transfer',
                  'catalog_source_inactive'
              )
              AND EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.zulip_streams AS stream
                  JOIN workspace_zulip_bridge.zulip_connections AS connection
                    ON connection.uuid = stream.source_connection_uuid
                  JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                    ON zulip_user.uuid = connection.zulip_user_uuid
                  WHERE stream.uuid = report.zulip_stream_uuid
                    AND connection.external_account_uuid =
                        report.external_account_uuid
                    AND connection.sync_enabled
                    AND NOT zulip_user.disabled
              )
            """
        )
        return int(result.rsplit(" ", 1)[-1])

    async def _retire_unneeded_reports(self) -> int:
        """Stop queued reports that no longer belong to the stream supplier."""

        result = await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports AS report
            SET processing_status = 'blocked', claimed_at = NULL,
                last_error = 'catalog_source_inactive',
                updated_at = clock_timestamp()
            WHERE report.processing_status IN ('pending', 'processing', 'failed')
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.zulip_streams AS stream
                  JOIN workspace_zulip_bridge.zulip_connections AS connection
                    ON connection.uuid = stream.source_connection_uuid
                  JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                    ON zulip_user.uuid = connection.zulip_user_uuid
                  WHERE stream.uuid = report.zulip_stream_uuid
                    AND connection.external_account_uuid =
                        report.external_account_uuid
                    AND connection.sync_enabled
                    AND NOT zulip_user.disabled
              )
            """
        )
        return int(result.rsplit(" ", 1)[-1])

    async def _build_catalog(self, source: _CatalogSource) -> dict[str, object]:
        participants = await self._participants(source)
        # Workspace direct chats require two distinct identities, while Zulip
        # also permits a user to send a private message to themselves. Model
        # that provider-only shape as a one-member channel instead of creating
        # a fake second identity or leaving its files permanently unauthorized.
        catalog_chat_type = (
            "channel"
            if source.chat_type == "direct" and len(participants) == 1
            else source.chat_type
        )
        topics = await self._topics(source)
        capabilities = _COMMON_CAPABILITIES
        if catalog_chat_type == "channel":
            capabilities |= _CHANNEL_CAPABILITIES
        return {
            "operation": "upsert",
            "external_account_uuid": str(source.account_uuid),
            "owner_user_uuid": str(source.owner_workspace_user_uuid),
            "provider_kind": "zulip",
            "project_id": str(source.project_uuid),
            "source": {
                "kind": "zulip",
                "chat_type": catalog_chat_type,
                "provider_chat_key": source.chat_key,
                "provider_realm_uuid": str(source.realm_uuid),
                "provider_owner_user_id": str(source.owner_zulip_user_id),
                "original_url": None,
            },
            "display_name": source.display_name,
            "description": source.description,
            "participants": participants,
            "topics": topics,
            "capabilities": {
                name: {
                    "available": True,
                    "revision": 1,
                    "limits": (
                        {"max_file_bytes": MAX_FILE_BYTES}
                        if name == "messenger.file.transfer"
                        else {}
                    ),
                }
                for name in sorted(capabilities)
            },
        }

    async def _participants(
        self,
        source: _CatalogSource,
    ) -> list[dict[str, object]]:
        if source.chat_type == "channel":
            rows = await self._pool.fetch(
                """
                SELECT member.uuid, member.zulip_user_id, member.full_name,
                       member.login, member.avatar_url
                FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
                JOIN workspace_zulip_bridge.zulip_users AS member
                  ON member.uuid = binding.zulip_user_uuid
                WHERE binding.zulip_stream_uuid = $1 AND NOT member.disabled
                ORDER BY member.zulip_user_id
                """,
                source.stream_uuid,
            )
        else:
            raw_ids = source.chat_parameters.get("participant_user_ids", [])
            if not isinstance(raw_ids, list):
                raw_ids = []
            participant_ids = [
                value
                for value in raw_ids
                if isinstance(value, int) and not isinstance(value, bool)
            ]
            if source.owner_zulip_user_id not in participant_ids:
                participant_ids.append(source.owner_zulip_user_id)
            rows = await self._pool.fetch(
                """
                SELECT uuid, zulip_user_id, full_name, login, avatar_url
                FROM workspace_zulip_bridge.zulip_users
                WHERE realm_uuid = $1 AND zulip_user_id = ANY($2::bigint[])
                ORDER BY zulip_user_id
                """,
                source.realm_uuid,
                sorted(set(participant_ids)),
            )
        participants: list[dict[str, object]] = [
            {
                "provider_user_id": str(row["zulip_user_id"]),
                "display_name": str(row["full_name"]),
                "email": str(row["login"]) if row["login"] else None,
                "avatar_urn": (
                    str(row["avatar_url"])
                    if str(row["avatar_url"] or "").startswith("urn:")
                    else None
                ),
                "is_owner": UUID(str(row["uuid"])) == source.owner_zulip_user_uuid,
            }
            for row in rows
        ]
        if not any(participant["is_owner"] for participant in participants):
            raise CatalogReportError("catalog_owner_missing", retryable=False)
        minimum = 1
        if source.chat_type == "group_direct":
            minimum = 3
        if len(participants) < minimum:
            raise CatalogReportError(
                "catalog_participants_incomplete",
                retryable=True,
            )
        return participants

    async def _topics(self, source: _CatalogSource) -> list[dict[str, object]]:
        if source.chat_type != "channel":
            topic_uuid = await self._pool.fetchval(
                """
                SELECT uuid
                FROM workspace_zulip_bridge.zulip_topics
                WHERE zulip_stream_uuid = $1
                ORDER BY created_at, uuid
                LIMIT 1
                """,
                source.stream_uuid,
            )
            if topic_uuid is None:
                raise CatalogReportError("catalog_topic_missing", retryable=True)
            return [
                {
                    "provider_topic_id": f"{source.chat_key}:default",
                    "name": "Zulip",
                    "is_default": True,
                }
            ]
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.fetchrow(
                """
                SELECT uuid
                FROM workspace_zulip_bridge.zulip_streams
                WHERE uuid = $1
                FOR UPDATE
                """,
                source.stream_uuid,
            )
            await self._ensure_topic_catalog_identities(connection, source)
            rows = await connection.fetch(
                """
                SELECT topic.uuid, topic.name, identity.provider_topic_id,
                       workspace_topic.uuid IS NOT NULL
                       AND NOT EXISTS (
                           SELECT 1
                           FROM workspace_zulip_bridge.workspace_chat_catalog_reports
                               AS report,
                           LATERAL jsonb_array_elements(COALESCE(
                               report.assignment #>
                                   '{workspace_projection,topics}',
                               '[]'::jsonb
                           )) AS assigned_topic
                           WHERE report.zulip_stream_uuid = topic.zulip_stream_uuid
                             AND assigned_topic ->> 'provider_topic_id' =
                                   identity.provider_topic_id
                       ) AS preserves_projection
                FROM workspace_zulip_bridge.zulip_topics AS topic
                LEFT JOIN
                    workspace_zulip_bridge.zulip_topic_catalog_identities
                    AS identity
                  ON identity.topic_uuid = topic.uuid
                LEFT JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = topic.zulip_stream_uuid
                LEFT JOIN workspace_zulip_bridge.zulip_realms AS realm
                  ON realm.uuid = stream.realm_uuid
                LEFT JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
                  ON mirror.provider_uuid = realm.workspace_provider_uuid
                 AND mirror.active_generation IS NOT NULL
                LEFT JOIN workspace_zulip_bridge.workspace_topics AS workspace_topic
                  ON workspace_topic.provider_uuid = mirror.provider_uuid
                 AND workspace_topic.snapshot_generation = mirror.active_generation
                 AND workspace_topic.uuid = topic.uuid
                WHERE topic.zulip_stream_uuid = $1
                ORDER BY topic.name, topic.uuid
                """,
                source.stream_uuid,
            )
        if any(row["provider_topic_id"] is None for row in rows):
            raise CatalogReportError(
                "catalog_topic_identity_ambiguous",
                retryable=False,
            )
        return [
            {
                "provider_topic_id": str(row["provider_topic_id"]),
                "name": str(row["name"]),
                "is_default": False,
                **(
                    {"projection_topic_uuid": str(row["uuid"])}
                    if row["preserves_projection"]
                    else {}
                ),
            }
            for row in rows
        ]

    async def _ensure_topic_catalog_identities(
        self,
        connection: asyncpg.Connection | PoolConnectionProxy,
        source: _CatalogSource,
    ) -> None:
        missing = await connection.fetch(
            """
            SELECT topic.uuid, topic.name, topic.is_done, topic.created_at,
                   COALESCE(
                       array_remove(
                           array_agg(alias.alias ORDER BY alias.created_at), NULL
                       ),
                       '{}'::text[]
                   ) AS aliases,
                   COALESCE(
                       array_remove(array_agg(
                           alias.alias ORDER BY alias.created_at
                       ) FILTER (WHERE alias.active), NULL),
                       '{}'::text[]
                   ) AS active_aliases
            FROM workspace_zulip_bridge.zulip_topics AS topic
            LEFT JOIN workspace_zulip_bridge.zulip_topic_aliases AS alias
              ON alias.topic_uuid = topic.uuid
             AND alias.zulip_stream_uuid = topic.zulip_stream_uuid
            LEFT JOIN workspace_zulip_bridge.zulip_topic_catalog_identities
                AS identity
              ON identity.topic_uuid = topic.uuid
            WHERE topic.zulip_stream_uuid = $1 AND identity.uuid IS NULL
            GROUP BY topic.uuid
            ORDER BY topic.created_at, topic.uuid
            """,
            source.stream_uuid,
        )
        if not missing:
            return
        report = await connection.fetchrow(
            """
            SELECT catalog, assignment, reported_at
            FROM workspace_zulip_bridge.workspace_chat_catalog_reports
            WHERE external_account_uuid = $2
              AND zulip_stream_uuid = $1 AND assignment IS NOT NULL
            ORDER BY reported_at DESC NULLS LAST, external_account_uuid
            LIMIT 1
            """,
            source.stream_uuid,
            source.account_uuid,
        )
        assigned_ids: set[str] = set()
        reported_at: datetime.datetime | None = None
        if report is not None:
            raw_assignment = report["assignment"]
            assignment = (
                json.loads(raw_assignment)
                if isinstance(raw_assignment, str)
                else raw_assignment
            )
            if isinstance(assignment, Mapping):
                projection = assignment.get("workspace_projection")
                topics = (
                    projection.get("topics")
                    if isinstance(projection, Mapping)
                    else None
                )
                if isinstance(topics, list):
                    assigned_ids = {
                        str(item["provider_topic_id"])
                        for item in topics
                        if isinstance(item, Mapping)
                        and item.get("provider_topic_id") is not None
                    }
            reported_at = report["reported_at"]
        reserved_ids = {
            str(value)
            for value in await connection.fetch(
                """
                SELECT provider_topic_id
                FROM workspace_zulip_bridge.zulip_topic_catalog_identities
                WHERE zulip_stream_uuid = $1
                """,
                source.stream_uuid,
            )
            for value in (value["provider_topic_id"],)
        }
        for row in missing:
            topic_uuid = UUID(str(row["uuid"]))
            aliases = [str(alias) for alias in row["aliases"]]
            active_aliases = [str(alias) for alias in row["active_aliases"]]
            current_display_name = topic_display_name(
                str(row["name"]),
                bool(row["is_done"]),
            )
            source_names = set(aliases)
            source_names.add(str(row["name"]))
            source_names.add(current_display_name)
            matching_ids = assigned_ids.intersection(
                _legacy_catalog_topic_provider_id(source.chat_key, name)
                for name in source_names
            )
            if len(matching_ids) > 1:
                raise CatalogReportError(
                    "catalog_topic_identity_ambiguous",
                    retryable=False,
                )
            if matching_ids:
                provider_topic_id = matching_ids.pop()
                catalog_topic_key = provider_topic_id.removeprefix(
                    f"{source.chat_key.removeprefix('channel:')}:"
                )
                linked = await connection.fetchval(
                    """
                    UPDATE workspace_zulip_bridge.zulip_topic_catalog_identities
                    SET topic_uuid = $3
                    WHERE zulip_stream_uuid = $1 AND provider_topic_id = $2
                      AND topic_uuid IS NULL
                    RETURNING topic_uuid
                    """,
                    source.stream_uuid,
                    provider_topic_id,
                    topic_uuid,
                )
                if linked is None and provider_topic_id in reserved_ids:
                    raise CatalogReportError(
                        "catalog_topic_identity_ambiguous",
                        retryable=False,
                    )
            else:
                if (
                    assigned_ids
                    and isinstance(reported_at, datetime.datetime)
                    and row["created_at"] <= reported_at
                ):
                    raise CatalogReportError(
                        "catalog_topic_identity_ambiguous",
                        retryable=False,
                    )
                raw_display_name = (
                    active_aliases[0]
                    if len(active_aliases) == 1
                    else current_display_name
                )
                catalog_topic_key = raw_display_name
                legacy_id = _legacy_catalog_topic_provider_id(
                    source.chat_key,
                    raw_display_name,
                )
                provider_topic_id = (
                    _opaque_catalog_topic_provider_id(topic_uuid)
                    if legacy_id in reserved_ids
                    else legacy_id
                )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topic_catalog_identities (
                    topic_uuid, zulip_stream_uuid, catalog_topic_key,
                    provider_topic_id
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
                """,
                topic_uuid,
                source.stream_uuid,
                catalog_topic_key,
                provider_topic_id,
            )
            reserved_ids.add(provider_topic_id)

    async def _queue_catalog(
        self,
        source: _CatalogSource,
        catalog: dict[str, object],
    ) -> int:
        catalog_hash = _canonical_hash(
            {
                "projection_revision": CATALOG_PROJECTION_REVISION,
                "catalog": catalog,
            }
        )
        resource_uuid = stable_external_chat_uuid(source.account_uuid, source.chat_key)
        report_uuid = uuid5(
            self._bridge_uuid,
            (
                f"catalog\0{resource_uuid}\0{source.desired_generation}\0"
                f"{catalog_hash.hex()}"
            ),
        )
        observed_at = _utc_now()
        report = _report(
            report_uuid,
            resource_uuid,
            source.desired_generation,
            observed_at,
            catalog,
        )
        result = await self._pool.execute(
            """
            INSERT INTO workspace_zulip_bridge.workspace_chat_catalog_reports (
                external_account_uuid, zulip_stream_uuid, resource_uuid,
                observed_generation, catalog, catalog_hash, projection_revision,
                report_uuid, report, source_activity_at, source_updated_at,
                processing_status
            ) VALUES (
                $1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9::jsonb, $10, $11,
                'pending'
            )
            ON CONFLICT (external_account_uuid, zulip_stream_uuid) DO UPDATE
            SET resource_uuid = EXCLUDED.resource_uuid,
                observed_generation = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN EXCLUDED.observed_generation
                    ELSE workspace_chat_catalog_reports.observed_generation
                END,
                catalog = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN EXCLUDED.catalog
                    ELSE workspace_chat_catalog_reports.catalog
                END,
                catalog_hash = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN EXCLUDED.catalog_hash
                    ELSE workspace_chat_catalog_reports.catalog_hash
                END,
                projection_revision = EXCLUDED.projection_revision,
                assignment_generation = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN NULL
                    ELSE workspace_chat_catalog_reports.assignment_generation
                END,
                assignment = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN NULL
                    ELSE workspace_chat_catalog_reports.assignment
                END,
                assignment_reconciled = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN false
                    ELSE workspace_chat_catalog_reports.assignment_reconciled
                END,
                assignment_repair_available_at = clock_timestamp(),
                assignment_repair_last_error = NULL,
                assignment_repair_stage = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN 0 ELSE workspace_chat_catalog_reports.assignment_repair_stage END,
                assignment_repair_entity_uuid = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN NULL ELSE workspace_chat_catalog_reports.assignment_repair_entity_uuid END,
                assignment_repair_created_at = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN NULL
                    ELSE workspace_chat_catalog_reports.assignment_repair_created_at
                END,
                assignment_repair_uuid = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN NULL
                    ELSE workspace_chat_catalog_reports.assignment_repair_uuid
                END,
                report_uuid = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN EXCLUDED.report_uuid
                    ELSE workspace_chat_catalog_reports.report_uuid
                END,
                report = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN EXCLUDED.report
                    ELSE workspace_chat_catalog_reports.report
                END,
                source_activity_at = EXCLUDED.source_activity_at,
                source_updated_at = EXCLUDED.source_updated_at,
                processing_status = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN 'pending'
                    ELSE workspace_chat_catalog_reports.processing_status
                END,
                attempt_count = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN 0 ELSE workspace_chat_catalog_reports.attempt_count
                END,
                available_at = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN clock_timestamp()
                    ELSE workspace_chat_catalog_reports.available_at
                END,
                claimed_at = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN NULL ELSE workspace_chat_catalog_reports.claimed_at
                END,
                reported_at = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN NULL
                    ELSE workspace_chat_catalog_reports.reported_at
                END,
                last_error = CASE
                    WHEN workspace_chat_catalog_reports.catalog_hash
                         IS DISTINCT FROM EXCLUDED.catalog_hash
                      OR workspace_chat_catalog_reports.observed_generation
                         IS DISTINCT FROM EXCLUDED.observed_generation
                    THEN NULL ELSE workspace_chat_catalog_reports.last_error
                END,
                updated_at = clock_timestamp()
            RETURNING processing_status
            """,
            source.account_uuid,
            source.stream_uuid,
            resource_uuid,
            source.desired_generation,
            json.dumps(catalog, ensure_ascii=False),
            catalog_hash,
            CATALOG_PROJECTION_REVISION,
            report_uuid,
            json.dumps(report, ensure_ascii=False),
            source.source_activity_at,
            source.source_updated_at,
        )
        return int(result.endswith(" 1"))

    async def _claim_reports(self, limit: int = 20) -> list[asyncpg.Record]:
        return await self._pool.fetch(
            """
            WITH expired AS (
                UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
                SET processing_status = 'failed', claimed_at = NULL,
                    available_at = clock_timestamp(), last_error = 'claim_expired',
                    updated_at = clock_timestamp()
                WHERE processing_status = 'processing'
                  AND claimed_at < clock_timestamp() - interval '5 minutes'
            ), candidate AS (
                SELECT report.external_account_uuid, report.zulip_stream_uuid
                FROM workspace_zulip_bridge.workspace_chat_catalog_reports AS report
                WHERE report.processing_status IN ('pending', 'failed')
                  AND report.available_at <= clock_timestamp()
                  AND EXISTS (
                      SELECT 1
                      FROM workspace_zulip_bridge.zulip_streams AS stream
                      JOIN workspace_zulip_bridge.zulip_connections AS connection
                        ON connection.uuid = stream.source_connection_uuid
                      JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                        ON zulip_user.uuid = connection.zulip_user_uuid
                      WHERE stream.uuid = report.zulip_stream_uuid
                        AND connection.external_account_uuid =
                            report.external_account_uuid
                        AND connection.sync_enabled
                        AND NOT zulip_user.disabled
                  )
                ORDER BY report.source_activity_at DESC,
                         report.source_updated_at DESC,
                         report.available_at, report.updated_at,
                         report.external_account_uuid, report.zulip_stream_uuid
                LIMIT $1 FOR UPDATE SKIP LOCKED
            )
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports AS report
            SET processing_status = 'processing', claimed_at = clock_timestamp(),
                attempt_count = attempt_count + 1, last_error = NULL,
                updated_at = clock_timestamp()
            FROM candidate
            WHERE report.external_account_uuid = candidate.external_account_uuid
              AND report.zulip_stream_uuid = candidate.zulip_stream_uuid
            RETURNING report.report_uuid, report.report
            """,
            limit,
        )

    async def _claim_report(self) -> asyncpg.Record | None:
        reports = await self._claim_reports(limit=1)
        return None if not reports else reports[0]

    def _client(self) -> httpx.AsyncClient:
        ca = self._state / "control-ca.pem"
        certificate = self._state / "bridge.crt"
        key = self._state / "bridge.key"
        for path in (ca, certificate, key):
            if not path.is_file():
                raise CatalogReportError("bridge_identity_not_ready")
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

    async def _send_report(self, report: dict[str, object]) -> str:
        report_uuid = UUID(str(report["report_uuid"]))
        return (await self._send_reports([report]))[report_uuid]

    async def _send_reports(
        self,
        reports: list[dict[str, object]],
    ) -> dict[UUID, str]:
        async with self._control_semaphore:
            async with self._client() as client:
                response = await client.post(
                    "/v1/observed-state/reports",
                    json={"reports": reports},
                )
        if response.status_code == 413 and len(reports) > 1:
            midpoint = len(reports) // 2
            outcomes = await self._send_reports(reports[:midpoint])
            outcomes.update(await self._send_reports(reports[midpoint:]))
            return outcomes
        if response.is_error:
            raise CatalogReportError(
                f"workspace_catalog_http_{response.status_code}",
                retryable=response.status_code == 429 or response.status_code >= 500,
            )
        try:
            payload = response.json()
        except ValueError as error:
            raise CatalogReportError("invalid_catalog_response") from error
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list) or len(results) != len(reports):
            raise CatalogReportError("invalid_catalog_response")
        outcomes = {}
        for report, result in zip(reports, results, strict=True):
            if not isinstance(result, dict) or result.get("report_uuid") != report.get(
                "report_uuid"
            ):
                raise CatalogReportError("invalid_catalog_response")
            outcome = result.get("status")
            if outcome not in {"applied", "duplicate", "stale", "rejected"}:
                raise CatalogReportError("invalid_catalog_response")
            outcomes[UUID(str(report["report_uuid"]))] = str(outcome)
        return outcomes

    async def _finish_report(self, report_uuid: UUID, outcome: str) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
            SET processing_status = CASE
                    WHEN $2 = 'rejected' THEN 'blocked' ELSE 'reported' END,
                claimed_at = NULL, reported_at = CASE
                    WHEN $2 = 'rejected' THEN reported_at
                    ELSE clock_timestamp() END,
                last_error = CASE
                    WHEN $2 = 'rejected' THEN 'catalog_rejected' ELSE NULL END,
                updated_at = clock_timestamp()
            WHERE report_uuid = $1 AND processing_status = 'processing'
            """,
            report_uuid,
            outcome,
        )

    async def _fail_report(
        self,
        report_uuid: UUID,
        reason: str,
        *,
        retryable: bool,
    ) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
            SET processing_status = CASE WHEN $3 THEN 'failed' ELSE 'blocked' END,
                claimed_at = NULL,
                available_at = CASE WHEN $3 THEN
                    clock_timestamp() + make_interval(
                        secs => LEAST(300, power(2, LEAST(attempt_count, 8)))::int
                    )
                    ELSE available_at END,
                last_error = $2, updated_at = clock_timestamp()
            WHERE report_uuid = $1 AND processing_status = 'processing'
            """,
            report_uuid,
            reason[:256],
            retryable,
        )


def _report(
    report_uuid: UUID,
    resource_uuid: UUID,
    generation: int,
    observed_at: str,
    catalog: dict[str, object],
) -> dict[str, object]:
    operation = catalog["operation"]
    return {
        "report_uuid": str(report_uuid),
        "resource_type": "external_chat_catalog",
        "resource_uuid": str(resource_uuid),
        "observed_generation": generation,
        "status": "ready" if operation == "upsert" else "deleted",
        "progress": {
            "phase": "discovery",
            "completed": 1,
            "total": 1,
            "last_progress_at": observed_at,
        },
        "safe_error": None,
        "observed_at": observed_at,
        "catalog": catalog,
    }


def _canonical_hash(value: object) -> bytes:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()


def _object(value: object) -> dict[str, object]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return {str(key): item for key, item in value.items()}


def _utc_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()
