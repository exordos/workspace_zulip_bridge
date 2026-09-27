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

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.stable_ids import stable_external_chat_uuid
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
    source_updated_at: datetime.datetime


class WorkspaceChatCatalogWorker:
    """Keep Workspace control assignments aligned with bridge chat state."""

    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        if settings.workspace_control_url is None:
            raise ValueError("Workspace control must be configured")
        if settings.workspace_bridge_instance_uuid is None:
            raise ValueError("Workspace bridge identity must be configured")
        self._pool = pool
        self._settings = settings
        self._control_url = settings.workspace_control_url.rstrip("/")
        self._bridge_uuid = settings.workspace_bridge_instance_uuid
        self._state = settings.workspace_control_state_dir

    async def run(self) -> None:
        while True:
            try:
                await self.process_once()
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
            # Catalog application can materialize chat bindings and notification
            # state in Workspace. Keep the producer deliberately paced even when
            # more reports are ready so a large file backlog cannot overwhelm the
            # control service's database pool.
            await asyncio.sleep(self._settings.workspace_control_poll_seconds)

    async def process_once(self) -> int:
        refreshed = await self._refresh_catalogs()
        retired = await self._retire_unneeded_reports()
        report = await self._claim_report()
        if report is None:
            return refreshed + retired
        report_uuid = UUID(str(report["report_uuid"]))
        try:
            outcome = await self._send_report(_object(report["report"]))
        except asyncio.CancelledError:
            raise
        except CatalogReportError as error:
            await self._fail_report(
                report_uuid,
                error.code,
                retryable=error.retryable,
            )
        except (TimeoutError, httpx.HTTPError) as error:
            await self._fail_report(
                report_uuid,
                type(error).__name__,
                retryable=True,
            )
        else:
            await self._finish_report(report_uuid, outcome)
        return refreshed + retired + 1

    async def _refresh_catalogs(self) -> int:
        changed = await self._reactivate_needed_reports()
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
              AND EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.workspace_file_projections
                      AS file_projection
                  WHERE file_projection.zulip_stream_uuid = stream.uuid
                    AND file_projection.processing_status IN (
                        'pending', 'processing', 'failed'
                    )
                    AND file_projection.last_error = 'workspace_file_http_403'
              )
              AND (
                  report.external_account_uuid IS NULL
                  OR report.observed_generation <> connection.desired_generation
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
            ORDER BY connection.external_account_uuid, stream.uuid
            LIMIT 20
            """
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
              AND report.last_error = 'catalog_not_required_for_file_transfer'
              AND EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.workspace_file_projections
                      AS file_projection
                  JOIN workspace_zulip_bridge.zulip_streams AS stream
                    ON stream.uuid = file_projection.zulip_stream_uuid
                  JOIN workspace_zulip_bridge.zulip_connections AS connection
                    ON connection.uuid = stream.source_connection_uuid
                  WHERE file_projection.zulip_stream_uuid = report.zulip_stream_uuid
                    AND connection.external_account_uuid =
                        report.external_account_uuid
                    AND file_projection.processing_status IN (
                        'pending', 'processing', 'failed'
                    )
                    AND file_projection.last_error = 'workspace_file_http_403'
              )
            """
        )
        return int(result.rsplit(" ", 1)[-1])

    async def _retire_unneeded_reports(self) -> int:
        """Stop queued reports that cannot authorize an active file transfer."""

        result = await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports AS report
            SET processing_status = 'blocked', claimed_at = NULL,
                last_error = 'catalog_not_required_for_file_transfer',
                updated_at = clock_timestamp()
            WHERE report.processing_status IN ('pending', 'processing', 'failed')
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.workspace_file_projections
                      AS file_projection
                  JOIN workspace_zulip_bridge.zulip_streams AS stream
                    ON stream.uuid = file_projection.zulip_stream_uuid
                  JOIN workspace_zulip_bridge.zulip_connections AS connection
                    ON connection.uuid = stream.source_connection_uuid
                  WHERE file_projection.zulip_stream_uuid = report.zulip_stream_uuid
                    AND connection.external_account_uuid =
                        report.external_account_uuid
                    AND file_projection.processing_status IN (
                        'pending', 'processing', 'failed'
                    )
                    AND file_projection.last_error = 'workspace_file_http_403'
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
            return [
                {
                    "provider_topic_id": f"{source.chat_key}:default",
                    "name": "Zulip",
                    "is_default": True,
                }
            ]
        rows = await self._pool.fetch(
            """
            SELECT name
            FROM workspace_zulip_bridge.zulip_topics
            WHERE zulip_stream_uuid = $1
            ORDER BY name, uuid
            """,
            source.stream_uuid,
        )
        provider_stream_id = source.chat_key.removeprefix("channel:")
        return [
            {
                "provider_topic_id": f"{provider_stream_id}:{row['name']}",
                "name": str(row["name"]),
                "is_default": False,
            }
            for row in rows
        ]

    async def _queue_catalog(
        self,
        source: _CatalogSource,
        catalog: dict[str, object],
    ) -> int:
        catalog_hash = _canonical_hash(catalog)
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
                observed_generation, catalog, catalog_hash, report_uuid, report,
                source_updated_at, processing_status
            ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8::jsonb, $9, 'pending')
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
                    THEN NULL ELSE workspace_chat_catalog_reports.reported_at
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
            report_uuid,
            json.dumps(report, ensure_ascii=False),
            source.source_updated_at,
        )
        return int(result.endswith(" 1"))

    async def _claim_report(self) -> asyncpg.Record | None:
        return await self._pool.fetchrow(
            """
            WITH expired AS (
                UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports
                SET processing_status = 'failed', claimed_at = NULL,
                    available_at = clock_timestamp(), last_error = 'claim_expired',
                    updated_at = clock_timestamp()
                WHERE processing_status = 'processing'
                  AND claimed_at < clock_timestamp() - interval '5 minutes'
            ), candidate AS (
                SELECT external_account_uuid, zulip_stream_uuid
                FROM workspace_zulip_bridge.workspace_chat_catalog_reports
                WHERE processing_status IN ('pending', 'failed')
                  AND available_at <= clock_timestamp()
                ORDER BY available_at, updated_at,
                         external_account_uuid, zulip_stream_uuid
                LIMIT 1 FOR UPDATE SKIP LOCKED
            )
            UPDATE workspace_zulip_bridge.workspace_chat_catalog_reports AS report
            SET processing_status = 'processing', claimed_at = clock_timestamp(),
                attempt_count = attempt_count + 1, last_error = NULL,
                updated_at = clock_timestamp()
            FROM candidate
            WHERE report.external_account_uuid = candidate.external_account_uuid
              AND report.zulip_stream_uuid = candidate.zulip_stream_uuid
            RETURNING report.report_uuid, report.report
            """
        )

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
        async with self._client() as client:
            response = await client.post(
                "/v1/observed-state/reports",
                json={"reports": [report]},
            )
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
        if not isinstance(results, list) or len(results) != 1:
            raise CatalogReportError("invalid_catalog_response")
        result = results[0]
        if not isinstance(result, dict) or result.get("report_uuid") != report.get(
            "report_uuid"
        ):
            raise CatalogReportError("invalid_catalog_response")
        outcome = result.get("status")
        if outcome not in {"applied", "duplicate", "stale", "rejected"}:
            raise CatalogReportError("invalid_catalog_response")
        return str(outcome)

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
