# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Bootstrap and converge the local Zulip and Workspace entity graphs."""

import asyncio
import hashlib
import json
import logging
import time
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
from asyncpg.pool import PoolConnectionProxy

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.stable_ids import stable_topic_binding_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.workspace_auth import WorkspaceTokenManager
from workspace_zulip_bridge.workspace_entities import validate_entity
from workspace_zulip_bridge.zulip_api import ZulipApiError
from workspace_zulip_bridge.zulip_outbound import ZulipOutboundError
from workspace_zulip_bridge.zulip_outbound import ZulipOutboundPending
from workspace_zulip_bridge.zulip_outbound import ZulipOutboundWriter

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
RECONCILIATION_VERSION = 3


class ProviderApiError(RuntimeError):
    """Safe, structured Provider API failure details."""

    def __init__(
        self,
        status_code: int,
        error_code: str,
        item_index: int | None,
    ) -> None:
        self.status_code = status_code
        self.error_code = error_code
        self.item_index = item_index
        suffix = "" if item_index is None else f" item_index={item_index}"
        super().__init__(
            f"Workspace Provider API returned {status_code} error={error_code}{suffix}"
        )


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


def _entity_dependencies(
    entity_type: str,
    data: Mapping[str, Any],
) -> tuple[tuple[str, UUID], ...]:
    fields = {
        "streams": (("users", "owner_uuid"), ("users", "direct_user_uuid")),
        "stream_bindings": (
            ("streams", "stream_uuid"),
            ("users", "user_uuid"),
            ("users", "who_uuid"),
        ),
        "topics": (("streams", "stream_uuid"),),
        "topic_bindings": (
            ("streams", "stream_uuid"),
            ("topics", "topic_uuid"),
            ("users", "user_uuid"),
        ),
        "messages": (
            ("streams", "stream_uuid"),
            ("topics", "topic_uuid"),
            ("users", "author_uuid"),
        ),
        "message_flags": (
            ("streams", "stream_uuid"),
            ("messages", "message_uuid"),
            ("users", "user_uuid"),
        ),
        "message_reactions": (
            ("messages", "message_uuid"),
            ("users", "user_uuid"),
        ),
    }.get(entity_type, ())
    dependencies = []
    for dependency_type, field in fields:
        value = data.get(field)
        if value is not None:
            dependencies.append((dependency_type, UUID(str(value))))
    return tuple(dependencies)


def _provider_api_error(response: httpx.Response) -> ProviderApiError:
    error_code = "unknown"
    item_index = None
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        candidate = payload.get("error")
        if (
            isinstance(candidate, str)
            and 0 < len(candidate) <= 128
            and all(
                character.isascii() and (character.isalnum() or character in "._-")
                for character in candidate
            )
        ):
            error_code = candidate
        candidate_index = payload.get("item_index")
        if (
            isinstance(candidate_index, int)
            and not isinstance(candidate_index, bool)
            and 0 <= candidate_index <= 1_000_000
        ):
            item_index = candidate_index
    return ProviderApiError(response.status_code, error_code, item_index)


def _bootstrap_error(error: BaseException) -> RuntimeError:
    if isinstance(error, RuntimeError) and str(error).startswith(
        "Workspace Provider API returned "
    ):
        return error
    return RuntimeError(f"Workspace bootstrap failed error={type(error).__name__}")


def workspace_api_url(settings: Settings) -> str:
    if settings.workspace_api_url is not None:
        configured = settings.workspace_api_url.rstrip("/")
        # Keep accepting the original documented value while routing provider
        # operations to the messenger service that owns this private API.
        if configured.endswith("/api/workspace/v1"):
            return f"{configured}/messenger"
        return configured
    assert settings.workspace_websocket_url is not None
    parsed = urlsplit(settings.workspace_websocket_url)
    scheme = "https" if parsed.scheme == "wss" else "http"
    suffix = "/events/ws"
    path = parsed.path
    if not path.endswith(suffix):
        raise ValueError("Workspace websocket URL must end with /events/ws")
    api_root = urlunsplit((scheme, parsed.netloc, path[: -len(suffix)], "", "")).rstrip(
        "/"
    )
    return f"{api_root}/messenger"


def workspace_directory_url(settings: Settings) -> str:
    root = workspace_api_url(settings).rstrip("/")
    if root.endswith("/messenger"):
        root = root[: -len("/messenger")]
    return f"{root}/users/"


_IDENTITY_FIELDS = {
    "stream_bindings": ("stream_uuid", "user_uuid"),
    "topic_bindings": ("stream_uuid", "topic_uuid", "user_uuid"),
    "messages": ("author_uuid",),
    "message_flags": ("stream_uuid", "message_uuid", "user_uuid"),
    "message_reactions": ("message_uuid", "user_uuid"),
}


def identity_rebind_required(
    entity_type: str,
    source: Mapping[str, Any],
    target: Mapping[str, Any] | None,
) -> bool:
    """Request the explicit Provider identity migration only when needed."""
    if target is None:
        return False
    return any(
        str(source.get(field)) != str(target.get(field))
        for field in _IDENTITY_FIELDS.get(entity_type, ())
    )


def _reaction_identity(data: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(data["message_uuid"]),
        str(data["user_uuid"]),
        str(data["emoji_name"]),
    )


class WorkspaceBootstrapper:
    def __init__(
        self,
        pool: asyncpg.Pool,
        settings: Settings,
        tokens: WorkspaceTokenManager | None = None,
    ) -> None:
        assert settings.workspace_provider_uuid is not None
        assert settings.workspace_project_id is not None
        assert settings.workspace_token_file is not None
        self._pool = pool
        self._settings = settings
        self._provider_uuid = settings.workspace_provider_uuid
        self._project_uuid = settings.workspace_project_id
        self._tokens = tokens or WorkspaceTokenManager(settings)
        self._next_identity_sync_at = 0.0
        self._registered = False

    async def ensure(self) -> bool:
        if not self._registered:
            await self._register_provider()
            self._registered = True
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
            if time.monotonic() >= self._next_identity_sync_at:
                await self._reconcile_workspace_identities(
                    UUID(str(row["active_generation"])),
                    schedule_changes=True,
                )
            return False
        await self.bootstrap()
        return True

    async def _register_provider(self) -> None:
        verify: bool | str = (
            True
            if self._settings.workspace_ca_file is None
            else str(self._settings.workspace_ca_file)
        )
        async with httpx.AsyncClient(
            verify=verify,
            timeout=httpx.Timeout(self._settings.workspace_request_timeout_seconds),
        ) as client:
            client.headers["Authorization"] = (
                f"Bearer {await self._tokens.access_token()}"
            )
            response = await client.put(
                f"{workspace_api_url(self._settings)}/provider/registration",
                json={"provider_uuid": str(self._provider_uuid), "name": "zulip"},
            )
            if response.status_code == 401:
                client.headers["Authorization"] = (
                    f"Bearer {await self._tokens.access_token(force_refresh=True)}"
                )
                response = await client.put(
                    f"{workspace_api_url(self._settings)}/provider/registration",
                    json={
                        "provider_uuid": str(self._provider_uuid),
                        "name": "zulip",
                    },
                )
        if response.is_error:
            raise _provider_api_error(response)
        registration = _json_object(response.json())
        if (
            UUID(str(registration["provider_uuid"])) != self._provider_uuid
            or UUID(str(registration["project_id"])) != self._project_uuid
            or registration.get("name") != "zulip"
            or registration.get("enabled") is not True
        ):
            raise ValueError("invalid Workspace provider registration")

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
        await self._discard_inactive_generations()
        try:
            await self._load_snapshot(client)
        except BaseException as exc:
            safe_error = _bootstrap_error(exc)
            await self._pool.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_mirror_state
                SET bootstrap_status = 'failed', last_error = $2,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1
                """,
                self._provider_uuid,
                str(safe_error),
            )
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise safe_error from None

    async def _discard_inactive_generations(self) -> None:
        """Drop partial snapshots left by an interrupted bootstrap."""
        state = await self._pool.fetchrow(
            """
            SELECT active_generation
            FROM workspace_zulip_bridge.workspace_mirror_state
            WHERE provider_uuid = $1
            """,
            self._provider_uuid,
        )
        if state is None:
            return
        active_generation = state["active_generation"]
        deleted: dict[str, int] = {}
        async with self._pool.acquire() as connection, connection.transaction():
            for entity_type in ENTITY_TYPES:
                if active_generation is None:
                    result = await connection.execute(
                        f"DELETE FROM workspace_zulip_bridge.workspace_{entity_type} "
                        "WHERE provider_uuid = $1",
                        self._provider_uuid,
                    )
                else:
                    result = await connection.execute(
                        f"DELETE FROM workspace_zulip_bridge.workspace_{entity_type} "
                        "WHERE provider_uuid = $1 AND snapshot_generation <> $2",
                        self._provider_uuid,
                        active_generation,
                    )
                count = int(result.rsplit(" ", 1)[-1])
                if count:
                    deleted[entity_type] = count
        if deleted:
            LOG.info(
                "Workspace bootstrap discarded incomplete generations: counts=%s",
                deleted,
            )

    async def _load_snapshot(self, client: httpx.AsyncClient | None) -> None:
        if client is None:
            verify: bool | str = (
                True
                if self._settings.workspace_ca_file is None
                else str(self._settings.workspace_ca_file)
            )
            async with httpx.AsyncClient(
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
        response = await self._get(
            client,
            f"{workspace_api_url(self._settings)}/provider/bootstrap",
            params={"mode": "paged"},
        )
        if response.is_error:
            raise _provider_api_error(response)
        meta = _json_object(response.json())
        if meta.get("record") != "manifest" or meta.get("schema_version") != 2:
            raise ValueError("invalid Workspace bootstrap manifest")
        generation = UUID(str(meta["snapshot_uuid"]))
        for entity_type in ENTITY_TYPES:
            after_uuid = UUID(int=0)
            while True:
                response = await self._get(
                    client,
                    f"{workspace_api_url(self._settings)}/provider/entities/"
                    f"{entity_type}",
                    params={
                        "limit": "500",
                        "snapshot_after_uuid": str(after_uuid),
                    },
                )
                if response.is_error:
                    raise _provider_api_error(response)
                page = _json_object(response.json())
                items = page.get("items")
                if not isinstance(items, list):
                    raise ValueError("invalid Workspace bootstrap page")
                for item in items:
                    record = _json_object(item)
                    if record.get("type") != entity_type:
                        raise ValueError("Workspace bootstrap entity type mismatch")
                    data = _json_object(record["data"])
                    validate_entity(entity_type, data)
                    digest.update(
                        (
                            json.dumps(
                                {"record": "entity", **record},
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                            + "\n"
                        ).encode("utf-8")
                    )
                    counts[entity_type] += 1
                    buffers[entity_type].append(
                        (
                            self._provider_uuid,
                            generation,
                            UUID(str(record["uuid"])),
                            self._project_uuid,
                            bytes.fromhex(str(record["content_hash"])),
                            _timestamp(str(record["source_updated_at"])),
                            json.dumps(data, separators=(",", ":")),
                        )
                    )
                    if len(buffers[entity_type]) >= 5000:
                        await self._copy(entity_type, buffers[entity_type])
                        buffers[entity_type].clear()
                        LOG.info(
                            "Workspace bootstrap progress: entity_type=%s count=%d",
                            entity_type,
                            counts[entity_type],
                        )
                next_cursor = page.get("next_cursor")
                if next_cursor is None:
                    break
                cursor = _json_object(next_cursor)
                next_after_uuid = UUID(str(cursor["after_uuid"]))
                if next_after_uuid <= after_uuid:
                    raise ValueError("Workspace bootstrap cursor did not advance")
                after_uuid = next_after_uuid
            LOG.info(
                "Workspace bootstrap entity loaded: entity_type=%s count=%d",
                entity_type,
                counts[entity_type],
            )
        for entity_type, records in buffers.items():
            await self._copy(entity_type, records)
        identity_count = await self._reconcile_workspace_identities(
            generation,
            client=client,
            schedule_changes=False,
        )
        counts["users"] += identity_count
        epoch_generation = UUID(str(meta["epoch_generation"]))
        epoch_version = int(meta["snapshot_epoch_version"])
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                DELETE FROM workspace_zulip_bridge.workspace_events
                WHERE provider_uuid = $1
                  AND (
                      epoch_generation IS DISTINCT FROM $2
                      OR epoch_version <= $3
                  )
                """,
                self._provider_uuid,
                epoch_generation,
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

    async def _reconcile_workspace_identities(
        self,
        generation: UUID,
        *,
        client: httpx.AsyncClient | None = None,
        schedule_changes: bool,
    ) -> int:
        if client is None:
            verify: bool | str = (
                True
                if self._settings.workspace_ca_file is None
                else str(self._settings.workspace_ca_file)
            )
            async with httpx.AsyncClient(
                verify=verify,
                timeout=httpx.Timeout(self._settings.workspace_request_timeout_seconds),
            ) as owned_client:
                return await self._reconcile_workspace_identities(
                    generation,
                    client=owned_client,
                    schedule_changes=schedule_changes,
                )
        response = await self._get(
            client,
            workspace_directory_url(self._settings),
            params={},
        )
        if response.is_error:
            raise _provider_api_error(response)
        raw_users = response.json()
        if not isinstance(raw_users, list):
            raise ValueError("invalid Workspace user directory")
        iam_by_email: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for raw_user in raw_users:
            user = _json_object(raw_user)
            email = str(user.get("email") or "").strip().casefold()
            if user.get("source") == "iam" and email:
                iam_by_email[email].append(user)
        canonical_users = {
            email: users[0] for email, users in iam_by_email.items() if len(users) == 1
        }
        local_users = await self._pool.fetch(
            """
            SELECT zulip_user.uuid, zulip_user.login,
                   zulip_user.workspace_user_uuid,
                   active_connection.owner_workspace_user_uuid
            FROM workspace_zulip_bridge.zulip_users AS zulip_user
            JOIN workspace_zulip_bridge.zulip_realms AS realm
              ON realm.uuid = zulip_user.realm_uuid
            JOIN LATERAL (
                SELECT connection.owner_workspace_user_uuid
                FROM workspace_zulip_bridge.zulip_connections AS connection
                WHERE connection.zulip_user_uuid = zulip_user.uuid
                  AND connection.sync_enabled
                  AND lower(btrim(connection.login)) =
                      lower(btrim(zulip_user.login))
                ORDER BY
                    (connection.owner_workspace_user_uuid IS NOT NULL) DESC,
                    connection.uuid
                LIMIT 1
            ) AS active_connection ON TRUE
            WHERE NOT zulip_user.is_bot
              AND (
                    realm.workspace_provider_uuid = $1
                    OR (
                        realm.workspace_provider_uuid IS NULL
                        AND realm.workspace_project_id = $2
                    )
              )
            """,
            self._provider_uuid,
            self._project_uuid,
        )
        desired: dict[UUID, UUID] = {}
        for row in local_users:
            user_uuid = UUID(str(row["uuid"]))
            if row["owner_workspace_user_uuid"] is not None:
                # An external account is an explicit ownership link. Its Zulip
                # login is allowed to differ from the IAM email, so the
                # directory heuristic must never erase this mapping.
                desired[user_uuid] = UUID(str(row["owner_workspace_user_uuid"]))
                continue
            email = str(row["login"]).strip().casefold()
            if email in canonical_users:
                desired[user_uuid] = UUID(str(canonical_users[email]["uuid"]))
        current = {
            UUID(str(row["uuid"])): (
                None
                if row["workspace_user_uuid"] is None
                else UUID(str(row["workspace_user_uuid"]))
            )
            for row in local_users
        }
        changed = {
            user_uuid
            for user_uuid in current.keys() | desired.keys()
            if current.get(user_uuid) != desired.get(user_uuid)
        }
        canonical_users_by_uuid = {
            UUID(str(value["uuid"])): value for value in canonical_users.values()
        }
        directory_rows = []
        for workspace_user_uuid in sorted(set(desired.values()), key=str):
            canonical_user = canonical_users_by_uuid.get(workspace_user_uuid)
            # The explicit external-account owner remains authoritative even
            # when the IAM directory no longer returns that user.  Keep the
            # Zulip-to-Workspace ownership link, but there is no profile row
            # available to mirror into workspace_users.
            if canonical_user is None:
                continue
            data = {
                "username": canonical_user["username"],
                "display_name": canonical_user.get("display_name")
                or canonical_user["username"],
                "email": canonical_user.get("email"),
                "avatar": canonical_user.get("avatar"),
                "status": canonical_user.get("status", "offline"),
                "last_ping_at": canonical_user.get("last_ping_at"),
                "status_emoji": canonical_user.get("status_emoji"),
                "status_text": canonical_user.get("status_text"),
                "disabled": False,
                "is_bot": False,
                "created_at": canonical_user["created_at"],
            }
            directory_rows.append(
                (
                    self._provider_uuid,
                    generation,
                    workspace_user_uuid,
                    self._project_uuid,
                    canonical_hash(data),
                    _timestamp(str(canonical_user["updated_at"])),
                    json.dumps(data, separators=(",", ":")),
                )
            )
        async with self._pool.acquire() as connection, connection.transaction():
            if local_users:
                await connection.executemany(
                    """
                    UPDATE workspace_zulip_bridge.zulip_users
                    SET workspace_user_uuid = $2
                    WHERE uuid = $1 AND workspace_user_uuid IS DISTINCT FROM $2
                    """,
                    [
                        (UUID(str(row["uuid"])), desired.get(UUID(str(row["uuid"]))))
                        for row in local_users
                    ],
                )
            if directory_rows:
                await connection.executemany(
                    """
                    INSERT INTO workspace_zulip_bridge.workspace_users (
                        provider_uuid, snapshot_generation, uuid,
                        workspace_project_id, content_hash,
                        source_updated_at, data
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                    ON CONFLICT (provider_uuid, snapshot_generation, uuid)
                    DO UPDATE SET content_hash = EXCLUDED.content_hash,
                        source_updated_at = EXCLUDED.source_updated_at,
                        data = EXCLUDED.data, updated_at = clock_timestamp()
                    """,
                    directory_rows,
                )
            if changed and schedule_changes:
                changed_values = list(changed)
                await connection.execute(
                    """
                    UPDATE workspace_zulip_bridge.zulip_streams AS stream
                    SET updated_at = clock_timestamp()
                    WHERE stream.owner_user_uuid = ANY($1::uuid[])
                       OR stream.direct_user_uuid = ANY($1::uuid[])
                       OR EXISTS (
                            SELECT 1
                            FROM workspace_zulip_bridge.zulip_connections AS source
                            WHERE source.uuid = stream.source_connection_uuid
                              AND source.zulip_user_uuid = ANY($1::uuid[])
                       )
                    """,
                    changed_values,
                )
                for table in (
                    "zulip_stream_bindings",
                    "zulip_topic_bindings",
                    "zulip_message_flags",
                    "zulip_message_reactions",
                ):
                    await connection.execute(
                        f"UPDATE workspace_zulip_bridge.{table} "
                        "SET updated_at = clock_timestamp() "
                        "WHERE zulip_user_uuid = ANY($1::uuid[])",
                        changed_values,
                    )
                await connection.execute(
                    """
                    UPDATE workspace_zulip_bridge.zulip_messages
                    SET source_updated_at = clock_timestamp()
                    WHERE sender_user_uuid = ANY($1::uuid[])
                    """,
                    changed_values,
                )
                await connection.executemany(
                    """
                    INSERT INTO workspace_zulip_bridge.sync_diffs (
                        provider_uuid, entity_type, entity_uuid, realm_uuid,
                        direction, source_updated_at
                    )
                    SELECT $1, 'users', zulip_user.uuid, zulip_user.realm_uuid,
                           'to_workspace', clock_timestamp()
                    FROM workspace_zulip_bridge.zulip_users AS zulip_user
                    WHERE zulip_user.uuid = $2
                      AND zulip_user.workspace_user_uuid IS NOT NULL
                    ON CONFLICT (provider_uuid, entity_type, entity_uuid)
                    DO UPDATE SET direction = 'to_workspace',
                        processing_status = 'pending', source_hash = NULL,
                        source_updated_at = EXCLUDED.source_updated_at,
                        available_at = clock_timestamp(), last_error = NULL,
                        updated_at = clock_timestamp()
                    """,
                    [(self._provider_uuid, user_uuid) for user_uuid in changed],
                )
        self._next_identity_sync_at = time.monotonic() + 300.0
        if changed:
            LOG.info(
                "Workspace identity links reconciled: linked=%d changed=%d",
                len(desired),
                len(changed),
            )
        return len(directory_rows)

    async def _get(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: dict[str, str],
    ) -> httpx.Response:
        client.headers["Authorization"] = f"Bearer {await self._tokens.access_token()}"
        response = await client.get(url, params=params)
        if response.status_code != 401:
            return response
        client.headers["Authorization"] = (
            f"Bearer {await self._tokens.access_token(force_refresh=True)}"
        )
        return await client.get(url, params=params)

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
        self._next_cleanup_at = 0.0

    async def run(self) -> None:
        while True:
            deleted = await self._maybe_cleanup_expired_events()
            changed = await self.process_once()
            if not changed and deleted != self._settings.event_cleanup_batch_size:
                await asyncio.sleep(self._settings.workspace_sync_poll_seconds)

    async def cleanup_expired_events(self) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            deleted = await connection.fetchval(
                """
                WITH expired AS MATERIALIZED (
                    SELECT event.sequence
                    FROM workspace_zulip_bridge.workspace_events AS event
                    WHERE event.provider_uuid = $1
                      AND event.processing_status IN (
                          'applied', 'skipped', 'failed'
                      )
                      AND event.received_at < (
                          clock_timestamp()
                          - make_interval(secs => $2::double precision)
                      )
                    ORDER BY event.received_at, event.sequence
                    LIMIT $3
                    FOR UPDATE SKIP LOCKED
                ), deleted AS (
                    DELETE FROM workspace_zulip_bridge.workspace_events AS event
                    USING expired
                    WHERE event.sequence = expired.sequence
                    RETURNING 1
                )
                SELECT count(*)::bigint FROM deleted
                """,
                self._provider_uuid,
                self._settings.event_retention_seconds,
                self._settings.event_cleanup_batch_size,
            )
        return int(deleted)

    async def _maybe_cleanup_expired_events(self) -> int:
        if time.monotonic() < self._next_cleanup_at:
            return 0
        deleted = await self.cleanup_expired_events()
        if deleted == self._settings.event_cleanup_batch_size:
            self._next_cleanup_at = 0.0
        else:
            self._next_cleanup_at = (
                time.monotonic() + self._settings.event_cleanup_interval_seconds
            )
        if deleted:
            LOG.info(
                "Expired terminal Workspace events deleted "
                "count=%s retention_seconds=%.3f",
                deleted,
                self._settings.event_retention_seconds,
            )
        return deleted

    async def process_once(self) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_events
                SET processing_status = 'pending', claimed_at = NULL,
                    available_at = clock_timestamp(), processed_at = NULL,
                    last_error = 'claim_expired'
                WHERE provider_uuid = $1
                  AND processing_status = 'processing'
                  AND claimed_at < (
                      clock_timestamp()
                      - make_interval(secs => $2::double precision)
                  )
                """,
                self._provider_uuid,
                self._settings.event_processor_claim_timeout_seconds,
            )
            rows = await connection.fetch(
                """
                WITH claim AS (
                    SELECT sequence
                    FROM workspace_zulip_bridge.workspace_events
                    WHERE provider_uuid = $1 AND processing_status = 'pending'
                      AND available_at <= clock_timestamp()
                    ORDER BY CASE object_type
                        WHEN 'user' THEN 0
                        WHEN 'stream' THEN 1
                        WHEN 'stream_binding' THEN 2
                        WHEN 'topic' THEN 3
                        WHEN 'topic_binding' THEN 4
                        WHEN 'message' THEN 5
                        WHEN 'message_flag' THEN 6
                        WHEN 'message_reaction' THEN 7
                        ELSE 8
                    END,
                    sequence
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
        rows = sorted(rows, key=lambda row: (row["epoch_version"], row["sequence"]))
        for row in rows:
            try:
                applied = await self._apply(row)
                status = "applied" if applied else "skipped"
                error = None
            except Exception as exc:
                error_type = type(exc).__name__
                LOG.error(
                    "Workspace mirror event failed error_type=%s",
                    error_type,
                )
                status = "retry"
                error = f"workspace_event_error:{error_type}"
            await self._pool.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_events
                SET processing_status = CASE
                        WHEN $2 = 'retry' AND attempt_count < $4 THEN 'pending'
                        WHEN $2 = 'retry' THEN 'failed'
                        ELSE $2
                    END,
                    claimed_at = NULL,
                    available_at = CASE
                        WHEN $2 = 'retry' AND attempt_count < $4
                        THEN clock_timestamp() + make_interval(
                            secs => LEAST(
                                $5 * power(2, LEAST(attempt_count - 1, 16)),
                                $6
                            )
                        )
                        ELSE available_at
                    END,
                    processed_at = CASE
                        WHEN $2 = 'retry' AND attempt_count < $4
                        THEN NULL
                        ELSE clock_timestamp()
                    END,
                    last_error = CASE
                        WHEN $2 = 'retry' AND attempt_count >= $4
                        THEN 'retry_exhausted:' || COALESCE($3, '')
                        ELSE $3
                    END,
                    updated_at = clock_timestamp()
                WHERE sequence = $1
                """,
                row["sequence"],
                status,
                error,
                self._settings.workspace_event_max_attempts,
                self._settings.workspace_retry_base_seconds,
                self._settings.workspace_retry_cap_seconds,
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
            if await self._newer_entity_event_applied(row, entity_uuid):
                continue
            data = {key: item for key, item in value.items() if key != "kind"}
            validate_entity(entity_type, data)
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
            partition_key = entity_uuid if entity_type == "streams" else None
            if entity_type in {
                "stream_bindings",
                "topics",
                "topic_bindings",
                "messages",
                "message_flags",
            }:
                raw_stream_uuid = data.get("stream_uuid")
                if raw_stream_uuid is not None:
                    partition_key = UUID(str(raw_stream_uuid))
            if partition_key is None and entity_type != "users":
                partition_key = await self._source_stream_uuid(
                    entity_type,
                    entity_uuid,
                    data,
                )
            await self._upsert_diff(
                entity_type,
                entity_uuid,
                partition_key,
                target_hash,
                source_updated_at,
            )
            applied = True
        return applied

    async def _newer_entity_event_applied(
        self,
        row: asyncpg.Record,
        entity_uuid: UUID,
    ) -> bool:
        """Ignore an older retry after a newer event became canonical."""
        return bool(
            await self._pool.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM workspace_zulip_bridge.workspace_events AS newer
                    WHERE newer.provider_uuid = $1
                      AND newer.epoch_generation IS NOT DISTINCT FROM $2
                      AND newer.epoch_version > $3
                      AND (
                          newer.entity_uuid = $4
                          OR EXISTS (
                              SELECT 1
                              FROM jsonb_array_elements(
                                  CASE
                                      WHEN jsonb_typeof(
                                          newer.payload -> 'payload' -> 'items'
                                      ) = 'array'
                                      THEN newer.payload -> 'payload' -> 'items'
                                      ELSE '[]'::jsonb
                                  END
                              ) AS item
                              WHERE item ->> 'uuid' = $4::text
                          )
                      )
                      AND newer.processing_status = 'applied'
                )
                """,
                self._provider_uuid,
                row["epoch_generation"],
                row["epoch_version"],
                entity_uuid,
            )
        )

    async def _source_stream_uuid(
        self,
        entity_type: str,
        entity_uuid: UUID,
        data: Mapping[str, Any],
    ) -> UUID | None:
        if entity_type == "message_reactions":
            raw_message_uuid = data.get("message_uuid")
            if raw_message_uuid is None:
                return None
            value = await self._pool.fetchval(
                """
                SELECT COALESCE(
                    (
                        SELECT (parent.data ->> 'stream_uuid')::uuid
                        FROM workspace_zulip_bridge.workspace_mirror_state AS mirror
                        JOIN workspace_zulip_bridge.workspace_messages AS parent
                          ON parent.provider_uuid = mirror.provider_uuid
                         AND parent.snapshot_generation = mirror.active_generation
                        WHERE mirror.provider_uuid = $1 AND parent.uuid = $2
                    ),
                    (
                        SELECT message.zulip_stream_uuid
                        FROM workspace_zulip_bridge.zulip_messages AS message
                        WHERE message.uuid = $2
                    )
                )
                """,
                self._provider_uuid,
                UUID(str(raw_message_uuid)),
            )
            return None if value is None else UUID(str(value))
        source = {
            "stream_bindings": ("zulip_stream_bindings", "zulip_stream_uuid"),
            "topics": ("zulip_topics", "zulip_stream_uuid"),
            "topic_bindings": ("zulip_topic_bindings", "zulip_stream_uuid"),
            "messages": ("zulip_messages", "zulip_stream_uuid"),
            "message_flags": ("zulip_message_flags", "zulip_stream_uuid"),
        }.get(entity_type)
        if source is None:
            return None
        table, column = source
        value = await self._pool.fetchval(
            f"SELECT {column} FROM workspace_zulip_bridge.{table} WHERE uuid = $1",
            entity_uuid,
        )
        return None if value is None else UUID(str(value))

    async def _upsert_diff(
        self,
        entity_type: str,
        entity_uuid: UUID,
        partition_key: UUID | None,
        target_hash: bytes | None,
        target_updated_at: datetime,
    ) -> bool:
        updated = await self._pool.fetchval(
            """
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                partition_key, direction, source_hash, target_hash,
                source_updated_at, target_updated_at, delivery_priority
            )
            SELECT $1, $2, $3, realm.uuid, $4, 'to_zulip', NULL, $5, $6, $6, 0
            FROM workspace_zulip_bridge.zulip_realms AS realm
            WHERE realm.workspace_provider_uuid = $1
              AND (
                  $2 = 'users'
                  OR EXISTS (
                      SELECT 1
                      FROM workspace_zulip_bridge.zulip_streams AS mapped_stream
                      WHERE mapped_stream.realm_uuid = realm.uuid
                        AND mapped_stream.uuid = $4
                  )
              )
            ON CONFLICT (provider_uuid, entity_type, entity_uuid)
            DO UPDATE SET direction = CASE
                    WHEN EXCLUDED.target_updated_at > sync_diffs.source_updated_at
                    THEN 'to_zulip'
                    ELSE 'to_workspace'
                END,
                partition_key = COALESCE(
                    EXCLUDED.partition_key, sync_diffs.partition_key
                ),
                delivery_priority = 0,
                processing_status = CASE
                    WHEN sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                      OR sync_diffs.target_updated_at
                         IS DISTINCT FROM EXCLUDED.target_updated_at
                    THEN 'pending' ELSE sync_diffs.processing_status END,
                available_at = CASE
                    WHEN sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                      OR sync_diffs.target_updated_at
                         IS DISTINCT FROM EXCLUDED.target_updated_at
                    THEN clock_timestamp() ELSE sync_diffs.available_at END,
                last_error = CASE
                    WHEN sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                      OR sync_diffs.target_updated_at
                         IS DISTINCT FROM EXCLUDED.target_updated_at
                    THEN NULL ELSE sync_diffs.last_error END,
                target_hash = EXCLUDED.target_hash,
                target_updated_at = EXCLUDED.target_updated_at,
                updated_at = clock_timestamp()
            RETURNING true
            """,
            self._provider_uuid,
            entity_type,
            entity_uuid,
            partition_key,
            target_hash,
            target_updated_at,
        )
        return bool(updated)


class WorkspaceDiffWorker:
    def __init__(
        self,
        pool: asyncpg.Pool,
        settings: Settings,
        *,
        plan_enabled: bool = True,
        partition: int = 0,
        partition_count: int = 1,
        tokens: WorkspaceTokenManager | None = None,
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
        self._tokens = tokens or WorkspaceTokenManager(settings)
        self._plan_enabled = plan_enabled
        self._partition = partition
        self._partition_count = partition_count
        self._unmapped_cleanup_done = False
        self._zulip_writer = ZulipOutboundWriter(pool, settings)

    async def run(self) -> None:
        verify: bool | str = (
            True
            if self._settings.workspace_ca_file is None
            else str(self._settings.workspace_ca_file)
        )
        try:
            async with httpx.AsyncClient(
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
                        LOG.warning(
                            "Workspace diff pass failed; retrying", exc_info=True
                        )
                        await asyncio.sleep(self._settings.workspace_retry_base_seconds)
                        continue
                    await asyncio.sleep(self._settings.workspace_sync_poll_seconds)
        finally:
            await self._zulip_writer.close()

    async def _plan_and_drain(self, client: httpx.AsyncClient) -> int:
        planned = await self.plan()
        if planned is None:
            return 0
        # Keep planning live while a large history queue is draining. Exhausting
        # one partition here can otherwise postpone cursor-independent repair for
        # hours or days.
        processed = await self.process_once(client)
        if planned == 0 and processed == 0:
            await self._complete_initial_sync()
        return processed

    async def _drain(self, client: httpx.AsyncClient) -> int:
        processed = 0
        while changed := await self.process_once(client):
            processed += changed
        return processed

    async def plan(self) -> int | None:
        realm_uuid = await self._link_realm()
        if realm_uuid is None:
            return None
        await self._ensure_direct_topics(realm_uuid)
        await self._ensure_topic_bindings(realm_uuid)
        state = await self._pool.fetchrow(
            """
            SELECT active_generation, initial_sync_completed_at,
                   reconciliation_version, target_scan_generation
            FROM workspace_zulip_bridge.workspace_mirror_state
            WHERE provider_uuid = $1 AND bootstrap_status = 'ready'
            """,
            self._provider_uuid,
        )
        if state is None or state["active_generation"] is None:
            return 0
        generation = state["active_generation"]
        scan_target_only = (
            state.get("initial_sync_completed_at") is not None
            and state.get("target_scan_generation", generation) != generation
        )
        if (
            state.get("initial_sync_completed_at") is not None
            and not self._unmapped_cleanup_done
        ):
            await self._skip_unmapped_target_diffs(realm_uuid, generation)
            self._unmapped_cleanup_done = True
        total = 0
        for entity_type, source in _SOURCE_TABLES.items():
            planned = await self._plan_entity(
                entity_type,
                source,
                realm_uuid,
                generation,
            )
            total += planned
            if scan_target_only:
                total += await self._plan_target_only(
                    entity_type,
                    source,
                    realm_uuid,
                    generation,
                )
        if scan_target_only:
            await self._pool.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_mirror_state
                SET target_scan_generation = $2,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1
                  AND active_generation = $2
                """,
                self._provider_uuid,
                generation,
            )
        total += await self._repair_missing_source_diffs(
            realm_uuid,
            generation,
        )
        if int(state.get("reconciliation_version", 0)) < RECONCILIATION_VERSION:
            total += await self._requeue_provider_owned_mentions()
            await self._pool.execute(
                """
                UPDATE workspace_zulip_bridge.workspace_mirror_state
                SET reconciliation_version = $2,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1
                  AND reconciliation_version < $2
                """,
                self._provider_uuid,
                RECONCILIATION_VERSION,
            )
        return total

    async def _requeue_provider_owned_mentions(self) -> int:
        """Retry rows blocked before ``mentioned`` became provider-owned."""
        result = await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET processing_status = 'pending', attempt_count = 0,
                available_at = clock_timestamp(), claimed_at = NULL,
                processed_at = NULL,
                last_error = 'requeued_provider_owned_mentioned',
                updated_at = clock_timestamp()
            WHERE provider_uuid = $1
              AND entity_type = 'message_flags'
              AND direction = 'to_zulip'
              AND processing_status = 'blocked'
              AND last_error =
                  'Zulip message flags cannot be updated: mentioned'
            """,
            self._provider_uuid,
        )
        return int(result.rsplit(" ", 1)[-1])

    async def _repair_missing_source_diffs(
        self,
        realm_uuid: UUID,
        generation: UUID,
    ) -> int:
        """Continuously sweep for source rows skipped by the main cursor."""
        cursor_rows = await self._pool.fetch(
            """
            SELECT entity_type, snapshot_generation, next_run_at
            FROM workspace_zulip_bridge.sync_repair_cursors
            WHERE provider_uuid = $1
            """,
            self._provider_uuid,
        )
        cursors = {row["entity_type"]: row for row in cursor_rows}
        now = datetime.now(UTC)
        total = 0
        for entity_type, source in _SOURCE_TABLES.items():
            cursor = cursors.get(entity_type)
            if (
                cursor is not None
                and cursor["snapshot_generation"] == generation
                and cursor["next_run_at"] > now
            ):
                continue
            total += await self._repair_entity(
                entity_type,
                source,
                realm_uuid,
                generation,
            )
        if total:
            LOG.info("Reconciled missing Workspace source rows: count=%d", total)
        return total

    async def _repair_entity(
        self,
        entity_type: str,
        source: Mapping[str, str],
        realm_uuid: UUID,
        generation: UUID,
    ) -> int:
        source_timestamp_column = (
            "source.source_updated_at"
            if entity_type == "messages"
            else "source.updated_at"
        )
        cursor_timestamp_column = source_timestamp_column
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.sync_repair_cursors (
                    provider_uuid, entity_type, snapshot_generation
                ) VALUES ($1, $2, $3)
                ON CONFLICT (provider_uuid, entity_type) DO UPDATE
                SET snapshot_generation = EXCLUDED.snapshot_generation,
                    source_updated_at = CASE
                        WHEN sync_repair_cursors.snapshot_generation
                             IS DISTINCT FROM EXCLUDED.snapshot_generation
                        THEN NULL ELSE sync_repair_cursors.source_updated_at END,
                    entity_uuid = CASE
                        WHEN sync_repair_cursors.snapshot_generation
                             IS DISTINCT FROM EXCLUDED.snapshot_generation
                        THEN NULL ELSE sync_repair_cursors.entity_uuid END,
                    next_run_at = CASE
                        WHEN sync_repair_cursors.snapshot_generation
                             IS DISTINCT FROM EXCLUDED.snapshot_generation
                        THEN clock_timestamp()
                        ELSE sync_repair_cursors.next_run_at END,
                    updated_at = clock_timestamp()
                """,
                self._provider_uuid,
                entity_type,
                generation,
            )
            cursor = await connection.fetchrow(
                """
                SELECT source_updated_at, entity_uuid, next_run_at
                FROM workspace_zulip_bridge.sync_repair_cursors
                WHERE provider_uuid = $1 AND entity_type = $2
                FOR UPDATE
                """,
                self._provider_uuid,
                entity_type,
            )
            assert cursor is not None
            if cursor["next_run_at"] > datetime.now(UTC):
                return 0
            rows = await connection.fetch(
                f"""
                WITH scanned AS MATERIALIZED (
                    SELECT source.uuid AS entity_uuid,
                           {source["partition"]} AS partition_key,
                           {source["hash"]} AS source_hash,
                           {source_timestamp_column} AS source_updated_at,
                           {cursor_timestamp_column} AS cursor_updated_at,
                           target.uuid AS target_uuid
                    FROM {source["from"]} AS source
                    {source["joins"]}
                    LEFT JOIN workspace_zulip_bridge.workspace_{entity_type}
                        AS target
                      ON target.provider_uuid = $1
                     AND target.snapshot_generation = $4
                     AND target.uuid = source.uuid
                    WHERE {source["where"]}
                      AND (
                          $5::timestamptz IS NULL
                          OR ({cursor_timestamp_column}, source.uuid) > ($5, $6)
                      )
                    ORDER BY {cursor_timestamp_column}, source.uuid
                    LIMIT $7
                ), repaired AS (
                    INSERT INTO workspace_zulip_bridge.sync_diffs (
                        provider_uuid, entity_type, entity_uuid, realm_uuid,
                        partition_key, direction, source_hash, target_hash,
                        source_updated_at, target_updated_at
                    )
                    SELECT $1, $2, scanned.entity_uuid, $3,
                           scanned.partition_key, 'to_workspace',
                           scanned.source_hash, NULL,
                           scanned.source_updated_at, NULL
                    FROM scanned
                    WHERE scanned.target_uuid IS NULL
                    ON CONFLICT (provider_uuid, entity_type, entity_uuid)
                    DO UPDATE SET
                        realm_uuid = EXCLUDED.realm_uuid,
                        partition_key = EXCLUDED.partition_key,
                        direction = 'to_workspace',
                        source_hash = EXCLUDED.source_hash,
                        target_hash = NULL,
                        source_updated_at = EXCLUDED.source_updated_at,
                        target_updated_at = NULL,
                        processing_status = 'pending',
                        available_at = clock_timestamp(),
                        claimed_at = NULL,
                        processed_at = NULL,
                        last_error = 'reconciled_missing_workspace_entity',
                        updated_at = clock_timestamp()
                    WHERE sync_diffs.processing_status IN (
                        'applied', 'skipped', 'blocked'
                    )
                       OR sync_diffs.direction <> 'to_workspace'
                    RETURNING 1
                )
                SELECT entity_uuid, cursor_updated_at,
                       (SELECT count(*) FROM repaired) AS repaired_count
                FROM scanned
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
                await connection.execute(
                    """
                    UPDATE workspace_zulip_bridge.sync_repair_cursors
                    SET source_updated_at = NULL, entity_uuid = NULL,
                        next_run_at = clock_timestamp()
                            + make_interval(secs => $3::double precision),
                        updated_at = clock_timestamp()
                    WHERE provider_uuid = $1 AND entity_type = $2
                    """,
                    self._provider_uuid,
                    entity_type,
                    self._settings.workspace_reconciliation_interval_seconds,
                )
                return 0
            last = max(
                rows,
                key=lambda row: (row["cursor_updated_at"], row["entity_uuid"]),
            )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.sync_repair_cursors
                SET source_updated_at = $3, entity_uuid = $4,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1 AND entity_type = $2
                """,
                self._provider_uuid,
                entity_type,
                last["cursor_updated_at"],
                last["entity_uuid"],
            )
            return int(rows[0]["repaired_count"])

    async def _plan_target_only(
        self,
        entity_type: str,
        source: Mapping[str, str],
        realm_uuid: UUID,
        generation: UUID,
    ) -> int:
        if entity_type == "message_reactions":
            partition = "(parent.data ->> 'stream_uuid')::uuid"
            joins = """
                JOIN workspace_zulip_bridge.workspace_messages AS parent
                  ON parent.provider_uuid = target.provider_uuid
                 AND parent.snapshot_generation = target.snapshot_generation
                 AND parent.uuid = (target.data ->> 'message_uuid')::uuid
                JOIN workspace_zulip_bridge.zulip_streams AS mapped_stream
                  ON mapped_stream.realm_uuid = $3
                 AND mapped_stream.uuid =
                     (parent.data ->> 'stream_uuid')::uuid
            """
        elif entity_type in {
            "stream_bindings",
            "topics",
            "topic_bindings",
            "messages",
            "message_flags",
        }:
            partition = "(target.data ->> 'stream_uuid')::uuid"
            joins = """
                JOIN workspace_zulip_bridge.zulip_streams AS mapped_stream
                  ON mapped_stream.realm_uuid = $3
                 AND mapped_stream.uuid =
                     (target.data ->> 'stream_uuid')::uuid
            """
        elif entity_type == "streams":
            partition = "NULL::uuid"
            joins = """
                JOIN workspace_zulip_bridge.zulip_streams AS mapped_stream
                  ON mapped_stream.realm_uuid = $3
                 AND mapped_stream.uuid = target.uuid
            """
        else:
            partition = "NULL::uuid"
            joins = ""
        source_table = source["from"].rsplit(".", 1)[-1]
        result = await self._pool.execute(
            f"""
            INSERT INTO workspace_zulip_bridge.sync_diffs (
                provider_uuid, entity_type, entity_uuid, realm_uuid,
                partition_key, direction, source_hash, target_hash,
                source_updated_at, target_updated_at
            )
            SELECT $1, $2, target.uuid, $3, {partition}, 'to_zulip',
                   NULL, target.content_hash,
                   target.source_updated_at, target.source_updated_at
            FROM workspace_zulip_bridge.workspace_{entity_type} AS target
            {joins}
            WHERE target.provider_uuid = $1
              AND target.snapshot_generation = $4
              AND (
                    $2 <> 'users'
                    OR NOT EXISTS (
                        SELECT 1
                        FROM workspace_zulip_bridge.zulip_users AS linked_user
                        WHERE linked_user.workspace_user_uuid = target.uuid
                    )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.{source_table} AS source
                  WHERE source.uuid = target.uuid
              )
            ON CONFLICT (provider_uuid, entity_type, entity_uuid)
            DO UPDATE SET
                direction = 'to_zulip',
                partition_key = EXCLUDED.partition_key,
                target_hash = EXCLUDED.target_hash,
                target_updated_at = EXCLUDED.target_updated_at,
                processing_status = CASE
                    WHEN sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                      OR sync_diffs.target_updated_at
                         IS DISTINCT FROM EXCLUDED.target_updated_at
                    THEN 'pending' ELSE sync_diffs.processing_status END,
                available_at = CASE
                    WHEN sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                      OR sync_diffs.target_updated_at
                         IS DISTINCT FROM EXCLUDED.target_updated_at
                    THEN clock_timestamp() ELSE sync_diffs.available_at END,
                updated_at = clock_timestamp()
            WHERE sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
               OR sync_diffs.target_updated_at
                  IS DISTINCT FROM EXCLUDED.target_updated_at
               OR sync_diffs.direction IS DISTINCT FROM 'to_zulip'
               OR sync_diffs.partition_key
                  IS DISTINCT FROM EXCLUDED.partition_key
            """,
            self._provider_uuid,
            entity_type,
            realm_uuid,
            generation,
        )
        return int(result.rsplit(" ", 1)[-1])

    async def _skip_unmapped_target_diffs(
        self,
        realm_uuid: UUID,
        generation: UUID,
    ) -> int:
        """Retire Workspace-only rows that are outside this Zulip realm."""
        total = 0
        result = await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs AS diff
            SET processing_status = 'skipped',
                processed_at = clock_timestamp(),
                last_error = 'outside_zulip_provider_scope',
                updated_at = clock_timestamp()
            FROM workspace_zulip_bridge.workspace_streams AS target
            WHERE diff.provider_uuid = $1
              AND diff.realm_uuid = $2
              AND diff.entity_type = 'streams'
              AND diff.entity_uuid = target.uuid
              AND target.provider_uuid = diff.provider_uuid
              AND target.snapshot_generation = $3
              AND diff.direction = 'to_zulip'
              AND diff.source_hash IS NULL
              AND diff.processing_status IN ('pending', 'failed', 'blocked')
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.zulip_streams AS mapped_stream
                  WHERE mapped_stream.realm_uuid = diff.realm_uuid
                    AND mapped_stream.uuid = target.uuid
              )
            """,
            self._provider_uuid,
            realm_uuid,
            generation,
        )
        total += int(result.rsplit(" ", 1)[-1])
        for entity_type in (
            "stream_bindings",
            "topics",
            "topic_bindings",
            "messages",
            "message_flags",
        ):
            result = await self._pool.execute(
                f"""
                UPDATE workspace_zulip_bridge.sync_diffs AS diff
                SET processing_status = 'skipped',
                    processed_at = clock_timestamp(),
                    last_error = 'outside_zulip_provider_scope',
                    updated_at = clock_timestamp()
                FROM workspace_zulip_bridge.workspace_{entity_type} AS target
                WHERE diff.provider_uuid = $1
                  AND diff.realm_uuid = $2
                  AND diff.entity_type = $4
                  AND diff.entity_uuid = target.uuid
                  AND target.provider_uuid = diff.provider_uuid
                  AND target.snapshot_generation = $3
                  AND diff.direction = 'to_zulip'
                  AND diff.source_hash IS NULL
                  AND diff.processing_status IN ('pending', 'failed', 'blocked')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM workspace_zulip_bridge.zulip_streams AS mapped_stream
                      WHERE mapped_stream.realm_uuid = diff.realm_uuid
                        AND mapped_stream.uuid =
                            (target.data ->> 'stream_uuid')::uuid
                  )
                """,
                self._provider_uuid,
                realm_uuid,
                generation,
                entity_type,
            )
            total += int(result.rsplit(" ", 1)[-1])
        result = await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs AS diff
            SET processing_status = 'skipped',
                processed_at = clock_timestamp(),
                last_error = 'outside_zulip_provider_scope',
                updated_at = clock_timestamp()
            FROM workspace_zulip_bridge.workspace_message_reactions AS target
            JOIN workspace_zulip_bridge.workspace_messages AS parent
              ON parent.provider_uuid = target.provider_uuid
             AND parent.snapshot_generation = target.snapshot_generation
             AND parent.uuid = (target.data ->> 'message_uuid')::uuid
            WHERE diff.provider_uuid = $1
              AND diff.realm_uuid = $2
              AND diff.entity_type = 'message_reactions'
              AND diff.entity_uuid = target.uuid
              AND target.provider_uuid = diff.provider_uuid
              AND target.snapshot_generation = $3
              AND diff.direction = 'to_zulip'
              AND diff.source_hash IS NULL
              AND diff.processing_status IN ('pending', 'failed', 'blocked')
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.zulip_streams AS mapped_stream
                  WHERE mapped_stream.realm_uuid = diff.realm_uuid
                    AND mapped_stream.uuid =
                        (parent.data ->> 'stream_uuid')::uuid
              )
            """,
            self._provider_uuid,
            realm_uuid,
            generation,
        )
        total += int(result.rsplit(" ", 1)[-1])
        if total:
            LOG.info("Skipped Workspace rows outside Zulip scope: count=%d", total)
        return total

    async def _plan_entity(
        self,
        entity_type: str,
        source: Mapping[str, str],
        realm_uuid: UUID,
        generation: UUID,
    ) -> int:
        source_timestamp_column = (
            "source.source_updated_at"
            if entity_type == "messages"
            else "source.updated_at"
        )
        cursor_timestamp_column = "source.updated_at"
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
                WITH candidates AS MATERIALIZED (
                    SELECT source.uuid AS entity_uuid,
                           {source["partition"]} AS partition_key,
                           CASE
                               WHEN target.source_updated_at > {source_timestamp_column}
                               THEN 'to_zulip'
                               ELSE 'to_workspace'
                           END AS direction,
                           {source["hash"]} AS source_hash,
                           target.content_hash AS target_hash,
                           {source_timestamp_column} AS source_updated_at,
                           {cursor_timestamp_column} AS cursor_updated_at,
                           target.source_updated_at AS target_updated_at
                    FROM {source["from"]} AS source
                    {source["joins"]}
                    LEFT JOIN workspace_zulip_bridge.workspace_{entity_type}
                        AS target
                      ON target.provider_uuid = $1
                     AND target.snapshot_generation = $4
                     AND target.uuid = source.uuid
                    WHERE {source["where"]}
                      AND (
                          $5::timestamptz IS NULL
                          OR ({cursor_timestamp_column}, source.uuid) > ($5, $6)
                      )
                    ORDER BY {cursor_timestamp_column}, source.uuid
                    LIMIT $7
                ), upsert AS (
                INSERT INTO workspace_zulip_bridge.sync_diffs (
                    provider_uuid, entity_type, entity_uuid, realm_uuid,
                    partition_key,
                    direction, source_hash, target_hash,
                    source_updated_at, target_updated_at
                )
                SELECT $1, $2, candidate.entity_uuid, $3,
                       candidate.partition_key, candidate.direction,
                       candidate.source_hash, candidate.target_hash,
                       candidate.source_updated_at,
                       candidate.target_updated_at
                FROM candidates AS candidate
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
                WHERE sync_diffs.source_hash IS DISTINCT FROM EXCLUDED.source_hash
                   OR sync_diffs.target_hash IS DISTINCT FROM EXCLUDED.target_hash
                   OR sync_diffs.source_updated_at
                      IS DISTINCT FROM EXCLUDED.source_updated_at
                   OR sync_diffs.target_updated_at
                      IS DISTINCT FROM EXCLUDED.target_updated_at
                   OR sync_diffs.direction IS DISTINCT FROM EXCLUDED.direction
                   OR sync_diffs.partition_key
                      IS DISTINCT FROM EXCLUDED.partition_key
                RETURNING 1
                )
                SELECT entity_uuid, cursor_updated_at
                FROM candidates
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
                key=lambda row: (row["cursor_updated_at"], row["entity_uuid"]),
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
                last["cursor_updated_at"],
                last["entity_uuid"],
            )
            return len(rows)

    async def process_once(self, client: httpx.AsyncClient) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.sync_diffs
                SET processing_status = 'pending', claimed_at = NULL,
                    available_at = clock_timestamp(), processed_at = NULL,
                    last_error = 'claim_expired', updated_at = clock_timestamp()
                WHERE provider_uuid = $1
                  AND processing_status = 'processing'
                  AND claimed_at < (
                      clock_timestamp()
                      - make_interval(secs => $2::double precision)
                  )
                """,
                self._provider_uuid,
                self._settings.event_processor_claim_timeout_seconds,
            )
            delivery_priority = await connection.fetchval(
                """
                SELECT delivery_priority
                FROM workspace_zulip_bridge.sync_diffs
                WHERE provider_uuid = $1
                  AND processing_status IN ('pending', 'failed')
                  AND available_at <= clock_timestamp()
                  AND (
                      ($2 = 0 AND entity_type NOT IN (
                          'messages', 'message_flags', 'message_reactions'
                      ))
                      OR (
                          entity_type IN (
                              'messages', 'message_flags', 'message_reactions'
                          )
                          AND (
                              (
                                  hashtextextended(
                                      COALESCE(partition_key, entity_uuid)::text,
                                      0
                                  ) % $3 + $3
                              ) % $3
                          ) = $2
                      )
                  )
                ORDER BY delivery_priority
                LIMIT 1
                """,
                self._provider_uuid,
                self._partition,
                self._partition_count,
            )
            if delivery_priority is None:
                return 0
            rows = await connection.fetch(
                """
                WITH claim AS (
                    SELECT provider_uuid, entity_type, entity_uuid
                    FROM workspace_zulip_bridge.sync_diffs
                    WHERE provider_uuid = $1
                      AND processing_status IN ('pending', 'failed')
                      AND available_at <= clock_timestamp()
                      AND delivery_priority = $5
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
                    ORDER BY delivery_priority,
                        CASE entity_type
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
                delivery_priority,
            )
        if not rows:
            return 0
        delivery_class = "live" if int(delivery_priority) == 0 else "backfill"
        to_workspace = [row for row in rows if row["direction"] == "to_workspace"]
        to_zulip = [row for row in rows if row["direction"] == "to_zulip"]
        if to_zulip:
            await self._write_to_zulip(to_zulip)
        if not to_workspace:
            return len(rows)
        candidates: list[
            tuple[asyncpg.Record, dict[str, Any], bytes, dict[str, Any]]
        ] = []
        loaded: dict[tuple[str, UUID], dict[str, Any]] = {}
        targets: dict[tuple[str, UUID], dict[str, Any]] = {}
        grouped: dict[str, list[UUID]] = defaultdict(list)
        for row in to_workspace:
            grouped[row["entity_type"]].append(row["entity_uuid"])
        for entity_type, entity_uuids in grouped.items():
            loaded.update(await self._load_zulip_entities(entity_type, entity_uuids))
            targets.update(
                await self._load_workspace_entities(entity_type, entity_uuids)
            )
        reaction_sources = [
            data
            for (entity_type, _), data in loaded.items()
            if entity_type == "message_reactions"
        ]
        reaction_owners = await self._workspace_reaction_owners(reaction_sources)
        skipped: list[asyncpg.Record] = []
        equivalent: list[tuple[asyncpg.Record, dict[str, Any], bytes]] = []
        for row in to_workspace:
            key = (row["entity_type"], row["entity_uuid"])
            data = loaded.get(key)
            target = targets.get(key)
            if _equivalent_entity(row["entity_type"], data, target):
                equivalent.append(
                    (row, data or {}, canonical_hash(data) if data else b"")
                )
                continue
            if data is None:
                candidates.append(
                    (
                        row,
                        {},
                        b"",
                        {
                            "action": "delete",
                            "type": row["entity_type"],
                            "uuid": str(row["entity_uuid"]),
                        },
                    )
                )
                continue
            if row["entity_type"] == "message_reactions":
                identity = _reaction_identity(data)
                owner = reaction_owners.setdefault(identity, row["entity_uuid"])
                if owner != row["entity_uuid"]:
                    skipped.append(row)
                    continue
            content_hash = canonical_hash(data)
            operation = {
                "action": "upsert",
                "type": row["entity_type"],
                "uuid": str(row["entity_uuid"]),
                "content_hash": content_hash.hex(),
                "source_updated_at": row["source_updated_at"]
                .isoformat()
                .replace("+00:00", "Z"),
                "data": data,
            }
            if identity_rebind_required(
                row["entity_type"],
                data,
                target,
            ):
                operation["rebind_identity"] = True
            candidates.append((row, data, content_hash, operation))
        if equivalent:
            await self._accept_equivalent(equivalent)
        if skipped:
            await self._mark(
                skipped,
                "skipped",
                "workspace_reaction_identity_duplicate",
            )
        if not candidates:
            return len(rows)
        ready, deferred = await self._partition_dependency_ready(candidates)
        if deferred:
            await self._defer_for_dependencies(deferred)
        if not ready:
            return len(rows)
        ready.sort(key=lambda item: PRIORITY[item[0]["entity_type"]])
        operations = [item[3] for item in ready]
        records = [item[:3] for item in ready]
        try:
            response = await self._post(
                client,
                f"{workspace_api_url(self._settings)}/provider/entities/actions/apply/invoke",
                json={
                    "delivery_class": delivery_class,
                    "operations": operations,
                },
            )
            if response.is_error:
                error = _provider_api_error(response)
                await self._isolate_provider_failure(records, error)
                raise error
            results = response.json()["results"]
            if len(results) != len(records):
                raise RuntimeError("Workspace batch result length mismatch")
            await self._accept(records)
        except ProviderApiError:
            raise
        except Exception as exc:
            await self._mark(
                [record[0] for record in records],
                "failed",
                str(exc)[:2048],
            )
            raise
        return len(rows)

    async def _partition_dependency_ready(
        self,
        candidates: list[tuple[asyncpg.Record, dict[str, Any], bytes, dict[str, Any]]],
    ) -> tuple[
        list[tuple[asyncpg.Record, dict[str, Any], bytes, dict[str, Any]]],
        list[asyncpg.Record],
    ]:
        dependencies: dict[str, set[UUID]] = defaultdict(set)
        for row, data, _, _ in candidates:
            if not data:
                continue
            for dependency_type, dependency_uuid in _entity_dependencies(
                row["entity_type"], data
            ):
                dependencies[dependency_type].add(dependency_uuid)
        ready_ids = await self._load_ready_dependency_ids(dependencies)
        ready = []
        deferred = []
        for candidate in sorted(
            candidates,
            key=lambda item: PRIORITY[item[0]["entity_type"]],
        ):
            row, data, _, _ = candidate
            required = _entity_dependencies(row["entity_type"], data) if data else ()
            if all(
                dependency_uuid in ready_ids[dependency_type]
                for dependency_type, dependency_uuid in required
            ):
                ready.append(candidate)
                if data:
                    ready_ids[row["entity_type"]].add(row["entity_uuid"])
            else:
                deferred.append(row)
        return ready, deferred

    async def _load_ready_dependency_ids(
        self,
        dependencies: Mapping[str, set[UUID]],
    ) -> dict[str, set[UUID]]:
        ready: dict[str, set[UUID]] = defaultdict(set)
        if not any(dependencies.values()):
            return ready
        rows = await self._pool.fetch(
            """
            WITH active AS (
                SELECT active_generation
                FROM workspace_zulip_bridge.workspace_mirror_state
                WHERE provider_uuid = $1
            )
            SELECT 'users' AS entity_type, entity.uuid
            FROM workspace_zulip_bridge.workspace_users AS entity
            JOIN active ON active.active_generation = entity.snapshot_generation
            WHERE entity.provider_uuid = $1 AND entity.uuid = ANY($2::uuid[])
            UNION ALL
            SELECT 'users', source.workspace_user_uuid
            FROM workspace_zulip_bridge.zulip_users AS source
            WHERE source.workspace_user_uuid = ANY($2::uuid[])
            UNION ALL
            SELECT 'streams', entity.uuid
            FROM workspace_zulip_bridge.workspace_streams AS entity
            JOIN active ON active.active_generation = entity.snapshot_generation
            WHERE entity.provider_uuid = $1 AND entity.uuid = ANY($3::uuid[])
            UNION ALL
            SELECT 'topics', entity.uuid
            FROM workspace_zulip_bridge.workspace_topics AS entity
            JOIN active ON active.active_generation = entity.snapshot_generation
            WHERE entity.provider_uuid = $1 AND entity.uuid = ANY($4::uuid[])
            UNION ALL
            SELECT 'messages', entity.uuid
            FROM workspace_zulip_bridge.workspace_messages AS entity
            JOIN active ON active.active_generation = entity.snapshot_generation
            WHERE entity.provider_uuid = $1 AND entity.uuid = ANY($5::uuid[])
            """,
            self._provider_uuid,
            list(dependencies.get("users", ())),
            list(dependencies.get("streams", ())),
            list(dependencies.get("topics", ())),
            list(dependencies.get("messages", ())),
        )
        for row in rows:
            ready[row["entity_type"]].add(UUID(str(row["uuid"])))
        return ready

    async def _defer_for_dependencies(self, rows: list[asyncpg.Record]) -> None:
        await self._release_claims(
            rows,
            "waiting_for_workspace_dependencies",
            delay_seconds=max(1.0, self._settings.workspace_sync_poll_seconds * 10),
        )

    async def _isolate_provider_failure(
        self,
        records: list[tuple[asyncpg.Record, dict[str, Any], bytes]],
        error: ProviderApiError,
    ) -> None:
        item_index = error.item_index
        if item_index is None or item_index >= len(records):
            await self._mark(
                [record[0] for record in records],
                "failed",
                str(error)[:2048],
            )
            return
        failed = records[item_index][0]
        rolled_back = [
            record[0] for index, record in enumerate(records) if index != item_index
        ]
        await self._mark([failed], "failed", str(error)[:2048])
        if rolled_back:
            await self._release_claims(
                rolled_back,
                f"workspace_batch_rolled_back item_index={item_index}",
                delay_seconds=0.0,
            )

    async def _release_claims(
        self,
        rows: list[asyncpg.Record],
        reason: str,
        *,
        delay_seconds: float,
    ) -> None:
        if not rows:
            return
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.executemany(
                """
                UPDATE workspace_zulip_bridge.sync_diffs
                SET processing_status = 'pending', claimed_at = NULL,
                    attempt_count = GREATEST(attempt_count - 1, 0),
                    available_at = clock_timestamp()
                        + make_interval(secs => $6::double precision),
                    processed_at = NULL, last_error = $5,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1 AND entity_type = $2 AND entity_uuid = $3
                  AND processing_status = 'processing' AND claimed_at = $4
                """,
                [
                    (
                        row["provider_uuid"],
                        row["entity_type"],
                        row["entity_uuid"],
                        row["claimed_at"],
                        reason,
                        delay_seconds,
                    )
                    for row in rows
                ],
            )

    async def _write_to_zulip(self, rows: list[asyncpg.Record]) -> None:
        source_entities: dict[tuple[str, UUID], dict[str, Any]] = {}
        target_entities: dict[tuple[str, UUID], dict[str, Any]] = {}
        grouped: dict[str, list[UUID]] = defaultdict(list)
        for row in rows:
            grouped[row["entity_type"]].append(row["entity_uuid"])
        for entity_type, entity_uuids in grouped.items():
            source_entities.update(
                await self._load_zulip_entities(entity_type, entity_uuids)
            )
            target_entities.update(
                await self._load_workspace_entities(entity_type, entity_uuids)
            )
        for row in rows:
            key = (row["entity_type"], row["entity_uuid"])
            source = source_entities.get(key)
            target = target_entities.get(key)
            if _equivalent_entity(row["entity_type"], source, target):
                await self._accept_to_zulip(row, "equivalent")
                continue
            try:
                await self._zulip_writer.apply(
                    row["entity_type"],
                    row["entity_uuid"],
                    source,
                    target,
                    row["target_updated_at"],
                )
            except ZulipOutboundPending as exc:
                await self._mark(
                    [row],
                    "failed",
                    str(exc)[:2048],
                    retry_base_seconds=self._settings.zulip_retry_base_seconds,
                    retry_cap_seconds=self._settings.zulip_retry_cap_seconds,
                )
            except ZulipOutboundError as exc:
                await self._mark([row], "blocked", str(exc)[:2048])
            except ZulipApiError as exc:
                if row["entity_type"] == "message_reactions" and (
                    (target is not None and exc.code == "REACTION_ALREADY_EXISTS")
                    or (target is None and exc.code == "REACTION_DOES_NOT_EXIST")
                ):
                    await self._accept_to_zulip(row, "already_converged")
                    continue
                status = "failed" if exc.retryable else "blocked"
                await self._mark(
                    [row],
                    status,
                    exc.code[:2048],
                    retry_base_seconds=self._settings.zulip_retry_base_seconds,
                    retry_cap_seconds=self._settings.zulip_retry_cap_seconds,
                )
            except Exception as exc:
                error_category = f"unexpected_error:{type(exc).__name__}"
                LOG.error(
                    "Workspace-to-Zulip mutation failed error=%s",
                    error_category,
                )
                await self._mark(
                    [row],
                    "failed",
                    error_category,
                    retry_base_seconds=self._settings.zulip_retry_base_seconds,
                    retry_cap_seconds=self._settings.zulip_retry_cap_seconds,
                )
            else:
                await self._accept_to_zulip(row, "written")

    async def _accept_to_zulip(
        self,
        row: asyncpg.Record,
        outcome: str,
    ) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.sync_diffs
            SET processing_status = 'applied', processed_at = clock_timestamp(),
                source_hash = target_hash,
                source_updated_at = COALESCE(target_updated_at, source_updated_at),
                last_error = $5, updated_at = clock_timestamp()
            WHERE provider_uuid = $1 AND entity_type = $2 AND entity_uuid = $3
              AND processing_status = 'processing' AND claimed_at = $4
            """,
            row["provider_uuid"],
            row["entity_type"],
            row["entity_uuid"],
            row["claimed_at"],
            outcome,
        )

    async def _active_claim_records(
        self,
        connection: asyncpg.Connection | PoolConnectionProxy,
        records: list[tuple[asyncpg.Record, dict[str, Any], bytes]],
    ) -> list[tuple[asyncpg.Record, dict[str, Any], bytes]]:
        if not records:
            return []
        active = await connection.fetch(
            """
            SELECT input.entity_type, input.entity_uuid, input.claimed_at
            FROM unnest($2::text[], $3::uuid[], $4::timestamptz[])
                AS input(entity_type, entity_uuid, claimed_at)
            JOIN workspace_zulip_bridge.sync_diffs AS diff
              ON diff.provider_uuid = $1
             AND diff.entity_type = input.entity_type
             AND diff.entity_uuid = input.entity_uuid
             AND diff.processing_status = 'processing'
             AND diff.claimed_at = input.claimed_at
            FOR UPDATE OF diff
            """,
            self._provider_uuid,
            [record[0]["entity_type"] for record in records],
            [record[0]["entity_uuid"] for record in records],
            [record[0]["claimed_at"] for record in records],
        )
        active_keys = {
            (row["entity_type"], row["entity_uuid"], row["claimed_at"])
            for row in active
        }
        return [
            record
            for record in records
            if (
                record[0]["entity_type"],
                record[0]["entity_uuid"],
                record[0]["claimed_at"],
            )
            in active_keys
        ]

    async def _post(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        json: dict[str, Any],
    ) -> httpx.Response:
        client.headers["Authorization"] = f"Bearer {await self._tokens.access_token()}"
        response = await client.post(url, json=json)
        if response.status_code != 401:
            return response
        client.headers["Authorization"] = (
            f"Bearer {await self._tokens.access_token(force_refresh=True)}"
        )
        return await client.post(url, json=json)

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
                  JOIN workspace_zulip_bridge.zulip_realms AS realm
                    ON realm.uuid = connection.realm_uuid
                  WHERE connection.sync_enabled
                    AND NOT zulip_user.disabled AND NOT zulip_user.is_bot
                    AND realm.workspace_provider_uuid = mirror.provider_uuid
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_zulip_bridge.zulip_connections AS connection
                  JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                    ON zulip_user.uuid = connection.zulip_user_uuid
                  JOIN workspace_zulip_bridge.zulip_realms AS realm
                    ON realm.uuid = connection.realm_uuid
                  WHERE connection.sync_enabled
                    AND NOT zulip_user.disabled AND NOT zulip_user.is_bot
                    AND realm.workspace_provider_uuid = mirror.provider_uuid
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
            records = await self._active_claim_records(connection, records)
            if not records:
                return
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
                FROM unnest($2::text[], $3::uuid[], $4::bytea[],
                            $5::timestamptz[])
                    AS input(entity_type, entity_uuid, target_hash, claimed_at)
                WHERE diff.provider_uuid = $1
                  AND diff.entity_type = input.entity_type
                  AND diff.entity_uuid = input.entity_uuid
                  AND diff.processing_status = 'processing'
                  AND diff.claimed_at = input.claimed_at
                """,
                self._provider_uuid,
                [record[0]["entity_type"] for record in records],
                [record[0]["entity_uuid"] for record in records],
                [record[2] or None for record in records],
                [record[0]["claimed_at"] for record in records],
            )

    async def _accept_equivalent(
        self, records: list[tuple[asyncpg.Record, dict[str, Any], bytes]]
    ) -> None:
        state = await self._pool.fetchrow(
            "SELECT active_generation FROM "
            "workspace_zulip_bridge.workspace_mirror_state "
            "WHERE provider_uuid = $1",
            self._provider_uuid,
        )
        if state is None or state["active_generation"] is None:
            raise RuntimeError("Workspace mirror generation disappeared")
        generation = state["active_generation"]
        async with self._pool.acquire() as connection, connection.transaction():
            records = await self._active_claim_records(connection, records)
            if not records:
                return
            grouped: dict[str, list[tuple[asyncpg.Record, dict[str, Any], bytes]]] = (
                defaultdict(list)
            )
            for record in records:
                if record[1]:
                    grouped[record[0]["entity_type"]].append(record)
            for entity_type, items in grouped.items():
                await connection.executemany(
                    f"""
                    UPDATE workspace_zulip_bridge.workspace_{entity_type}
                    SET content_hash = $4, source_updated_at = $5,
                        updated_at = clock_timestamp()
                    WHERE provider_uuid = $1 AND snapshot_generation = $2
                      AND uuid = $3
                    """,
                    [
                        (
                            self._provider_uuid,
                            generation,
                            row["entity_uuid"],
                            content_hash,
                            row["source_updated_at"],
                        )
                        for row, _, content_hash in items
                    ],
                )
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.sync_diffs AS diff
                SET processing_status = 'applied', processed_at = clock_timestamp(),
                    source_hash = input.content_hash,
                    target_hash = input.content_hash,
                    target_updated_at = diff.source_updated_at,
                    last_error = 'equivalent', updated_at = clock_timestamp()
                FROM unnest($2::text[], $3::uuid[], $4::bytea[],
                            $5::timestamptz[])
                    AS input(entity_type, entity_uuid, content_hash, claimed_at)
                WHERE diff.provider_uuid = $1
                  AND diff.entity_type = input.entity_type
                  AND diff.entity_uuid = input.entity_uuid
                  AND diff.processing_status = 'processing'
                  AND diff.claimed_at = input.claimed_at
                """,
                self._provider_uuid,
                [record[0]["entity_type"] for record in records],
                [record[0]["entity_uuid"] for record in records],
                [record[2] or None for record in records],
                [record[0]["claimed_at"] for record in records],
            )

    async def _mark(
        self,
        rows: list[asyncpg.Record],
        status: str,
        error: str,
        *,
        retry_base_seconds: float | None = None,
        retry_cap_seconds: float | None = None,
    ) -> None:
        retry_base_seconds = (
            self._settings.workspace_retry_base_seconds
            if retry_base_seconds is None
            else retry_base_seconds
        )
        retry_cap_seconds = (
            self._settings.workspace_retry_cap_seconds
            if retry_cap_seconds is None
            else retry_cap_seconds
        )
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.executemany(
                """
                UPDATE workspace_zulip_bridge.sync_diffs
                SET processing_status = $5, last_error = $6,
                    available_at = CASE WHEN $5 = 'failed'
                        THEN clock_timestamp() + make_interval(
                            secs => LEAST(
                                $8::double precision,
                                $7::double precision * power(
                                    2::double precision,
                                    GREATEST(attempt_count - 1, 0)
                                )
                            )
                        )
                        ELSE available_at END,
                    processed_at = CASE WHEN $5 IN ('blocked', 'skipped')
                        THEN clock_timestamp() ELSE processed_at END,
                    updated_at = clock_timestamp()
                WHERE provider_uuid = $1 AND entity_type = $2 AND entity_uuid = $3
                  AND processing_status = 'processing' AND claimed_at = $4
                """,
                [
                    (
                        row["provider_uuid"],
                        row["entity_type"],
                        row["entity_uuid"],
                        row["claimed_at"],
                        status,
                        error,
                        retry_base_seconds,
                        retry_cap_seconds,
                    )
                    for row in rows
                ],
            )

    async def _link_realm(self) -> UUID | None:
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
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("Workspace provider maps to multiple Zulip realms")
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

    async def _ensure_topic_bindings(self, realm_uuid: UUID) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                SELECT topic.uuid AS topic_uuid,
                       topic.zulip_stream_uuid AS stream_uuid,
                       binding.zulip_user_uuid AS user_uuid,
                       GREATEST(topic.created_at, binding.created_at) AS created_at,
                       jsonb_build_object(
                           'stream_uuid', topic.zulip_stream_uuid,
                           'topic_uuid', topic.uuid,
                           'user_uuid', binding.zulip_user_uuid,
                           'notification_mode', 'default',
                           'created_at',
                               GREATEST(topic.created_at, binding.created_at)
                       ) AS data
                FROM workspace_zulip_bridge.zulip_topics AS topic
                JOIN workspace_zulip_bridge.zulip_streams AS stream
                  ON stream.uuid = topic.zulip_stream_uuid
                JOIN workspace_zulip_bridge.zulip_stream_bindings AS binding
                  ON binding.zulip_stream_uuid = topic.zulip_stream_uuid
                WHERE stream.realm_uuid = $1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM workspace_zulip_bridge.zulip_topic_bindings AS existing
                      WHERE existing.topic_uuid = topic.uuid
                        AND existing.zulip_user_uuid = binding.zulip_user_uuid
                  )
                """,
                realm_uuid,
            )
            if not rows:
                return
            records = [
                (
                    stable_topic_binding_uuid(row["topic_uuid"], row["user_uuid"]),
                    row["stream_uuid"],
                    row["topic_uuid"],
                    row["user_uuid"],
                    canonical_hash(_json_object(row["data"])),
                    row["created_at"],
                )
                for row in rows
            ]
            await connection.execute(
                """
                CREATE TEMPORARY TABLE pending_topic_bindings (
                    uuid uuid NOT NULL,
                    stream_uuid uuid NOT NULL,
                    topic_uuid uuid NOT NULL,
                    user_uuid uuid NOT NULL,
                    content_hash bytea NOT NULL,
                    created_at timestamptz NOT NULL
                ) ON COMMIT DROP
                """
            )
            await connection.copy_records_to_table(
                "pending_topic_bindings",
                records=records,
                columns=(
                    "uuid",
                    "stream_uuid",
                    "topic_uuid",
                    "user_uuid",
                    "content_hash",
                    "created_at",
                ),
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_topic_bindings (
                    uuid, zulip_stream_uuid, topic_uuid, zulip_user_uuid,
                    notification_mode, content_hash, created_at, updated_at
                )
                SELECT uuid, stream_uuid, topic_uuid, user_uuid,
                       'default', content_hash, created_at, created_at
                FROM pending_topic_bindings
                ON CONFLICT (topic_uuid, zulip_user_uuid) DO NOTHING
                """
            )

    async def _load_zulip_entities(
        self, entity_type: str, entity_uuids: list[UUID]
    ) -> dict[tuple[str, UUID], dict[str, Any]]:
        rows = await self._pool.fetch(_ENTITY_QUERIES[entity_type], entity_uuids)
        return {
            (entity_type, UUID(str(row["entity_uuid"]))): _json_object(row["data"])
            for row in rows
        }

    async def _load_workspace_entities(
        self, entity_type: str, entity_uuids: list[UUID]
    ) -> dict[tuple[str, UUID], dict[str, Any]]:
        rows = await self._pool.fetch(
            f"""
            SELECT entity.uuid AS entity_uuid, entity.data
            FROM workspace_zulip_bridge.workspace_{entity_type} AS entity
            JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
              ON mirror.provider_uuid = entity.provider_uuid
             AND mirror.active_generation = entity.snapshot_generation
            WHERE entity.provider_uuid = $1
              AND entity.uuid = ANY($2::uuid[])
            """,
            self._provider_uuid,
            entity_uuids,
        )
        return {
            (entity_type, UUID(str(row["entity_uuid"]))): _json_object(row["data"])
            for row in rows
        }

    async def _workspace_reaction_owners(
        self,
        reactions: list[dict[str, Any]],
    ) -> dict[tuple[str, str, str], UUID]:
        if not reactions:
            return {}
        requested = [
            {
                "message_uuid": identity[0],
                "user_uuid": identity[1],
                "emoji_name": identity[2],
            }
            for identity in sorted({_reaction_identity(data) for data in reactions})
        ]
        rows = await self._pool.fetch(
            """
            SELECT entity.uuid, entity.data
            FROM workspace_zulip_bridge.workspace_message_reactions AS entity
            JOIN workspace_zulip_bridge.workspace_mirror_state AS mirror
              ON mirror.provider_uuid = entity.provider_uuid
             AND mirror.active_generation = entity.snapshot_generation
            JOIN jsonb_to_recordset($2::jsonb) AS requested(
                message_uuid text, user_uuid text, emoji_name text
            ) ON requested.message_uuid = entity.data ->> 'message_uuid'
               AND requested.user_uuid = entity.data ->> 'user_uuid'
               AND requested.emoji_name = entity.data ->> 'emoji_name'
            WHERE entity.provider_uuid = $1
            """,
            self._provider_uuid,
            json.dumps(requested, separators=(",", ":")),
        )
        return {
            _reaction_identity(_json_object(row["data"])): UUID(str(row["uuid"]))
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


def _equivalent_entity(
    entity_type: str,
    source: dict[str, Any] | None,
    target: dict[str, Any] | None,
) -> bool:
    if source is None or target is None:
        return source is target
    normalized_source = _normalized_entity(entity_type, source)
    normalized_target = _normalized_entity(entity_type, target)
    if (
        entity_type == "streams"
        and target.get("history_public_to_subscribers") is not None
    ):
        normalized_source["history_public_to_subscribers"] = bool(
            source.get("history_public_to_subscribers", False)
        )
        normalized_target["history_public_to_subscribers"] = bool(
            target["history_public_to_subscribers"]
        )
    return normalized_source == normalized_target


def _normalized_entity(entity_type: str, data: dict[str, Any]) -> dict[str, Any]:
    value = dict(_normalize_timestamps(data))
    if entity_type == "streams":
        # The public Workspace API keeps the historical client field names
        # (``owner`` plus per-user projection counters), while the provider
        # contract uses ``owner_uuid`` and only canonical stream attributes.
        # Compare the shared state, otherwise unread counters make an imported
        # Zulip stream look like a newer Workspace edit and echo it back.
        value = {
            "name": value.get("name"),
            "description": value.get("description") or "",
            "owner_uuid": value.get("owner_uuid")
            or value.get("owner")
            or value.get("user_uuid"),
            "invite_only": bool(value.get("invite_only", False)),
            "announce": bool(value.get("announce", False)),
            "direct_user_uuid": value.get("direct_user_uuid"),
            "private": bool(value.get("private", False)),
            "is_archived": bool(value.get("is_archived", False)),
            "color": value.get("color") or 0,
        }
    elif entity_type == "topics":
        value.pop("color", None)
    elif entity_type == "messages":
        # Workspace returns the public message representation, which also
        # contains its UUID, provider metadata and denormalized reactions.
        # Zulip stores those concerns separately.  Restrict round-trip
        # comparison to the fields owned by the message itself so the Zulip
        # echo of a Workspace-created message converges instead of attempting
        # to create the same native Workspace entity through the provider API.
        value = {
            field: value.get(field)
            for field in (
                "stream_uuid",
                "topic_uuid",
                "author_uuid",
                "payload",
                "created_at",
            )
        }
    elif entity_type == "message_reactions":
        # Zulip's message/reaction snapshots do not expose when a reaction was
        # originally created.  A reload therefore assigns a new ingestion
        # timestamp to the same stable reaction identity.  Workspace also
        # returns compatibility metadata (source, project_id and old_* fields)
        # that is not part of the Zulip reaction.  Comparing either category
        # causes a false Workspace -> Zulip echo and a duplicate-reaction
        # BAD_REQUEST response.
        value = {
            field: value.get(field)
            for field in ("message_uuid", "user_uuid", "emoji_name")
        }
    return value


def _normalize_timestamps(value: Any, key: str = "") -> Any:
    if isinstance(value, dict):
        return {
            item_key: _normalize_timestamps(item, item_key)
            for item_key, item in value.items()
        }
    if isinstance(value, list):
        return [_normalize_timestamps(item) for item in value]
    if isinstance(value, str) and key.endswith("_at"):
        try:
            return _timestamp(value).isoformat().replace("+00:00", "Z")
        except ValueError:
            return value
    return value


_SOURCE_TABLES = {
    "users": {
        "from": "workspace_zulip_bridge.zulip_users",
        "joins": "",
        "where": "source.realm_uuid = $3 AND source.workspace_user_uuid IS NULL",
        "hash": "source.profile_hash",
        "partition": "NULL::uuid",
    },
    "streams": {
        "from": "workspace_zulip_bridge.zulip_streams",
        "joins": """
            LEFT JOIN workspace_zulip_bridge.zulip_connections AS supplier
              ON supplier.uuid = source.source_connection_uuid
            LEFT JOIN workspace_zulip_bridge.zulip_users AS supplier_user
              ON supplier_user.uuid = supplier.zulip_user_uuid
            LEFT JOIN workspace_zulip_bridge.zulip_users AS owner_user
              ON owner_user.uuid = source.owner_user_uuid
            LEFT JOIN workspace_zulip_bridge.zulip_users AS direct_user_source
              ON direct_user_source.uuid = source.direct_user_uuid
        """,
        "where": """
            source.realm_uuid = $3 AND source.source_connection_uuid IS NOT NULL
        """,
        "hash": "source.content_hash",
        "partition": "NULL::uuid",
    },
    "stream_bindings": {
        "from": "workspace_zulip_bridge.zulip_stream_bindings",
        "joins": """
            JOIN workspace_zulip_bridge.zulip_streams AS parent
              ON parent.uuid = source.zulip_stream_uuid
            JOIN workspace_zulip_bridge.zulip_users AS bound_user
              ON bound_user.uuid = source.zulip_user_uuid
        """,
        "where": """
            parent.realm_uuid = $3 AND parent.source_connection_uuid IS NOT NULL
        """,
        "hash": "source.content_hash",
        "partition": "NULL::uuid",
    },
    "topics": {
        "from": "workspace_zulip_bridge.zulip_topics",
        "joins": "JOIN workspace_zulip_bridge.zulip_streams AS parent ON parent.uuid = source.zulip_stream_uuid",
        "where": """
            parent.realm_uuid = $3 AND parent.source_connection_uuid IS NOT NULL
        """,
        "hash": "source.content_hash",
        "partition": "NULL::uuid",
    },
    "topic_bindings": {
        "from": "workspace_zulip_bridge.zulip_topic_bindings",
        "joins": """
            JOIN workspace_zulip_bridge.zulip_streams AS parent
              ON parent.uuid = source.zulip_stream_uuid
            JOIN workspace_zulip_bridge.zulip_users AS bound_user
              ON bound_user.uuid = source.zulip_user_uuid
        """,
        "where": """
            parent.realm_uuid = $3 AND parent.source_connection_uuid IS NOT NULL
        """,
        "hash": "source.content_hash",
        "partition": "NULL::uuid",
    },
    "messages": {
        "from": "workspace_zulip_bridge.zulip_messages",
        "joins": """
            JOIN workspace_zulip_bridge.zulip_users AS sender_user
              ON sender_user.uuid = source.sender_user_uuid
        """,
        "where": """
            source.realm_uuid = $3
        """,
        "hash": "source.content_hash",
        "partition": "source.zulip_stream_uuid",
    },
    "message_flags": {
        "from": "workspace_zulip_bridge.zulip_message_flags",
        "joins": """
            JOIN workspace_zulip_bridge.zulip_users AS flag_user
              ON flag_user.uuid = source.zulip_user_uuid
        """,
        "where": """
            source.realm_uuid = $3
        """,
        "hash": "source.flags_hash",
        "partition": "source.zulip_stream_uuid",
    },
    "message_reactions": {
        "from": "workspace_zulip_bridge.zulip_message_reactions",
        "joins": """
            JOIN workspace_zulip_bridge.zulip_messages AS message
              ON message.uuid = source.message_uuid
            JOIN workspace_zulip_bridge.zulip_users AS reaction_user
              ON reaction_user.uuid = source.zulip_user_uuid
        """,
        "where": """
            source.realm_uuid = $3
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
          AND zulip_user.workspace_user_uuid IS NULL
    """,
    "streams": """
        SELECT stream.uuid AS entity_uuid, jsonb_build_object(
            'name', stream.name, 'description', stream.description,
            'owner_uuid', COALESCE(
                owner_user.workspace_user_uuid, stream.owner_user_uuid,
                connection_user.workspace_user_uuid, connection.zulip_user_uuid
            ),
            'invite_only', stream.invite_only, 'announce', stream.announce,
            'direct_user_uuid', COALESCE(
                direct_user.workspace_user_uuid, stream.direct_user_uuid
            ),
            'private', stream.private, 'is_archived', stream.is_archived,
            'color', COALESCE(stream.color, 0),
            'history_public_to_subscribers',
                COALESCE((stream.chat_parameters ->> 'history_public_to_subscribers')::boolean, true),
            'created_at', stream.created_at
        ) AS data
        FROM workspace_zulip_bridge.zulip_streams AS stream
        LEFT JOIN workspace_zulip_bridge.zulip_connections AS connection
          ON connection.uuid = stream.source_connection_uuid
        LEFT JOIN workspace_zulip_bridge.zulip_users AS connection_user
          ON connection_user.uuid = connection.zulip_user_uuid
        LEFT JOIN workspace_zulip_bridge.zulip_users AS owner_user
          ON owner_user.uuid = stream.owner_user_uuid
        LEFT JOIN workspace_zulip_bridge.zulip_users AS direct_user
          ON direct_user.uuid = stream.direct_user_uuid
        WHERE stream.uuid = ANY($1::uuid[])
    """,
    "stream_bindings": """
        SELECT binding.uuid AS entity_uuid, jsonb_build_object(
            'stream_uuid', binding.zulip_stream_uuid,
            'user_uuid', COALESCE(
                bound_user.workspace_user_uuid, binding.zulip_user_uuid
            ),
            'who_uuid', COALESCE(
                owner_user.workspace_user_uuid, stream.owner_user_uuid,
                connection_user.workspace_user_uuid, connection.zulip_user_uuid
            ),
            'role', binding.role,
            'notification_mode', binding.notification_mode,
            'created_at', binding.created_at
        ) AS data
        FROM workspace_zulip_bridge.zulip_stream_bindings AS binding
        JOIN workspace_zulip_bridge.zulip_streams AS stream
          ON stream.uuid = binding.zulip_stream_uuid
        JOIN workspace_zulip_bridge.zulip_users AS bound_user
          ON bound_user.uuid = binding.zulip_user_uuid
        LEFT JOIN workspace_zulip_bridge.zulip_connections AS connection
          ON connection.uuid = stream.source_connection_uuid
        LEFT JOIN workspace_zulip_bridge.zulip_users AS connection_user
          ON connection_user.uuid = connection.zulip_user_uuid
        LEFT JOIN workspace_zulip_bridge.zulip_users AS owner_user
          ON owner_user.uuid = stream.owner_user_uuid
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
        SELECT binding.uuid AS entity_uuid, jsonb_build_object(
            'stream_uuid', binding.zulip_stream_uuid,
            'topic_uuid', binding.topic_uuid,
            'user_uuid', COALESCE(
                zulip_user.workspace_user_uuid, binding.zulip_user_uuid
            ), 'notification_mode', binding.notification_mode,
            'created_at', binding.created_at
        ) AS data
        FROM workspace_zulip_bridge.zulip_topic_bindings AS binding
        JOIN workspace_zulip_bridge.zulip_users AS zulip_user
          ON zulip_user.uuid = binding.zulip_user_uuid
        WHERE binding.uuid = ANY($1::uuid[])
    """,
    "messages": """
        SELECT message.uuid AS entity_uuid, jsonb_build_object(
            'stream_uuid', message.zulip_stream_uuid,
            'topic_uuid', message.topic_uuid,
            'author_uuid', COALESCE(
                sender.workspace_user_uuid, message.sender_user_uuid
            ),
            'payload', jsonb_build_object(
                'kind', 'markdown', 'content', message.content
            ),
            'created_at', message.created_at
        ) AS data
        FROM workspace_zulip_bridge.zulip_messages AS message
        JOIN workspace_zulip_bridge.zulip_users AS sender
          ON sender.uuid = message.sender_user_uuid
        WHERE message.uuid = ANY($1::uuid[])
    """,
    "message_flags": """
        SELECT flag.uuid AS entity_uuid, jsonb_build_object(
            'stream_uuid', flag.zulip_stream_uuid,
            'message_uuid', flag.message_uuid,
            'user_uuid', COALESCE(
                flag_user.workspace_user_uuid, flag.zulip_user_uuid
            ), 'read', flag.is_read,
            'pinned', false, 'starred', flag.is_starred,
            'mentioned', flag.is_mentioned
        ) AS data
        FROM workspace_zulip_bridge.zulip_message_flags AS flag
        JOIN workspace_zulip_bridge.zulip_users AS flag_user
          ON flag_user.uuid = flag.zulip_user_uuid
        WHERE flag.uuid = ANY($1::uuid[])
    """,
    "message_reactions": """
        SELECT reaction.uuid AS entity_uuid, jsonb_build_object(
            'message_uuid', reaction.message_uuid,
            'user_uuid', COALESCE(
                reaction_user.workspace_user_uuid, reaction.zulip_user_uuid
            ),
            'emoji_name', reaction.emoji_name,
            'created_at', reaction.created_at
        ) AS data
        FROM workspace_zulip_bridge.zulip_message_reactions AS reaction
        JOIN workspace_zulip_bridge.zulip_users AS reaction_user
          ON reaction_user.uuid = reaction.zulip_user_uuid
        WHERE reaction.uuid = ANY($1::uuid[])
    """,
}
