import contextlib
import copy
import dataclasses
import datetime
import hashlib
import json
import threading
import typing
import uuid

from restalchemy.storage.sql import engines, sessions

from workspace_zulip_bridge import canonical, control, emoji

PARTICIPANT_RECHECK_INTERVAL_SECONDS = 3600


def backfill_health_component(account_uuid: str, provider_chat_key: str) -> str:
    return f"provider:{account_uuid}:{provider_chat_key}"


def provider_account_health_component(account_uuid: str) -> str:
    return f"provider:{account_uuid}"


def _same_provider_identity_replay(
    accepted: dict[str, object],
    current: dict[str, object],
) -> bool:
    """Recognize one immutable provider identity after canonical UUID linking."""

    normalized = []
    for record in (accepted, current):
        operation = record.get("operation")
        if (
            not isinstance(operation, dict)
            or operation.get("kind") != "identity.upsert"
        ):
            return False
        entity_uuid = operation.get("entity_uuid")
        account_uuid = record.get("account_uuid")
        if not isinstance(entity_uuid, str) or not isinstance(account_uuid, str):
            return False
        if record.get("causal_lane") != f"identity:{account_uuid}:{entity_uuid}":
            return False
        value = json.loads(json.dumps(record))
        value["operation_sha256"] = ""
        value["causal_lane"] = f"identity:{account_uuid}:<canonical-identity>"
        value["operation"]["entity_uuid"] = "<canonical-identity>"
        normalized.append(value)
    return normalized[0] == normalized[1]


def _merge_catalog_participants(
    current: list[object],
    observed: list[dict[str, object]],
    *,
    authoritative: bool = False,
) -> list[dict[str, object]]:
    """Merge participant facts, optionally replacing the membership set."""
    participants: dict[str, dict[str, object]] = {}
    observed_ids = {
        str(value["provider_user_id"])
        for value in observed
        if value.get("provider_user_id") is not None
    }
    for value in current:
        if not isinstance(value, dict) or value.get("provider_user_id") is None:
            continue
        provider_user_id = str(value["provider_user_id"])
        if authoritative and provider_user_id not in observed_ids:
            continue
        participants[provider_user_id] = dict(value)
    for value in observed:
        if value.get("provider_user_id") is None:
            continue
        provider_user_id = str(value["provider_user_id"])
        prior = participants.get(provider_user_id)
        if prior is None:
            participants[provider_user_id] = dict(value)
            continue
        merged = dict(prior)
        merged["is_owner"] = bool(prior.get("is_owner")) or bool(value.get("is_owner"))
        for name in ("email", "avatar_urn"):
            if not merged.get(name) and value.get(name):
                merged[name] = value[name]
        prior_name = str(prior.get("display_name", "")).strip()
        observed_name = str(value.get("display_name", "")).strip()
        if (
            authoritative
            and observed_name
            and observed_name != provider_user_id
        ):
            merged["display_name"] = observed_name
        elif (not prior_name or prior_name == provider_user_id) and observed_name:
            merged["display_name"] = observed_name
        participants[provider_user_id] = merged
    return [participants[key] for key in sorted(participants)]


def _validate_required_capabilities(value: object) -> None:
    if not isinstance(value, dict):
        raise ValueError("Desired resource capability requirements are invalid")
    for name, requirement in value.items():
        supported = control.CAPABILITIES.get(name)
        if supported is None or not isinstance(requirement, dict):
            raise ValueError("Desired resource requires an unsupported capability")
        minimum = requirement.get("min_revision")
        limits = requirement.get("limits")
        if (
            not isinstance(minimum, int)
            or isinstance(minimum, bool)
            or minimum < 1
            or minimum > supported["revision"]
            or not isinstance(limits, dict)
        ):
            raise ValueError("Desired resource requires an unsupported capability")
        supported_limits = typing.cast(dict[str, object], supported["limits"])
        if any(supported_limits.get(key) != limit for key, limit in limits.items()):
            raise ValueError("Desired resource requires an unsupported capability")


def _validate_desired_upsert(
    resource_type: object,
    resource_uuid: object,
    generation: object,
    required_capabilities: object,
    resource: object,
) -> None:
    if resource_type not in control.RESOURCE_TYPES or not isinstance(resource, dict):
        raise ValueError("Unsupported desired-state resource type")
    if (
        resource.get("resource_type") != resource_type
        or uuid.UUID(str(resource.get("uuid"))) != uuid.UUID(str(resource_uuid))
        or not isinstance(generation, int)
        or isinstance(generation, bool)
        or generation < 1
        or resource.get("generation") != generation
    ):
        raise ValueError("Desired resource identity or generation mismatch")
    _validate_required_capabilities(required_capabilities)


def _validate_desired_delete(
    resource_type: object,
    resource_uuid: object,
    generation: object,
) -> None:
    if resource_type not in control.RESOURCE_TYPES:
        raise ValueError("Unsupported desired-state resource type")
    try:
        uuid.UUID(str(resource_uuid))
    except (TypeError, ValueError) as exc:
        raise ValueError("Desired resource identity is invalid") from exc
    if (
        not isinstance(generation, int)
        or isinstance(generation, bool)
        or generation < 1
    ):
        raise ValueError("Desired resource generation is invalid")


def _validated_snapshot_resource(resource: object) -> dict[str, object]:
    if not isinstance(resource, dict):
        raise ValueError("Desired snapshot resource is invalid")
    _validate_desired_upsert(
        resource.get("resource_type"),
        resource.get("uuid"),
        resource.get("generation"),
        resource.get("required_capabilities"),
        resource,
    )
    return {
        key: value for key, value in resource.items() if key != "required_capabilities"
    }


@dataclasses.dataclass(frozen=True)
class QueuedOperation:
    record_uuid: uuid.UUID
    record: dict[str, object]
    priority: int
    attempts: int = 0
    provider_attempted_at: datetime.datetime | None = None
    auto_resend_count: int = 0
    reconciliation_check_count: int = 0
    provider_rendered_content: str | None = None


class QueueStore(typing.Protocol):
    def enqueue(self, record: dict[str, object], priority: int) -> bool: ...

    def claim(
        self, worker_id: str, lease_seconds: int = 60
    ) -> QueuedOperation | None: ...

    def claim_terminal(
        self, worker_id: str, lease_seconds: int = 60
    ) -> tuple[QueuedOperation, str] | None: ...

    def reap_expired_running(self) -> int: ...

    def complete(
        self, item: QueuedOperation, result: dict[str, object], outcome: str
    ) -> None: ...

    def retry(
        self, item: QueuedOperation, available_at: datetime.datetime, code: str
    ) -> None: ...

    def record_provider_attempt(
        self,
        item: QueuedOperation,
        queue_id: str,
        local_id: str,
        last_event_id: int,
        provider_rendered_content: str,
    ) -> None: ...

    def mark_uncertain(self, item: QueuedOperation, code: str) -> None: ...

    def defer_uncertain(
        self, item: QueuedOperation, available_at: datetime.datetime, code: str
    ) -> None: ...

    def provider_event_cursor(self, account_uuid: str) -> dict[str, object] | None: ...

    def update_provider_event_cursor(
        self,
        account_uuid: str,
        queue_id: str,
        last_event_id: int,
        provider_realm_uuid: str | None = None,
        provider_owner_user_id: str | None = None,
        provider_account_generation: int | None = None,
    ) -> None: ...

    def record_provider_event(
        self, account_uuid: str, queue_id: str, event: dict[str, object]
    ) -> bool: ...

    def invalidate_provider_event_cursor(self, account_uuid: str) -> None: ...

    def uncertain_by_local_id(
        self, account_uuid: str, queue_id: str, local_id: str
    ) -> QueuedOperation | None: ...

    def require_manual_reconciliation(self, account_uuid: str, code: str) -> None: ...

    def claim_uncertain(self, worker_id: str) -> QueuedOperation | None: ...

    def schedule_reconciliation_check(
        self,
        item: QueuedOperation,
        after: datetime.datetime,
        evidence: dict[str, object],
    ) -> None: ...

    def schedule_single_resend(
        self, item: QueuedOperation, evidence: dict[str, object]
    ) -> None: ...

    def require_operation_manual_reconciliation(
        self, item: QueuedOperation, code: str, evidence: dict[str, object]
    ) -> None: ...

    def pending_results(self, limit: int = 100) -> list[dict[str, object]]: ...

    def mark_result_sent(self, record_uuid: str) -> None: ...

    def finalize_provider_result_response(
        self, record_uuid: str, status: str
    ) -> None: ...


_ENGINE_LOCK = threading.Lock()
_ENGINE_POOL_CONFIG = {"min_size": 1, "max_size": 20}


def _engine_for(connection_url: str) -> engines.AbstractEngine:
    engine_name = (
        "workspace_zulip_bridge_"
        + hashlib.sha256(connection_url.encode("utf-8")).hexdigest()
    )
    with _ENGINE_LOCK:
        try:
            return engines.engine_factory.get_engine(engine_name)
        except ValueError:
            engines.engine_factory.configure_factory(
                db_url=connection_url,
                config=_ENGINE_POOL_CONFIG,
                name=engine_name,
            )
            return engines.engine_factory.get_engine(engine_name)


class RestAlchemyStore:
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self._transaction_state = threading.local()

    @contextlib.contextmanager
    def session(self) -> typing.Iterator[sessions.PgSQLSession]:
        current = getattr(self._transaction_state, "session", None)
        if current is not None:
            yield current
            return
        with _engine_for(self.connection_url).session_manager() as session:
            self._transaction_state.session = session
            try:
                yield session
            finally:
                del self._transaction_state.session

    @contextlib.contextmanager
    def transaction(self) -> typing.Iterator[sessions.PgSQLSession]:
        """Reuse one database transaction for nested store operations."""
        with self.session() as session:
            yield session

    def control_cursor(self) -> str:
        with self.session() as session:
            row = session.execute(
                "SELECT control_cursor FROM bridge_metadata WHERE singleton"
            ).fetchone()
            return str(row["control_cursor"])

    def blocked_batch(self) -> dict[str, object] | None:
        with self.session() as session:
            row = session.execute(
                "SELECT blocked_batch FROM bridge_metadata WHERE singleton"
            ).fetchone()
            return (
                None
                if row is None or row["blocked_batch"] is None
                else typing.cast(dict[str, object], row["blocked_batch"])
            )

    def set_blocked_batch(self, cursor: str, next_cursor: str, code: str) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_metadata
                SET blocked_batch = %s, updated_at = now()
                WHERE singleton
                """,
                (
                    json.dumps(
                        {
                            "cursor": cursor,
                            "next_cursor": next_cursor,
                            "safe_error": {
                                "code": code,
                                "message": "Desired-state batch is not compatible with this bridge image.",
                            },
                        }
                    ),
                ),
            )

    def clear_blocked_batch(self) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_metadata
                SET blocked_batch = NULL, updated_at = now()
                WHERE singleton
                """
            )

    def merge_catalog_topology(
        self,
        account_uuid: str,
        provider_chat_key: str,
        participants: list[dict[str, object]],
        topics: list[dict[str, object]],
        *,
        authoritative_participants: bool = False,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        """Merge a catalog view transactionally."""
        with self.session() as session:
            row = session.execute(
                """
                SELECT participants, topics FROM external_chat_catalog_state
                WHERE account_uuid = %s AND provider_chat_key = %s
                FOR UPDATE
                """,
                (account_uuid, provider_chat_key),
            ).fetchone()
            old_participants = [] if row is None else row["participants"]
            old_topics = [] if row is None else row["topics"]
            topic_map = {
                str(value["provider_topic_id"]): value
                for value in [*old_topics, *topics]
                if isinstance(value, dict)
                and value.get("provider_topic_id") is not None
            }
            merged_participants = _merge_catalog_participants(
                old_participants,
                participants,
                authoritative=authoritative_participants,
            )
            merged_topics = [topic_map[key] for key in sorted(topic_map)]
            session.execute(
                """
                INSERT INTO external_chat_catalog_state (
                    account_uuid, provider_chat_key, participants, topics
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (account_uuid, provider_chat_key) DO UPDATE SET
                    participants = EXCLUDED.participants,
                    topics = EXCLUDED.topics,
                    updated_at = now()
                """,
                (
                    account_uuid,
                    provider_chat_key,
                    json.dumps(merged_participants),
                    json.dumps(merged_topics),
                ),
            )
            return merged_participants, merged_topics

    def delete_catalog_topology(
        self, account_uuid: str, provider_chat_key: str
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                DELETE FROM external_chat_catalog_state
                WHERE account_uuid = %s AND provider_chat_key = %s
                """,
                (account_uuid, provider_chat_key),
            )

    @staticmethod
    def _reconcile_provider_account_generation(
        session: sessions.PgSQLSession,
        account_uuid: str,
        generation: int,
    ) -> None:
        """Reset only a breaker that belongs to an older desired generation."""
        changed = session.execute(
            """
            INSERT INTO scheduler_accounts (
                account_uuid, provider_generation, provider_state,
                provider_retry_count, provider_retry_after,
                provider_error_code, provider_state_updated_at
            ) VALUES (%s, %s, 'ready', 0, NULL, NULL, now())
            ON CONFLICT (account_uuid) DO UPDATE SET
                provider_generation = EXCLUDED.provider_generation,
                provider_state = 'ready',
                provider_retry_count = 0,
                provider_retry_after = NULL,
                provider_error_code = NULL,
                provider_state_updated_at = now()
            WHERE scheduler_accounts.provider_generation IS DISTINCT FROM
                  EXCLUDED.provider_generation
            RETURNING account_uuid
            """,
            (account_uuid, generation),
        ).fetchone()
        if changed is None:
            return
        session.execute(
            """
            UPDATE zulip_backfill_jobs
            SET state = 'pending', lease_until = NULL, available_at = now(),
                retry_count = 0, last_error_code = NULL, updated_at = now()
            WHERE account_uuid = %s AND state = 'failed'
              AND last_error_code IN ('unauthorized', 'unauthorized_account')
            """,
            (account_uuid,),
        )
        session.execute(
            "DELETE FROM bridge_health WHERE component = %s",
            (provider_account_health_component(account_uuid),),
        )

    def apply_desired_changes(
        self, changes: list[dict[str, object]], next_cursor: str
    ) -> None:
        for change in changes:
            operation = change.get("operation")
            resource_type = change.get("resource_type")
            if resource_type not in control.RESOURCE_TYPES:
                raise ValueError("Unsupported desired-state resource type")
            if operation == "upsert":
                _validate_desired_upsert(
                    resource_type,
                    change.get("resource_uuid"),
                    change.get("generation"),
                    change.get("required_capabilities"),
                    change.get("resource"),
                )
            elif operation == "delete":
                _validate_desired_delete(
                    resource_type,
                    change.get("resource_uuid"),
                    change.get("generation"),
                )
            else:
                raise ValueError("Unsupported desired-state operation")
        with self.session() as session:
            for change in changes:
                resource_type = str(change["resource_type"])
                resource_uuid = str(change["resource_uuid"])
                generation = int(change["generation"])
                operation = str(change["operation"])
                if resource_type not in control.RESOURCE_TYPES:
                    raise ValueError("Unsupported desired-state resource type")
                if operation not in {"upsert", "delete"}:
                    raise ValueError("Unsupported desired-state operation")
                body = change.get("resource") if operation == "upsert" else None
                previous = session.execute(
                    """
                    SELECT body FROM desired_resources
                    WHERE resource_type = %s AND resource_uuid = %s
                    """,
                    (resource_type, resource_uuid),
                ).fetchone()
                applied = session.execute(
                    """
                    INSERT INTO desired_resources (
                        resource_type, resource_uuid, generation, body, deleted
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (resource_type, resource_uuid) DO UPDATE SET
                        generation = EXCLUDED.generation,
                        body = EXCLUDED.body,
                        deleted = EXCLUDED.deleted,
                        updated_at = now()
                    WHERE desired_resources.generation < EXCLUDED.generation
                    RETURNING body, deleted
                    """,
                    (
                        resource_type,
                        resource_uuid,
                        generation,
                        json.dumps(body) if body is not None else None,
                        operation == "delete",
                    ),
                ).fetchone()
                if applied is None:
                    continue
                if resource_type == "external_account":
                    session.execute(
                        """
                        DELETE FROM zulip_event_cursors
                        WHERE account_uuid = %s
                        """,
                        (resource_uuid,),
                    )
                    if operation == "upsert":
                        self._reconcile_provider_account_generation(
                            session,
                            resource_uuid,
                            generation,
                        )
                if resource_type == "external_chat_assignment":
                    if operation == "upsert":
                        if (
                            previous is not None
                            and previous["body"] is not None
                            and not self._assignment_projection_is_additive(
                                typing.cast(dict[str, object], previous["body"]),
                                typing.cast(dict[str, object], body),
                            )
                        ):
                            self._tombstone_workspace_projection(
                                session,
                                typing.cast(dict[str, object], previous["body"]),
                            )
                        self._materialize_workspace_projection(
                            session, typing.cast(dict[str, object], body)
                        )
                        self._wake_assignment_pending_events(
                            session,
                            str(
                                typing.cast(dict[str, object], body)[
                                    "external_account_uuid"
                                ]
                            ),
                        )
                    elif previous is not None and previous["body"] is not None:
                        self._tombstone_workspace_projection(
                            session,
                            typing.cast(dict[str, object], previous["body"]),
                        )
            session.execute(
                """
                UPDATE bridge_metadata
                SET control_cursor = %s, updated_at = now()
                WHERE singleton
                """,
                (next_cursor,),
            )

    def install_snapshot(
        self, resources: list[dict[str, object]], anchor_cursor: str
    ) -> None:
        validated = [_validated_snapshot_resource(resource) for resource in resources]
        with self.session() as session:
            session.execute(
                """
                UPDATE provider_mappings
                SET deleted = true, updated_at = now()
                WHERE entity_kind IN ('identity', 'stream', 'topic') AND NOT deleted
                """
            )
            session.execute(
                """
                UPDATE provider_mapping_aliases
                SET deleted = true, updated_at = now()
                WHERE entity_kind = 'topic' AND NOT deleted
                """
            )
            session.execute("DELETE FROM desired_resources")
            for resource in validated:
                session.execute(
                    """
                    INSERT INTO desired_resources (
                        resource_type, resource_uuid, generation, body, deleted
                    ) VALUES (%s, %s, %s, %s, false)
                    """,
                    (
                        str(resource["resource_type"]),
                        str(resource["uuid"]),
                        int(resource["generation"]),
                        json.dumps(resource),
                    ),
                )
                if resource["resource_type"] == "external_chat_assignment":
                    self._materialize_workspace_projection(session, resource)
                    self._wake_assignment_pending_events(
                        session,
                        str(resource["external_account_uuid"]),
                    )
                elif resource["resource_type"] == "external_account":
                    self._reconcile_provider_account_generation(
                        session,
                        str(resource["uuid"]),
                        int(resource["generation"]),
                    )
            session.execute(
                """
                DELETE FROM zulip_event_cursors AS cursor
                WHERE cursor.provider_account_generation IS NULL
                   OR NOT EXISTS (
                        SELECT 1
                        FROM desired_resources AS account
                        WHERE account.resource_type = 'external_account'
                          AND account.resource_uuid = cursor.account_uuid
                          AND NOT account.deleted
                          AND account.generation =
                              cursor.provider_account_generation
                   )
                """
            )
            session.execute(
                """
                UPDATE bridge_metadata SET control_cursor = %s, updated_at = now()
                WHERE singleton
                """,
                (anchor_cursor,),
            )

    @staticmethod
    def _replace_projection_mapping(
        session: sessions.PgSQLSession,
        account_uuid: str,
        entity_kind: str,
        workspace_uuid: str,
        provider_id: str,
        metadata: dict[str, object],
        authoritative_workspace_uuids: list[str] | None = None,
    ) -> None:
        session.execute(
            """
            DELETE FROM provider_mappings
            WHERE account_uuid = %s AND entity_kind = %s
              AND workspace_uuid = %s AND provider_id <> %s
            """,
            (account_uuid, entity_kind, workspace_uuid, provider_id),
        )
        if entity_kind == "topic":
            authoritative_workspace_uuids = list(
                dict.fromkeys([*(authoritative_workspace_uuids or []), workspace_uuid])
            )
            # An alias only represents a displaced Workspace UUID. Once that
            # UUID appears in the current projection it is authoritative again,
            # so an older redirect must not continue to override its mapping.
            session.execute(
                """
                UPDATE provider_mapping_aliases
                SET deleted = true, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = %s
                  AND workspace_uuid = %s AND provider_id <> %s
                  AND NOT deleted
                """,
                (account_uuid, entity_kind, workspace_uuid, provider_id),
            )
            # A provider rename can make the backend recanonicalize the same
            # provider topic under a new Workspace UUID. Keep the displaced
            # UUID routable instead of silently orphaning its existing topic.
            session.execute(
                """
                UPDATE provider_mapping_aliases
                SET metadata = %s, deleted = false, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = %s
                  AND provider_id = %s AND workspace_uuid <> %s
                  AND NOT (workspace_uuid = ANY(%s))
                """,
                (
                    json.dumps(metadata),
                    account_uuid,
                    entity_kind,
                    provider_id,
                    workspace_uuid,
                    authoritative_workspace_uuids,
                ),
            )
            session.execute(
                """
                INSERT INTO provider_mapping_aliases (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    metadata, deleted
                )
                SELECT mapping.account_uuid, mapping.entity_kind,
                       mapping.workspace_uuid, %s, %s, false
                FROM provider_mappings AS mapping
                WHERE mapping.account_uuid = %s
                  AND mapping.entity_kind = %s
                  AND mapping.provider_id = %s
                  AND mapping.workspace_uuid <> %s
                  AND NOT (mapping.workspace_uuid = ANY(%s))
                ON CONFLICT (
                    account_uuid, entity_kind, workspace_uuid
                ) DO UPDATE SET
                    provider_id = EXCLUDED.provider_id,
                    metadata = EXCLUDED.metadata,
                    deleted = false, updated_at = now()
                """,
                (
                    provider_id,
                    json.dumps(metadata),
                    account_uuid,
                    entity_kind,
                    provider_id,
                    workspace_uuid,
                    authoritative_workspace_uuids,
                ),
            )
        session.execute(
            """
            INSERT INTO provider_mappings (
                account_uuid, entity_kind, workspace_uuid, provider_id,
                metadata, deleted
            ) VALUES (%s, %s, %s, %s, %s, false)
            ON CONFLICT (account_uuid, entity_kind, provider_id) DO UPDATE SET
                workspace_uuid = EXCLUDED.workspace_uuid,
                metadata = EXCLUDED.metadata,
                deleted = false, updated_at = now()
            """,
            (
                account_uuid,
                entity_kind,
                workspace_uuid,
                provider_id,
                json.dumps(metadata),
            ),
        )

    @staticmethod
    def _materialize_workspace_projection(
        session: sessions.PgSQLSession,
        assignment: dict[str, object],
    ) -> None:
        projection = assignment.get("workspace_projection")
        provider_chat = assignment.get("provider_chat")
        if not isinstance(projection, dict) or not isinstance(provider_chat, dict):
            return
        account_uuid = str(assignment["external_account_uuid"])
        project_uuid = str(assignment["project_id"])
        chat_key = str(provider_chat["provider_chat_key"])
        stream = projection.get("stream")
        participants = projection.get("participants")
        topics = projection.get("topics")
        if (
            not isinstance(stream, dict)
            or not isinstance(participants, list)
            or not isinstance(topics, list)
        ):
            raise ValueError("Invalid workspace projection mapping")
        participant_uuids: list[str] = []
        for raw_participant in participants:
            if not isinstance(raw_participant, dict):
                raise ValueError("Invalid workspace projection participant")
            identity_uuid = str(raw_participant["identity_uuid"])
            participant_uuids.append(identity_uuid)
            RestAlchemyStore._replace_projection_mapping(
                session,
                account_uuid,
                "identity",
                identity_uuid,
                str(raw_participant["provider_user_id"]),
                {
                    "display_name": raw_participant["display_name"],
                    "email": raw_participant.get("email"),
                    "avatar_urn": raw_participant.get("avatar_urn"),
                    "active": True,
                    "role": raw_participant["role"],
                },
            )
        stream_uuid = str(stream["uuid"])
        RestAlchemyStore._replace_projection_mapping(
            session,
            account_uuid,
            "stream",
            stream_uuid,
            chat_key,
            {
                "chat_type": provider_chat["chat_type"],
                "project_uuid": project_uuid,
                "participants": participant_uuids,
                "name": stream["name"],
                "description": stream["description"],
                "private": stream["private"],
                "default_topic_uuid": stream.get("default_topic_uuid"),
            },
        )
        authoritative_topic_uuids = [
            str(raw_topic["topic_uuid"])
            for raw_topic in topics
            if isinstance(raw_topic, dict)
        ]
        for raw_topic in topics:
            if not isinstance(raw_topic, dict):
                raise ValueError("Invalid workspace projection topic")
            RestAlchemyStore._replace_projection_mapping(
                session,
                account_uuid,
                "topic",
                str(raw_topic["topic_uuid"]),
                str(raw_topic["provider_topic_id"]),
                {
                    "stream_uuid": stream_uuid,
                    "chat_key": chat_key,
                    "name": raw_topic["name"],
                    "is_default": raw_topic["is_default"],
                },
                authoritative_topic_uuids,
            )

    @staticmethod
    def _wake_assignment_pending_events(
        session: sessions.PgSQLSession,
        account_uuid: str,
    ) -> None:
        """Wake journal events after their projection mapping arrives."""
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = now()
            WHERE account_uuid = %s
              AND processing_state = 'pending'
              AND processing_reason = 'provider_chat_assignment_pending'
            """,
            (account_uuid,),
        )

    @staticmethod
    def _assignment_projection_keys(
        assignment: dict[str, object],
    ) -> tuple[tuple[str, str] | None, set[tuple[str, str]], set[tuple[str, str]]]:
        projection = assignment.get("workspace_projection")
        provider_chat = assignment.get("provider_chat")
        if not isinstance(projection, dict) or not isinstance(provider_chat, dict):
            return None, set(), set()
        stream = projection.get("stream")
        stream_key = (
            None
            if not isinstance(stream, dict)
            or stream.get("uuid") is None
            or provider_chat.get("provider_chat_key") is None
            else (
                str(stream["uuid"]),
                str(provider_chat["provider_chat_key"]),
            )
        )
        participant_keys = {
            (str(participant["identity_uuid"]), str(participant["provider_user_id"]))
            for participant in projection.get("participants", [])
            if isinstance(participant, dict)
            and participant.get("identity_uuid") is not None
            and participant.get("provider_user_id") is not None
        }
        topic_keys = {
            (str(topic["topic_uuid"]), str(topic["provider_topic_id"]))
            for topic in projection.get("topics", [])
            if isinstance(topic, dict)
            and topic.get("topic_uuid") is not None
            and topic.get("provider_topic_id") is not None
        }
        return stream_key, participant_keys, topic_keys

    @classmethod
    def _assignment_projection_is_additive(
        cls,
        previous: dict[str, object],
        current: dict[str, object],
    ) -> bool:
        """Return true when materialization cannot leave stale mappings behind."""
        previous_stream, previous_participants, previous_topics = (
            cls._assignment_projection_keys(previous)
        )
        current_stream, current_participants, current_topics = (
            cls._assignment_projection_keys(current)
        )
        return (
            previous_stream is not None
            and previous_stream == current_stream
            and previous_participants <= current_participants
            and previous_topics <= current_topics
        )

    @staticmethod
    def _tombstone_workspace_projection(
        session: sessions.PgSQLSession,
        assignment: dict[str, object],
    ) -> None:
        projection = assignment.get("workspace_projection")
        if not isinstance(projection, dict):
            return
        stream = projection.get("stream")
        if not isinstance(stream, dict):
            return
        participants = projection.get("participants", [])
        participant_uuids = [
            str(participant["identity_uuid"])
            for participant in participants
            if isinstance(participant, dict) and participant.get("identity_uuid")
        ]
        topics = projection.get("topics", [])
        topic_uuids = [
            str(topic["topic_uuid"])
            for topic in topics
            if isinstance(topic, dict) and topic.get("topic_uuid")
        ]
        session.execute(
            """
            UPDATE provider_mappings
            SET deleted = true, updated_at = now()
            WHERE account_uuid = %s
              AND (
                  (entity_kind = 'stream' AND workspace_uuid = %s)
                  OR (
                      entity_kind = 'topic' AND workspace_uuid = ANY(%s)
                  )
                  OR (
                      entity_kind = 'identity' AND workspace_uuid = ANY(%s)
                      AND NOT EXISTS (
                          SELECT 1
                          FROM desired_resources AS other_assignment,
                               jsonb_array_elements(
                                   other_assignment.body->'workspace_projection'
                                       ->'participants'
                               ) AS participant
                          WHERE other_assignment.resource_type =
                                'external_chat_assignment'
                            AND NOT other_assignment.deleted
                            AND other_assignment.body->>'external_account_uuid' =
                                provider_mappings.account_uuid::text
                            AND participant->>'identity_uuid' =
                                provider_mappings.workspace_uuid::text
                      )
                  )
              )
            """,
            (
                str(assignment["external_account_uuid"]),
                str(stream["uuid"]),
                topic_uuids,
                participant_uuids,
            ),
        )
        session.execute(
            """
            UPDATE provider_mapping_aliases
            SET deleted = true, updated_at = now()
            WHERE account_uuid = %s AND entity_kind = 'topic'
              AND (
                  workspace_uuid = ANY(%s)
                  OR metadata->>'stream_uuid' = %s
              )
            """,
            (
                str(assignment["external_account_uuid"]),
                topic_uuids,
                str(stream["uuid"]),
            ),
        )

    def desired_resource(
        self, resource_type: str, resource_uuid: str
    ) -> dict[str, object] | None:
        with self.session() as session:
            row = session.execute(
                """
                SELECT body FROM desired_resources
                WHERE resource_type = %s AND resource_uuid = %s AND NOT deleted
                """,
                (resource_type, resource_uuid),
            ).fetchone()
            return None if row is None else typing.cast(dict[str, object], row["body"])

    def account_settings(self, account_uuid: str) -> dict[str, object] | None:
        resource = self.account_resource(account_uuid)
        if resource is None:
            return None
        return typing.cast(dict[str, object], resource["settings"])

    def account_resource(self, account_uuid: str) -> dict[str, object] | None:
        return self.desired_resource("external_account", account_uuid)

    def provider_policy(self, provider_kind: str = "zulip") -> dict[str, object] | None:
        with self.session() as session:
            row = session.execute(
                """
                SELECT body FROM desired_resources
                WHERE resource_type = 'external_provider_policy'
                  AND NOT deleted AND body->>'provider_kind' = %s
                ORDER BY generation DESC LIMIT 1
                """,
                (provider_kind,),
            ).fetchone()
            return None if row is None else typing.cast(dict[str, object], row["body"])

    def provider_is_enabled(self, provider_kind: str = "zulip") -> bool:
        policy = self.provider_policy(provider_kind)
        return (
            policy is not None
            and policy.get("enabled") is True
            and policy.get("emergency_suspended") is not True
        )

    def account_is_active(self, account_uuid: str) -> bool:
        account = self.account_resource(account_uuid)
        return (
            self.provider_is_enabled("zulip")
            and account is not None
            and account.get("synchronization_enabled") is True
        )

    def custom_ca_bundle(
        self, provider_kind: str = "zulip"
    ) -> dict[str, object] | None:
        policy = self.provider_policy(provider_kind)
        if policy is None or policy.get("custom_ca_bundle_uuid") is None:
            return None
        return self.desired_resource(
            "custom_ca_bundle", str(policy["custom_ca_bundle_uuid"])
        )

    def effective_file_limit(self, hard_limit: int) -> int:
        policy = self.provider_policy("zulip")
        if policy is None:
            return 0
        limits = policy.get("limits")
        if not isinstance(limits, dict):
            return 0
        configured = limits.get("max_file_bytes")
        if not isinstance(configured, int) or isinstance(configured, bool):
            return 0
        return max(0, min(hard_limit, configured))

    def assignment_for_provider_chat(
        self, account_uuid: str, provider_chat_key: str
    ) -> dict[str, object] | None:
        with self.session() as session:
            row = session.execute(
                """
                SELECT body FROM desired_resources
                WHERE resource_type = 'external_chat_assignment'
                  AND NOT deleted
                  AND body->>'external_account_uuid' = %s
                  AND body->'provider_chat'->>'provider_chat_key' = %s
                LIMIT 1
                """,
                (account_uuid, provider_chat_key),
            ).fetchone()
            return None if row is None else typing.cast(dict[str, object], row["body"])

    def reconcile_assignment_projection(
        self, account_uuid: str, provider_chat_key: str
    ) -> bool:
        """Rebuild local mappings from the latest durable assignment."""
        with self.session() as session:
            row = session.execute(
                """
                SELECT body FROM desired_resources
                WHERE resource_type = 'external_chat_assignment'
                  AND NOT deleted
                  AND body->>'external_account_uuid' = %s
                  AND body->'provider_chat'->>'provider_chat_key' = %s
                LIMIT 1
                """,
                (account_uuid, provider_chat_key),
            ).fetchone()
            if row is None:
                return False
            self._materialize_workspace_projection(
                session, typing.cast(dict[str, object], row["body"])
            )
            self._wake_assignment_pending_events(session, account_uuid)
            return True

    def assignments_needing_live_report(
        self, account_uuid: str
    ) -> list[dict[str, object]]:
        with self.session() as session:
            rows = session.execute(
                """
                SELECT assignment.body
                FROM desired_resources AS assignment
                JOIN zulip_backfill_jobs AS job
                  ON job.account_uuid::text =
                     assignment.body->>'external_account_uuid'
                 AND job.provider_chat_key =
                     assignment.body->'provider_chat'->>'provider_chat_key'
                 AND job.state = 'complete'
                WHERE assignment.resource_type = 'external_chat_assignment'
                  AND NOT assignment.deleted
                  AND assignment.body->>'external_account_uuid' = %s
                  AND COALESCE(
                      (assignment.body->>'selected')::boolean, true
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM observed_report_outbox AS report
                      WHERE report.body->>'resource_type' =
                            'external_chat_assignment'
                        AND report.body->>'resource_uuid' =
                            assignment.resource_uuid::text
                        AND (
                            report.body->>'observed_generation'
                        )::bigint = assignment.generation
                        AND report.body->>'status' = 'live_ready'
                        AND (
                            report.result_status IS NULL
                            OR report.result_status IN ('applied', 'duplicate')
                        )
                  )
                ORDER BY assignment.resource_uuid
                """,
                (account_uuid,),
            ).fetchall()
            return [typing.cast(dict[str, object], row["body"]) for row in rows]

    def provider_mapping(
        self, account_uuid: str, entity_kind: str, provider_id: str
    ) -> dict[str, object] | None:
        with self.session() as session:
            return session.execute(
                """
                SELECT mapping.workspace_uuid, mapping.provider_id,
                       mapping.provider_revision, mapping.metadata,
                       EXISTS (
                           SELECT 1 FROM provider_mapping_aliases AS alias
                           WHERE alias.account_uuid = mapping.account_uuid
                             AND alias.entity_kind = mapping.entity_kind
                             AND alias.workspace_uuid = mapping.workspace_uuid
                             AND alias.provider_id = mapping.provider_id
                             AND NOT alias.deleted
                       ) AS convergent_alias
                FROM provider_mappings AS mapping
                WHERE mapping.account_uuid = %s AND mapping.entity_kind = %s
                  AND mapping.provider_id = %s AND NOT mapping.deleted
                """,
                (account_uuid, entity_kind, provider_id),
            ).fetchone()

    def provider_message_mapping(
        self,
        account_uuid: str,
        provider_id: str,
    ) -> dict[str, object] | None:
        """Include a tombstone only while a later recreation is nonterminal."""
        with self.session() as session:
            return session.execute(
                """
                SELECT mapping.workspace_uuid, mapping.provider_id,
                       mapping.provider_revision, mapping.metadata,
                       mapping.deleted AS pending_tombstone,
                       EXISTS (
                           SELECT 1 FROM provider_mapping_aliases AS alias
                           WHERE alias.account_uuid = mapping.account_uuid
                             AND alias.entity_kind = mapping.entity_kind
                             AND alias.workspace_uuid = mapping.workspace_uuid
                             AND alias.provider_id = mapping.provider_id
                             AND NOT alias.deleted
                       ) AS convergent_alias
                FROM provider_mappings AS mapping
                WHERE mapping.account_uuid = %s
                  AND mapping.entity_kind = 'message'
                  AND mapping.provider_id = %s
                  AND (
                      NOT mapping.deleted
                      OR EXISTS (
                          SELECT 1
                          FROM zulip_provider_events AS event
                          WHERE event.account_uuid = mapping.account_uuid
                            AND event.processing_state IN (
                                'pending', 'delivering'
                            )
                            AND event.provider_message_context->>'context_kind' =
                                'pending_delete_recreations'
                            AND event.provider_message_context->'messages' ?
                                mapping.provider_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM workspace_delivery_outbox AS delivery
                          JOIN operation_idempotency AS operation
                            ON operation.operation_uuid = delivery.operation_uuid
                          WHERE delivery.account_uuid = mapping.account_uuid
                            AND delivery.record->'operation'->>'entity_uuid' =
                                mapping.workspace_uuid::text
                            AND delivery.record->'operation'->>'kind' =
                                'message.create'
                            AND operation.terminal_outcome IS NULL
                      )
                  )
                """,
                (account_uuid, provider_id),
            ).fetchone()

    def provider_message_tombstone(
        self,
        account_uuid: str,
        provider_id: str,
    ) -> dict[str, object] | None:
        """Return a committed message tombstone for an explicit recreation."""
        with self.session() as session:
            return session.execute(
                """
                SELECT workspace_uuid, provider_id, provider_revision, metadata,
                       true AS pending_tombstone, false AS convergent_alias
                FROM provider_mappings
                WHERE account_uuid = %s AND entity_kind = 'message'
                  AND provider_id = %s AND deleted
                """,
                (account_uuid, provider_id),
            ).fetchone()

    def provider_mapping_by_name(
        self, account_uuid: str, entity_kind: str, name: str
    ) -> dict[str, object] | None:
        with self.session() as session:
            return session.execute(
                """
                SELECT mapping.workspace_uuid, mapping.provider_id,
                       mapping.provider_revision, mapping.metadata,
                       EXISTS (
                           SELECT 1 FROM provider_mapping_aliases AS alias
                           WHERE alias.account_uuid = mapping.account_uuid
                             AND alias.entity_kind = mapping.entity_kind
                             AND alias.workspace_uuid = mapping.workspace_uuid
                             AND alias.provider_id = mapping.provider_id
                             AND NOT alias.deleted
                       ) AS convergent_alias
                FROM provider_mappings AS mapping
                WHERE mapping.account_uuid = %s AND mapping.entity_kind = %s
                  AND LOWER(mapping.metadata->>'name') = LOWER(%s)
                  AND (
                      mapping.entity_kind <> 'stream'
                      OR mapping.metadata->>'chat_type' = 'channel'
                  )
                  AND NOT mapping.deleted
                ORDER BY mapping.updated_at DESC
                LIMIT 1
                """,
                (account_uuid, entity_kind, name),
            ).fetchone()

    def workspace_mapping(
        self, account_uuid: str, entity_kind: str, workspace_uuid: str
    ) -> dict[str, object] | None:
        # An active alias is an explicit redirect from a displaced Workspace
        # UUID and must outrank a stale primary row retained in a full projection.
        with self.session() as session:
            return session.execute(
                """
                SELECT workspace_uuid, provider_id, provider_revision, metadata
                FROM (
                    SELECT workspace_uuid, provider_id, provider_revision, metadata,
                           1 AS source_order
                    FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = %s
                      AND workspace_uuid = %s AND NOT deleted
                    UNION ALL
                    SELECT alias.workspace_uuid, alias.provider_id,
                           mapping.provider_revision, alias.metadata, 0 AS source_order
                    FROM provider_mapping_aliases AS alias
                    LEFT JOIN provider_mappings AS mapping
                      ON mapping.account_uuid = alias.account_uuid
                     AND mapping.entity_kind = alias.entity_kind
                     AND mapping.provider_id = alias.provider_id
                     AND NOT mapping.deleted
                    WHERE alias.account_uuid = %s AND alias.entity_kind = %s
                      AND alias.workspace_uuid = %s AND NOT alias.deleted
                ) AS candidates
                ORDER BY source_order
                LIMIT 1
                """,
                (
                    account_uuid,
                    entity_kind,
                    workspace_uuid,
                    account_uuid,
                    entity_kind,
                    workspace_uuid,
                ),
            ).fetchone()

    def topic_message_mapping(
        self, account_uuid: str, topic_uuid: str
    ) -> dict[str, object] | None:
        with self.session() as session:
            return session.execute(
                """
                SELECT workspace_uuid, provider_id, provider_revision, metadata
                FROM provider_mappings
                WHERE account_uuid = %s AND entity_kind = 'message'
                  AND metadata->>'topic_uuid' = %s AND NOT deleted
                  AND provider_id ~ '^[0-9]+$'
                ORDER BY provider_id::bigint DESC
                LIMIT 1
                """,
                (account_uuid, topic_uuid),
            ).fetchone()

    def accepted_provider_message_context(
        self, account_uuid: str, queue_id: str, event_id: int
    ) -> dict[str, object] | None:
        with self.session() as session:
            return session.execute(
                """
                WITH provider_event AS (
                    SELECT prepared_records, processing_state
                    FROM zulip_provider_events
                    WHERE account_uuid = %s AND queue_id = %s
                      AND event_id = %s
                ),
                outbox_records AS (
                    SELECT record, created_at
                    FROM workspace_delivery_outbox
                    WHERE account_uuid = %s AND provider_queue_id = %s
                      AND provider_event_id = %s
                ),
                accepted_sequence AS (
                    SELECT COALESCE(
                        provider_event.prepared_records,
                        (
                            SELECT jsonb_agg(
                                outbox_record.record
                                ORDER BY outbox_record.created_at
                            )
                            FROM outbox_records AS outbox_record
                        )
                    ) AS records,
                    provider_event.prepared_records IS NOT NULL
                    OR provider_event.processing_state IN (
                        'delivering', 'processed'
                    ) AS complete
                    FROM provider_event
                ),
                accepted AS (
                    SELECT accepted_record.record
                    FROM accepted_sequence,
                    LATERAL jsonb_array_elements(
                        accepted_sequence.records
                    ) WITH ORDINALITY AS accepted_record(record, position)
                )
                SELECT message_delivery.record->>'project_uuid' AS project_uuid,
                       message_delivery.record->'operation'->>'entity_uuid'
                           AS message_uuid,
                       message_delivery.record->'operation'->'provider'->>'chat_id'
                           AS chat_key,
                       message_delivery.record->'operation'->'payload'->>'stream_uuid'
                           AS stream_uuid,
                       message_delivery.record->'operation'->'payload'->>'topic_uuid'
                           AS topic_uuid,
                       message_delivery.record->'operation'->'payload'->>'author_uuid'
                           AS author_uuid,
                       message_delivery.record->'operation' AS message_operation,
                       accepted_sequence.records AS accepted_records,
                       accepted_sequence.complete AS accepted_records_complete
                FROM accepted AS message_delivery, accepted_sequence
                WHERE message_delivery.record->'operation'->>'kind'
                    = 'message.create'
                LIMIT 1
                """,
                (
                    account_uuid,
                    queue_id,
                    event_id,
                    account_uuid,
                    queue_id,
                    event_id,
                ),
            ).fetchone()

    def pending_provider_message_context(
        self, account_uuid: str, workspace_uuid: str
    ) -> dict[str, object] | None:
        """Return the newest accepted destination before its result commits."""
        with self.session() as session:
            row = session.execute(
                """
                SELECT delivery.record
                FROM workspace_delivery_outbox AS delivery
                JOIN operation_idempotency AS operation
                  ON operation.operation_uuid = delivery.operation_uuid
                WHERE delivery.account_uuid = %s
                  AND operation.terminal_outcome IS NULL
                  AND delivery.record->'operation'->>'kind' IN (
                      'message.create', 'message.update', 'message.delete'
                  )
                  AND delivery.record->'operation'->>'entity_uuid' = %s
                ORDER BY (delivery.record->>'sequence')::bigint DESC NULLS LAST,
                         delivery.created_at DESC
                LIMIT 1
                """,
                (account_uuid, workspace_uuid),
            ).fetchone()
            if row is None:
                return None
            record = typing.cast(dict[str, object], row["record"])
            operation = typing.cast(dict[str, object], record["operation"])
            if operation["kind"] == "message.delete":
                return {"deleted": True, "causal_lane": record["causal_lane"]}
            payload = typing.cast(dict[str, object], operation["payload"])
            provider = typing.cast(dict[str, object], operation["provider"])
            extensions = typing.cast(
                dict[str, object], operation.get("extensions", {})
            )
            return {
                "project_uuid": record["project_uuid"],
                "stream_uuid": payload["stream_uuid"],
                "topic_uuid": payload["topic_uuid"],
                "chat_key": provider["chat_id"],
                "causal_lane": record["causal_lane"],
                **(
                    {"subject": extensions["subject"]}
                    if extensions.get("subject") is not None
                    else {}
                ),
            }

    def workspace_message_mappings_through(
        self,
        account_uuid: str,
        stream_uuid: str,
        topic_uuid: str | None,
        through_workspace_uuid: str,
    ) -> list[dict[str, object]]:
        with self.session() as session:
            boundary = session.execute(
                """
                SELECT provider_id FROM provider_mappings
                WHERE account_uuid = %s AND entity_kind = 'message'
                  AND workspace_uuid = %s AND NOT deleted
                UNION ALL
                SELECT provider_id FROM provider_mapping_aliases
                WHERE account_uuid = %s AND entity_kind = 'message'
                  AND workspace_uuid = %s AND NOT deleted
                LIMIT 1
                """,
                (
                    account_uuid,
                    through_workspace_uuid,
                    account_uuid,
                    through_workspace_uuid,
                ),
            ).fetchone()
            if boundary is None or not str(boundary["provider_id"]).isdigit():
                return []
            parameters: list[object] = [
                account_uuid,
                stream_uuid,
                int(boundary["provider_id"]),
            ]
            topic_clause = ""
            if topic_uuid is not None:
                topic_clause = "AND metadata->>'topic_uuid' = %s"
                parameters.append(topic_uuid)
            return list(
                session.execute(
                    f"""
                    SELECT workspace_uuid, provider_id, provider_revision, metadata
                    FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = 'message'
                      AND metadata->>'stream_uuid' = %s AND NOT deleted
                      AND provider_id ~ '^[0-9]+$'
                      AND provider_id::bigint <= %s
                      {topic_clause}
                    ORDER BY provider_id::bigint
                    """,
                    parameters,
                ).fetchall()
            )

    def remember_provider_mapping(
        self,
        account_uuid: str,
        entity_kind: str,
        provider_id: str,
        workspace_uuid: str,
        metadata: dict[str, object],
        provider_revision: str | None = None,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    provider_revision, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_uuid, entity_kind, provider_id) DO UPDATE SET
                    provider_revision = COALESCE(
                        EXCLUDED.provider_revision,
                        provider_mappings.provider_revision
                    ),
                    metadata = EXCLUDED.metadata,
                    deleted = false,
                    updated_at = now()
                """,
                (
                    account_uuid,
                    entity_kind,
                    workspace_uuid,
                    provider_id,
                    provider_revision,
                    json.dumps(metadata),
                ),
            )

    def remember_pending_provider_message_recreation(
        self,
        account_uuid: str,
        provider_id: str,
        workspace_uuid: str,
        metadata: dict[str, object],
    ) -> None:
        """Refresh a recreation target without clearing a committed tombstone."""
        with self.session() as session:
            mapping = session.execute(
                """
                UPDATE provider_mappings
                SET metadata = %s, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = 'message'
                  AND provider_id = %s AND workspace_uuid = %s
                RETURNING workspace_uuid
                """,
                (
                    json.dumps(metadata),
                    account_uuid,
                    provider_id,
                    workspace_uuid,
                ),
            ).fetchone()
        if mapping is None:
            raise ValueError("provider_message_mapping_not_found")

    def rename_provider_mapping(
        self,
        account_uuid: str,
        entity_kind: str,
        old_provider_id: str,
        new_provider_id: str,
        metadata: dict[str, object],
        provider_revision: str | None = None,
    ) -> dict[str, object] | None:
        with self.session() as session:
            # The lock is acquired in its own statement so a caller that waits
            # for a competing rename gets a fresh READ COMMITTED snapshot for
            # the mapping query below.
            session.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"{account_uuid}:{entity_kind}:{new_provider_id}",),
            )
            row = session.execute(
                """
                WITH existing_target AS (
                    UPDATE provider_mappings AS mapping
                    SET provider_revision = CASE
                            WHEN mapping.deleted
                            THEN COALESCE(%s, mapping.provider_revision)
                            ELSE mapping.provider_revision
                        END,
                        metadata = CASE
                            WHEN mapping.deleted THEN %s
                            ELSE mapping.metadata
                        END,
                        deleted = false,
                        updated_at = CASE
                            WHEN mapping.deleted THEN now()
                            ELSE mapping.updated_at
                        END
                    WHERE mapping.account_uuid = %s
                      AND mapping.entity_kind = %s
                      AND mapping.provider_id = %s
                    RETURNING mapping.workspace_uuid, mapping.provider_id,
                              mapping.provider_revision, mapping.metadata,
                              false AS mapping_renamed
                ),
                renamed AS (
                    UPDATE provider_mappings AS mapping
                    SET provider_id = %s, provider_revision = %s, metadata = %s,
                        deleted = false, updated_at = now()
                    WHERE mapping.account_uuid = %s
                      AND mapping.entity_kind = %s
                      AND mapping.provider_id = %s
                      AND NOT mapping.deleted
                      AND NOT EXISTS (SELECT 1 FROM existing_target)
                    RETURNING mapping.workspace_uuid, mapping.provider_id,
                              mapping.provider_revision, mapping.metadata,
                              true AS mapping_renamed
                )
                SELECT * FROM renamed
                UNION ALL
                SELECT * FROM existing_target
                LIMIT 1
                """,
                (
                    provider_revision,
                    json.dumps(metadata),
                    account_uuid,
                    entity_kind,
                    new_provider_id,
                    new_provider_id,
                    provider_revision,
                    json.dumps(metadata),
                    account_uuid,
                    entity_kind,
                    old_provider_id,
                ),
            ).fetchone()
            mapping_renamed = row is not None and bool(row["mapping_renamed"])
            if mapping_renamed:
                session.execute(
                    """
                    UPDATE provider_mapping_aliases
                    SET provider_id = %s, metadata = %s,
                        deleted = false, updated_at = now()
                    WHERE account_uuid = %s AND entity_kind = %s
                      AND provider_id = %s AND NOT deleted
                    """,
                    (
                        new_provider_id,
                        json.dumps(metadata),
                        account_uuid,
                        entity_kind,
                        old_provider_id,
                    ),
                )
            if row is None:
                return None
            result = dict(row)
            result.pop("mapping_renamed", None)
            return result

    @staticmethod
    def _select_reaction_mappings(
        rows: list[dict[str, object]],
        provider_id: str,
        legacy_provider_id: str,
        metadata: dict[str, object],
        create_if_missing: bool = True,
    ) -> tuple[dict[str, object] | None, list[dict[str, object]]]:
        reaction_type = str(metadata["reaction_type"])
        emoji_code = str(metadata["emoji_code"])
        if reaction_type == "unicode_emoji":
            emoji_code = emoji.canonical_unicode_emoji_code(emoji_code)

        def same_reaction(row: dict[str, object]) -> bool:
            row_provider_id = str(row["provider_id"])
            if row_provider_id in {provider_id, legacy_provider_id}:
                return True
            row_metadata = typing.cast(dict[str, object], row["metadata"])
            if str(row_metadata.get("reaction_type")) != reaction_type:
                return False
            row_code = str(row_metadata.get("emoji_code", ""))
            if reaction_type == "unicode_emoji":
                try:
                    row_code = emoji.canonical_unicode_emoji_code(row_code)
                except ValueError:
                    return False
            return row_code == emoji_code

        candidates = [row for row in rows if same_reaction(row)]
        active = [row for row in candidates if not bool(row["deleted"])]
        survivor = next(
            (row for row in active if str(row["provider_id"]) == provider_id),
            active[0] if active else None,
        )
        if survivor is None and create_if_missing:
            survivor = next(
                (row for row in candidates if str(row["provider_id"]) == provider_id),
                candidates[0] if candidates else None,
            )
        displaced = [
            {
                "workspace_uuid": row["workspace_uuid"],
                "provider_id": row["provider_id"],
                "provider_revision": row["provider_revision"],
                "metadata": row["metadata"],
            }
            for row in active
            if survivor is not None
            and row["workspace_uuid"] != survivor["workspace_uuid"]
        ]
        return survivor, displaced

    def plan_reaction_mapping(
        self,
        account_uuid: str,
        provider_message_id: str,
        provider_user_id: str,
        provider_id: str,
        legacy_provider_id: str,
        workspace_uuid: str,
        metadata: dict[str, object],
        create_if_missing: bool = True,
    ) -> tuple[dict[str, object] | None, list[dict[str, object]]]:
        """Plan convergence without hiding mappings before cleanup is accepted."""

        provider_prefix = f"{provider_message_id}:{provider_user_id}:%"
        with self.session() as session:
            rows = list(
                session.execute(
                    """
                    SELECT workspace_uuid, provider_id, provider_revision,
                           metadata, deleted, updated_at
                    FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = 'reaction'
                      AND provider_id LIKE %s
                    ORDER BY deleted, updated_at, workspace_uuid
                    """,
                    (account_uuid, provider_prefix),
                ).fetchall()
            )
            survivor, displaced = self._select_reaction_mappings(
                rows,
                provider_id,
                legacy_provider_id,
                metadata,
                create_if_missing,
            )
            mapping = (
                {
                    "workspace_uuid": workspace_uuid,
                    "provider_id": provider_id,
                    "provider_revision": None,
                    "metadata": metadata,
                }
                if survivor is None and create_if_missing
                else survivor
            )
            return mapping, displaced

    def _converge_reaction_mapping(
        self,
        session: sessions.PgSQLSession,
        account_uuid: str,
        provider_message_id: str,
        provider_user_id: str,
        provider_id: str,
        legacy_provider_id: str,
        workspace_uuid: str,
        metadata: dict[str, object],
        create_if_missing: bool = True,
    ) -> tuple[dict[str, object] | None, list[dict[str, object]]]:
        lock_key = ":".join(
            (account_uuid, "reaction", provider_message_id, provider_user_id)
        )
        provider_prefix = f"{provider_message_id}:{provider_user_id}:%"
        session.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (lock_key,),
        )
        rows = list(
            session.execute(
                """
                SELECT workspace_uuid, provider_id, provider_revision,
                       metadata, deleted, updated_at
                FROM provider_mappings
                WHERE account_uuid = %s AND entity_kind = 'reaction'
                  AND provider_id LIKE %s
                ORDER BY deleted, updated_at, workspace_uuid
                FOR UPDATE
                """,
                (account_uuid, provider_prefix),
            ).fetchall()
        )
        survivor, displaced = self._select_reaction_mappings(
            rows,
            provider_id,
            legacy_provider_id,
            metadata,
            create_if_missing,
        )
        if survivor is not None:
            # A deleted canonical row can block renaming the active legacy
            # survivor. It is safe to remove because it has no live
            # Workspace projection. Active displaced rows stay recoverable
            # until their individual cleanup operations are accepted.
            if str(survivor["provider_id"]) != provider_id:
                session.execute(
                    """
                    DELETE FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = 'reaction'
                      AND provider_id = %s AND deleted
                    """,
                    (account_uuid, provider_id),
                )
            mapping = session.execute(
                """
                UPDATE provider_mappings
                SET provider_id = %s, metadata = %s,
                    deleted = false, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = 'reaction'
                  AND workspace_uuid = %s AND provider_id = %s
                RETURNING workspace_uuid, provider_id, provider_revision,
                          metadata
                """,
                (
                    provider_id,
                    json.dumps(metadata),
                    account_uuid,
                    survivor["workspace_uuid"],
                    survivor["provider_id"],
                ),
            ).fetchone()
        elif create_if_missing:
            mapping = session.execute(
                """
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    metadata, deleted
                ) VALUES (%s, 'reaction', %s, %s, %s, false)
                ON CONFLICT (account_uuid, entity_kind, provider_id)
                DO UPDATE SET metadata = EXCLUDED.metadata,
                              deleted = false, updated_at = now()
                RETURNING workspace_uuid, provider_id, provider_revision,
                          metadata
                """,
                (
                    account_uuid,
                    workspace_uuid,
                    provider_id,
                    json.dumps(metadata),
                ),
            ).fetchone()
        else:
            mapping = None
        if mapping is None and not create_if_missing:
            return None, displaced
        if mapping is None:
            raise ValueError("Reaction mapping convergence failed")
        return mapping, displaced

    def converge_reaction_mapping(
        self,
        account_uuid: str,
        provider_message_id: str,
        provider_user_id: str,
        provider_id: str,
        legacy_provider_id: str,
        workspace_uuid: str,
        metadata: dict[str, object],
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        """Canonicalize one survivor; accepted cleanup tombstones aliases."""

        with self.session() as session:
            return self._converge_reaction_mapping(
                session,
                account_uuid,
                provider_message_id,
                provider_user_id,
                provider_id,
                legacy_provider_id,
                workspace_uuid,
                metadata,
            )

    def mark_provider_mapping_deleted(
        self, account_uuid: str, entity_kind: str, provider_id: str
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE provider_mappings
                SET deleted = true, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = %s AND provider_id = %s
                """,
                (account_uuid, entity_kind, provider_id),
            )

    def pending_provider_events(self, limit: int = 100) -> list[dict[str, object]]:
        """Select a fair batch of eligible per-account FIFO journal heads."""
        with self.session() as session:
            return list(
                session.execute(
                    """
                    WITH candidates AS MATERIALIZED (
                        SELECT event.account_uuid, event.queue_id,
                               event.event_id, event.body,
                               event.processing_reason, event.retry_count,
                               event.assignment_pending_since,
                               event.assignment_catalog_reported_at,
                               event.provider_message_context,
                               event.created_at, event.available_at
                        FROM scheduler_accounts AS journal
                        JOIN LATERAL (
                            SELECT account_uuid, queue_id, event_id, body,
                                   processing_reason, retry_count,
                                   assignment_pending_since,
                                   assignment_catalog_reported_at,
                                   provider_message_context,
                                   created_at, available_at
                            FROM zulip_provider_events
                            WHERE account_uuid = journal.account_uuid
                              AND processing_state = 'pending'
                            ORDER BY created_at, event_id, queue_id
                            LIMIT 1
                        ) AS event ON true
                        WHERE event.available_at <= now()
                          AND (
                              NOT EXISTS (
                                  SELECT 1
                                  FROM desired_resources AS account
                                  WHERE account.resource_type =
                                        'external_account'
                                    AND account.resource_uuid =
                                        event.account_uuid
                                    AND NOT account.deleted
                                    AND COALESCE(
                                          (account.body
                                              ->>'synchronization_enabled')
                                              ::boolean,
                                          false
                                        )
                                    )
                              OR EXISTS (
                                  SELECT 1
                                  FROM desired_resources AS account
                                  LEFT JOIN scheduler_accounts AS scheduler
                                    ON scheduler.account_uuid =
                                       account.resource_uuid
                                  WHERE account.resource_type =
                                        'external_account'
                                    AND account.resource_uuid =
                                        event.account_uuid
                                    AND NOT account.deleted
                                    AND COALESCE(
                                          (account.body
                                              ->>'synchronization_enabled')
                                              ::boolean,
                                          false
                                        )
                                    AND (
                                        scheduler.account_uuid IS NULL
                                        OR scheduler.provider_generation
                                           IS DISTINCT FROM account.generation
                                        OR scheduler.provider_state = 'ready'
                                        OR (
                                            scheduler.provider_state = 'backoff'
                                            AND scheduler.provider_retry_after
                                                <= now()
                                        )
                                    )
                              )
                          )
                        ORDER BY
                            journal.last_provider_event_dispatched_at NULLS FIRST,
                            event.available_at, event.created_at,
                            event.event_id, event.queue_id, event.account_uuid
                        FOR UPDATE OF journal SKIP LOCKED
                        LIMIT %s
                    ), dispatched AS (
                        UPDATE scheduler_accounts AS journal
                        SET last_provider_event_dispatched_at = clock_timestamp()
                        FROM candidates AS event
                        WHERE journal.account_uuid = event.account_uuid
                        RETURNING journal.account_uuid
                    )
                    SELECT event.account_uuid, event.queue_id, event.event_id,
                           event.body, event.processing_reason,
                           event.retry_count,
                           event.assignment_pending_since,
                           event.assignment_catalog_reported_at,
                           event.provider_message_context
                    FROM candidates AS event
                    JOIN dispatched
                      ON dispatched.account_uuid = event.account_uuid
                    ORDER BY event.created_at, event.event_id, event.queue_id
                    """,
                    (limit,),
                ).fetchall()
            )

    def has_pending_provider_events(self) -> bool:
        """Return whether live Zulip journal work still needs processing."""
        with self.session() as session:
            row = session.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM scheduler_accounts AS journal
                    JOIN LATERAL (
                        SELECT account_uuid, queue_id, event_id,
                               processing_state, available_at
                        FROM zulip_provider_events
                        WHERE account_uuid = journal.account_uuid
                          AND processing_state IN ('pending', 'delivering')
                        ORDER BY created_at, event_id, queue_id
                        LIMIT 1
                    ) AS event ON true
                    WHERE (
                          (
                              event.processing_state = 'pending'
                              AND event.available_at <= now()
                              AND (
                                  NOT EXISTS (
                                      SELECT 1
                                      FROM desired_resources AS account
                                      WHERE account.resource_type =
                                            'external_account'
                                        AND account.resource_uuid =
                                            event.account_uuid
                                        AND NOT account.deleted
                                        AND COALESCE(
                                              (account.body
                                                  ->>'synchronization_enabled')
                                                  ::boolean,
                                              false
                                            )
                                  )
                                  OR EXISTS (
                                      SELECT 1
                                      FROM desired_resources AS account
                                      LEFT JOIN scheduler_accounts AS scheduler
                                        ON scheduler.account_uuid =
                                           account.resource_uuid
                                      WHERE account.resource_type =
                                            'external_account'
                                        AND account.resource_uuid =
                                            event.account_uuid
                                        AND NOT account.deleted
                                        AND COALESCE(
                                              (account.body
                                                  ->>'synchronization_enabled')
                                                  ::boolean,
                                              false
                                            )
                                        AND (
                                            scheduler.account_uuid IS NULL
                                            OR scheduler.provider_generation
                                               IS DISTINCT FROM
                                               account.generation
                                            OR scheduler.provider_state = 'ready'
                                            OR (
                                                scheduler.provider_state =
                                                    'backoff'
                                                AND scheduler.provider_retry_after
                                                    <= now()
                                            )
                                        )
                                  )
                              )
                          )
                          OR (
                              event.processing_state = 'delivering'
                              AND EXISTS (
                                  SELECT 1
                                  FROM workspace_delivery_outbox AS delivery
                                  WHERE delivery.account_uuid =
                                        event.account_uuid
                                    AND delivery.provider_queue_id =
                                        event.queue_id
                                    AND delivery.provider_event_id =
                                        event.event_id
                                    AND delivery.sent_at IS NULL
                                    AND delivery.submission_state IN (
                                        'pending', 'submitting', 'ambiguous',
                                        'awaiting_result'
                                    )
                              )
                          )
                      )
                ) AS pending
                """
            ).fetchone()
            return bool(row and row["pending"])

    def retry_provider_event(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        reason: str,
    ) -> None:
        normalized_reason = reason[:128] or "provider_file_unavailable"
        with self.session() as session:
            session.execute(
                """
                WITH retry AS (SELECT %s::text AS reason)
                UPDATE zulip_provider_events
                SET retry_count = CASE
                        WHEN retry.reason = 'provider_event_processing_failed'
                          AND processing_reason IS DISTINCT FROM retry.reason
                        THEN 1
                        ELSE retry_count + 1
                    END,
                    available_at = now() + CASE
                        WHEN retry.reason IN (
                            'provider_chat_assignment_pending',
                            'provider_chat_participants_pending',
                            'provider_event_replay_incomplete'
                        ) THEN interval '250 milliseconds'
                        WHEN retry.reason = 'provider_event_processing_failed'
                          AND processing_reason IS DISTINCT FROM retry.reason
                        THEN interval '1 second'
                        ELSE LEAST(300, power(2, LEAST(retry_count, 8)))
                             * interval '1 second'
                    END,
                    assignment_pending_since = CASE
                        WHEN retry.reason = 'provider_chat_assignment_pending'
                        THEN COALESCE(assignment_pending_since, now())
                        ELSE assignment_pending_since
                    END,
                    processing_reason = retry.reason
                FROM retry
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state = 'pending'
                """,
                (
                    normalized_reason,
                    account_uuid,
                    queue_id,
                    event_id,
                ),
            )

    def cache_provider_event_message_context(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        provider_message_context: dict[str, object],
    ) -> dict[str, object]:
        """Persist the minimal provider message facts needed by a reaction."""
        with self.session() as session:
            row = session.execute(
                """
                UPDATE zulip_provider_events
                SET provider_message_context = COALESCE(
                        provider_message_context,
                        %s
                    )
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state = 'pending'
                RETURNING provider_message_context
                """,
                (
                    json.dumps(provider_message_context),
                    account_uuid,
                    queue_id,
                    event_id,
                ),
            ).fetchone()
        if row is None or not isinstance(row["provider_message_context"], dict):
            raise ValueError("Provider event message context is unavailable")
        return typing.cast(dict[str, object], row["provider_message_context"])

    def mark_provider_event_catalog_reported(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
    ) -> bool:
        """Persist that the discovered chat catalog reached the durable outbox."""
        with self.session() as session:
            row = session.execute(
                """
                UPDATE zulip_provider_events
                SET assignment_catalog_reported_at = COALESCE(
                        assignment_catalog_reported_at,
                        now()
                    )
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state = 'pending'
                RETURNING assignment_catalog_reported_at
                """,
                (account_uuid, queue_id, event_id),
            ).fetchone()
        return row is not None

    def release_dependency_gated_provider_events(self) -> int:
        """Wake live events whose control-plane dependency may now be ready."""
        with self.session() as session:
            row = session.execute(
                """
                WITH released AS (
                    UPDATE zulip_provider_events
                    SET available_at = now()
                    WHERE processing_state = 'pending'
                      AND processing_reason IN (
                          'provider_chat_assignment_pending',
                          'provider_chat_participants_pending',
                          'provider_event_replay_incomplete'
                      )
                      AND available_at > now()
                    RETURNING 1
                )
                SELECT count(*) AS count FROM released
                """
            ).fetchone()
            return int(row["count"])

    def mark_provider_event_processed(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        supported: bool,
        reason: str | None = None,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_provider_events
                SET processing_state = %s, processing_reason = %s
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                """,
                (
                    "processed" if supported else "unsupported",
                    None if reason is None else reason[:128],
                    account_uuid,
                    queue_id,
                    event_id,
                ),
            )

    def finalize_provider_event(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        supported: bool,
        deleted_message_ids: list[str],
        reason: str | None = None,
    ) -> None:
        """Atomically publish delete tombstones after delivery is durable."""
        with self.session() as session:
            if deleted_message_ids:
                session.execute(
                    """
                    UPDATE provider_mappings
                    SET deleted = true, updated_at = now()
                    WHERE account_uuid = %s AND entity_kind = 'message'
                      AND provider_id = ANY(%s) AND NOT deleted
                    """,
                    (account_uuid, deleted_message_ids),
                )
            session.execute(
                """
                UPDATE zulip_provider_events
                SET processing_state = %s, processing_reason = %s
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state = 'pending'
                """,
                (
                    "processed" if supported else "unsupported",
                    None if reason is None else reason[:128],
                    account_uuid,
                    queue_id,
                    event_id,
                ),
            )

    def mark_provider_event_invalid(
        self, account_uuid: str, queue_id: str, event_id: int, reason: str
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_provider_events
                SET processing_state = 'invalid', processing_reason = %s
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                """,
                (
                    reason[:128] or "invalid_provider_event",
                    account_uuid,
                    queue_id,
                    event_id,
                ),
            )

    def ignore_provider_event_for_inactive_account(
        self, account_uuid: str, queue_id: str, event_id: int
    ) -> bool:
        """Terminalize stale work only when its account is explicitly inactive."""
        with self.session() as session:
            ignored = session.execute(
                """
                UPDATE zulip_provider_events AS event
                SET processing_state = 'ignored',
                    processing_reason = 'account_inactive',
                    prepared_records = NULL
                WHERE event.account_uuid = %s
                  AND event.queue_id = %s
                  AND event.event_id = %s
                  AND event.processing_state IN ('pending', 'delivering')
                  AND NOT EXISTS (
                      SELECT 1 FROM desired_resources AS account
                      WHERE account.resource_type = 'external_account'
                        AND account.resource_uuid = event.account_uuid
                        AND NOT account.deleted
                        AND COALESCE(
                            (account.body->>'synchronization_enabled')::boolean,
                            false
                        )
                  )
                RETURNING event.account_uuid
                """,
                (account_uuid, queue_id, event_id),
            ).fetchone()
            if ignored is None:
                return False
            session.execute(
                """
                UPDATE workspace_delivery_outbox
                SET submission_state = 'cancelled',
                    submission_error_code = 'account_inactive'
                WHERE account_uuid = %s
                  AND provider_queue_id = %s
                  AND provider_event_id = %s
                  AND sent_at IS NULL
                  AND submission_state NOT IN ('cancelled', 'rejected', 'sent')
                """,
                (account_uuid, queue_id, event_id),
            )
            return True

    def ignore_provider_reaction_outside_history_window(
        self,
        account_uuid: str,
        provider_chat_key: str,
        provider_message_id: str,
        provider_message_time: datetime.datetime,
        queue_id: str,
        event_id: int,
    ) -> bool:
        """Terminalize a reaction only when its message cannot be backfilled."""
        with self.session() as session:
            ignored = session.execute(
                """
                UPDATE zulip_provider_events AS event
                SET processing_state = 'ignored',
                    processing_reason = 'provider_message_outside_history',
                    prepared_records = NULL
                WHERE event.account_uuid = %s
                  AND event.queue_id = %s
                  AND event.event_id = %s
                  AND event.processing_state = 'pending'
                  AND NOT EXISTS (
                      SELECT 1 FROM provider_mappings AS mapping
                      WHERE mapping.account_uuid = event.account_uuid
                        AND mapping.entity_kind = 'message'
                        AND mapping.provider_id = %s
                        AND NOT mapping.deleted
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM zulip_provider_events AS source_event
                      WHERE source_event.account_uuid = event.account_uuid
                        AND source_event.event_type = 'message'
                        AND source_event.processing_state IN (
                            'pending', 'delivering'
                        )
                        AND source_event.body->'message'->>'id' = %s
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM zulip_provider_events AS echo_event
                      JOIN bridge_operations AS operation
                        ON operation.account_uuid = echo_event.account_uuid
                       AND operation.provider_queue_id = echo_event.queue_id
                       AND operation.provider_local_id =
                           echo_event.body->>'local_message_id'
                       AND operation.provider_local_id IS NOT NULL
                       AND operation.state IN (
                           'pending', 'running', 'uncertain'
                       )
                       AND operation.record->'operation'->>'kind' =
                           'message.create'
                      WHERE echo_event.account_uuid = event.account_uuid
                        AND echo_event.event_type = 'message'
                        AND echo_event.body ? 'local_message_id'
                        AND echo_event.body->'message'->>'id' = %s
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM zulip_queue_catchup_jobs AS catchup
                      WHERE catchup.account_uuid = event.account_uuid
                        AND catchup.provider_chat_key = %s
                        AND catchup.state <> 'complete'
                  )
                  AND EXISTS (
                      SELECT 1
                      FROM zulip_backfill_jobs AS job
                      JOIN desired_resources AS assignment
                        ON assignment.resource_type =
                           'external_chat_assignment'
                       AND NOT assignment.deleted
                       AND assignment.body->>'external_account_uuid' =
                           job.account_uuid::text
                       AND assignment.body->'provider_chat'
                               ->>'provider_chat_key' =
                           job.provider_chat_key
                       AND COALESCE(
                           (assignment.body->>'selected')::boolean, true
                       )
                       AND assignment.body->>'history_depth' =
                           job.history_depth
                      WHERE job.account_uuid = event.account_uuid
                        AND job.provider_chat_key = %s
                        AND job.state = 'complete'
                        AND job.cutoff_at IS NOT NULL
                        AND %s < job.cutoff_at
                  )
                RETURNING event.account_uuid
                """,
                (
                    account_uuid,
                    queue_id,
                    event_id,
                    provider_message_id,
                    provider_message_id,
                    provider_message_id,
                    provider_chat_key,
                    provider_chat_key,
                    provider_message_time,
                ),
            ).fetchone()
            return ignored is not None

    def producer_lane_position(
        self, operation_uuid: str, origin: str, causal_lane: str
    ) -> tuple[int, str | None]:
        with self.session() as session:
            existing = session.execute(
                """
                SELECT lane_sequence, predecessor_operation_uuid
                FROM producer_operations WHERE operation_uuid = %s
                """,
                (operation_uuid,),
            ).fetchone()
            if existing is not None:
                return int(existing["lane_sequence"]), (
                    None
                    if existing["predecessor_operation_uuid"] is None
                    else str(existing["predecessor_operation_uuid"])
                )
            return 0, None

    @staticmethod
    def _allocate_producer_lane(
        session: sessions.PgSQLSession,
        record: dict[str, object],
    ) -> None:
        if int(record["sequence"]) != 0:
            return
        operation_uuid = str(record["operation_uuid"])
        origin = str(record["origin"])
        causal_lane = str(record["causal_lane"])
        counter = session.execute(
            """
                INSERT INTO producer_lane_counters (origin, causal_lane)
                VALUES (%s, %s)
                ON CONFLICT (origin, causal_lane) DO UPDATE
                SET updated_at = now()
                RETURNING last_sequence, last_operation_uuid
                """,
            (origin, causal_lane),
        ).fetchone()
        sequence = int(counter["last_sequence"]) + 1
        predecessor = counter["last_operation_uuid"]
        session.execute(
            """
                INSERT INTO producer_operations (
                    operation_uuid, origin, causal_lane, lane_sequence,
                    predecessor_operation_uuid
                ) VALUES (%s, %s, %s, %s, %s)
                """,
            (operation_uuid, origin, causal_lane, sequence, predecessor),
        )
        session.execute(
            """
                UPDATE producer_lane_counters
                SET last_sequence = %s, last_operation_uuid = %s, updated_at = now()
                WHERE origin = %s AND causal_lane = %s
                """,
            (sequence, operation_uuid, origin, causal_lane),
        )
        record["sequence"] = sequence
        record["predecessor_operation_uuid"] = (
            None if predecessor is None else str(predecessor)
        )
        record["operation_sha256"] = canonical.operation_digest(record)

    def prepare_provider_event_records(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        records: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        """Persist the complete immutable event sequence before enqueueing it."""
        with self.session() as session:
            provider_event = session.execute(
                """
                SELECT processing_state, prepared_records
                FROM zulip_provider_events
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                FOR UPDATE
                """,
                (account_uuid, queue_id, event_id),
            ).fetchone()
            if provider_event is None:
                raise ValueError("unknown_provider_event")
            prepared = provider_event["prepared_records"]
            if prepared is not None:
                if not isinstance(prepared, list):
                    raise ValueError("invalid_prepared_provider_event")
                return copy.deepcopy(prepared)
            if provider_event["processing_state"] != "pending":
                raise ValueError("provider_event_not_pending")
            prepared = copy.deepcopy(records)
            for record in prepared:
                self._allocate_producer_lane(session, record)
            self._validate_reaction_mapping_plans(session, prepared)
            session.execute(
                """
                UPDATE zulip_provider_events
                SET prepared_records = %s
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state = 'pending'
                """,
                (json.dumps(prepared), account_uuid, queue_id, event_id),
            )
            return prepared

    def _validate_reaction_mapping_plans(
        self,
        session: sessions.PgSQLSession,
        records: list[dict[str, object]],
    ) -> None:
        for record in records:
            transport = record.get("transport")
            if not isinstance(transport, dict):
                continue
            plan = transport.get("reaction_mapping")
            if plan is None:
                continue
            if not isinstance(plan, dict):
                raise ValueError("invalid_reaction_mapping_plan")
            metadata = plan.get("metadata")
            displaced_plan = plan.get("displaced")
            create_if_missing = plan.get("create_if_missing")
            if (
                not isinstance(metadata, dict)
                or not isinstance(displaced_plan, list)
                or not isinstance(create_if_missing, bool)
            ):
                raise ValueError("invalid_reaction_mapping_plan")
            operation = record.get("operation")
            if not isinstance(operation, dict) or operation.get("kind") not in {
                "reaction.upsert",
                "reaction.delete",
            }:
                raise ValueError("invalid_reaction_mapping_operation")
            if create_if_missing != (operation["kind"] == "reaction.upsert"):
                raise ValueError("invalid_reaction_mapping_plan")
            expected_displaced = {
                (str(item["workspace_uuid"]), str(item["provider_id"]))
                for item in displaced_plan
                if isinstance(item, dict)
                and item.get("workspace_uuid") is not None
                and item.get("provider_id") is not None
            }
            if len(expected_displaced) != len(displaced_plan):
                raise ValueError("invalid_reaction_mapping_plan")
            account_uuid = str(record["account_uuid"])
            provider_message_id = str(plan["provider_message_id"])
            provider_user_id = str(plan["provider_user_id"])
            provider_id = str(plan["provider_id"])
            legacy_provider_id = str(plan["legacy_provider_id"])
            lock_key = ":".join(
                (
                    account_uuid,
                    "reaction",
                    provider_message_id,
                    provider_user_id,
                )
            )
            session.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (lock_key,),
            )
            rows = list(
                session.execute(
                    """
                    SELECT workspace_uuid, provider_id, provider_revision,
                           metadata, deleted, updated_at
                    FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = 'reaction'
                      AND provider_id LIKE %s
                    ORDER BY deleted, updated_at, workspace_uuid
                    FOR UPDATE
                    """,
                    (
                        account_uuid,
                        f"{provider_message_id}:{provider_user_id}:%",
                    ),
                ).fetchall()
            )
            survivor, displaced = self._select_reaction_mappings(
                rows,
                provider_id,
                legacy_provider_id,
                typing.cast(dict[str, object], metadata),
                create_if_missing,
            )
            planned_workspace_uuid = str(plan["workspace_uuid"])
            actual_workspace_uuid = (
                planned_workspace_uuid
                if survivor is None and create_if_missing
                else None
                if survivor is None
                else str(survivor["workspace_uuid"])
            )
            if actual_workspace_uuid != planned_workspace_uuid:
                raise ValueError("reaction_mapping_plan_changed")
            actual_displaced = {
                (str(item["workspace_uuid"]), str(item["provider_id"]))
                for item in displaced
            }
            if not actual_displaced.issubset(expected_displaced):
                raise ValueError("reaction_mapping_plan_changed")

    def _commit_reaction_mapping_transition(
        self,
        session: sessions.PgSQLSession,
        record: dict[str, object],
    ) -> None:
        """Publish reaction mapping changes only after Workspace acceptance."""
        transport = record.get("transport")
        if not isinstance(transport, dict):
            return
        account_uuid = str(record["account_uuid"])
        delete_target = transport.get("reaction_mapping_delete")
        if delete_target is not None and not isinstance(delete_target, dict):
            raise ValueError("invalid_reaction_mapping_delete")
        if isinstance(delete_target, dict):
            workspace_uuid = delete_target.get("workspace_uuid")
            provider_id = delete_target.get("provider_id")
            if workspace_uuid is None or provider_id is None:
                raise ValueError("invalid_reaction_mapping_delete")
            session.execute(
                """
                UPDATE provider_mappings
                SET deleted = true, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = 'reaction'
                  AND workspace_uuid = %s AND provider_id = %s
                  AND NOT deleted
                """,
                (account_uuid, str(workspace_uuid), str(provider_id)),
            )
        plan = transport.get("reaction_mapping")
        if plan is None:
            return
        if not isinstance(plan, dict):
            raise ValueError("invalid_reaction_mapping_plan")
        metadata = plan.get("metadata")
        create_if_missing = plan.get("create_if_missing")
        if not isinstance(metadata, dict) or not isinstance(create_if_missing, bool):
            raise ValueError("invalid_reaction_mapping_plan")
        operation = record.get("operation")
        if not isinstance(operation, dict) or operation.get("kind") not in {
            "reaction.upsert",
            "reaction.delete",
        }:
            raise ValueError("invalid_reaction_mapping_operation")
        if create_if_missing != (operation["kind"] == "reaction.upsert"):
            raise ValueError("invalid_reaction_mapping_plan")
        mapping, _displaced = self._converge_reaction_mapping(
            session,
            account_uuid,
            str(plan["provider_message_id"]),
            str(plan["provider_user_id"]),
            str(plan["provider_id"]),
            str(plan["legacy_provider_id"]),
            str(plan["workspace_uuid"]),
            typing.cast(dict[str, object], metadata),
            True,
        )
        if mapping is None or str(mapping["workspace_uuid"]) != str(
            plan["workspace_uuid"]
        ):
            raise ValueError("reaction_mapping_plan_changed")
        if operation["kind"] == "reaction.delete":
            session.execute(
                """
                UPDATE provider_mappings
                SET deleted = true, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = 'reaction'
                  AND workspace_uuid = %s AND provider_id = %s
                """,
                (
                    account_uuid,
                    str(plan["workspace_uuid"]),
                    str(plan["provider_id"]),
                ),
            )

    @staticmethod
    def _requeue_rejected_reaction_cleanup(
        session: sessions.PgSQLSession,
        record: dict[str, object],
        priority: int,
        assignment: dict[str, object],
    ) -> bool:
        """Retry an identical history cleanup after Provider API rejection."""
        operation = record.get("operation")
        transport = record.get("transport")
        if (
            not isinstance(operation, dict)
            or operation.get("kind") != "reaction.delete"
            or not isinstance(transport, dict)
        ):
            return False
        delete_target = transport.get("reaction_mapping_delete")
        if not isinstance(delete_target, dict):
            return False
        if not all(
            isinstance(delete_target.get(field), str)
            for field in ("workspace_uuid", "provider_id")
        ):
            return False
        requeued = session.execute(
            """
            UPDATE workspace_delivery_outbox
            SET submission_state = 'pending',
                submission_error_code = NULL,
                next_submission_at = now(),
                priority = LEAST(priority, %s),
                assignment_uuid = %s,
                assignment_generation = %s,
                assignment_project_uuid = %s
            WHERE operation_uuid = %s
              AND account_uuid = %s
              AND sent_at IS NULL
              AND submission_state = 'rejected'
              AND provider_queue_id IS NULL
              AND provider_event_id IS NULL
              AND record->>'operation_sha256' = %s
              AND record->'operation'->>'kind' = 'reaction.delete'
              AND record->'operation'->>'entity_uuid' = %s
              AND record->'transport'->'reaction_mapping_delete' = %s::jsonb
            RETURNING record_uuid
            """,
            (
                priority,
                str(assignment["resource_uuid"]),
                int(assignment["generation"]),
                str(assignment["project_uuid"]),
                str(record["operation_uuid"]),
                str(record["account_uuid"]),
                str(record["operation_sha256"]),
                str(operation["entity_uuid"]),
                json.dumps(delete_target),
            ),
        ).fetchone()
        return requeued is not None

    def enqueue_workspace_delivery(
        self,
        record: dict[str, object],
        priority: int,
        provider_queue_id: str | None = None,
        provider_event_id: int | None = None,
    ) -> bool:
        with self.session() as session:
            operation_uuid = str(record["operation_uuid"])
            self._allocate_producer_lane(session, record)
            operation_sha256 = str(record["operation_sha256"])
            account = session.execute(
                """
                SELECT generation FROM desired_resources
                WHERE resource_type = 'external_account'
                  AND resource_uuid = %s AND NOT deleted
                """,
                (str(record["account_uuid"]),),
            ).fetchone()
            if account is None:
                raise ValueError("Unknown external account")
            account_generation = int(account["generation"])
            existing = session.execute(
                "SELECT operation_sha256, terminal_outcome "
                "FROM operation_idempotency WHERE operation_uuid = %s",
                (operation_uuid,),
            ).fetchone()
            if (
                existing is not None
                and existing["operation_sha256"] != operation_sha256
            ):
                accepted = session.execute(
                    """
                    SELECT record, provider_queue_id, provider_event_id
                    FROM workspace_delivery_outbox
                    WHERE operation_uuid = %s
                    FOR UPDATE
                    """,
                    (operation_uuid,),
                ).fetchone()
                if (
                    provider_queue_id is not None
                    and provider_event_id is not None
                    and accepted is not None
                    and accepted["provider_queue_id"] == provider_queue_id
                    and accepted["provider_event_id"] == provider_event_id
                    and isinstance(accepted["record"], dict)
                    and _same_provider_identity_replay(accepted["record"], record)
                ):
                    return False
                raise ValueError("Operation UUID reused with a different digest")
            if existing is not None and existing["terminal_outcome"] is not None:
                return False
            session.execute(
                """
                INSERT INTO operation_idempotency (operation_uuid, operation_sha256)
                VALUES (%s, %s)
                ON CONFLICT (operation_uuid) DO NOTHING
                """,
                (operation_uuid, operation_sha256),
            )
            operation = typing.cast(dict[str, object] | None, record.get("operation"))
            if operation is None:
                result = session.execute(
                    """
                    INSERT INTO workspace_delivery_outbox (
                        record_uuid, operation_uuid, account_uuid,
                        account_generation, priority, record
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (operation_uuid) DO NOTHING
                    RETURNING record_uuid
                    """,
                    (
                        str(record["record_uuid"]),
                        operation_uuid,
                        str(record["account_uuid"]),
                        account_generation,
                        priority,
                        json.dumps(record),
                    ),
                ).fetchone()
                return result is not None
            provider = typing.cast(dict[str, object], operation["provider"])
            account_global = (
                operation.get("kind") == "identity.upsert"
                and provider.get("chat_id") == "account"
            )
            assignment = None
            if not account_global:
                assignment = session.execute(
                    """
                    SELECT resource_uuid, generation,
                           body->>'project_id' AS project_uuid
                    FROM desired_resources
                    WHERE resource_type = 'external_chat_assignment'
                      AND NOT deleted
                      AND body->>'external_account_uuid' = %s
                      AND body->'provider_chat'->>'provider_chat_key' = %s
                      AND COALESCE((body->>'selected')::boolean, true)
                      AND body->>'project_id' = %s
                    LIMIT 1
                    """,
                    (
                        str(record["account_uuid"]),
                        str(provider["chat_id"]),
                        str(record["project_uuid"]),
                    ),
                ).fetchone()
                if assignment is None:
                    raise ValueError("provider_chat_assignment_pending")
            if (
                existing is not None
                and provider_queue_id is None
                and provider_event_id is None
                and assignment is not None
                and self._requeue_rejected_reaction_cleanup(
                    session,
                    record,
                    priority,
                    assignment,
                )
            ):
                return True
            if operation.get("kind") == "topic.upsert" and assignment is not None:
                payload = typing.cast(dict[str, object], operation["payload"])
                duplicate_topic = session.execute(
                    """
                    UPDATE workspace_delivery_outbox AS delivery
                    SET priority = LEAST(delivery.priority, %s)
                    WHERE delivery.sent_at IS NULL
                      AND delivery.account_uuid = %s
                      AND delivery.assignment_uuid = %s
                      AND delivery.assignment_generation = %s
                      AND delivery.assignment_project_uuid = %s
                      AND delivery.record->'operation'->>'kind' = 'topic.upsert'
                      AND delivery.record->'operation'->>'entity_uuid' = %s
                      AND delivery.record->'operation'->'payload'
                              ->>'stream_uuid' = %s
                      AND delivery.record->'operation'->'payload'->>'name' = %s
                      AND delivery.record->'operation'->'provider'
                              ->>'entity_id' = %s
                    RETURNING delivery.record_uuid
                    """,
                    (
                        priority,
                        str(record["account_uuid"]),
                        str(assignment["resource_uuid"]),
                        int(assignment["generation"]),
                        str(assignment["project_uuid"]),
                        str(operation["entity_uuid"]),
                        str(payload["stream_uuid"]),
                        str(payload["name"]),
                        str(provider["entity_id"]),
                    ),
                ).fetchone()
                if duplicate_topic is not None:
                    return False
            result = session.execute(
                """
                INSERT INTO workspace_delivery_outbox (
                    record_uuid, operation_uuid, account_uuid,
                    account_generation, assignment_uuid, assignment_generation,
                    assignment_project_uuid, provider_queue_id, provider_event_id,
                    priority, record
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (operation_uuid) DO NOTHING
                RETURNING record_uuid
                """,
                (
                    str(record["record_uuid"]),
                    operation_uuid,
                    str(record["account_uuid"]),
                    account_generation,
                    None if assignment is None else str(assignment["resource_uuid"]),
                    None if assignment is None else int(assignment["generation"]),
                    None if assignment is None else str(assignment["project_uuid"]),
                    provider_queue_id,
                    provider_event_id,
                    priority,
                    json.dumps(record),
                ),
            ).fetchone()
            if (
                result is not None
                and provider_queue_id is None
                and provider_event_id is None
            ):
                self._validate_reaction_mapping_plans(session, [record])
            return result is not None

    def enqueue_provider_event_records(
        self,
        records: list[dict[str, object]],
        priority: int,
        account_uuid: str,
        queue_id: str,
        event_id: int,
    ) -> None:
        """Publish a complete provider event to the outbox atomically."""
        with self.transaction():
            for record in records:
                self.enqueue_workspace_delivery(
                    record,
                    priority,
                    queue_id,
                    event_id,
                )
            self.mark_provider_event_delivering(account_uuid, queue_id, event_id)

    def pending_workspace_deliveries(
        self,
        minimum_priority: int = 0,
        maximum_priority: int = 2,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        if not 0 <= minimum_priority <= maximum_priority <= 2:
            raise ValueError("Invalid workspace delivery priority range")
        with self.session() as session:
            session.execute(
                """
                UPDATE workspace_delivery_outbox AS message_delivery
                SET priority = read_delivery.priority
                FROM workspace_delivery_outbox AS read_delivery
                WHERE message_delivery.sent_at IS NULL
                  AND %s = 0
                  AND read_delivery.sent_at IS NULL
                  AND read_delivery.priority BETWEEN %s AND %s
                  AND message_delivery.priority > read_delivery.priority
                  AND message_delivery.account_uuid =
                      read_delivery.account_uuid
                  AND message_delivery.assignment_uuid IS NOT DISTINCT FROM
                      read_delivery.assignment_uuid
                  AND message_delivery.assignment_generation
                      IS NOT DISTINCT FROM
                      read_delivery.assignment_generation
                  AND message_delivery.assignment_project_uuid
                      IS NOT DISTINCT FROM
                      read_delivery.assignment_project_uuid
                  AND read_delivery.record->'operation'->>'kind' =
                      'read_state.set'
                  AND message_delivery.record->'operation'->>'kind'
                      IN ('message.create', 'message.update')
                  AND message_delivery.record->'operation'->>'entity_uuid' IN (
                      SELECT jsonb_array_elements_text(
                          read_delivery.record->'operation'->'payload'
                              ->'message_uuids'
                      )
                  )
                """,
                (minimum_priority, minimum_priority, maximum_priority),
            )
            session.execute(
                """
                UPDATE workspace_delivery_outbox AS message_delivery
                SET priority = reaction_delivery.priority
                FROM workspace_delivery_outbox AS reaction_delivery
                WHERE message_delivery.sent_at IS NULL
                  AND %s = 0
                  AND reaction_delivery.sent_at IS NULL
                  AND reaction_delivery.priority BETWEEN %s AND %s
                  AND message_delivery.priority > reaction_delivery.priority
                  AND message_delivery.account_uuid =
                      reaction_delivery.account_uuid
                  AND message_delivery.assignment_uuid IS NOT DISTINCT FROM
                      reaction_delivery.assignment_uuid
                  AND message_delivery.assignment_generation
                      IS NOT DISTINCT FROM
                      reaction_delivery.assignment_generation
                  AND message_delivery.assignment_project_uuid
                      IS NOT DISTINCT FROM
                      reaction_delivery.assignment_project_uuid
                  AND reaction_delivery.record->'operation'->>'kind' IN (
                      'reaction.upsert', 'reaction.delete'
                  )
                  AND message_delivery.record->'operation'->>'kind'
                      IN ('message.create', 'message.update')
                  AND message_delivery.record->'operation'->>'entity_uuid' =
                      reaction_delivery.record->'operation'->'payload'
                          ->>'message_uuid'
                """,
                (minimum_priority, minimum_priority, maximum_priority),
            )
            session.execute(
                """
                UPDATE workspace_delivery_outbox AS topic_delivery
                SET priority = message_delivery.priority
                FROM workspace_delivery_outbox AS message_delivery
                WHERE topic_delivery.sent_at IS NULL
                  AND %s = 0
                  AND message_delivery.sent_at IS NULL
                  AND message_delivery.priority BETWEEN %s AND %s
                  AND topic_delivery.priority > message_delivery.priority
                  AND topic_delivery.account_uuid =
                      message_delivery.account_uuid
                  AND topic_delivery.assignment_uuid IS NOT DISTINCT FROM
                      message_delivery.assignment_uuid
                  AND topic_delivery.assignment_generation
                      IS NOT DISTINCT FROM
                      message_delivery.assignment_generation
                  AND topic_delivery.assignment_project_uuid
                      IS NOT DISTINCT FROM
                      message_delivery.assignment_project_uuid
                  AND topic_delivery.record->'operation'->>'kind' =
                      'topic.upsert'
                  AND message_delivery.record->'operation'->>'kind' IN (
                      'message.create', 'message.update', 'read_state.set',
                      'reaction.upsert', 'reaction.delete'
                  )
                  AND topic_delivery.record->'operation'->>'entity_uuid' =
                      message_delivery.record->'operation'->'payload'
                          ->>'topic_uuid'
                """,
                (minimum_priority, minimum_priority, maximum_priority),
            )
            rows = session.execute(
                """
                    SELECT delivery.record FROM workspace_delivery_outbox AS delivery
                    JOIN desired_resources AS account
                      ON account.resource_type = 'external_account'
                     AND account.resource_uuid = delivery.account_uuid
                     AND account.generation = delivery.account_generation
                     AND NOT account.deleted
                    LEFT JOIN desired_resources AS assignment
                      ON assignment.resource_type = 'external_chat_assignment'
                     AND assignment.resource_uuid = delivery.assignment_uuid
                     AND assignment.generation = delivery.assignment_generation
                     AND NOT assignment.deleted
                     AND assignment.body->>'project_id' =
                         delivery.assignment_project_uuid::text
                     AND COALESCE(
                         (assignment.body->>'selected')::boolean, true
                     )
                    WHERE delivery.sent_at IS NULL
                      AND (
                          delivery.submission_state IN ('pending', 'ambiguous')
                          OR (
                              delivery.submission_state = 'awaiting_result'
                              AND delivery.next_submission_at <= now()
                          )
                      )
                      AND delivery.priority BETWEEN %s AND %s
                      AND (
                          delivery.assignment_uuid IS NULL
                          OR assignment.resource_uuid IS NOT NULL
                      )
                      AND (
                          delivery.record->'operation'->>'kind' NOT IN (
                              'message.create', 'message.update', 'read_state.set',
                              'reaction.upsert', 'reaction.delete'
                          )
                          OR NOT EXISTS (
                              SELECT 1
                              FROM workspace_delivery_outbox AS topic_delivery
                              WHERE topic_delivery.sent_at IS NULL
                                AND topic_delivery.account_uuid =
                                    delivery.account_uuid
                                AND topic_delivery.assignment_uuid IS NOT DISTINCT
                                    FROM delivery.assignment_uuid
                                AND topic_delivery.assignment_generation
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_generation
                                AND topic_delivery.assignment_project_uuid
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_project_uuid
                                AND topic_delivery.record->'operation'->>'kind' =
                                    'topic.upsert'
                                AND topic_delivery.record->'operation'
                                        ->>'entity_uuid' =
                                    delivery.record->'operation'->'payload'
                                        ->>'topic_uuid'
                          )
                      )
                      AND (
                          delivery.record->'operation'->>'kind' NOT IN (
                              'message.update', 'message.delete'
                          )
                          OR NOT EXISTS (
                              SELECT 1
                              FROM workspace_delivery_outbox AS message_create
                              WHERE message_create.sent_at IS NULL
                                AND message_create.account_uuid =
                                    delivery.account_uuid
                                AND message_create.assignment_uuid
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_uuid
                                AND message_create.assignment_generation
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_generation
                                AND message_create.assignment_project_uuid
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_project_uuid
                                AND message_create.record->'operation'->>'kind' =
                                    'message.create'
                                AND message_create.record->'operation'
                                        ->>'entity_uuid' =
                                    delivery.record->'operation'->>'entity_uuid'
                          )
                      )
                      AND (
                          delivery.record->'operation'->>'kind' <> 'read_state.set'
                          OR NOT EXISTS (
                              SELECT 1
                              FROM workspace_delivery_outbox AS message_delivery
                              WHERE message_delivery.sent_at IS NULL
                                AND message_delivery.account_uuid =
                                    delivery.account_uuid
                                AND message_delivery.assignment_uuid
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_uuid
                                AND message_delivery.assignment_generation
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_generation
                                AND message_delivery.assignment_project_uuid
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_project_uuid
                                AND message_delivery.record->'operation'->>'kind'
                                    IN ('message.create', 'message.update')
                                AND message_delivery.record->'operation'
                                        ->>'entity_uuid' IN (
                                    SELECT jsonb_array_elements_text(
                                        delivery.record->'operation'->'payload'
                                            ->'message_uuids'
                                    )
                                )
                          )
                      )
                      AND (
                          delivery.record->'operation'->>'kind' <> 'read_state.set'
                          OR NOT EXISTS (
                              SELECT 1
                              FROM jsonb_array_elements_text(
                                  delivery.record->'operation'->'payload'
                                      ->'message_uuids'
                              ) AS read_message(message_uuid)
                              WHERE NOT EXISTS (
                                  SELECT 1
                                  FROM provider_mappings AS message_mapping
                                  WHERE message_mapping.account_uuid =
                                      delivery.account_uuid
                                    AND message_mapping.entity_kind = 'message'
                                    AND message_mapping.workspace_uuid =
                                        read_message.message_uuid::uuid
                                    AND NOT message_mapping.deleted
                                    AND message_mapping.metadata
                                            ->>'workspace_delivery_state' =
                                        'committed'
                              )
                          )
                      )
                      AND (
                          delivery.record->'operation'->>'kind' NOT IN (
                              'reaction.upsert', 'reaction.delete'
                          )
                          OR NOT EXISTS (
                              SELECT 1
                              FROM workspace_delivery_outbox AS message_create
                              WHERE message_create.sent_at IS NULL
                                AND message_create.account_uuid =
                                    delivery.account_uuid
                                AND message_create.assignment_uuid
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_uuid
                                AND message_create.assignment_generation
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_generation
                                AND message_create.assignment_project_uuid
                                    IS NOT DISTINCT FROM
                                    delivery.assignment_project_uuid
                                AND message_create.record->'operation'->>'kind' =
                                    'message.create'
                                AND message_create.record->'operation'
                                        ->>'entity_uuid' =
                                    delivery.record->'operation'->'payload'
                                        ->>'message_uuid'
                          )
                      )
                      AND (
                          delivery.record->'operation'->>'kind' NOT IN (
                              'reaction.upsert', 'reaction.delete'
                          )
                          OR EXISTS (
                              SELECT 1
                              FROM provider_mappings AS message_mapping
                              WHERE message_mapping.account_uuid =
                                  delivery.account_uuid
                                AND message_mapping.entity_kind = 'message'
                                AND message_mapping.workspace_uuid =
                                    (
                                        delivery.record->'operation'->'payload'
                                            ->>'message_uuid'
                                    )::uuid
                                AND NOT message_mapping.deleted
                                AND message_mapping.metadata
                                        ->>'workspace_delivery_state' =
                                    'committed'
                          )
                      )
                    ORDER BY priority, created_at LIMIT %s
                    """,
                (minimum_priority, maximum_priority, limit),
            ).fetchall()
            return [typing.cast(dict[str, object], row["record"]) for row in rows]

    def has_pending_workspace_deliveries(
        self,
        minimum_priority: int = 0,
        maximum_priority: int = 2,
    ) -> bool:
        """Probe ready delivery work without running dependency projection SQL."""
        if not 0 <= minimum_priority <= maximum_priority <= 2:
            raise ValueError("Invalid workspace delivery priority range")
        with self.session() as session:
            row = session.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM workspace_delivery_outbox AS delivery
                    JOIN desired_resources AS account
                      ON account.resource_type = 'external_account'
                     AND account.resource_uuid = delivery.account_uuid
                     AND account.generation = delivery.account_generation
                     AND NOT account.deleted
                    LEFT JOIN desired_resources AS assignment
                      ON assignment.resource_type = 'external_chat_assignment'
                     AND assignment.resource_uuid = delivery.assignment_uuid
                     AND assignment.generation = delivery.assignment_generation
                     AND NOT assignment.deleted
                     AND assignment.body->>'project_id' =
                         delivery.assignment_project_uuid::text
                     AND COALESCE(
                         (assignment.body->>'selected')::boolean, true
                     )
                    WHERE delivery.sent_at IS NULL
                      AND delivery.priority BETWEEN %s AND %s
                      AND (
                          delivery.assignment_uuid IS NULL
                          OR assignment.resource_uuid IS NOT NULL
                      )
                      AND (
                          delivery.submission_state IN ('pending', 'ambiguous')
                          OR (
                              delivery.submission_state = 'awaiting_result'
                              AND delivery.next_submission_at <= now()
                          )
                      )
                ) AS pending
                """,
                (minimum_priority, maximum_priority),
            ).fetchone()
            return bool(row and row["pending"])

    @staticmethod
    def _recover_provider_event_assignment_change(
        session: sessions.PgSQLSession,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        selected_assignment_exists: bool | None = None,
    ) -> bool:
        """Drop abandoned projections and replay an unsubmitted message."""
        provider_event = session.execute(
            """
            SELECT prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            FOR UPDATE
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        if provider_event is None:
            return False
        prepared_records = provider_event["prepared_records"]
        prepared = (
            typing.cast(list[dict[str, object]], prepared_records)
            if isinstance(prepared_records, list)
            else []
        )
        chat_keys: set[str] = set()
        message_create_operation_uuids: list[str] = []
        for record in prepared:
            operation = record.get("operation")
            if not isinstance(operation, dict):
                continue
            provider = operation.get("provider")
            if isinstance(provider, dict) and provider.get("chat_id") is not None:
                chat_keys.add(str(provider["chat_id"]))
            if operation.get("kind") != "message.create":
                continue
            operation_uuid = record.get("operation_uuid")
            if not isinstance(operation_uuid, str):
                continue
            message_create_operation_uuids.append(operation_uuid)
            provider_id = (
                None if not isinstance(provider, dict) else provider.get("entity_id")
            )
            if provider_id is None:
                continue
            session.execute(
                """
                UPDATE provider_mappings AS mapping
                SET deleted = true, updated_at = now()
                WHERE mapping.account_uuid = %s
                  AND mapping.entity_kind = 'message'
                  AND mapping.provider_id = %s
                  AND mapping.workspace_uuid = %s
                  AND mapping.metadata->>'project_uuid' = %s
                  AND mapping.metadata->>'workspace_delivery_state' = 'pending'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM workspace_delivery_outbox AS delivery
                      WHERE delivery.operation_uuid = %s
                  )
                """,
                (
                    account_uuid,
                    str(provider_id),
                    str(operation["entity_uuid"]),
                    str(record["project_uuid"]),
                    operation_uuid,
                ),
            )
        if selected_assignment_exists is None:
            selected_assignment_exists = False
            if chat_keys:
                selected_assignment_exists = (
                    session.execute(
                        """
                        SELECT 1
                        FROM desired_resources
                        WHERE resource_type = 'external_chat_assignment'
                          AND NOT deleted
                          AND body->>'external_account_uuid' = %s
                          AND body->'provider_chat'->>'provider_chat_key'
                              = ANY(%s)
                          AND COALESCE(
                              (body->>'selected')::boolean, true
                          )
                        LIMIT 1
                        """,
                        (account_uuid, sorted(chat_keys)),
                    ).fetchone()
                    is not None
                )
        submitted_message = None
        if message_create_operation_uuids:
            submitted_message = session.execute(
                """
                SELECT 1
                FROM workspace_delivery_outbox
                WHERE account_uuid = %s AND provider_queue_id = %s
                  AND provider_event_id = %s
                  AND operation_uuid = ANY(%s)
                LIMIT 1
                """,
                (
                    account_uuid,
                    queue_id,
                    event_id,
                    message_create_operation_uuids,
                ),
            ).fetchone()
        if (
            selected_assignment_exists
            and message_create_operation_uuids
            and submitted_message is None
        ):
            # Setup operations can already be submitting while message.create
            # has not reached the outbox. Forget the old target snapshot and
            # reconvert that message against the current assignment before
            # processing later edits.
            session.execute(
                """
                UPDATE zulip_provider_events
                SET processing_state = 'pending', available_at = now(),
                    processing_reason = 'assignment_changed',
                    prepared_records = NULL
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state IN ('pending', 'delivering')
                """,
                (account_uuid, queue_id, event_id),
            )
            return True
        session.execute(
            """
            UPDATE zulip_provider_events
            SET processing_state = 'processed',
                processing_reason = 'assignment_changed',
                prepared_records = NULL
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
              AND processing_state IN ('pending', 'delivering')
            """,
            (account_uuid, queue_id, event_id),
        )
        return False

    def reset_stale_workspace_deliveries(self) -> int:
        with self.session() as session:
            stale = session.execute(
                """
                DELETE FROM workspace_delivery_outbox AS delivery
                WHERE delivery.sent_at IS NULL
                  AND delivery.submission_state IN ('pending', 'rejected') AND (
                    (
                        delivery.assignment_uuid IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM desired_resources AS assignment
                            WHERE assignment.resource_type =
                                  'external_chat_assignment'
                              AND assignment.resource_uuid =
                                  delivery.assignment_uuid
                              AND assignment.generation =
                                  delivery.assignment_generation
                              AND NOT assignment.deleted
                              AND assignment.body->>'project_id' =
                                  delivery.assignment_project_uuid::text
                              AND COALESCE(
                                  (assignment.body->>'selected')::boolean, true
                              )
                        )
                    ) OR (
                        delivery.assignment_uuid IS NULL AND NOT EXISTS (
                            SELECT 1 FROM desired_resources AS account
                            WHERE account.resource_type = 'external_account'
                              AND account.resource_uuid = delivery.account_uuid
                              AND account.generation = delivery.account_generation
                              AND NOT account.deleted
                        )
                    )
                )
                RETURNING operation_uuid, account_uuid,
                          assignment_project_uuid, provider_queue_id,
                          provider_event_id, priority, record
                """
            ).fetchall()
            if not stale:
                return 0
            operation_ids = [row["operation_uuid"] for row in stale]
            session.execute(
                """
                DELETE FROM operation_idempotency
                WHERE operation_uuid = ANY(%s)
                  AND terminal_outcome IS NULL
                """,
                (operation_ids,),
            )
            recovered_provider_events: set[tuple[str, str, int]] = set()
            for row in stale:
                if row["priority"] == 2:
                    record = typing.cast(dict[str, object], row["record"])
                    operation = typing.cast(
                        dict[str, object] | None,
                        record.get("operation"),
                    )
                    if operation is not None:
                        provider = typing.cast(
                            dict[str, object],
                            operation["provider"],
                        )
                        session.execute(
                            """
                            UPDATE zulip_backfill_jobs
                            SET state = 'pending', next_anchor = NULL,
                                lease_until = NULL, available_at = now(),
                                retry_count = 0, last_error_code = NULL,
                                updated_at = now()
                            WHERE account_uuid = %s AND provider_chat_key = %s
                              AND state != 'cancelled'
                            """,
                            (
                                row["account_uuid"],
                                str(provider["chat_id"]),
                            ),
                        )
                if row["provider_queue_id"] is None:
                    continue
                provider_event_key = (
                    str(row["account_uuid"]),
                    str(row["provider_queue_id"]),
                    int(row["provider_event_id"]),
                )
                if provider_event_key in recovered_provider_events:
                    continue
                recovered_provider_events.add(provider_event_key)
                record = typing.cast(dict[str, object], row["record"])
                operation = typing.cast(
                    dict[str, object] | None,
                    record.get("operation"),
                )
                provider = (
                    typing.cast(dict[str, object], operation["provider"])
                    if operation is not None
                    else {}
                )
                current_assignment = session.execute(
                    """
                    SELECT body->>'project_id' AS project_uuid
                    FROM desired_resources
                    WHERE resource_type = 'external_chat_assignment'
                      AND NOT deleted
                      AND body->>'external_account_uuid' = %s
                      AND body->'provider_chat'->>'provider_chat_key' = %s
                      AND COALESCE((body->>'selected')::boolean, true)
                    LIMIT 1
                    """,
                    (
                        str(row["account_uuid"]),
                        str(provider.get("chat_id", "")),
                    ),
                ).fetchone()
                same_project = current_assignment is not None and str(
                    current_assignment["project_uuid"]
                ) == str(row["assignment_project_uuid"])
                if same_project:
                    session.execute(
                        """
                        UPDATE zulip_provider_events
                        SET processing_state = 'pending', available_at = now(),
                            processing_reason = 'assignment_changed'
                        WHERE account_uuid = %s AND queue_id = %s
                          AND event_id = %s
                          AND processing_state = 'delivering'
                        """,
                        (
                            row["account_uuid"],
                            row["provider_queue_id"],
                            row["provider_event_id"],
                        ),
                    )
                    continue
                self._recover_provider_event_assignment_change(
                    session,
                    str(row["account_uuid"]),
                    str(row["provider_queue_id"]),
                    int(row["provider_event_id"]),
                    current_assignment is not None,
                )
            return len(stale)

    def finalize_provider_event_assignment_changed(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
    ) -> bool:
        """Recover or finish an event whose prepared target is no longer active."""
        with self.session() as session:
            removed = session.execute(
                """
                DELETE FROM workspace_delivery_outbox
                WHERE account_uuid = %s AND provider_queue_id = %s
                  AND provider_event_id = %s AND sent_at IS NULL
                  AND submission_state = 'pending'
                RETURNING operation_uuid
                """,
                (account_uuid, queue_id, event_id),
            ).fetchall()
            if removed:
                session.execute(
                    """
                    DELETE FROM operation_idempotency
                    WHERE operation_uuid = ANY(%s)
                      AND terminal_outcome IS NULL
                    """,
                    ([row["operation_uuid"] for row in removed],),
                )
            return self._recover_provider_event_assignment_change(
                session,
                account_uuid,
                queue_id,
                event_id,
            )

    def mark_interrupted_workspace_deliveries_ambiguous(self) -> int:
        with self.session() as session:
            rows = session.execute(
                """
                UPDATE workspace_delivery_outbox
                SET submission_state = 'ambiguous', next_submission_at = now()
                WHERE sent_at IS NULL AND submission_state = 'submitting'
                RETURNING account_uuid, provider_queue_id, provider_event_id
                """
            ).fetchall()
            for row in rows:
                if row["provider_queue_id"] is None:
                    continue
                session.execute(
                    """
                    UPDATE zulip_provider_events
                    SET processing_reason = 'workspace_delivery_ambiguous'
                    WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                      AND processing_state = 'delivering'
                    """,
                    (
                        row["account_uuid"],
                        row["provider_queue_id"],
                        row["provider_event_id"],
                    ),
                )
            return len(rows)

    def mark_workspace_delivery_submitting(self, record_uuid: str) -> bool:
        with self.session() as session:
            row = session.execute(
                """
                UPDATE workspace_delivery_outbox AS delivery
                SET submission_state = 'submitting',
                    submission_attempts = delivery.submission_attempts + 1
                WHERE delivery.record_uuid = %s AND delivery.sent_at IS NULL
                  AND (
                      delivery.submission_state IN ('pending', 'ambiguous')
                      OR (
                          delivery.submission_state = 'awaiting_result'
                          AND delivery.next_submission_at <= now()
                      )
                  )
                  AND EXISTS (
                      SELECT 1 FROM desired_resources AS account
                      WHERE account.resource_type = 'external_account'
                        AND account.resource_uuid = delivery.account_uuid
                        AND account.generation = delivery.account_generation
                        AND NOT account.deleted
                  )
                  AND (
                      delivery.assignment_uuid IS NULL OR EXISTS (
                          SELECT 1 FROM desired_resources AS assignment
                          WHERE assignment.resource_type =
                                'external_chat_assignment'
                            AND assignment.resource_uuid =
                                delivery.assignment_uuid
                            AND assignment.generation =
                                delivery.assignment_generation
                            AND NOT assignment.deleted
                            AND assignment.body->>'project_id' =
                                delivery.assignment_project_uuid::text
                            AND COALESCE(
                                (assignment.body->>'selected')::boolean, true
                            )
                      )
                  )
                RETURNING delivery.record_uuid
                """,
                (record_uuid,),
            ).fetchone()
            return row is not None

    def mark_provider_event_delivering(
        self, account_uuid: str, queue_id: str, event_id: int
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_provider_events
                SET processing_state = 'delivering', processing_reason = NULL
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state = 'pending'
                """,
                (account_uuid, queue_id, event_id),
            )

    def finalize_ready_provider_events(self) -> int:
        with self.session() as session:
            rows = session.execute(
                """
                SELECT event.account_uuid, event.queue_id, event.event_id, event.body
                FROM zulip_provider_events AS event
                WHERE event.processing_state = 'delivering'
                  AND NOT EXISTS (
                      SELECT 1 FROM workspace_delivery_outbox AS delivery
                      WHERE delivery.account_uuid = event.account_uuid
                        AND delivery.provider_queue_id = event.queue_id
                        AND delivery.provider_event_id = event.event_id
                        AND delivery.sent_at IS NULL
                  )
                FOR UPDATE
                """
            ).fetchall()
            for row in rows:
                event = typing.cast(dict[str, object], row["body"])
                if event.get("type") == "delete_message":
                    raw_ids = event.get("message_ids")
                    if raw_ids is None and event.get("message_id") is not None:
                        raw_ids = [event["message_id"]]
                    session.execute(
                        """
                        UPDATE provider_mappings
                        SET deleted = true, updated_at = now()
                        WHERE account_uuid = %s AND entity_kind = 'message'
                          AND provider_id = ANY(%s) AND NOT deleted
                        """,
                        (
                            row["account_uuid"],
                            [str(value) for value in raw_ids or []],
                        ),
                    )
                session.execute(
                    """
                    UPDATE zulip_provider_events
                    SET processing_state = 'processed', prepared_records = NULL
                    WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                      AND processing_state = 'delivering'
                    """,
                    (row["account_uuid"], row["queue_id"], row["event_id"]),
                )
            return len(rows)

    def prune_terminal_delivery_state(
        self,
        limit: int = 10_000,
    ) -> tuple[int, int]:
        """Bound bulky terminal journals while retaining durable idempotency."""
        with self.session() as session:
            deleted_deliveries = session.execute(
                """
                WITH candidates AS (
                    SELECT record_uuid
                    FROM workspace_delivery_outbox
                    WHERE submission_state = 'sent' AND sent_at IS NOT NULL
                      AND sent_at < now() - CASE
                          WHEN priority = 2 THEN interval '1 minute'
                          ELSE interval '10 minutes'
                      END
                    ORDER BY sent_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                ), deleted AS (
                    DELETE FROM workspace_delivery_outbox AS delivery
                    USING candidates
                    WHERE delivery.record_uuid = candidates.record_uuid
                    RETURNING delivery.record_uuid
                )
                SELECT count(*) AS total FROM deleted
                """,
                (limit,),
            ).fetchone()
            deleted_events = session.execute(
                """
                WITH candidates AS (
                    SELECT event.account_uuid, event.queue_id, event.event_id
                    FROM zulip_provider_events AS event
                    WHERE event.processing_state IN (
                        'processed', 'unsupported', 'invalid', 'ignored'
                    )
                      AND event.created_at < now() - interval '10 minutes'
                      AND NOT EXISTS (
                          SELECT 1 FROM workspace_delivery_outbox AS delivery
                          WHERE delivery.account_uuid = event.account_uuid
                            AND delivery.provider_queue_id = event.queue_id
                            AND delivery.provider_event_id = event.event_id
                      )
                    ORDER BY event.created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                ), deleted AS (
                    DELETE FROM zulip_provider_events AS event
                    USING candidates
                    WHERE event.account_uuid = candidates.account_uuid
                      AND event.queue_id = candidates.queue_id
                      AND event.event_id = candidates.event_id
                    RETURNING event.event_id
                )
                SELECT count(*) AS total FROM deleted
                """,
                (limit,),
            ).fetchone()
            return int(deleted_deliveries["total"]), int(deleted_events["total"])

    def mark_workspace_delivery_submitted(self, record_uuid: str) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE workspace_delivery_outbox
                SET submission_state = 'awaiting_result',
                    last_submitted_at = now(),
                    next_submission_at = now() + (
                        LEAST(
                            300,
                            power(2, LEAST(submission_attempts, 8))::integer
                        ) * interval '1 second'
                    )
                WHERE record_uuid = %s AND sent_at IS NULL
                  AND submission_state = 'submitting'
                """,
                (record_uuid,),
            )

    def active_account_uuids(self) -> list[str]:
        if not self.provider_is_enabled("zulip"):
            return []
        with self.session() as session:
            rows = session.execute(
                """
                SELECT resource_uuid FROM desired_resources
                WHERE resource_type = 'external_account' AND NOT deleted
                  AND COALESCE((body->>'synchronization_enabled')::boolean, false)
                ORDER BY resource_uuid
                """
            ).fetchall()
            return [str(row["resource_uuid"]) for row in rows]

    def eligible_account_uuids(self) -> list[str]:
        """Return active accounts whose persisted provider breaker permits work."""
        if not self.provider_is_enabled("zulip"):
            return []
        with self.session() as session:
            rows = session.execute(
                """
                SELECT account.resource_uuid
                FROM desired_resources AS account
                LEFT JOIN scheduler_accounts AS scheduler
                  ON scheduler.account_uuid = account.resource_uuid
                WHERE account.resource_type = 'external_account'
                  AND NOT account.deleted
                  AND COALESCE(
                        (account.body->>'synchronization_enabled')::boolean,
                        false
                      )
                  AND (
                      scheduler.account_uuid IS NULL
                      OR scheduler.provider_generation IS DISTINCT FROM
                         account.generation
                      OR scheduler.provider_state = 'ready'
                      OR (
                          scheduler.provider_state = 'backoff'
                          AND scheduler.provider_retry_after <= now()
                      )
                  )
                ORDER BY account.resource_uuid
                """
            ).fetchall()
            return [str(row["resource_uuid"]) for row in rows]

    def provider_account_is_eligible(self, account_uuid: str) -> bool:
        with self.session() as session:
            row = session.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM desired_resources AS account
                    LEFT JOIN scheduler_accounts AS scheduler
                      ON scheduler.account_uuid = account.resource_uuid
                    WHERE account.resource_type = 'external_account'
                      AND account.resource_uuid = %s
                      AND NOT account.deleted
                      AND COALESCE(
                            (account.body->>'synchronization_enabled')::boolean,
                            false
                          )
                      AND (
                          scheduler.account_uuid IS NULL
                          OR scheduler.provider_generation IS DISTINCT FROM
                             account.generation
                          OR scheduler.provider_state = 'ready'
                          OR (
                              scheduler.provider_state = 'backoff'
                              AND scheduler.provider_retry_after <= now()
                          )
                      )
                ) AS eligible
                """,
                (account_uuid,),
            ).fetchone()
            return bool(row and row["eligible"])

    def record_provider_account_failure(
        self,
        account_uuid: str,
        attempted_generation: int,
        code: str,
        retryable: bool,
    ) -> dict[str, object] | None:
        """Persist a failure only for the generation used by the request."""
        state = "backoff" if retryable else "auth_required"
        with self.session() as session:
            desired = session.execute(
                """
                SELECT generation
                FROM desired_resources
                WHERE resource_type = 'external_account'
                  AND resource_uuid = %s
                  AND generation = %s
                  AND NOT deleted
                  AND COALESCE(
                        (body->>'synchronization_enabled')::boolean, false
                      )
                FOR UPDATE
                """,
                (account_uuid, attempted_generation),
            ).fetchone()
            if desired is None:
                return None
            self._reconcile_provider_account_generation(
                session, account_uuid, attempted_generation
            )
            row = session.execute(
                """
                UPDATE scheduler_accounts
                SET provider_state = CASE
                        WHEN provider_state = 'auth_required'
                        THEN 'auth_required'
                        ELSE %s
                    END,
                    provider_retry_count = provider_retry_count + 1,
                    provider_retry_after = CASE
                        WHEN provider_state = 'auth_required' THEN NULL
                        WHEN %s THEN now() + (
                            random() * LEAST(
                                300.0,
                                power(
                                    2.0,
                                    LEAST(provider_retry_count, 8)::double precision
                                )
                            ) * interval '1 second'
                        )
                        ELSE NULL
                    END,
                    provider_error_code = CASE
                        WHEN provider_state = 'auth_required'
                        THEN provider_error_code
                        ELSE %s
                    END,
                    provider_state_updated_at = now()
                WHERE account_uuid = %s AND provider_generation = %s
                RETURNING provider_generation, provider_state,
                          provider_retry_count, provider_retry_after,
                          provider_error_code
                """,
                (
                    state,
                    retryable,
                    code[:128],
                    account_uuid,
                    attempted_generation,
                ),
            ).fetchone()
            return typing.cast(dict[str, object] | None, row)

    def record_provider_account_success(
        self, account_uuid: str, attempted_generation: int
    ) -> bool | None:
        """Close a transient breaker without clearing sticky authentication state."""
        with self.session() as session:
            desired = session.execute(
                """
                SELECT generation
                FROM desired_resources
                WHERE resource_type = 'external_account'
                  AND resource_uuid = %s
                  AND generation = %s
                  AND NOT deleted
                  AND COALESCE(
                        (body->>'synchronization_enabled')::boolean, false
                      )
                FOR UPDATE
                """,
                (account_uuid, attempted_generation),
            ).fetchone()
            if desired is None:
                return None
            self._reconcile_provider_account_generation(
                session, account_uuid, attempted_generation
            )
            row = session.execute(
                """
                UPDATE scheduler_accounts AS scheduler
                SET provider_state = 'ready', provider_retry_count = 0,
                    provider_retry_after = NULL, provider_error_code = NULL,
                    provider_state_updated_at = now()
                FROM desired_resources AS account
                WHERE scheduler.account_uuid = %s
                  AND account.resource_type = 'external_account'
                  AND account.resource_uuid = scheduler.account_uuid
                  AND NOT account.deleted
                  AND account.generation = %s
                  AND account.generation = scheduler.provider_generation
                  AND scheduler.provider_state = 'backoff'
                RETURNING scheduler.account_uuid
                """,
                (account_uuid, attempted_generation),
            ).fetchone()
            if row is None:
                return False
            session.execute(
                "DELETE FROM bridge_health WHERE component = %s",
                (provider_account_health_component(account_uuid),),
            )
            return True

    def reap_expired_history_leases(self) -> int:
        """Release stale participant and history leases from interrupted workers."""
        with self.session() as session:
            participants = session.execute(
                """
                UPDATE zulip_participant_sync
                SET state = 'pending', lease_until = NULL, updated_at = now()
                WHERE state = 'running' AND lease_until < now()
                RETURNING account_uuid
                """
            ).fetchall()
            backfills = session.execute(
                """
                UPDATE zulip_backfill_jobs
                SET state = 'pending', lease_until = NULL, updated_at = now()
                WHERE state = 'running' AND lease_until < now()
                RETURNING account_uuid
                """
            ).fetchall()
            return len(participants) + len(backfills)

    def reconcile_participant_sync(self) -> None:
        with self.session() as session:
            session.execute(
                """
                INSERT INTO scheduler_accounts (
                    account_uuid, provider_generation, provider_state,
                    provider_retry_count, provider_retry_after,
                    provider_error_code, provider_state_updated_at
                )
                SELECT account.resource_uuid, account.generation, 'ready',
                       0, NULL, NULL, now()
                FROM desired_resources AS account
                WHERE account.resource_type = 'external_account'
                  AND NOT account.deleted
                  AND COALESCE(
                      (account.body->>'synchronization_enabled')::boolean,
                      false
                  )
                ORDER BY account.resource_uuid
                ON CONFLICT (account_uuid) DO UPDATE SET
                    provider_generation = EXCLUDED.provider_generation,
                    provider_state = 'ready', provider_retry_count = 0,
                    provider_retry_after = NULL, provider_error_code = NULL,
                    provider_state_updated_at = now()
                WHERE scheduler_accounts.provider_generation IS DISTINCT FROM
                      EXCLUDED.provider_generation
                """
            )
            session.execute(
                """
                INSERT INTO zulip_participant_sync (
                    account_uuid, provider_chat_key, assignment_generation, state
                )
                SELECT
                    (assignment.body->>'external_account_uuid')::uuid,
                    assignment.body->'provider_chat'->>'provider_chat_key',
                    assignment.generation,
                    CASE
                        WHEN assignment.body->'provider_chat'->>'chat_type' =
                             'channel'
                        THEN 'pending'
                        ELSE 'ready'
                    END
                FROM desired_resources AS assignment
                WHERE assignment.resource_type = 'external_chat_assignment'
                  AND NOT assignment.deleted
                  AND COALESCE(
                      (assignment.body->>'selected')::boolean, true
                  )
                ORDER BY
                    (assignment.body->>'external_account_uuid')::uuid,
                    assignment.body->'provider_chat'->>'provider_chat_key'
                ON CONFLICT (account_uuid, provider_chat_key) DO UPDATE SET
                    assignment_generation = EXCLUDED.assignment_generation,
                    state = CASE
                        WHEN EXCLUDED.state = 'ready'
                        THEN 'ready'
                        WHEN zulip_participant_sync.assignment_generation =
                             EXCLUDED.assignment_generation
                        THEN zulip_participant_sync.state
                        ELSE EXCLUDED.state
                    END,
                    lease_until = CASE
                        WHEN EXCLUDED.state = 'ready'
                        THEN NULL
                        WHEN zulip_participant_sync.assignment_generation =
                             EXCLUDED.assignment_generation
                        THEN zulip_participant_sync.lease_until
                        ELSE NULL
                    END,
                    provider_user_ids = CASE
                        WHEN EXCLUDED.state = 'ready'
                        THEN '[]'::jsonb
                        WHEN zulip_participant_sync.assignment_generation =
                             EXCLUDED.assignment_generation
                        THEN zulip_participant_sync.provider_user_ids
                        ELSE '[]'::jsonb
                    END,
                    updated_at = CASE
                        WHEN zulip_participant_sync.assignment_generation =
                             EXCLUDED.assignment_generation
                        THEN zulip_participant_sync.updated_at
                        ELSE now()
                    END
                WHERE zulip_participant_sync.assignment_generation IS DISTINCT FROM
                          EXCLUDED.assignment_generation
                   OR (
                       EXCLUDED.state = 'ready'
                       AND zulip_participant_sync.state IS DISTINCT FROM 'ready'
                   )
                """
            )
            session.execute(
                """
                DELETE FROM zulip_participant_sync AS participant_sync
                WHERE NOT EXISTS (
                    SELECT 1 FROM desired_resources AS assignment
                    WHERE assignment.resource_type = 'external_chat_assignment'
                      AND NOT assignment.deleted
                      AND assignment.body->>'external_account_uuid' =
                          participant_sync.account_uuid::text
                      AND assignment.body->'provider_chat'
                              ->>'provider_chat_key' =
                          participant_sync.provider_chat_key
                      AND COALESCE(
                          (assignment.body->>'selected')::boolean, true
                      )
                )
                """
            )

    def invalidate_participant_sync(
        self, account_uuid: str, provider_chat_keys: list[str]
    ) -> None:
        """Schedule prompt refreshes for channels changed by provider events."""
        if not provider_chat_keys:
            return
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_participant_sync
                SET state = 'pending', lease_until = NULL, updated_at = now()
                WHERE account_uuid = %s
                  AND provider_chat_key = ANY(%s)
                """,
                (account_uuid, sorted(set(provider_chat_keys))),
            )

    def claim_participant_sync_batch(self, limit: int = 50) -> list[dict[str, object]]:
        if limit < 1:
            raise ValueError("Participant sync claim limit must be positive")
        with self.session() as session:
            rows = session.execute(
                """
                WITH candidate_account AS MATERIALIZED (
                    SELECT scheduler.account_uuid
                    FROM scheduler_accounts AS scheduler
                    JOIN desired_resources AS provider_account
                      ON provider_account.resource_type = 'external_account'
                     AND provider_account.resource_uuid = scheduler.account_uuid
                     AND NOT provider_account.deleted
                     AND provider_account.generation =
                         scheduler.provider_generation
                     AND COALESCE(
                           (provider_account.body
                               ->>'synchronization_enabled')::boolean,
                           false
                         )
                    JOIN LATERAL (
                        SELECT participant_sync.updated_at
                        FROM zulip_participant_sync AS participant_sync
                        JOIN desired_resources AS assignment
                          ON assignment.resource_type =
                             'external_chat_assignment'
                         AND NOT assignment.deleted
                         AND assignment.generation =
                             participant_sync.assignment_generation
                         AND assignment.body->>'external_account_uuid' =
                             participant_sync.account_uuid::text
                         AND assignment.body->'provider_chat'
                                 ->>'provider_chat_key' =
                             participant_sync.provider_chat_key
                         AND assignment.body->'provider_chat'->>'chat_type' =
                             'channel'
                        WHERE participant_sync.account_uuid =
                              scheduler.account_uuid
                          AND (
                              participant_sync.state = 'pending'
                              OR (
                                  participant_sync.state = 'running'
                                  AND participant_sync.lease_until < now()
                              )
                              OR (
                                  participant_sync.state = 'reported'
                                  AND participant_sync.updated_at <
                                      now() - interval '30 seconds'
                              )
                              OR (
                                  participant_sync.state = 'ready'
                                  AND participant_sync.updated_at <
                                      now() - make_interval(secs => %s)
                              )
                          )
                        ORDER BY participant_sync.updated_at,
                                 participant_sync.provider_chat_key
                        LIMIT 1
                    ) AS oldest ON true
                    WHERE scheduler.provider_state = 'ready'
                       OR (
                           scheduler.provider_state = 'backoff'
                           AND scheduler.provider_retry_after <= now()
                       )
                    ORDER BY scheduler.last_participant_sync_at NULLS FIRST,
                             oldest.updated_at,
                             scheduler.account_uuid
                    FOR UPDATE OF scheduler SKIP LOCKED
                    LIMIT 1
                ), candidates AS MATERIALIZED (
                    SELECT participant_sync.account_uuid,
                           participant_sync.provider_chat_key,
                           participant_sync.assignment_generation,
                           assignment.body AS assignment
                    FROM zulip_participant_sync AS participant_sync
                    JOIN candidate_account AS account
                      ON account.account_uuid = participant_sync.account_uuid
                    JOIN desired_resources AS assignment
                      ON assignment.resource_type =
                         'external_chat_assignment'
                     AND NOT assignment.deleted
                     AND assignment.generation =
                         participant_sync.assignment_generation
                     AND assignment.body->>'external_account_uuid' =
                         participant_sync.account_uuid::text
                     AND assignment.body->'provider_chat'
                             ->>'provider_chat_key' =
                         participant_sync.provider_chat_key
                     AND assignment.body->'provider_chat'->>'chat_type' =
                         'channel'
                    WHERE participant_sync.state = 'pending'
                       OR (
                           participant_sync.state = 'running'
                           AND participant_sync.lease_until < now()
                       )
                       OR (
                           participant_sync.state = 'reported'
                           AND participant_sync.updated_at <
                               now() - interval '30 seconds'
                       )
                       OR (
                           participant_sync.state = 'ready'
                           AND participant_sync.updated_at <
                               now() - make_interval(secs => %s)
                       )
                    ORDER BY participant_sync.updated_at,
                             participant_sync.provider_chat_key
                    FOR UPDATE OF participant_sync SKIP LOCKED
                    LIMIT %s
                ), claimed AS (
                UPDATE zulip_participant_sync AS participant_sync
                SET state = 'running',
                    lease_until = now() + interval '60 seconds',
                    updated_at = now()
                FROM candidates
                WHERE participant_sync.account_uuid = candidates.account_uuid
                  AND participant_sync.provider_chat_key =
                      candidates.provider_chat_key
                RETURNING participant_sync.account_uuid,
                          participant_sync.provider_chat_key,
                          participant_sync.assignment_generation,
                          candidates.assignment
                ), dispatched AS (
                    UPDATE scheduler_accounts AS scheduler
                    SET last_participant_sync_at = now()
                    FROM candidate_account AS account
                    WHERE scheduler.account_uuid = account.account_uuid
                    RETURNING scheduler.account_uuid
                )
                SELECT claimed.account_uuid,
                       claimed.provider_chat_key,
                       claimed.assignment_generation,
                       claimed.assignment
                FROM claimed
                JOIN dispatched USING (account_uuid)
                ORDER BY claimed.provider_chat_key
                """,
                (
                    PARTICIPANT_RECHECK_INTERVAL_SECONDS,
                    PARTICIPANT_RECHECK_INTERVAL_SECONDS,
                    limit,
                ),
            ).fetchall()
            return typing.cast(list[dict[str, object]], rows)

    def claim_participant_sync(self) -> dict[str, object] | None:
        rows = self.claim_participant_sync_batch(1)
        return None if not rows else rows[0]

    def complete_participant_sync(
        self,
        account_uuid: str,
        provider_chat_key: str,
        assignment_generation: int,
        provider_user_ids: list[int],
        ready: bool,
    ) -> None:
        self.complete_participant_sync_batch(
            [
                {
                    "account_uuid": account_uuid,
                    "provider_chat_key": provider_chat_key,
                    "assignment_generation": assignment_generation,
                    "provider_user_ids": provider_user_ids,
                    "ready": ready,
                }
            ]
        )

    def complete_participant_sync_batch(self, updates: list[dict[str, object]]) -> None:
        if not updates:
            return
        normalized = [
            {
                "account_uuid": str(update["account_uuid"]),
                "provider_chat_key": str(update["provider_chat_key"]),
                "assignment_generation": int(update["assignment_generation"]),
                "provider_user_ids": sorted(
                    {int(value) for value in update["provider_user_ids"]}
                ),
                "ready": bool(update["ready"]),
            }
            for update in updates
        ]
        with self.session() as session:
            session.execute(
                """
                WITH updates AS (
                    SELECT
                        (value->>'account_uuid')::uuid AS account_uuid,
                        value->>'provider_chat_key' AS provider_chat_key,
                        (value->>'assignment_generation')::bigint
                            AS assignment_generation,
                        value->'provider_user_ids' AS provider_user_ids,
                        (value->>'ready')::boolean AS ready
                    FROM jsonb_array_elements(%s::jsonb) AS value
                )
                UPDATE zulip_participant_sync AS participant_sync
                SET state = CASE WHEN updates.ready THEN 'ready' ELSE 'reported' END,
                    lease_until = NULL,
                    provider_user_ids = updates.provider_user_ids,
                    updated_at = now()
                FROM updates
                WHERE participant_sync.account_uuid = updates.account_uuid
                  AND participant_sync.provider_chat_key =
                      updates.provider_chat_key
                  AND participant_sync.assignment_generation =
                      updates.assignment_generation
                  AND participant_sync.state = 'running'
                """,
                (json.dumps(normalized),),
            )

    def release_participant_sync(
        self,
        account_uuid: str,
        provider_chat_key: str,
        assignment_generation: int,
    ) -> None:
        self.release_participant_sync_batch(
            [
                {
                    "account_uuid": account_uuid,
                    "provider_chat_key": provider_chat_key,
                    "assignment_generation": assignment_generation,
                }
            ]
        )

    def release_participant_sync_batch(self, jobs: list[dict[str, object]]) -> None:
        if not jobs:
            return
        normalized = [
            {
                "account_uuid": str(job["account_uuid"]),
                "provider_chat_key": str(job["provider_chat_key"]),
                "assignment_generation": int(job["assignment_generation"]),
            }
            for job in jobs
        ]
        with self.session() as session:
            session.execute(
                """
                WITH jobs AS (
                    SELECT
                        (value->>'account_uuid')::uuid AS account_uuid,
                        value->>'provider_chat_key' AS provider_chat_key,
                        (value->>'assignment_generation')::bigint
                            AS assignment_generation
                    FROM jsonb_array_elements(%s::jsonb) AS value
                )
                UPDATE zulip_participant_sync AS participant_sync
                SET state = 'pending', lease_until = NULL, updated_at = now()
                FROM jobs
                WHERE participant_sync.account_uuid = jobs.account_uuid
                  AND participant_sync.provider_chat_key = jobs.provider_chat_key
                  AND participant_sync.assignment_generation =
                      jobs.assignment_generation
                  AND participant_sync.state = 'running'
                """,
                (json.dumps(normalized),),
            )

    def assignment_participants_ready(
        self,
        account_uuid: str,
        provider_chat_key: str,
        assignment_generation: int,
    ) -> bool:
        with self.session() as session:
            row = session.execute(
                """
                SELECT state = 'ready' AS ready
                FROM zulip_participant_sync
                WHERE account_uuid = %s AND provider_chat_key = %s
                  AND assignment_generation = %s
                """,
                (account_uuid, provider_chat_key, assignment_generation),
            ).fetchone()
            return row is not None and bool(row["ready"])

    def reconcile_backfill_jobs(self) -> None:
        with self.session() as session:
            session.execute(
                """
                INSERT INTO zulip_backfill_jobs (
                    account_uuid, provider_chat_key, history_depth, cutoff_at, state
                )
                SELECT
                    (assignment.body->>'external_account_uuid')::uuid,
                    assignment.body->'provider_chat'->>'provider_chat_key',
                    assignment.body->>'history_depth',
                    CASE assignment.body->>'history_depth'
                        WHEN 'new' THEN assignment.updated_at
                        WHEN '7_days' THEN now() - interval '7 days'
                        WHEN '30_days' THEN now() - interval '30 days'
                        WHEN '90_days' THEN now() - interval '90 days'
                        ELSE NULL
                    END,
                    CASE assignment.body->>'history_depth'
                        WHEN 'new' THEN 'complete'
                        ELSE 'pending'
                    END
                FROM desired_resources AS assignment
                JOIN desired_resources AS account
                  ON account.resource_type = 'external_account'
                 AND account.resource_uuid::text =
                     assignment.body->>'external_account_uuid'
                 AND NOT account.deleted
                WHERE assignment.resource_type = 'external_chat_assignment'
                  AND NOT assignment.deleted
                  AND COALESCE((assignment.body->>'selected')::boolean, true)
                ORDER BY
                    (assignment.body->>'external_account_uuid')::uuid,
                    assignment.body->'provider_chat'->>'provider_chat_key'
                ON CONFLICT (account_uuid, provider_chat_key) DO UPDATE SET
                    next_anchor = CASE
                        WHEN zulip_backfill_jobs.state <> 'cancelled'
                         AND zulip_backfill_jobs.history_depth =
                             EXCLUDED.history_depth
                        THEN zulip_backfill_jobs.next_anchor
                        ELSE NULL
                    END,
                    history_depth = EXCLUDED.history_depth,
                    cutoff_at = CASE
                        WHEN zulip_backfill_jobs.state <> 'cancelled'
                         AND zulip_backfill_jobs.history_depth =
                             EXCLUDED.history_depth
                        THEN COALESCE(
                            zulip_backfill_jobs.cutoff_at,
                            zulip_backfill_jobs.updated_at
                        )
                        ELSE EXCLUDED.cutoff_at
                    END,
                    state = CASE
                        WHEN zulip_backfill_jobs.state = 'cancelled'
                        THEN EXCLUDED.state
                        WHEN zulip_backfill_jobs.history_depth = EXCLUDED.history_depth
                        THEN zulip_backfill_jobs.state
                        ELSE EXCLUDED.state
                    END,
                    updated_at = now()
                WHERE zulip_backfill_jobs.state = 'cancelled'
                   OR zulip_backfill_jobs.history_depth IS DISTINCT FROM
                          EXCLUDED.history_depth
                   OR (
                       zulip_backfill_jobs.history_depth = 'new'
                       AND zulip_backfill_jobs.cutoff_at IS NULL
                   )
                """
            )
            session.execute(
                """
                UPDATE zulip_backfill_jobs AS job
                SET state = 'cancelled', updated_at = now()
                WHERE job.state <> 'cancelled'
                  AND NOT EXISTS (
                    SELECT 1 FROM desired_resources AS assignment
                    JOIN desired_resources AS account
                      ON account.resource_type = 'external_account'
                     AND account.resource_uuid::text =
                         assignment.body->>'external_account_uuid'
                     AND NOT account.deleted
                    WHERE assignment.resource_type = 'external_chat_assignment'
                      AND NOT assignment.deleted
                      AND assignment.body->>'external_account_uuid' =
                          job.account_uuid::text
                      AND assignment.body->'provider_chat'->>'provider_chat_key' =
                          job.provider_chat_key
                      AND COALESCE(
                          (assignment.body->>'selected')::boolean, true
                      )
                )
                """
            )
            session.execute(
                """
                DELETE FROM bridge_health AS health
                USING zulip_backfill_jobs AS job
                WHERE job.state <> 'failed'
                  AND health.component =
                      'provider:' || job.account_uuid::text || ':' ||
                      job.provider_chat_key
                """
            )
            session.execute(
                """
                DELETE FROM zulip_queue_catchup_jobs AS job
                WHERE NOT EXISTS (
                    SELECT 1 FROM desired_resources AS assignment
                    WHERE assignment.resource_type = 'external_chat_assignment'
                      AND NOT assignment.deleted
                      AND assignment.body->>'external_account_uuid' =
                          job.account_uuid::text
                      AND assignment.body->'provider_chat'
                              ->>'provider_chat_key' =
                          job.provider_chat_key
                      AND COALESCE(
                          (assignment.body->>'selected')::boolean, true
                      )
                )
                """
            )

    def catalog_reports_accepted(self, account_uuid: str, generation: int) -> bool:
        with self.session() as session:
            row = session.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (
                        (body->>'resource_uuid')::uuid
                    ) result_status
                    FROM observed_report_outbox
                    WHERE body->>'resource_type' = 'external_chat_catalog'
                      AND (
                          body->'catalog'->>'external_account_uuid'
                      )::uuid = %s
                      AND (body->>'observed_generation')::bigint = %s
                    ORDER BY (body->>'resource_uuid')::uuid, created_at DESC
                )
                SELECT NOT EXISTS (
                    SELECT 1 FROM latest
                    WHERE COALESCE(result_status, '')
                          NOT IN ('applied', 'duplicate')
                ) AS accepted
                """,
                (account_uuid, generation),
            ).fetchone()
            return bool(row["accepted"])

    def catalog_assignments_ready(self, account_uuid: str, generation: int) -> bool:
        account = self.account_resource(account_uuid)
        if account is None:
            return False
        settings = typing.cast(dict[str, object], account["settings"])
        if settings.get("selection_mode") != "all":
            return True
        policy = self.provider_policy("zulip") or {}
        limits = policy.get("limits")
        maximum = (
            limits.get("max_selected_chats_per_account", 0)
            if isinstance(limits, dict)
            else 0
        )
        if not isinstance(maximum, int) or isinstance(maximum, bool):
            maximum = 0
        with self.session() as session:
            catalog_count = session.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (
                        (body->>'resource_uuid')::uuid
                    ) body, result_status
                    FROM observed_report_outbox
                    WHERE body->>'resource_type' = 'external_chat_catalog'
                      AND (
                          body->'catalog'->>'external_account_uuid'
                      )::uuid = %s
                      AND (body->>'observed_generation')::bigint = %s
                    ORDER BY (body->>'resource_uuid')::uuid, created_at DESC
                )
                SELECT COUNT(*) AS count FROM latest
                WHERE body->'catalog'->>'operation' = 'upsert'
                  AND result_status IN ('applied', 'duplicate')
                """,
                (account_uuid, generation),
            ).fetchone()["count"]
            assignment_count = session.execute(
                """
                SELECT COUNT(*) AS count FROM desired_resources
                WHERE resource_type = 'external_chat_assignment'
                  AND NOT deleted
                  AND (body->>'external_account_uuid')::uuid = %s
                  AND COALESCE((body->>'selected')::boolean, true)
                """,
                (account_uuid,),
            ).fetchone()["count"]
        return int(assignment_count) >= min(int(catalog_count), maximum)

    def initial_backfill_ready(self, account_uuid: str) -> bool:
        with self.session() as session:
            row = session.execute(
                """
                SELECT
                    NOT EXISTS (
                        SELECT 1 FROM desired_resources AS assignment
                        WHERE assignment.resource_type = 'external_chat_assignment'
                          AND NOT assignment.deleted
                          AND (
                              assignment.body->>'external_account_uuid'
                          )::uuid = %s
                          AND COALESCE(
                              (assignment.body->>'selected')::boolean, true
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM zulip_participant_sync
                                  AS participant_sync
                              WHERE participant_sync.account_uuid = %s
                                AND participant_sync.provider_chat_key =
                                    assignment.body->'provider_chat'
                                        ->>'provider_chat_key'
                                AND participant_sync.assignment_generation =
                                    assignment.generation
                                AND participant_sync.state = 'ready'
                          )
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM desired_resources AS assignment
                        WHERE assignment.resource_type =
                              'external_chat_assignment'
                          AND NOT assignment.deleted
                          AND (
                              assignment.body->>'external_account_uuid'
                          )::uuid = %s
                          AND COALESCE(
                              (assignment.body->>'selected')::boolean, true
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM zulip_backfill_jobs AS job
                              WHERE job.account_uuid = %s
                                AND job.provider_chat_key =
                                    assignment.body->'provider_chat'->>'provider_chat_key'
                                AND job.state = 'complete'
                          )
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM workspace_delivery_outbox AS delivery
                        JOIN desired_resources AS account
                          ON account.resource_type = 'external_account'
                         AND account.resource_uuid = delivery.account_uuid
                         AND NOT account.deleted
                        LEFT JOIN operation_idempotency AS operation
                          ON operation.operation_uuid = delivery.operation_uuid
                        WHERE delivery.account_uuid = %s
                          AND delivery.priority = 2
                          AND delivery.account_generation = account.generation
                          AND operation.terminal_outcome IS DISTINCT FROM 'committed'
                    ) AS ready
                """,
                (
                    account_uuid,
                    account_uuid,
                    account_uuid,
                    account_uuid,
                    account_uuid,
                ),
            ).fetchone()
            return bool(row["ready"])

    def claim_backfill_job(self) -> dict[str, object] | None:
        with self.session() as session:
            return session.execute(
                """
                WITH candidate_account AS MATERIALIZED (
                    SELECT scheduler.account_uuid
                    FROM scheduler_accounts AS scheduler
                    JOIN desired_resources AS provider_account
                      ON provider_account.resource_type = 'external_account'
                     AND provider_account.resource_uuid = scheduler.account_uuid
                     AND NOT provider_account.deleted
                     AND provider_account.generation =
                         scheduler.provider_generation
                     AND COALESCE(
                           (provider_account.body
                               ->>'synchronization_enabled')::boolean,
                           false
                         )
                    JOIN LATERAL (
                        SELECT job.updated_at
                        FROM zulip_backfill_jobs AS job
                        JOIN desired_resources AS assignment
                          ON assignment.resource_type =
                             'external_chat_assignment'
                         AND NOT assignment.deleted
                         AND assignment.body->>'external_account_uuid' =
                             job.account_uuid::text
                         AND assignment.body->'provider_chat'
                                 ->>'provider_chat_key' =
                             job.provider_chat_key
                        JOIN zulip_participant_sync AS participant_sync
                          ON participant_sync.account_uuid = job.account_uuid
                         AND participant_sync.provider_chat_key =
                             job.provider_chat_key
                         AND participant_sync.assignment_generation =
                             assignment.generation
                         AND participant_sync.state = 'ready'
                        WHERE job.account_uuid = scheduler.account_uuid
                          AND (
                              (
                                  job.state = 'pending'
                                  AND job.available_at <= now()
                              ) OR (
                                  job.state = 'running'
                                  AND job.lease_until < now()
                              )
                          )
                        ORDER BY job.updated_at, job.provider_chat_key
                        LIMIT 1
                    ) AS oldest ON true
                    WHERE scheduler.provider_state = 'ready'
                       OR (
                           scheduler.provider_state = 'backoff'
                           AND scheduler.provider_retry_after <= now()
                       )
                    ORDER BY scheduler.last_backfill_at NULLS FIRST,
                             oldest.updated_at,
                             scheduler.account_uuid
                    FOR UPDATE OF scheduler SKIP LOCKED
                    LIMIT 1
                ), candidate AS MATERIALIZED (
                    SELECT job.account_uuid, job.provider_chat_key
                    FROM zulip_backfill_jobs AS job
                    JOIN candidate_account AS account
                      ON account.account_uuid = job.account_uuid
                    JOIN desired_resources AS assignment
                      ON assignment.resource_type =
                         'external_chat_assignment'
                     AND NOT assignment.deleted
                     AND assignment.body->>'external_account_uuid' =
                         job.account_uuid::text
                     AND assignment.body->'provider_chat'
                             ->>'provider_chat_key' =
                         job.provider_chat_key
                    JOIN zulip_participant_sync AS participant_sync
                      ON participant_sync.account_uuid = job.account_uuid
                     AND participant_sync.provider_chat_key =
                         job.provider_chat_key
                     AND participant_sync.assignment_generation =
                         assignment.generation
                     AND participant_sync.state = 'ready'
                    WHERE (
                        job.state = 'pending' AND job.available_at <= now()
                    ) OR (
                        job.state = 'running' AND job.lease_until < now()
                    )
                    ORDER BY job.updated_at, job.provider_chat_key
                    FOR UPDATE OF job SKIP LOCKED LIMIT 1
                ), claimed AS (
                UPDATE zulip_backfill_jobs AS job
                SET state = 'running', lease_until = now() + interval '60 seconds',
                    updated_at = now()
                FROM candidate
                WHERE job.account_uuid = candidate.account_uuid
                  AND job.provider_chat_key = candidate.provider_chat_key
                RETURNING job.account_uuid, job.provider_chat_key,
                          job.history_depth, job.next_anchor, job.cutoff_at,
                          job.retry_count
                ), dispatched AS (
                    UPDATE scheduler_accounts AS scheduler
                    SET last_backfill_at = now()
                    FROM candidate_account AS account
                    WHERE scheduler.account_uuid = account.account_uuid
                    RETURNING scheduler.account_uuid
                )
                SELECT claimed.account_uuid, claimed.provider_chat_key,
                       claimed.history_depth, claimed.next_anchor,
                       claimed.cutoff_at, claimed.retry_count
                FROM claimed
                JOIN dispatched USING (account_uuid)
                """
            ).fetchone()

    def advance_backfill_job(
        self,
        account_uuid: str,
        provider_chat_key: str,
        next_anchor: int | None,
        complete: bool,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_backfill_jobs
                SET next_anchor = %s, state = %s, lease_until = NULL,
                    available_at = now(), retry_count = 0,
                    last_error_code = NULL, updated_at = now()
                WHERE account_uuid = %s AND provider_chat_key = %s
                """,
                (
                    next_anchor,
                    "complete" if complete else "pending",
                    account_uuid,
                    provider_chat_key,
                ),
            )
            session.execute(
                "DELETE FROM bridge_health WHERE component = %s",
                (backfill_health_component(account_uuid, provider_chat_key),),
            )

    def release_backfill_job(self, account_uuid: str, provider_chat_key: str) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_backfill_jobs
                SET state = 'pending', lease_until = NULL,
                    available_at = now() + interval '1 second', updated_at = now()
                WHERE account_uuid = %s AND provider_chat_key = %s
                  AND state = 'running'
                """,
                (account_uuid, provider_chat_key),
            )

    def defer_backfill_job(
        self,
        account_uuid: str,
        provider_chat_key: str,
        available_at: datetime.datetime,
        code: str,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_backfill_jobs
                SET state = 'pending', lease_until = NULL,
                    available_at = %s, retry_count = retry_count + 1,
                    last_error_code = %s, updated_at = now()
                WHERE account_uuid = %s AND provider_chat_key = %s
                  AND state = 'running'
                """,
                (available_at, code, account_uuid, provider_chat_key),
            )

    def fail_backfill_job(
        self,
        account_uuid: str,
        provider_chat_key: str,
        code: str,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_backfill_jobs
                SET state = 'failed', lease_until = NULL,
                    last_error_code = %s, updated_at = now()
                WHERE account_uuid = %s AND provider_chat_key = %s
                  AND state = 'running'
                """,
                (code, account_uuid, provider_chat_key),
            )

    def enqueue(self, record: dict[str, object], priority: int) -> bool:
        if priority not in {0, 1, 2}:
            raise ValueError("Invalid operation priority")
        with self.session() as session:
            self._allocate_producer_lane(session, record)
            operation_uuid = str(record["operation_uuid"])
            operation_sha256 = str(record["operation_sha256"])
            prior = session.execute(
                """
                SELECT operation_sha256, terminal_outcome, manual_retry_allowed
                FROM operation_idempotency
                WHERE operation_uuid = %s
                """,
                (operation_uuid,),
            ).fetchone()
            if prior is not None and prior["operation_sha256"] != operation_sha256:
                raise ValueError("Operation UUID reused with a different digest")
            attempt = int(record["attempt"])
            if attempt > 1 and (
                prior is None
                or prior["terminal_outcome"] not in {"rejected", "expired"}
                or prior["manual_retry_allowed"] is not True
            ):
                raise ValueError("Higher attempt is not authorized by prior result")
            operation = typing.cast(dict[str, object], record["operation"])
            provider = typing.cast(dict[str, object], operation["provider"])
            assignment = session.execute(
                """
                SELECT resource_uuid, generation
                FROM desired_resources
                WHERE resource_type = 'external_chat_assignment'
                  AND NOT deleted
                  AND body->>'external_account_uuid' = %s
                  AND body->'provider_chat'->>'provider_chat_key' = %s
                  AND body->>'project_id' = %s
                  AND COALESCE((body->>'selected')::boolean, false)
                LIMIT 1
                """,
                (
                    str(record["account_uuid"]),
                    str(provider["chat_id"]),
                    str(record["project_uuid"]),
                ),
            ).fetchone()
            if assignment is None:
                raise ValueError("Operation does not match an active assignment")
            if attempt > 1:
                previous_attempt = session.execute(
                    """
                    SELECT max(attempt) AS attempt FROM bridge_operations
                    WHERE operation_uuid = %s
                    """,
                    (operation_uuid,),
                ).fetchone()
                if (
                    previous_attempt is None
                    or previous_attempt["attempt"] is None
                    or attempt != int(previous_attempt["attempt"]) + 1
                ):
                    raise ValueError("Manual retry attempt is not consecutive")
            session.execute(
                """
                INSERT INTO operation_idempotency (operation_uuid, operation_sha256)
                VALUES (%s, %s)
                ON CONFLICT (operation_uuid) DO NOTHING
                """,
                (operation_uuid, operation_sha256),
            )
            result = session.execute(
                """
                INSERT INTO bridge_operations (
                    record_uuid, operation_uuid, attempt, operation_sha256,
                    account_uuid, project_uuid, origin, causal_lane,
                    lane_sequence, predecessor_operation_uuid,
                    assignment_uuid, assignment_generation, priority, state,
                    expires_at, record
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, 'pending', %s, %s
                )
                ON CONFLICT (record_uuid) DO NOTHING
                RETURNING record_uuid
                """,
                (
                    str(record["record_uuid"]),
                    operation_uuid,
                    attempt,
                    operation_sha256,
                    str(record["account_uuid"]),
                    str(record["project_uuid"]),
                    str(record["origin"]),
                    str(record["causal_lane"]),
                    int(record["sequence"]),
                    record["predecessor_operation_uuid"],
                    str(assignment["resource_uuid"]),
                    int(assignment["generation"]),
                    priority,
                    record["expires_at"],
                    json.dumps(record),
                ),
            ).fetchone()
            return result is not None

    def bind_provider_lease(self, record: dict[str, object]) -> bool:
        """Attach a renewed backend lease to existing durable work or outcome."""
        transport = typing.cast(dict[str, object], record["transport"])
        with self.session() as session:
            updated = session.execute(
                """
                UPDATE bridge_operations
                SET record = jsonb_set(
                        record, '{transport}', %s::jsonb, true
                    ),
                    result_record = CASE
                        WHEN result_record IS NULL THEN NULL
                        ELSE jsonb_set(
                            result_record, '{transport}', %s::jsonb, true
                        )
                    END,
                    result_sent_at = CASE
                        WHEN result_record IS NULL THEN result_sent_at
                        ELSE NULL
                    END,
                    expires_at = %s,
                    updated_at = now()
                WHERE record_uuid = %s
                  AND operation_uuid = %s
                  AND operation_sha256 = %s
                RETURNING record_uuid
                """,
                (
                    json.dumps(transport),
                    json.dumps(transport),
                    record["expires_at"],
                    str(record["record_uuid"]),
                    str(record["operation_uuid"]),
                    str(record["operation_sha256"]),
                ),
            ).fetchone()
            return updated is not None

    def release_provider_event_submissions(self, record_uuids: list[str]) -> None:
        if not record_uuids:
            return
        with self.session() as session:
            session.execute(
                """
                UPDATE workspace_delivery_outbox
                SET submission_state = 'pending', next_submission_at = now()
                WHERE record_uuid = ANY(%s::uuid[])
                  AND submission_state = 'submitting'
                  AND sent_at IS NULL
                """,
                (record_uuids,),
            )

    def reject_provider_event_submission(
        self,
        record_uuid: str,
        error_code: str,
    ) -> bool:
        """Quarantine one permanent Provider API rejection for reconciliation."""
        with self.session() as session:
            row = session.execute(
                """
                WITH rejected AS (
                    UPDATE workspace_delivery_outbox
                    SET submission_state = 'rejected',
                        submission_error_code = %s
                    WHERE record_uuid = %s
                      AND submission_state = 'submitting'
                      AND sent_at IS NULL
                    RETURNING account_uuid, provider_queue_id, provider_event_id
                ), marked AS (
                    UPDATE zulip_provider_events AS event
                    SET processing_reason = 'workspace_delivery_rejected'
                    FROM rejected
                    WHERE event.account_uuid = rejected.account_uuid
                      AND event.queue_id = rejected.provider_queue_id
                      AND event.event_id = rejected.provider_event_id
                      AND event.processing_state = 'delivering'
                    RETURNING event.event_id
                )
                SELECT EXISTS (SELECT 1 FROM rejected) AS rejected
                """,
                (error_code[:128], record_uuid),
            ).fetchone()
            if bool(row["rejected"]):
                return True
            row = session.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM workspace_delivery_outbox
                    WHERE record_uuid = %s
                      AND (
                          sent_at IS NOT NULL
                          OR submission_state = 'rejected'
                      )
                ) AS terminal
                """,
                (record_uuid,),
            ).fetchone()
            return bool(row["terminal"])

    @staticmethod
    def _queued_operation(row: dict[str, object]) -> QueuedOperation:
        return QueuedOperation(
            record_uuid=uuid.UUID(str(row["record_uuid"])),
            record=typing.cast(dict[str, object], row["record"]),
            priority=int(row["priority"]),
            attempts=int(row["retry_count"]),
            provider_attempted_at=typing.cast(
                datetime.datetime | None, row["provider_attempted_at"]
            ),
            auto_resend_count=int(row["auto_resend_count"]),
            reconciliation_check_count=int(row["reconciliation_check_count"]),
            provider_rendered_content=typing.cast(
                str | None, row["provider_rendered_content"]
            ),
        )

    def claim_terminal(
        self, worker_id: str, lease_seconds: int = 60
    ) -> tuple[QueuedOperation, str] | None:
        """Claim pending work that can no longer call the provider safely."""
        with self.session() as session:
            row = session.execute(
                """
                WITH candidate AS (
                    SELECT operation.record_uuid,
                           CASE
                               WHEN operation.expires_at <= now() THEN 'expired'
                               WHEN account.generation =
                                    scheduler.provider_generation
                                AND scheduler.provider_state = 'auth_required'
                               THEN 'unauthorized_account'
                               ELSE 'cancelled'
                           END AS terminal_reason
                    FROM bridge_operations AS operation
                    LEFT JOIN desired_resources AS assignment
                      ON assignment.resource_type = 'external_chat_assignment'
                     AND assignment.resource_uuid = operation.assignment_uuid
                     AND NOT assignment.deleted
                    LEFT JOIN desired_resources AS account
                      ON account.resource_type = 'external_account'
                     AND account.resource_uuid = operation.account_uuid
                     AND NOT account.deleted
                    LEFT JOIN scheduler_accounts AS scheduler
                      ON scheduler.account_uuid = operation.account_uuid
                    WHERE operation.state = 'pending'
                      AND operation.available_at <= now()
                      AND (
                          operation.expires_at <= now()
                          OR assignment.resource_uuid IS NULL
                          OR assignment.generation <> operation.assignment_generation
                          OR assignment.body->>'project_id'
                                <> operation.project_uuid::text
                          OR NOT COALESCE(
                                (assignment.body->>'selected')::boolean, false
                             )
                          OR account.resource_uuid IS NULL
                          OR NOT COALESCE(
                                (account.body->>'synchronization_enabled')::boolean,
                                false
                             )
                          OR (
                              account.generation = scheduler.provider_generation
                              AND scheduler.provider_state = 'auth_required'
                          )
                      )
                    ORDER BY operation.created_at
                    FOR UPDATE OF operation SKIP LOCKED
                    LIMIT 1
                )
                UPDATE bridge_operations AS operation
                SET state = 'running', lease_owner = %s,
                    lease_until = now() + (%s * interval '1 second'),
                    updated_at = now()
                FROM candidate
                WHERE operation.record_uuid = candidate.record_uuid
                RETURNING operation.record_uuid, operation.record,
                          operation.priority, operation.retry_count,
                          operation.provider_attempted_at,
                          operation.auto_resend_count,
                          operation.reconciliation_check_count,
                          operation.manual_context->>'provider_rendered_content'
                              AS provider_rendered_content,
                          candidate.terminal_reason
                """,
                (worker_id, lease_seconds),
            ).fetchone()
            if row is None:
                return None
            return self._queued_operation(row), str(row["terminal_reason"])

    def claim(self, worker_id: str, lease_seconds: int = 60) -> QueuedOperation | None:
        with self.session() as session:
            row = session.execute(
                """
                WITH candidates AS (
                    SELECT operation.record_uuid
                    FROM bridge_operations AS operation
                    JOIN scheduler_accounts AS scheduler
                      ON scheduler.account_uuid = operation.account_uuid
                    JOIN desired_resources AS account
                      ON account.resource_type = 'external_account'
                     AND account.resource_uuid = operation.account_uuid
                     AND NOT account.deleted
                     AND account.generation = scheduler.provider_generation
                     AND COALESCE(
                           (account.body->>'synchronization_enabled')::boolean,
                           false
                         )
                    LEFT JOIN causal_lane_state AS lane
                      ON lane.origin = operation.origin
                     AND lane.causal_lane = operation.causal_lane
                    JOIN desired_resources AS assignment
                      ON assignment.resource_type = 'external_chat_assignment'
                     AND assignment.resource_uuid = operation.assignment_uuid
                     AND assignment.generation = operation.assignment_generation
                     AND NOT assignment.deleted
                     AND assignment.body->>'project_id' = operation.project_uuid::text
                     AND COALESCE(
                           (assignment.body->>'selected')::boolean, false
                         )
                    WHERE operation.state = 'pending'
                      AND operation.available_at <= now()
                      AND (operation.expires_at IS NULL OR operation.expires_at > now())
                      AND (
                          scheduler.provider_state = 'ready'
                          OR (
                              scheduler.provider_state = 'backoff'
                              AND scheduler.provider_retry_after <= now()
                          )
                      )
                      AND (
                          (
                              operation.attempt = 1
                              AND operation.lane_sequence =
                                  COALESCE(lane.last_sequence, 0) + 1
                              AND operation.predecessor_operation_uuid
                                  IS NOT DISTINCT FROM lane.last_operation_uuid
                          )
                          OR (
                              operation.attempt > 1
                              AND NOT EXISTS (
                                  SELECT 1 FROM bridge_operations AS later_delete
                                  WHERE later_delete.origin = operation.origin
                                    AND later_delete.causal_lane =
                                        operation.causal_lane
                                    AND later_delete.lane_sequence >
                                        operation.lane_sequence
                                    AND later_delete.state = 'committed'
                                    AND later_delete.record->'operation'->>'kind'
                                        IN (
                                            'message.delete', 'topic.delete',
                                            'stream.delete'
                                        )
                                    AND later_delete.record->'operation'
                                            ->>'entity_uuid' =
                                        operation.record->'operation'
                                            ->>'entity_uuid'
                              )
                          )
                      )
                    ORDER BY operation.priority,
                             scheduler.last_dispatched_at NULLS FIRST,
                             operation.available_at,
                             operation.created_at
                    FOR UPDATE OF operation SKIP LOCKED
                    LIMIT 1
                )
                UPDATE bridge_operations AS operation
                SET state = 'running', lease_owner = %s,
                    lease_until = now() + (%s * interval '1 second'),
                    updated_at = now()
                FROM candidates
                WHERE operation.record_uuid = candidates.record_uuid
                RETURNING operation.record_uuid, operation.record,
                          operation.priority, operation.retry_count,
                          operation.provider_attempted_at,
                          operation.auto_resend_count,
                          operation.reconciliation_check_count,
                          operation.manual_context->>'provider_rendered_content'
                              AS provider_rendered_content
                """,
                (worker_id, lease_seconds),
            ).fetchone()
            if row is None:
                return None
            record = typing.cast(dict[str, object], row["record"])
            session.execute(
                """
                INSERT INTO scheduler_accounts (account_uuid, last_dispatched_at)
                VALUES (%s, now())
                ON CONFLICT (account_uuid) DO UPDATE
                SET last_dispatched_at = EXCLUDED.last_dispatched_at
                """,
                (str(record["account_uuid"]),),
            )
            return self._queued_operation(row)

    def reap_expired_running(self) -> int:
        """Recover operations whose worker died while holding a lease.

        Once any provider attempt evidence exists, the operation can never be
        returned to the ordinary retry lane: doing so could duplicate a send.
        It is moved to reconciliation instead. Operations that provably did
        not reach the provider become pending again.
        """
        with self.session() as session:
            rows = session.execute(
                """
                UPDATE bridge_operations
                SET state = CASE
                        WHEN provider_attempted_at IS NOT NULL
                          OR provider_queue_id IS NOT NULL
                          OR provider_local_id IS NOT NULL
                        THEN 'uncertain'
                        ELSE 'pending'
                    END,
                    available_at = CASE
                        WHEN provider_attempted_at IS NULL
                         AND provider_queue_id IS NULL
                         AND provider_local_id IS NULL
                        THEN now()
                        ELSE available_at
                    END,
                    reconciliation_after = CASE
                        WHEN provider_attempted_at IS NOT NULL
                          OR provider_queue_id IS NOT NULL
                          OR provider_local_id IS NOT NULL
                        THEN now()
                        ELSE reconciliation_after
                    END,
                    reconciliation_check_count = CASE
                        WHEN provider_attempted_at IS NOT NULL
                          OR provider_queue_id IS NOT NULL
                          OR provider_local_id IS NOT NULL
                        THEN 0
                        ELSE reconciliation_check_count
                    END,
                    lease_owner = NULL, lease_until = NULL, updated_at = now()
                WHERE state = 'running' AND lease_until < now()
                RETURNING record_uuid
                """
            ).fetchall()
            return len(rows)

    def complete(
        self, item: QueuedOperation, result: dict[str, object], outcome: str
    ) -> None:
        if outcome not in {"committed", "rejected", "expired", "cancelled"}:
            raise ValueError("Invalid terminal outcome")
        with self.session() as session:
            current = session.execute(
                """
                SELECT state FROM bridge_operations
                WHERE record_uuid = %s
                FOR UPDATE
                """,
                (str(item.record_uuid),),
            ).fetchone()
            if current is None:
                raise ValueError("Unknown bridge operation")
            if current["state"] in {"committed", "rejected", "expired", "cancelled"}:
                return
            result_body = typing.cast(dict[str, object], result["result"])
            target_entity_id = result_body.get("provider_entity_id")
            target_revision = result_body.get("provider_revision")
            manual_retry_allowed = result_body.get("manual_retry_allowed") is True
            session.execute(
                """
                UPDATE bridge_operations
                SET state = %s, result_record = %s, lease_owner = NULL,
                    lease_until = NULL, updated_at = now()
                WHERE record_uuid = %s AND state IN ('running', 'uncertain')
                """,
                (outcome, json.dumps(result), str(item.record_uuid)),
            )
            session.execute(
                """
                UPDATE operation_idempotency
                SET terminal_outcome = %s, result_record_uuid = %s,
                    target_entity_id = %s, target_revision = %s,
                    manual_retry_allowed = %s,
                    updated_at = now()
                WHERE operation_uuid = %s
                """,
                (
                    outcome,
                    str(result["record_uuid"]),
                    target_entity_id,
                    target_revision,
                    manual_retry_allowed,
                    str(item.record["operation_uuid"]),
                ),
            )
            if int(item.record["attempt"]) == 1:
                session.execute(
                    """
                    INSERT INTO causal_lane_state (
                        origin, causal_lane, last_sequence, last_operation_uuid
                    ) VALUES (%s, %s, 0, NULL)
                    ON CONFLICT (origin, causal_lane) DO NOTHING
                    """,
                    (str(item.record["origin"]), str(item.record["causal_lane"])),
                )
                advanced = session.execute(
                    """
                    UPDATE causal_lane_state
                    SET last_sequence = %s, last_operation_uuid = %s,
                        updated_at = now()
                    WHERE origin = %s AND causal_lane = %s
                      AND last_sequence = %s
                      AND last_operation_uuid IS NOT DISTINCT FROM %s
                    RETURNING last_sequence
                    """,
                    (
                        int(item.record["sequence"]),
                        str(item.record["operation_uuid"]),
                        str(item.record["origin"]),
                        str(item.record["causal_lane"]),
                        int(item.record["sequence"]) - 1,
                        item.record["predecessor_operation_uuid"],
                    ),
                ).fetchone()
                if advanced is None:
                    raise ValueError("Causal lane state changed before completion")
            if outcome == "committed":
                self._persist_committed_mapping(
                    session,
                    item.record,
                    None if target_entity_id is None else str(target_entity_id),
                    None if target_revision is None else str(target_revision),
                )

    @staticmethod
    def _persist_committed_mapping(
        session: sessions.PgSQLSession,
        record: dict[str, object],
        provider_entity_id: str | None,
        provider_revision: str | None,
    ) -> None:
        operation = typing.cast(dict[str, object], record["operation"])
        kind = str(operation["kind"])
        payload = typing.cast(dict[str, object], operation["payload"])
        provider = typing.cast(dict[str, object], operation["provider"])
        account_uuid = str(record["account_uuid"])
        workspace_uuid = str(operation["entity_uuid"])
        if kind == "message.create":
            if provider_entity_id is None:
                raise ValueError("Committed message create has no provider identifier")
            session.execute(
                """
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    provider_revision, metadata, deleted
                ) VALUES (%s, 'message', %s, %s, %s, %s, false)
                ON CONFLICT (account_uuid, entity_kind, provider_id) DO UPDATE SET
                    provider_revision = EXCLUDED.provider_revision,
                    metadata = provider_mappings.metadata || EXCLUDED.metadata,
                    deleted = false,
                    updated_at = now()
                """,
                (
                    account_uuid,
                    workspace_uuid,
                    provider_entity_id,
                    provider_revision,
                    json.dumps(
                        {
                            "stream_uuid": payload["stream_uuid"],
                            "topic_uuid": payload["topic_uuid"],
                            "author_uuid": payload["author_uuid"],
                            "chat_key": provider["chat_id"],
                            "project_uuid": record["project_uuid"],
                            "causal_lane": record["causal_lane"],
                            "mapping_origin": str(record["origin"]),
                            "workspace_delivery_state": "committed",
                        }
                    ),
                ),
            )
            session.execute(
                """
                INSERT INTO provider_mapping_aliases (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    metadata, deleted
                ) VALUES (%s, 'message', %s, %s, %s, false)
                ON CONFLICT (account_uuid, entity_kind, workspace_uuid) DO UPDATE SET
                    provider_id = EXCLUDED.provider_id,
                    metadata = EXCLUDED.metadata,
                    deleted = false,
                    updated_at = now()
                """,
                (
                    account_uuid,
                    workspace_uuid,
                    provider_entity_id,
                    json.dumps(
                        {
                            "stream_uuid": payload["stream_uuid"],
                            "topic_uuid": payload["topic_uuid"],
                            "author_uuid": payload["author_uuid"],
                            "chat_key": provider["chat_id"],
                            "project_uuid": record["project_uuid"],
                            "causal_lane": record["causal_lane"],
                            "mapping_origin": str(record["origin"]),
                            "workspace_delivery_state": "committed",
                        }
                    ),
                ),
            )
        elif kind in {"reaction.create", "reaction.update"}:
            if provider_entity_id is None:
                raise ValueError(
                    "Committed reaction mutation has no provider identifier"
                )
            metadata = {
                "project_uuid": record["project_uuid"],
                "message_uuid": payload["message_uuid"],
                "user_uuid": payload["user_uuid"],
                "emoji_name": payload["emoji_name"],
                "chat_key": provider["chat_id"],
                "mapping_origin": str(record["origin"]),
                "workspace_delivery_state": "committed",
            }
            session.execute(
                """
                WITH removed_stale_workspace_mapping AS (
                    DELETE FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = 'reaction'
                      AND workspace_uuid = %s AND provider_id <> %s
                )
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    provider_revision, metadata, deleted
                ) VALUES (%s, 'reaction', %s, %s, %s, %s, false)
                ON CONFLICT (account_uuid, entity_kind, provider_id) DO UPDATE SET
                    workspace_uuid = EXCLUDED.workspace_uuid,
                    provider_revision = EXCLUDED.provider_revision,
                    metadata = provider_mappings.metadata || EXCLUDED.metadata,
                    deleted = false,
                    updated_at = now()
                """,
                (
                    account_uuid,
                    workspace_uuid,
                    provider_entity_id,
                    account_uuid,
                    workspace_uuid,
                    provider_entity_id,
                    provider_revision,
                    json.dumps(metadata),
                ),
            )
        elif kind == "message.update":
            extensions = typing.cast(dict[str, object], operation.get("extensions", {}))
            payload = typing.cast(dict[str, object], operation["payload"])
            session.execute(
                """
                UPDATE provider_mappings
                SET provider_revision = COALESCE(%s, provider_revision),
                    metadata = metadata || jsonb_strip_nulls(jsonb_build_object(
                        'content_sha256', %s::text,
                        'provider_content_sha256', %s::text,
                        'subject', %s::text,
                        'stream_uuid', %s::text,
                        'topic_uuid', %s::text,
                        'chat_key', %s::text,
                        'project_uuid', %s::text
                    )) || jsonb_build_object(
                        'workspace_delivery_state', 'committed'
                    ),
                    deleted = false, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = 'message'
                  AND workspace_uuid = %s
                """,
                (
                    provider_revision,
                    extensions.get("content_sha256"),
                    extensions.get("provider_content_sha256"),
                    extensions.get("subject"),
                    payload.get("stream_uuid"),
                    payload.get("topic_uuid"),
                    provider.get("chat_id"),
                    record.get("project_uuid"),
                    account_uuid,
                    workspace_uuid,
                ),
            )
            session.execute(
                """
                UPDATE provider_mapping_aliases
                SET metadata = metadata || jsonb_strip_nulls(jsonb_build_object(
                        'content_sha256', %s::text,
                        'provider_content_sha256', %s::text,
                        'subject', %s::text,
                        'stream_uuid', %s::text,
                        'topic_uuid', %s::text,
                        'chat_key', %s::text,
                        'project_uuid', %s::text
                    )) || jsonb_build_object(
                        'workspace_delivery_state', 'committed'
                    ),
                    deleted = false, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = 'message'
                  AND workspace_uuid = %s
                """,
                (
                    extensions.get("content_sha256"),
                    extensions.get("provider_content_sha256"),
                    extensions.get("subject"),
                    payload.get("stream_uuid"),
                    payload.get("topic_uuid"),
                    provider.get("chat_id"),
                    record.get("project_uuid"),
                    account_uuid,
                    workspace_uuid,
                ),
            )
        elif kind in {"topic.upsert", "stream.upsert"}:
            entity_kind = kind.partition(".")[0]
            session.execute(
                """
                UPDATE provider_mappings
                SET provider_revision = COALESCE(%s, provider_revision),
                    deleted = false, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = %s
                  AND workspace_uuid = %s
                """,
                (provider_revision, account_uuid, entity_kind, workspace_uuid),
            )
        elif kind == "message.delete":
            session.execute(
                """
                UPDATE provider_mappings
                SET deleted = true, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = 'message'
                  AND workspace_uuid = %s
                """,
                (account_uuid, workspace_uuid),
            )
            session.execute(
                """
                UPDATE provider_mapping_aliases
                SET deleted = true, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = 'message'
                  AND workspace_uuid = %s
                """,
                (account_uuid, workspace_uuid),
            )
        elif kind == "reaction.delete":
            session.execute(
                """
                UPDATE provider_mappings
                SET deleted = true, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = 'reaction'
                  AND workspace_uuid = %s
                """,
                (account_uuid, workspace_uuid),
            )

    def retry(
        self, item: QueuedOperation, available_at: datetime.datetime, code: str
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET state = 'pending', available_at = %s, last_error_code = %s,
                    retry_count = retry_count + 1,
                    lease_owner = NULL, lease_until = NULL, updated_at = now()
                WHERE record_uuid = %s
                """,
                (available_at, code, str(item.record_uuid)),
            )

    def record_provider_attempt(
        self,
        item: QueuedOperation,
        queue_id: str,
        local_id: str,
        last_event_id: int,
        provider_rendered_content: str,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET provider_queue_id = %s, provider_local_id = %s,
                    provider_attempted_at = COALESCE(provider_attempted_at, now()),
                    manual_context = COALESCE(manual_context, '{}'::jsonb)
                        || jsonb_build_object(
                            'provider_rendered_content', %s::text
                        ),
                    updated_at = now()
                WHERE record_uuid = %s AND state = 'running'
                """,
                (
                    queue_id,
                    local_id,
                    provider_rendered_content,
                    str(item.record_uuid),
                ),
            )
            # Queue registration/cursor ownership belongs exclusively to the
            # long-lived provider poller. Send correlation is operation-local.

    def mark_uncertain(self, item: QueuedOperation, code: str) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET state = 'uncertain', last_error_code = %s,
                    reconciliation_check_count = 0,
                    reconciliation_after = now() + interval '5 seconds',
                    lease_owner = NULL, lease_until = NULL, updated_at = now()
                WHERE record_uuid = %s
                """,
                (code, str(item.record_uuid)),
            )

    def claim_uncertain(self, worker_id: str) -> QueuedOperation | None:
        with self.session() as session:
            row = session.execute(
                """
                WITH candidate AS (
                    SELECT operation.record_uuid
                    FROM bridge_operations AS operation
                    JOIN scheduler_accounts AS scheduler
                      ON scheduler.account_uuid = operation.account_uuid
                    JOIN desired_resources AS account
                      ON account.resource_type = 'external_account'
                     AND account.resource_uuid = operation.account_uuid
                     AND NOT account.deleted
                     AND account.generation = scheduler.provider_generation
                    WHERE operation.state = 'uncertain'
                      AND NOT operation.manual_reconciliation_required
                      AND operation.reconciliation_after <= now()
                      AND (
                          operation.lease_until IS NULL
                          OR operation.lease_until < now()
                      )
                      AND (
                          scheduler.provider_state = 'ready'
                          OR (
                              scheduler.provider_state = 'backoff'
                              AND scheduler.provider_retry_after <= now()
                          )
                      )
                    ORDER BY operation.reconciliation_after,
                             operation.created_at
                    FOR UPDATE OF operation SKIP LOCKED
                    LIMIT 1
                )
                UPDATE bridge_operations AS operation
                SET lease_owner = %s, lease_until = now() + interval '60 seconds',
                    updated_at = now()
                FROM candidate
                WHERE operation.record_uuid = candidate.record_uuid
                RETURNING operation.record_uuid, operation.record,
                          operation.priority, operation.retry_count,
                          operation.provider_attempted_at,
                          operation.auto_resend_count,
                          operation.reconciliation_check_count,
                          operation.manual_context->>'provider_rendered_content'
                              AS provider_rendered_content
                """,
                (worker_id,),
            ).fetchone()
            if row is None:
                return None
            return QueuedOperation(
                record_uuid=uuid.UUID(str(row["record_uuid"])),
                record=typing.cast(dict[str, object], row["record"]),
                priority=int(row["priority"]),
                attempts=int(row["retry_count"]),
                provider_attempted_at=row["provider_attempted_at"],
                auto_resend_count=int(row["auto_resend_count"]),
                reconciliation_check_count=int(row["reconciliation_check_count"]),
                provider_rendered_content=row["provider_rendered_content"],
            )

    def defer_uncertain(
        self,
        item: QueuedOperation,
        available_at: datetime.datetime,
        code: str,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET reconciliation_after = %s, last_error_code = %s,
                    lease_owner = NULL, lease_until = NULL, updated_at = now()
                WHERE record_uuid = %s AND state = 'uncertain'
                """,
                (available_at, code[:128], str(item.record_uuid)),
            )

    def schedule_reconciliation_check(
        self,
        item: QueuedOperation,
        after: datetime.datetime,
        evidence: dict[str, object],
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET reconciliation_check_count = reconciliation_check_count + 1,
                    reconciliation_after = %s,
                    reconciliation_evidence = reconciliation_evidence || %s::jsonb,
                    lease_owner = NULL, lease_until = NULL, updated_at = now()
                WHERE record_uuid = %s AND state = 'uncertain'
                """,
                (after, json.dumps([evidence]), str(item.record_uuid)),
            )

    def schedule_single_resend(
        self, item: QueuedOperation, evidence: dict[str, object]
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET state = 'pending', available_at = now(),
                    auto_resend_count = auto_resend_count + 1,
                    reconciliation_evidence = reconciliation_evidence || %s::jsonb,
                    lease_owner = NULL, lease_until = NULL, updated_at = now()
                WHERE record_uuid = %s AND state = 'uncertain'
                  AND auto_resend_count = 0
                """,
                (json.dumps([evidence]), str(item.record_uuid)),
            )

    def require_operation_manual_reconciliation(
        self, item: QueuedOperation, code: str, evidence: dict[str, object]
    ) -> None:
        now = datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
        record = item.record
        result: dict[str, object] = {
            "schema": record["schema"],
            "schema_version": record["schema_version"],
            "record_kind": "result",
            "record_uuid": str(uuid.uuid5(item.record_uuid, f"manual:{code}")),
            "operation_uuid": record["operation_uuid"],
            "attempt": record["attempt"],
            "operation_sha256": record["operation_sha256"],
            "account_uuid": record["account_uuid"],
            "project_uuid": record["project_uuid"],
            "origin": record["origin"],
            "causal_lane": record["causal_lane"],
            "sequence": record["sequence"],
            "predecessor_operation_uuid": record["predecessor_operation_uuid"],
            "created_at": now,
            "expires_at": record["expires_at"],
            "in_reply_to_record_uuid": record["record_uuid"],
            "result": {
                "outcome": "manual_reconciliation_required",
                "committed_at": None,
                "provider_entity_id": None,
                "provider_revision": None,
                "safe_error": {
                    "code": code,
                    "message": "The provider operation requires reconciliation.",
                },
                "manual_retry_allowed": False,
                "reconciliation": {"reason": code, "evidence": evidence},
            },
        }
        if "transport" in record:
            result["transport"] = record["transport"]
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET state = 'rejected',
                    manual_reconciliation_required = true,
                    last_error_code = %s,
                    reconciliation_evidence = reconciliation_evidence || %s::jsonb,
                    result_record = %s::jsonb,
                    result_sent_at = NULL,
                    manual_context = jsonb_build_object(
                        'operation_uuid', operation_uuid,
                        'account_uuid', account_uuid,
                        'causal_lane', causal_lane,
                        'original_link', NULL,
                        'duplicate_risk_warning',
                        'An explicit retry may create a duplicate Zulip message.'
                    ),
                    lease_owner = NULL, lease_until = NULL, updated_at = now()
                WHERE record_uuid = %s AND state = 'uncertain'
                """,
                (
                    code,
                    json.dumps([evidence]),
                    json.dumps(result),
                    str(item.record_uuid),
                ),
            )
            session.execute(
                """
                UPDATE operation_idempotency
                SET terminal_outcome = 'rejected',
                    result_record_uuid = %s,
                    manual_retry_allowed = false,
                    updated_at = now()
                WHERE operation_uuid = %s
                  AND terminal_outcome IS NULL
                """,
                (str(result["record_uuid"]), str(record["operation_uuid"])),
            )

    def provider_event_cursor(self, account_uuid: str) -> dict[str, object] | None:
        with self.session() as session:
            return session.execute(
                """
                SELECT queue_id, last_event_id, provider_realm_uuid,
                       provider_owner_user_id, provider_account_generation
                FROM zulip_event_cursors
                WHERE account_uuid = %s
                """,
                (account_uuid,),
            ).fetchone()

    def update_provider_event_cursor(
        self,
        account_uuid: str,
        queue_id: str,
        last_event_id: int,
        provider_realm_uuid: str | None = None,
        provider_owner_user_id: str | None = None,
        provider_account_generation: int | None = None,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                INSERT INTO zulip_event_cursors (
                    account_uuid, queue_id, last_event_id,
                    provider_realm_uuid, provider_owner_user_id,
                    provider_account_generation
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_uuid) DO UPDATE SET
                    queue_id = EXCLUDED.queue_id,
                    last_event_id = CASE
                        WHEN zulip_event_cursors.queue_id = EXCLUDED.queue_id
                        THEN GREATEST(
                            zulip_event_cursors.last_event_id,
                            EXCLUDED.last_event_id
                        )
                        ELSE EXCLUDED.last_event_id
                    END,
                    provider_realm_uuid = COALESCE(
                        EXCLUDED.provider_realm_uuid,
                        zulip_event_cursors.provider_realm_uuid
                    ),
                    provider_owner_user_id = COALESCE(
                        EXCLUDED.provider_owner_user_id,
                        zulip_event_cursors.provider_owner_user_id
                    ),
                    provider_account_generation = COALESCE(
                        EXCLUDED.provider_account_generation,
                        zulip_event_cursors.provider_account_generation
                    ),
                    updated_at = now()
                """,
                (
                    account_uuid,
                    queue_id,
                    last_event_id,
                    provider_realm_uuid,
                    provider_owner_user_id,
                    provider_account_generation,
                ),
            )

    def record_provider_event(
        self, account_uuid: str, queue_id: str, event: dict[str, object]
    ) -> bool:
        with self.session() as session:
            result = session.execute(
                """
                WITH registered_account AS (
                    INSERT INTO scheduler_accounts (account_uuid)
                    VALUES (%s)
                    ON CONFLICT (account_uuid) DO NOTHING
                )
                INSERT INTO zulip_provider_events (
                    account_uuid, queue_id, event_id, event_type, body
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (account_uuid, queue_id, event_id) DO NOTHING
                RETURNING event_id
                """,
                (
                    account_uuid,
                    account_uuid,
                    queue_id,
                    int(event["id"]),
                    str(event["type"]),
                    json.dumps(event),
                ),
            ).fetchone()
            return result is not None

    def invalidate_provider_event_cursor(self, account_uuid: str) -> None:
        with self.session() as session:
            session.execute(
                "DELETE FROM zulip_event_cursors WHERE account_uuid = %s",
                (account_uuid,),
            )

    def begin_provider_queue_catchup(self, account_uuid: str) -> None:
        """Persist the recovery boundary before discarding a dead queue."""
        with self.session() as session:
            session.execute(
                """
                WITH selected_chats AS (
                    SELECT
                        (assignment.body->>'external_account_uuid')::uuid
                            AS account_uuid,
                        assignment.body->'provider_chat'->>'provider_chat_key'
                            AS provider_chat_key
                    FROM desired_resources AS assignment
                    WHERE assignment.resource_type = 'external_chat_assignment'
                      AND NOT assignment.deleted
                      AND assignment.body->>'external_account_uuid' = %s
                      AND COALESCE(
                          (assignment.body->>'selected')::boolean, true
                      )
                    UNION
                    SELECT mapping.account_uuid,
                           mapping.metadata->>'chat_key'
                    FROM provider_mappings AS mapping
                    JOIN desired_resources AS account
                      ON account.resource_type = 'external_account'
                     AND account.resource_uuid = mapping.account_uuid
                     AND NOT account.deleted
                    WHERE mapping.account_uuid = %s
                      AND mapping.entity_kind = 'message'
                      AND NOT mapping.deleted
                      AND account.body->'settings'->>'selection_mode' = 'all'
                      AND mapping.metadata->>'chat_key' IS NOT NULL
                )
                INSERT INTO zulip_queue_catchup_jobs (
                    account_uuid, provider_chat_key,
                    checkpoint_provider_message_id, state
                )
                SELECT
                    selected.account_uuid,
                    selected.provider_chat_key,
                    max(
                        CASE WHEN mapping.provider_id ~ '^[0-9]+$'
                             THEN mapping.provider_id::bigint ELSE NULL END
                    ),
                    'pending'
                FROM selected_chats AS selected
                LEFT JOIN provider_mappings AS mapping
                  ON mapping.account_uuid = selected.account_uuid
                 AND mapping.entity_kind = 'message'
                 AND NOT mapping.deleted
                 AND mapping.metadata->>'chat_key' = selected.provider_chat_key
                GROUP BY selected.account_uuid, selected.provider_chat_key
                ON CONFLICT (account_uuid, provider_chat_key) DO UPDATE SET
                    checkpoint_provider_message_id =
                        EXCLUDED.checkpoint_provider_message_id,
                    next_anchor = NULL,
                    seen_provider_message_ids = '[]'::jsonb,
                    page_count = 0,
                    state = 'pending', safe_error_code = NULL,
                    updated_at = now()
                """,
                (account_uuid, account_uuid),
            )
            session.execute(
                """
                UPDATE zulip_participant_sync AS participant_sync
                SET state = 'pending', lease_until = NULL,
                    provider_user_ids = '[]'::jsonb, updated_at = now()
                FROM desired_resources AS assignment
                WHERE participant_sync.account_uuid = %s
                  AND assignment.resource_type = 'external_chat_assignment'
                  AND NOT assignment.deleted
                  AND assignment.body->>'external_account_uuid' =
                      participant_sync.account_uuid::text
                  AND assignment.body->'provider_chat'
                          ->>'provider_chat_key' =
                      participant_sync.provider_chat_key
                  AND assignment.generation =
                      participant_sync.assignment_generation
                  AND COALESCE(
                      (assignment.body->>'selected')::boolean, true
                  )
                """,
                (account_uuid,),
            )
            session.execute(
                """
                UPDATE zulip_backfill_jobs AS job
                SET next_anchor = NULL,
                    state = CASE
                        WHEN job.history_depth = 'new' THEN 'complete'
                        ELSE 'pending'
                    END,
                    available_at = now(), retry_count = 0,
                    last_error_code = NULL, lease_until = NULL,
                    updated_at = now()
                WHERE job.account_uuid = %s
                  AND EXISTS (
                      SELECT 1 FROM desired_resources AS assignment
                      WHERE assignment.resource_type =
                            'external_chat_assignment'
                        AND NOT assignment.deleted
                        AND assignment.body->>'external_account_uuid' =
                            job.account_uuid::text
                        AND assignment.body->'provider_chat'
                                ->>'provider_chat_key' =
                            job.provider_chat_key
                        AND COALESCE(
                            (assignment.body->>'selected')::boolean, true
                        )
                  )
                """,
                (account_uuid,),
            )

    def pending_provider_catchup(self, account_uuid: str) -> dict[str, object] | None:
        with self.session() as session:
            return session.execute(
                """
                SELECT account_uuid, provider_chat_key,
                       checkpoint_provider_message_id, next_anchor,
                       seen_provider_message_ids, page_count
                FROM zulip_queue_catchup_jobs
                WHERE account_uuid = %s AND state = 'pending'
                ORDER BY updated_at, provider_chat_key
                LIMIT 1
                """,
                (account_uuid,),
            ).fetchone()

    def provider_catchup_ready(self, account_uuid: str) -> bool:
        with self.session() as session:
            row = session.execute(
                """
                SELECT NOT EXISTS (
                    SELECT 1 FROM zulip_queue_catchup_jobs
                    WHERE account_uuid = %s AND state <> 'complete'
                ) AS ready
                """,
                (account_uuid,),
            ).fetchone()
            return bool(row["ready"])

    def mapped_provider_messages(
        self, account_uuid: str, provider_chat_key: str, minimum_id: int
    ) -> list[dict[str, object]]:
        with self.session() as session:
            return list(
                session.execute(
                    """
                    SELECT workspace_uuid, provider_id, provider_revision, metadata
                    FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = 'message'
                      AND NOT deleted
                      AND metadata->>'chat_key' = %s
                      AND provider_id ~ '^[0-9]+$'
                      AND provider_id::bigint >= %s
                    ORDER BY provider_id::bigint DESC
                    """,
                    (account_uuid, provider_chat_key, minimum_id),
                ).fetchall()
            )

    def advance_provider_catchup(
        self,
        account_uuid: str,
        provider_chat_key: str,
        seen_ids: list[int],
        next_anchor: int | None,
        complete: bool,
        safe_error_code: str | None = None,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_queue_catchup_jobs
                SET seen_provider_message_ids = (
                        SELECT jsonb_agg(DISTINCT value)
                        FROM jsonb_array_elements(
                            seen_provider_message_ids || %s::jsonb
                        ) AS values(value)
                    ),
                    next_anchor = %s,
                    page_count = page_count + 1,
                    state = CASE
                        WHEN %s::text IS NOT NULL THEN 'manual'
                        WHEN %s THEN 'complete'
                        ELSE 'pending'
                    END,
                    safe_error_code = %s,
                    updated_at = now()
                WHERE account_uuid = %s AND provider_chat_key = %s
                """,
                (
                    json.dumps(seen_ids),
                    next_anchor,
                    safe_error_code,
                    complete,
                    safe_error_code,
                    account_uuid,
                    provider_chat_key,
                ),
            )

    def uncertain_by_local_id(
        self, account_uuid: str, queue_id: str, local_id: str
    ) -> QueuedOperation | None:
        with self.session() as session:
            row = session.execute(
                """
                SELECT record_uuid, record, priority, retry_count
                     , provider_attempted_at, auto_resend_count
                     , reconciliation_check_count
                     , manual_context->>'provider_rendered_content'
                         AS provider_rendered_content
                FROM bridge_operations
                WHERE account_uuid = %s AND provider_queue_id = %s
                  AND provider_local_id = %s AND state = 'uncertain'
                """,
                (account_uuid, queue_id, local_id),
            ).fetchone()
            if row is None:
                return None
            return QueuedOperation(
                record_uuid=uuid.UUID(str(row["record_uuid"])),
                record=typing.cast(dict[str, object], row["record"]),
                priority=int(row["priority"]),
                attempts=int(row["retry_count"]),
                provider_attempted_at=row["provider_attempted_at"],
                auto_resend_count=int(row["auto_resend_count"]),
                reconciliation_check_count=int(row["reconciliation_check_count"]),
                provider_rendered_content=row["provider_rendered_content"],
            )

    def require_manual_reconciliation(self, account_uuid: str, code: str) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET manual_reconciliation_required = true,
                    last_error_code = %s,
                    manual_context = jsonb_build_object(
                        'operation_uuid', operation_uuid,
                        'account_uuid', account_uuid,
                        'causal_lane', causal_lane,
                        'original_link', NULL,
                        'duplicate_risk_warning',
                        'An explicit retry may create a duplicate Zulip message.'
                    ),
                    updated_at = now()
                WHERE account_uuid = %s AND state = 'uncertain'
                """,
                (code, account_uuid),
            )

    def pending_results(self, limit: int = 100) -> list[dict[str, object]]:
        with self.session() as session:
            rows = session.execute(
                """
                SELECT result_record FROM bridge_operations
                WHERE result_record IS NOT NULL AND result_sent_at IS NULL
                ORDER BY updated_at LIMIT %s
                """,
                (limit,),
            ).fetchall()
            return [
                typing.cast(dict[str, object], row["result_record"]) for row in rows
            ]

    def mark_result_sent(self, record_uuid: str) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations SET result_sent_at = now(), updated_at = now()
                WHERE result_record->>'record_uuid' = %s
                """,
                (record_uuid,),
            )

    def finalize_provider_result_response(self, record_uuid: str, status: str) -> None:
        """Persist one terminal Provider API acknowledgement without retry loops."""
        if status not in {
            "applied",
            "duplicate",
            "conflict",
            "not_found",
            "stale_lease",
            "rejected",
        }:
            raise ValueError("Unsupported Provider result response status")
        manual = status in {"conflict", "not_found", "rejected"}
        code = (
            None if status in {"applied", "duplicate"} else f"provider_result_{status}"
        )
        with self.session() as session:
            session.execute(
                """
                UPDATE bridge_operations
                SET result_sent_at = now(),
                    manual_reconciliation_required =
                        manual_reconciliation_required OR %s::boolean,
                    last_error_code = COALESCE(%s::text, last_error_code),
                    reconciliation_evidence = CASE
                        WHEN %s::text IS NULL THEN reconciliation_evidence
                        ELSE reconciliation_evidence || jsonb_build_array(
                            jsonb_build_object(
                                'kind', 'provider_result_response',
                                'status', %s::text
                            )
                        )
                    END,
                    updated_at = now()
                WHERE result_record->>'record_uuid' = %s
                """,
                (manual, code, code, status, record_uuid),
            )

    def accept_result(self, result: dict[str, object]) -> None:
        result_body = typing.cast(dict[str, object], result["result"])
        outcome = str(result_body["outcome"])
        with self.session() as session:
            row = session.execute(
                """
                SELECT operation.operation_sha256, operation.terminal_outcome,
                       operation.result_record_uuid, operation.target_entity_id,
                       operation.target_revision, operation.manual_retry_allowed,
                       delivery.record
                FROM operation_idempotency AS operation
                JOIN workspace_delivery_outbox AS delivery
                  ON delivery.operation_uuid = operation.operation_uuid
                WHERE operation.operation_uuid = %s
                FOR UPDATE OF operation
                """,
                (str(result["operation_uuid"]),),
            ).fetchone()
            if row is None:
                raise ValueError("Result does not match a known operation")
            if row["operation_sha256"] != result["operation_sha256"]:
                raise ValueError("Result operation digest mismatch")
            operation_record = typing.cast(dict[str, object], row["record"])
            exact_fields = (
                "operation_uuid",
                "attempt",
                "account_uuid",
                "project_uuid",
                "origin",
                "causal_lane",
                "sequence",
                "predecessor_operation_uuid",
            )
            if any(
                result.get(field) != operation_record.get(field)
                for field in exact_fields
            ):
                raise ValueError("Result operation binding mismatch")
            if result.get("in_reply_to_record_uuid") != operation_record.get(
                "record_uuid"
            ):
                raise ValueError("Result record binding mismatch")
            prior_result_uuid = row["result_record_uuid"]
            if row["terminal_outcome"] is not None:
                exact_result = (
                    row["terminal_outcome"] == outcome
                    and prior_result_uuid is not None
                    and str(prior_result_uuid) == str(result["record_uuid"])
                )
                same_target = (
                    row["terminal_outcome"] == outcome
                    and row["target_entity_id"] == result_body.get("provider_entity_id")
                    and row["target_revision"] == result_body.get("provider_revision")
                    and row["manual_retry_allowed"]
                    == (result_body.get("manual_retry_allowed") is True)
                )
                if exact_result or same_target:
                    session.execute(
                        """
                        UPDATE workspace_delivery_outbox
                        SET sent_at = COALESCE(sent_at, now()),
                            submission_state = 'sent'
                        WHERE operation_uuid = %s
                        """,
                        (str(result["operation_uuid"]),),
                    )
                    return
                raise ValueError("Stale result cannot replace terminal outcome")
            session.execute(
                """
                UPDATE operation_idempotency
                SET terminal_outcome = %s, result_record_uuid = %s,
                    target_entity_id = %s, target_revision = %s,
                    manual_retry_allowed = %s,
                    updated_at = now()
                WHERE operation_uuid = %s AND terminal_outcome IS NULL
                """,
                (
                    outcome,
                    str(result["record_uuid"]),
                    result_body.get("provider_entity_id"),
                    result_body.get("provider_revision"),
                    result_body.get("manual_retry_allowed") is True,
                    str(result["operation_uuid"]),
                ),
            )
            session.execute(
                """
                UPDATE workspace_delivery_outbox
                SET sent_at = COALESCE(sent_at, now()), submission_state = 'sent'
                WHERE operation_uuid = %s
                """,
                (str(result["operation_uuid"]),),
            )
            operation = typing.cast(
                dict[str, object] | None, operation_record.get("operation")
            )
            if (
                outcome == "committed"
                and operation_record.get("origin") == "zulip"
                and operation is not None
            ):
                provider = typing.cast(dict[str, object], operation["provider"])
                kind = operation.get("kind")
                if kind in {"reaction.upsert", "reaction.delete"}:
                    self._commit_reaction_mapping_transition(
                        session,
                        operation_record,
                    )
                elif kind == "message.delete":
                    session.execute(
                        """
                        UPDATE provider_mappings
                        SET deleted = true, updated_at = now()
                        WHERE account_uuid = %s AND entity_kind = 'message'
                          AND workspace_uuid = %s AND NOT deleted
                        """,
                        (
                            str(operation_record["account_uuid"]),
                            str(operation["entity_uuid"]),
                        ),
                    )
                    session.execute(
                        """
                        UPDATE provider_mapping_aliases
                        SET deleted = true, updated_at = now()
                        WHERE account_uuid = %s AND entity_kind = 'message'
                          AND workspace_uuid = %s AND NOT deleted
                        """,
                        (
                            str(operation_record["account_uuid"]),
                            str(operation["entity_uuid"]),
                        ),
                    )
                elif kind == "message.update":
                    payload = typing.cast(dict[str, object], operation["payload"])
                    extensions = typing.cast(
                        dict[str, object], operation.get("extensions", {})
                    )
                    session.execute(
                        """
                        UPDATE provider_mappings
                        SET metadata = metadata || jsonb_strip_nulls(
                                jsonb_build_object(
                                    'project_uuid', %s::text,
                                    'stream_uuid', %s::text,
                                    'topic_uuid', %s::text,
                                    'chat_key', %s::text,
                                    'causal_lane', %s::text,
                                    'subject', %s::text
                                )
                            ) || jsonb_build_object(
                                'workspace_delivery_state', 'committed'
                            ),
                            updated_at = now()
                        WHERE account_uuid = %s AND entity_kind = 'message'
                          AND workspace_uuid = %s AND NOT deleted
                        """,
                        (
                            str(operation_record["project_uuid"]),
                            str(payload["stream_uuid"]),
                            str(payload["topic_uuid"]),
                            str(provider["chat_id"]),
                            str(operation_record["causal_lane"]),
                            extensions.get("subject"),
                            str(operation_record["account_uuid"]),
                            str(operation["entity_uuid"]),
                        ),
                    )
                    session.execute(
                        """
                        UPDATE provider_mapping_aliases
                        SET metadata = metadata || jsonb_strip_nulls(
                                jsonb_build_object(
                                    'project_uuid', %s::text,
                                    'stream_uuid', %s::text,
                                    'topic_uuid', %s::text,
                                    'chat_key', %s::text,
                                    'causal_lane', %s::text,
                                    'subject', %s::text
                                )
                            ) || jsonb_build_object(
                                'workspace_delivery_state', 'committed'
                            ),
                            updated_at = now()
                        WHERE account_uuid = %s AND entity_kind = 'message'
                          AND workspace_uuid = %s AND NOT deleted
                        """,
                        (
                            str(operation_record["project_uuid"]),
                            str(payload["stream_uuid"]),
                            str(payload["topic_uuid"]),
                            str(provider["chat_id"]),
                            str(operation_record["causal_lane"]),
                            extensions.get("subject"),
                            str(operation_record["account_uuid"]),
                            str(operation["entity_uuid"]),
                        ),
                    )
                elif kind == "message.create":
                    self._persist_committed_mapping(
                        session,
                        operation_record,
                        str(provider["entity_id"]),
                        (
                            None
                            if result_body.get("provider_revision") is None
                            else str(result_body["provider_revision"])
                        ),
                    )
                elif kind == "topic.upsert":
                    entity_kind = kind.partition(".")[0]
                    session.execute(
                        """
                        UPDATE provider_mappings
                        SET metadata = jsonb_set(
                                metadata,
                                '{workspace_delivery_state}',
                                '"committed"'::jsonb,
                                true
                            ),
                            updated_at = now()
                        WHERE account_uuid = %s AND entity_kind = %s
                          AND provider_id = %s AND workspace_uuid = %s
                          AND NOT deleted
                        """,
                        (
                            str(operation_record["account_uuid"]),
                            entity_kind,
                            str(provider["entity_id"]),
                            str(operation["entity_uuid"]),
                        ),
                    )

    def enqueue_observed_report(self, report: dict[str, object]) -> bool:
        with self.session() as session:
            previous = session.execute(
                """
                SELECT body FROM (
                    SELECT body, result_status, completed_at
                    FROM observed_report_outbox
                    WHERE body->>'resource_type' = %s
                      AND body->>'resource_uuid' = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                ) AS latest
                WHERE result_status IS DISTINCT FROM 'rejected'
                   OR completed_at > clock_timestamp() - interval '5 minutes'
                """,
                (str(report["resource_type"]), str(report["resource_uuid"])),
            ).fetchone()
            if previous is not None:
                previous_body = typing.cast(dict[str, object], previous["body"])
                previous_semantic = {
                    key: value
                    for key, value in previous_body.items()
                    if key not in {"report_uuid", "observed_at"}
                }
                report_semantic = {
                    key: value
                    for key, value in report.items()
                    if key not in {"report_uuid", "observed_at"}
                }
                for semantic in (previous_semantic, report_semantic):
                    progress = semantic.get("progress")
                    if isinstance(progress, dict):
                        semantic["progress"] = {
                            key: value
                            for key, value in progress.items()
                            if key != "last_progress_at"
                        }
                if previous_semantic == report_semantic:
                    return False
            row = session.execute(
                """
                INSERT INTO observed_report_outbox (report_uuid, body)
                VALUES (%s, %s)
                ON CONFLICT (report_uuid) DO NOTHING
                RETURNING report_uuid
                """,
                (str(report["report_uuid"]), json.dumps(report)),
            ).fetchone()
            return row is not None

    def pending_observed_reports(self, limit: int = 500) -> list[dict[str, object]]:
        with self.session() as session:
            session.execute(
                """
                WITH ranked AS (
                    SELECT report_uuid,
                           row_number() OVER (
                               PARTITION BY body->>'resource_type',
                                            body->>'resource_uuid'
                               ORDER BY created_at DESC, report_uuid DESC
                           ) AS position
                    FROM observed_report_outbox
                    WHERE completed_at IS NULL
                )
                UPDATE observed_report_outbox AS report
                SET completed_at = now(), result_status = 'superseded'
                FROM ranked
                WHERE report.report_uuid = ranked.report_uuid
                  AND ranked.position > 1
                """
            )
            rows = session.execute(
                """
                WITH dependency_chats AS (
                    SELECT DISTINCT
                           account_uuid::text AS account_uuid,
                           'channel:' || (body->'message'->>'stream_id')
                               AS provider_chat_key
                    FROM zulip_provider_events
                    WHERE processing_state = 'pending'
                      AND body->>'type' = 'message'
                      AND body->'message'->>'stream_id' IS NOT NULL
                ), pending AS (
                    SELECT report.body, report.created_at, report.report_uuid,
                           dependency.account_uuid IS NOT NULL
                               AS live_dependency,
                           row_number() OVER (
                               PARTITION BY COALESCE(
                                   report.body->'catalog'->>'external_account_uuid',
                                   CASE
                                       WHEN report.body->>'resource_type' =
                                            'external_account'
                                       THEN report.body->>'resource_uuid'
                                   END,
                                   report.body->>'resource_uuid'
                               )
                               ORDER BY
                                   (dependency.account_uuid IS NOT NULL) DESC,
                                   report.created_at,
                                   report.report_uuid
                           ) AS account_position
                    FROM observed_report_outbox AS report
                    LEFT JOIN dependency_chats AS dependency
                      ON dependency.account_uuid =
                           report.body->'catalog'->>'external_account_uuid'
                     AND dependency.provider_chat_key =
                           report.body->'catalog'->'source'->>'provider_chat_key'
                    WHERE report.completed_at IS NULL
                      AND report.available_at <= now()
                )
                SELECT body FROM pending
                ORDER BY
                         (body->>'resource_type' = 'external_account') DESC,
                         account_position, live_dependency DESC,
                         created_at, report_uuid
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
            return [typing.cast(dict[str, object], row["body"]) for row in rows]

    def apply_observed_report_results(self, results: list[dict[str, object]]) -> None:
        terminal_statuses = {"applied", "duplicate", "stale"}
        with self.session() as session:
            for result in results:
                report_uuid = str(result["report_uuid"])
                status = str(result["status"])
                safe_error = result.get("safe_error")
                retryable = (
                    isinstance(safe_error, dict) and safe_error.get("retryable") is True
                )
                if status in terminal_statuses or (
                    status == "rejected" and not retryable
                ):
                    session.execute(
                        """
                        UPDATE observed_report_outbox
                        SET completed_at = now(), result_status = %s
                        WHERE report_uuid = %s
                        """,
                        (status, report_uuid),
                    )
                    continue
                session.execute(
                    """
                    UPDATE observed_report_outbox
                    SET attempts = attempts + 1,
                        available_at = now() + (
                            LEAST(300, (1 << LEAST(attempts, 8))) * interval '1 second'
                        )
                    WHERE report_uuid = %s AND completed_at IS NULL
                    """,
                    (report_uuid,),
                )

    def mark_health(self, component: str, status: str, code: str | None = None) -> None:
        with self.session() as session:
            session.execute(
                """
                INSERT INTO bridge_health (
                    component, status, progressed_at, safe_error_code
                ) VALUES (%s, %s, now(), %s)
                ON CONFLICT (component) DO UPDATE SET
                    status = EXCLUDED.status,
                    progressed_at = EXCLUDED.progressed_at,
                    safe_error_code = EXCLUDED.safe_error_code,
                    updated_at = now()
                """,
                (component, status, code),
            )

    def health(self) -> list[dict[str, object]]:
        with self.session() as session:
            return list(
                session.execute(
                    """
                    SELECT component, status, progressed_at, safe_error_code
                    FROM bridge_health ORDER BY component
                    """
                ).fetchall()
            )
