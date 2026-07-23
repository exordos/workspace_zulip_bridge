import contextlib
import dataclasses
import datetime
import hashlib
import json
import threading
import typing
import uuid

from restalchemy.storage.sql import engines, sessions

from workspace_zulip_bridge import canonical, control


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
        merged["is_owner"] = bool(prior.get("is_owner")) or bool(
            value.get("is_owner")
        )
        for name in ("email", "avatar_urn"):
            if not merged.get(name) and value.get(name):
                merged[name] = value[name]
        prior_name = str(prior.get("display_name", "")).strip()
        observed_name = str(value.get("display_name", "")).strip()
        if (not prior_name or prior_name == provider_user_id) and observed_name:
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

    def provider_event_cursor(self, account_uuid: str) -> dict[str, object] | None: ...

    def update_provider_event_cursor(
        self, account_uuid: str, queue_id: str, last_event_id: int
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
    engine_name = "workspace_zulip_bridge_" + hashlib.sha256(
        connection_url.encode("utf-8")
    ).hexdigest()
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

    @contextlib.contextmanager
    def session(self) -> typing.Iterator[sessions.PgSQLSession]:
        with _engine_for(self.connection_url).session_manager() as session:
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
                if resource_type == "external_chat_assignment":
                    if operation == "upsert":
                        if previous is not None and previous["body"] is not None:
                            self._tombstone_workspace_projection(
                                session,
                                typing.cast(dict[str, object], previous["body"]),
                            )
                        self._materialize_workspace_projection(
                            session, typing.cast(dict[str, object], body)
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
            session.execute(
                """
                UPDATE bridge_metadata SET control_cursor = %s, updated_at = now()
                WHERE singleton
                """,
                (anchor_cursor,),
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
            session.execute(
                """
                WITH removed_stale_workspace_mapping AS (
                    DELETE FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = 'identity'
                      AND workspace_uuid = %s AND provider_id <> %s
                )
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    metadata, deleted
                ) VALUES (%s, 'identity', %s, %s, %s, false)
                ON CONFLICT (account_uuid, entity_kind, provider_id) DO UPDATE SET
                    workspace_uuid = EXCLUDED.workspace_uuid,
                    metadata = EXCLUDED.metadata,
                    deleted = false, updated_at = now()
                """,
                (
                    account_uuid,
                    identity_uuid,
                    str(raw_participant["provider_user_id"]),
                    account_uuid,
                    identity_uuid,
                    str(raw_participant["provider_user_id"]),
                    json.dumps(
                        {
                            "display_name": raw_participant["display_name"],
                            "email": raw_participant.get("email"),
                            "avatar_urn": raw_participant.get("avatar_urn"),
                            "active": True,
                            "role": raw_participant["role"],
                        }
                    ),
                ),
            )
        stream_uuid = str(stream["uuid"])
        session.execute(
            """
            WITH removed_stale_workspace_mapping AS (
                DELETE FROM provider_mappings
                WHERE account_uuid = %s AND entity_kind = 'stream'
                  AND workspace_uuid = %s AND provider_id <> %s
            )
            INSERT INTO provider_mappings (
                account_uuid, entity_kind, workspace_uuid, provider_id,
                metadata, deleted
            ) VALUES (%s, 'stream', %s, %s, %s, false)
            ON CONFLICT (account_uuid, entity_kind, provider_id) DO UPDATE SET
                workspace_uuid = EXCLUDED.workspace_uuid,
                metadata = EXCLUDED.metadata,
                deleted = false, updated_at = now()
            """,
            (
                account_uuid,
                stream_uuid,
                chat_key,
                account_uuid,
                stream_uuid,
                chat_key,
                json.dumps(
                    {
                        "chat_type": provider_chat["chat_type"],
                        "project_uuid": project_uuid,
                        "participants": participant_uuids,
                        "name": stream["name"],
                        "description": stream["description"],
                        "private": stream["private"],
                        "default_topic_uuid": stream.get("default_topic_uuid"),
                    }
                ),
            ),
        )
        for raw_topic in topics:
            if not isinstance(raw_topic, dict):
                raise ValueError("Invalid workspace projection topic")
            session.execute(
                """
                WITH removed_stale_workspace_mapping AS (
                    DELETE FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = 'topic'
                      AND workspace_uuid = %s AND provider_id <> %s
                )
                INSERT INTO provider_mappings (
                    account_uuid, entity_kind, workspace_uuid, provider_id,
                    metadata, deleted
                ) VALUES (%s, 'topic', %s, %s, %s, false)
                ON CONFLICT (account_uuid, entity_kind, provider_id) DO UPDATE SET
                    workspace_uuid = EXCLUDED.workspace_uuid,
                    metadata = EXCLUDED.metadata,
                    deleted = false, updated_at = now()
                """,
                (
                    account_uuid,
                    str(raw_topic["topic_uuid"]),
                    str(raw_topic["provider_topic_id"]),
                    account_uuid,
                    str(raw_topic["topic_uuid"]),
                    str(raw_topic["provider_topic_id"]),
                    json.dumps(
                        {
                            "stream_uuid": stream_uuid,
                            "chat_key": chat_key,
                            "name": raw_topic["name"],
                            "is_default": raw_topic["is_default"],
                        }
                    ),
                ),
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
            return [
                typing.cast(dict[str, object], row["body"])
                for row in rows
            ]

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

    def workspace_mapping(
        self, account_uuid: str, entity_kind: str, workspace_uuid: str
    ) -> dict[str, object] | None:
        with self.session() as session:
            return session.execute(
                """
                SELECT workspace_uuid, provider_id, provider_revision, metadata
                FROM (
                    SELECT workspace_uuid, provider_id, provider_revision, metadata,
                           0 AS source_order
                    FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = %s
                      AND workspace_uuid = %s AND NOT deleted
                    UNION ALL
                    SELECT alias.workspace_uuid, alias.provider_id,
                           mapping.provider_revision, alias.metadata, 1 AS source_order
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
            row = session.execute(
                """
                UPDATE provider_mappings
                SET provider_id = %s, provider_revision = %s, metadata = %s,
                    deleted = false, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = %s
                  AND provider_id = %s AND NOT deleted
                RETURNING workspace_uuid, provider_id, provider_revision, metadata
                """,
                (
                    new_provider_id,
                    provider_revision,
                    json.dumps(metadata),
                    account_uuid,
                    entity_kind,
                    old_provider_id,
                ),
            ).fetchone()
            return row

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
        with self.session() as session:
            return list(
                session.execute(
                    """
                    SELECT account_uuid, queue_id, event_id, body
                    FROM zulip_provider_events
                    WHERE processing_state = 'pending' AND available_at <= now()
                    ORDER BY created_at, event_id LIMIT %s
                    """,
                    (limit,),
                ).fetchall()
            )

    def retry_provider_event(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        reason: str,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_provider_events
                SET retry_count = retry_count + 1,
                    available_at = now() + (
                        LEAST(300, power(2, LEAST(retry_count, 8)))
                        * interval '1 second'
                    ),
                    processing_reason = %s
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state = 'pending'
                """,
                (
                    reason[:128] or "provider_file_unavailable",
                    account_uuid,
                    queue_id,
                    event_id,
                ),
            )

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
                "SELECT operation_sha256 FROM operation_idempotency "
                "WHERE operation_uuid = %s",
                (operation_uuid,),
            ).fetchone()
            if (
                existing is not None
                and existing["operation_sha256"] != operation_sha256
            ):
                raise ValueError("Operation UUID reused with a different digest")
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
            return result is not None

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
                  AND read_delivery.sent_at IS NULL
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
                """
            )
            session.execute(
                """
                UPDATE workspace_delivery_outbox AS topic_delivery
                SET priority = message_delivery.priority
                FROM workspace_delivery_outbox AS message_delivery
                WHERE topic_delivery.sent_at IS NULL
                  AND message_delivery.sent_at IS NULL
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
                      'message.create', 'message.update', 'read_state.set'
                  )
                  AND topic_delivery.record->'operation'->>'entity_uuid' =
                      message_delivery.record->'operation'->'payload'
                          ->>'topic_uuid'
                """
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
                              'message.create', 'message.update', 'read_state.set'
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
                                    AND message_mapping.workspace_uuid::text =
                                        read_message.message_uuid
                                    AND NOT message_mapping.deleted
                                    AND message_mapping.metadata
                                            ->>'workspace_delivery_state' =
                                        'committed'
                              )
                          )
                      )
                    ORDER BY priority, created_at LIMIT %s
                    """,
                (minimum_priority, maximum_priority, limit),
            ).fetchall()
            return [typing.cast(dict[str, object], row["record"]) for row in rows]

    def reset_stale_workspace_deliveries(self) -> int:
        with self.session() as session:
            stale = session.execute(
                """
                DELETE FROM workspace_delivery_outbox AS delivery
                WHERE delivery.sent_at IS NULL
                  AND delivery.submission_state = 'pending' AND (
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
                          provider_queue_id, provider_event_id, priority, record
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
                session.execute(
                    """
                    UPDATE zulip_provider_events
                    SET processing_state = 'pending', available_at = now(),
                        processing_reason = 'assignment_changed'
                    WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                      AND processing_state = 'delivering'
                    """,
                    (
                        row["account_uuid"],
                        row["provider_queue_id"],
                        row["provider_event_id"],
                    ),
                )
            return len(stale)

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
                    UPDATE zulip_provider_events SET processing_state = 'processed'
                    WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                      AND processing_state = 'delivering'
                    """,
                    (row["account_uuid"], row["queue_id"], row["event_id"]),
                )
            return len(rows)

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

    def reconcile_participant_sync(self) -> None:
        with self.session() as session:
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

    def claim_participant_sync(self) -> dict[str, object] | None:
        with self.session() as session:
            return session.execute(
                """
                WITH candidate AS (
                    SELECT participant_sync.account_uuid,
                           participant_sync.provider_chat_key
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
                    ORDER BY participant_sync.updated_at,
                             participant_sync.provider_chat_key
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE zulip_participant_sync AS participant_sync
                SET state = 'running',
                    lease_until = now() + interval '60 seconds',
                    updated_at = now()
                FROM candidate
                WHERE participant_sync.account_uuid = candidate.account_uuid
                  AND participant_sync.provider_chat_key =
                      candidate.provider_chat_key
                RETURNING participant_sync.account_uuid,
                          participant_sync.provider_chat_key,
                          participant_sync.assignment_generation
                """
            ).fetchone()

    def complete_participant_sync(
        self,
        account_uuid: str,
        provider_chat_key: str,
        assignment_generation: int,
        provider_user_ids: list[int],
        ready: bool,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_participant_sync
                SET state = %s, lease_until = NULL,
                    provider_user_ids = %s::jsonb, updated_at = now()
                WHERE account_uuid = %s AND provider_chat_key = %s
                  AND assignment_generation = %s
                  AND state = 'running'
                """,
                (
                    "ready" if ready else "reported",
                    json.dumps(sorted(set(provider_user_ids))),
                    account_uuid,
                    provider_chat_key,
                    assignment_generation,
                ),
            )

    def release_participant_sync(
        self,
        account_uuid: str,
        provider_chat_key: str,
        assignment_generation: int,
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                UPDATE zulip_participant_sync
                SET state = 'pending', lease_until = NULL, updated_at = now()
                WHERE account_uuid = %s AND provider_chat_key = %s
                  AND assignment_generation = %s
                  AND state = 'running'
                """,
                (account_uuid, provider_chat_key, assignment_generation),
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
                        THEN zulip_backfill_jobs.cutoff_at
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
                """
            )
            session.execute(
                """
                UPDATE zulip_backfill_jobs AS job
                SET state = 'cancelled', updated_at = now()
                WHERE job.state <> 'cancelled'
                  AND NOT EXISTS (
                    SELECT 1 FROM desired_resources AS assignment
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
                    SELECT DISTINCT ON (body->>'resource_uuid') result_status
                    FROM observed_report_outbox
                    WHERE body->>'resource_type' = 'external_chat_catalog'
                      AND body->'catalog'->>'external_account_uuid' = %s
                      AND (body->>'observed_generation')::bigint = %s
                    ORDER BY body->>'resource_uuid', created_at DESC
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
                    SELECT DISTINCT ON (body->>'resource_uuid') body, result_status
                    FROM observed_report_outbox
                    WHERE body->>'resource_type' = 'external_chat_catalog'
                      AND body->'catalog'->>'external_account_uuid' = %s
                      AND (body->>'observed_generation')::bigint = %s
                    ORDER BY body->>'resource_uuid', created_at DESC
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
                  AND body->>'external_account_uuid' = %s
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
                          AND assignment.body->>'external_account_uuid' = %s
                          AND COALESCE(
                              (assignment.body->>'selected')::boolean, true
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM zulip_participant_sync
                                  AS participant_sync
                              WHERE participant_sync.account_uuid::text = %s
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
                          AND assignment.body->>'external_account_uuid' = %s
                          AND COALESCE(
                              (assignment.body->>'selected')::boolean, true
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM zulip_backfill_jobs AS job
                              WHERE job.account_uuid::text = %s
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
                        WHERE delivery.account_uuid::text = %s
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
                WITH candidate AS (
                    SELECT job.account_uuid, job.provider_chat_key
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
                    WHERE (
                        job.state = 'pending' AND job.available_at <= now()
                    ) OR (
                        job.state = 'running' AND job.lease_until < now()
                    )
                    ORDER BY job.updated_at
                    FOR UPDATE SKIP LOCKED LIMIT 1
                )
                UPDATE zulip_backfill_jobs AS job
                SET state = 'running', lease_until = now() + interval '60 seconds',
                    updated_at = now()
                FROM candidate
                WHERE job.account_uuid = candidate.account_uuid
                  AND job.provider_chat_key = candidate.provider_chat_key
                RETURNING job.account_uuid, job.provider_chat_key,
                          job.history_depth, job.next_anchor, job.cutoff_at,
                          job.retry_count
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
                    LEFT JOIN scheduler_accounts AS account
                      ON account.account_uuid = operation.account_uuid
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
                             account.last_dispatched_at NULLS FIRST,
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
                            "mapping_origin": str(record["origin"]),
                            "workspace_delivery_state": "committed",
                        }
                    ),
                ),
            )
        elif kind == "message.update":
            extensions = typing.cast(dict[str, object], operation.get("extensions", {}))
            session.execute(
                """
                UPDATE provider_mappings
                SET provider_revision = COALESCE(%s, provider_revision),
                    metadata = metadata || jsonb_strip_nulls(jsonb_build_object(
                        'content_sha256', %s::text,
                        'provider_content_sha256', %s::text,
                        'subject', %s::text
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
                    SELECT record_uuid FROM bridge_operations
                    WHERE state = 'uncertain'
                      AND NOT manual_reconciliation_required
                      AND reconciliation_after <= now()
                      AND (lease_until IS NULL OR lease_until < now())
                    ORDER BY reconciliation_after, created_at
                    FOR UPDATE SKIP LOCKED
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
                SELECT queue_id, last_event_id FROM zulip_event_cursors
                WHERE account_uuid = %s
                """,
                (account_uuid,),
            ).fetchone()

    def update_provider_event_cursor(
        self, account_uuid: str, queue_id: str, last_event_id: int
    ) -> None:
        with self.session() as session:
            session.execute(
                """
                INSERT INTO zulip_event_cursors (
                    account_uuid, queue_id, last_event_id
                ) VALUES (%s, %s, %s)
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
                    updated_at = now()
                """,
                (account_uuid, queue_id, last_event_id),
            )

    def record_provider_event(
        self, account_uuid: str, queue_id: str, event: dict[str, object]
    ) -> bool:
        with self.session() as session:
            result = session.execute(
                """
                INSERT INTO zulip_provider_events (
                    account_uuid, queue_id, event_id, event_type, body
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (account_uuid, queue_id, event_id) DO NOTHING
                RETURNING event_id
                """,
                (
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
                       operation.result_record_uuid, delivery.record
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
                if (
                    row["terminal_outcome"] == outcome
                    and prior_result_uuid is not None
                    and str(prior_result_uuid) == str(result["record_uuid"])
                ):
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
                and operation.get("kind") == "message.create"
            ):
                provider = typing.cast(dict[str, object], operation["provider"])
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
                    WHERE account_uuid = %s AND entity_kind = 'message'
                      AND provider_id = %s AND workspace_uuid = %s
                      AND NOT deleted
                    """,
                    (
                        str(operation_record["account_uuid"]),
                        str(provider["entity_id"]),
                        str(operation["entity_uuid"]),
                    ),
                )

    def enqueue_observed_report(self, report: dict[str, object]) -> bool:
        with self.session() as session:
            previous = session.execute(
                """
                SELECT body FROM observed_report_outbox
                WHERE body->>'resource_type' = %s
                  AND body->>'resource_uuid' = %s
                ORDER BY created_at DESC
                LIMIT 1
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
                SELECT body FROM observed_report_outbox
                WHERE completed_at IS NULL AND available_at <= now()
                ORDER BY created_at LIMIT %s
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
