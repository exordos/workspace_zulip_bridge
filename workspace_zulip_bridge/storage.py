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


def _provider_event_requires_account_barrier(event: dict[str, object]) -> bool:
    """Return whether one event causally spans more than one provider chat."""

    if event.get("type") != "update_message":
        return False
    source_stream_id = event.get("stream_id")
    destination_stream_id = event.get("new_stream_id")
    return (
        isinstance(source_stream_id, int)
        and isinstance(destination_stream_id, int)
        and source_stream_id != destination_stream_id
    )


def _provider_event_static_causal_lane(event: dict[str, object]) -> str | None:
    """Return a provider-side ordering lane without requiring persisted mappings."""

    if _provider_event_requires_account_barrier(event):
        return None
    event_type = event.get("type")
    if event_type == "message":
        message = event.get("message")
        if not isinstance(message, dict):
            return None
        if message.get("type") == "stream":
            stream_id = message.get("stream_id")
            return f"channel:{stream_id}" if isinstance(stream_id, int) else None
        recipients = message.get("display_recipient")
        if not isinstance(recipients, list):
            return None
        if not recipients or any(
            not isinstance(recipient, dict)
            or not isinstance(recipient.get("id"), int)
            for recipient in recipients
        ):
            return None
        participant_ids = sorted(int(recipient["id"]) for recipient in recipients)
        chat_type = "direct" if len(participant_ids) == 2 else "group_direct"
        return f"{chat_type}:{','.join(map(str, participant_ids))}"
    if event_type == "user_topic":
        stream_id = event.get("stream_id")
        return f"channel:{stream_id}" if isinstance(stream_id, int) else None
    if event_type == "update_message":
        stream_id = event.get("stream_id")
        if isinstance(stream_id, int):
            return f"channel:{stream_id}"
    if event_type in {"subscription", "user_settings"}:
        stream_ids: set[int] = set()
        stream_id = event.get("stream_id")
        if isinstance(stream_id, int):
            stream_ids.add(stream_id)
        multiple_stream_ids = event.get("stream_ids")
        if isinstance(multiple_stream_ids, list):
            for value in multiple_stream_ids:
                if isinstance(value, int):
                    stream_ids.add(value)
        subscriptions = event.get("subscriptions")
        if isinstance(subscriptions, list):
            for subscription in subscriptions:
                if isinstance(subscription, dict) and isinstance(
                    subscription.get("stream_id"), int
                ):
                    stream_ids.add(int(subscription["stream_id"]))
        if len(stream_ids) == 1:
            return f"channel:{next(iter(stream_ids))}"
        if event_type == "subscription":
            return None
        return "account:user-settings"
    if event_type == "realm_user":
        person = event.get("person")
        provider_user_id = (
            person.get("user_id") if isinstance(person, dict) else event.get("user_id")
        )
        if isinstance(provider_user_id, int):
            return f"identity:{provider_user_id}"
        return "account:identities"
    return None


def _provider_event_message_ids(event: dict[str, object]) -> list[str]:
    values: list[object] = []
    if event.get("message_id") is not None:
        values.append(event["message_id"])
    message_ids = event.get("message_ids", event.get("messages", []))
    if isinstance(message_ids, list):
        values.extend(message_ids)
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if isinstance(value, bool):
            continue
        try:
            normalized = str(int(str(value)))
        except (TypeError, ValueError):
            continue
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _provider_mapping_lock_key(
    account_uuid: str, entity_kind: str, provider_id: str
) -> str:
    """Serialize mapping visibility with decisions that depend on its absence."""
    return f"{account_uuid}:{entity_kind}:{provider_id}"


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


def _provider_read_semantic_sha256(record: dict[str, object]) -> str | None:
    """Return a stable identity for one logical provider read operation."""

    operation = record.get("operation")
    if not isinstance(operation, dict) or operation.get("kind") != "read_state.set":
        return None
    value = copy.deepcopy(record)
    normalized_operation = typing.cast(dict[str, object], value["operation"])
    normalized_operation.pop("occurred_at", None)
    for key in (
        "_workspace_read_semantic_sha256",
        "created_at",
        "expires_at",
        "operation_sha256",
        "operation_uuid",
        "predecessor_operation_uuid",
        "sequence",
        "transport",
    ):
        value.pop(key, None)
    return hashlib.sha256(canonical.canonical_json(value)).hexdigest()


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
        if isinstance(value.get("_provider_active"), bool):
            merged["_provider_active"] = value["_provider_active"]
        for name in ("email", "avatar_urn"):
            if not merged.get(name) and value.get(name):
                merged[name] = value[name]
        prior_name = str(prior.get("display_name", "")).strip()
        observed_name = str(value.get("display_name", "")).strip()
        if authoritative and observed_name and observed_name != provider_user_id:
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
        self,
        record_uuid: str,
        status: str,
        lease_uuid: str | None = None,
    ) -> None: ...

    def finalize_provider_result_responses(
        self,
        responses: list[tuple[str, str, str | None]],
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
            self._delete_catalog_topology_in_session(
                session, account_uuid, provider_chat_key
            )

    @staticmethod
    def _delete_catalog_topology_in_session(
        session: sessions.PgSQLSession,
        account_uuid: str,
        provider_chat_key: str,
    ) -> None:
        session.execute(
            """
            DELETE FROM external_chat_catalog_state
            WHERE account_uuid = %s AND provider_chat_key = %s
            """,
            (account_uuid, provider_chat_key),
        )
        session.execute(
            """
            UPDATE zulip_provider_events
            SET available_at = now()
            WHERE account_uuid = %s
              AND causal_lane = %s
              AND processing_state = 'pending'
              AND processing_reason = 'provider_chat_assignment_pending'
            """,
            (account_uuid, provider_chat_key),
        )

    def omitted_cataloged_channels(
        self, account_uuid: str, provider_chat_keys: set[str]
    ) -> list[str]:
        """Return channels omitted from one authoritative registration snapshot."""
        with self.session() as session:
            rows = session.execute(
                """
                SELECT provider_chat_key
                FROM external_chat_catalog_state
                WHERE account_uuid = %s
                  AND provider_chat_key LIKE 'channel:%%'
                """,
                (account_uuid,),
            ).fetchall()
            return sorted(
                str(row["provider_chat_key"])
                for row in rows
                if str(row["provider_chat_key"]) not in provider_chat_keys
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
                metadata = provider_mappings.metadata || EXCLUDED.metadata,
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
        catalog_state = session.execute(
            """
            SELECT participants
            FROM external_chat_catalog_state
            WHERE account_uuid = %s AND provider_chat_key = %s
            """,
            (account_uuid, chat_key),
        ).fetchone()
        catalog_participants = (
            catalog_state.get("participants", [])
            if isinstance(catalog_state, dict)
            else []
        )
        if not isinstance(catalog_participants, list):
            catalog_participants = []
        provider_activity = {
            str(participant["provider_user_id"]): participant["_provider_active"]
            for participant in catalog_participants
            if isinstance(participant, dict)
            and participant.get("provider_user_id") is not None
            and isinstance(participant.get("_provider_active"), bool)
        }
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
                    "active": provider_activity.get(
                        str(raw_participant["provider_user_id"]), True
                    ),
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

    def provider_chat_is_cataloged(
        self, account_uuid: str, provider_chat_key: str
    ) -> bool:
        """Return whether the latest local catalog still contains one chat."""
        with self.session() as session:
            row = session.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM external_chat_catalog_state
                    WHERE account_uuid = %s AND provider_chat_key = %s
                ) AS cataloged
                """,
                (account_uuid, provider_chat_key),
            ).fetchone()
            return bool(row and row["cataloged"])

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

    def provider_topic_mappings(
        self, account_uuid: str
    ) -> list[dict[str, object]]:
        """Return active non-default topics for registration tombstones."""
        with self.session() as session:
            return list(
                session.execute(
                    """
                    SELECT provider_id, metadata
                    FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = 'topic'
                      AND NOT deleted
                      AND metadata->>'notification_mode' IN (
                          'mute', 'unmute', 'follow'
                      )
                    ORDER BY provider_id
                    """,
                    (account_uuid,),
                ).fetchall()
            )

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

    def workspace_mappings(
        self,
        account_uuid: str,
        entity_kind: str,
        workspace_uuids: list[str],
    ) -> dict[str, dict[str, object]]:
        """Resolve one exact provider mapping page in a single transaction."""
        if not workspace_uuids:
            return {}
        with self.session() as session:
            rows = session.execute(
                """
                SELECT DISTINCT ON (workspace_uuid)
                       workspace_uuid::text AS workspace_uuid,
                       provider_id, provider_revision, metadata
                FROM (
                    SELECT workspace_uuid, provider_id, provider_revision, metadata,
                           1 AS source_order
                    FROM provider_mappings
                    WHERE account_uuid = %s AND entity_kind = %s
                      AND workspace_uuid = ANY(%s::uuid[]) AND NOT deleted
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
                      AND alias.workspace_uuid = ANY(%s::uuid[])
                      AND NOT alias.deleted
                ) AS candidates
                ORDER BY workspace_uuid, source_order
                """,
                (
                    account_uuid,
                    entity_kind,
                    workspace_uuids,
                    account_uuid,
                    entity_kind,
                    workspace_uuids,
                ),
            ).fetchall()
            return {str(row["workspace_uuid"]): row for row in rows}

    def tombstoned_workspace_mapping(
        self, account_uuid: str, entity_kind: str, workspace_uuid: str
    ) -> dict[str, object] | None:
        """Return retained mapping data without restoring active projection state."""
        with self.session() as session:
            return session.execute(
                """
                SELECT workspace_uuid, provider_id, provider_revision, metadata
                FROM provider_mappings
                WHERE account_uuid = %s AND entity_kind = %s
                  AND workspace_uuid = %s AND deleted
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (account_uuid, entity_kind, workspace_uuid),
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
                  AND delivery.submission_state != 'rejected'
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
            extensions = typing.cast(dict[str, object], operation.get("extensions", {}))
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
            if entity_kind == "message":
                session.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (
                        _provider_mapping_lock_key(
                            account_uuid, entity_kind, provider_id
                        ),
                    ),
                )
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
            session.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (
                    _provider_mapping_lock_key(
                        account_uuid, "message", provider_id
                    ),
                ),
            )
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
            lock_provider_ids = (
                sorted({old_provider_id, new_provider_id})
                if entity_kind == "message"
                else [new_provider_id]
            )
            for lock_provider_id in lock_provider_ids:
                session.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (
                        _provider_mapping_lock_key(
                            account_uuid, entity_kind, lock_provider_id
                        ),
                    ),
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
            if entity_kind == "message":
                session.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (
                        _provider_mapping_lock_key(
                            account_uuid, entity_kind, provider_id
                        ),
                    ),
                )
            session.execute(
                """
                UPDATE provider_mappings
                SET deleted = true, updated_at = now()
                WHERE account_uuid = %s AND entity_kind = %s AND provider_id = %s
                """,
                (account_uuid, entity_kind, provider_id),
            )

    def pending_provider_events(self, limit: int = 100) -> list[dict[str, object]]:
        """Select fair chat-lane heads while preserving account-wide barriers."""
        # Let one busy account expose several independent lanes without
        # allowing it to consume the entire global scheduler quantum.
        per_account_limit = max(1, min(limit, 4))
        with self.session() as session:
            # This bounded selector is latency-sensitive and too branch-heavy
            # for PostgreSQL JIT compilation to amortize on each scheduler tick.
            session.execute("SET LOCAL jit = off")
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
                               event.causal_lane, event.created_at,
                               event.available_at
                        FROM scheduler_accounts AS journal
                        JOIN LATERAL (
                            SELECT choice.*
                            FROM (
                                SELECT lane_event.account_uuid,
                                       lane_event.queue_id,
                                       lane_event.event_id,
                                       lane_event.body,
                                       lane_event.processing_reason,
                                       lane_event.retry_count,
                                       lane_event.assignment_pending_since,
                                       lane_event.assignment_catalog_reported_at,
                                       lane_event.provider_message_context,
                                       lane_event.causal_lane,
                                       lane_event.created_at,
                                       lane_event.available_at,
                                       lane.last_provider_event_dispatched_at
                                           AS lane_dispatched_at
                                FROM (
                                    SELECT DISTINCT event.causal_lane
                                    FROM zulip_provider_events AS event
                                    WHERE event.account_uuid =
                                          journal.account_uuid
                                      AND event.causal_lane IS NOT NULL
                                      AND event.processing_state IN (
                                          'pending', 'delivering'
                                      )
                                ) AS active_lane
                                LEFT JOIN scheduler_provider_event_lanes AS lane
                                  ON lane.account_uuid = journal.account_uuid
                                 AND lane.causal_lane = active_lane.causal_lane
                                JOIN LATERAL (
                                    SELECT event.account_uuid, event.queue_id,
                                           event.event_id, event.body,
                                           event.processing_state,
                                           event.processing_reason,
                                           event.retry_count,
                                           event.assignment_pending_since,
                                           event.assignment_catalog_reported_at,
                                           event.provider_message_context,
                                           event.causal_lane,
                                           event.created_at,
                                           event.available_at
                                    FROM zulip_provider_events AS event
                                    WHERE event.account_uuid =
                                          journal.account_uuid
                                      AND event.causal_lane =
                                          active_lane.causal_lane
                                      AND event.processing_state IN (
                                          'pending', 'delivering'
                                      )
                                    ORDER BY event.created_at,
                                             event.event_id, event.queue_id
                                    LIMIT 1
                                ) AS lane_event ON true
                                WHERE lane_event.processing_state = 'pending'
                                  AND lane_event.available_at <= now()
                                  AND NOT EXISTS (
                                      SELECT 1
                                      FROM zulip_provider_events AS barrier
                                      WHERE barrier.account_uuid =
                                            lane_event.account_uuid
                                        AND barrier.causal_lane IS NULL
                                        AND barrier.processing_state IN (
                                            'pending', 'delivering'
                                        )
                                        AND (
                                            barrier.created_at,
                                            barrier.event_id,
                                            barrier.queue_id
                                        ) < (
                                            lane_event.created_at,
                                            lane_event.event_id,
                                            lane_event.queue_id
                                        )
                                  )
                                UNION ALL
                                SELECT global_event.account_uuid,
                                       global_event.queue_id,
                                       global_event.event_id,
                                       global_event.body,
                                       global_event.processing_reason,
                                       global_event.retry_count,
                                       global_event.assignment_pending_since,
                                       global_event.assignment_catalog_reported_at,
                                       global_event.provider_message_context,
                                       global_event.causal_lane,
                                       global_event.created_at,
                                       global_event.available_at,
                                       NULL::timestamptz AS lane_dispatched_at
                                FROM LATERAL (
                                    SELECT event.account_uuid, event.queue_id,
                                           event.event_id, event.event_type,
                                           event.body,
                                           event.processing_state,
                                           event.processing_reason,
                                           event.retry_count,
                                           event.assignment_pending_since,
                                           event.assignment_catalog_reported_at,
                                           event.provider_message_context,
                                           event.causal_lane,
                                           event.created_at,
                                           event.available_at
                                    FROM zulip_provider_events AS event
                                    WHERE event.account_uuid = journal.account_uuid
                                      AND event.causal_lane IS NULL
                                      AND event.processing_state IN (
                                          'pending', 'delivering'
                                      )
                                    ORDER BY event.created_at,
                                             event.event_id, event.queue_id
                                    LIMIT 1
                                ) AS global_event
                                WHERE global_event.processing_state = 'pending'
                                  AND global_event.available_at <= now()
                                  AND NOT EXISTS (
                                      SELECT 1
                                      FROM zulip_provider_events AS predecessor
                                      WHERE predecessor.account_uuid =
                                            global_event.account_uuid
                                        AND predecessor.processing_state IN (
                                            'pending', 'delivering'
                                        )
                                        -- Grouped flags are converted into
                                        -- durable chat-lane outbox records.
                                        -- They may pass an older durable
                                        -- delivery only while its completion
                                        -- cannot change or remove the message
                                        -- materialization used by conversion.
                                        AND (
                                            predecessor.processing_state =
                                                'pending'
                                            OR global_event.event_type !=
                                                'update_message_flags'
                                            OR predecessor.event_type =
                                                'delete_message'
                                            OR (
                                                predecessor.event_type =
                                                    'update_message'
                                                AND (
                                                    COALESCE(
                                                        predecessor.body
                                                            ->>'new_stream_id',
                                                        predecessor.body
                                                            ->>'stream_id'
                                                    ) IS DISTINCT FROM
                                                        predecessor.body
                                                            ->>'stream_id'
                                                    OR (
                                                        predecessor.body ?
                                                            'orig_subject'
                                                        AND predecessor.body ?
                                                            'subject'
                                                        AND predecessor.body
                                                                ->>'orig_subject'
                                                            IS DISTINCT FROM
                                                            predecessor.body
                                                                ->>'subject'
                                                    )
                                                )
                                            )
                                        )
                                        AND (
                                            predecessor.created_at,
                                            predecessor.event_id,
                                            predecessor.queue_id
                                        ) < (
                                            global_event.created_at,
                                            global_event.event_id,
                                            global_event.queue_id
                                        )
                                  )
                            ) AS choice
                            ORDER BY choice.lane_dispatched_at NULLS FIRST,
                                     choice.available_at, choice.created_at,
                                     choice.event_id, choice.queue_id
                            LIMIT %s
                        ) AS event ON true
                        WHERE (
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
                    ), dispatched_lanes AS (
                        INSERT INTO scheduler_provider_event_lanes (
                            account_uuid, causal_lane,
                            last_provider_event_dispatched_at
                        )
                        SELECT event.account_uuid, event.causal_lane,
                               clock_timestamp()
                        FROM candidates AS event
                        WHERE event.causal_lane IS NOT NULL
                        ON CONFLICT (account_uuid, causal_lane) DO UPDATE SET
                            last_provider_event_dispatched_at =
                                EXCLUDED.last_provider_event_dispatched_at
                        RETURNING account_uuid, causal_lane
                    )
                    SELECT event.account_uuid, event.queue_id, event.event_id,
                           event.body, event.processing_reason,
                           event.retry_count,
                           event.assignment_pending_since,
                           event.assignment_catalog_reported_at,
                           event.provider_message_context,
                           event.causal_lane, event.created_at
                    FROM candidates AS event
                    JOIN dispatched
                      ON dispatched.account_uuid = event.account_uuid
                    ORDER BY event.created_at, event.event_id, event.queue_id
                    """,
                    (per_account_limit, limit),
                ).fetchall()
            )

    def pending_provider_event_lane_batch(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        causal_lane: str,
        limit: int,
    ) -> list[dict[str, object]]:
        """Extend one selected head with its ordered pending lane prefix."""
        with self.session() as session:
            return list(
                session.execute(
                    """
                    WITH anchor AS (
                        SELECT created_at, event_id, queue_id
                        FROM zulip_provider_events
                        WHERE account_uuid = %s AND queue_id = %s
                          AND event_id = %s AND causal_lane = %s
                          AND processing_state = 'pending'
                    )
                    SELECT event.account_uuid, event.queue_id, event.event_id,
                           event.body, event.processing_reason,
                           event.retry_count, event.assignment_pending_since,
                           event.assignment_catalog_reported_at,
                           event.provider_message_context, event.causal_lane,
                           event.created_at
                    FROM zulip_provider_events AS event
                    CROSS JOIN anchor
                    WHERE event.account_uuid = %s
                      AND event.causal_lane = %s
                      AND event.processing_state = 'pending'
                      AND event.available_at <= now()
                      AND (
                          event.created_at, event.event_id, event.queue_id
                      ) >= (
                          anchor.created_at, anchor.event_id, anchor.queue_id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM zulip_provider_events AS predecessor
                          WHERE predecessor.account_uuid = event.account_uuid
                            AND predecessor.causal_lane = event.causal_lane
                            AND predecessor.processing_state = 'pending'
                            AND predecessor.available_at > now()
                            AND (
                                predecessor.created_at,
                                predecessor.event_id,
                                predecessor.queue_id
                            ) >= (
                                anchor.created_at,
                                anchor.event_id,
                                anchor.queue_id
                            )
                            AND (
                                predecessor.created_at,
                                predecessor.event_id,
                                predecessor.queue_id
                            ) < (
                                event.created_at,
                                event.event_id,
                                event.queue_id
                            )
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM zulip_provider_events AS delivering
                          WHERE delivering.account_uuid = event.account_uuid
                            AND delivering.causal_lane = event.causal_lane
                            AND delivering.processing_state = 'delivering'
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM zulip_provider_events AS barrier
                          WHERE barrier.account_uuid = event.account_uuid
                            AND barrier.causal_lane IS NULL
                            AND barrier.processing_state IN (
                                'pending', 'delivering'
                            )
                            AND (
                                barrier.created_at,
                                barrier.event_id,
                                barrier.queue_id
                            ) < (
                                event.created_at,
                                event.event_id,
                                event.queue_id
                            )
                      )
                    ORDER BY event.created_at, event.event_id, event.queue_id
                    LIMIT %s
                    """,
                    (
                        account_uuid,
                        queue_id,
                        event_id,
                        causal_lane,
                        account_uuid,
                        causal_lane,
                        max(1, limit),
                    ),
                ).fetchall()
            )

    def has_pending_provider_events(self) -> bool:
        """Return whether live Zulip journal work still needs processing."""
        with self.session() as session:
            session.execute("SET LOCAL jit = off")
            row = session.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM scheduler_accounts AS journal
                    WHERE (
                          EXISTS (
                              SELECT 1
                              FROM zulip_provider_events AS event
                              WHERE event.account_uuid = journal.account_uuid
                                AND event.processing_state = 'delivering'
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
                          OR (
                              (
                                  NOT EXISTS (
                                      SELECT 1
                                      FROM desired_resources AS account
                                      WHERE account.resource_type =
                                            'external_account'
                                        AND account.resource_uuid =
                                            journal.account_uuid
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
                                            journal.account_uuid
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
                              AND (
                                  EXISTS (
                                      SELECT 1
                                      FROM (
                                          SELECT DISTINCT event.causal_lane
                                          FROM zulip_provider_events AS event
                                          WHERE event.account_uuid =
                                                journal.account_uuid
                                            AND event.causal_lane IS NOT NULL
                                            AND event.processing_state IN (
                                                'pending', 'delivering'
                                            )
                                      ) AS active_lane
                                      JOIN LATERAL (
                                          SELECT event.account_uuid,
                                                 event.created_at,
                                                 event.event_id,
                                                 event.queue_id,
                                                 event.processing_state,
                                                 event.available_at
                                          FROM zulip_provider_events AS event
                                          WHERE event.account_uuid =
                                                journal.account_uuid
                                            AND event.causal_lane =
                                                active_lane.causal_lane
                                            AND event.processing_state IN (
                                                'pending', 'delivering'
                                            )
                                          ORDER BY event.created_at,
                                                   event.event_id,
                                                   event.queue_id
                                          LIMIT 1
                                      ) AS event ON true
                                      WHERE event.processing_state = 'pending'
                                        AND event.available_at <= now()
                                        AND NOT EXISTS (
                                            SELECT 1
                                            FROM zulip_provider_events AS barrier
                                            WHERE barrier.account_uuid =
                                                  event.account_uuid
                                              AND barrier.causal_lane IS NULL
                                              AND barrier.processing_state IN (
                                                  'pending', 'delivering'
                                              )
                                              AND (
                                                  barrier.created_at,
                                                  barrier.event_id,
                                                  barrier.queue_id
                                              ) < (
                                                  event.created_at,
                                                  event.event_id,
                                                  event.queue_id
                                              )
                                        )
                                  )
                                  OR EXISTS (
                                      SELECT 1
                                      FROM LATERAL (
                                          SELECT event.created_at,
                                                 event.event_id,
                                                 event.queue_id,
                                                 event.event_type,
                                                 event.processing_state,
                                                 event.available_at
                                          FROM zulip_provider_events AS event
                                          WHERE event.account_uuid =
                                                journal.account_uuid
                                            AND event.causal_lane IS NULL
                                            AND event.processing_state IN (
                                                'pending', 'delivering'
                                            )
                                          ORDER BY event.created_at,
                                                   event.event_id,
                                                   event.queue_id
                                          LIMIT 1
                                      ) AS event
                                      WHERE event.processing_state = 'pending'
                                        AND event.available_at <= now()
                                        AND NOT EXISTS (
                                            SELECT 1
                                            FROM zulip_provider_events
                                                AS predecessor
                                            WHERE predecessor.account_uuid =
                                                  journal.account_uuid
                                              AND predecessor.processing_state
                                                  IN ('pending', 'delivering')
                                              -- Keep this readiness probe in
                                              -- lockstep with the selector's
                                              -- grouped-flags exception above.
                                              AND (
                                                  predecessor.processing_state =
                                                      'pending'
                                                  OR event.event_type !=
                                                      'update_message_flags'
                                                  OR predecessor.event_type =
                                                      'delete_message'
                                                  OR (
                                                      predecessor.event_type =
                                                          'update_message'
                                                      AND (
                                                          COALESCE(
                                                              predecessor.body
                                                                  ->>'new_stream_id',
                                                              predecessor.body
                                                                  ->>'stream_id'
                                                          ) IS DISTINCT FROM
                                                              predecessor.body
                                                                  ->>'stream_id'
                                                          OR (
                                                              predecessor.body ?
                                                                  'orig_subject'
                                                              AND predecessor.body ?
                                                                  'subject'
                                                              AND predecessor.body
                                                                      ->>'orig_subject'
                                                                  IS DISTINCT FROM
                                                                  predecessor.body
                                                                      ->>'subject'
                                                          )
                                                      )
                                                  )
                                              )
                                              AND (
                                                  predecessor.created_at,
                                                  predecessor.event_id,
                                                  predecessor.queue_id
                                              ) < (
                                                  event.created_at,
                                                  event.event_id,
                                                  event.queue_id
                                              )
                                        )
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
            if normalized_reason == "provider_message_mapping_changed":
                self._narrow_pre_enqueue_grouped_read_snapshot(
                    session,
                    account_uuid,
                    queue_id,
                    event_id,
                )
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
                        WHEN retry.reason =
                             'provider_chat_assignment_pending'
                        THEN LEAST(
                                 60,
                                 power(2, LEAST(retry_count, 6))
                             ) * interval '1 second'
                        WHEN retry.reason IN (
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

    @staticmethod
    def _narrow_pre_enqueue_grouped_read_snapshot(
        session: sessions.PgSQLSession,
        account_uuid: str,
        queue_id: str,
        event_id: int,
    ) -> None:
        """Remove retired messages without changing surviving operation identities."""
        event_row = session.execute(
            """
            SELECT body
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
              AND processing_state = 'pending'
              AND prepared_records IS NOT NULL
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        if event_row is None:
            return
        event = typing.cast(dict[str, object], event_row["body"])
        provider_message_ids = sorted(_provider_event_message_ids(event))
        if not provider_message_ids:
            return
        for provider_message_id in provider_message_ids:
            session.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (
                    _provider_mapping_lock_key(
                        account_uuid,
                        "message",
                        provider_message_id,
                    ),
                ),
            )
        provider_event = session.execute(
            """
            SELECT prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
              AND processing_state = 'pending'
              AND prepared_records IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM workspace_delivery_outbox AS delivery
                  WHERE delivery.account_uuid =
                        zulip_provider_events.account_uuid
                    AND delivery.provider_queue_id =
                        zulip_provider_events.queue_id
                    AND delivery.provider_event_id =
                        zulip_provider_events.event_id
              )
            FOR UPDATE
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        if provider_event is None or not isinstance(
            provider_event["prepared_records"], list
        ):
            return
        mappings = session.execute(
            """
            SELECT workspace_uuid, metadata->>'project_uuid' AS project_uuid,
                   metadata->>'stream_uuid' AS stream_uuid,
                   metadata->>'topic_uuid' AS topic_uuid
            FROM provider_mappings
            WHERE account_uuid = %s AND entity_kind = 'message'
              AND provider_id = ANY(%s) AND NOT deleted
            """,
            (account_uuid, provider_message_ids),
        ).fetchall()
        active = {
            (
                str(mapping["workspace_uuid"]),
                str(mapping["project_uuid"]),
                str(mapping["stream_uuid"]),
                str(mapping["topic_uuid"]),
            )
            for mapping in mappings
        }
        eligible_mappings = session.execute(
            """
            SELECT message.workspace_uuid,
                   message.metadata->>'project_uuid' AS project_uuid,
                   message.metadata->>'stream_uuid' AS stream_uuid,
                   message.metadata->>'topic_uuid' AS topic_uuid
            FROM provider_mappings AS message
            JOIN desired_resources AS assignment
              ON assignment.resource_type = 'external_chat_assignment'
             AND NOT assignment.deleted
             AND assignment.body->>'external_account_uuid' = %s
             AND assignment.body->'provider_chat'->>'provider_chat_key' =
                 message.metadata->>'chat_key'
             AND assignment.body->>'project_id' =
                 message.metadata->>'project_uuid'
            JOIN provider_mappings AS stream
              ON stream.account_uuid = message.account_uuid
             AND stream.entity_kind = 'stream'
             AND stream.provider_id = message.metadata->>'chat_key'
             AND stream.workspace_uuid::text =
                 message.metadata->>'stream_uuid'
             AND NOT stream.deleted
            WHERE message.account_uuid = %s
              AND message.entity_kind = 'message'
              AND message.provider_id = ANY(%s)
              AND NOT message.deleted
            """,
            (account_uuid, account_uuid, provider_message_ids),
        ).fetchall()
        current = {
            (
                str(mapping["workspace_uuid"]),
                str(mapping["project_uuid"]),
                str(mapping["stream_uuid"]),
                str(mapping["topic_uuid"]),
            )
            for mapping in eligible_mappings
        }
        prepared = copy.deepcopy(
            typing.cast(list[dict[str, object]], provider_event["prepared_records"])
        )
        expected: set[tuple[str, str, str, str]] = set()
        for record in prepared:
            operation = record.get("operation")
            if not isinstance(operation, dict) or operation.get("kind") != (
                "read_state.set"
            ):
                continue
            payload = operation.get("payload")
            if not isinstance(payload, dict) or not isinstance(
                payload.get("message_uuids"), list
            ):
                continue
            expected.update(
                (
                    str(message_uuid),
                    str(record["project_uuid"]),
                    str(payload["stream_uuid"]),
                    str(payload["topic_uuid"]),
                )
                for message_uuid in payload["message_uuids"]
            )
        active_uuids = {mapping[0] for mapping in active}
        projection_changed = any(mapping not in expected for mapping in active) or any(
            mapping[0] in active_uuids and mapping not in current
            for mapping in expected
        )
        if projection_changed:
            session.execute(
                """
                UPDATE zulip_provider_events
                SET processing_state = 'invalid',
                    processing_reason = 'provider_message_projection_changed',
                    prepared_records = NULL
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state = 'pending'
                """,
                (account_uuid, queue_id, event_id),
            )
            return
        narrowed: list[dict[str, object]] = []
        changed = False
        for record in prepared:
            operation = record.get("operation")
            if not isinstance(operation, dict) or operation.get("kind") != (
                "read_state.set"
            ):
                narrowed.append(record)
                continue
            payload = operation.get("payload")
            if not isinstance(payload, dict):
                narrowed.append(record)
                continue
            message_uuids = payload.get("message_uuids")
            if not isinstance(message_uuids, list):
                narrowed.append(record)
                continue
            remaining = [
                str(message_uuid)
                for message_uuid in message_uuids
                if (
                    str(message_uuid),
                    str(record["project_uuid"]),
                    str(payload["stream_uuid"]),
                    str(payload["topic_uuid"]),
                )
                in current
            ]
            if remaining == [str(value) for value in message_uuids]:
                narrowed.append(record)
                continue
            changed = True
            if not remaining:
                continue
            payload["message_uuids"] = remaining
            record["operation_sha256"] = canonical.operation_digest(record)
            narrowed.append(record)
        if not changed:
            return
        session.execute(
            """
            UPDATE zulip_provider_events
            SET prepared_records = %s
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
              AND processing_state = 'pending'
            """,
            (json.dumps(narrowed), account_uuid, queue_id, event_id),
        )

    def cache_provider_event_message_context(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        provider_message_context: dict[str, object],
    ) -> dict[str, object]:
        """Persist the minimal provider message facts needed by a reaction."""
        causal_lane = _provider_event_static_causal_lane(
            {"type": "message", "message": provider_message_context}
        )
        message_ids = _provider_event_message_ids(
            {"message_id": provider_message_context.get("id")}
        )
        provisional_lane = f"message:{message_ids[0]}" if message_ids else None
        with self.session() as session:
            row = session.execute(
                """
                WITH registered_lane AS (
                    INSERT INTO scheduler_provider_event_lanes (
                        account_uuid, causal_lane
                    )
                    SELECT %s, %s::text WHERE %s::text IS NOT NULL
                    ON CONFLICT (account_uuid, causal_lane) DO NOTHING
                ), reclassified_events AS (
                    UPDATE zulip_provider_events
                    SET causal_lane = %s
                    WHERE %s::text IS NOT NULL
                      AND account_uuid = %s
                      AND causal_lane = %s
                      AND (queue_id, event_id) <> (%s, %s)
                      AND processing_state IN ('pending', 'delivering')
                    RETURNING event_id
                )
                UPDATE zulip_provider_events
                SET provider_message_context = COALESCE(
                        provider_message_context,
                        %s
                    ),
                    causal_lane = COALESCE(%s, causal_lane)
                WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                  AND processing_state = 'pending'
                RETURNING provider_message_context
                """,
                (
                    account_uuid,
                    causal_lane,
                    causal_lane,
                    causal_lane,
                    causal_lane,
                    account_uuid,
                    provisional_lane,
                    queue_id,
                    event_id,
                    json.dumps(provider_message_context),
                    causal_lane,
                    account_uuid,
                    queue_id,
                    event_id,
                ),
            ).fetchone()
        if row is None or not isinstance(row["provider_message_context"], dict):
            raise ValueError("Provider event message context is unavailable")
        return typing.cast(dict[str, object], row["provider_message_context"])

    def _refresh_provider_event_causal_lane(
        self,
        session: sessions.PgSQLSession,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        event: dict[str, object],
    ) -> tuple[bool, str | None]:
        """Lock message mappings and refresh one selected row's durable lane."""
        provider_message_ids = sorted(_provider_event_message_ids(event))
        for provider_message_id in provider_message_ids:
            session.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (
                    _provider_mapping_lock_key(
                        account_uuid, "message", provider_message_id
                    ),
                ),
            )
        current = session.execute(
            """
            SELECT causal_lane
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
              AND processing_state = 'pending'
            FOR UPDATE
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        if current is None:
            return False, None
        current_lane = typing.cast(str | None, current["causal_lane"])
        if not provider_message_ids:
            return True, current_lane
        resolved_lane = self._provider_event_causal_lane(
            session, account_uuid, event
        )
        if resolved_lane == current_lane:
            return True, current_lane
        session.execute(
            """
            WITH registered_lane AS (
                INSERT INTO scheduler_provider_event_lanes (
                    account_uuid, causal_lane
                )
                SELECT %s, %s::text WHERE %s::text IS NOT NULL
                ON CONFLICT (account_uuid, causal_lane) DO NOTHING
            )
            UPDATE zulip_provider_events
            SET causal_lane = %s
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
              AND processing_state = 'pending'
            """,
            (
                account_uuid,
                resolved_lane,
                resolved_lane,
                resolved_lane,
                account_uuid,
                queue_id,
                event_id,
            ),
        )
        return True, resolved_lane

    def refresh_provider_event_causal_lane(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        event: dict[str, object],
    ) -> str | None:
        """Refresh any selected event whose lane depends on message mappings."""
        with self.session() as session:
            _, causal_lane = self._refresh_provider_event_causal_lane(
                session, account_uuid, queue_id, event_id, event
            )
            return causal_lane

    @contextlib.contextmanager
    def provider_event_lane_guard(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        event: dict[str, object],
        selected_causal_lane: object,
    ) -> typing.Iterator[bool]:
        """Hold mapping and journal locks through durable event persistence."""
        with self.transaction() as session:
            exists, causal_lane = self._refresh_provider_event_causal_lane(
                session, account_uuid, queue_id, event_id, event
            )
            yield exists and causal_lane == selected_causal_lane

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

    def finalize_redundant_provider_message_event(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
    ) -> bool:
        """Finish a replayed create after its message is already committed."""
        with self.session() as session:
            row = session.execute(
                """
                UPDATE zulip_provider_events AS event
                SET processing_state = 'processed', processing_reason = NULL
                WHERE event.account_uuid = %s
                  AND event.queue_id = %s
                  AND event.event_id = %s
                  AND event.event_type = 'message'
                  AND event.processing_state = 'pending'
                  AND CASE
                          WHEN event.body->'message' ? 'flags' THEN
                              jsonb_typeof(event.body->'message'->'flags')
                          ELSE jsonb_typeof(event.body->'flags')
                      END IS DISTINCT FROM 'array'
                  AND EXISTS (
                      SELECT 1
                      FROM provider_mappings AS mapping
                      WHERE mapping.account_uuid = event.account_uuid
                        AND mapping.entity_kind = 'message'
                        AND mapping.provider_id =
                            event.body->'message'->>'id'
                        AND NOT mapping.deleted
                        AND mapping.metadata->>'workspace_delivery_state' =
                            'committed'
                  )
                RETURNING event.event_id
                """,
                (account_uuid, queue_id, event_id),
            ).fetchone()
            return row is not None

    def finalize_redundant_provider_message_events(self) -> int:
        """Bulk-finish replayed creates already materialized by backfill."""
        with self.session() as session:
            row = session.execute(
                """
                WITH finalized AS (
                    UPDATE zulip_provider_events AS event
                    SET processing_state = 'processed', processing_reason = NULL
                    WHERE event.event_type = 'message'
                      AND event.processing_state = 'pending'
                      AND CASE
                              WHEN event.body->'message' ? 'flags' THEN
                                  jsonb_typeof(event.body->'message'->'flags')
                              ELSE jsonb_typeof(event.body->'flags')
                          END IS DISTINCT FROM 'array'
                      AND EXISTS (
                          SELECT 1
                          FROM provider_mappings AS mapping
                          WHERE mapping.account_uuid = event.account_uuid
                            AND mapping.entity_kind = 'message'
                            AND mapping.provider_id =
                                event.body->'message'->>'id'
                            AND NOT mapping.deleted
                            AND mapping.metadata
                                    ->>'workspace_delivery_state' = 'committed'
                      )
                    RETURNING 1
                )
                SELECT count(*) AS count FROM finalized
                """
            ).fetchone()
            return int(row["count"])

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

    def finalize_provider_event_if_lane_current(
        self,
        account_uuid: str,
        queue_id: str,
        event_id: int,
        event: dict[str, object],
        selected_causal_lane: object,
        supported: bool,
        deleted_message_ids: list[str],
    ) -> bool:
        """Refresh a mapping-dependent lane and terminalize it atomically."""
        with self.provider_event_lane_guard(
            account_uuid,
            queue_id,
            event_id,
            event,
            selected_causal_lane,
        ) as lane_current:
            if not lane_current:
                return False
            self.finalize_provider_event(
                account_uuid,
                queue_id,
                event_id,
                supported,
                deleted_message_ids,
            )
            return True

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
        existing = session.execute(
            """
            SELECT lane_sequence, predecessor_operation_uuid
            FROM producer_operations WHERE operation_uuid = %s
            """,
            (operation_uuid,),
        ).fetchone()
        if existing is not None:
            record["sequence"] = int(existing["lane_sequence"])
            record["predecessor_operation_uuid"] = (
                None
                if existing["predecessor_operation_uuid"] is None
                else str(existing["predecessor_operation_uuid"])
            )
            record["operation_sha256"] = canonical.operation_digest(record)
            return
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
                SELECT processing_state, prepared_records, body
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
                self._validate_provider_event_message_mappings(
                    session,
                    account_uuid,
                    typing.cast(dict[str, object], provider_event["body"]),
                    prepared,
                )
                return copy.deepcopy(prepared)
            if provider_event["processing_state"] != "pending":
                raise ValueError("provider_event_not_pending")
            prepared = copy.deepcopy(records)
            self._validate_provider_event_message_mappings(
                session,
                account_uuid,
                typing.cast(dict[str, object], provider_event["body"]),
                prepared,
            )
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

    @staticmethod
    def _validate_provider_event_message_mappings(
        session: sessions.PgSQLSession,
        account_uuid: str,
        event: dict[str, object],
        records: list[dict[str, object]],
    ) -> None:
        """Reject grouped reads converted from a stale mapping snapshot."""
        if event.get("type") != "update_message_flags":
            return
        provider_message_ids = _provider_event_message_ids(event)
        if not provider_message_ids:
            return
        reads = [
            record
            for record in records
            if isinstance(record.get("operation"), dict)
            and typing.cast(dict[str, object], record["operation"]).get("kind")
            == "read_state.set"
        ]
        if not reads:
            return
        mappings = session.execute(
            """
            SELECT message.workspace_uuid,
                   message.metadata->>'project_uuid' AS project_uuid,
                   message.metadata->>'stream_uuid' AS stream_uuid,
                   message.metadata->>'topic_uuid' AS topic_uuid
            FROM provider_mappings AS message
            JOIN desired_resources AS assignment
              ON assignment.resource_type = 'external_chat_assignment'
             AND NOT assignment.deleted
             AND assignment.body->>'external_account_uuid' = %s
             AND assignment.body->'provider_chat'->>'provider_chat_key' =
                 message.metadata->>'chat_key'
             AND assignment.body->>'project_id' =
                 message.metadata->>'project_uuid'
            JOIN provider_mappings AS stream
              ON stream.account_uuid = message.account_uuid
             AND stream.entity_kind = 'stream'
             AND stream.provider_id = message.metadata->>'chat_key'
             AND stream.workspace_uuid::text =
                 message.metadata->>'stream_uuid'
             AND NOT stream.deleted
            WHERE message.account_uuid = %s
              AND message.entity_kind = 'message'
              AND message.provider_id = ANY(%s)
              AND NOT message.deleted
            """,
            (account_uuid, account_uuid, provider_message_ids),
        ).fetchall()
        current = {
            (
                str(mapping["workspace_uuid"]),
                str(mapping["project_uuid"]),
                str(mapping["stream_uuid"]),
                str(mapping["topic_uuid"]),
            )
            for mapping in mappings
        }
        for record in reads:
            operation = typing.cast(dict[str, object], record["operation"])
            payload = typing.cast(dict[str, object], operation["payload"])
            expected = {
                (
                    str(message_uuid),
                    str(record["project_uuid"]),
                    str(payload["stream_uuid"]),
                    str(payload["topic_uuid"]),
                )
                for message_uuid in typing.cast(
                    list[object], payload.get("message_uuids", [])
                )
            }
            if not expected.issubset(current):
                raise ValueError("provider_message_mapping_changed")

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
            WITH requeued AS (
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
                RETURNING operation_uuid, record_uuid
            ), reopened AS (
                UPDATE operation_idempotency AS operation
                SET terminal_outcome = NULL, updated_at = now()
                FROM requeued
                WHERE operation.operation_uuid = requeued.operation_uuid
                  AND (
                      operation.terminal_outcome IS NULL
                      OR operation.terminal_outcome = 'rejected'
                  )
                RETURNING operation.operation_uuid
            )
            SELECT record_uuid FROM requeued
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
            operation = typing.cast(dict[str, object] | None, record.get("operation"))
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
            may_requeue_rejected_cleanup = (
                existing is not None
                and existing["terminal_outcome"] == "rejected"
                and provider_queue_id is None
                and provider_event_id is None
                and isinstance(operation, dict)
                and operation.get("kind") == "reaction.delete"
                and isinstance(record.get("transport"), dict)
                and isinstance(
                    typing.cast(dict[str, object], record["transport"]).get(
                        "reaction_mapping_delete"
                    ),
                    dict,
                )
            )
            if (
                existing is not None
                and existing["terminal_outcome"] is not None
                and not may_requeue_rejected_cleanup
            ):
                return False
            session.execute(
                """
                INSERT INTO operation_idempotency (operation_uuid, operation_sha256)
                VALUES (%s, %s)
                ON CONFLICT (operation_uuid) DO NOTHING
                """,
                (operation_uuid, operation_sha256),
            )
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
                      AND delivery.submission_state IN (
                          'pending', 'submitting', 'ambiguous',
                          'awaiting_result'
                      )
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
                  AND message_delivery.submission_state IN (
                      'pending', 'submitting', 'ambiguous', 'awaiting_result'
                  )
                  AND %s = 0
                  AND read_delivery.sent_at IS NULL
                  AND read_delivery.submission_state IN (
                      'pending', 'submitting', 'ambiguous', 'awaiting_result'
                  )
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
                  AND (
                      (
                          message_delivery.record->>'origin' =
                              read_delivery.record->>'origin'
                          AND message_delivery.record->>'causal_lane' =
                              read_delivery.record->>'causal_lane'
                          AND (message_delivery.record->>'sequence')::bigint <
                              (read_delivery.record->>'sequence')::bigint
                      ) OR (
                          message_delivery.provider_queue_id =
                              read_delivery.provider_queue_id
                          AND message_delivery.provider_event_id =
                              read_delivery.provider_event_id
                      )
                  )
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
                  AND message_delivery.submission_state IN (
                      'pending', 'submitting', 'ambiguous', 'awaiting_result'
                  )
                  AND %s = 0
                  AND reaction_delivery.sent_at IS NULL
                  AND reaction_delivery.submission_state IN (
                      'pending', 'submitting', 'ambiguous', 'awaiting_result'
                  )
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
                  AND (
                      (
                          message_delivery.record->>'origin' =
                              reaction_delivery.record->>'origin'
                          AND message_delivery.record->>'causal_lane' =
                              reaction_delivery.record->>'causal_lane'
                          AND (message_delivery.record->>'sequence')::bigint <
                              (reaction_delivery.record->>'sequence')::bigint
                      ) OR (
                          message_delivery.provider_queue_id =
                              reaction_delivery.provider_queue_id
                          AND message_delivery.provider_event_id =
                              reaction_delivery.provider_event_id
                      )
                  )
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
                  AND topic_delivery.submission_state IN (
                      'pending', 'submitting', 'ambiguous', 'awaiting_result'
                  )
                  AND %s = 0
                  AND message_delivery.sent_at IS NULL
                  AND message_delivery.submission_state IN (
                      'pending', 'submitting', 'ambiguous', 'awaiting_result'
                  )
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
                  AND (
                      (
                          topic_delivery.record->>'origin' =
                              message_delivery.record->>'origin'
                          AND topic_delivery.record->>'causal_lane' =
                              message_delivery.record->>'causal_lane'
                          AND (topic_delivery.record->>'sequence')::bigint <
                              (message_delivery.record->>'sequence')::bigint
                      ) OR (
                          topic_delivery.provider_queue_id =
                              message_delivery.provider_queue_id
                          AND topic_delivery.provider_event_id =
                              message_delivery.provider_event_id
                      )
                  )
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
                     AND COALESCE(
                         (account.body->>'synchronization_enabled')::boolean,
                         false
                     )
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
                    WHERE COALESCE(
                              (
                                  SELECT COALESCE(
                                             (policy.body->>'enabled')::boolean,
                                             false
                                         )
                                         AND NOT COALESCE(
                                             (
                                                 policy.body
                                                     ->>'emergency_suspended'
                                             )::boolean,
                                             false
                                         )
                                  FROM desired_resources AS policy
                                  WHERE policy.resource_type =
                                        'external_provider_policy'
                                    AND NOT policy.deleted
                                    AND policy.body->>'provider_kind' = 'zulip'
                                  ORDER BY policy.generation DESC
                                  LIMIT 1
                              ),
                              false
                          )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM observed_report_outbox AS report
                          WHERE report.completed_at IS NULL
                            AND report.body->>'resource_type' =
                                'external_chat_catalog'
                            -- Workspace preserves the catalog resource UUID as
                            -- the external_chat_assignment UUID.
                            AND report.body->>'resource_uuid' =
                                delivery.assignment_uuid::text
                            AND report.body->>'status' = 'ready'
                            AND COALESCE(
                                report.body->'catalog'->>'operation',
                                'upsert'
                            ) = 'upsert'
                      )
                      AND delivery.sent_at IS NULL
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
                                AND topic_delivery.submission_state IN (
                                    'pending', 'submitting', 'ambiguous',
                                    'awaiting_result'
                                )
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
                                AND (
                                    (
                                        topic_delivery.record->>'origin' =
                                            delivery.record->>'origin'
                                        AND topic_delivery.record
                                                ->>'causal_lane' =
                                            delivery.record->>'causal_lane'
                                        AND (topic_delivery.record
                                                ->>'sequence')::bigint <
                                            (delivery.record
                                                ->>'sequence')::bigint
                                    ) OR (
                                        topic_delivery.provider_queue_id =
                                            delivery.provider_queue_id
                                        AND topic_delivery.provider_event_id =
                                            delivery.provider_event_id
                                    )
                                )
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
                                AND message_create.submission_state IN (
                                    'pending', 'submitting', 'ambiguous',
                                    'awaiting_result'
                                )
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
                                AND (
                                    (
                                        message_create.record->>'origin' =
                                            delivery.record->>'origin'
                                        AND message_create.record
                                                ->>'causal_lane' =
                                            delivery.record->>'causal_lane'
                                        AND (message_create.record
                                                ->>'sequence')::bigint <
                                            (delivery.record
                                                ->>'sequence')::bigint
                                    ) OR (
                                        message_create.provider_queue_id =
                                            delivery.provider_queue_id
                                        AND message_create.provider_event_id =
                                            delivery.provider_event_id
                                    )
                                )
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
                                AND message_delivery.submission_state IN (
                                    'pending', 'submitting', 'ambiguous',
                                    'awaiting_result'
                                )
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
                                AND (
                                    (
                                        message_delivery.record->>'origin' =
                                            delivery.record->>'origin'
                                        AND message_delivery.record
                                                ->>'causal_lane' =
                                            delivery.record->>'causal_lane'
                                        AND (message_delivery.record
                                                ->>'sequence')::bigint <
                                            (delivery.record
                                                ->>'sequence')::bigint
                                    ) OR (
                                        message_delivery.provider_queue_id =
                                            delivery.provider_queue_id
                                        AND message_delivery.provider_event_id =
                                            delivery.provider_event_id
                                    )
                                )
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
                                AND message_create.submission_state IN (
                                    'pending', 'submitting', 'ambiguous',
                                    'awaiting_result'
                                )
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
                                AND (
                                    (
                                        message_create.record->>'origin' =
                                            delivery.record->>'origin'
                                        AND message_create.record
                                                ->>'causal_lane' =
                                            delivery.record->>'causal_lane'
                                        AND (message_create.record
                                                ->>'sequence')::bigint <
                                            (delivery.record
                                                ->>'sequence')::bigint
                                    ) OR (
                                        message_create.provider_queue_id =
                                            delivery.provider_queue_id
                                        AND message_create.provider_event_id =
                                            delivery.provider_event_id
                                    )
                                )
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
        """Probe delivery work whose own chat materialization is complete."""
        if not 0 <= minimum_priority <= maximum_priority <= 2:
            raise ValueError("Invalid workspace delivery priority range")
        with self.session() as session:
            row = session.execute(
                """
                SELECT COALESCE(
                    (
                        SELECT COALESCE(
                                   (policy.body->>'enabled')::boolean,
                                   false
                               )
                               AND NOT COALESCE(
                                   (policy.body->>'emergency_suspended')::boolean,
                                   false
                               )
                        FROM desired_resources AS policy
                        WHERE policy.resource_type = 'external_provider_policy'
                          AND NOT policy.deleted
                          AND policy.body->>'provider_kind' = 'zulip'
                        ORDER BY policy.generation DESC
                        LIMIT 1
                    ),
                    false
                ) AND EXISTS (
                    SELECT 1
                    FROM workspace_delivery_outbox AS delivery
                    JOIN desired_resources AS account
                      ON account.resource_type = 'external_account'
                     AND account.resource_uuid = delivery.account_uuid
                     AND account.generation = delivery.account_generation
                     AND NOT account.deleted
                     AND COALESCE(
                         (account.body->>'synchronization_enabled')::boolean,
                         false
                     )
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
                      AND NOT EXISTS (
                          SELECT 1
                          FROM observed_report_outbox AS report
                          WHERE report.completed_at IS NULL
                            AND report.body->>'resource_type' =
                                'external_chat_catalog'
                            -- Workspace preserves the catalog resource UUID as
                            -- the external_chat_assignment UUID.
                            AND report.body->>'resource_uuid' =
                                delivery.assignment_uuid::text
                            AND report.body->>'status' = 'ready'
                            AND COALESCE(
                                report.body->'catalog'->>'operation',
                                'upsert'
                            ) = 'upsert'
                      )
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
            SELECT processing_state, processing_reason, prepared_records
            FROM zulip_provider_events
            WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
            FOR UPDATE
            """,
            (account_uuid, queue_id, event_id),
        ).fetchone()
        if provider_event is None:
            return False
        recoverable_rejection = (
            provider_event["processing_state"] == "invalid"
            and provider_event["processing_reason"] == "workspace_delivery_rejected"
        )
        if provider_event["processing_state"] not in {
            "pending",
            "delivering",
        } and not recoverable_rejection:
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
                  AND (
                      processing_state IN ('pending', 'delivering')
                      OR (
                          processing_state = 'invalid'
                          AND processing_reason = 'workspace_delivery_rejected'
                      )
                  )
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
              AND (
                  processing_state IN ('pending', 'delivering')
                  OR (
                      processing_state = 'invalid'
                      AND processing_reason = 'workspace_delivery_rejected'
                  )
              )
            """,
            (account_uuid, queue_id, event_id),
        )
        return False

    def reset_stale_workspace_deliveries(self) -> int:
        with self.session() as session:
            stale_predicate = """
                delivery.sent_at IS NULL AND (
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
            """
            locked_stale = session.execute(
                f"""
                SELECT record_uuid, operation_uuid, account_uuid,
                       assignment_uuid, assignment_project_uuid,
                       provider_queue_id, provider_event_id, priority,
                       submission_state, submission_attempts, record
                FROM workspace_delivery_outbox AS delivery
                WHERE {stale_predicate}
                ORDER BY record_uuid
                FOR UPDATE
                """
            ).fetchall()
            candidate_operations = [
                row["operation_uuid"] for row in locked_stale
            ]
            if candidate_operations:
                # Terminal rejection also locks the durable outbox row before
                # its idempotency row. Keep the same explicit order here and
                # in accept_result so concurrent reset/result paths cannot
                # wait on one another in opposite directions.
                session.execute(
                    """
                    SELECT operation_uuid
                    FROM operation_idempotency
                    WHERE operation_uuid = ANY(%s)
                    ORDER BY operation_uuid
                    FOR UPDATE
                    """,
                    (candidate_operations,),
                ).fetchall()
            unsafe_provider_events: set[tuple[str, str, int]] = set()
            for locked in locked_stale:
                unsafe = locked["submission_state"] in {
                    "submitting",
                    "ambiguous",
                    "awaiting_result",
                } or (
                    locked["submission_state"] == "pending"
                    and int(locked["submission_attempts"]) > 0
                )
                if not unsafe:
                    continue
                account = session.execute(
                    """
                    SELECT generation
                    FROM desired_resources
                    WHERE resource_type = 'external_account'
                      AND resource_uuid = %s AND NOT deleted
                    """,
                    (locked["account_uuid"],),
                ).fetchone()
                record = typing.cast(dict[str, object], locked["record"])
                operation = typing.cast(
                    dict[str, object] | None,
                    record.get("operation"),
                )
                provider = (
                    typing.cast(dict[str, object], operation["provider"])
                    if operation is not None
                    else {}
                )
                compatible_assignment = None
                if locked["assignment_uuid"] is not None and account is not None:
                    compatible_assignment = session.execute(
                        """
                        SELECT resource_uuid, generation,
                               body->>'project_id' AS project_uuid
                        FROM desired_resources
                        WHERE resource_type = 'external_chat_assignment'
                          AND NOT deleted
                          AND body->>'external_account_uuid' = %s
                          AND body->'provider_chat'->>'provider_chat_key' = %s
                          AND body->>'project_id' = %s
                          AND COALESCE((body->>'selected')::boolean, true)
                        LIMIT 1
                        """,
                        (
                            str(locked["account_uuid"]),
                            str(provider.get("chat_id", "")),
                            str(locked["assignment_project_uuid"]),
                        ),
                    ).fetchone()
                compatible = account is not None and (
                    locked["assignment_uuid"] is None
                    or compatible_assignment is not None
                )
                payload = (
                    typing.cast(dict[str, object], operation.get("payload"))
                    if operation is not None
                    and isinstance(operation.get("payload"), dict)
                    else {}
                )
                stream_uuid = payload.get("stream_uuid")
                topic_uuid = payload.get("topic_uuid")
                if operation is not None and operation.get("kind") in {
                    "stream.upsert",
                    "stream.delete",
                }:
                    stream_uuid = operation.get("entity_uuid")
                if operation is not None and operation.get("kind") in {
                    "topic.upsert",
                    "topic.delete",
                }:
                    topic_uuid = operation.get("entity_uuid")
                if (
                    compatible
                    and compatible_assignment is not None
                    and stream_uuid is not None
                ):
                    stream_mapping = session.execute(
                        """
                        SELECT provider_id, metadata
                        FROM provider_mappings
                        WHERE account_uuid = %s
                          AND entity_kind = 'stream'
                          AND provider_id = %s
                          AND workspace_uuid = %s
                          AND metadata->>'project_uuid' = %s
                          AND NOT deleted
                        """,
                        (
                            locked["account_uuid"],
                            str(provider.get("chat_id", "")),
                            str(stream_uuid),
                            str(compatible_assignment["project_uuid"]),
                        ),
                    ).fetchone()
                    compatible = stream_mapping is not None
                    if (
                        compatible
                        and operation is not None
                        and operation.get("kind") == "stream.upsert"
                    ):
                        stream_metadata = typing.cast(
                            dict[str, object], stream_mapping["metadata"]
                        )
                        payload_participants = payload.get("participant_uuids")
                        mapping_participants = stream_metadata.get("participants")
                        compatible = (
                            str(provider.get("entity_id"))
                            == str(stream_mapping["provider_id"])
                            and payload.get("name") == stream_metadata.get("name")
                            and payload.get("description")
                            == stream_metadata.get("description")
                            and payload.get("private")
                            == stream_metadata.get("private")
                            and payload.get("chat_kind")
                            == stream_metadata.get("chat_type")
                            and isinstance(payload_participants, list)
                            and isinstance(mapping_participants, list)
                            and sorted(
                                str(value) for value in payload_participants
                            )
                            == sorted(
                                str(value) for value in mapping_participants
                            )
                            and (
                                None
                                if payload.get("default_topic_uuid") is None
                                else str(payload["default_topic_uuid"])
                            )
                            == (
                                None
                                if stream_metadata.get("default_topic_uuid") is None
                                else str(stream_metadata["default_topic_uuid"])
                            )
                        )
                if (
                    compatible
                    and compatible_assignment is not None
                    and topic_uuid is not None
                ):
                    topic_mapping = session.execute(
                        """
                        SELECT provider_id, metadata
                        FROM provider_mappings
                        WHERE account_uuid = %s
                          AND entity_kind = 'topic'
                          AND workspace_uuid = %s
                          AND metadata->>'chat_key' = %s
                          AND (
                              %s::text IS NULL
                              OR metadata->>'stream_uuid' = %s
                          )
                          AND NOT deleted
                        """,
                        (
                            locked["account_uuid"],
                            str(topic_uuid),
                            str(provider.get("chat_id", "")),
                            stream_uuid,
                            stream_uuid,
                        ),
                    ).fetchone()
                    compatible = topic_mapping is not None
                    if (
                        compatible
                        and operation is not None
                        and operation.get("kind") == "topic.upsert"
                    ):
                        topic_metadata = typing.cast(
                            dict[str, object], topic_mapping["metadata"]
                        )
                        compatible = (
                            str(provider.get("entity_id"))
                            == str(topic_mapping["provider_id"])
                            and payload.get("name") == topic_metadata.get("name")
                        )
                if compatible:
                    session.execute(
                        """
                        UPDATE workspace_delivery_outbox
                        SET account_generation = %s,
                            assignment_uuid = %s,
                            assignment_generation = %s,
                            assignment_project_uuid = %s,
                            next_submission_at = now()
                        WHERE record_uuid = %s AND sent_at IS NULL
                        """,
                        (
                            int(account["generation"]),
                            (
                                None
                                if compatible_assignment is None
                                else compatible_assignment["resource_uuid"]
                            ),
                            (
                                None
                                if compatible_assignment is None
                                else int(compatible_assignment["generation"])
                            ),
                            (
                                None
                                if compatible_assignment is None
                                else compatible_assignment["project_uuid"]
                            ),
                            locked["record_uuid"],
                        ),
                    )
                    continue
                if (
                    locked["provider_queue_id"] is not None
                    and locked["provider_event_id"] is not None
                ):
                    unsafe_provider_events.add(
                        (
                            str(locked["account_uuid"]),
                            str(locked["provider_queue_id"]),
                            int(locked["provider_event_id"]),
                        )
                    )
                    continue
                session.execute(
                    """
                    UPDATE workspace_delivery_outbox
                    SET submission_state = CASE
                            WHEN submission_state = 'submitting'
                            THEN 'submitting'
                            ELSE 'ambiguous'
                        END,
                        submission_error_code =
                            'workspace_delivery_assignment_ambiguous'
                    WHERE record_uuid = %s AND sent_at IS NULL
                    """,
                    (locked["record_uuid"],),
                )
                if int(locked["priority"]) == 2 and operation is not None:
                    session.execute(
                        """
                        UPDATE zulip_backfill_jobs
                        SET state = 'failed', lease_until = NULL,
                            last_error_code =
                                'workspace_delivery_assignment_ambiguous',
                            updated_at = now()
                        WHERE account_uuid = %s AND provider_chat_key = %s
                          AND state != 'cancelled'
                        """,
                        (
                            locked["account_uuid"],
                            str(provider.get("chat_id", "")),
                        ),
                    )
            stale = session.execute(
                f"""
                DELETE FROM workspace_delivery_outbox AS delivery
                WHERE {stale_predicate}
                  AND (
                      (
                          delivery.submission_state = 'pending'
                          AND delivery.submission_attempts = 0
                      )
                      OR delivery.submission_state = 'rejected'
                  )
                RETURNING operation_uuid, account_uuid,
                          assignment_project_uuid, provider_queue_id,
                          provider_event_id, priority, record
                """
            ).fetchall()
            operation_ids = [row["operation_uuid"] for row in stale]
            if operation_ids:
                session.execute(
                    """
                    DELETE FROM operation_idempotency
                    WHERE operation_uuid = ANY(%s)
                      AND (
                          terminal_outcome IS NULL
                          OR terminal_outcome = 'rejected'
                      )
                    """,
                    (operation_ids,),
                )
            recovered_provider_events: set[tuple[str, str, int]] = set()
            quarantined_provider_events: set[tuple[str, str, int]] = set()

            def quarantine_ambiguous_assignment(
                provider_event_key: tuple[str, str, int],
            ) -> None:
                if provider_event_key in quarantined_provider_events:
                    return
                quarantined_provider_events.add(provider_event_key)
                event_account_uuid, event_queue_id, event_id = provider_event_key
                # A sibling may already have reached Workspace. Replaying the
                # source against a new assignment generation could duplicate
                # that operation, while leaving the stale sibling in place
                # makes the journal event impossible to finalize.
                self._recover_provider_event_assignment_change(
                    session,
                    event_account_uuid,
                    event_queue_id,
                    event_id,
                    False,
                )
                session.execute(
                    """
                    UPDATE zulip_provider_events
                    SET processing_state = 'invalid',
                        processing_reason =
                            'workspace_delivery_assignment_ambiguous',
                        prepared_records = NULL
                    WHERE account_uuid = %s AND queue_id = %s
                      AND event_id = %s
                    """,
                    provider_event_key,
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
                provider_event_key = (
                    str(row["account_uuid"]),
                    str(row["provider_queue_id"]),
                    int(row["provider_event_id"]),
                )
                if provider_event_key in recovered_provider_events:
                    continue
                recovered_provider_events.add(provider_event_key)
                if provider_event_key in unsafe_provider_events:
                    quarantine_ambiguous_assignment(provider_event_key)
                    continue
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
                          AND (
                              processing_state = 'delivering'
                              OR (
                                  processing_state = 'invalid'
                                  AND processing_reason =
                                      'workspace_delivery_rejected'
                              )
                          )
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
            for provider_event_key in (
                unsafe_provider_events - quarantined_provider_events
            ):
                quarantine_ambiguous_assignment(provider_event_key)
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

    @staticmethod
    def _filter_rejected_grouped_read_dependencies(
        session: sessions.PgSQLSession,
    ) -> int:
        """Narrow never-submitted grouped reads to their still-valid subset."""
        rows = session.execute(
            """
            SELECT dependent.record_uuid, dependent.operation_uuid,
                   dependent.record, dependent.provider_queue_id,
                   dependent.provider_event_id,
                   blocked.message_uuids
            FROM workspace_delivery_outbox AS dependent
            JOIN LATERAL (
                SELECT array_agg(DISTINCT dependency.record->'operation'
                                     ->>'entity_uuid') AS message_uuids
                FROM workspace_delivery_outbox AS dependency
                JOIN operation_idempotency AS dependency_operation
                  ON dependency_operation.operation_uuid =
                     dependency.operation_uuid
                WHERE dependency.sent_at IS NULL
                  AND dependency.submission_state = 'rejected'
                  AND dependency_operation.terminal_outcome = 'rejected'
                  AND dependent.created_at <= dependency_operation.updated_at
                  AND dependency.account_uuid = dependent.account_uuid
                  AND dependency.assignment_uuid IS NOT DISTINCT FROM
                      dependent.assignment_uuid
                  AND dependency.assignment_generation IS NOT DISTINCT FROM
                      dependent.assignment_generation
                  AND dependency.assignment_project_uuid IS NOT DISTINCT FROM
                      dependent.assignment_project_uuid
                  AND (
                      (
                          dependency.record->>'origin' =
                              dependent.record->>'origin'
                          AND dependency.record->>'causal_lane' =
                              dependent.record->>'causal_lane'
                          AND (dependency.record->>'sequence')::bigint <
                              (dependent.record->>'sequence')::bigint
                      ) OR (
                          dependency.provider_queue_id =
                              dependent.provider_queue_id
                          AND dependency.provider_event_id =
                              dependent.provider_event_id
                      )
                  )
                  AND dependency.record->'operation'->>'entity_uuid' IN (
                      SELECT jsonb_array_elements_text(
                          dependent.record->'operation'->'payload'
                              ->'message_uuids'
                      )
                  )
                  AND (
                      dependency.record->'operation'->>'kind' =
                          'message.create'
                      OR (
                          dependency.record->'operation'->>'kind' =
                              'message.update'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM provider_mappings AS mapping
                              WHERE mapping.account_uuid =
                                    dependency.account_uuid
                                AND mapping.entity_kind = 'message'
                                AND mapping.workspace_uuid =
                                    (dependency.record->'operation'
                                        ->>'entity_uuid')::uuid
                                AND NOT mapping.deleted
                                AND mapping.metadata
                                        ->>'workspace_delivery_state' =
                                    'committed'
                                AND mapping.metadata->>'project_uuid' =
                                    dependency.record->>'project_uuid'
                                AND mapping.metadata->>'stream_uuid' =
                                    dependency.record->'operation'->'payload'
                                        ->>'stream_uuid'
                                AND mapping.metadata->>'topic_uuid' =
                                    dependency.record->'operation'->'payload'
                                        ->>'topic_uuid'
                          )
                      )
                  )
            ) AS blocked ON cardinality(blocked.message_uuids) > 0
            WHERE dependent.sent_at IS NULL
              AND dependent.submission_state = 'pending'
              AND dependent.submission_attempts = 0
              AND dependent.record->'operation'->>'kind' = 'read_state.set'
            FOR UPDATE OF dependent
            """
        ).fetchall()
        filtered = 0
        for row in rows:
            record = copy.deepcopy(typing.cast(dict[str, object], row["record"]))
            operation = typing.cast(dict[str, object], record["operation"])
            payload = typing.cast(dict[str, object], operation["payload"])
            affected = {str(value) for value in row["message_uuids"]}
            remaining = [
                str(value)
                for value in typing.cast(list[object], payload["message_uuids"])
                if str(value) not in affected
            ]
            if remaining:
                payload["message_uuids"] = remaining
                record["operation_sha256"] = canonical.operation_digest(record)
                updated = session.execute(
                    """
                    UPDATE workspace_delivery_outbox
                    SET record = %s
                    WHERE record_uuid = %s AND sent_at IS NULL
                      AND submission_state = 'pending'
                      AND submission_attempts = 0
                    RETURNING operation_uuid
                    """,
                    (json.dumps(record), row["record_uuid"]),
                ).fetchone()
                if updated is None:
                    continue
                session.execute(
                    """
                    UPDATE operation_idempotency
                    SET operation_sha256 = %s, updated_at = now()
                    WHERE operation_uuid = %s AND terminal_outcome IS NULL
                    """,
                    (record["operation_sha256"], row["operation_uuid"]),
                )
            else:
                removed = session.execute(
                    """
                    DELETE FROM workspace_delivery_outbox
                    WHERE record_uuid = %s AND sent_at IS NULL
                      AND submission_state = 'pending'
                      AND submission_attempts = 0
                    RETURNING operation_uuid
                    """,
                    (row["record_uuid"],),
                ).fetchone()
                if removed is None:
                    continue
                session.execute(
                    """
                    DELETE FROM operation_idempotency
                    WHERE operation_uuid = %s AND terminal_outcome IS NULL
                    """,
                    (row["operation_uuid"],),
                )
            filtered += 1
            if row["provider_queue_id"] is None or row["provider_event_id"] is None:
                continue
            if remaining:
                session.execute(
                    """
                    UPDATE zulip_provider_events AS event
                    SET prepared_records = (
                        SELECT COALESCE(
                            jsonb_agg(
                                CASE
                                    WHEN accepted.record->>'operation_uuid' = %s
                                    THEN %s::jsonb
                                    ELSE accepted.record
                                END
                                ORDER BY accepted.position
                            ),
                            '[]'::jsonb
                        )
                        FROM jsonb_array_elements(event.prepared_records)
                            WITH ORDINALITY AS accepted(record, position)
                    )
                    WHERE event.account_uuid = %s AND event.queue_id = %s
                      AND event.event_id = %s
                      AND event.processing_state = 'delivering'
                      AND event.prepared_records IS NOT NULL
                    """,
                    (
                        str(row["operation_uuid"]),
                        json.dumps(record),
                        str(record["account_uuid"]),
                        row["provider_queue_id"],
                        row["provider_event_id"],
                    ),
                )
            else:
                session.execute(
                    """
                    UPDATE zulip_provider_events AS event
                    SET prepared_records = (
                        SELECT COALESCE(
                            jsonb_agg(accepted.record ORDER BY accepted.position)
                                FILTER (
                                    WHERE accepted.record->>'operation_uuid' != %s
                                ),
                            '[]'::jsonb
                        )
                        FROM jsonb_array_elements(event.prepared_records)
                            WITH ORDINALITY AS accepted(record, position)
                    )
                    WHERE event.account_uuid = %s AND event.queue_id = %s
                      AND event.event_id = %s
                      AND event.processing_state = 'delivering'
                      AND event.prepared_records IS NOT NULL
                    """,
                    (
                        str(row["operation_uuid"]),
                        str(record["account_uuid"]),
                        row["provider_queue_id"],
                        row["provider_event_id"],
                    ),
                )
        return filtered

    @staticmethod
    def _quarantine_rejected_workspace_delivery_dependents(
        session: sessions.PgSQLSession,
    ) -> int:
        """Reject unsent records whose materialization dependency was rejected."""
        session.execute(
            """
            UPDATE operation_idempotency AS operation
            SET terminal_outcome = 'rejected', updated_at = now()
            FROM workspace_delivery_outbox AS delivery
            WHERE delivery.operation_uuid = operation.operation_uuid
              AND delivery.sent_at IS NULL
              AND delivery.submission_state = 'rejected'
              AND operation.terminal_outcome IS NULL
            """
        )
        session.execute(
            """
            UPDATE provider_mappings AS mapping
            SET deleted = true, updated_at = now()
            FROM workspace_delivery_outbox AS delivery
            JOIN operation_idempotency AS operation
              ON operation.operation_uuid = delivery.operation_uuid
            WHERE delivery.account_uuid = mapping.account_uuid
              AND delivery.sent_at IS NULL
              AND delivery.submission_state = 'rejected'
              AND operation.terminal_outcome = 'rejected'
              AND delivery.record->'operation'->>'kind' = 'message.create'
              AND mapping.entity_kind = 'message'
              AND mapping.workspace_uuid =
                  (delivery.record->'operation'->>'entity_uuid')::uuid
              AND mapping.metadata->>'workspace_delivery_state' = 'pending'
              AND NOT mapping.deleted
            """
        )
        RestAlchemyStore._filter_rejected_grouped_read_dependencies(session)
        total = 0
        while True:
            row = session.execute(
                """
                WITH blocked AS MATERIALIZED (
                    SELECT dependent.record_uuid
                    FROM workspace_delivery_outbox AS dependent
                    WHERE dependent.sent_at IS NULL
                      AND dependent.submission_state = 'pending'
                      AND EXISTS (
                          SELECT 1
                          FROM workspace_delivery_outbox AS dependency
                          JOIN operation_idempotency AS dependency_operation
                            ON dependency_operation.operation_uuid =
                               dependency.operation_uuid
                          WHERE dependency.sent_at IS NULL
                            AND dependency.submission_state = 'rejected'
                            AND dependency_operation.terminal_outcome = 'rejected'
                            -- Only records already prepared when the rejection
                            -- became terminal can depend on that failed
                            -- materialization.  Retained evidence must not
                            -- poison records created by later provider events.
                            AND dependent.created_at <=
                                dependency_operation.updated_at
                            AND dependency.account_uuid = dependent.account_uuid
                            AND dependency.assignment_uuid IS NOT DISTINCT FROM
                                dependent.assignment_uuid
                            AND dependency.assignment_generation
                                IS NOT DISTINCT FROM
                                dependent.assignment_generation
                            AND dependency.assignment_project_uuid
                                IS NOT DISTINCT FROM
                                dependent.assignment_project_uuid
                            AND (
                                (
                                    dependency.record->>'origin' =
                                        dependent.record->>'origin'
                                    AND dependency.record->>'causal_lane' =
                                        dependent.record->>'causal_lane'
                                    AND (dependency.record
                                            ->>'sequence')::bigint <
                                        (dependent.record
                                            ->>'sequence')::bigint
                                ) OR (
                                    dependency.provider_queue_id =
                                        dependent.provider_queue_id
                                    AND dependency.provider_event_id =
                                        dependent.provider_event_id
                                )
                            )
                            AND (
                                (
                                    dependency.record->'operation'->>'kind' =
                                        'topic.upsert'
                                    AND dependent.record->'operation'->>'kind' IN (
                                        'message.create', 'message.update',
                                        'read_state.set', 'reaction.upsert',
                                        'reaction.delete'
                                    )
                                    AND dependency.record->'operation'
                                            ->>'entity_uuid' =
                                        dependent.record->'operation'->'payload'
                                            ->>'topic_uuid'
                                )
                                OR (
                                    dependency.record->'operation'->>'kind' =
                                        'message.create'
                                    AND dependent.record->'operation'->>'kind' IN (
                                        'message.update', 'message.delete'
                                    )
                                    AND dependency.record->'operation'
                                            ->>'entity_uuid' =
                                        dependent.record->'operation'
                                            ->>'entity_uuid'
                                )
                                OR (
                                    dependent.record->'operation'->>'kind' =
                                        'read_state.set'
                                    AND (
                                        dependency.record->'operation'->>'kind' =
                                            'message.create'
                                        OR (
                                            dependency.record->'operation'
                                                    ->>'kind' = 'message.update'
                                            AND NOT EXISTS (
                                                SELECT 1
                                                FROM provider_mappings AS mapping
                                                WHERE mapping.account_uuid =
                                                      dependency.account_uuid
                                                  AND mapping.entity_kind =
                                                      'message'
                                                  AND mapping.workspace_uuid =
                                                      (dependency.record
                                                          ->'operation'
                                                          ->>'entity_uuid')::uuid
                                                  AND NOT mapping.deleted
                                                  AND mapping.metadata
                                                          ->>'workspace_delivery_state'
                                                      = 'committed'
                                                  AND mapping.metadata
                                                          ->>'project_uuid' =
                                                      dependency.record
                                                          ->>'project_uuid'
                                                  AND mapping.metadata
                                                          ->>'stream_uuid' =
                                                      dependency.record
                                                          ->'operation'->'payload'
                                                          ->>'stream_uuid'
                                                  AND mapping.metadata
                                                          ->>'topic_uuid' =
                                                      dependency.record
                                                          ->'operation'->'payload'
                                                          ->>'topic_uuid'
                                            )
                                        )
                                    )
                                    AND dependency.record->'operation'
                                            ->>'entity_uuid' IN (
                                        SELECT jsonb_array_elements_text(
                                            dependent.record->'operation'->'payload'
                                                ->'message_uuids'
                                        )
                                    )
                                )
                                OR (
                                    dependent.record->'operation'->>'kind' IN (
                                        'reaction.upsert', 'reaction.delete'
                                    )
                                    AND dependency.record->'operation'->>'kind' =
                                        'message.create'
                                    AND dependency.record->'operation'
                                            ->>'entity_uuid' =
                                        dependent.record->'operation'->'payload'
                                            ->>'message_uuid'
                                )
                            )
                      )
                ), rejected AS (
                    UPDATE workspace_delivery_outbox AS delivery
                    SET submission_state = 'rejected',
                        submission_error_code =
                            'workspace_delivery_dependency_rejected'
                    FROM blocked
                    WHERE delivery.record_uuid = blocked.record_uuid
                      AND delivery.sent_at IS NULL
                      AND delivery.submission_state = 'pending'
                    RETURNING delivery.account_uuid,
                              delivery.operation_uuid,
                              delivery.provider_queue_id,
                              delivery.provider_event_id
                ), terminalized AS (
                    UPDATE operation_idempotency AS operation
                    SET terminal_outcome = 'rejected', updated_at = now()
                    FROM rejected
                    WHERE operation.operation_uuid = rejected.operation_uuid
                      AND operation.terminal_outcome IS NULL
                    RETURNING operation.operation_uuid
                ), marked AS (
                    UPDATE zulip_provider_events AS event
                    SET processing_reason = 'workspace_delivery_rejected'
                    FROM (
                        SELECT DISTINCT account_uuid, provider_queue_id,
                                        provider_event_id
                        FROM rejected
                        WHERE provider_queue_id IS NOT NULL
                          AND provider_event_id IS NOT NULL
                    ) AS source
                    WHERE event.account_uuid = source.account_uuid
                      AND event.queue_id = source.provider_queue_id
                      AND event.event_id = source.provider_event_id
                      AND event.processing_state = 'delivering'
                    RETURNING event.event_id
                )
                SELECT count(*) AS total FROM rejected
                """
            ).fetchone()
            quarantined = int(row["total"])
            total += quarantined
            if quarantined == 0:
                return total

    def finalize_ready_provider_events(self) -> int:
        """Finish deliveries once every outbox record reached a terminal state.

        A permanent Provider API rejection is terminal for the immutable
        record, but it is not a successful delivery.  Keep its durable outbox
        evidence and quarantine the source journal event instead of leaving a
        ``delivering`` predecessor that blocks its causal lane forever.
        """
        with self.session() as session:
            self._quarantine_rejected_workspace_delivery_dependents(session)
            rows = session.execute(
                """
                SELECT event.account_uuid, event.queue_id, event.event_id,
                       event.body,
                       EXISTS (
                           SELECT 1
                           FROM workspace_delivery_outbox AS rejected
                           WHERE rejected.account_uuid = event.account_uuid
                             AND rejected.provider_queue_id = event.queue_id
                             AND rejected.provider_event_id = event.event_id
                             AND rejected.sent_at IS NULL
                             AND rejected.submission_state = 'rejected'
                       ) AS rejected
                FROM zulip_provider_events AS event
                WHERE event.processing_state = 'delivering'
                  AND NOT EXISTS (
                      SELECT 1 FROM workspace_delivery_outbox AS delivery
                      WHERE delivery.account_uuid = event.account_uuid
                        AND delivery.provider_queue_id = event.queue_id
                        AND delivery.provider_event_id = event.event_id
                        AND delivery.sent_at IS NULL
                        AND delivery.submission_state != 'rejected'
                  )
                FOR UPDATE
                """
            ).fetchall()
            for row in rows:
                event = typing.cast(dict[str, object], row["body"])
                rejected = bool(row["rejected"])
                if not rejected and event.get("type") == "delete_message":
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
                    SET processing_state = CASE
                            WHEN %s::boolean THEN 'invalid'
                            ELSE 'processed'
                        END,
                        processing_reason = CASE
                            WHEN %s::boolean
                            THEN 'workspace_delivery_rejected'
                            ELSE processing_reason
                        END,
                        prepared_records = CASE
                            WHEN %s::boolean THEN prepared_records
                            ELSE NULL
                        END
                    WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                      AND processing_state = 'delivering'
                    """,
                    (
                        rejected,
                        rejected,
                        rejected,
                        row["account_uuid"],
                        row["queue_id"],
                        row["event_id"],
                    ),
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
            session.execute(
                """
                WITH candidates AS MATERIALIZED (
                    SELECT operation.record_uuid,
                           operation.operation_uuid
                    FROM bridge_operations AS operation
                    JOIN operation_idempotency AS legacy
                      ON legacy.operation_uuid = operation.operation_uuid
                     AND legacy.terminal_outcome IS NOT NULL
                    WHERE operation.state = 'committed'
                      AND operation.result_sent_at IS NOT NULL
                      AND NOT operation.manual_reconciliation_required
                      AND operation.last_error_code IS DISTINCT FROM
                          'provider_result_stale_lease'
                      AND operation.updated_at <
                          now() - interval '10 minutes'
                      AND operation.record->'operation'->>'kind' =
                          'read_state.set'
                    ORDER BY operation.updated_at, operation.record_uuid
                    LIMIT %s
                    FOR UPDATE OF operation SKIP LOCKED
                ), physical_idempotency AS (
                    INSERT INTO operation_idempotency (
                        operation_uuid, operation_sha256, terminal_outcome,
                        target_entity_id, target_revision, result_record_uuid,
                        manual_retry_allowed, updated_at
                    )
                    SELECT
                        candidate.record_uuid,
                        legacy.operation_sha256,
                        legacy.terminal_outcome,
                        legacy.target_entity_id,
                        legacy.target_revision,
                        legacy.result_record_uuid,
                        legacy.manual_retry_allowed,
                        legacy.updated_at
                    FROM candidates AS candidate
                    JOIN operation_idempotency AS legacy
                      ON legacy.operation_uuid = candidate.operation_uuid
                    ON CONFLICT (operation_uuid) DO NOTHING
                    RETURNING operation_uuid
                ), safe AS MATERIALIZED (
                    SELECT candidate.record_uuid
                    FROM candidates AS candidate
                    JOIN operation_idempotency AS legacy
                      ON legacy.operation_uuid = candidate.operation_uuid
                    LEFT JOIN operation_idempotency AS physical
                      ON physical.operation_uuid = candidate.record_uuid
                    LEFT JOIN physical_idempotency AS inserted
                      ON inserted.operation_uuid = candidate.record_uuid
                    WHERE inserted.operation_uuid IS NOT NULL
                       OR (
                            physical.terminal_outcome IS NOT DISTINCT FROM
                                legacy.terminal_outcome
                            AND physical.manual_retry_allowed =
                                legacy.manual_retry_allowed
                          )
                )
                DELETE FROM bridge_operations AS operation
                USING safe
                WHERE operation.record_uuid = safe.record_uuid
                """,
                (limit,),
            )
            # Walk old terminal reports through a durable cursor and probe the
            # semantic head through the resource-order index.  Both the outer
            # scan and every head lookup stay bounded even when every retained
            # report belongs to a distinct resource.
            session.execute(
                """
                WITH prune_cursor AS MATERIALIZED (
                    SELECT last_completed_at, last_report_uuid
                    FROM observed_report_prune_state
                    WHERE singleton
                    FOR UPDATE
                ), probe AS MATERIALIZED (
                    SELECT report.report_uuid, report.body,
                           report.completed_at
                    FROM observed_report_outbox AS report
                    CROSS JOIN prune_cursor AS cursor
                    WHERE report.completed_at <
                          now() - interval '10 minutes'
                      AND (
                          cursor.last_completed_at IS NULL
                          OR (report.completed_at, report.report_uuid) >
                             (cursor.last_completed_at,
                              cursor.last_report_uuid)
                      )
                    ORDER BY report.completed_at, report.report_uuid
                    LIMIT %s
                    FOR UPDATE OF report SKIP LOCKED
                ), classified AS MATERIALIZED (
                    SELECT probe.report_uuid,
                           semantic_head.report_uuid AS head_report_uuid
                    FROM probe
                    JOIN LATERAL (
                        SELECT candidate.report_uuid
                        FROM observed_report_outbox AS candidate
                        WHERE candidate.body->>'resource_type' =
                                  probe.body->>'resource_type'
                          AND (candidate.body->>'resource_uuid')::uuid =
                              (probe.body->>'resource_uuid')::uuid
                        ORDER BY
                            (candidate.body->>'observed_generation')::bigint DESC,
                            COALESCE(
                                workspace_bridge_observed_at(
                                    candidate.body->>'observed_at'
                                ),
                                candidate.created_at
                            ) DESC,
                            candidate.created_at DESC,
                            candidate.report_uuid DESC
                        LIMIT 1
                    ) AS semantic_head ON true
                ), deleted AS (
                    DELETE FROM observed_report_outbox AS report
                    USING classified
                    WHERE report.report_uuid = classified.report_uuid
                      AND classified.head_report_uuid <>
                          classified.report_uuid
                    RETURNING report.report_uuid
                ), advanced AS (
                    UPDATE observed_report_prune_state AS state
                    SET last_completed_at = tail.completed_at,
                        last_report_uuid = tail.report_uuid
                    FROM (
                        SELECT completed_at, report_uuid
                        FROM probe
                        ORDER BY completed_at DESC, report_uuid DESC
                        LIMIT 1
                    ) AS tail
                    WHERE state.singleton
                    RETURNING state.singleton
                ), rewound AS (
                    UPDATE observed_report_prune_state AS state
                    SET last_completed_at = NULL,
                        last_report_uuid = NULL
                    WHERE state.singleton
                      AND NOT EXISTS (SELECT 1 FROM probe)
                      AND EXISTS (
                          SELECT 1
                          FROM prune_cursor
                          WHERE last_completed_at IS NOT NULL
                      )
                    RETURNING state.singleton
                )
                SELECT count(*) AS total FROM deleted
                """,
                (limit,),
            )
            session.execute(
                """
                DELETE FROM scheduler_provider_event_lanes AS lane
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM zulip_provider_events AS event
                    WHERE event.account_uuid = lane.account_uuid
                      AND event.causal_lane = lane.causal_lane
                      AND event.processing_state IN ('pending', 'delivering')
                )
                """
            )
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
                    ORDER BY (body->>'resource_uuid')::uuid,
                             (body->>'observed_generation')::bigint DESC,
                             COALESCE(
                                 workspace_bridge_observed_at(
                                     body->>'observed_at'
                                 ),
                                 created_at
                             ) DESC,
                             created_at DESC,
                             report_uuid DESC
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
                    ORDER BY (body->>'resource_uuid')::uuid,
                             (body->>'observed_generation')::bigint DESC,
                             COALESCE(
                                 workspace_bridge_observed_at(
                                     body->>'observed_at'
                                 ),
                                 created_at
                             ) DESC,
                             created_at DESC,
                             report_uuid DESC
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
            attempt = int(record["attempt"])
            operation = typing.cast(dict[str, object], record["operation"])
            if (
                attempt == 1
                and prior is not None
                and prior["terminal_outcome"] is not None
                and operation.get("kind") == "read_state.set"
            ):
                # A compact terminal operation can be pruned after its result
                # acknowledgement.  Older retained digests include a generated
                # read timestamp, so recognize the immutable physical page UUID
                # before comparing that legacy digest.
                return False
            if prior is not None and prior["operation_sha256"] != operation_sha256:
                raise ValueError("Operation UUID reused with a different digest")
            if (
                attempt == 1
                and prior is not None
                and prior["terminal_outcome"] is not None
            ):
                # A compact terminal operation can be pruned after its result
                # acknowledgement.  Its durable idempotency row must still
                # prevent a delayed lease replay from calling Zulip again.
                return False
            if attempt > 1 and (
                prior is None
                or prior["terminal_outcome"] not in {"rejected", "expired"}
                or prior["manual_retry_allowed"] is not True
            ):
                raise ValueError("Higher attempt is not authorized by prior result")
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
        read_semantic_sha256 = _provider_read_semantic_sha256(record)
        with self.session() as session:
            updated = session.execute(
                """
                WITH candidate AS (
                    SELECT %s::jsonb AS record,
                           %s::text AS read_semantic_sha256
                )
                UPDATE bridge_operations AS persisted
                SET record = CASE
                        WHEN candidate.read_semantic_sha256 IS NULL THEN
                            jsonb_set(
                                persisted.record, '{transport}', %s::jsonb, true
                            )
                        ELSE jsonb_set(
                            jsonb_set(
                                persisted.record, '{transport}', %s::jsonb, true
                            ),
                            '{_workspace_read_semantic_sha256}',
                            to_jsonb(candidate.read_semantic_sha256),
                            true
                        )
                    END,
                    result_record = CASE
                        WHEN persisted.result_record IS NULL THEN NULL
                        ELSE jsonb_set(
                            persisted.result_record,
                            '{transport}', %s::jsonb, true
                        )
                    END,
                    result_sent_at = CASE
                        WHEN persisted.result_record IS NULL
                        THEN persisted.result_sent_at
                        ELSE NULL
                    END,
                    last_error_code = CASE
                        WHEN persisted.result_record IS NOT NULL
                         AND persisted.last_error_code =
                             'provider_result_stale_lease'
                        THEN NULL
                        ELSE persisted.last_error_code
                    END,
                    expires_at = %s,
                    updated_at = now()
                FROM candidate
                WHERE persisted.record_uuid = %s
                  AND (
                      (
                          persisted.operation_uuid = %s
                          AND persisted.operation_sha256 = %s
                      )
                      OR (
                          persisted.record #>> '{operation,kind}' =
                              'read_state.set'
                          AND candidate.record #>> '{operation,kind}' =
                              'read_state.set'
                          AND (
                              persisted.record
                                  ->>'_workspace_read_semantic_sha256' =
                                  candidate.read_semantic_sha256
                              OR (
                                  persisted.record
                                      ->>'_workspace_read_semantic_sha256'
                                      IS NULL
                                  AND (
                                      persisted.record
                                          #- '{operation,occurred_at}'
                                  ) - ARRAY[
                                      'created_at', 'expires_at',
                                      'operation_sha256', 'operation_uuid',
                                      'predecessor_operation_uuid', 'sequence',
                                      'transport'
                                  ] = (
                                      candidate.record
                                          #- '{operation,occurred_at}'
                                  ) - ARRAY[
                                      'created_at', 'expires_at',
                                      'operation_sha256', 'operation_uuid',
                                      'predecessor_operation_uuid', 'sequence',
                                      'transport'
                                  ]
                              )
                              OR (
                                  persisted.record
                                      ->>'_workspace_read_semantic_sha256'
                                      IS NULL
                                  AND persisted.result_record IS NOT NULL
                                  AND persisted.state IN (
                                      'committed', 'rejected', 'expired',
                                      'cancelled'
                                  )
                                  AND persisted.record
                                      #> '{operation,payload,message_uuids}' =
                                      '[]'::jsonb
                                  AND (
                                      persisted.record
                                          #- '{operation,occurred_at}'
                                  ) - ARRAY[
                                      'created_at', 'expires_at',
                                      'operation_sha256', 'operation_uuid',
                                      'predecessor_operation_uuid', 'sequence',
                                      'transport'
                                  ] = (
                                      jsonb_set(
                                          candidate.record,
                                          '{operation,payload,message_uuids}',
                                          '[]'::jsonb,
                                          false
                                      ) #- '{operation,occurred_at}'
                                  ) - ARRAY[
                                      'created_at', 'expires_at',
                                      'operation_sha256', 'operation_uuid',
                                      'predecessor_operation_uuid', 'sequence',
                                      'transport'
                                  ]
                              )
                          )
                      )
                  )
                RETURNING persisted.record_uuid
                """,
                (
                    json.dumps(record),
                    read_semantic_sha256,
                    json.dumps(transport),
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
            # Provider-event preparation holds this same lock from its final
            # mapping refresh through outbox persistence.  Taking it before
            # terminalization makes either the read or the rejection wholly
            # visible first; neither can persist from an uncommitted snapshot.
            target = session.execute(
                """
                SELECT account_uuid,
                       record->'operation'->>'kind' AS operation_kind,
                       record->'operation'->'provider'->>'entity_id'
                           AS provider_entity_id
                FROM workspace_delivery_outbox
                WHERE record_uuid = %s
                  AND submission_state = 'submitting'
                  AND sent_at IS NULL
                """,
                (record_uuid,),
            ).fetchone()
            if (
                target is not None
                and target["operation_kind"]
                in {"message.create", "message.update", "message.delete"}
                and target["provider_entity_id"] is not None
            ):
                session.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (
                        _provider_mapping_lock_key(
                            str(target["account_uuid"]),
                            "message",
                            str(target["provider_entity_id"]),
                        ),
                    ),
                )
            row = session.execute(
                """
                WITH rejected AS (
                    UPDATE workspace_delivery_outbox
                    SET submission_state = 'rejected',
                        submission_error_code = %s
                    WHERE record_uuid = %s
                      AND submission_state = 'submitting'
                      AND sent_at IS NULL
                    RETURNING operation_uuid, account_uuid,
                              provider_queue_id, provider_event_id, record
                ), terminalized AS (
                    UPDATE operation_idempotency AS operation
                    -- Unlike now(), this records the boundary after any wait
                    -- for the mapping lock above, not transaction start time.
                    SET terminal_outcome = 'rejected',
                        updated_at = clock_timestamp()
                    FROM rejected
                    WHERE operation.operation_uuid = rejected.operation_uuid
                      AND operation.terminal_outcome IS NULL
                    RETURNING operation.operation_uuid
                ), retired_mapping AS (
                    UPDATE provider_mappings AS mapping
                    SET deleted = true, updated_at = clock_timestamp()
                    FROM rejected
                    WHERE rejected.record->'operation'->>'kind' =
                          'message.create'
                      AND mapping.account_uuid = rejected.account_uuid
                      AND mapping.entity_kind = 'message'
                      AND mapping.workspace_uuid =
                          (rejected.record->'operation'->>'entity_uuid')::uuid
                      AND mapping.metadata->>'workspace_delivery_state' =
                          'pending'
                      AND NOT mapping.deleted
                    RETURNING mapping.workspace_uuid
                ), reset_topic_mapping AS (
                    UPDATE provider_mappings AS mapping
                    SET metadata = jsonb_set(
                            mapping.metadata,
                            '{workspace_delivery_state}',
                            '"pending"'::jsonb,
                            true
                        ),
                        updated_at = clock_timestamp()
                    FROM rejected
                    WHERE rejected.record->'operation'->>'kind' =
                          'topic.upsert'
                      AND mapping.account_uuid = rejected.account_uuid
                      AND mapping.entity_kind = 'topic'
                      AND mapping.workspace_uuid =
                          (rejected.record->'operation'->>'entity_uuid')::uuid
                      AND mapping.provider_id = rejected.record->'operation'
                              ->'provider'->>'entity_id'
                      AND NOT mapping.deleted
                    RETURNING mapping.workspace_uuid
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
        provider_message_id = provider.get("entity_id")
        if kind in {"message.update", "message.delete"} and (
            provider_message_id is not None
        ):
            session.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (
                    _provider_mapping_lock_key(
                        account_uuid, "message", str(provider_message_id)
                    ),
                ),
            )
        if kind == "message.create":
            if provider_entity_id is None:
                raise ValueError("Committed message create has no provider identifier")
            session.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (
                    _provider_mapping_lock_key(
                        account_uuid, "message", provider_entity_id
                    ),
                ),
            )
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

    @staticmethod
    def _provider_event_causal_lane(
        session: sessions.PgSQLSession,
        account_uuid: str,
        event: dict[str, object],
    ) -> str | None:
        if _provider_event_requires_account_barrier(event):
            return None
        static_lane = _provider_event_static_causal_lane(event)
        if static_lane is not None:
            return static_lane
        message_ids = _provider_event_message_ids(event)
        if not message_ids:
            return None
        mapped = session.execute(
            """
            SELECT provider_id, metadata->>'chat_key' AS causal_lane
            FROM provider_mappings
            WHERE account_uuid = %s AND entity_kind = 'message'
              AND provider_id = ANY(%s) AND NOT deleted
              AND metadata->>'chat_key' IS NOT NULL
            """,
            (account_uuid, message_ids),
        ).fetchall()
        resolved = {
            str(row["provider_id"]): str(row["causal_lane"]) for row in mapped
        }
        if set(resolved) == set(message_ids):
            resolved_lanes = set(resolved.values())
            return resolved_lanes.pop() if len(resolved_lanes) == 1 else None
        sources = session.execute(
            """
                SELECT DISTINCT ON (source.provider_id)
                       source.provider_id, source.causal_lane
                FROM (
                    SELECT body->'message'->>'id' AS provider_id,
                           causal_lane, created_at, event_id, queue_id
                    FROM zulip_provider_events
                    WHERE account_uuid = %s AND event_type = 'message'
                      AND body->'message'->>'id' = ANY(%s)
                      AND causal_lane IS NOT NULL
                    UNION ALL
                    SELECT provider_message_context->>'id' AS provider_id,
                           causal_lane, created_at, event_id, queue_id
                    FROM zulip_provider_events
                    WHERE account_uuid = %s
                      AND provider_message_context->>'id' = ANY(%s)
                      AND causal_lane IS NOT NULL
                ) AS source
                ORDER BY source.provider_id, source.created_at,
                         source.event_id, source.queue_id
            """,
            (account_uuid, message_ids, account_uuid, message_ids),
        ).fetchall()
        for row in sources:
            resolved.setdefault(str(row["provider_id"]), str(row["causal_lane"]))
        resolved_lanes = set(resolved.values())
        if set(resolved) == set(message_ids) and len(resolved_lanes) == 1:
            return resolved_lanes.pop()
        if len(message_ids) == 1:
            return f"message:{message_ids[0]}"
        return None

    def record_provider_event(
        self, account_uuid: str, queue_id: str, event: dict[str, object]
    ) -> bool:
        with self.session() as session:
            causal_lane = self._provider_event_causal_lane(
                session, account_uuid, event
            )
            result = session.execute(
                """
                WITH registered_account AS (
                    INSERT INTO scheduler_accounts (account_uuid)
                    VALUES (%s)
                    ON CONFLICT (account_uuid) DO NOTHING
                ), registered_lane AS (
                    INSERT INTO scheduler_provider_event_lanes (
                        account_uuid, causal_lane
                    )
                    SELECT %s, %s::text WHERE %s::text IS NOT NULL
                    ON CONFLICT (account_uuid, causal_lane) DO NOTHING
                )
                INSERT INTO zulip_provider_events (
                    account_uuid, queue_id, event_id, event_type, body,
                    causal_lane
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_uuid, queue_id, event_id) DO NOTHING
                RETURNING event_id
                """,
                (
                    account_uuid,
                    account_uuid,
                    causal_lane,
                    causal_lane,
                    account_uuid,
                    queue_id,
                    int(event["id"]),
                    str(event["type"]),
                    json.dumps(event),
                    causal_lane,
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
                SET seen_provider_message_ids = COALESCE(
                        (
                            SELECT jsonb_agg(DISTINCT value)
                            FROM jsonb_array_elements(
                                seen_provider_message_ids || %s::jsonb
                            ) AS values(value)
                        ),
                        '[]'::jsonb
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
                ORDER BY updated_at, record_uuid LIMIT %s
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
                WHERE (result_record->>'record_uuid')::uuid = %s
                """,
                (record_uuid,),
            )

    def finalize_provider_result_response(
        self,
        record_uuid: str,
        status: str,
        lease_uuid: str | None = None,
    ) -> None:
        """Persist one terminal Provider API acknowledgement without retry loops."""
        self.finalize_provider_result_responses(
            [(record_uuid, status, lease_uuid)]
        )

    def finalize_provider_result_responses(
        self,
        responses: list[tuple[str, str, str | None]],
    ) -> None:
        """Persist one Provider API result page with two indexed SQL statements."""
        if not responses:
            return
        allowed_statuses = {
            "applied",
            "duplicate",
            "conflict",
            "not_found",
            "stale_lease",
            "rejected",
        }
        if any(status not in allowed_statuses for _, status, _ in responses):
            raise ValueError("Unsupported Provider result response status")
        result_uuids = [record_uuid for record_uuid, _, _ in responses]
        statuses = [status for _, status, _ in responses]
        lease_uuids = [lease_uuid for _, _, lease_uuid in responses]
        with self.session() as session:
            persisted_rows = session.execute(
                """
                WITH responses AS (
                    SELECT *
                    FROM unnest(%s::uuid[], %s::text[], %s::text[])
                         WITH ORDINALITY AS response(
                             result_record_uuid, status, lease_uuid, ordinal
                         )
                )
                SELECT operation.record_uuid, operation.record,
                       response.result_record_uuid, response.status,
                       response.lease_uuid, response.ordinal
                FROM responses AS response
                JOIN bridge_operations AS operation
                  ON (operation.result_record->>'record_uuid')::uuid =
                     response.result_record_uuid
                WHERE response.lease_uuid IS NULL
                   OR operation.result_record #>> '{transport,lease_uuid}' =
                      response.lease_uuid
                ORDER BY response.ordinal
                FOR UPDATE OF operation
                """,
                (result_uuids, statuses, lease_uuids),
            ).fetchall()
            if not persisted_rows:
                return
            operation_uuids = []
            matched_statuses = []
            semantic_sha256s = []
            for row in persisted_rows:
                persisted_record = typing.cast(dict[str, object], row["record"])
                stored_semantic_sha256 = persisted_record.get(
                    "_workspace_read_semantic_sha256"
                )
                semantic_sha256s.append(
                    stored_semantic_sha256
                    if isinstance(stored_semantic_sha256, str)
                    else _provider_read_semantic_sha256(persisted_record)
                )
                operation_uuids.append(str(row["record_uuid"]))
                matched_statuses.append(str(row["status"]))
            session.execute(
                """
                WITH responses AS (
                    SELECT *
                    FROM unnest(%s::uuid[], %s::text[], %s::text[])
                         AS response(
                             record_uuid, status, read_semantic_sha256
                         )
                )
                UPDATE bridge_operations AS operation
                SET result_sent_at = now(),
                    record = CASE
                        WHEN response.status IN ('applied', 'duplicate')
                         AND operation.record #>> '{operation,kind}' =
                             'read_state.set'
                        THEN jsonb_set(
                            jsonb_set(
                                operation.record,
                                '{_workspace_read_semantic_sha256}',
                                to_jsonb(response.read_semantic_sha256),
                                true
                            ),
                            '{operation,payload,message_uuids}',
                            '[]'::jsonb,
                            false
                        )
                        ELSE operation.record
                    END,
                    manual_reconciliation_required =
                        operation.manual_reconciliation_required
                        OR response.status IN (
                            'conflict', 'not_found', 'rejected'
                        ),
                    last_error_code = CASE
                        WHEN response.status IN ('applied', 'duplicate')
                        THEN NULL
                        ELSE 'provider_result_' || response.status
                    END,
                    reconciliation_evidence = CASE
                        WHEN response.status IN ('applied', 'duplicate')
                        THEN operation.reconciliation_evidence
                        ELSE operation.reconciliation_evidence ||
                             jsonb_build_array(
                                 jsonb_build_object(
                                     'kind', 'provider_result_response',
                                     'status', response.status
                                 )
                             )
                    END,
                    updated_at = now()
                FROM responses AS response
                WHERE operation.record_uuid = response.record_uuid
                """,
                (operation_uuids, matched_statuses, semantic_sha256s),
            )

    def accept_result(self, result: dict[str, object]) -> None:
        result_body = typing.cast(dict[str, object], result["result"])
        outcome = str(result_body["outcome"])
        with self.session() as session:
            delivery = session.execute(
                """
                SELECT record
                FROM workspace_delivery_outbox
                WHERE operation_uuid = %s
                FOR UPDATE
                """,
                (str(result["operation_uuid"]),),
            ).fetchone()
            if delivery is None:
                raise ValueError("Result does not match a known operation")
            row = session.execute(
                """
                SELECT operation_sha256, terminal_outcome,
                       result_record_uuid, target_entity_id,
                       target_revision, manual_retry_allowed
                FROM operation_idempotency
                WHERE operation_uuid = %s
                FOR UPDATE
                """,
                (str(result["operation_uuid"]),),
            ).fetchone()
            if row is None:
                raise ValueError("Result does not match a known operation")
            if row["operation_sha256"] != result["operation_sha256"]:
                raise ValueError("Result operation digest mismatch")
            operation_record = typing.cast(dict[str, object], delivery["record"])
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

    @staticmethod
    def _enqueue_observed_report_in_session(
        session: sessions.PgSQLSession,
        report: dict[str, object],
        *,
        confirm_existing: bool,
    ) -> bool:
        previous = session.execute(
            """
            SELECT body, result_status,
                   result_status = 'rejected'
                   AND completed_at <=
                       clock_timestamp() - interval '5 minutes'
                       AS rejection_cooldown_elapsed
            FROM (
                SELECT body, result_status, completed_at
                FROM observed_report_outbox
                WHERE body->>'resource_type' = %s
                  AND (body->>'resource_uuid')::uuid = %s::uuid
                ORDER BY (body->>'observed_generation')::bigint DESC,
                         COALESCE(
                             workspace_bridge_observed_at(
                                 body->>'observed_at'
                             ),
                             created_at
                         ) DESC,
                         created_at DESC,
                         report_uuid DESC
                LIMIT 1
                FOR UPDATE
            ) AS latest
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
            if (
                previous_semantic == report_semantic
                and not previous["rejection_cooldown_elapsed"]
            ):
                return bool(
                    confirm_existing
                    and previous["result_status"] not in {"rejected", "stale"}
                )
        row = session.execute(
            """
            INSERT INTO observed_report_outbox (report_uuid, body)
            VALUES (%s, %s)
            ON CONFLICT (report_uuid) DO NOTHING
            RETURNING report_uuid
            """,
            (
                str(report["report_uuid"]),
                json.dumps(report),
            ),
        ).fetchone()
        return row is not None

    def _enqueue_observed_report(
        self,
        report: dict[str, object],
        *,
        confirm_existing: bool,
    ) -> bool:
        with self.session() as session:
            return self._enqueue_observed_report_in_session(
                session,
                report,
                confirm_existing=confirm_existing,
            )

    def enqueue_observed_report(self, report: dict[str, object]) -> bool:
        return self._enqueue_observed_report(report, confirm_existing=False)

    def ensure_observed_report(self, report: dict[str, object]) -> bool:
        """Ensure an equivalent report is still eligible to satisfy control."""
        return self._enqueue_observed_report(report, confirm_existing=True)

    def ensure_provider_event_catalog_report(
        self,
        report: dict[str, object],
        account_uuid: str,
        queue_id: str,
        event_id: int,
    ) -> bool:
        """Atomically ensure a live catalog report and mark its journal event."""
        with self.session() as session:
            durable = self._enqueue_observed_report_in_session(
                session,
                report,
                confirm_existing=True,
            )
            if not durable:
                return False
            marker = session.execute(
                """
                WITH marker AS (
                    SELECT account_uuid, queue_id, event_id, event_type,
                           causal_lane, body
                    FROM zulip_provider_events
                    WHERE account_uuid = %s AND queue_id = %s AND event_id = %s
                      AND processing_state = 'pending'
                )
                UPDATE zulip_provider_events AS event
                SET assignment_catalog_reported_at = COALESCE(
                        event.assignment_catalog_reported_at,
                        now()
                    )
                FROM marker
                WHERE event.account_uuid = marker.account_uuid
                  AND event.processing_state = 'pending'
                  AND (
                      (
                          event.queue_id = marker.queue_id
                          AND event.event_id = marker.event_id
                      )
                      OR (
                          marker.event_type = 'message'
                          AND event.event_type = 'message'
                          AND event.causal_lane = marker.causal_lane
                          AND event.body->'message'->'display_recipient'
                              IS NOT DISTINCT FROM
                              marker.body->'message'->'display_recipient'
                          AND (
                              marker.body->'message'->>'type'
                                  IS DISTINCT FROM 'stream'
                              OR event.body->'message'->>'subject'
                                  IS NOT DISTINCT FROM
                                  marker.body->'message'->>'subject'
                          )
                      )
                  )
                RETURNING event.assignment_catalog_reported_at
                """,
                (account_uuid, queue_id, event_id),
            ).fetchall()
            return bool(marker)

    def ensure_catalog_deletion(
        self,
        report: dict[str, object],
        account_uuid: str,
        provider_chat_key: str,
        *,
        provider_event_marker: tuple[str, str, int] | None = None,
    ) -> bool:
        """Atomically retain a delete report, retire topology, and wake its lane."""
        with self.session() as session:
            durable = self._enqueue_observed_report_in_session(
                session,
                report,
                confirm_existing=True,
            )
            if not durable:
                return False
            if provider_event_marker is not None:
                marker = session.execute(
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
                    provider_event_marker,
                ).fetchone()
                if marker is None:
                    return False
            self._delete_catalog_topology_in_session(
                session, account_uuid, provider_chat_key
            )
            return True

    def pending_observed_reports(self, limit: int = 500) -> list[dict[str, object]]:
        with self.session() as session:
            session.execute(
                """
                WITH ranked AS (
                    SELECT report_uuid,
                           row_number() OVER (
                               PARTITION BY body->>'resource_type',
                                            (body->>'resource_uuid')::uuid
                               ORDER BY
                                   (body->>'observed_generation')::bigint DESC,
                                   COALESCE(
                                       workspace_bridge_observed_at(
                                           body->>'observed_at'
                                       ),
                                       created_at
                                   ) DESC,
                                   created_at DESC,
                                   report_uuid DESC
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
                WITH dependency_heads AS MATERIALIZED (
                    SELECT journal.account_uuid, event.body
                    FROM scheduler_accounts AS journal
                    JOIN LATERAL (
                        SELECT body
                        FROM zulip_provider_events
                        WHERE account_uuid = journal.account_uuid
                          AND processing_state = 'pending'
                        ORDER BY created_at, event_id, queue_id
                        LIMIT 1
                    ) AS event ON true
                    WHERE event.body->>'type' IN ('message', 'user_topic')
                ), dependency_chats AS (
                    SELECT account_uuid::text AS account_uuid,
                           CASE
                               WHEN body->>'type' = 'user_topic'
                                    AND body->>'stream_id' IS NOT NULL
                               THEN 'channel:' || (body->>'stream_id')
                               WHEN body->'message'->>'stream_id' IS NOT NULL
                               THEN 'channel:' ||
                                    (body->'message'->>'stream_id')
                               WHEN jsonb_typeof(
                                        body->'message'->'display_recipient'
                                    ) = 'array'
                               THEN CASE
                                        WHEN jsonb_array_length(
                                                 body->'message'
                                                     ->'display_recipient'
                                             ) = 2
                                        THEN 'direct:'
                                        ELSE 'group_direct:'
                                    END || COALESCE(
                                        (
                                            SELECT string_agg(
                                                participant->>'id',
                                                ',' ORDER BY
                                                (participant->>'id')::bigint
                                            )
                                            FROM jsonb_array_elements(
                                                body->'message'
                                                    ->'display_recipient'
                                            ) AS participant
                                            WHERE participant->>'id' ~ '^[0-9]+$'
                                        ),
                                        ''
                                    )
                           END AS provider_chat_key
                    FROM dependency_heads
                ), pending AS (
                    SELECT report.body, report.created_at, report.report_uuid,
                           report.body->>'resource_type' =
                               'external_chat_catalog'
                           AND report.body->>'status' = 'ready'
                           AND COALESCE(
                               report.body->'catalog'->>'operation',
                               'upsert'
                           ) = 'upsert' AS chat_materialization,
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
                                   (
                                       report.body->>'resource_type' =
                                           'external_chat_catalog'
                                       AND report.body->>'status' = 'ready'
                                       AND COALESCE(
                                           report.body->'catalog'->>'operation',
                                           'upsert'
                                       ) = 'upsert'
                                   ) DESC,
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
                         chat_materialization DESC, live_dependency DESC,
                         account_position,
                         created_at, report_uuid
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
            return [typing.cast(dict[str, object], row["body"]) for row in rows]

    def has_pending_chat_materializations(self) -> bool:
        """Return whether any incomplete catalog upsert is waiting."""
        with self.session() as session:
            row = session.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM observed_report_outbox AS report
                    WHERE report.completed_at IS NULL
                      AND report.body->>'resource_type' =
                          'external_chat_catalog'
                      AND report.body->>'status' = 'ready'
                      AND COALESCE(
                          report.body->'catalog'->>'operation',
                          'upsert'
                      ) = 'upsert'
                ) AS pending
                """
            ).fetchone()
            return bool(row and row["pending"])

    @staticmethod
    def _clear_unapplied_catalog_report_markers(
        session: sessions.PgSQLSession,
        report_uuid: str,
    ) -> None:
        """Allow dependency events to republish after an unapplied result."""
        session.execute(
            """
            WITH unapplied_catalog AS (
                SELECT body->'catalog'->>'external_account_uuid'
                           AS account_uuid,
                       body->'catalog'->'source'->>'provider_chat_key'
                           AS provider_chat_key
                FROM observed_report_outbox
                WHERE report_uuid = %s
                  AND body->>'resource_type' = 'external_chat_catalog'
            ), event_messages AS (
                SELECT event.account_uuid, event.queue_id, event.event_id,
                       unapplied.provider_chat_key,
                       CASE
                           WHEN event.body->>'type' = 'message'
                           THEN event.body->'message'
                           WHEN event.body->>'type' = 'reaction'
                           THEN event.provider_message_context
                           WHEN event.body->>'type' = 'user_topic'
                           THEN jsonb_build_object(
                               'stream_id', event.body->'stream_id'
                           )
                       END AS message
                FROM zulip_provider_events AS event
                JOIN unapplied_catalog AS unapplied
                  ON unapplied.account_uuid = event.account_uuid::text
                WHERE event.processing_state = 'pending'
                  AND event.assignment_catalog_reported_at IS NOT NULL
            ), matching_events AS (
                SELECT account_uuid, queue_id, event_id
                FROM event_messages
                WHERE CASE
                          WHEN message->>'stream_id' IS NOT NULL
                          THEN 'channel:' || (message->>'stream_id')
                          WHEN jsonb_typeof(message->'display_recipient') = 'array'
                          THEN CASE
                                   WHEN jsonb_array_length(
                                            message->'display_recipient'
                                        ) = 2
                                   THEN 'direct:'
                                   ELSE 'group_direct:'
                               END || COALESCE(
                                   (
                                       SELECT string_agg(
                                           participant->>'id',
                                           ',' ORDER BY
                                           (participant->>'id')::bigint
                                       )
                                       FROM jsonb_array_elements(
                                           message->'display_recipient'
                                       ) AS participant
                                       WHERE participant->>'id' ~ '^[0-9]+$'
                                   ),
                                   ''
                               )
                      END = provider_chat_key
            )
            UPDATE zulip_provider_events AS event
            SET assignment_catalog_reported_at = NULL
            FROM matching_events AS matching
            WHERE event.account_uuid = matching.account_uuid
              AND event.queue_id = matching.queue_id
              AND event.event_id = matching.event_id
            """,
            (report_uuid,),
        )

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
                    if status in {"rejected", "stale"}:
                        self._clear_unapplied_catalog_report_markers(
                            session,
                            report_uuid,
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
