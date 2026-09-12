# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from typing import Any
from uuid import UUID

import asyncpg

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.event_store import EventStore
from workspace_zulip_bridge.message_history import build_message_page
from workspace_zulip_bridge.message_history import message_state_hash
from workspace_zulip_bridge.models import ZulipMessage
from workspace_zulip_bridge.stable_ids import stable_chat_uuid

LOG = logging.getLogger(__name__)

_MESSAGE_EVENT_TYPES = frozenset(
    {
        "message",
        "reaction",
        "update_message",
        "update_message_flags",
        "delete_message",
    }
)
_FLAG_FIELDS = {
    "read": "is_read",
    "starred": "is_starred",
    "collapsed": "is_collapsed",
    "mentioned": "is_mentioned",
    "stream_wildcard_mentioned": "is_stream_wildcard_mentioned",
    "topic_wildcard_mentioned": "is_topic_wildcard_mentioned",
    "has_alert_word": "has_alert_word",
    "historical": "is_historical",
    "wildcard_mentioned": "is_stream_wildcard_mentioned",
}


@dataclass(frozen=True, slots=True)
class EventBatchStats:
    claimed: int = 0
    applied: int = 0
    skipped: int = 0
    failed: int = 0
    messages_changed: int = 0
    messages_deleted: int = 0
    chats_changed: int = 0
    elapsed_seconds: float = 0.0

    @property
    def events_per_second(self) -> float:
        if not self.elapsed_seconds:
            return 0.0
        return self.claimed / self.elapsed_seconds


@dataclass(frozen=True, slots=True)
class _ClaimedEvent:
    uuid: UUID
    user_uuid: UUID
    endpoint: str
    queue_id: str
    own_user_id: int | None
    event_type: str
    payload: Mapping[str, Any]
    active_queue: bool


@dataclass(frozen=True, slots=True)
class _MessageRoute:
    chat_key: str
    supplier_user_uuid: UUID | None


@dataclass(frozen=True, slots=True)
class _RoutedEvent:
    event: _ClaimedEvent
    message_ids: tuple[int, ...] = ()
    chat_key: str | None = None
    skip_reason: str | None = None


@dataclass(frozen=True, slots=True)
class _MessageSnapshot:
    message_id: int
    chat_key: str
    topic_name: str | None
    sender_user_uuid: UUID
    content: str
    is_read: bool
    is_starred: bool
    is_collapsed: bool
    is_mentioned: bool
    is_stream_wildcard_mentioned: bool
    is_topic_wildcard_mentioned: bool
    has_alert_word: bool
    is_historical: bool
    reactions: tuple[Mapping[str, str], ...]
    sent_at: int


@dataclass(frozen=True, slots=True)
class _Outcome:
    event_uuid: UUID
    status: str
    reason: str
    messages_changed: int = 0
    messages_deleted: int = 0
    chats_changed: int = 0


class ZulipEventProcessor:
    def __init__(
        self,
        pool: asyncpg.Pool,
        store: EventStore,
        settings: Settings,
    ) -> None:
        self._pool = pool
        self._store = store
        self._settings = settings
        self._next_cleanup_at = 0.0

    async def run(self) -> None:
        while True:
            deleted = await self._maybe_cleanup_expired_events()
            stats = await self.process_once()
            if stats.claimed:
                LOG.info(
                    "Zulip event batch processed claimed=%s applied=%s skipped=%s "
                    "failed=%s message_changes=%s message_deletes=%s "
                    "chat_changes=%s elapsed_seconds=%.6f events_per_second=%.3f",
                    stats.claimed,
                    stats.applied,
                    stats.skipped,
                    stats.failed,
                    stats.messages_changed,
                    stats.messages_deleted,
                    stats.chats_changed,
                    stats.elapsed_seconds,
                    stats.events_per_second,
                )
                continue
            if deleted == self._settings.event_cleanup_batch_size:
                continue
            await asyncio.sleep(self._settings.event_processor_poll_seconds)

    async def cleanup_expired_events(self) -> int:
        """Delete one bounded batch of terminal events past their retention."""
        async with self._pool.acquire() as connection, connection.transaction():
            deleted = await connection.fetchval(
                """
                WITH expired AS MATERIALIZED (
                    SELECT event.uuid
                    FROM workspace_zulip_bridge.zulip_events AS event
                    WHERE event.processing_status IN ('applied', 'skipped', 'failed')
                      AND event.created_at < (
                          clock_timestamp()
                          - make_interval(secs => $1::double precision)
                      )
                    ORDER BY event.created_at, event.uuid
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                ),
                deleted AS (
                    DELETE FROM workspace_zulip_bridge.zulip_events AS event
                    USING expired
                    WHERE event.uuid = expired.uuid
                    RETURNING 1
                )
                SELECT count(*)::bigint
                FROM deleted
                """,
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
                "Expired terminal Zulip events deleted count=%s retention_seconds=%.3f",
                deleted,
                self._settings.event_retention_seconds,
            )
        return deleted

    async def process_once(self) -> EventBatchStats:
        started_at = time.monotonic()
        events = await self._claim_events()
        if not events:
            return EventBatchStats(elapsed_seconds=time.monotonic() - started_at)

        chat_suppliers, message_routes = await self._load_routes(events)
        routed = [
            self._route_event(event, chat_suppliers, message_routes) for event in events
        ]
        outcomes: list[_Outcome] = []
        pending_mutation_batch: list[_RoutedEvent] = []
        pending_batch_kind: str | None = None

        async def flush_mutation_batch() -> None:
            nonlocal pending_batch_kind, pending_mutation_batch
            if not pending_mutation_batch or pending_batch_kind is None:
                return
            try:
                if pending_batch_kind == "flags":
                    batch_outcomes = await self._apply_flags_batch(
                        pending_mutation_batch
                    )
                elif pending_batch_kind == "reactions":
                    batch_outcomes = await self._apply_reactions_batch(
                        pending_mutation_batch
                    )
                else:
                    batch_outcomes = await self._apply_messages_batch(
                        pending_mutation_batch
                    )
                outcomes.extend(batch_outcomes)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.exception(
                    "Zulip event mutation batch failed kind=%s events=%s",
                    pending_batch_kind,
                    len(pending_mutation_batch),
                )
                outcomes.extend(
                    _Outcome(
                        item.event.uuid,
                        "failed",
                        f"handler_error:{type(exc).__name__}",
                    )
                    for item in pending_mutation_batch
                )
            pending_mutation_batch = []
            pending_batch_kind = None

        for item in routed:
            if item.skip_reason is not None:
                outcomes.append(_Outcome(item.event.uuid, "skipped", item.skip_reason))
                continue
            batch_kind = _mutation_batch_kind(item)
            if batch_kind is not None:
                if pending_mutation_batch and (
                    pending_batch_kind != batch_kind
                    or pending_mutation_batch[0].event.user_uuid != item.event.user_uuid
                    or pending_mutation_batch[0].event.queue_id != item.event.queue_id
                ):
                    await flush_mutation_batch()
                pending_batch_kind = batch_kind
                pending_mutation_batch.append(item)
                continue
            await flush_mutation_batch()
            try:
                outcomes.append(await self._apply_event(item))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.exception(
                    "Zulip event failed event_uuid=%s event_type=%s",
                    item.event.uuid,
                    item.event.event_type,
                )
                outcomes.append(
                    _Outcome(
                        item.event.uuid,
                        "failed",
                        f"handler_error:{type(exc).__name__}",
                    )
                )
        await flush_mutation_batch()
        await self._finish_events(outcomes)
        elapsed = time.monotonic() - started_at
        return EventBatchStats(
            claimed=len(events),
            applied=sum(outcome.status == "applied" for outcome in outcomes),
            skipped=sum(outcome.status == "skipped" for outcome in outcomes),
            failed=sum(outcome.status == "failed" for outcome in outcomes),
            messages_changed=sum(outcome.messages_changed for outcome in outcomes),
            messages_deleted=sum(outcome.messages_deleted for outcome in outcomes),
            chats_changed=sum(outcome.chats_changed for outcome in outcomes),
            elapsed_seconds=elapsed,
        )

    async def _claim_events(self) -> list[_ClaimedEvent]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_events
                SET processing_status = 'pending',
                    claimed_at = NULL,
                    outcome_reason = 'claim_expired'
                WHERE processing_status = 'processing'
                  AND claimed_at < (
                      clock_timestamp()
                      - make_interval(secs => $1::double precision)
                  )
                """,
                self._settings.event_processor_claim_timeout_seconds,
            )
            rows = await connection.fetch(
                """
                WITH candidates AS MATERIALIZED (
                    SELECT event.uuid
                    FROM workspace_zulip_bridge.zulip_events AS event
                    WHERE event.processing_status = 'pending'
                      AND event.available_at <= clock_timestamp()
                    ORDER BY event.created_at,
                             event.zulip_user_uuid,
                             event.queue_id,
                             event.event_id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                ),
                claimed AS (
                    UPDATE workspace_zulip_bridge.zulip_events AS event
                    SET processing_status = 'processing',
                        attempt_count = event.attempt_count + 1,
                        claimed_at = clock_timestamp(),
                        processed_at = NULL,
                        outcome_reason = NULL
                    FROM candidates
                    WHERE event.uuid = candidates.uuid
                    RETURNING event.*
                )
                SELECT claimed.uuid,
                       claimed.zulip_user_uuid,
                       zulip_user.endpoint,
                       claimed.queue_id,
                       zulip_user.zulip_user_id,
                       claimed.event_type,
                       claimed.payload::text AS payload_json,
                       (
                           zulip_user.queue_id = claimed.queue_id
                           AND NOT zulip_user.disabled
                           AND zulip_user.api_key IS NOT NULL
                       ) AS active_queue
                FROM claimed
                JOIN workspace_zulip_bridge.zulip_users AS zulip_user
                  ON zulip_user.uuid = claimed.zulip_user_uuid
                ORDER BY claimed.created_at,
                         claimed.zulip_user_uuid,
                         claimed.queue_id,
                         claimed.event_id
                """,
                self._settings.event_processor_batch_size,
            )
        events: list[_ClaimedEvent] = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            if not isinstance(payload, Mapping):
                payload = {}
            events.append(
                _ClaimedEvent(
                    uuid=row["uuid"],
                    user_uuid=row["zulip_user_uuid"],
                    endpoint=row["endpoint"],
                    queue_id=row["queue_id"],
                    own_user_id=row["zulip_user_id"],
                    event_type=row["event_type"],
                    payload=payload,
                    active_queue=row["active_queue"],
                )
            )
        return events

    async def _load_routes(
        self,
        events: list[_ClaimedEvent],
    ) -> tuple[
        dict[tuple[str, str], UUID | None],
        dict[tuple[str, int], _MessageRoute],
    ]:
        chat_uuids: set[UUID] = set()
        message_ids_by_endpoint: dict[str, set[int]] = {}
        for event in events:
            if event.own_user_id is not None:
                chat_key = _event_chat_key(event.payload, event.own_user_id)
                if chat_key is not None:
                    chat_uuids.add(stable_chat_uuid(event.endpoint, chat_key))
            destination_stream_id = event.payload.get("new_stream_id")
            if isinstance(destination_stream_id, int):
                chat_uuids.add(
                    stable_chat_uuid(
                        event.endpoint,
                        f"channel:{destination_stream_id}",
                    )
                )
            ids = _event_message_ids(event.payload, event.event_type)
            if ids:
                message_ids_by_endpoint.setdefault(event.endpoint, set()).update(ids)

        chat_suppliers: dict[tuple[str, str], UUID | None] = {}
        message_routes: dict[tuple[str, int], _MessageRoute] = {}
        async with self._pool.acquire() as connection:
            if chat_uuids:
                rows = await connection.fetch(
                    """
                    SELECT endpoint, chat_key, supplier_user_uuid
                    FROM workspace_zulip_bridge.zulip_chats
                    WHERE uuid = ANY($1::uuid[])
                    """,
                    list(chat_uuids),
                )
                chat_suppliers = {
                    (row["endpoint"], row["chat_key"]): row["supplier_user_uuid"]
                    for row in rows
                }
            for endpoint, message_ids in message_ids_by_endpoint.items():
                rows = await connection.fetch(
                    """
                    SELECT message.zulip_message_id,
                           chat.chat_key,
                           chat.supplier_user_uuid
                    FROM workspace_zulip_bridge.zulip_messages AS message
                    JOIN workspace_zulip_bridge.zulip_chats AS chat
                      ON chat.uuid = message.zulip_chat_uuid
                    WHERE chat.endpoint = $1
                      AND message.zulip_message_id = ANY($2::bigint[])
                    """,
                    endpoint,
                    list(message_ids),
                )
                for row in rows:
                    message_routes[(endpoint, row["zulip_message_id"])] = _MessageRoute(
                        chat_key=row["chat_key"],
                        supplier_user_uuid=row["supplier_user_uuid"],
                    )
        # A message and its first reaction or flag update may be in the same
        # claimed batch before that message exists in PostgreSQL. Seed routes
        # from complete message events so later deltas still pass through the
        # canonical chat-supplier gate.
        for event in events:
            if event.event_type != "message" or event.own_user_id is None:
                continue
            chat_key = _event_chat_key(event.payload, event.own_user_id)
            announced_message_ids = _event_message_ids(event.payload, event.event_type)
            if chat_key is None or not announced_message_ids:
                continue
            route = _MessageRoute(
                chat_key=chat_key,
                supplier_user_uuid=chat_suppliers.get((event.endpoint, chat_key)),
            )
            for message_id in announced_message_ids:
                message_routes.setdefault((event.endpoint, message_id), route)
        return chat_suppliers, message_routes

    def _route_event(
        self,
        event: _ClaimedEvent,
        chat_suppliers: Mapping[tuple[str, str], UUID | None],
        message_routes: Mapping[tuple[str, int], _MessageRoute],
    ) -> _RoutedEvent:
        if not event.active_queue:
            return _RoutedEvent(event, skip_reason="stale_queue")
        if event.event_type not in _MESSAGE_EVENT_TYPES | {"stream"}:
            return _RoutedEvent(event, skip_reason="unsupported_event_type")
        if event.event_type == "stream":
            if event.payload.get("op") != "update":
                return _RoutedEvent(event, skip_reason="unsupported_stream_operation")
            chat_key = _event_chat_key(event.payload, event.own_user_id)
            if chat_key is None:
                return _RoutedEvent(event, skip_reason="invalid_chat_target")
            supplier = chat_suppliers.get((event.endpoint, chat_key))
            if supplier is None:
                return _RoutedEvent(
                    event, chat_key=chat_key, skip_reason="chat_unassigned"
                )
            if supplier != event.user_uuid:
                return _RoutedEvent(
                    event,
                    chat_key=chat_key,
                    skip_reason="not_chat_supplier",
                )
            return _RoutedEvent(event, chat_key=chat_key)
        if event.event_type == "message":
            chat_key = _event_chat_key(event.payload, event.own_user_id)
            if chat_key is None:
                return _RoutedEvent(event, skip_reason="invalid_chat_target")
            supplier = chat_suppliers.get((event.endpoint, chat_key))
            if supplier is None:
                return _RoutedEvent(
                    event, chat_key=chat_key, skip_reason="chat_unassigned"
                )
            if supplier != event.user_uuid:
                return _RoutedEvent(
                    event,
                    chat_key=chat_key,
                    skip_reason="not_chat_supplier",
                )
            return _RoutedEvent(event, chat_key=chat_key)

        message_ids = _event_message_ids(event.payload, event.event_type)
        if event.event_type == "update_message_flags" and event.payload.get("all"):
            return _RoutedEvent(event)
        if not message_ids:
            return _RoutedEvent(event, skip_reason="missing_message_target")

        destination_stream_id = event.payload.get("new_stream_id")
        accepted: list[int] = []
        for message_id in message_ids:
            route = message_routes.get((event.endpoint, message_id))
            if isinstance(destination_stream_id, int):
                destination_key = f"channel:{destination_stream_id}"
                supplier = chat_suppliers.get((event.endpoint, destination_key))
            elif route is not None:
                supplier = route.supplier_user_uuid
            else:
                supplier = None
            if supplier == event.user_uuid:
                accepted.append(message_id)
        if not accepted:
            reason = (
                "message_not_materialized"
                if not any(
                    (event.endpoint, message_id) in message_routes
                    for message_id in message_ids
                )
                else "not_chat_supplier"
            )
            return _RoutedEvent(event, skip_reason=reason)
        return _RoutedEvent(event, message_ids=tuple(accepted))

    async def _apply_event(self, item: _RoutedEvent) -> _Outcome:
        event_type = item.event.event_type
        if event_type == "message":
            return await self._apply_new_message(item)
        if event_type == "reaction":
            return await self._apply_reaction(item)
        if event_type == "update_message_flags":
            return await self._apply_flags(item)
        if event_type == "update_message":
            return await self._apply_message_update(item)
        if event_type == "delete_message":
            return await self._apply_delete(item)
        if event_type == "stream":
            return await self._apply_stream_update(item)
        return _Outcome(item.event.uuid, "skipped", "unsupported_event_type")

    async def _apply_new_message(self, item: _RoutedEvent) -> _Outcome:
        raw_message = item.event.payload.get("message")
        if not isinstance(raw_message, Mapping) or item.event.own_user_id is None:
            return _Outcome(item.event.uuid, "failed", "invalid_message_event")
        user_uuids = await self._load_user_uuids(item.event.endpoint)
        built = build_message_page(
            [raw_message],
            own_user_id=item.event.own_user_id,
            user_uuids=user_uuids,
            stream_ids_by_name={},
            allowed_chat_keys={item.chat_key} if item.chat_key is not None else set(),
        )
        if not built.messages:
            return _Outcome(item.event.uuid, "skipped", "message_filtered")
        result = await self._store.apply_live_messages(
            item.event.user_uuid,
            item.event.queue_id,
            (),
            built.messages,
            (),
        )
        return _Outcome(
            item.event.uuid,
            "applied",
            "message",
            messages_changed=result.messages_changed,
        )

    async def _apply_messages_batch(
        self,
        items: list[_RoutedEvent],
    ) -> list[_Outcome]:
        first = items[0]
        if first.event.own_user_id is None:
            return [
                _Outcome(item.event.uuid, "failed", "invalid_message_event")
                for item in items
            ]
        user_uuids = await self._load_user_uuids(first.event.endpoint)
        messages: list[ZulipMessage] = []
        outcomes: list[_Outcome] = []
        for item in items:
            raw_message = item.event.payload.get("message")
            if not isinstance(raw_message, Mapping) or item.chat_key is None:
                outcomes.append(
                    _Outcome(item.event.uuid, "failed", "invalid_message_event")
                )
                continue
            built = build_message_page(
                [raw_message],
                own_user_id=first.event.own_user_id,
                user_uuids=user_uuids,
                stream_ids_by_name={},
                allowed_chat_keys={item.chat_key},
            )
            if not built.messages:
                outcomes.append(
                    _Outcome(item.event.uuid, "skipped", "message_filtered")
                )
                continue
            messages.extend(built.messages)
            outcomes.append(_Outcome(item.event.uuid, "applied", "message"))
        if not messages:
            return outcomes
        result = await self._store.apply_live_messages(
            first.event.user_uuid,
            first.event.queue_id,
            (),
            messages,
            (),
        )
        if result.messages_changed == 0 and result.messages_unchanged == 0:
            return [
                _Outcome(outcome.event_uuid, "skipped", "not_chat_supplier")
                if outcome.status == "applied"
                else outcome
                for outcome in outcomes
            ]
        for index, outcome in enumerate(outcomes):
            if outcome.status == "applied":
                outcomes[index] = replace(
                    outcome,
                    messages_changed=result.messages_changed,
                )
                break
        return outcomes

    async def _apply_reaction(self, item: _RoutedEvent) -> _Outcome:
        change = _reaction_change(item.event.payload)
        if change is None:
            return _Outcome(item.event.uuid, "failed", "invalid_reaction_event")
        reaction_user_id, emoji_name, emoji_code, reaction_type, operation = change
        user_uuids = await self._load_user_uuids(item.event.endpoint)
        reaction_user_uuid = user_uuids.get(reaction_user_id)
        if reaction_user_uuid is None:
            return _Outcome(item.event.uuid, "skipped", "reaction_user_filtered")
        snapshots = await self._load_message_snapshots(
            item.event.endpoint,
            item.message_ids,
        )
        messages: list[ZulipMessage] = []
        for snapshot in snapshots:
            reactions = [dict(reaction) for reaction in snapshot.reactions]
            identity = (str(reaction_user_uuid), reaction_type, emoji_code)
            reactions = [
                reaction
                for reaction in reactions
                if (
                    reaction["user_uuid"],
                    reaction["reaction_type"],
                    reaction["emoji_code"],
                )
                != identity
            ]
            if operation == "add":
                reactions.append(
                    {
                        "user_uuid": str(reaction_user_uuid),
                        "emoji_name": emoji_name,
                        "emoji_code": emoji_code,
                        "reaction_type": reaction_type,
                    }
                )
            reactions.sort(key=_reaction_sort_key)
            messages.append(_snapshot_message(snapshot, reactions=tuple(reactions)))
        return await self._store_messages(item, messages, "reaction")

    async def _apply_reactions_batch(
        self,
        items: list[_RoutedEvent],
    ) -> list[_Outcome]:
        first = items[0]
        message_ids = tuple(
            sorted({message_id for item in items for message_id in item.message_ids})
        )
        snapshots = {
            snapshot.message_id: snapshot
            for snapshot in await self._load_message_snapshots(
                first.event.endpoint,
                message_ids,
            )
        }
        user_uuids = await self._load_user_uuids(first.event.endpoint)
        touched: set[int] = set()
        outcomes: list[_Outcome] = []
        for item in items:
            change = _reaction_change(item.event.payload)
            if change is None:
                outcomes.append(
                    _Outcome(item.event.uuid, "failed", "invalid_reaction_event")
                )
                continue
            reaction_user_id, emoji_name, emoji_code, reaction_type, operation = change
            reaction_user_uuid = user_uuids.get(reaction_user_id)
            if reaction_user_uuid is None:
                outcomes.append(
                    _Outcome(item.event.uuid, "skipped", "reaction_user_filtered")
                )
                continue
            materialized = False
            for message_id in item.message_ids:
                snapshot = snapshots.get(message_id)
                if snapshot is None:
                    continue
                reactions = [dict(reaction) for reaction in snapshot.reactions]
                identity = (str(reaction_user_uuid), reaction_type, emoji_code)
                reactions = [
                    reaction
                    for reaction in reactions
                    if (
                        reaction["user_uuid"],
                        reaction["reaction_type"],
                        reaction["emoji_code"],
                    )
                    != identity
                ]
                if operation == "add":
                    reactions.append(
                        {
                            "user_uuid": str(reaction_user_uuid),
                            "emoji_name": emoji_name,
                            "emoji_code": emoji_code,
                            "reaction_type": reaction_type,
                        }
                    )
                reactions.sort(key=_reaction_sort_key)
                snapshots[message_id] = replace(
                    snapshot,
                    reactions=tuple(reactions),
                )
                touched.add(message_id)
                materialized = True
            outcomes.append(
                _Outcome(
                    item.event.uuid,
                    "applied" if materialized else "skipped",
                    "reaction" if materialized else "message_not_materialized",
                )
            )
        if not touched:
            return outcomes
        messages = [
            _snapshot_message(snapshots[message_id]) for message_id in sorted(touched)
        ]
        result = await self._store.apply_live_messages(
            first.event.user_uuid,
            first.event.queue_id,
            (),
            messages,
            (),
        )
        if result.messages_changed == 0 and result.messages_unchanged == 0:
            return [
                _Outcome(outcome.event_uuid, "skipped", "not_chat_supplier")
                if outcome.status == "applied"
                else outcome
                for outcome in outcomes
            ]
        for index, outcome in enumerate(outcomes):
            if outcome.status == "applied":
                outcomes[index] = replace(
                    outcome,
                    messages_changed=result.messages_changed,
                )
                break
        return outcomes

    async def _apply_flags(self, item: _RoutedEvent) -> _Outcome:
        payload = item.event.payload
        change = _flag_change(payload)
        if change is None:
            return _Outcome(item.event.uuid, "skipped", "unsupported_flag")
        field, value = change
        flag = payload.get("flag")
        operation = payload.get("op", payload.get("operation"))
        message_ids = item.message_ids
        if payload.get("all"):
            if flag != "read" or operation != "add":
                return _Outcome(item.event.uuid, "skipped", "unsupported_bulk_flag")
            message_ids = await self._load_supplier_message_ids(item.event.user_uuid)
        changed = 0
        for offset in range(
            0, len(message_ids), self._settings.zulip_message_page_size
        ):
            snapshots = await self._load_message_snapshots(
                item.event.endpoint,
                message_ids[offset : offset + self._settings.zulip_message_page_size],
            )
            messages = [
                _snapshot_message(_snapshot_with_flag(snapshot, field, value))
                for snapshot in snapshots
            ]
            outcome = await self._store_messages(item, messages, "message_flags")
            changed += outcome.messages_changed
        return _Outcome(
            item.event.uuid,
            "applied",
            "message_flags",
            messages_changed=changed,
        )

    async def _apply_flags_batch(
        self,
        items: list[_RoutedEvent],
    ) -> list[_Outcome]:
        first = items[0]
        message_ids = tuple(
            sorted({message_id for item in items for message_id in item.message_ids})
        )
        snapshots = {
            snapshot.message_id: snapshot
            for snapshot in await self._load_message_snapshots(
                first.event.endpoint,
                message_ids,
            )
        }
        touched: set[int] = set()
        outcomes: list[_Outcome] = []
        for item in items:
            change = _batchable_flag_change(item.event.payload)
            if change is None:
                outcomes.append(
                    _Outcome(item.event.uuid, "skipped", "unsupported_flag")
                )
                continue
            field, value = change
            materialized = False
            for message_id in item.message_ids:
                snapshot = snapshots.get(message_id)
                if snapshot is None:
                    continue
                snapshots[message_id] = _snapshot_with_flag(snapshot, field, value)
                touched.add(message_id)
                materialized = True
            outcomes.append(
                _Outcome(
                    item.event.uuid,
                    "applied" if materialized else "skipped",
                    "message_flags" if materialized else "message_not_materialized",
                )
            )
        if not touched:
            return outcomes
        messages = [
            _snapshot_message(snapshots[message_id]) for message_id in sorted(touched)
        ]
        result = await self._store.apply_live_messages(
            first.event.user_uuid,
            first.event.queue_id,
            (),
            messages,
            (),
        )
        if result.messages_changed == 0 and result.messages_unchanged == 0:
            return [
                _Outcome(outcome.event_uuid, "skipped", "not_chat_supplier")
                if outcome.status == "applied"
                else outcome
                for outcome in outcomes
            ]
        for index, outcome in enumerate(outcomes):
            if outcome.status == "applied":
                outcomes[index] = replace(
                    outcome,
                    messages_changed=result.messages_changed,
                )
                break
        return outcomes

    async def _apply_message_update(self, item: _RoutedEvent) -> _Outcome:
        payload = item.event.payload
        primary_id = payload.get("message_id")
        snapshots = await self._load_message_snapshots(
            item.event.endpoint,
            item.message_ids,
        )
        messages: list[ZulipMessage] = []
        for snapshot in snapshots:
            changes: dict[str, object] = {}
            new_stream_id = payload.get("new_stream_id")
            if isinstance(new_stream_id, int):
                changes["chat_key"] = f"channel:{new_stream_id}"
            subject = payload.get("subject")
            if isinstance(subject, str) and snapshot.topic_name is not None:
                changes["topic_name"] = subject
            if snapshot.message_id == primary_id:
                content = payload.get("content")
                if isinstance(content, str):
                    changes["content"] = content
                raw_flags = payload.get("flags")
                if isinstance(raw_flags, list) and all(
                    isinstance(flag, str) for flag in raw_flags
                ):
                    flags = set(raw_flags)
                    changes.update(
                        {
                            "is_read": "read" in flags,
                            "is_starred": "starred" in flags,
                            "is_collapsed": "collapsed" in flags,
                            "is_mentioned": "mentioned" in flags,
                            "is_stream_wildcard_mentioned": (
                                "stream_wildcard_mentioned" in flags
                                or "wildcard_mentioned" in flags
                            ),
                            "is_topic_wildcard_mentioned": (
                                "topic_wildcard_mentioned" in flags
                            ),
                            "has_alert_word": "has_alert_word" in flags,
                            "is_historical": "historical" in flags,
                        }
                    )
            messages.append(_snapshot_message(snapshot, **changes))
        return await self._store_messages(item, messages, "message_update")

    async def _apply_delete(self, item: _RoutedEvent) -> _Outcome:
        result = await self._store.apply_live_messages(
            item.event.user_uuid,
            item.event.queue_id,
            (),
            (),
            item.message_ids,
        )
        await self._prune_empty_topics(item.event.user_uuid)
        return _Outcome(
            item.event.uuid,
            "applied",
            "message_delete",
            messages_deleted=result.messages_deleted,
        )

    async def _apply_stream_update(self, item: _RoutedEvent) -> _Outcome:
        if item.chat_key is None:
            return _Outcome(item.event.uuid, "failed", "invalid_chat_target")
        payload = item.event.payload
        property_name = payload.get("property")
        if not isinstance(property_name, str):
            return _Outcome(item.event.uuid, "failed", "invalid_stream_event")
        chat_uuid = stable_chat_uuid(item.event.endpoint, item.chat_key)
        async with self._pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """
                SELECT name, chat_parameters::text AS parameters_json
                FROM workspace_zulip_bridge.zulip_chats
                WHERE uuid = $1
                  AND supplier_user_uuid = $2
                FOR UPDATE
                """,
                chat_uuid,
                item.event.user_uuid,
            )
            if row is None:
                return _Outcome(item.event.uuid, "skipped", "not_chat_supplier")
            name = row["name"]
            parameters = json.loads(row["parameters_json"])
            if not isinstance(parameters, dict):
                raise ValueError("invalid stored chat parameters")
            if property_name == "name":
                value = payload.get("value")
                if not isinstance(value, str):
                    return _Outcome(item.event.uuid, "failed", "invalid_stream_name")
                name = value
            else:
                parameters[property_name] = payload.get("value")
                if property_name == "description" and isinstance(
                    payload.get("rendered_description"), str
                ):
                    parameters["rendered_description"] = payload["rendered_description"]
            parameters_json = json.dumps(
                parameters,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            content_hash = _chat_content_hash(
                "channel",
                item.chat_key,
                name,
                parameters_json,
            )
            status = await connection.execute(
                """
                UPDATE workspace_zulip_bridge.zulip_chats
                SET name = $3,
                    chat_parameters = $4::jsonb,
                    content_hash = $5
                WHERE uuid = $1
                  AND supplier_user_uuid = $2
                  AND content_hash IS DISTINCT FROM $5
                """,
                chat_uuid,
                item.event.user_uuid,
                name,
                parameters_json,
                content_hash,
            )
        return _Outcome(
            item.event.uuid,
            "applied",
            "stream_update",
            chats_changed=int(status == "UPDATE 1"),
        )

    async def _store_messages(
        self,
        item: _RoutedEvent,
        messages: list[ZulipMessage],
        reason: str,
    ) -> _Outcome:
        if not messages:
            return _Outcome(item.event.uuid, "skipped", "message_not_materialized")
        result = await self._store.apply_live_messages(
            item.event.user_uuid,
            item.event.queue_id,
            (),
            messages,
            (),
        )
        if reason == "message_update":
            await self._prune_empty_topics(item.event.user_uuid)
        if result.messages_changed == 0 and result.messages_unchanged == 0:
            return _Outcome(item.event.uuid, "skipped", "not_chat_supplier")
        return _Outcome(
            item.event.uuid,
            "applied",
            reason,
            messages_changed=result.messages_changed,
        )

    async def _prune_empty_topics(self, user_uuid: UUID) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM workspace_zulip_bridge.zulip_topics AS topic
                WHERE topic.zulip_user_uuid = $1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM workspace_zulip_bridge.zulip_messages AS message
                      WHERE message.topic_uuid = topic.uuid
                  )
                """,
                user_uuid,
            )

    async def _load_user_uuids(self, endpoint: str) -> dict[int, UUID]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT zulip_user_id, uuid
                FROM workspace_zulip_bridge.zulip_users
                WHERE endpoint = $1
                  AND zulip_user_id IS NOT NULL
                """,
                endpoint,
            )
        return {row["zulip_user_id"]: row["uuid"] for row in rows}

    async def _load_message_snapshots(
        self,
        endpoint: str,
        message_ids: tuple[int, ...],
    ) -> list[_MessageSnapshot]:
        if not message_ids:
            return []
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT message.zulip_message_id,
                       chat.chat_key,
                       topic.name AS topic_name,
                       message.sender_user_uuid,
                       message.content,
                       message.is_read,
                       message.is_starred,
                       message.is_collapsed,
                       message.is_mentioned,
                       message.is_stream_wildcard_mentioned,
                       message.is_topic_wildcard_mentioned,
                       message.has_alert_word,
                       message.is_historical,
                       message.reactions::text AS reactions_json,
                       extract(epoch FROM message.created_at)::bigint AS sent_at
                FROM workspace_zulip_bridge.zulip_messages AS message
                JOIN workspace_zulip_bridge.zulip_chats AS chat
                  ON chat.uuid = message.zulip_chat_uuid
                LEFT JOIN workspace_zulip_bridge.zulip_topics AS topic
                  ON topic.uuid = message.topic_uuid
                WHERE chat.endpoint = $1
                  AND message.zulip_message_id = ANY($2::bigint[])
                ORDER BY message.zulip_message_id
                """,
                endpoint,
                list(message_ids),
            )
        snapshots: list[_MessageSnapshot] = []
        for row in rows:
            reactions = json.loads(row["reactions_json"])
            if not isinstance(reactions, list) or not all(
                isinstance(reaction, Mapping) for reaction in reactions
            ):
                raise ValueError("invalid stored reactions")
            snapshots.append(
                _MessageSnapshot(
                    message_id=row["zulip_message_id"],
                    chat_key=row["chat_key"],
                    topic_name=row["topic_name"],
                    sender_user_uuid=row["sender_user_uuid"],
                    content=row["content"],
                    is_read=row["is_read"],
                    is_starred=row["is_starred"],
                    is_collapsed=row["is_collapsed"],
                    is_mentioned=row["is_mentioned"],
                    is_stream_wildcard_mentioned=(row["is_stream_wildcard_mentioned"]),
                    is_topic_wildcard_mentioned=(row["is_topic_wildcard_mentioned"]),
                    has_alert_word=row["has_alert_word"],
                    is_historical=row["is_historical"],
                    reactions=tuple(reactions),
                    sent_at=row["sent_at"],
                )
            )
        return snapshots

    async def _load_supplier_message_ids(self, user_uuid: UUID) -> tuple[int, ...]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT message.zulip_message_id
                FROM workspace_zulip_bridge.zulip_messages AS message
                JOIN workspace_zulip_bridge.zulip_chats AS chat
                  ON chat.uuid = message.zulip_chat_uuid
                 AND chat.supplier_user_uuid = $1
                ORDER BY message.zulip_message_id
                """,
                user_uuid,
            )
        return tuple(row["zulip_message_id"] for row in rows)

    async def _finish_events(self, outcomes: list[_Outcome]) -> None:
        if not outcomes:
            return
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                WITH outcomes AS (
                    SELECT uuid, status, reason
                    FROM unnest($1::uuid[], $2::text[], $3::text[])
                        AS outcome(uuid, status, reason)
                )
                UPDATE workspace_zulip_bridge.zulip_events AS event
                SET processing_status = outcomes.status,
                    processed_at = clock_timestamp(),
                    outcome_reason = outcomes.reason
                FROM outcomes
                WHERE event.uuid = outcomes.uuid
                  AND event.processing_status = 'processing'
                """,
                [outcome.event_uuid for outcome in outcomes],
                [outcome.status for outcome in outcomes],
                [outcome.reason for outcome in outcomes],
            )


def _event_message_ids(payload: Mapping[str, Any], event_type: str) -> tuple[int, ...]:
    values: set[int] = set()
    message_id = payload.get("message_id")
    if isinstance(message_id, int):
        values.add(message_id)
    raw_ids = payload.get("message_ids")
    if isinstance(raw_ids, list):
        values.update(value for value in raw_ids if isinstance(value, int))
    if event_type == "update_message_flags":
        messages = payload.get("messages")
        if isinstance(messages, list):
            values.update(value for value in messages if isinstance(value, int))
    if event_type == "message":
        message = payload.get("message")
        if isinstance(message, Mapping):
            nested_id = message.get("id")
            if isinstance(nested_id, int):
                values.add(nested_id)
    return tuple(sorted(values))


def _flag_change(payload: Mapping[str, Any]) -> tuple[str, bool] | None:
    flag = payload.get("flag")
    operation = payload.get("op", payload.get("operation"))
    field = _FLAG_FIELDS.get(flag) if isinstance(flag, str) else None
    if field is None or operation not in {"add", "remove"}:
        return None
    return field, operation == "add"


def _batchable_flag_change(payload: Mapping[str, Any]) -> tuple[str, bool] | None:
    if payload.get("all"):
        return None
    return _flag_change(payload)


def _reaction_change(
    payload: Mapping[str, Any],
) -> tuple[int, str, str, str, str] | None:
    reaction_user_id = payload.get("user_id")
    emoji_name = payload.get("emoji_name")
    emoji_code = payload.get("emoji_code")
    reaction_type = payload.get("reaction_type")
    operation = payload.get("op")
    if (
        not isinstance(reaction_user_id, int)
        or not isinstance(emoji_name, str)
        or not isinstance(emoji_code, str)
        or not isinstance(reaction_type, str)
        or operation not in {"add", "remove"}
    ):
        return None
    return reaction_user_id, emoji_name, emoji_code, reaction_type, operation


def _mutation_batch_kind(item: _RoutedEvent) -> str | None:
    if item.event.event_type == "message":
        if _batchable_message_event(item.event) and item.chat_key is not None:
            return "messages"
    elif item.event.event_type == "update_message_flags":
        if _batchable_flag_change(item.event.payload) is not None:
            return "flags"
    elif item.event.event_type == "reaction":
        if _reaction_change(item.event.payload) is not None:
            return "reactions"
    return None


def _batchable_message_event(event: _ClaimedEvent) -> bool:
    message = event.payload.get("message")
    if not isinstance(message, Mapping) or event.own_user_id is None:
        return False
    if (
        not isinstance(message.get("id"), int)
        or not isinstance(message.get("sender_id"), int)
        or not isinstance(message.get("content"), str)
        or not isinstance(message.get("timestamp"), int)
        or message.get("type") not in {"stream", "private"}
    ):
        return False
    flags = message.get("flags")
    reactions = message.get("reactions")
    return (
        isinstance(flags, list)
        and all(isinstance(flag, str) for flag in flags)
        and isinstance(reactions, list)
    )


def _event_chat_key(
    payload: Mapping[str, Any],
    own_user_id: int | None,
) -> str | None:
    if payload.get("type") == "stream":
        stream_id = payload.get("stream_id")
        if isinstance(stream_id, int):
            return f"channel:{stream_id}"
        return None
    message = payload.get("message")
    if not isinstance(message, Mapping):
        return None
    if message.get("type") == "stream":
        stream_id = message.get("stream_id")
        if isinstance(stream_id, int):
            return f"channel:{stream_id}"
        return None
    if message.get("type") != "private" or own_user_id is None:
        return None
    recipients = message.get("display_recipient")
    if not isinstance(recipients, list):
        return None
    user_ids = {own_user_id}
    for recipient in recipients:
        if not isinstance(recipient, Mapping):
            return None
        user_id = recipient.get("id")
        if not isinstance(user_id, int):
            return None
        user_ids.add(user_id)
    return "direct:" + ",".join(str(user_id) for user_id in sorted(user_ids))


def _snapshot_message(
    snapshot: _MessageSnapshot,
    **changes: Any,
) -> ZulipMessage:
    updated = replace(snapshot, **changes)
    reactions = [dict(reaction) for reaction in updated.reactions]
    message_hash = message_state_hash(
        sender_user_uuid=updated.sender_user_uuid,
        chat_key=updated.chat_key,
        topic_name=updated.topic_name,
        content=updated.content,
        sent_at=updated.sent_at,
        is_read=updated.is_read,
        is_starred=updated.is_starred,
        is_collapsed=updated.is_collapsed,
        is_mentioned=updated.is_mentioned,
        is_stream_wildcard_mentioned=updated.is_stream_wildcard_mentioned,
        is_topic_wildcard_mentioned=updated.is_topic_wildcard_mentioned,
        has_alert_word=updated.has_alert_word,
        is_historical=updated.is_historical,
        reactions=reactions,
    )
    return ZulipMessage(
        message_id=updated.message_id,
        chat_key=updated.chat_key,
        topic_name=updated.topic_name,
        sender_user_uuid=updated.sender_user_uuid,
        content=updated.content,
        is_read=updated.is_read,
        is_starred=updated.is_starred,
        is_collapsed=updated.is_collapsed,
        is_mentioned=updated.is_mentioned,
        is_stream_wildcard_mentioned=updated.is_stream_wildcard_mentioned,
        is_topic_wildcard_mentioned=updated.is_topic_wildcard_mentioned,
        has_alert_word=updated.has_alert_word,
        is_historical=updated.is_historical,
        reactions_json=json.dumps(
            reactions,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        message_hash=message_hash,
        sent_at=updated.sent_at,
    )


def _snapshot_with_flag(
    snapshot: _MessageSnapshot,
    field: str,
    value: bool,
) -> _MessageSnapshot:
    if field == "is_read":
        return replace(snapshot, is_read=value)
    if field == "is_starred":
        return replace(snapshot, is_starred=value)
    if field == "is_collapsed":
        return replace(snapshot, is_collapsed=value)
    if field == "is_mentioned":
        return replace(snapshot, is_mentioned=value)
    if field == "is_stream_wildcard_mentioned":
        return replace(snapshot, is_stream_wildcard_mentioned=value)
    if field == "is_topic_wildcard_mentioned":
        return replace(snapshot, is_topic_wildcard_mentioned=value)
    if field == "has_alert_word":
        return replace(snapshot, has_alert_word=value)
    if field == "is_historical":
        return replace(snapshot, is_historical=value)
    raise ValueError(f"unsupported message flag field: {field}")


def _reaction_sort_key(reaction: Mapping[str, str]) -> tuple[str, str, str, str]:
    return (
        reaction["user_uuid"],
        reaction["reaction_type"],
        reaction["emoji_code"],
        reaction["emoji_name"],
    )


def _chat_content_hash(
    chat_type: str,
    chat_key: str,
    name: str,
    parameters_json: str,
) -> bytes:
    digest = hashlib.sha256()
    for value in (chat_type, chat_key, name, parameters_json):
        digest.update(value.encode("utf-8"))
        digest.update(b"\0")
    return digest.digest()
