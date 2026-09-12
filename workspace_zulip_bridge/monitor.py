# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import argparse
import asyncio
import json
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from workspace_zulip_bridge.config import Settings


@dataclass(frozen=True, slots=True)
class MonitorSnapshot:
    sampled_at: datetime
    users_total: int
    users_sync_enabled: int
    user_statuses: dict[str, int]
    queues_ready: int
    chats_total: int
    chat_types: dict[str, int]
    chat_memberships_total: int
    chats_assigned: int
    chats_pending_history: int
    topics_total: int
    messages_total: int
    messages_total_exact: bool
    recent_message_changes: int
    reactions_total: int | None
    events_total: int
    events_total_exact: bool
    recent_window_seconds: float
    recent_events: int
    recent_event_types: dict[str, int]
    event_processing_statuses: dict[str, int]
    recent_processed_events: int
    recent_processing_outcomes: dict[str, int]
    oldest_pending_event_seconds: float
    average_processing_latency_seconds: float
    p95_processing_latency_seconds: float
    event_table_bytes: int
    message_table_bytes: int
    message_heap_bytes: int
    message_index_bytes: int
    topic_table_bytes: int
    chat_table_bytes: int
    chat_user_table_bytes: int
    database_bytes: int
    total_event_types: dict[str, int] | None = None

    @property
    def queues_missing(self) -> int:
        return self.users_sync_enabled - self.queues_ready

    @property
    def events_per_second(self) -> float:
        return self.recent_events / self.recent_window_seconds

    @property
    def messages_per_second(self) -> float:
        return self.recent_message_changes / self.recent_window_seconds

    @property
    def processed_events_per_second(self) -> float:
        return self.recent_processed_events / self.recent_window_seconds


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print read-only Workspace Zulip bridge counters"
    )
    parser.add_argument(
        "--interval",
        type=_positive_float,
        default=5.0,
        help="seconds between samples (default: 5)",
    )
    parser.add_argument(
        "--window",
        type=_positive_float,
        default=60.0,
        help="rolling event-rate window in seconds (default: 60)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="print one sample and exit",
    )
    parser.add_argument(
        "--exact",
        action="store_true",
        help="scan the event table for an exact total and totals by type",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit one JSON object per line",
    )
    return parser


async def collect_snapshot(
    connection: asyncpg.Connection,
    *,
    window_seconds: float,
    exact: bool,
) -> MonitorSnapshot:
    summary = await connection.fetchrow(
        """
        WITH user_counts AS (
            SELECT count(*) AS users_total,
                   count(*) FILTER (
                       WHERE NOT disabled AND api_key IS NOT NULL
                   ) AS users_sync_enabled,
                   count(*) FILTER (
                       WHERE NOT disabled
                         AND api_key IS NOT NULL
                         AND queue_id IS NOT NULL
                   ) AS queues_ready
            FROM workspace_zulip_bridge.zulip_users
        ),
        relation_stats AS (
            SELECT COALESCE(max(n_live_tup) FILTER (
                       WHERE relname = 'zulip_events'
                   ), 0)::bigint AS events_estimate,
                   COALESCE(max(n_live_tup) FILTER (
                       WHERE relname = 'zulip_messages'
                   ), 0)::bigint AS messages_estimate
            FROM pg_stat_user_tables
            WHERE schemaname = 'workspace_zulip_bridge'
              AND relname IN ('zulip_events', 'zulip_messages')
        ),
        chat_counts AS (
            SELECT count(*) FILTER (
                       WHERE supplier_user_uuid IS NOT NULL
                   ) AS chats_assigned,
                   count(*) FILTER (
                       WHERE supplier_user_uuid IS NOT NULL
                         AND history_loaded_at IS NULL
                   ) AS chats_pending_history
            FROM workspace_zulip_bridge.zulip_chats
        )
        SELECT clock_timestamp() AS sampled_at,
               user_counts.users_total,
               user_counts.users_sync_enabled,
               user_counts.queues_ready,
               relation_stats.events_estimate,
               relation_stats.messages_estimate,
               chat_counts.chats_assigned,
               chat_counts.chats_pending_history,
               (
                   SELECT count(*)
                   FROM workspace_zulip_bridge.zulip_chat_users
               ) AS chat_memberships_total,
               pg_total_relation_size(
                   'workspace_zulip_bridge.zulip_events'
               ) AS event_table_bytes,
               pg_total_relation_size(
                   'workspace_zulip_bridge.zulip_messages'
               ) AS message_table_bytes,
               pg_relation_size(
                   'workspace_zulip_bridge.zulip_messages'
               ) AS message_heap_bytes,
               pg_indexes_size(
                   'workspace_zulip_bridge.zulip_messages'
               ) AS message_index_bytes,
               pg_total_relation_size(
                   'workspace_zulip_bridge.zulip_topics'
               ) AS topic_table_bytes,
               pg_total_relation_size(
                   'workspace_zulip_bridge.zulip_chats'
               ) AS chat_table_bytes,
               pg_total_relation_size(
                   'workspace_zulip_bridge.zulip_chat_users'
               ) AS chat_user_table_bytes,
               pg_database_size(current_database()) AS database_bytes
        FROM user_counts
        CROSS JOIN relation_stats
        CROSS JOIN chat_counts
        """
    )
    if summary is None:
        raise RuntimeError("monitor summary query returned no row")

    user_status_rows = await connection.fetch(
        """
        SELECT status, count(*) AS users
        FROM workspace_zulip_bridge.zulip_users
        GROUP BY status
        ORDER BY status
        """
    )
    chat_type_rows = await connection.fetch(
        """
        SELECT chat_type, count(*) AS chats
        FROM workspace_zulip_bridge.zulip_chats
        GROUP BY chat_type
        ORDER BY chat_type
        """
    )
    user_statuses = {row["status"]: row["users"] for row in user_status_rows}
    chat_types = {row["chat_type"]: row["chats"] for row in chat_type_rows}
    processing_status_rows = await connection.fetch(
        """
        SELECT processing_status, count(*) AS events
        FROM workspace_zulip_bridge.zulip_events
        GROUP BY processing_status
        ORDER BY processing_status
        """
    )
    event_processing_statuses = {
        row["processing_status"]: row["events"] for row in processing_status_rows
    }
    topics_total = await connection.fetchval(
        "SELECT count(*) FROM workspace_zulip_bridge.zulip_topics"
    )

    sampled_at = summary["sampled_at"]
    recent_message_changes = await connection.fetchval(
        """
        SELECT count(*)
        FROM workspace_zulip_bridge.zulip_messages
        WHERE updated_at > (
                  $1::timestamptz
                  - make_interval(secs => $2::double precision)
              )
          AND updated_at <= $1::timestamptz
        """,
        sampled_at,
        window_seconds,
    )
    processing_row = await connection.fetchrow(
        """
        SELECT count(*) FILTER (
                   WHERE processed_at > (
                       $1::timestamptz
                       - make_interval(secs => $2::double precision)
                   )
               ) AS recent_processed,
               COALESCE(
                   extract(
                       epoch FROM (
                           $1::timestamptz
                           - min(created_at) FILTER (
                               WHERE processing_status = 'pending'
                           )
                       )
                   ),
                   0
               ) AS oldest_pending_seconds,
               COALESCE(
                   avg(extract(epoch FROM (processed_at - created_at)))
                       FILTER (
                           WHERE processed_at > (
                               $1::timestamptz
                               - make_interval(secs => $2::double precision)
                           )
                       ),
                   0
               ) AS average_latency_seconds,
               COALESCE(
                   percentile_cont(0.95) WITHIN GROUP (
                       ORDER BY extract(epoch FROM (processed_at - created_at))
                   ) FILTER (
                       WHERE processed_at > (
                           $1::timestamptz
                           - make_interval(secs => $2::double precision)
                       )
                   ),
                   0
               ) AS p95_latency_seconds
        FROM workspace_zulip_bridge.zulip_events
        WHERE created_at <= $1::timestamptz
        """,
        sampled_at,
        window_seconds,
    )
    if processing_row is None:
        raise RuntimeError("monitor event processing query returned no row")
    recent_processing_rows = await connection.fetch(
        """
        SELECT processing_status, count(*) AS events
        FROM workspace_zulip_bridge.zulip_events
        WHERE processed_at > (
                  $1::timestamptz
                  - make_interval(secs => $2::double precision)
              )
          AND processed_at <= $1::timestamptz
        GROUP BY processing_status
        ORDER BY processing_status
        """,
        sampled_at,
        window_seconds,
    )
    recent_processing_outcomes = {
        row["processing_status"]: row["events"] for row in recent_processing_rows
    }
    if exact:
        message_row = await connection.fetchrow(
            """
            SELECT count(*) AS messages,
                   COALESCE(sum(jsonb_array_length(reactions)), 0) AS reactions
            FROM workspace_zulip_bridge.zulip_messages
            """
        )
        if message_row is None:
            raise RuntimeError("monitor message query returned no row")
        messages_total = message_row["messages"]
        reactions_total = message_row["reactions"]
        event_rows = await connection.fetch(
            """
            SELECT event_type,
                   count(*) AS total_events,
                   count(*) FILTER (
                       WHERE created_at > (
                           $1::timestamptz
                           - make_interval(secs => $2::double precision)
                       )
                   ) AS recent_events
            FROM workspace_zulip_bridge.zulip_events
            WHERE created_at <= $1::timestamptz
            GROUP BY event_type
            ORDER BY event_type
            """,
            sampled_at,
            window_seconds,
        )
        total_event_types = {
            row["event_type"]: row["total_events"] for row in event_rows
        }
        events_total = sum(total_event_types.values())
    else:
        messages_total = summary["messages_estimate"]
        reactions_total = None
        event_rows = await connection.fetch(
            """
            SELECT event_type, count(*) AS recent_events
            FROM workspace_zulip_bridge.zulip_events
            WHERE created_at > (
                      $1::timestamptz
                      - make_interval(secs => $2::double precision)
                  )
              AND created_at <= $1::timestamptz
            GROUP BY event_type
            ORDER BY event_type
            """,
            sampled_at,
            window_seconds,
        )
        total_event_types = None
        events_total = summary["events_estimate"]

    recent_event_types = {
        row["event_type"]: row["recent_events"]
        for row in event_rows
        if row["recent_events"] > 0
    }
    return MonitorSnapshot(
        sampled_at=sampled_at,
        users_total=summary["users_total"],
        users_sync_enabled=summary["users_sync_enabled"],
        user_statuses=user_statuses,
        queues_ready=summary["queues_ready"],
        chats_total=sum(chat_types.values()),
        chat_types=chat_types,
        chat_memberships_total=summary["chat_memberships_total"],
        chats_assigned=summary["chats_assigned"],
        chats_pending_history=summary["chats_pending_history"],
        topics_total=topics_total,
        messages_total=messages_total,
        messages_total_exact=exact,
        recent_message_changes=recent_message_changes,
        reactions_total=reactions_total,
        events_total=events_total,
        events_total_exact=exact,
        recent_window_seconds=window_seconds,
        recent_events=sum(recent_event_types.values()),
        recent_event_types=recent_event_types,
        event_processing_statuses=event_processing_statuses,
        recent_processed_events=processing_row["recent_processed"],
        recent_processing_outcomes=recent_processing_outcomes,
        oldest_pending_event_seconds=float(processing_row["oldest_pending_seconds"]),
        average_processing_latency_seconds=float(
            processing_row["average_latency_seconds"]
        ),
        p95_processing_latency_seconds=float(processing_row["p95_latency_seconds"]),
        event_table_bytes=summary["event_table_bytes"],
        message_table_bytes=summary["message_table_bytes"],
        message_heap_bytes=summary["message_heap_bytes"],
        message_index_bytes=summary["message_index_bytes"],
        topic_table_bytes=summary["topic_table_bytes"],
        chat_table_bytes=summary["chat_table_bytes"],
        chat_user_table_bytes=summary["chat_user_table_bytes"],
        database_bytes=summary["database_bytes"],
        total_event_types=total_event_types,
    )


def format_snapshot(snapshot: MonitorSnapshot, *, as_json: bool) -> str:
    if as_json:
        values: dict[str, Any] = asdict(snapshot)
        values["sampled_at"] = snapshot.sampled_at.isoformat()
        values["queues_missing"] = snapshot.queues_missing
        values["events_per_second"] = round(snapshot.events_per_second, 3)
        values["messages_per_second"] = round(snapshot.messages_per_second, 3)
        values["processed_events_per_second"] = round(
            snapshot.processed_events_per_second,
            3,
        )
        return json.dumps(values, ensure_ascii=False, separators=(",", ":"))

    event_label = "events" if snapshot.events_total_exact else "events_estimate"
    fields = [
        snapshot.sampled_at.isoformat(),
        f"users={snapshot.users_total}",
        f"sync_enabled={snapshot.users_sync_enabled}",
        "statuses=" + json.dumps(snapshot.user_statuses, separators=(",", ":")),
        f"queues={snapshot.queues_ready}/{snapshot.users_sync_enabled}",
        f"queues_missing={snapshot.queues_missing}",
        f"chats={snapshot.chats_total}",
        f"chat_memberships={snapshot.chat_memberships_total}",
        f"chats_assigned={snapshot.chats_assigned}",
        f"chats_unassigned={snapshot.chats_total - snapshot.chats_assigned}",
        f"chats_pending_history={snapshot.chats_pending_history}",
        "chat_types=" + json.dumps(snapshot.chat_types, separators=(",", ":")),
        f"topics={snapshot.topics_total}",
        ("messages=" if snapshot.messages_total_exact else "messages_estimate=")
        + str(snapshot.messages_total),
        f"message_changes={snapshot.recent_message_changes}/"
        f"{snapshot.recent_window_seconds:g}s",
        f"message_rate={snapshot.messages_per_second:.3f}/s",
        f"{event_label}={snapshot.events_total}",
        f"recent={snapshot.recent_events}/{snapshot.recent_window_seconds:g}s",
        f"rate={snapshot.events_per_second:.3f}/s",
        "processing_statuses="
        + json.dumps(snapshot.event_processing_statuses, separators=(",", ":")),
        f"processed={snapshot.recent_processed_events}/"
        f"{snapshot.recent_window_seconds:g}s",
        f"processor_rate={snapshot.processed_events_per_second:.3f}/s",
        "processing_outcomes="
        + json.dumps(snapshot.recent_processing_outcomes, separators=(",", ":")),
        f"oldest_pending_seconds={snapshot.oldest_pending_event_seconds:.3f}",
        "processing_latency_avg_seconds="
        f"{snapshot.average_processing_latency_seconds:.6f}",
        f"processing_latency_p95_seconds={snapshot.p95_processing_latency_seconds:.6f}",
        f"event_table_bytes={snapshot.event_table_bytes}",
        f"message_table_bytes={snapshot.message_table_bytes}",
        f"message_heap_bytes={snapshot.message_heap_bytes}",
        f"message_index_bytes={snapshot.message_index_bytes}",
        f"topic_table_bytes={snapshot.topic_table_bytes}",
        f"chat_table_bytes={snapshot.chat_table_bytes}",
        f"chat_user_table_bytes={snapshot.chat_user_table_bytes}",
        f"database_bytes={snapshot.database_bytes}",
        "recent_types="
        + json.dumps(snapshot.recent_event_types, separators=(",", ":")),
    ]
    if snapshot.total_event_types is not None:
        fields.append(
            "total_types="
            + json.dumps(snapshot.total_event_types, separators=(",", ":"))
        )
    if snapshot.reactions_total is not None:
        fields.append(f"reactions={snapshot.reactions_total}")
    return " ".join(fields)


async def _run(args: argparse.Namespace) -> None:
    settings = Settings.from_env()
    connection = await asyncpg.connect(
        dsn=settings.database_dsn,
        command_timeout=settings.db_command_timeout_seconds,
        server_settings={
            "application_name": "workspace-zulip-bridge-monitor",
            "timezone": "UTC",
            "default_text_search_config": "pg_catalog.simple",
        },
    )
    try:
        while True:
            snapshot = await collect_snapshot(
                connection,
                window_seconds=args.window,
                exact=args.exact,
            )
            print(format_snapshot(snapshot, as_json=args.json), flush=True)
            if args.once:
                return
            await asyncio.sleep(args.interval)
    finally:
        await connection.close()


def main() -> None:
    args = _parser().parse_args()
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
