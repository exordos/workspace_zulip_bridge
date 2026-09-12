# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import argparse
import json
from datetime import UTC
from datetime import datetime

import pytest

from workspace_zulip_bridge.monitor import MonitorSnapshot
from workspace_zulip_bridge.monitor import _positive_float
from workspace_zulip_bridge.monitor import format_snapshot


def _snapshot(*, exact: bool) -> MonitorSnapshot:
    return MonitorSnapshot(
        sampled_at=datetime(2026, 9, 12, 12, 0, tzinfo=UTC),
        users_total=275,
        users_sync_enabled=274,
        user_statuses={"active": 270, "filling": 5},
        queues_ready=274,
        chats_total=1200,
        chat_types={"channel": 700, "direct": 450, "group_direct": 50},
        chat_memberships_total=32075,
        chats_assigned=1180,
        chats_pending_history=15,
        topics_total=900,
        messages_total=125000,
        messages_total_exact=exact,
        recent_message_changes=120,
        reactions_total=400 if exact else None,
        events_total=3000,
        events_total_exact=exact,
        recent_window_seconds=60.0,
        recent_events=30,
        recent_event_types={"message": 20, "reaction": 10},
        event_processing_statuses={"applied": 2600, "pending": 20, "skipped": 380},
        recent_processed_events=25,
        recent_processing_outcomes={"applied": 20, "skipped": 5},
        oldest_pending_event_seconds=1.25,
        average_processing_latency_seconds=0.05,
        p95_processing_latency_seconds=0.12,
        event_table_bytes=65536,
        message_table_bytes=1048576,
        message_heap_bytes=524288,
        message_index_bytes=393216,
        topic_table_bytes=32768,
        chat_table_bytes=65536,
        chat_user_table_bytes=131072,
        database_bytes=2097152,
        total_event_types={"message": 2500, "reaction": 500} if exact else None,
    )


def test_human_snapshot_marks_estimated_event_total() -> None:
    rendered = format_snapshot(_snapshot(exact=False), as_json=False)

    assert "users=275" in rendered
    assert 'statuses={"active":270,"filling":5}' in rendered
    assert "sync_enabled=274" in rendered
    assert "queues=274/274" in rendered
    assert "queues_missing=0" in rendered
    assert "chats=1200" in rendered
    assert "chat_memberships=32075" in rendered
    assert "chats_assigned=1180" in rendered
    assert "chats_unassigned=20" in rendered
    assert "chats_pending_history=15" in rendered
    assert 'chat_types={"channel":700,"direct":450,"group_direct":50}' in rendered
    assert "topics=900" in rendered
    assert "messages_estimate=125000" in rendered
    assert "message_rate=2.000/s" in rendered
    assert "events_estimate=3000" in rendered
    assert "rate=0.500/s" in rendered
    assert "processor_rate=0.417/s" in rendered
    assert 'processing_statuses={"applied":2600,"pending":20,"skipped":380}' in rendered
    assert "oldest_pending_seconds=1.250" in rendered
    assert "processing_latency_p95_seconds=0.120000" in rendered
    assert 'recent_types={"message":20,"reaction":10}' in rendered
    assert "total_types=" not in rendered


def test_exact_json_snapshot_contains_derived_counters() -> None:
    rendered = json.loads(format_snapshot(_snapshot(exact=True), as_json=True))

    assert rendered["sampled_at"] == "2026-09-12T12:00:00+00:00"
    assert rendered["events_total_exact"] is True
    assert rendered["users_sync_enabled"] == 274
    assert rendered["queues_missing"] == 0
    assert rendered["user_statuses"] == {"active": 270, "filling": 5}
    assert rendered["chats_total"] == 1200
    assert rendered["chat_memberships_total"] == 32075
    assert rendered["chats_assigned"] == 1180
    assert rendered["chats_pending_history"] == 15
    assert rendered["topics_total"] == 900
    assert rendered["messages_total"] == 125000
    assert rendered["messages_per_second"] == 2.0
    assert rendered["reactions_total"] == 400
    assert rendered["events_per_second"] == 0.5
    assert rendered["processed_events_per_second"] == 0.417
    assert rendered["recent_processing_outcomes"] == {"applied": 20, "skipped": 5}
    assert rendered["total_event_types"] == {"message": 2500, "reaction": 500}


@pytest.mark.parametrize("value", ["0", "-1", "-0.5"])
def test_monitor_intervals_must_be_positive(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="positive"):
        _positive_float(value)
