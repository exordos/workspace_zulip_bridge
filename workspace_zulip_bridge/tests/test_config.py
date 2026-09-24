# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from pathlib import Path
from uuid import UUID

import pytest

from workspace_zulip_bridge.config import Settings


def test_defaults_use_local_postgresql_socket() -> None:
    settings = Settings.from_env({})

    assert "host=/var/run/postgresql" in settings.database_dsn
    assert settings.db_pool_min_size == 2
    assert settings.db_pool_max_size == 16
    assert settings.db_probe_seconds == 30.0
    assert settings.zulip_db_ack_timeout_seconds == 120.0
    assert settings.event_processor_batch_size == 128
    assert settings.event_processor_realtime_batch_size == 16
    assert settings.event_processor_realtime_window_seconds == 300.0
    assert settings.event_processor_poll_seconds == 0.05
    assert settings.event_processor_max_attempts == 8
    assert settings.event_processor_retry_base_seconds == 0.25
    assert settings.event_processor_retry_cap_seconds == 30.0
    assert settings.event_retention_seconds == 86400.0
    assert settings.event_cleanup_interval_seconds == 300.0
    assert settings.event_cleanup_batch_size == 10000
    assert not settings.workspace_events_enabled
    assert settings.workspace_event_batch_size == 500
    assert settings.workspace_event_max_attempts == 8
    assert settings.workspace_sync_plan_batch_size == 5000
    assert settings.workspace_sync_batch_size == 500
    assert settings.workspace_sync_workers == 2
    assert settings.zulip_queue_gap_reconciliation_seconds == 86400.0
    assert settings.workspace_dependency_retry_base_seconds == 2.0
    assert settings.workspace_dependency_retry_cap_seconds == 300.0


def test_environment_overrides_are_parsed(tmp_path: Path) -> None:
    ca_file = tmp_path / "zulip-ca.pem"
    ca_file.write_text("test certificate")
    token_file = tmp_path / "workspace.token"
    token_file.write_text("header.payload.signature")
    settings = Settings.from_env(
        {
            "WZB_DATABASE_DSN": "postgresql://database/bridge",
            "WZB_DB_POOL_MIN_SIZE": "4",
            "WZB_DB_POOL_MAX_SIZE": "32",
            "WZB_DB_PROBE_SECONDS": "0.05",
            "WZB_USER_REFRESH_SECONDS": "0.25",
            "WZB_ZULIP_CA_FILE": str(ca_file),
            "WZB_ZULIP_REGISTRATION_CONCURRENCY": "4",
            "WZB_ZULIP_MESSAGE_SCAN_CONCURRENCY": "12",
            "WZB_ZULIP_HISTORY_CONCURRENCY": "6",
            "WZB_ZULIP_DIRECTORY_CACHE_TTL_SECONDS": "90",
            "WZB_ZULIP_CHAT_FILL_TIMEOUT_SECONDS": "240",
            "WZB_ZULIP_MESSAGE_PAGE_SIZE": "4000",
            "WZB_EVENT_PROCESSOR_BATCH_SIZE": "750",
            "WZB_EVENT_PROCESSOR_REALTIME_BATCH_SIZE": "24",
            "WZB_EVENT_PROCESSOR_REALTIME_WINDOW_SECONDS": "180",
            "WZB_EVENT_PROCESSOR_POLL_SECONDS": "0.1",
            "WZB_EVENT_PROCESSOR_CLAIM_TIMEOUT_SECONDS": "30",
            "WZB_EVENT_PROCESSOR_MAX_ATTEMPTS": "5",
            "WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS": "0.5",
            "WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS": "12",
            "WZB_EVENT_RETENTION_SECONDS": "3600",
            "WZB_EVENT_CLEANUP_INTERVAL_SECONDS": "10",
            "WZB_EVENT_CLEANUP_BATCH_SIZE": "2500",
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "wss://workspace.example/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
            "WZB_WORKSPACE_EVENT_BATCH_SIZE": "250",
            "WZB_WORKSPACE_EVENT_FLUSH_SECONDS": "0.02",
            "WZB_WORKSPACE_EVENT_MAX_ATTEMPTS": "6",
            "WZB_WORKSPACE_SYNC_PLAN_BATCH_SIZE": "2500",
            "WZB_WORKSPACE_SYNC_BATCH_SIZE": "250",
            "WZB_WORKSPACE_SYNC_WORKERS": "4",
            "WZB_ZULIP_QUEUE_GAP_RECONCILIATION_SECONDS": "7200",
            "WZB_WORKSPACE_DEPENDENCY_RETRY_BASE_SECONDS": "4",
            "WZB_WORKSPACE_DEPENDENCY_RETRY_CAP_SECONDS": "90",
        }
    )

    assert settings.database_dsn == "postgresql://database/bridge"
    assert settings.db_pool_min_size == 4
    assert settings.db_pool_max_size == 32
    assert settings.db_probe_seconds == 0.05
    assert settings.user_refresh_seconds == 0.25
    assert settings.zulip_ca_file == ca_file
    assert settings.zulip_registration_concurrency == 4
    assert settings.zulip_message_scan_concurrency == 12
    assert settings.zulip_history_concurrency == 6
    assert settings.zulip_directory_cache_ttl_seconds == 90
    assert settings.zulip_chat_fill_timeout_seconds == 240
    assert settings.zulip_message_page_size == 4000
    assert settings.event_processor_batch_size == 750
    assert settings.event_processor_realtime_batch_size == 24
    assert settings.event_processor_realtime_window_seconds == 180.0
    assert settings.event_processor_poll_seconds == 0.1
    assert settings.event_processor_claim_timeout_seconds == 30
    assert settings.event_processor_max_attempts == 5
    assert settings.event_processor_retry_base_seconds == 0.5
    assert settings.event_processor_retry_cap_seconds == 12
    assert settings.event_retention_seconds == 3600
    assert settings.event_cleanup_interval_seconds == 10
    assert settings.event_cleanup_batch_size == 2500
    assert settings.workspace_events_enabled
    assert settings.workspace_project_id == UUID("10000000-0000-0000-0000-000000000001")
    assert settings.workspace_event_batch_size == 250
    assert settings.workspace_event_flush_seconds == 0.02
    assert settings.workspace_event_max_attempts == 6
    assert settings.workspace_sync_plan_batch_size == 2500
    assert settings.workspace_sync_batch_size == 250
    assert settings.workspace_sync_workers == 4
    assert settings.zulip_queue_gap_reconciliation_seconds == 7200.0
    assert settings.workspace_dependency_retry_base_seconds == 4.0
    assert settings.workspace_dependency_retry_cap_seconds == 90.0


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("WZB_DB_POOL_MIN_SIZE", "0"),
        ("WZB_DB_COMMAND_TIMEOUT_SECONDS", "0"),
        ("WZB_DB_PROBE_SECONDS", "0"),
        ("WZB_DB_POOL_MAX_SIZE", "not-a-number"),
        ("WZB_ZULIP_REGISTRATION_CONCURRENCY", "0"),
        ("WZB_ZULIP_MESSAGE_SCAN_CONCURRENCY", "0"),
        ("WZB_ZULIP_HISTORY_CONCURRENCY", "0"),
        ("WZB_ZULIP_DIRECTORY_CACHE_TTL_SECONDS", "0"),
        ("WZB_ZULIP_CHAT_FILL_TIMEOUT_SECONDS", "0"),
        ("WZB_ZULIP_MESSAGE_PAGE_SIZE", "5001"),
        ("WZB_EVENT_PROCESSOR_BATCH_SIZE", "0"),
        ("WZB_EVENT_PROCESSOR_BATCH_SIZE", "10001"),
        ("WZB_EVENT_PROCESSOR_REALTIME_BATCH_SIZE", "0"),
        ("WZB_EVENT_PROCESSOR_REALTIME_BATCH_SIZE", "10001"),
        ("WZB_EVENT_PROCESSOR_REALTIME_WINDOW_SECONDS", "0"),
        ("WZB_EVENT_PROCESSOR_POLL_SECONDS", "0"),
        ("WZB_ZULIP_QUEUE_GAP_RECONCILIATION_SECONDS", "0"),
        ("WZB_WORKSPACE_DEPENDENCY_RETRY_BASE_SECONDS", "0"),
        ("WZB_WORKSPACE_DEPENDENCY_RETRY_CAP_SECONDS", "0"),
        ("WZB_EVENT_PROCESSOR_CLAIM_TIMEOUT_SECONDS", "0"),
        ("WZB_EVENT_PROCESSOR_MAX_ATTEMPTS", "0"),
        ("WZB_EVENT_PROCESSOR_RETRY_BASE_SECONDS", "0"),
        ("WZB_EVENT_PROCESSOR_RETRY_CAP_SECONDS", "0"),
        ("WZB_EVENT_RETENTION_SECONDS", "0"),
        ("WZB_EVENT_CLEANUP_INTERVAL_SECONDS", "0"),
        ("WZB_EVENT_CLEANUP_BATCH_SIZE", "0"),
        ("WZB_EVENT_CLEANUP_BATCH_SIZE", "100001"),
        ("WZB_WORKSPACE_EVENT_BATCH_SIZE", "0"),
        ("WZB_WORKSPACE_EVENT_BATCH_SIZE", "10001"),
        ("WZB_WORKSPACE_EVENT_FLUSH_SECONDS", "0"),
        ("WZB_WORKSPACE_EVENT_MAX_ATTEMPTS", "0"),
        ("WZB_WORKSPACE_SYNC_PLAN_BATCH_SIZE", "0"),
        ("WZB_WORKSPACE_SYNC_PLAN_BATCH_SIZE", "100001"),
        ("WZB_WORKSPACE_SYNC_WORKERS", "0"),
        ("WZB_WORKSPACE_SYNC_WORKERS", "9"),
        ("WZB_ZULIP_RETRY_CAP_SECONDS", "0"),
    ],
)
def test_invalid_values_fail_fast(name: str, value: str) -> None:
    with pytest.raises(ValueError):
        Settings.from_env({name: value})


def test_pool_maximum_cannot_be_smaller_than_minimum() -> None:
    with pytest.raises(ValueError, match="at least"):
        Settings.from_env(
            {
                "WZB_DB_POOL_MIN_SIZE": "8",
                "WZB_DB_POOL_MAX_SIZE": "4",
            }
        )


def test_history_concurrency_reserves_pool_connections() -> None:
    with pytest.raises(ValueError, match="smaller"):
        Settings.from_env(
            {
                "WZB_DB_POOL_MAX_SIZE": "12",
                "WZB_ZULIP_HISTORY_CONCURRENCY": "12",
            }
        )


def test_retry_cap_cannot_be_smaller_than_base() -> None:
    with pytest.raises(ValueError, match="at least"):
        Settings.from_env(
            {
                "WZB_ZULIP_RETRY_BASE_SECONDS": "10",
                "WZB_ZULIP_RETRY_CAP_SECONDS": "5",
            }
        )


def test_dependency_retry_cap_cannot_be_smaller_than_base() -> None:
    with pytest.raises(ValueError, match="at least"):
        Settings.from_env(
            {
                "WZB_WORKSPACE_DEPENDENCY_RETRY_BASE_SECONDS": "10",
                "WZB_WORKSPACE_DEPENDENCY_RETRY_CAP_SECONDS": "5",
            }
        )


def test_database_ack_timeout_must_exceed_command_timeout() -> None:
    with pytest.raises(ValueError, match="must be greater"):
        Settings.from_env(
            {
                "WZB_DB_COMMAND_TIMEOUT_SECONDS": "30",
                "WZB_ZULIP_DB_ACK_TIMEOUT_SECONDS": "30",
            }
        )


def test_ca_file_must_exist() -> None:
    with pytest.raises(ValueError, match="readable file"):
        Settings.from_env({"WZB_ZULIP_CA_FILE": "/missing/zulip-ca.pem"})


def test_control_ca_uses_system_trust_until_bundle_is_materialized(
    tmp_path: Path,
) -> None:
    ca_file = tmp_path / "control" / "zulip-ca.pem"
    enrollment_file = tmp_path / "enrollment.secret"
    enrollment_file.write_text("enrollment-secret\n")
    settings = Settings.from_env(
        {
            "WZB_ZULIP_CA_FILE": str(ca_file),
            "WZB_WORKSPACE_CONTROL_URL": "https://control.example:21443",
            "WZB_WORKSPACE_CONTROL_BOOTSTRAP_URL": "http://control.example:21085",
            "WZB_WORKSPACE_CONTROL_HOSTNAME": "control.example",
            "WZB_WORKSPACE_REALM_UUID": "10000000-0000-0000-0000-000000000003",
            "WZB_WORKSPACE_BRIDGE_INSTANCE_UUID": (
                "10000000-0000-0000-0000-000000000004"
            ),
            "WZB_WORKSPACE_ENROLLMENT_SECRET_FILE": str(enrollment_file),
        }
    )

    assert settings.zulip_ca_materialization_file == ca_file
    assert settings.effective_zulip_ca_file is None

    ca_file.parent.mkdir()
    ca_file.write_text("test certificate")

    assert settings.effective_zulip_ca_file == ca_file


def test_workspace_websocket_configuration_is_atomic(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="configured together"):
        Settings.from_env(
            {"WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws"}
        )

    token_file = tmp_path / "workspace.token"
    token_file.write_text("token")
    values = {
        "WZB_WORKSPACE_WEBSOCKET_URL": "https://workspace.example/events/ws",
        "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
        "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
        "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
    }
    with pytest.raises(ValueError, match="ws or wss"):
        Settings.from_env(values)


def test_workspace_password_and_control_configuration_is_atomic(
    tmp_path: Path,
) -> None:
    password_file = tmp_path / "workspace.password"
    password_file.write_text("secret\n")
    enrollment_file = tmp_path / "enrollment.secret"
    enrollment_file.write_text("enrollment-secret\n")
    settings = Settings.from_env(
        {
            "WZB_ZULIP_CA_FILE": str(tmp_path / "zulip-ca.pem"),
            "WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws",
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(tmp_path / "workspace.token"),
            "WZB_WORKSPACE_USERNAME": "workspace-zulip-bridge",
            "WZB_WORKSPACE_PASSWORD_FILE": str(password_file),
            "WZB_WORKSPACE_CONTROL_URL": "https://control.example:21443",
            "WZB_WORKSPACE_CONTROL_BOOTSTRAP_URL": "http://control.example:21085",
            "WZB_WORKSPACE_CONTROL_HOSTNAME": "control.example",
            "WZB_WORKSPACE_REALM_UUID": "10000000-0000-0000-0000-000000000003",
            "WZB_WORKSPACE_BRIDGE_INSTANCE_UUID": (
                "10000000-0000-0000-0000-000000000004"
            ),
            "WZB_WORKSPACE_ENROLLMENT_SECRET_FILE": str(enrollment_file),
            "WZB_WORKSPACE_CONTROL_STATE_DIR": str(tmp_path / "control"),
        }
    )

    assert settings.workspace_events_enabled
    assert settings.workspace_control_enabled
    assert settings.workspace_username == "workspace-zulip-bridge"
    assert settings.workspace_control_state_dir == tmp_path / "control"

    with pytest.raises(ValueError, match="configured together"):
        Settings.from_env(
            {"WZB_WORKSPACE_CONTROL_URL": "https://control.example:21443"}
        )
