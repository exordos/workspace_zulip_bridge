# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID


def _read_int(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _read_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = values.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc


def _read_uuid(values: Mapping[str, str], name: str) -> UUID | None:
    raw = values.get(name)
    if not raw:
        return None
    try:
        return UUID(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a UUID") from exc


@dataclass(frozen=True, slots=True)
class Settings:
    database_dsn: str = "postgresql:///workspace_zulip_bridge?host=/var/run/postgresql"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 16
    db_command_timeout_seconds: float = 30.0
    db_probe_seconds: float = 30.0
    account_refresh_seconds: float = 5.0
    zulip_ca_file: Path | None = None
    zulip_connect_timeout_seconds: float = 10.0
    zulip_default_longpoll_timeout_seconds: float = 180.0
    zulip_db_ack_timeout_seconds: float = 120.0
    zulip_retry_base_seconds: float = 1.0
    zulip_retry_cap_seconds: float = 60.0
    zulip_idle_queue_timeout_seconds: int = 3600
    zulip_registration_concurrency: int = 8
    workspace_websocket_url: str | None = None
    workspace_api_url: str | None = None
    workspace_project_id: UUID | None = None
    workspace_provider_uuid: UUID | None = None
    workspace_token_file: Path | None = None
    workspace_refresh_token_file: Path | None = None
    workspace_username: str | None = None
    workspace_password_file: Path | None = None
    workspace_token_url: str | None = None
    workspace_ca_file: Path | None = None
    workspace_event_batch_size: int = 500
    workspace_event_flush_seconds: float = 0.1
    workspace_retry_base_seconds: float = 1.0
    workspace_retry_cap_seconds: float = 60.0
    workspace_request_timeout_seconds: float = 60.0
    workspace_control_url: str | None = None
    workspace_control_bootstrap_url: str | None = None
    workspace_control_hostname: str | None = None
    workspace_realm_uuid: UUID | None = None
    workspace_bridge_instance_uuid: UUID | None = None
    workspace_enrollment_secret_file: Path | None = None
    workspace_control_state_dir: Path = Path("/var/lib/workspace_zulip_bridge/control")
    workspace_control_poll_seconds: float = 2.0
    thread_stop_timeout_seconds: float = 5.0
    log_level: str = "INFO"

    @property
    def zulip_ca_materialization_file(self) -> Path | None:
        if self.zulip_ca_file is not None:
            return self.zulip_ca_file
        if self.workspace_control_url is not None:
            return self.workspace_control_state_dir / "zulip-ca.pem"
        return None

    @property
    def effective_zulip_ca_file(self) -> Path | None:
        path = self.zulip_ca_materialization_file
        return path if path is not None and path.is_file() else None

    @property
    def workspace_events_enabled(self) -> bool:
        return self.workspace_websocket_url is not None

    @property
    def workspace_control_enabled(self) -> bool:
        return self.workspace_control_url is not None

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> "Settings":
        source = os.environ if values is None else values
        settings = cls(
            database_dsn=source.get(
                "WZB_DATABASE_DSN",
                "postgresql:///workspace_zulip_bridge?host=/var/run/postgresql",
            ),
            db_pool_min_size=_read_int(source, "WZB_DB_POOL_MIN_SIZE", 2),
            db_pool_max_size=_read_int(source, "WZB_DB_POOL_MAX_SIZE", 16),
            db_command_timeout_seconds=_read_float(
                source, "WZB_DB_COMMAND_TIMEOUT_SECONDS", 30.0
            ),
            db_probe_seconds=_read_float(source, "WZB_DB_PROBE_SECONDS", 30.0),
            account_refresh_seconds=_read_float(
                source,
                "WZB_ACCOUNT_REFRESH_SECONDS",
                _read_float(source, "WZB_USER_REFRESH_SECONDS", 5.0),
            ),
            zulip_ca_file=(
                Path(source["WZB_ZULIP_CA_FILE"])
                if source.get("WZB_ZULIP_CA_FILE")
                else None
            ),
            zulip_connect_timeout_seconds=_read_float(
                source, "WZB_ZULIP_CONNECT_TIMEOUT_SECONDS", 10.0
            ),
            zulip_default_longpoll_timeout_seconds=_read_float(
                source, "WZB_ZULIP_DEFAULT_LONGPOLL_TIMEOUT_SECONDS", 180.0
            ),
            zulip_db_ack_timeout_seconds=_read_float(
                source, "WZB_ZULIP_DB_ACK_TIMEOUT_SECONDS", 120.0
            ),
            zulip_retry_base_seconds=_read_float(
                source, "WZB_ZULIP_RETRY_BASE_SECONDS", 1.0
            ),
            zulip_retry_cap_seconds=_read_float(
                source, "WZB_ZULIP_RETRY_CAP_SECONDS", 60.0
            ),
            zulip_idle_queue_timeout_seconds=_read_int(
                source, "WZB_ZULIP_IDLE_QUEUE_TIMEOUT_SECONDS", 3600
            ),
            zulip_registration_concurrency=_read_int(
                source, "WZB_ZULIP_REGISTRATION_CONCURRENCY", 8
            ),
            workspace_websocket_url=source.get("WZB_WORKSPACE_WEBSOCKET_URL") or None,
            workspace_api_url=source.get("WZB_WORKSPACE_API_URL") or None,
            workspace_project_id=_read_uuid(source, "WZB_WORKSPACE_PROJECT_ID"),
            workspace_provider_uuid=_read_uuid(source, "WZB_WORKSPACE_PROVIDER_UUID"),
            workspace_token_file=(
                Path(source["WZB_WORKSPACE_TOKEN_FILE"])
                if source.get("WZB_WORKSPACE_TOKEN_FILE")
                else None
            ),
            workspace_refresh_token_file=(
                Path(source["WZB_WORKSPACE_REFRESH_TOKEN_FILE"])
                if source.get("WZB_WORKSPACE_REFRESH_TOKEN_FILE")
                else None
            ),
            workspace_username=source.get("WZB_WORKSPACE_USERNAME") or None,
            workspace_password_file=(
                Path(source["WZB_WORKSPACE_PASSWORD_FILE"])
                if source.get("WZB_WORKSPACE_PASSWORD_FILE")
                else None
            ),
            workspace_token_url=source.get("WZB_WORKSPACE_TOKEN_URL") or None,
            workspace_ca_file=(
                Path(source["WZB_WORKSPACE_CA_FILE"])
                if source.get("WZB_WORKSPACE_CA_FILE")
                else None
            ),
            workspace_event_batch_size=_read_int(
                source, "WZB_WORKSPACE_EVENT_BATCH_SIZE", 500
            ),
            workspace_event_flush_seconds=_read_float(
                source, "WZB_WORKSPACE_EVENT_FLUSH_SECONDS", 0.1
            ),
            workspace_retry_base_seconds=_read_float(
                source, "WZB_WORKSPACE_RETRY_BASE_SECONDS", 1.0
            ),
            workspace_retry_cap_seconds=_read_float(
                source, "WZB_WORKSPACE_RETRY_CAP_SECONDS", 60.0
            ),
            workspace_request_timeout_seconds=_read_float(
                source, "WZB_WORKSPACE_REQUEST_TIMEOUT_SECONDS", 60.0
            ),
            workspace_control_url=source.get("WZB_WORKSPACE_CONTROL_URL") or None,
            workspace_control_bootstrap_url=(
                source.get("WZB_WORKSPACE_CONTROL_BOOTSTRAP_URL") or None
            ),
            workspace_control_hostname=(
                source.get("WZB_WORKSPACE_CONTROL_HOSTNAME") or None
            ),
            workspace_realm_uuid=_read_uuid(source, "WZB_WORKSPACE_REALM_UUID"),
            workspace_bridge_instance_uuid=_read_uuid(
                source, "WZB_WORKSPACE_BRIDGE_INSTANCE_UUID"
            ),
            workspace_enrollment_secret_file=(
                Path(source["WZB_WORKSPACE_ENROLLMENT_SECRET_FILE"])
                if source.get("WZB_WORKSPACE_ENROLLMENT_SECRET_FILE")
                else None
            ),
            workspace_control_state_dir=Path(
                source.get(
                    "WZB_WORKSPACE_CONTROL_STATE_DIR",
                    "/var/lib/workspace_zulip_bridge/control",
                )
            ),
            workspace_control_poll_seconds=_read_float(
                source, "WZB_WORKSPACE_CONTROL_POLL_SECONDS", 2.0
            ),
            thread_stop_timeout_seconds=_read_float(
                source, "WZB_THREAD_STOP_TIMEOUT_SECONDS", 5.0
            ),
            log_level=source.get("WZB_LOG_LEVEL", "INFO").upper(),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        positive = {
            "WZB_DB_POOL_MIN_SIZE": self.db_pool_min_size,
            "WZB_DB_POOL_MAX_SIZE": self.db_pool_max_size,
            "WZB_DB_COMMAND_TIMEOUT_SECONDS": self.db_command_timeout_seconds,
            "WZB_DB_PROBE_SECONDS": self.db_probe_seconds,
            "WZB_ACCOUNT_REFRESH_SECONDS": self.account_refresh_seconds,
            "WZB_ZULIP_CONNECT_TIMEOUT_SECONDS": self.zulip_connect_timeout_seconds,
            "WZB_ZULIP_DEFAULT_LONGPOLL_TIMEOUT_SECONDS": (
                self.zulip_default_longpoll_timeout_seconds
            ),
            "WZB_ZULIP_DB_ACK_TIMEOUT_SECONDS": self.zulip_db_ack_timeout_seconds,
            "WZB_ZULIP_RETRY_BASE_SECONDS": self.zulip_retry_base_seconds,
            "WZB_ZULIP_RETRY_CAP_SECONDS": self.zulip_retry_cap_seconds,
            "WZB_ZULIP_IDLE_QUEUE_TIMEOUT_SECONDS": (
                self.zulip_idle_queue_timeout_seconds
            ),
            "WZB_ZULIP_REGISTRATION_CONCURRENCY": (self.zulip_registration_concurrency),
            "WZB_WORKSPACE_EVENT_BATCH_SIZE": self.workspace_event_batch_size,
            "WZB_WORKSPACE_EVENT_FLUSH_SECONDS": self.workspace_event_flush_seconds,
            "WZB_WORKSPACE_RETRY_BASE_SECONDS": self.workspace_retry_base_seconds,
            "WZB_WORKSPACE_RETRY_CAP_SECONDS": self.workspace_retry_cap_seconds,
            "WZB_WORKSPACE_REQUEST_TIMEOUT_SECONDS": (
                self.workspace_request_timeout_seconds
            ),
            "WZB_WORKSPACE_CONTROL_POLL_SECONDS": self.workspace_control_poll_seconds,
            "WZB_THREAD_STOP_TIMEOUT_SECONDS": self.thread_stop_timeout_seconds,
        }
        for name, value in positive.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if not self.database_dsn:
            raise ValueError("WZB_DATABASE_DSN must not be empty")
        if self.db_pool_max_size < self.db_pool_min_size:
            raise ValueError(
                "WZB_DB_POOL_MAX_SIZE must be at least WZB_DB_POOL_MIN_SIZE"
            )
        if self.zulip_db_ack_timeout_seconds <= self.db_command_timeout_seconds:
            raise ValueError(
                "WZB_ZULIP_DB_ACK_TIMEOUT_SECONDS must be greater than "
                "WZB_DB_COMMAND_TIMEOUT_SECONDS"
            )
        if self.zulip_retry_cap_seconds < self.zulip_retry_base_seconds:
            raise ValueError(
                "WZB_ZULIP_RETRY_CAP_SECONDS must be at least "
                "WZB_ZULIP_RETRY_BASE_SECONDS"
            )
        if self.workspace_retry_cap_seconds < self.workspace_retry_base_seconds:
            raise ValueError(
                "WZB_WORKSPACE_RETRY_CAP_SECONDS must be at least "
                "WZB_WORKSPACE_RETRY_BASE_SECONDS"
            )
        if (
            self.zulip_ca_file is not None
            and not self.zulip_ca_file.is_file()
            and self.workspace_control_url is None
        ):
            raise ValueError("WZB_ZULIP_CA_FILE must name a readable file")
        workspace_required = (
            self.workspace_websocket_url,
            self.workspace_project_id,
            self.workspace_provider_uuid,
            self.workspace_token_file,
        )
        if any(value is not None for value in workspace_required) and not all(
            value is not None for value in workspace_required
        ):
            raise ValueError(
                "Workspace websocket URL, project, provider, and token file "
                "must be configured together"
            )
        if self.workspace_websocket_url is not None:
            if urlsplit(self.workspace_websocket_url).scheme not in {"ws", "wss"}:
                raise ValueError("WZB_WORKSPACE_WEBSOCKET_URL must use ws or wss")
            assert self.workspace_token_file is not None
            password_login = (
                self.workspace_username is not None
                and self.workspace_password_file is not None
            )
            if not self.workspace_token_file.is_file() and not password_login:
                raise ValueError(
                    "WZB_WORKSPACE_TOKEN_FILE must exist unless Workspace "
                    "username and password file are configured"
                )
        if self.workspace_api_url is not None and urlsplit(
            self.workspace_api_url
        ).scheme not in {"http", "https"}:
            raise ValueError("WZB_WORKSPACE_API_URL must use http or https")
        if (self.workspace_username is None) != (self.workspace_password_file is None):
            raise ValueError(
                "WZB_WORKSPACE_USERNAME and WZB_WORKSPACE_PASSWORD_FILE "
                "must be configured together"
            )
        if (
            self.workspace_password_file is not None
            and not self.workspace_password_file.is_file()
        ):
            raise ValueError("WZB_WORKSPACE_PASSWORD_FILE must name a readable file")
        if self.workspace_ca_file is not None and not self.workspace_ca_file.is_file():
            raise ValueError("WZB_WORKSPACE_CA_FILE must name a readable file")
        if (
            self.workspace_refresh_token_file is not None
            and self.workspace_refresh_token_file.exists()
            and not self.workspace_refresh_token_file.is_file()
        ):
            raise ValueError("WZB_WORKSPACE_REFRESH_TOKEN_FILE must be a file")
        if self.workspace_token_url is not None and urlsplit(
            self.workspace_token_url
        ).scheme not in {"http", "https"}:
            raise ValueError("WZB_WORKSPACE_TOKEN_URL must use http or https")
        control_required = (
            self.workspace_control_url,
            self.workspace_control_bootstrap_url,
            self.workspace_control_hostname,
            self.workspace_realm_uuid,
            self.workspace_bridge_instance_uuid,
            self.workspace_enrollment_secret_file,
        )
        if any(value is not None for value in control_required) and not all(
            value is not None for value in control_required
        ):
            raise ValueError("Workspace control settings must be configured together")
        if self.workspace_control_url is not None:
            if urlsplit(self.workspace_control_url).scheme != "https":
                raise ValueError("WZB_WORKSPACE_CONTROL_URL must use https")
            assert self.workspace_control_bootstrap_url is not None
            if urlsplit(self.workspace_control_bootstrap_url).scheme != "http":
                raise ValueError("WZB_WORKSPACE_CONTROL_BOOTSTRAP_URL must use http")
            assert self.workspace_enrollment_secret_file is not None
            if not self.workspace_enrollment_secret_file.is_file():
                raise ValueError(
                    "WZB_WORKSPACE_ENROLLMENT_SECRET_FILE must name a readable file"
                )
