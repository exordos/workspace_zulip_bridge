# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


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


@dataclass(frozen=True, slots=True)
class Settings:
    database_dsn: str
    db_pool_min_size: int = 2
    db_pool_max_size: int = 16
    db_command_timeout_seconds: float = 30.0
    db_probe_seconds: float = 30.0
    user_refresh_seconds: float = 5.0
    zulip_ca_file: Path | None = None
    zulip_connect_timeout_seconds: float = 10.0
    zulip_default_longpoll_timeout_seconds: float = 180.0
    zulip_db_ack_timeout_seconds: float = 120.0
    zulip_retry_base_seconds: float = 1.0
    zulip_retry_cap_seconds: float = 60.0
    zulip_idle_queue_timeout_seconds: int = 3600
    zulip_registration_concurrency: int = 8
    zulip_message_scan_concurrency: int = 32
    zulip_history_concurrency: int = 12
    zulip_directory_cache_ttl_seconds: float = 60.0
    zulip_chat_fill_timeout_seconds: float = 120.0
    zulip_message_page_size: int = 5000
    event_processor_batch_size: int = 1000
    event_processor_poll_seconds: float = 0.05
    event_processor_claim_timeout_seconds: float = 60.0
    event_retention_seconds: float = 86400.0
    event_cleanup_interval_seconds: float = 300.0
    event_cleanup_batch_size: int = 10000
    thread_stop_timeout_seconds: float = 5.0
    log_level: str = "INFO"

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
            user_refresh_seconds=_read_float(source, "WZB_USER_REFRESH_SECONDS", 5.0),
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
            zulip_message_scan_concurrency=_read_int(
                source, "WZB_ZULIP_MESSAGE_SCAN_CONCURRENCY", 32
            ),
            zulip_history_concurrency=_read_int(
                source, "WZB_ZULIP_HISTORY_CONCURRENCY", 12
            ),
            zulip_directory_cache_ttl_seconds=_read_float(
                source, "WZB_ZULIP_DIRECTORY_CACHE_TTL_SECONDS", 60.0
            ),
            zulip_chat_fill_timeout_seconds=_read_float(
                source, "WZB_ZULIP_CHAT_FILL_TIMEOUT_SECONDS", 120.0
            ),
            zulip_message_page_size=_read_int(
                source, "WZB_ZULIP_MESSAGE_PAGE_SIZE", 5000
            ),
            event_processor_batch_size=_read_int(
                source, "WZB_EVENT_PROCESSOR_BATCH_SIZE", 1000
            ),
            event_processor_poll_seconds=_read_float(
                source, "WZB_EVENT_PROCESSOR_POLL_SECONDS", 0.05
            ),
            event_processor_claim_timeout_seconds=_read_float(
                source, "WZB_EVENT_PROCESSOR_CLAIM_TIMEOUT_SECONDS", 60.0
            ),
            event_retention_seconds=_read_float(
                source, "WZB_EVENT_RETENTION_SECONDS", 86400.0
            ),
            event_cleanup_interval_seconds=_read_float(
                source, "WZB_EVENT_CLEANUP_INTERVAL_SECONDS", 300.0
            ),
            event_cleanup_batch_size=_read_int(
                source, "WZB_EVENT_CLEANUP_BATCH_SIZE", 10000
            ),
            thread_stop_timeout_seconds=_read_float(
                source, "WZB_THREAD_STOP_TIMEOUT_SECONDS", 5.0
            ),
            log_level=source.get("WZB_LOG_LEVEL", "INFO").upper(),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not self.database_dsn:
            raise ValueError("WZB_DATABASE_DSN must not be empty")
        if self.db_pool_min_size < 1:
            raise ValueError("WZB_DB_POOL_MIN_SIZE must be positive")
        if self.db_pool_max_size < self.db_pool_min_size:
            raise ValueError(
                "WZB_DB_POOL_MAX_SIZE must be at least WZB_DB_POOL_MIN_SIZE"
            )
        if self.db_command_timeout_seconds <= 0:
            raise ValueError("WZB_DB_COMMAND_TIMEOUT_SECONDS must be positive")
        if self.db_probe_seconds <= 0:
            raise ValueError("WZB_DB_PROBE_SECONDS must be positive")
        positive_float_values = {
            "WZB_USER_REFRESH_SECONDS": self.user_refresh_seconds,
            "WZB_ZULIP_CONNECT_TIMEOUT_SECONDS": (self.zulip_connect_timeout_seconds),
            "WZB_ZULIP_DEFAULT_LONGPOLL_TIMEOUT_SECONDS": (
                self.zulip_default_longpoll_timeout_seconds
            ),
            "WZB_ZULIP_DB_ACK_TIMEOUT_SECONDS": self.zulip_db_ack_timeout_seconds,
            "WZB_ZULIP_RETRY_BASE_SECONDS": self.zulip_retry_base_seconds,
            "WZB_ZULIP_RETRY_CAP_SECONDS": self.zulip_retry_cap_seconds,
            "WZB_ZULIP_DIRECTORY_CACHE_TTL_SECONDS": (
                self.zulip_directory_cache_ttl_seconds
            ),
            "WZB_ZULIP_CHAT_FILL_TIMEOUT_SECONDS": (
                self.zulip_chat_fill_timeout_seconds
            ),
            "WZB_EVENT_PROCESSOR_POLL_SECONDS": self.event_processor_poll_seconds,
            "WZB_EVENT_PROCESSOR_CLAIM_TIMEOUT_SECONDS": (
                self.event_processor_claim_timeout_seconds
            ),
            "WZB_EVENT_RETENTION_SECONDS": self.event_retention_seconds,
            "WZB_EVENT_CLEANUP_INTERVAL_SECONDS": (self.event_cleanup_interval_seconds),
            "WZB_THREAD_STOP_TIMEOUT_SECONDS": self.thread_stop_timeout_seconds,
        }
        for name, value in positive_float_values.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.zulip_retry_cap_seconds < self.zulip_retry_base_seconds:
            raise ValueError(
                "WZB_ZULIP_RETRY_CAP_SECONDS must be at least "
                "WZB_ZULIP_RETRY_BASE_SECONDS"
            )
        if self.zulip_db_ack_timeout_seconds <= self.db_command_timeout_seconds:
            raise ValueError(
                "WZB_ZULIP_DB_ACK_TIMEOUT_SECONDS must be greater than "
                "WZB_DB_COMMAND_TIMEOUT_SECONDS"
            )
        if self.zulip_idle_queue_timeout_seconds < 1:
            raise ValueError("WZB_ZULIP_IDLE_QUEUE_TIMEOUT_SECONDS must be positive")
        if self.zulip_registration_concurrency < 1:
            raise ValueError("WZB_ZULIP_REGISTRATION_CONCURRENCY must be positive")
        if self.zulip_message_scan_concurrency < 1:
            raise ValueError("WZB_ZULIP_MESSAGE_SCAN_CONCURRENCY must be positive")
        if self.zulip_history_concurrency < 1:
            raise ValueError("WZB_ZULIP_HISTORY_CONCURRENCY must be positive")
        if self.zulip_history_concurrency >= self.db_pool_max_size:
            raise ValueError(
                "WZB_ZULIP_HISTORY_CONCURRENCY must be smaller than "
                "WZB_DB_POOL_MAX_SIZE"
            )
        if not 1 <= self.zulip_message_page_size <= 5000:
            raise ValueError("WZB_ZULIP_MESSAGE_PAGE_SIZE must be between 1 and 5000")
        if not 1 <= self.event_processor_batch_size <= 10000:
            raise ValueError(
                "WZB_EVENT_PROCESSOR_BATCH_SIZE must be between 1 and 10000"
            )
        if not 1 <= self.event_cleanup_batch_size <= 100000:
            raise ValueError(
                "WZB_EVENT_CLEANUP_BATCH_SIZE must be between 1 and 100000"
            )
        if self.zulip_ca_file is not None and not self.zulip_ca_file.is_file():
            raise ValueError("WZB_ZULIP_CA_FILE must name a readable file")
