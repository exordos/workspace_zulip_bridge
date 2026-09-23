# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from pathlib import Path

import pytest

from workspace_zulip_bridge.config import Settings


def test_minimal_settings_have_no_history_or_sync_knobs() -> None:
    settings = Settings.from_env({})

    assert settings.account_refresh_seconds == 5.0
    assert not hasattr(settings, "zulip_history_concurrency")
    assert not hasattr(settings, "event_processor_batch_size")
    assert not hasattr(settings, "workspace_sync_workers")


def test_workspace_secret_files_can_be_reused(tmp_path: Path) -> None:
    token = tmp_path / "workspace.token"
    token.write_text("header.payload.signature")
    secret = tmp_path / "enrollment.secret"
    secret.write_text("existing-enrollment-secret")

    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws",
            "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
            "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
            "WZB_WORKSPACE_TOKEN_FILE": str(token),
            "WZB_WORKSPACE_CONTROL_URL": "https://control.example:21443",
            "WZB_WORKSPACE_CONTROL_BOOTSTRAP_URL": "http://control.example:21085",
            "WZB_WORKSPACE_CONTROL_HOSTNAME": "control.example",
            "WZB_WORKSPACE_REALM_UUID": "10000000-0000-0000-0000-000000000003",
            "WZB_WORKSPACE_BRIDGE_INSTANCE_UUID": (
                "10000000-0000-0000-0000-000000000004"
            ),
            "WZB_WORKSPACE_ENROLLMENT_SECRET_FILE": str(secret),
            "WZB_WORKSPACE_CONTROL_STATE_DIR": str(tmp_path / "existing-control"),
        }
    )

    assert settings.workspace_token_file == token
    assert settings.workspace_enrollment_secret_file == secret
    assert settings.workspace_control_state_dir == tmp_path / "existing-control"


def test_partial_workspace_socket_configuration_is_rejected() -> None:
    with pytest.raises(ValueError, match="configured together"):
        Settings.from_env(
            {"WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws"}
        )


def test_missing_workspace_ca_is_rejected(tmp_path: Path) -> None:
    token = tmp_path / "workspace.token"
    token.write_text("header.payload.signature")

    with pytest.raises(ValueError, match="WZB_WORKSPACE_CA_FILE"):
        Settings.from_env(
            {
                "WZB_WORKSPACE_WEBSOCKET_URL": "wss://workspace.example/events/ws",
                "WZB_WORKSPACE_PROJECT_ID": ("10000000-0000-0000-0000-000000000001"),
                "WZB_WORKSPACE_PROVIDER_UUID": ("10000000-0000-0000-0000-000000000002"),
                "WZB_WORKSPACE_TOKEN_FILE": str(token),
                "WZB_WORKSPACE_CA_FILE": str(tmp_path / "missing-ca.pem"),
            }
        )
