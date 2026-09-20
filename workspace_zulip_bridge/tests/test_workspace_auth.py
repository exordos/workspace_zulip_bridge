# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
from pathlib import Path

import httpx
import pytest

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.workspace_auth import WorkspaceTokenManager
from workspace_zulip_bridge.workspace_sync import workspace_api_url


def test_workspace_api_url_routes_provider_calls_to_messenger_service(
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "workspace.token"
    token_file.write_text("access-token")
    common = {
        "WZB_WORKSPACE_WEBSOCKET_URL": (
            "wss://workspace.example/api/workspace/v1/events/ws"
        ),
        "WZB_WORKSPACE_PROJECT_ID": "10000000-0000-0000-0000-000000000001",
        "WZB_WORKSPACE_PROVIDER_UUID": "10000000-0000-0000-0000-000000000002",
        "WZB_WORKSPACE_TOKEN_FILE": str(token_file),
    }
    assert workspace_api_url(Settings.from_env(common)) == (
        "https://workspace.example/api/workspace/v1/messenger"
    )
    assert (
        workspace_api_url(
            Settings.from_env(
                {
                    **common,
                    "WZB_WORKSPACE_API_URL": (
                        "https://workspace.example/api/workspace/v1"
                    ),
                }
            )
        )
        == "https://workspace.example/api/workspace/v1/messenger"
    )


def test_expired_access_token_is_refreshed_and_rotation_is_persisted(
    tmp_path: Path,
) -> None:
    asyncio.run(_refresh_round_trip(tmp_path))


async def _refresh_round_trip(tmp_path: Path) -> None:
    access_file = tmp_path / "workspace.token"
    refresh_file = tmp_path / "workspace.refresh-token"
    access_file.write_text("header.eyJleHAiOjB9.signature")
    refresh_file.write_text("old-refresh")
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "access_token": "fresh-access",
                "refresh_token": "fresh-refresh",
            },
        )

    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "wss://workspace.example/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_API_URL": "https://workspace.example/api/workspace/v1",
            "WZB_WORKSPACE_PROJECT_ID": ("10000000-0000-0000-0000-000000000001"),
            "WZB_WORKSPACE_PROVIDER_UUID": ("10000000-0000-0000-0000-000000000002"),
            "WZB_WORKSPACE_TOKEN_FILE": str(access_file),
            "WZB_WORKSPACE_REFRESH_TOKEN_FILE": str(refresh_file),
        }
    )
    manager = WorkspaceTokenManager(
        settings,
        transport=httpx.MockTransport(handler),
    )

    assert await manager.access_token() == "fresh-access"
    assert await manager.access_token() == "fresh-access"
    assert access_file.read_text() == "fresh-access\n"
    assert refresh_file.read_text() == "fresh-refresh\n"
    assert len(requests) == 1
    assert requests[0].url == (
        "https://workspace.example/api/core/v1/iam/clients/"
        "default/actions/get_token/invoke"
    )
    assert requests[0].content.decode() == (
        "grant_type=refresh_token&refresh_token=old-refresh&"
        "scope=openid+email+profile+project%3A"
        "10000000-0000-0000-0000-000000000001"
    )


def test_refresh_failure_exposes_only_validated_error_code(tmp_path: Path) -> None:
    asyncio.run(_refresh_failure_exposes_only_validated_error_code(tmp_path))


async def _refresh_failure_exposes_only_validated_error_code(
    tmp_path: Path,
) -> None:
    access_file = tmp_path / "workspace.token"
    refresh_file = tmp_path / "workspace.refresh-token"
    access_file.write_text("header.eyJleHAiOjB9.signature")
    refresh_file.write_text("secret-refresh-token")

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            json={
                "error": "invalid_grant",
                "error_description": "secret-refresh-token was rejected",
            },
        )

    settings = Settings.from_env(
        {
            "WZB_WORKSPACE_WEBSOCKET_URL": (
                "wss://workspace.example/api/workspace/v1/events/ws"
            ),
            "WZB_WORKSPACE_PROJECT_ID": ("10000000-0000-0000-0000-000000000001"),
            "WZB_WORKSPACE_PROVIDER_UUID": ("10000000-0000-0000-0000-000000000002"),
            "WZB_WORKSPACE_TOKEN_FILE": str(access_file),
            "WZB_WORKSPACE_REFRESH_TOKEN_FILE": str(refresh_file),
        }
    )
    manager = WorkspaceTokenManager(
        settings,
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(
        RuntimeError,
        match=r"^Workspace token refresh returned 401 error=invalid_grant$",
    ) as error:
        await manager.access_token()

    assert "secret-refresh-token" not in str(error.value)
