# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Rotating Workspace access-token management shared by bridge workers."""

import asyncio
import base64
import json
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

import httpx

from workspace_zulip_bridge.config import Settings


class WorkspaceTokenManager:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        assert settings.workspace_token_file is not None
        self._settings = settings
        self._access_path = settings.workspace_token_file
        self._refresh_path = settings.workspace_refresh_token_file
        self._token_url = settings.workspace_token_url or _token_url(settings)
        self._verify: bool | str = (
            True
            if settings.workspace_ca_file is None
            else str(settings.workspace_ca_file)
        )
        self._timeout = settings.workspace_request_timeout_seconds
        self._transport = transport
        self._lock = asyncio.Lock()
        self._access_token: str | None = None
        self._refresh_token: str | None = None

    async def access_token(self, *, force_refresh: bool = False) -> str:
        async with self._lock:
            if self._access_token is None:
                self._access_token = await asyncio.to_thread(
                    _read_token, self._access_path
                )
            access_token = self._access_token
            if not force_refresh and not _expires_soon(access_token):
                return access_token
            if self._refresh_path is None:
                return access_token
            if self._refresh_token is None:
                self._refresh_token = await asyncio.to_thread(
                    _read_token, self._refresh_path
                )
            refresh_token = self._refresh_token
            response = await self._refresh(refresh_token)
            refreshed_access_token = _response_token(response, "access_token")
            assert refreshed_access_token is not None
            access_token = refreshed_access_token
            next_refresh_token = _response_token(
                response, "refresh_token", required=False
            )
            self._access_token = access_token
            await asyncio.to_thread(_write_token, self._access_path, access_token)
            if next_refresh_token is not None:
                self._refresh_token = next_refresh_token
                await asyncio.to_thread(
                    _write_token,
                    self._refresh_path,
                    next_refresh_token,
                )
            return access_token

    async def _refresh(self, refresh_token: str) -> dict[str, Any]:
        if self._token_url is None:
            raise RuntimeError("Workspace refresh token URL is not configured")
        async with httpx.AsyncClient(
            verify=self._verify,
            timeout=httpx.Timeout(self._timeout),
            transport=self._transport,
        ) as client:
            response = await client.post(
                self._token_url,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "scope": (
                        "openid email profile "
                        f"project:{self._settings.workspace_project_id}"
                    ),
                },
                headers={"Accept": "application/json"},
            )
        if response.is_error:
            error_code = "unknown"
            try:
                payload = response.json()
            except ValueError:
                payload = None
            if isinstance(payload, dict):
                value = payload.get("error")
                if isinstance(value, str) and re.fullmatch(
                    r"[A-Za-z0-9._-]{1,128}", value
                ):
                    error_code = value
            raise RuntimeError(
                "Workspace token refresh returned "
                f"{response.status_code} error={error_code}"
            )
        value = response.json()
        if not isinstance(value, dict):
            raise RuntimeError("Workspace token refresh returned invalid JSON")
        return value


def _token_url(settings: Settings) -> str | None:
    source = settings.workspace_api_url
    if source is None and settings.workspace_websocket_url is not None:
        source = settings.workspace_websocket_url
    if source is None:
        return None
    parsed = urlsplit(source)
    scheme = {"ws": "http", "wss": "https"}.get(parsed.scheme, parsed.scheme)
    return urlunsplit(
        (
            scheme,
            parsed.netloc,
            "/api/core/v1/iam/clients/default/actions/get_token/invoke",
            "",
            "",
        )
    )


def _read_token(path: Path) -> str:
    token = path.read_text(encoding="utf-8").strip()
    if not token or any(character.isspace() for character in token):
        raise ValueError(f"{path.name} must contain one token")
    return token


def _write_token(path: Path, token: str) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(token + "\n", encoding="utf-8")
        temporary.chmod(0o600)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _expires_soon(token: str) -> bool:
    try:
        payload = token.split(".", 2)[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        expires_at = float(claims["exp"])
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False
    return expires_at <= time.time() + 60


def _response_token(
    response: dict[str, Any],
    name: str,
    *,
    required: bool = True,
) -> str | None:
    value = response.get(name)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"Workspace token refresh omitted {name}")
    token = value.strip()
    if any(character.isspace() for character in token):
        raise RuntimeError(f"Workspace token refresh returned invalid {name}")
    return token
