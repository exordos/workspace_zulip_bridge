# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import json
from pathlib import Path
from uuid import UUID

import pyhpke
import pytest
from cryptography.hazmat.primitives.asymmetric import x25519

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.workspace_control import WorkspaceControlWorker
from workspace_zulip_bridge.workspace_control import _decode
from workspace_zulip_bridge.workspace_control import _encode
from workspace_zulip_bridge.zulip_api import ZulipApiError

REALM_UUID = UUID("10000000-0000-0000-0000-000000000001")
INSTANCE_UUID = UUID("10000000-0000-0000-0000-000000000002")
ACCOUNT_UUID = UUID("10000000-0000-0000-0000-000000000003")
OWNER_UUID = UUID("10000000-0000-0000-0000-000000000004")
PROJECT_UUID = UUID("10000000-0000-0000-0000-000000000005")


def _worker(tmp_path: Path) -> WorkspaceControlWorker:
    secret = tmp_path / "enrollment.secret"
    secret.write_text("existing-enrollment-secret")
    return WorkspaceControlWorker(
        object(),  # type: ignore[arg-type]
        Settings(
            database_dsn="postgresql:///test",
            workspace_control_url="https://control.example:21443",
            workspace_control_bootstrap_url="http://control.example:21085",
            workspace_control_hostname="control.example",
            workspace_project_id=PROJECT_UUID,
            workspace_realm_uuid=REALM_UUID,
            workspace_bridge_instance_uuid=INSTANCE_UUID,
            workspace_enrollment_secret_file=secret,
            workspace_control_state_dir=tmp_path / "existing-control",
        ),
    )


def test_existing_enrollment_keys_are_reused(tmp_path: Path) -> None:
    worker = _worker(tmp_path)
    first = worker._load_or_create_request()
    key_before = worker._credential_key.read_bytes()
    request_before = worker._request.read_bytes()

    second = worker._load_or_create_request()

    assert second == first
    assert worker._credential_key.read_bytes() == key_before
    assert worker._request.read_bytes() == request_before


def test_v4_ignores_the_legacy_desired_state_cursor(tmp_path: Path) -> None:
    state = tmp_path / "existing-control"
    state.mkdir()
    (state / "desired-state-cursor").write_text("legacy-cursor")
    worker = _worker(tmp_path)

    assert worker._load_cursor() is None
    assert worker._cursor == state / "desired-state-v4-cursor"


def test_zulip_auth_is_checked_before_account_is_reported_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []

    class FakeClient:
        def __init__(self, endpoint: str, email: str, api_key: str, **kwargs: object):
            calls.append((endpoint, email, api_key, kwargs))

        def check_auth(self) -> None:
            calls.append("checked")

        def close(self) -> None:
            calls.append("closed")

    monkeypatch.setattr(
        "workspace_zulip_bridge.workspace_control.ZulipApiClient",
        FakeClient,
    )
    worker = _worker(tmp_path)

    worker._check_zulip_auth(
        "https://zulip.example.test",
        "agent@example.test",
        "private-key",
    )

    assert calls[1:] == ["checked", "closed"]


def test_zulip_auth_failure_is_reported_to_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(tmp_path)
    reports: list[tuple[str, dict[str, object] | None]] = []

    async def fail_account(_resource: object) -> None:
        raise ZulipApiError(
            "UNAUTHORIZED",
            retryable=False,
            status_code=401,
        )

    async def record_report(
        _resource: object,
        status: str,
        *,
        safe_error: dict[str, object] | None = None,
    ) -> None:
        reports.append((status, safe_error))

    monkeypatch.setattr(worker, "_apply_account", fail_account)
    monkeypatch.setattr(worker, "_report_observed", record_report)

    retry = asyncio.run(
        worker._apply_resource(
            {
                "resource_type": "external_account",
                "synchronization_enabled": True,
            }
        )
    )

    assert retry is False
    assert reports == [
        (
            "auth_required",
            {
                "code": "auth_required",
                "message": "Zulip authentication failed.",
                "retryable": False,
            },
        )
    ]


def test_credential_envelope_remains_resource_bound(tmp_path: Path) -> None:
    worker = _worker(tmp_path)
    request = worker._load_or_create_request()
    recipient = request["encryption_public_key"]
    public_key = pyhpke.KEMKey.from_pyca_cryptography_key(
        x25519.X25519PublicKey.from_public_bytes(_decode(recipient["public_key"]))
    )
    suite = pyhpke.CipherSuite.new(
        pyhpke.KEMId.DHKEM_X25519_HKDF_SHA256,
        pyhpke.KDFId.HKDF_SHA256,
        pyhpke.AEADId.AES256_GCM,
    )
    associated_data = {
        "realm_uuid": str(REALM_UUID),
        "provider_kind": "zulip",
        "bridge_instance_uuid": str(INSTANCE_UUID),
        "identity_generation": 1,
        "credential_key_uuid": recipient["key_uuid"],
        "account_uuid": str(ACCOUNT_UUID),
        "owner_user_uuid": str(OWNER_UUID),
        "account_generation": 7,
        "schema": "workspace.external-credential.zulip/v1",
        "algorithm": "HPKE-v1-BASE-X25519-HKDF-SHA256-AES-256-GCM",
    }
    plaintext = {
        "server_url": "https://zulip.example.test",
        "email": "agent@example.test",
        "api_key": "private-key",
    }
    aad = json.dumps(
        associated_data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    encapsulated, sender = suite.create_sender_context(
        public_key,
        info=b"workspace-external-credential-zulip-v1",
    )
    envelope = {
        "schema": associated_data["schema"],
        "algorithm": associated_data["algorithm"],
        "associated_data": associated_data,
        "encapsulated_key": _encode(encapsulated),
        "ciphertext": _encode(
            sender.seal(json.dumps(plaintext, sort_keys=True).encode(), aad=aad)
        ),
    }

    assert (
        worker._decrypt_credentials(ACCOUNT_UUID, OWNER_UUID, 7, envelope) == plaintext
    )
    with pytest.raises(ValueError, match="associated data"):
        worker._decrypt_credentials(ACCOUNT_UUID, OWNER_UUID, 8, envelope)
