# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import datetime
import hashlib
import json
from pathlib import Path
from uuid import UUID

import httpx
import pyhpke
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric import x25519

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.workspace_control import WorkspaceControlWorker
from workspace_zulip_bridge.workspace_control import _decode
from workspace_zulip_bridge.workspace_control import _encode

REALM_UUID = UUID("10000000-0000-0000-0000-000000000001")
INSTANCE_UUID = UUID("10000000-0000-0000-0000-000000000002")
ACCOUNT_UUID = UUID("10000000-0000-0000-0000-000000000003")
OWNER_UUID = UUID("10000000-0000-0000-0000-000000000004")
PROJECT_UUID = UUID("10000000-0000-0000-0000-000000000005")


def _worker(tmp_path: Path) -> WorkspaceControlWorker:
    secret = tmp_path / "enrollment.secret"
    secret.write_text("enrollment-secret\n")
    settings = Settings(
        database_dsn="postgresql:///test",
        workspace_control_url="https://control.example:21443",
        workspace_control_bootstrap_url="http://control.example:21085",
        workspace_control_hostname="control.example",
        workspace_project_id=PROJECT_UUID,
        workspace_realm_uuid=REALM_UUID,
        workspace_bridge_instance_uuid=INSTANCE_UUID,
        workspace_enrollment_secret_file=secret,
        workspace_control_state_dir=tmp_path / "control",
    )
    return WorkspaceControlWorker(object(), settings)  # type: ignore[arg-type]


def test_control_heartbeat_is_independent_from_desired_state_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(_control_heartbeat_is_independent(tmp_path, monkeypatch))


async def _control_heartbeat_is_independent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(tmp_path)
    sync_attempted = asyncio.Event()
    heartbeat_sent = asyncio.Event()

    async def ensure_enrolled() -> None:
        return None

    async def sync_once() -> None:
        sync_attempted.set()
        raise RuntimeError("desired state unavailable")

    async def heartbeat() -> None:
        heartbeat_sent.set()

    monkeypatch.setattr(worker, "_ensure_enrolled", ensure_enrolled)
    monkeypatch.setattr(worker, "_sync_once", sync_once)
    monkeypatch.setattr(worker, "_heartbeat", heartbeat)

    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(sync_attempted.wait(), timeout=0.1)
    await asyncio.wait_for(heartbeat_sent.wait(), timeout=0.1)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def test_control_rejects_account_for_another_workspace_project(
    tmp_path: Path,
) -> None:
    worker = _worker(tmp_path)
    resource = {
        "uuid": str(ACCOUNT_UUID),
        "generation": 1,
        "owner_user_uuid": str(OWNER_UUID),
        "synchronization_enabled": True,
        "settings": {
            "server_url": "https://zulip.example.test",
            "default_project_id": "20000000-0000-0000-0000-000000000001",
        },
    }

    with pytest.raises(ValueError, match="workspace_project_mismatch"):
        asyncio.run(worker._apply_account(resource))


def test_control_reports_retryable_provider_network_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(_control_reports_retryable_network_failure(tmp_path, monkeypatch))


async def _control_reports_retryable_network_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(tmp_path)
    reports: list[tuple[str, dict[str, object] | None]] = []
    resource = {
        "resource_type": "external_account",
        "uuid": str(ACCOUNT_UUID),
        "generation": 1,
    }

    async def apply_account(candidate: object) -> None:
        assert candidate is resource
        raise httpx.ConnectError("provider unavailable")

    async def report_observed(
        candidate: object,
        status: str,
        *,
        safe_error: dict[str, object] | None = None,
    ) -> None:
        assert candidate is resource
        reports.append((status, safe_error))

    monkeypatch.setattr(worker, "_apply_account", apply_account)
    monkeypatch.setattr(worker, "_report_observed", report_observed)

    assert await worker._apply_resource(resource) is True
    assert reports == [
        (
            "disconnected",
            {
                "code": "provider_unreachable",
                "message": "Zulip is temporarily unavailable.",
                "retryable": True,
            },
        )
    ]


def test_control_certificate_is_renewed_before_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(tmp_path)
    worker._state.mkdir()
    worker._ca.write_text("configured trust bundle")
    worker._load_or_create_request()
    key = serialization.load_pem_private_key(worker._tls_key.read_bytes(), None)
    assert isinstance(key, ec.EllipticCurvePrivateKey)
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([]))
        .issuer_name(x509.Name([]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=6))
        .sign(key, hashes.SHA256())
    )
    worker._certificate.write_bytes(
        certificate.public_bytes(serialization.Encoding.PEM)
    )
    renewed = False

    async def renew() -> None:
        nonlocal renewed
        renewed = True

    monkeypatch.setattr(worker, "_renew_certificate", renew)
    asyncio.run(worker._ensure_enrolled())

    assert renewed is True


def test_control_keeps_syncing_when_valid_certificate_renewal_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = _worker(tmp_path)
    worker._state.mkdir()
    worker._ca.write_text("configured trust bundle")
    worker._load_or_create_request()
    key = serialization.load_pem_private_key(worker._tls_key.read_bytes(), None)
    assert isinstance(key, ec.EllipticCurvePrivateKey)
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([]))
        .issuer_name(x509.Name([]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=6))
        .sign(key, hashes.SHA256())
    )
    worker._certificate.write_bytes(
        certificate.public_bytes(serialization.Encoding.PEM)
    )

    async def renew() -> None:
        raise RuntimeError("renewal endpoint unavailable")

    monkeypatch.setattr(worker, "_renew_certificate", renew)
    asyncio.run(worker._ensure_enrolled())

    assert "certificate renewal deferred: error=RuntimeError" in caplog.text


def test_control_materializes_and_removes_zulip_ca_bundle(tmp_path: Path) -> None:
    worker = _worker(tmp_path)
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([]))
        .issuer_name(x509.Name([]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    pem = certificate.public_bytes(serialization.Encoding.PEM).decode("ascii")
    resource = {
        "provider_kind": "zulip",
        "certificates_pem": [pem],
        "sha256": hashlib.sha256(pem.encode("ascii")).hexdigest(),
    }

    assert worker._settings.effective_zulip_ca_file is None

    worker._apply_zulip_ca(resource)

    ca_file = worker._settings.workspace_control_state_dir / "zulip-ca.pem"
    assert worker._settings.effective_zulip_ca_file == ca_file
    assert ca_file.read_text() == pem

    worker._remove_zulip_ca()

    assert worker._settings.effective_zulip_ca_file is None


def test_control_uses_system_trust_when_zulip_ca_was_not_delivered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(tmp_path)
    captured: dict[str, object] = {}

    class Client:
        def __init__(self, *args: object, **kwargs: object) -> None:
            captured.update(kwargs)

        def get_own_user(self) -> dict[str, int]:
            return {"user_id": 7}

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setattr(
        "workspace_zulip_bridge.workspace_control.ZulipApiClient",
        Client,
    )

    assert worker._read_zulip_identity(
        "https://zulip.example.test",
        "agent@example.test",
        "private-key",
    ) == {"user_id": 7}
    assert captured["ca_file"] is None
    assert captured["closed"] is True


def test_control_credential_envelope_is_recipient_and_resource_bound(
    tmp_path: Path,
) -> None:
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
        worker._decrypt_credentials(
            ACCOUNT_UUID,
            OWNER_UUID,
            7,
            envelope,
        )
        == plaintext
    )
    with pytest.raises(ValueError, match="associated data"):
        worker._decrypt_credentials(
            ACCOUNT_UUID,
            OWNER_UUID,
            8,
            envelope,
        )
