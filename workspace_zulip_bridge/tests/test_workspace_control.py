# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import datetime
import json
from pathlib import Path
from uuid import UUID

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


def _worker(tmp_path: Path) -> WorkspaceControlWorker:
    secret = tmp_path / "enrollment.secret"
    secret.write_text("enrollment-secret\n")
    settings = Settings(
        database_dsn="postgresql:///test",
        workspace_control_url="https://control.example:21443",
        workspace_control_bootstrap_url="http://control.example:21085",
        workspace_control_hostname="control.example",
        workspace_realm_uuid=REALM_UUID,
        workspace_bridge_instance_uuid=INSTANCE_UUID,
        workspace_enrollment_secret_file=secret,
        workspace_control_state_dir=tmp_path / "control",
    )
    return WorkspaceControlWorker(object(), settings)  # type: ignore[arg-type]


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
