import datetime
import hashlib
import hmac
import json
import pathlib
import ssl

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from workspace_zulip_bridge import config, credentials, enrollment


def _ca_pem() -> bytes:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM)


def _ca(common_name: str = "Test CA"):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def _leaf(runtime, ca_key, ca_certificate, public_key, validity_days=30):
    now = datetime.datetime.now(datetime.UTC)
    return (
        x509.CertificateBuilder()
        .subject_name(x509.Name([]))
        .issuer_name(ca_certificate.subject)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=validity_days))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.UniformResourceIdentifier(
                        enrollment.EnrollmentClient(runtime)._identity_uri()
                    )
                ]
            ),
            False,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), False)
        .sign(ca_key, hashes.SHA256())
    )


def _issuance(runtime, request, ca_key, ca_certificate, trust_bundle):
    csr = x509.load_pem_x509_csr(request["csr_pem"].encode("ascii"))
    certificate = _leaf(
        runtime, ca_key, ca_certificate, csr.public_key(), validity_days=30
    )
    return {
        "request_uuid": request["request_uuid"],
        "identity": {
            "realm_uuid": runtime.identity.realm_uuid,
            "provider_kind": "zulip",
            "bridge_instance_uuid": runtime.identity.bridge_instance_uuid,
            "identity_generation": runtime.identity.identity_generation,
            "uri_san": enrollment.EnrollmentClient(runtime)._identity_uri(),
        },
        "certificate_pem": certificate.public_bytes(serialization.Encoding.PEM).decode(
            "ascii"
        ),
        "trust_bundle_pem": [
            certificate.decode("ascii") for certificate in trust_bundle
        ],
    }


def _runtime(tmp_path: pathlib.Path) -> config.RuntimeConfig:
    secret = tmp_path / "enrollment-secret"
    secret.write_bytes(b"exact-secret")
    password = tmp_path / "password"
    password.write_text("password")
    pki = tmp_path / "pki"
    return config.RuntimeConfig(
        database=config.DatabaseConfig("postgresql:///test"),
        control=config.ControlConfig(
            base_url="https://control.invalid",
            bootstrap_url="http://control.invalid:21085",
            hostname="control.invalid",
            ca_file=pki / "ca.crt",
            certificate_file=pki / "bridge.crt",
            private_key_file=pki / "bridge.key",
            credential_private_key_file=pki / "credential.key",
        ),
        identity=config.IdentityConfig(
            realm_uuid="00000000-0000-0000-0000-000000000001",
            bridge_instance_uuid="00000000-0000-0000-0000-000000000002",
            identity_generation=1,
            enrollment_secret_file=secret,
        ),
        provider_api=config.ProviderApiConfig(
            "https://control.invalid",
            pki / "ca.crt",
            pki / "bridge.crt",
            pki / "bridge.key",
        ),
        file_api=config.FileApiConfig(
            "https://control.invalid",
            pki / "ca.crt",
            pki / "bridge.crt",
            pki / "bridge.key",
        ),
        health_file=tmp_path / "health",
        worker_id="worker",
    )


def test_zb_sec_001_ca_bootstrap_verifies_exact_nonce_bound_hmac(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path)
    ca_pem = _ca_pem()
    nonce = "ab" * 32
    monkeypatch.setattr(enrollment.secrets, "token_hex", lambda count: nonce)

    def handler(request):
        assert request.headers.get_list("Content-Length") == ["0"]
        message = enrollment.ca_hmac_message(
            nonce,
            runtime.control.hostname,
            runtime.identity.bridge_instance_uuid,
            runtime.identity.identity_generation,
            ca_pem,
        )
        signature = hmac.new(
            enrollment.enrollment_verifier(b"exact-secret"),
            message,
            hashlib.sha256,
        ).hexdigest()
        return httpx.Response(
            200,
            content=ca_pem,
            headers={
                "Content-Length": str(len(ca_pem)),
                "X-Workspace-CA-HMAC-SHA256": signature,
            },
        )

    client = httpx.Client(
        base_url=runtime.control.bootstrap_url,
        transport=httpx.MockTransport(handler),
    )
    assert enrollment.EnrollmentClient(runtime, client).bootstrap_ca() == ca_pem
    assert runtime.control.ca_file.read_bytes() == ca_pem
    assert runtime.control.ca_file.stat().st_mode & 0o777 == 0o644


def test_zb_sec_001_invalid_ca_hmac_fails_without_install(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path)
    ca_pem = _ca_pem()
    monkeypatch.setattr(enrollment.secrets, "token_hex", lambda count: "ab" * 32)
    client = httpx.Client(
        base_url=runtime.control.bootstrap_url,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                content=ca_pem,
                headers={
                    "Content-Length": str(len(ca_pem)),
                    "X-Workspace-CA-HMAC-SHA256": "0" * 64,
                },
            )
        ),
    )
    with pytest.raises(ValueError, match="HMAC"):
        enrollment.EnrollmentClient(runtime, client).bootstrap_ca()
    assert not runtime.control.ca_file.exists()


def test_zb_sec_002_enrollment_request_and_private_keys_are_reused(tmp_path):
    runtime = _runtime(tmp_path)
    client = enrollment.EnrollmentClient(runtime)
    first = client._enrollment_request()
    first_tls_key = runtime.control.private_key_file.read_bytes()
    first_credential_key = runtime.control.credential_private_key_file.read_bytes()
    second = client._enrollment_request()
    assert second == first
    assert runtime.control.private_key_file.read_bytes() == first_tls_key
    assert (
        runtime.control.credential_private_key_file.read_bytes() == first_credential_key
    )
    credentials.CredentialDecryptor(
        runtime.control.credential_private_key_file,
        runtime.identity.realm_uuid,
        runtime.identity.bridge_instance_uuid,
        runtime.identity.identity_generation,
        first["encryption_public_key"]["key_uuid"],
    )


def test_zb_sec_003_renewal_rotates_key_and_installs_dual_trust(tmp_path):
    runtime = _runtime(tmp_path)
    client = enrollment.EnrollmentClient(runtime)
    client._enrollment_request()
    old_key = serialization.load_pem_private_key(
        runtime.control.private_key_file.read_bytes(), password=None
    )
    old_ca_key, old_ca = _ca("Old CA")
    old_leaf = _leaf(runtime, old_ca_key, old_ca, old_key.public_key(), validity_days=1)
    runtime.control.certificate_file.write_bytes(
        old_leaf.public_bytes(serialization.Encoding.PEM)
    )
    runtime.control.ca_file.write_bytes(old_ca.public_bytes(serialization.Encoding.PEM))
    old_key_pem = runtime.control.private_key_file.read_bytes()
    new_ca_key, new_ca = _ca("New CA")
    trust_bundle = [
        old_ca.public_bytes(serialization.Encoding.PEM),
        new_ca.public_bytes(serialization.Encoding.PEM),
    ]

    def factory(**kwargs):
        assert isinstance(kwargs["verify"], ssl.SSLContext)
        assert "cert" not in kwargs

        def handler(request):
            payload = json.loads(request.content)
            assert request.url.path == "/v1/certificate-renewals"
            return httpx.Response(
                200,
                json=_issuance(runtime, payload, new_ca_key, new_ca, trust_bundle),
            )

        return httpx.Client(transport=httpx.MockTransport(handler), **kwargs)

    renewal_client = enrollment.EnrollmentClient(
        runtime, enrollment_client_factory=factory
    )
    assert renewal_client.renew_if_needed()
    assert runtime.control.private_key_file.read_bytes() != old_key_pem
    assert runtime.control.ca_file.read_bytes() == b"".join(trust_bundle)
    installed = x509.load_pem_x509_certificate(
        runtime.control.certificate_file.read_bytes()
    )
    assert installed.issuer == new_ca.subject
    assert not (
        runtime.control.private_key_file.parent / "renewal-request.json"
    ).exists()
    assert not (runtime.control.private_key_file.parent / "renewal-next.key").exists()
    assert not (
        runtime.control.private_key_file.parent / "renewal-issuance.json"
    ).exists()


def test_zb_sec_003_persisted_issuance_repairs_interrupted_renewal(tmp_path):
    runtime = _runtime(tmp_path)
    client = enrollment.EnrollmentClient(runtime)
    client._enrollment_request()
    ca_key, ca_certificate = _ca()
    next_key_pem, csr_pem = client._new_tls_key_and_csr()
    request = {
        "request_uuid": "00000000-0000-0000-0000-000000000099",
        "csr_pem": csr_pem,
    }
    issuance = _issuance(
        runtime,
        request,
        ca_key,
        ca_certificate,
        [ca_certificate.public_bytes(serialization.Encoding.PEM)],
    )
    request_path, key_path, issuance_path = client._renewal_paths()
    enrollment._atomic_write(request_path, json.dumps(request).encode("utf-8"), 0o600)
    enrollment._atomic_write(key_path, next_key_pem, 0o600)
    enrollment._atomic_write(issuance_path, json.dumps(issuance).encode("utf-8"), 0o600)

    assert client._recover_pending_renewal()
    assert runtime.control.certificate_file.is_file()
    assert runtime.control.private_key_file.read_bytes() == next_key_pem
    assert not request_path.exists()
    assert not key_path.exists()
    assert not issuance_path.exists()
