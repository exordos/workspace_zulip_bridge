import argparse
import base64
import datetime
import hashlib
import hmac
import json
import os
import pathlib
import re
import secrets
import ssl
import tempfile
import uuid

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, x25519
from cryptography.x509.oid import ExtendedKeyUsageOID

from workspace_zulip_bridge import config, mtls

MAX_CA_BYTES = 1024 * 1024
ENROLLMENT_CONTEXT = b"workspace-bridge-enrollment-v1\0"
CA_CONTEXT = b"workspace-external-bridge-control-ca-v1\0"
HOSTNAME_RE = re.compile(
    r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)


def _atomic_write(path: pathlib.Path, content: bytes, mode: int) -> None:
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def enrollment_verifier(token: bytes) -> bytes:
    token.decode("utf-8")
    return hashlib.sha256(ENROLLMENT_CONTEXT + token).digest()


def ca_hmac_message(
    nonce: str,
    hostname: str,
    bridge_instance_uuid: str,
    generation: int,
    ca_pem: bytes,
) -> bytes:
    return b"\0".join(
        (
            CA_CONTEXT[:-1],
            nonce.encode("ascii"),
            hostname.encode("utf-8"),
            str(uuid.UUID(bridge_instance_uuid)).encode("ascii"),
            str(generation).encode("ascii"),
            ca_pem,
        )
    )


class EnrollmentClient:
    def __init__(
        self,
        runtime: config.RuntimeConfig,
        bootstrap_client: httpx.Client | None = None,
        enrollment_client_factory=None,
    ):
        self.runtime = runtime
        self.bootstrap_client = bootstrap_client or httpx.Client(
            base_url=runtime.control.bootstrap_url,
            timeout=10.0,
            follow_redirects=False,
        )
        self.enrollment_client_factory = enrollment_client_factory or httpx.Client
        self.pki_dir = runtime.control.private_key_file.parent

    def _token_bytes(self) -> bytes:
        token = self.runtime.identity.enrollment_secret()
        text = token.decode("utf-8")
        if not text or any(character in text for character in "\r\n\0"):
            raise ValueError("Enrollment token is not a valid HTTP header value")
        return token

    def bootstrap_ca(self) -> bytes:
        hostname = self.runtime.control.hostname
        if not HOSTNAME_RE.fullmatch(hostname):
            raise ValueError("Invalid configured control hostname")
        nonce = secrets.token_hex(32)
        generation = self.runtime.identity.identity_generation
        response = self.bootstrap_client.get(
            "/ca.crt",
            headers={"Content-Length": "0"},
            params={
                "nonce": nonce,
                "hostname": hostname,
                "bridge_instance_uuid": self.runtime.identity.bridge_instance_uuid,
                "enrollment_generation": str(generation),
            },
        )
        response.raise_for_status()
        length = response.headers.get("Content-Length")
        if length is None or int(length) != len(response.content):
            raise ValueError("Control CA Content-Length mismatch")
        if not 0 < len(response.content) <= MAX_CA_BYTES:
            raise ValueError("Control CA response size is invalid")
        supplied = response.headers.get("X-Workspace-CA-HMAC-SHA256", "")
        expected = hmac.new(
            enrollment_verifier(self._token_bytes()),
            ca_hmac_message(
                nonce,
                hostname,
                self.runtime.identity.bridge_instance_uuid,
                generation,
                response.content,
            ),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, supplied):
            raise ValueError("Control CA HMAC verification failed")
        ssl.create_default_context(cadata=response.content.decode("ascii"))
        _atomic_write(self.runtime.control.ca_file, response.content, 0o644)
        return response.content

    def _identity_uri(self) -> str:
        return (
            "https://schemas.genesis-corporation.ru/workspace/external-bridge/v1/"
            f"realms/{uuid.UUID(self.runtime.identity.realm_uuid)}/providers/zulip/"
            f"instances/{uuid.UUID(self.runtime.identity.bridge_instance_uuid)}/"
            f"generations/{self.runtime.identity.identity_generation}"
        )

    def _new_tls_key_and_csr(self) -> tuple[bytes, str]:
        tls_key = ec.generate_private_key(ec.SECP256R1())
        tls_key_pem = tls_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(x509.Name([]))
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.UniformResourceIdentifier(self._identity_uri())]
                ),
                False,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), False
            )
            .sign(tls_key, hashes.SHA256())
        )
        return tls_key_pem, csr.public_bytes(serialization.Encoding.PEM).decode("ascii")

    def _create_enrollment_request(self) -> dict[str, object]:
        tls_key_pem, csr_pem = self._new_tls_key_and_csr()
        credential_key = x25519.X25519PrivateKey.generate()
        credential_private_pem = credential_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        credential_public = credential_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        _atomic_write(self.runtime.control.private_key_file, tls_key_pem, 0o600)
        _atomic_write(
            self.runtime.control.credential_private_key_file,
            credential_private_pem,
            0o600,
        )
        request = {
            "request_uuid": str(uuid.uuid4()),
            "enrollment_generation": self.runtime.identity.identity_generation,
            "realm_uuid": str(uuid.UUID(self.runtime.identity.realm_uuid)),
            "provider_kind": "zulip",
            "bridge_instance_uuid": str(
                uuid.UUID(self.runtime.identity.bridge_instance_uuid)
            ),
            "csr_pem": csr_pem,
            "encryption_public_key": {
                "key_uuid": str(uuid.uuid4()),
                "algorithm": "X25519",
                "public_key": base64.urlsafe_b64encode(credential_public)
                .rstrip(b"=")
                .decode("ascii"),
            },
        }
        _atomic_write(
            self.pki_dir / "enrollment-request.json",
            json.dumps(request, sort_keys=True).encode("utf-8"),
            0o600,
        )
        return request

    def _validate_issuance(
        self,
        issuance: dict[str, object],
        request_uuid: str,
        private_key_pem: bytes,
    ) -> tuple[bytes, bytes]:
        if issuance.get("request_uuid") != request_uuid:
            raise ValueError("Certificate response request UUID mismatch")
        expected_identity = {
            "realm_uuid": str(uuid.UUID(self.runtime.identity.realm_uuid)),
            "provider_kind": "zulip",
            "bridge_instance_uuid": str(
                uuid.UUID(self.runtime.identity.bridge_instance_uuid)
            ),
            "identity_generation": self.runtime.identity.identity_generation,
            "uri_san": self._identity_uri(),
        }
        if issuance.get("identity") != expected_identity:
            raise ValueError("Certificate identity mismatch")
        certificate_value = issuance.get("certificate_pem")
        trust_values = issuance.get("trust_bundle_pem")
        if (
            not isinstance(certificate_value, str)
            or not isinstance(trust_values, list)
            or not trust_values
            or not all(isinstance(value, str) for value in trust_values)
        ):
            raise ValueError("Invalid certificate issuance payload")
        certificate_pem = certificate_value.encode("ascii")
        certificate = x509.load_pem_x509_certificate(certificate_pem)
        private_key = serialization.load_pem_private_key(private_key_pem, password=None)
        if (
            certificate.public_key().public_numbers()
            != private_key.public_key().public_numbers()
        ):
            raise ValueError("Certificate key mismatch")
        names = certificate.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value.get_values_for_type(x509.UniformResourceIdentifier)
        if names != [self._identity_uri()]:
            raise ValueError("Certificate URI identity mismatch")
        usages = certificate.extensions.get_extension_for_class(
            x509.ExtendedKeyUsage
        ).value
        if list(usages) != [ExtendedKeyUsageOID.CLIENT_AUTH]:
            raise ValueError("Certificate extended key usage mismatch")
        trust_bundle = "".join(trust_values).encode("ascii")
        ssl.create_default_context(cadata=trust_bundle.decode("ascii"))
        return certificate_pem, trust_bundle

    def _renewal_paths(self) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
        return (
            self.pki_dir / "renewal-request.json",
            self.pki_dir / "renewal-next.key",
            self.pki_dir / "renewal-issuance.json",
        )

    def _install_renewal(
        self,
        request: dict[str, object],
        issuance: dict[str, object],
        next_key_pem: bytes,
    ) -> None:
        certificate_pem, trust_bundle = self._validate_issuance(
            issuance, str(request["request_uuid"]), next_key_pem
        )
        # Persist the dual-trust bundle first. If the process stops between the
        # key and certificate replacements, the persisted issuance repairs the
        # pair before any network client is created on the next start.
        _atomic_write(self.runtime.control.ca_file, trust_bundle, 0o644)
        _atomic_write(self.runtime.control.private_key_file, next_key_pem, 0o600)
        _atomic_write(self.runtime.control.certificate_file, certificate_pem, 0o600)

    def _recover_pending_renewal(self) -> bool:
        request_path, key_path, issuance_path = self._renewal_paths()
        if not issuance_path.exists():
            return False
        if not request_path.is_file() or not key_path.is_file():
            raise ValueError("Incomplete persisted certificate renewal")
        request = json.loads(request_path.read_text(encoding="utf-8"))
        issuance = json.loads(issuance_path.read_text(encoding="utf-8"))
        self._install_renewal(request, issuance, key_path.read_bytes())
        request_path.unlink(missing_ok=True)
        key_path.unlink(missing_ok=True)
        issuance_path.unlink(missing_ok=True)
        return True

    def _validate_current_certificate(self) -> x509.Certificate:
        certificate = x509.load_pem_x509_certificate(
            self.runtime.control.certificate_file.read_bytes()
        )
        private_key = serialization.load_pem_private_key(
            self.runtime.control.private_key_file.read_bytes(), password=None
        )
        if (
            certificate.public_key().public_numbers()
            != private_key.public_key().public_numbers()
        ):
            raise ValueError("Enrollment certificate key mismatch")
        names = certificate.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value.get_values_for_type(x509.UniformResourceIdentifier)
        if names != [self._identity_uri()]:
            raise ValueError("Enrollment certificate identity mismatch")
        return certificate

    def _enrollment_request(self) -> dict[str, object]:
        request_file = self.pki_dir / "enrollment-request.json"
        if not request_file.exists():
            return self._create_enrollment_request()
        if not self.runtime.control.private_key_file.is_file():
            raise ValueError("Persisted enrollment request has no TLS private key")
        if not self.runtime.control.credential_private_key_file.is_file():
            raise ValueError(
                "Persisted enrollment request has no credential private key"
            )
        request = json.loads(request_file.read_text(encoding="utf-8"))
        expected = {
            "enrollment_generation": self.runtime.identity.identity_generation,
            "realm_uuid": str(uuid.UUID(self.runtime.identity.realm_uuid)),
            "provider_kind": "zulip",
            "bridge_instance_uuid": str(
                uuid.UUID(self.runtime.identity.bridge_instance_uuid)
            ),
        }
        if any(request.get(key) != value for key, value in expected.items()):
            raise ValueError("Persisted enrollment request identity mismatch")
        return request

    def enroll(self) -> None:
        self._recover_pending_renewal()
        if self.runtime.control.certificate_file.is_file():
            if not self.runtime.control.private_key_file.is_file():
                raise ValueError("Enrollment certificate has no private key")
            if not self.runtime.control.credential_private_key_file.is_file():
                raise ValueError("Enrollment certificate has no credential key")
            if not self.runtime.control.ca_file.is_file():
                raise ValueError("Enrollment certificate has no trust bundle")
            certificate = self._validate_current_certificate()
            if certificate.not_valid_after_utc <= datetime.datetime.now(datetime.UTC):
                raise ValueError("Enrollment certificate has expired")
            return
        if not self.runtime.control.ca_file.is_file():
            self.bootstrap_ca()
        request = self._enrollment_request()
        request_uuid = str(request["request_uuid"])
        token = self._token_bytes().decode("utf-8")
        with self.enrollment_client_factory(
            base_url=self.runtime.control.base_url,
            verify=str(self.runtime.control.ca_file),
            timeout=10.0,
            follow_redirects=False,
        ) as client:
            response = client.post(
                "/v1/enrollments",
                json=request,
                headers={"X-Workspace-Enrollment-Token": token},
            )
            response.raise_for_status()
            issuance = response.json()
        certificate_pem, trust_bundle = self._validate_issuance(
            issuance,
            request_uuid,
            self.runtime.control.private_key_file.read_bytes(),
        )
        _atomic_write(self.runtime.control.certificate_file, certificate_pem, 0o600)
        _atomic_write(self.runtime.control.ca_file, trust_bundle, 0o644)

    def renew_if_needed(self, force: bool = False) -> bool:
        if self._recover_pending_renewal():
            return True
        self.enroll()
        certificate = self._validate_current_certificate()
        renew_at = certificate.not_valid_after_utc - datetime.timedelta(days=7)
        if not force and datetime.datetime.now(datetime.UTC) < renew_at:
            return False
        request_path, key_path, issuance_path = self._renewal_paths()
        if request_path.exists():
            if not key_path.is_file():
                raise ValueError("Persisted renewal request has no private key")
            request = json.loads(request_path.read_text(encoding="utf-8"))
            next_key_pem = key_path.read_bytes()
        else:
            next_key_pem, csr_pem = self._new_tls_key_and_csr()
            request = {"request_uuid": str(uuid.uuid4()), "csr_pem": csr_pem}
            _atomic_write(key_path, next_key_pem, 0o600)
            _atomic_write(
                request_path,
                json.dumps(request, sort_keys=True).encode("utf-8"),
                0o600,
            )
        with self.enrollment_client_factory(
            base_url=self.runtime.control.base_url,
            verify=mtls.client_context(
                self.runtime.control.ca_file,
                self.runtime.control.certificate_file,
                self.runtime.control.private_key_file,
            ),
            timeout=10.0,
            follow_redirects=False,
        ) as client:
            response = client.post("/v1/certificate-renewals", json=request)
            response.raise_for_status()
            issuance = response.json()
        self._validate_issuance(issuance, str(request["request_uuid"]), next_key_pem)
        _atomic_write(
            issuance_path,
            json.dumps(issuance, sort_keys=True).encode("utf-8"),
            0o600,
        )
        self._install_renewal(request, issuance, next_key_pem)
        request_path.unlink(missing_ok=True)
        key_path.unlink(missing_ok=True)
        issuance_path.unlink(missing_ok=True)
        return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        default=pathlib.Path("/etc/workspace-zulip-bridge/bridge.conf"),
    )
    arguments = parser.parse_args()
    EnrollmentClient(config.load(arguments.config)).enroll()
