import base64
import json
import pathlib
import typing
import uuid

import pyhpke

from workspace_zulip_bridge import canonical, zulip_adapter

ALGORITHM = "HPKE-v1-BASE-X25519-HKDF-SHA256-AES-256-GCM"
SCHEMA = "workspace.external-credential.zulip/v1"
INFO = b"workspace-external-credential-zulip-v1"


def _decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


class CredentialDecryptor:
    def __init__(
        self,
        private_key_file: pathlib.Path,
        realm_uuid: str,
        bridge_instance_uuid: str,
        identity_generation: int,
        credential_key_uuid: str,
    ):
        self.private_key = pyhpke.KEMKey.from_pem(private_key_file.read_bytes())
        self.realm_uuid = str(uuid.UUID(realm_uuid))
        self.bridge_instance_uuid = str(uuid.UUID(bridge_instance_uuid))
        if identity_generation < 1:
            raise ValueError("Identity generation must be positive")
        self.identity_generation = identity_generation
        self.credential_key_uuid = str(uuid.UUID(credential_key_uuid))
        self.suite = pyhpke.CipherSuite.new(
            pyhpke.KEMId.DHKEM_X25519_HKDF_SHA256,
            pyhpke.KDFId.HKDF_SHA256,
            pyhpke.AEADId.AES256_GCM,
        )

    def decrypt(
        self,
        account_uuid: str,
        owner_user_uuid: str,
        account_generation: int,
        envelope: dict[str, object],
    ) -> zulip_adapter.ZulipCredentials:
        if envelope["schema"] != SCHEMA or envelope["algorithm"] != ALGORITHM:
            raise ValueError("Unsupported credential envelope")
        if set(envelope) != {
            "schema",
            "algorithm",
            "associated_data",
            "encapsulated_key",
            "ciphertext",
        }:
            raise ValueError("Invalid credential envelope fields")
        associated_data = typing.cast(dict[str, object], envelope["associated_data"])
        expected = {
            "realm_uuid": self.realm_uuid,
            "provider_kind": "zulip",
            "bridge_instance_uuid": self.bridge_instance_uuid,
            "identity_generation": self.identity_generation,
            "credential_key_uuid": self.credential_key_uuid,
            "account_uuid": str(uuid.UUID(account_uuid)),
            "owner_user_uuid": str(uuid.UUID(owner_user_uuid)),
            "account_generation": account_generation,
            "schema": SCHEMA,
            "algorithm": ALGORITHM,
        }
        if associated_data != expected:
            raise ValueError("Credential associated data mismatch")
        recipient = self.suite.create_recipient_context(
            _decode(str(envelope["encapsulated_key"])),
            self.private_key,
            info=INFO,
        )
        plaintext = recipient.open(
            _decode(str(envelope["ciphertext"])),
            aad=canonical.canonical_json(associated_data),
        )
        value = json.loads(plaintext.decode("utf-8"))
        if set(value) != {"server_url", "email", "api_key"}:
            raise ValueError("Invalid decrypted Zulip credential")
        return zulip_adapter.ZulipCredentials(
            site=value["server_url"],
            email=value["email"],
            api_key=value["api_key"],
        )


def credential_key_uuid(enrollment_request_file: pathlib.Path) -> str:
    request = json.loads(enrollment_request_file.read_text(encoding="utf-8"))
    public_key = typing.cast(dict[str, object], request["encryption_public_key"])
    if public_key.get("algorithm") != "X25519":
        raise ValueError("Unsupported enrollment credential key")
    return str(uuid.UUID(str(public_key["key_uuid"])))
