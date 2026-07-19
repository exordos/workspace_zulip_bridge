import base64
import copy
import pathlib

import pyhpke
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import x25519

from workspace_zulip_bridge import canonical, credentials

ACCOUNT_UUID = "44444444-4444-4444-4444-444444444444"
OWNER_UUID = "55555555-5555-5555-5555-555555555555"
FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _write_private_key(path: pathlib.Path) -> x25519.X25519PrivateKey:
    private_key = x25519.X25519PrivateKey.generate()
    path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    path.chmod(0o600)
    return private_key


def test_private_keys_are_not_stored_as_repository_fixtures():
    assert not list(FIXTURES.glob("*private*.pem"))


@pytest.fixture
def hpke_credential(tmp_path):
    private_key_file = tmp_path / "credential.key"
    private_key = _write_private_key(private_key_file)
    associated_data = {
        "realm_uuid": "11111111-1111-1111-1111-111111111111",
        "provider_kind": "zulip",
        "bridge_instance_uuid": "22222222-2222-2222-2222-222222222222",
        "identity_generation": 3,
        "credential_key_uuid": "33333333-3333-3333-3333-333333333333",
        "account_uuid": ACCOUNT_UUID,
        "owner_user_uuid": OWNER_UUID,
        "account_generation": 7,
        "schema": credentials.SCHEMA,
        "algorithm": credentials.ALGORITHM,
    }
    suite = pyhpke.CipherSuite.new(
        pyhpke.KEMId.DHKEM_X25519_HKDF_SHA256,
        pyhpke.KDFId.HKDF_SHA256,
        pyhpke.AEADId.AES256_GCM,
    )
    recipient_key = pyhpke.KEMKey.from_pyca_cryptography_key(private_key.public_key())
    encapsulated_key, sender = suite.create_sender_context(
        recipient_key,
        info=credentials.INFO,
    )
    plaintext = canonical.canonical_json(
        {
            "server_url": "https://zulip.example.invalid",
            "email": "owner@example.invalid",
            "api_key": "test-api-key",
        }
    )
    envelope = {
        "schema": credentials.SCHEMA,
        "algorithm": credentials.ALGORITHM,
        "associated_data": associated_data,
        "encapsulated_key": _b64url(encapsulated_key),
        "ciphertext": _b64url(
            sender.seal(plaintext, aad=canonical.canonical_json(associated_data))
        ),
    }
    return private_key_file, envelope


def _decryptor(private_key, **overrides):
    values = {
        "realm_uuid": "11111111-1111-1111-1111-111111111111",
        "bridge_instance_uuid": "22222222-2222-2222-2222-222222222222",
        "identity_generation": 3,
        "credential_key_uuid": "33333333-3333-3333-3333-333333333333",
    }
    values.update(overrides)
    return credentials.CredentialDecryptor(private_key, **values)


def test_runtime_hpke_fixture_decrypts_with_canonical_aad(hpke_credential):
    private_key, envelope = hpke_credential
    decrypted = _decryptor(private_key).decrypt(ACCOUNT_UUID, OWNER_UUID, 7, envelope)
    assert decrypted.site == "https://zulip.example.invalid"
    assert decrypted.email == "owner@example.invalid"
    assert decrypted.api_key == "test-api-key"


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("realm_uuid", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        ("bridge_instance_uuid", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        ("identity_generation", 4),
        ("credential_key_uuid", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
    ],
)
def test_wrong_bridge_identity_aad_is_rejected(hpke_credential, field, wrong):
    private_key, envelope = hpke_credential
    with pytest.raises(ValueError, match="associated data"):
        _decryptor(private_key, **{field: wrong}).decrypt(
            ACCOUNT_UUID, OWNER_UUID, 7, envelope
        )


@pytest.mark.parametrize(
    ("account_uuid", "owner_uuid", "generation"),
    [
        ("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", OWNER_UUID, 7),
        (ACCOUNT_UUID, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", 7),
        (ACCOUNT_UUID, OWNER_UUID, 8),
    ],
)
def test_wrong_account_owner_or_generation_is_rejected(
    hpke_credential, account_uuid, owner_uuid, generation
):
    private_key, envelope = hpke_credential
    with pytest.raises(ValueError, match="associated data"):
        _decryptor(private_key).decrypt(account_uuid, owner_uuid, generation, envelope)


def test_wrong_provider_and_wrong_private_key_are_rejected(tmp_path, hpke_credential):
    private_key, envelope = hpke_credential
    wrong_provider = copy.deepcopy(envelope)
    wrong_provider["associated_data"]["provider_kind"] = "other"
    with pytest.raises(ValueError, match="associated data"):
        _decryptor(private_key).decrypt(ACCOUNT_UUID, OWNER_UUID, 7, wrong_provider)

    wrong_key = tmp_path / "wrong.key"
    _write_private_key(wrong_key)
    with pytest.raises(Exception):
        _decryptor(private_key=wrong_key).decrypt(ACCOUNT_UUID, OWNER_UUID, 7, envelope)
