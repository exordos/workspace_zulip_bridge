import pathlib

import pytest

from workspace_zulip_bridge import config


def test_enrollment_secret_uses_exact_utf8_bytes(tmp_path: pathlib.Path):
    secret = tmp_path / "secret"
    secret.write_bytes("påss\n".encode())
    settings = config.IdentityConfig(
        realm_uuid="00000000-0000-0000-0000-000000000001",
        bridge_instance_uuid="00000000-0000-0000-0000-000000000002",
        identity_generation=1,
        enrollment_secret_file=secret,
    )
    assert settings.enrollment_secret() == "påss\n".encode()


def test_runtime_config_requires_provider_api_section(tmp_path: pathlib.Path):
    path = tmp_path / "bridge.conf"
    path.write_text(
        """
[database]
dsn = postgresql://bridge
[control]
base_url = https://backend/control
bootstrap_url = https://backend/bootstrap
hostname = backend
ca_file = /run/control/ca.pem
certificate_file = /run/control/client.pem
private_key_file = /run/control/client.key
credential_private_key_file = /run/control/credential.key
[identity]
realm_uuid = 00000000-0000-0000-0000-000000000001
bridge_instance_uuid = 00000000-0000-0000-0000-000000000002
identity_generation = 1
enrollment_secret_file = /run/secrets/enrollment
[file_api]
base_url = https://backend/files
ca_file = /run/file/ca.pem
certificate_file = /run/file/client.pem
private_key_file = /run/file/client.key
[service]
health_file = /run/health
worker_id = test
""",
        encoding="utf-8",
    )

    with pytest.raises(KeyError, match="provider_api"):
        config.load(path)


def test_example_config_sets_bounded_provider_poll_workers():
    path = pathlib.Path(__file__).parents[2] / "etc/bridge.conf.example"

    assert config.load(path).provider_api.poll_workers == 16
