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
[db]
connection_url = postgresql://bridge
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


def test_legacy_database_dsn_remains_readable_during_config_upgrade(
    tmp_path: pathlib.Path,
):
    source = pathlib.Path(__file__).parents[2] / "etc/bridge.conf.example"
    path = tmp_path / "bridge.conf"
    path.write_text(
        source.read_text(encoding="utf-8")
        .replace("[db]", "[database]", 1)
        .replace(
            "connection_url = postgresql:///workspace_zulip_bridge",
            "dsn = postgresql:///legacy_bridge",
        ),
        encoding="utf-8",
    )

    assert config.load(path).database.connection_url == "postgresql:///legacy_bridge"
