import configparser
import dataclasses
import pathlib


@dataclasses.dataclass(frozen=True)
class DatabaseConfig:
    connection_url: str


@dataclasses.dataclass(frozen=True)
class ControlConfig:
    base_url: str
    bootstrap_url: str
    hostname: str
    ca_file: pathlib.Path
    certificate_file: pathlib.Path
    private_key_file: pathlib.Path
    credential_private_key_file: pathlib.Path
    poll_interval_seconds: float = 2.0
    heartbeat_interval_seconds: float = 10.0
    retry_base_seconds: float = 1.0
    retry_cap_seconds: float = 30.0
    retry_after_cap_seconds: float = 300.0


@dataclasses.dataclass(frozen=True)
class IdentityConfig:
    realm_uuid: str
    bridge_instance_uuid: str
    identity_generation: int
    enrollment_secret_file: pathlib.Path

    def enrollment_secret(self) -> bytes:
        value = self.enrollment_secret_file.read_bytes()
        value.decode("utf-8")
        return value


@dataclasses.dataclass(frozen=True)
class FileApiConfig:
    base_url: str
    ca_file: pathlib.Path
    certificate_file: pathlib.Path
    private_key_file: pathlib.Path


@dataclasses.dataclass(frozen=True)
class ProviderApiConfig:
    base_url: str
    ca_file: pathlib.Path
    certificate_file: pathlib.Path
    private_key_file: pathlib.Path
    poll_interval_seconds: float = 2.0
    lease_seconds: int = 300
    batch_size: int = 20
    timeout_seconds: float = 30.0
    poll_workers: int = 16


@dataclasses.dataclass(frozen=True)
class RuntimeConfig:
    database: DatabaseConfig
    control: ControlConfig
    identity: IdentityConfig
    provider_api: ProviderApiConfig
    file_api: FileApiConfig
    health_file: pathlib.Path
    worker_id: str


def _path(section: configparser.SectionProxy, key: str) -> pathlib.Path:
    return pathlib.Path(section[key])


def load(path: str | pathlib.Path) -> RuntimeConfig:
    parser = configparser.ConfigParser(interpolation=None)
    read = parser.read(path)
    if not read:
        raise FileNotFoundError(path)
    database = parser["db"] if parser.has_section("db") else parser["database"]
    connection_url = database.get("connection_url") or database["dsn"]
    control = parser["control"]
    identity = parser["identity"]
    provider_api = parser["provider_api"]
    file_api = parser["file_api"]
    service = parser["service"]
    return RuntimeConfig(
        database=DatabaseConfig(connection_url=connection_url),
        control=ControlConfig(
            base_url=control["base_url"].rstrip("/"),
            bootstrap_url=control["bootstrap_url"].rstrip("/"),
            hostname=control["hostname"],
            ca_file=_path(control, "ca_file"),
            certificate_file=_path(control, "certificate_file"),
            private_key_file=_path(control, "private_key_file"),
            credential_private_key_file=_path(control, "credential_private_key_file"),
            poll_interval_seconds=control.getfloat("poll_interval_seconds", 2.0),
            heartbeat_interval_seconds=control.getfloat(
                "heartbeat_interval_seconds", 10.0
            ),
            retry_base_seconds=control.getfloat("retry_base_seconds", 1.0),
            retry_cap_seconds=control.getfloat("retry_cap_seconds", 30.0),
            retry_after_cap_seconds=control.getfloat("retry_after_cap_seconds", 300.0),
        ),
        identity=IdentityConfig(
            realm_uuid=identity["realm_uuid"],
            bridge_instance_uuid=identity["bridge_instance_uuid"],
            identity_generation=identity.getint("identity_generation"),
            enrollment_secret_file=_path(identity, "enrollment_secret_file"),
        ),
        provider_api=ProviderApiConfig(
            base_url=provider_api["base_url"].rstrip("/"),
            ca_file=_path(provider_api, "ca_file"),
            certificate_file=_path(provider_api, "certificate_file"),
            private_key_file=_path(provider_api, "private_key_file"),
            poll_interval_seconds=provider_api.getfloat("poll_interval_seconds", 2.0),
            lease_seconds=provider_api.getint("lease_seconds", 300),
            batch_size=provider_api.getint("batch_size", 20),
            timeout_seconds=provider_api.getfloat("timeout_seconds", 30.0),
            poll_workers=provider_api.getint("poll_workers", 16),
        ),
        file_api=FileApiConfig(
            base_url=file_api["base_url"].rstrip("/"),
            ca_file=_path(file_api, "ca_file"),
            certificate_file=_path(file_api, "certificate_file"),
            private_key_file=_path(file_api, "private_key_file"),
        ),
        health_file=_path(service, "health_file"),
        worker_id=service["worker_id"],
    )
