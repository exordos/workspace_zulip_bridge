import argparse
import logging
import pathlib

from workspace_zulip_bridge import (
    config,
    control,
    credentials,
    enrollment,
    file_api,
    provider_api,
    scheduler,
    service,
    storage,
)


def build(
    runtime: config.RuntimeConfig,
    certificate_renewer=None,
) -> service.BridgeService:
    store = storage.RestAlchemyStore(runtime.database.connection_url)
    store.load_confirmed_delivery_state()
    decryptor = credentials.CredentialDecryptor(
        runtime.control.credential_private_key_file,
        runtime.identity.realm_uuid,
        runtime.identity.bridge_instance_uuid,
        runtime.identity.identity_generation,
        credentials.credential_key_uuid(
            runtime.control.credential_private_key_file.parent
            / "enrollment-request.json"
        ),
    )
    file_client = file_api.FileApiClient(runtime.file_api)
    registry = service.AdapterRegistry(store, decryptor, file_client=file_client)
    operation_scheduler = scheduler.Scheduler(store, registry, runtime.worker_id)
    return service.BridgeService(
        store=store,
        control_client=control.ControlClient(runtime.control),
        operation_scheduler=operation_scheduler,
        provider_adapters=registry,
        provider_client=provider_api.ProviderApiClient(runtime.provider_api),
        health_file=runtime.health_file,
        file_client=file_client,
        certificate_renewer=certificate_renewer,
        provider_poll_interval_seconds=(runtime.provider_api.poll_interval_seconds),
        provider_event_long_polling=runtime.provider_api.event_long_polling,
        provider_lease_seconds=runtime.provider_api.lease_seconds,
        provider_batch_size=runtime.provider_api.batch_size,
        control_poll_interval_seconds=runtime.control.poll_interval_seconds,
        heartbeat_interval_seconds=runtime.control.heartbeat_interval_seconds,
        control_retry_base_seconds=runtime.control.retry_base_seconds,
        control_retry_cap_seconds=runtime.control.retry_cap_seconds,
        control_retry_after_cap_seconds=runtime.control.retry_after_cap_seconds,
    )


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("workspace_zulip_bridge").setLevel(logging.INFO)
    # httpx logs the complete request URL at INFO. File transfers use signed
    # URLs, so keep the HTTP stack above INFO even if an embedding runtime has
    # already configured a more verbose root logger.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        default=pathlib.Path("/etc/workspace-zulip-bridge/bridge.conf"),
    )
    arguments = parser.parse_args()
    runtime = config.load(arguments.config)
    enrollment_client = enrollment.EnrollmentClient(runtime)
    enrollment_client.enroll()
    build(runtime, enrollment_client.renew_if_needed).run()
