import concurrent.futures
import dataclasses
import datetime
import hashlib
import pathlib
import random
import ssl
import tempfile
import time
import typing
import uuid

import certifi
import httpx

from workspace_zulip_bridge import (
    canonical,
    control,
    converter,
    credentials,
    file_api,
    provider_api,
    provider_protocol,
    scheduler,
    storage,
    zulip_adapter,
)


class AdapterRegistry:
    def __init__(
        self,
        store: storage.RestAlchemyStore,
        decryptor: credentials.CredentialDecryptor,
        custom_ca_dir: pathlib.Path = pathlib.Path(
            "/run/workspace-zulip-bridge/provider-ca"
        ),
        file_client: file_api.FileApiClient | None = None,
    ):
        self.store = store
        self.decryptor = decryptor
        self.file_client = file_client
        self.custom_ca_dir = custom_ca_dir

    def _cert_bundle(self) -> str | None:
        resource = self.store.custom_ca_bundle("zulip")
        if resource is None:
            return None
        certificates = resource.get("certificates_pem")
        if (
            not isinstance(certificates, list)
            or not certificates
            or not all(isinstance(value, str) for value in certificates)
        ):
            raise zulip_adapter.ZulipOperationError("invalid_custom_ca_bundle", False)
        custom_pem = "".join(typing.cast(list[str], certificates))
        try:
            ssl.create_default_context(cadata=custom_pem)
        except ssl.SSLError as exc:
            raise zulip_adapter.ZulipOperationError(
                "invalid_custom_ca_bundle", False
            ) from exc
        digest = hashlib.sha256(custom_pem.encode("ascii")).hexdigest()
        self.custom_ca_dir.mkdir(mode=0o750, parents=True, exist_ok=True)
        target = self.custom_ca_dir / f"zulip-{digest}.pem"
        if not target.is_file():
            system_bundle = pathlib.Path(certifi.where()).read_bytes()
            content = system_bundle.rstrip() + b"\n" + custom_pem.encode("ascii")
            descriptor, temporary = tempfile.mkstemp(
                prefix=".zulip-ca-", dir=self.custom_ca_dir
            )
            try:
                with open(descriptor, "wb", closefd=True) as stream:
                    stream.write(content)
                    stream.flush()
                pathlib.Path(temporary).chmod(0o644)
                pathlib.Path(temporary).replace(target)
            except BaseException:
                pathlib.Path(temporary).unlink(missing_ok=True)
                raise
        for stale in self.custom_ca_dir.glob("zulip-*.pem"):
            if stale != target:
                stale.unlink(missing_ok=True)
        return str(target)

    def __call__(self, account_uuid: str) -> zulip_adapter.OfficialZulipAdapter:
        if not self.store.provider_is_enabled("zulip"):
            raise zulip_adapter.ZulipOperationError("provider_suspended", True)
        resource = self.store.desired_resource("external_account", account_uuid)
        if resource is None or not resource["synchronization_enabled"]:
            raise zulip_adapter.ZulipOperationError("unauthorized_account", False)
        try:
            generation = int(resource["generation"])
            if generation < 1:
                raise ValueError("Account generation must be positive")
            envelope = typing.cast(dict[str, object], resource["credential_envelope"])
            associated_data = typing.cast(
                dict[str, object], envelope["associated_data"]
            )
            credential_generation = associated_data["account_generation"]
            if (
                isinstance(credential_generation, bool)
                or not isinstance(credential_generation, int)
                or credential_generation < 1
                or credential_generation > generation
            ):
                raise ValueError("Invalid credential account generation")
            account_credentials = self.decryptor.decrypt(
                account_uuid,
                str(resource["owner_user_uuid"]),
                credential_generation,
                envelope,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise zulip_adapter.ZulipOperationError(
                "unauthorized_account", False
            ) from exc
        account_credentials = dataclasses.replace(
            account_credentials,
            cert_bundle=self._cert_bundle(),
        )
        return zulip_adapter.OfficialZulipAdapter(
            account_credentials,
            routing=_AccountRouting(self.store, account_uuid),
            owner_user_uuid=str(resource["owner_user_uuid"]),
            account_uuid=account_uuid,
            file_client=self.file_client,
            file_limit=lambda: self.store.effective_file_limit(file_api.MAX_FILE_BYTES),
        )


class _AccountRouting:
    def __init__(self, store: storage.RestAlchemyStore, account_uuid: str):
        self.store = store
        self.account_uuid = account_uuid

    def provider_mapping(
        self, entity_kind: str, provider_id: str
    ) -> dict[str, object] | None:
        return self.store.provider_mapping(self.account_uuid, entity_kind, provider_id)

    def workspace_mapping(
        self, entity_kind: str, workspace_uuid: str
    ) -> dict[str, object] | None:
        return self.store.workspace_mapping(
            self.account_uuid, entity_kind, workspace_uuid
        )

    def topic_message_mapping(self, topic_uuid: str) -> dict[str, object] | None:
        return self.store.topic_message_mapping(self.account_uuid, topic_uuid)

    def workspace_message_mappings_through(
        self, stream_uuid: str, topic_uuid: str | None, through_workspace_uuid: str
    ) -> list[dict[str, object]]:
        return self.store.workspace_message_mappings_through(
            self.account_uuid, stream_uuid, topic_uuid, through_workspace_uuid
        )

    def external_chat_uuid(self, provider_chat_key: str) -> str:
        assignment = self.store.assignment_for_provider_chat(
            self.account_uuid, provider_chat_key
        )
        if assignment is not None:
            return str(assignment["uuid"])
        return converter.stable_entity_uuid(
            self.account_uuid, "external_chat", provider_chat_key
        )


class BridgeService:
    MAX_QUEUE_CATCHUP_PAGES = 20
    MAX_CONTROL_SNAPSHOT_PAGES = 10_000
    MAX_CONTROL_SNAPSHOT_RESOURCES = 2_000_000
    PROVIDER_POLL_INTERVAL_SECONDS = 2.0
    HISTORY_QUANTUM_INTERVAL_SECONDS = 1.0

    def __init__(
        self,
        store: storage.RestAlchemyStore,
        control_client: control.ControlClient,
        operation_scheduler: scheduler.Scheduler,
        provider_adapters: AdapterRegistry,
        provider_client: provider_api.ProviderApiClient,
        health_file: pathlib.Path,
        file_client: file_api.FileApiClient | None = None,
        certificate_renewer: typing.Callable[[bool], bool] | None = None,
        control_poll_interval_seconds: float = 2.0,
        heartbeat_interval_seconds: float = 10.0,
        control_retry_base_seconds: float = 1.0,
        control_retry_cap_seconds: float = 30.0,
        control_retry_after_cap_seconds: float = 300.0,
        provider_poll_interval_seconds: float = 2.0,
        provider_lease_seconds: int = 300,
        provider_batch_size: int = 20,
        provider_poll_workers: int = 16,
    ):
        if provider_client is None:
            raise ValueError("Provider API client is required")
        if not 1 <= provider_poll_workers <= 64:
            raise ValueError("Provider poll worker count must be between 1 and 64")
        self.store = store
        self.control = control_client
        self.scheduler = operation_scheduler
        self.provider_adapters = provider_adapters
        self.health_file = health_file
        self.file_client = file_client
        self.provider_api = provider_client
        self.certificate_renewer = certificate_renewer
        self.control_poll_interval_seconds = control_poll_interval_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.control_retry_base_seconds = control_retry_base_seconds
        self.control_retry_cap_seconds = control_retry_cap_seconds
        self.control_retry_after_cap_seconds = control_retry_after_cap_seconds
        self.provider_poll_interval_seconds = provider_poll_interval_seconds
        self.provider_lease_seconds = provider_lease_seconds
        self.provider_batch_size = provider_batch_size
        self.provider_poll_workers = provider_poll_workers
        self.provider_lease_request_uuid: uuid.UUID | None = None
        self.last_control = 0.0
        self.last_heartbeat = 0.0
        self.last_certificate_check = 0.0
        self.last_provider_poll = 0.0
        self.last_history_quantum = time.monotonic()
        self.provider_retry_attempts: dict[str, int] = {}
        self.provider_retry_after: dict[str, float] = {}
        self.provider_random = random.Random()
        self.control_retry_attempts = 0
        self.control_retry_after = 0.0
        self.heartbeat_retry_attempts = 0
        self.heartbeat_retry_after = 0.0
        self.control_random = random.Random()
        self.control_lane_health: dict[str, bool | None] = {
            "heartbeat": None,
            "control": None,
            "desired": None,
        }
        self.control_lane_errors: dict[str, str | None] = {
            "heartbeat": None,
            "control": None,
            "desired": None,
        }

    def synchronize_control(self) -> bool:
        cursor = self.store.control_cursor()
        if not cursor:
            self._install_control_snapshot()
            return True
        try:
            batch = self.control.desired_changes(cursor)
        except control.ControlCursorExpired:
            self._install_control_snapshot()
            return True
        try:
            self.store.apply_desired_changes(
                typing.cast(list[dict[str, object]], batch["changes"]),
                str(batch["next_cursor"]),
            )
        except (KeyError, TypeError, ValueError):
            self.store.set_blocked_batch(
                cursor,
                str(batch.get("next_cursor", cursor)),
                "unsupported_desired_batch",
            )
            self.store.mark_health("control", "degraded", "unsupported_desired_batch")
            return False
        self.store.clear_blocked_batch()
        return True

    def _install_control_snapshot(self) -> None:
        """Fetch every page before atomically replacing the desired state."""
        session = self.control.create_snapshot()
        token = str(session["snapshot_token"])
        resources: list[dict[str, object]] = []
        page_cursor = None
        seen_page_cursors: set[str] = set()
        page_count = 0
        while True:
            if page_count >= self.MAX_CONTROL_SNAPSHOT_PAGES:
                raise ValueError("Control snapshot page limit exceeded")
            page = self.control.snapshot_page(token, page_cursor)
            page_count += 1
            page_resources = typing.cast(list[dict[str, object]], page["resources"])
            if (
                len(resources) + len(page_resources)
                > self.MAX_CONTROL_SNAPSHOT_RESOURCES
            ):
                raise ValueError("Control snapshot resource limit exceeded")
            resources.extend(page_resources)
            next_page_cursor = page["next_page_cursor"]
            if next_page_cursor is None:
                break
            page_cursor = str(next_page_cursor)
            if page_cursor in seen_page_cursors:
                raise ValueError("Control snapshot page cursor repeated")
            seen_page_cursors.add(page_cursor)
        self.store.install_snapshot(resources, str(session["anchor_cursor"]))

    def heartbeat(self) -> None:
        blocked_batch = (
            self.store.blocked_batch() if hasattr(self.store, "blocked_batch") else None
        )
        response = self.control.heartbeat(blocked_batch)
        migration = response.get("ca_migration")
        if isinstance(migration, dict) and migration.get("renewal_required") is True:
            self._renew_certificate(True)

    def _renew_certificate(self, force: bool) -> bool:
        if self.certificate_renewer is None:
            return False
        try:
            renewed = self.certificate_renewer(force)
        except (httpx.HTTPError, OSError, RuntimeError, ValueError):
            self.store.mark_health(
                "certificate", "degraded", "certificate_renewal_failed"
            )
            return False
        if not renewed:
            return False
        self.control.reload_tls()
        self.provider_api.reload_tls()
        if self.file_client is not None:
            self.file_client.reload_tls()
        self.store.mark_health("certificate", "healthy")
        return True

    def _defer_control_call(
        self,
        lane: str,
        now: float,
        retry_after_seconds: float | None,
    ) -> None:
        attempts_name = f"{lane}_retry_attempts"
        retry_after_name = f"{lane}_retry_after"
        attempts = getattr(self, attempts_name, 0) + 1
        setattr(self, attempts_name, attempts)
        base = getattr(self, "control_retry_base_seconds", 1.0)
        cap = getattr(self, "control_retry_cap_seconds", 30.0)
        ceiling = min(cap, base * (2 ** min(attempts - 1, 30)))
        delay = getattr(self, "control_random", random).uniform(0.0, ceiling)
        if retry_after_seconds is not None:
            retry_after_cap = getattr(self, "control_retry_after_cap_seconds", 300.0)
            delay = max(delay, min(retry_after_seconds, retry_after_cap))
        setattr(self, retry_after_name, now + delay)

    def _clear_control_retry(self, lane: str) -> None:
        setattr(self, f"{lane}_retry_attempts", 0)
        setattr(self, f"{lane}_retry_after", 0.0)

    def _set_control_lane_health(
        self,
        lane: str,
        healthy: bool,
        error_code: str = "control_transport_unavailable",
    ) -> None:
        lanes = getattr(self, "control_lane_health", None)
        if lanes is None:
            lanes = {"heartbeat": None, "control": None, "desired": None}
            self.control_lane_health = lanes
        errors = getattr(self, "control_lane_errors", None)
        if errors is None:
            errors = {"heartbeat": None, "control": None, "desired": None}
            self.control_lane_errors = errors
        lanes[lane] = healthy
        errors[lane] = None if healthy else error_code
        if False in lanes.values():
            aggregate_error = (
                errors.get("desired")
                or errors.get("control")
                or errors.get("heartbeat")
            )
            self.store.mark_health("control", "degraded", aggregate_error)
        elif all(value is True for value in lanes.values()):
            self.store.mark_health("control", "healthy")

    @staticmethod
    def _retry_after_seconds(exc: BaseException) -> float | None:
        if isinstance(exc, control.ControlRetryableError):
            return exc.retry_after_seconds
        return None

    def _run_heartbeat(self, now: float) -> bool:
        if now < getattr(self, "heartbeat_retry_after", 0.0):
            return False
        interval = getattr(self, "heartbeat_interval_seconds", 10.0)
        if now - self.last_heartbeat < interval:
            return False
        try:
            self.heartbeat()
        except (httpx.TransportError, control.ControlRetryableError) as exc:
            self._defer_control_call("heartbeat", now, self._retry_after_seconds(exc))
            self._set_control_lane_health("heartbeat", False)
            return False
        self._clear_control_retry("heartbeat")
        self._set_control_lane_health("heartbeat", True)
        self.last_heartbeat = now
        return True

    def _run_control_poll(self, now: float) -> bool:
        if now < getattr(self, "control_retry_after", 0.0):
            return False
        interval = getattr(self, "control_poll_interval_seconds", 2.0)
        if now - self.last_control < interval:
            return False
        try:
            synchronized = self.synchronize_control()
        except (httpx.TransportError, control.ControlRetryableError) as exc:
            self._defer_control_call("control", now, self._retry_after_seconds(exc))
            self._set_control_lane_health("control", False)
            return False
        self._clear_control_retry("control")
        if synchronized is False:
            self._set_control_lane_health("desired", False, "unsupported_desired_batch")
        else:
            self._set_control_lane_health("desired", True)
        self._set_control_lane_health("control", True)
        self.last_control = now
        return True

    def _flush_observed_reports(self, now: float) -> int:
        if now < getattr(self, "control_retry_after", 0.0):
            return 0
        try:
            sent = self.flush_observed_reports()
        except (httpx.TransportError, control.ControlRetryableError) as exc:
            self._defer_control_call("control", now, self._retry_after_seconds(exc))
            self._set_control_lane_health("control", False)
            return 0
        if sent:
            self._clear_control_retry("control")
            self._set_control_lane_health("control", True)
        return sent

    def poll_provider_operations(self) -> int:
        """Lease Workspace-to-Zulip operations from the private HTTP data plane."""
        request_uuid = self.provider_lease_request_uuid or uuid.uuid4()
        self.provider_lease_request_uuid = request_uuid
        response = self.provider_api.lease_operations(
            request_uuid,
            limit=self.provider_batch_size,
            lease_seconds=self.provider_lease_seconds,
        )
        operations = typing.cast(list[dict[str, object]], response["operations"])
        processed = 0
        immediate_results: list[dict[str, object]] = []
        for leased in operations:
            try:
                record = provider_protocol.leased_operation_record(self.store, leased)
                rebound = (
                    self.store.bind_provider_lease(record)
                    if hasattr(self.store, "bind_provider_lease")
                    else False
                )
                processed += int(rebound or self.store.enqueue(record, 0))
            except (KeyError, TypeError, ValueError):
                immediate_results.append(
                    {
                        "result_uuid": str(
                            uuid.uuid5(
                                converter.OPERATION_NAMESPACE,
                                f"provider-rejected:{leased['provider_operation_uuid']}:{leased['lease_uuid']}",
                            )
                        ),
                        "provider_operation_uuid": str(
                            leased["provider_operation_uuid"]
                        ),
                        "lease_uuid": str(leased["lease_uuid"]),
                        "status": "failed",
                        "safe_error": "unsupported_operation",
                    }
                )
        if immediate_results:
            self.provider_api.report_results(immediate_results)
            processed += len(immediate_results)
        self.provider_lease_request_uuid = None
        self.store.mark_health("provider_api", "healthy")
        return processed

    def flush_provider_results(self) -> int:
        records = self.store.pending_results(100)
        if not records:
            return 0
        payloads = [provider_protocol.result_payload(record) for record in records]
        response = self.provider_api.report_results(payloads)
        results = typing.cast(list[dict[str, object]], response["results"])
        expected = [str(payload["result_uuid"]) for payload in payloads]
        actual = [str(result["result_uuid"]) for result in results]
        if actual != expected:
            raise ValueError("Provider result response does not match request order")
        sent = 0
        for record, result in zip(records, results, strict=True):
            status = str(result["status"])
            self.store.finalize_provider_result_response(
                str(record["record_uuid"]), status
            )
            if status in {"applied", "duplicate"}:
                sent += 1
        return sent

    def _poll_provider_account(
        self, account_uuid: str
    ) -> tuple[int, zulip_adapter.ZulipOperationError | None]:
        """Poll one account with an adapter owned only by this worker call."""
        adapter = None
        try:
            adapter = self.provider_adapters(account_uuid)
            cursor = self.store.provider_event_cursor(account_uuid)
            if cursor is None:
                queue_id, last_event_id = adapter.ensure_queue()
                # Persist the queue before catalog, participant, or history work.
                # A restart can then resume the same queue instead of opening a
                # gap while the initial synchronization is still in progress.
                self.store.update_provider_event_cursor(
                    account_uuid, queue_id, last_event_id
                )
                registration = adapter.take_registration_snapshot()
                if registration is not None:
                    self._queue_registration_reports(
                        account_uuid,
                        registration,
                        getattr(adapter, "server_url", ""),
                    )
            else:
                queue_id = str(cursor["queue_id"])
                last_event_id = int(cursor["last_event_id"])
                adapter.restore_queue(queue_id, last_event_id)
            catchup_ready = self._run_provider_queue_catchup(account_uuid, adapter)
            if not catchup_ready:
                self._queue_account_report(account_uuid, "backfill")
                return 0, None
            events = adapter.events(queue_id, last_event_id)
            initial_sync_ready = self._initial_sync_ready(account_uuid)
            self._queue_account_report(
                account_uuid,
                "live_ready" if initial_sync_ready else "backfill",
            )
            if initial_sync_ready:
                self._queue_ready_assignment_reports(account_uuid)
        except zulip_adapter.ZulipOperationError as exc:
            if exc.code == "bad_event_queue_id":
                self.store.begin_provider_queue_catchup(account_uuid)
                self.store.invalidate_provider_event_cursor(account_uuid)
                if adapter is not None:
                    adapter.invalidate_queue()
            return 0, exc
        processed = 0
        for event in events:
            event_id = int(event["id"])
            self.store.record_provider_event(account_uuid, queue_id, event)
            local_id = event.get("local_message_id")
            message = event.get("message")
            if local_id is not None and isinstance(message, dict):
                provider_message_id = message.get("id")
                if provider_message_id is not None:
                    self.scheduler.reconcile_local_echo(
                        account_uuid,
                        queue_id,
                        str(local_id),
                        str(provider_message_id),
                    )
            self.store.update_provider_event_cursor(account_uuid, queue_id, event_id)
            processed += 1
        return processed, None

    def poll_provider_events(self) -> int:
        now = time.monotonic()
        active_accounts = self.store.active_account_uuids()
        accounts = [
            account_uuid
            for account_uuid in active_accounts
            if self.provider_retry_after.get(account_uuid, 0.0) <= now
        ]
        if not accounts:
            return 0
        workers = min(getattr(self, "provider_poll_workers", 16), len(accounts))
        processed = 0
        failed = len(accounts) < len(active_accounts)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._poll_provider_account, account_uuid): account_uuid
                for account_uuid in accounts
            }
            for future in concurrent.futures.as_completed(futures):
                account_uuid = futures[future]
                account_processed, error = future.result()
                if error is None:
                    self._clear_provider_retry(account_uuid)
                    processed += account_processed
                    continue
                failed = True
                self._defer_provider_account(account_uuid, now)
                self.store.mark_health("provider", "degraded", error.code)
                self._queue_account_report(
                    account_uuid,
                    "degraded",
                    error.code,
                )
        if not failed:
            self.store.mark_health("provider", "healthy")
        return processed

    @staticmethod
    def _observed_report_uuid(report: dict[str, object]) -> str:
        semantic = {key: value for key, value in report.items() if key != "report_uuid"}
        digest = hashlib.sha256(canonical.canonical_json(semantic)).hexdigest()
        return str(uuid.uuid5(converter.OPERATION_NAMESPACE, f"observed:{digest}"))

    def _queue_observed_report(
        self,
        resource_type: str,
        resource_uuid: str,
        generation: int,
        status: str,
        phase: str,
        catalog: dict[str, object] | None = None,
        safe_error_code: str | None = None,
    ) -> None:
        observed_at = (
            datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
        )
        report: dict[str, object] = {
            "report_uuid": "",
            "resource_type": resource_type,
            "resource_uuid": str(uuid.UUID(resource_uuid)),
            "observed_generation": generation,
            "status": status,
            "progress": {
                "phase": phase,
                "completed": 1,
                "total": 1,
                "last_progress_at": observed_at,
            },
            "safe_error": (
                None
                if safe_error_code is None
                else {
                    "code": safe_error_code,
                    "message": "The provider history synchronization failed.",
                }
            ),
            "observed_at": observed_at,
        }
        if catalog is not None:
            report["catalog"] = catalog
        report["report_uuid"] = self._observed_report_uuid(report)
        self.store.enqueue_observed_report(report)

    def _queue_account_report(
        self,
        account_uuid: str,
        status: str,
        safe_error_code: str | None = None,
    ) -> None:
        account = self.store.account_resource(account_uuid)
        if account is None:
            return
        self._queue_observed_report(
            "external_account",
            account_uuid,
            int(account["generation"]),
            status,
            (
                "live"
                if status == "live_ready"
                else "retry"
                if status == "degraded"
                else "backfill"
            ),
            safe_error_code=safe_error_code,
        )

    def _queue_ready_assignment_reports(self, account_uuid: str) -> None:
        for assignment in self.store.assignments_needing_live_report(account_uuid):
            self._queue_observed_report(
                "external_chat_assignment",
                str(assignment["uuid"]),
                int(assignment["generation"]),
                "live_ready",
                "live",
            )

    def _queue_registration_reports(
        self,
        account_uuid: str,
        registration: dict[str, object],
        server_url: str,
    ) -> None:
        account = self.store.account_resource(account_uuid)
        if account is None:
            return
        settings = typing.cast(dict[str, object], account["settings"])
        owner_uuid = str(account["owner_user_uuid"])
        generation = int(account["generation"])
        project_uuid = str(settings["default_project_id"])
        provider_user_id = registration.get("user_id")
        if isinstance(provider_user_id, int):
            owner_name = next(
                (
                    str(person.get("full_name", provider_user_id))
                    for person in typing.cast(
                        list[dict[str, object]], registration.get("realm_users", [])
                    )
                    if person.get("user_id") == provider_user_id
                ),
                str(provider_user_id),
            )
            self.store.remember_provider_mapping(
                account_uuid,
                "identity",
                str(provider_user_id),
                owner_uuid,
                {
                    "display_name": owner_name,
                    "email": settings.get("email"),
                    "avatar_urn": None,
                    "active": True,
                },
            )
        people = {
            int(person["user_id"]): person
            for person in typing.cast(
                list[dict[str, object]], registration.get("realm_users", [])
            )
            if isinstance(person.get("user_id"), int)
        }
        catalog: dict[str, tuple[str, str, list[dict[str, object]]]] = {}
        for subscription in typing.cast(
            list[dict[str, object]], registration.get("subscriptions", [])
        ):
            stream_id = subscription.get("stream_id")
            name = subscription.get("name")
            if isinstance(stream_id, int) and isinstance(name, str) and name:
                chat_key = f"channel:{stream_id}"
                assignment_lookup = getattr(
                    self.store, "assignment_for_provider_chat", None
                )
                assignment = (
                    assignment_lookup(account_uuid, chat_key)
                    if assignment_lookup is not None
                    else None
                )
                subscribers = subscription.get("subscribers")
                participant_ids: set[int] = set()
                if assignment is not None and bool(assignment.get("selected", True)):
                    participant_ids.update(
                        value
                        for value in (
                            typing.cast(list[object], subscribers)
                            if isinstance(subscribers, list)
                            else []
                        )
                        if isinstance(value, int)
                    )
                    if isinstance(provider_user_id, int):
                        participant_ids.add(provider_user_id)
                channel_participants = [
                    self._catalog_participant(
                        people.get(value, {"user_id": value}),
                        value == provider_user_id,
                    )
                    for value in sorted(participant_ids)
                ]
                catalog[chat_key] = (
                    "channel",
                    name,
                    channel_participants,
                )
        for conversation in typing.cast(
            list[dict[str, object]],
            registration.get("recent_private_conversations", []),
        ):
            user_ids = conversation.get("user_ids")
            if not isinstance(user_ids, list) or not all(
                isinstance(value, int) for value in user_ids
            ):
                continue
            participants = set(typing.cast(list[int], user_ids))
            if isinstance(provider_user_id, int):
                participants.add(provider_user_id)
            if len(participants) < 2:
                continue
            ordered = sorted(participants)
            chat_type = "direct" if len(ordered) == 2 else "group_direct"
            chat_key = f"{chat_type}:{','.join(map(str, ordered))}"
            peer_names = [
                str(people.get(value, {}).get("full_name", value))
                for value in ordered
                if value != provider_user_id
            ]
            participants = [
                self._catalog_participant(
                    people.get(value, {"user_id": value}),
                    value == provider_user_id,
                )
                for value in ordered
            ]
            catalog[chat_key] = (chat_type, ", ".join(peer_names), participants)
        for chat_key, (chat_type, display_name, participants) in catalog.items():
            topics = (
                [
                    {
                        "provider_topic_id": f"{chat_key}:default",
                        "name": "default",
                        "is_default": True,
                    }
                ]
                if chat_type in {"direct", "group_direct"}
                else []
            )
            self._queue_catalog_report(
                account_uuid,
                owner_uuid,
                project_uuid,
                generation,
                chat_key,
                chat_type,
                display_name,
                server_url,
                participants=participants,
                topics=topics,
                authoritative_participants=True,
            )

    @staticmethod
    def _projection_participant_ids(
        assignment: dict[str, object],
    ) -> set[int]:
        projection = assignment.get("workspace_projection")
        if not isinstance(projection, dict):
            return set()
        participants = projection.get("participants")
        if not isinstance(participants, list):
            return set()
        result: set[int] = set()
        for participant in participants:
            if not isinstance(participant, dict):
                continue
            try:
                result.add(int(str(participant["provider_user_id"])))
            except (KeyError, TypeError, ValueError):
                continue
        return result

    def _assignment_participants_ready(
        self,
        account_uuid: str,
        chat_key: str,
        assignment: dict[str, object],
    ) -> bool:
        checker = getattr(self.store, "assignment_participants_ready", None)
        if checker is None:
            return True
        return bool(
            checker(account_uuid, chat_key, int(assignment["generation"]))
        )

    def refresh_selected_participants_once(self) -> bool:
        job = self.store.claim_participant_sync()
        if job is None:
            return False
        account_uuid = str(job["account_uuid"])
        chat_key = str(job["provider_chat_key"])
        generation = int(job["assignment_generation"])
        assignment = self.store.assignment_for_provider_chat(account_uuid, chat_key)
        if (
            assignment is None
            or int(assignment["generation"]) != generation
            or not bool(assignment.get("selected", True))
            or self.store.provider_event_cursor(account_uuid) is None
        ):
            self.store.release_participant_sync(
                account_uuid, chat_key, generation
            )
            return False
        try:
            adapter = self.provider_adapters(account_uuid)
            registration = adapter.channel_catalog(chat_key)
            self._queue_registration_reports(
                account_uuid,
                registration,
                getattr(adapter, "server_url", ""),
            )
        except zulip_adapter.ZulipOperationError as exc:
            self.store.release_participant_sync(
                account_uuid, chat_key, generation
            )
            self.store.mark_health("provider", "degraded", exc.code)
            self._queue_account_report(account_uuid, "degraded", exc.code)
            return False
        subscription = typing.cast(
            list[dict[str, object]], registration["subscriptions"]
        )[0]
        provider_user_ids = {
            int(value)
            for value in typing.cast(list[object], subscription["subscribers"])
        }
        provider_user_ids.add(int(registration["user_id"]))
        ready = provider_user_ids == self._projection_participant_ids(assignment)
        self.store.complete_participant_sync(
            account_uuid,
            chat_key,
            generation,
            sorted(provider_user_ids),
            ready,
        )
        return True

    @staticmethod
    def _catalog_participant(
        person: dict[str, object], is_owner: bool
    ) -> dict[str, object]:
        provider_user_id = person.get("user_id", person.get("id"))
        if not isinstance(provider_user_id, int):
            raise ValueError("Invalid Zulip catalog participant")
        return {
            "provider_user_id": str(provider_user_id),
            "display_name": str(person.get("full_name", provider_user_id)),
            "email": person.get("email"),
            "avatar_urn": None,
            "is_owner": is_owner,
        }

    def _queue_catalog_report(
        self,
        account_uuid: str,
        owner_uuid: str,
        project_uuid: str,
        generation: int,
        chat_key: str,
        chat_type: str,
        display_name: str,
        server_url: str,
        operation: str = "upsert",
        participants: list[dict[str, object]] | None = None,
        topics: list[dict[str, object]] | None = None,
        authoritative_participants: bool = False,
    ) -> None:
        if operation == "upsert" and hasattr(self.store, "merge_catalog_topology"):
            participants, topics = self.store.merge_catalog_topology(
                account_uuid,
                chat_key,
                participants or [],
                topics or [],
                authoritative_participants=authoritative_participants,
            )
        elif operation == "delete" and hasattr(self.store, "delete_catalog_topology"):
            self.store.delete_catalog_topology(account_uuid, chat_key)
        common_capabilities = {
            "messenger.chat_catalog",
            "messenger.message.send",
            "messenger.message.edit",
            "messenger.message.delete",
            "messenger.message.read",
            "messenger.file.transfer",
        }
        if chat_type == "channel":
            common_capabilities.update(
                {"messenger.stream.rename", "messenger.topic.rename"}
            )
        capabilities = {
            name: {"available": True, **control.CAPABILITIES[name]}
            for name in sorted(common_capabilities)
        }
        external_chat_uuid = converter.stable_entity_uuid(
            account_uuid, "external_chat", chat_key
        )
        self._queue_observed_report(
            "external_chat_catalog",
            external_chat_uuid,
            generation,
            "ready" if operation == "upsert" else "deleted",
            "discovery",
            {
                "operation": operation,
                "external_account_uuid": account_uuid,
                "owner_user_uuid": owner_uuid,
                "provider_kind": "zulip",
                "project_id": project_uuid,
                "source": {
                    "kind": "zulip",
                    "chat_type": chat_type,
                    "provider_chat_key": chat_key,
                    "original_url": self._catalog_original_url(server_url, chat_key),
                },
                "display_name": display_name,
                "description": "",
                "participants": participants or [],
                "topics": topics or [],
                "capabilities": capabilities,
            },
        )

    @staticmethod
    def _catalog_original_url(server_url: str, chat_key: str) -> str | None:
        provider_site = server_url.rstrip("/")
        if not provider_site:
            return None
        chat_type, _, identifiers = chat_key.partition(":")
        if chat_type == "channel":
            return f"{provider_site}/#narrow/channel/{identifiers}"
        if chat_type == "direct":
            return f"{provider_site}/#narrow/dm/{identifiers}-dm"
        if chat_type == "group_direct":
            return f"{provider_site}/#narrow/dm/{identifiers}-group"
        return provider_site

    def _queue_event_catalog(
        self, account_uuid: str, event: dict[str, object], server_url: str
    ) -> None:
        account = self.store.account_resource(account_uuid)
        if account is None:
            return
        settings = typing.cast(dict[str, object], account["settings"])
        common = (
            str(account["owner_user_uuid"]),
            str(settings["default_project_id"]),
            int(account["generation"]),
        )
        event_type = event.get("type")
        if event_type == "message":
            message = typing.cast(dict[str, object], event["message"])
            chat_type, chat_key = converter.provider_chat_reference(message)
            recipient = message.get("display_recipient")
            participants: list[dict[str, object]] = []
            topics: list[dict[str, object]] = []
            if isinstance(recipient, str):
                display_name = recipient
                subject = message.get("subject")
                stream_id = message.get("stream_id")
                if isinstance(stream_id, int) and isinstance(subject, str) and subject:
                    topics.append(
                        {
                            "provider_topic_id": f"{stream_id}:{subject}",
                            "name": subject,
                            "is_default": False,
                        }
                    )
            elif isinstance(recipient, list):
                display_name = ", ".join(
                    str(person.get("full_name", person.get("email", "User")))
                    for person in recipient
                    if isinstance(person, dict)
                )
                participants = [
                    self._catalog_participant(
                        typing.cast(dict[str, object], person),
                        bool(typing.cast(dict[str, object], person).get("is_me")),
                    )
                    for person in recipient
                    if isinstance(person, dict) and isinstance(person.get("id"), int)
                ]
            else:
                return
            if display_name:
                self._queue_catalog_report(
                    account_uuid,
                    *common,
                    chat_key,
                    chat_type,
                    display_name,
                    server_url,
                    participants=participants,
                    topics=(
                        topics
                        if chat_type == "channel"
                        else [
                            {
                                "provider_topic_id": f"{chat_key}:default",
                                "name": "default",
                                "is_default": True,
                            }
                        ]
                    ),
                )
            return
        if event_type != "subscription":
            return
        operation = str(event.get("op"))
        subscriptions: list[dict[str, object]] = []
        if operation in {"add", "remove"}:
            subscriptions = typing.cast(
                list[dict[str, object]], event.get("subscriptions", [])
            )
        elif operation == "update" and event.get("property") == "name":
            subscriptions = [
                {"stream_id": event.get("stream_id"), "name": event.get("value")}
            ]
        for subscription in subscriptions:
            stream_id = subscription.get("stream_id")
            display_name = subscription.get("name")
            if not isinstance(stream_id, int) or not isinstance(display_name, str):
                continue
            self._queue_catalog_report(
                account_uuid,
                *common,
                f"channel:{stream_id}",
                "channel",
                display_name,
                server_url,
                "delete" if operation == "remove" else "upsert",
            )

    def _initial_sync_ready(self, account_uuid: str) -> bool:
        account = self.store.account_resource(account_uuid)
        if account is None:
            return False
        generation = int(account["generation"])
        return (
            self.store.provider_catchup_ready(account_uuid)
            and self.store.catalog_reports_accepted(account_uuid, generation)
            and self.store.catalog_assignments_ready(account_uuid, generation)
            and self.store.initial_backfill_ready(account_uuid)
        )

    def flush_observed_reports(self) -> int:
        reports = self.store.pending_observed_reports(500)
        if not reports:
            return 0
        response = self.control.observed_reports(reports)
        results = typing.cast(list[dict[str, object]], response["results"])
        expected = [str(report["report_uuid"]) for report in reports]
        actual = [str(result["report_uuid"]) for result in results]
        if actual != expected:
            raise ValueError("Observed report results do not match request order")
        self.store.apply_observed_report_results(results)
        return len(results)

    def _defer_provider_account(self, account_uuid: str, now: float) -> None:
        attempts = self.provider_retry_attempts.get(account_uuid, 0) + 1
        self.provider_retry_attempts[account_uuid] = attempts
        ceiling = min(300.0, float(2 ** min(attempts - 1, 8)))
        self.provider_retry_after[account_uuid] = now + self.provider_random.uniform(
            0.0, ceiling
        )

    def _clear_provider_retry(self, account_uuid: str) -> None:
        self.provider_retry_attempts.pop(account_uuid, None)
        self.provider_retry_after.pop(account_uuid, None)

    def _run_provider_queue_catchup(
        self,
        account_uuid: str,
        adapter: zulip_adapter.OfficialZulipAdapter,
    ) -> bool:
        """Reconcile one bounded newest-first page before enabling live events."""
        if not hasattr(self.store, "pending_provider_catchup"):
            return True
        job = self.store.pending_provider_catchup(account_uuid)
        if job is None:
            return self.store.provider_catchup_ready(account_uuid)
        chat_key = str(job["provider_chat_key"])
        page_count = int(job["page_count"])
        if page_count >= self.MAX_QUEUE_CATCHUP_PAGES:
            self.store.advance_provider_catchup(
                account_uuid,
                chat_key,
                [],
                None,
                False,
                "provider_queue_catchup_limit_exceeded",
            )
            return False
        anchor = "newest" if job["next_anchor"] is None else int(job["next_anchor"])
        messages = adapter.message_history(chat_key, anchor=anchor)
        checkpoint = (
            None
            if job["checkpoint_provider_message_id"] is None
            else int(job["checkpoint_provider_message_id"])
        )
        prior_seen = {int(value) for value in job["seen_provider_message_ids"]}
        page_ids = {int(message["id"]) for message in messages}
        seen_ids = prior_seen | page_ids
        reached_checkpoint = checkpoint is None or any(
            message_id <= checkpoint for message_id in page_ids
        )
        complete = reached_checkpoint or len(messages) < 100

        unmapped_messages = []
        for message in converter.newest_first(messages):
            provider_message_id = str(message["id"])
            mapping = self.store.provider_mapping(
                account_uuid, "message", provider_message_id
            )
            if mapping is None:
                unmapped_messages.append(message)
                continue
            metadata = typing.cast(dict[str, object], mapping["metadata"])
            provider_content_sha256 = hashlib.sha256(
                str(message["content"]).encode("utf-8")
            ).hexdigest()
            current_subject = str(message.get("subject", ""))
            if (
                metadata.get("provider_content_sha256") == provider_content_sha256
                and metadata.get("subject", "") == current_subject
            ):
                continue
            event = {
                "id": int(message["id"]),
                "type": "update_message",
                "message_id": int(message["id"]),
                "message_ids": [int(message["id"])],
                "content": message["content"],
                "edit_timestamp": message.get(
                    "last_edit_timestamp", message["timestamp"]
                ),
                "stream_id": message.get("stream_id"),
                "orig_subject": metadata.get("subject", current_subject),
                "subject": current_subject,
            }
            records = self._event_records_with_file_fallback(
                adapter,
                account_uuid,
                converter.stable_entity_uuid(
                    account_uuid,
                    "external_chat",
                    str(metadata["chat_key"]),
                ),
                f"catchup:{chat_key}",
                event,
                "backfill",
            )
            for record in records:
                self.store.enqueue_workspace_delivery(record, 2)

        if unmapped_messages:
            try:
                self.enqueue_backfill(account_uuid, chat_key, unmapped_messages)
            except ValueError as exc:
                if str(exc) != "provider_chat_assignment_pending":
                    raise
                # Queue recovery can overlap the Workspace control-plane work
                # that creates stream/topic mappings for a newly selected chat.
                # Leave the catch-up checkpoint untouched and retry after those
                # mappings have arrived.
                return False

        if complete and checkpoint is not None:
            known = self.store.mapped_provider_messages(
                account_uuid, chat_key, checkpoint
            )
            for mapping in known:
                provider_message_id = int(mapping["provider_id"])
                if provider_message_id in seen_ids:
                    continue
                delete_event = {
                    "id": provider_message_id,
                    "type": "delete_message",
                    "message_ids": [provider_message_id],
                }
                for record in converter.event_records(
                    self.store,
                    account_uuid,
                    f"catchup:{chat_key}",
                    delete_event,
                    "backfill",
                    adapter.server_url,
                ):
                    self.store.enqueue_workspace_delivery(record, 2)

        next_anchor = (
            None
            if not messages
            else min(int(message["id"]) for message in messages) - 1
        )
        self.store.advance_provider_catchup(
            account_uuid,
            chat_key,
            sorted(page_ids),
            next_anchor,
            complete,
        )
        return complete and self.store.provider_catchup_ready(account_uuid)

    def _file_resolver(
        self,
        adapter: zulip_adapter.OfficialZulipAdapter,
        account_uuid: str,
        external_chat_uuid: str,
        event_id: int,
    ) -> converter.FileResolver | None:
        if self.file_client is None:
            return None

        def resolve(provider_url: str, display_name: str) -> str:
            max_bytes = self.store.effective_file_limit(file_api.MAX_FILE_BYTES)
            downloaded = adapter.download_file(provider_url, max_bytes=max_bytes)
            incoming_uuid = uuid.uuid5(
                converter.ENTITY_NAMESPACE,
                f"zulip-file:{account_uuid}:{event_id}:{provider_url}",
            )
            transfer_operation_uuid = uuid.uuid5(
                converter.OPERATION_NAMESPACE,
                f"zulip-file-import:{account_uuid}:{event_id}:{provider_url}",
            )
            try:
                return self.file_client.import_file(
                    transfer_operation_uuid,
                    uuid.UUID(account_uuid),
                    uuid.UUID(external_chat_uuid),
                    file_api.IncomingFile(
                        incoming_uuid,
                        display_name or downloaded.name,
                        downloaded.content_type,
                        downloaded.content,
                    ),
                    max_bytes=max_bytes,
                )
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                retryable = status in {408, 425, 429} or status >= 500
                raise zulip_adapter.ZulipOperationError(
                    "workspace_file_import_unavailable", retryable
                ) from exc
            except httpx.TransportError as exc:
                raise zulip_adapter.ZulipOperationError(
                    "workspace_file_import_unavailable", True
                ) from exc

        return resolve

    def _event_records_with_file_fallback(
        self,
        adapter: zulip_adapter.OfficialZulipAdapter,
        account_uuid: str,
        external_chat_uuid: str,
        queue_id: str,
        event: dict[str, object],
        delivery_class: str,
    ) -> list[dict[str, object]]:
        try:
            return converter.event_records(
                self.store,
                account_uuid,
                queue_id,
                event,
                delivery_class,
                adapter.server_url,
                self._file_resolver(
                    adapter,
                    account_uuid,
                    external_chat_uuid,
                    int(event["id"]),
                ),
            )
        except zulip_adapter.ZulipOperationError as exc:
            if exc.retryable:
                raise
            return converter.event_records(
                self.store,
                account_uuid,
                queue_id,
                event,
                delivery_class,
                adapter.server_url,
                None,
            )

    def process_provider_journal(self) -> int:
        processed = 0
        if hasattr(self.store, "mark_interrupted_workspace_deliveries_ambiguous"):
            self.store.mark_interrupted_workspace_deliveries_ambiguous()
        if hasattr(self.store, "reset_stale_workspace_deliveries"):
            self.store.reset_stale_workspace_deliveries()
        supported_types = {
            "message",
            "update_message",
            "delete_message",
            "update_message_flags",
            "subscription",
            "realm_user",
        }
        for row in self.store.pending_provider_events():
            account_uuid = str(row["account_uuid"])
            queue_id = str(row["queue_id"])
            event_id = int(row["event_id"])
            event = typing.cast(dict[str, object], row["body"])
            if not self.store.account_is_active(account_uuid):
                continue
            try:
                adapter = self.provider_adapters(account_uuid)
            except zulip_adapter.ZulipOperationError as exc:
                self.store.mark_health("provider", "degraded", exc.code)
                continue
            supported = str(event["type"]) in supported_types
            try:
                self._queue_event_catalog(account_uuid, event, adapter.server_url)
                external_chat_uuid = uuid.UUID(int=0)
                if event["type"] == "message":
                    message = typing.cast(dict[str, object], event["message"])
                    _, chat_key = converter.provider_chat_reference(message)
                    assignment_lookup = getattr(
                        self.store, "assignment_for_provider_chat", None
                    )
                    assignment = (
                        assignment_lookup(account_uuid, chat_key)
                        if assignment_lookup is not None
                        else None
                    )
                    if assignment is not None and not self._assignment_participants_ready(
                        account_uuid, chat_key, assignment
                    ):
                        raise ValueError("provider_chat_participants_pending")
                    external_chat_uuid = uuid.UUID(
                        converter.stable_entity_uuid(
                            account_uuid, "external_chat", chat_key
                        )
                    )
                elif event["type"] in {
                    "update_message",
                    "delete_message",
                    "update_message_flags",
                }:
                    message_ids = event.get("message_ids", event.get("messages"))
                    if message_ids is None and event.get("message_id") is not None:
                        message_ids = [event["message_id"]]
                    first_message_id = next(
                        iter(typing.cast(list[object], message_ids or [])), None
                    )
                    if first_message_id is not None:
                        mapping = self.store.provider_mapping(
                            account_uuid, "message", str(first_message_id)
                        )
                        if mapping is not None:
                            metadata = typing.cast(
                                dict[str, object], mapping["metadata"]
                            )
                            chat_key = str(metadata["chat_key"])
                            external_chat_uuid = uuid.UUID(
                                _AccountRouting(
                                    self.store, account_uuid
                                ).external_chat_uuid(chat_key)
                            )
                records = self._event_records_with_file_fallback(
                    adapter,
                    account_uuid,
                    str(external_chat_uuid),
                    queue_id,
                    event,
                    "live",
                )
            except zulip_adapter.ZulipOperationError as exc:
                if exc.retryable:
                    self.store.retry_provider_event(
                        account_uuid, queue_id, event_id, exc.code
                    )
                    self.store.mark_health("provider", "degraded", exc.code)
                    continue
                try:
                    records = converter.event_records(
                        self.store,
                        account_uuid,
                        queue_id,
                        event,
                        "live",
                        adapter.server_url,
                        None,
                    )
                except (KeyError, TypeError, ValueError):
                    self.store.mark_provider_event_invalid(
                        account_uuid, queue_id, event_id, exc.code
                    )
                    processed += 1
                    continue
            except ValueError as exc:
                if str(exc) in {
                    "provider_chat_assignment_pending",
                    "provider_chat_participants_pending",
                }:
                    self.store.retry_provider_event(
                        account_uuid,
                        queue_id,
                        event_id,
                        str(exc),
                    )
                    continue
                if str(exc) == "provider_chat_not_selected":
                    records = []
                else:
                    self.store.mark_provider_event_invalid(
                        account_uuid, queue_id, event_id, type(exc).__name__
                    )
                    processed += 1
                    continue
            except (KeyError, TypeError) as exc:
                self.store.mark_provider_event_invalid(
                    account_uuid, queue_id, event_id, type(exc).__name__
                )
                processed += 1
                continue
            for record in records:
                if hasattr(self.store, "mark_provider_event_delivering"):
                    self.store.enqueue_workspace_delivery(record, 0, queue_id, event_id)
                else:
                    self.store.enqueue_workspace_delivery(record, 0)
            deleted_message_ids: list[str] = []
            if event.get("type") == "delete_message":
                raw_ids = event.get("message_ids")
                if raw_ids is None and event.get("message_id") is not None:
                    raw_ids = [event["message_id"]]
                deleted_message_ids = [
                    str(value) for value in typing.cast(list[object], raw_ids or [])
                ]
            if records and hasattr(self.store, "mark_provider_event_delivering"):
                self.store.mark_provider_event_delivering(
                    account_uuid, queue_id, event_id
                )
            else:
                self.store.finalize_provider_event(
                    account_uuid,
                    queue_id,
                    event_id,
                    supported,
                    deleted_message_ids,
                )
            processed += 1
        return processed

    def enqueue_backfill(
        self,
        account_uuid: str,
        provider_chat_key: str,
        messages: list[dict[str, object]],
    ) -> int:
        """Discover historical messages newest-first without outranking live work."""
        adapter = self.provider_adapters(account_uuid)
        enqueued = 0
        assignment = self.store.assignment_for_provider_chat(
            account_uuid, provider_chat_key
        )
        if assignment is None:
            raise ValueError("provider_chat_assignment_pending")
        if not self._assignment_participants_ready(
            account_uuid, provider_chat_key, assignment
        ):
            raise ValueError("provider_chat_participants_pending")
        queue_id = (
            f"backfill:{provider_chat_key}:"
            f"{assignment['uuid']}:{assignment['generation']}"
        )
        ordered_messages = converter.newest_first(messages)
        for message in ordered_messages:
            self._queue_event_catalog(
                account_uuid,
                {
                    "id": int(message["id"]),
                    "type": "message",
                    "message": message,
                },
                adapter.server_url,
            )
        for message in ordered_messages:
            event = {
                "id": int(message["id"]),
                "type": "message",
                "message": message,
            }
            _, chat_key = converter.provider_chat_reference(message)
            external_chat_uuid = converter.stable_entity_uuid(
                account_uuid, "external_chat", chat_key
            )
            records = self._event_records_with_file_fallback(
                adapter,
                account_uuid,
                external_chat_uuid,
                queue_id,
                event,
                "backfill",
            )
            for record in records:
                enqueued += int(self.store.enqueue_workspace_delivery(record, 2))
        return enqueued

    def run_backfill_once(self) -> bool:
        job = self.store.claim_backfill_job()
        if job is None:
            return False
        account_uuid = str(job["account_uuid"])
        provider_chat_key = str(job["provider_chat_key"])
        if not self.store.account_is_active(account_uuid):
            self.store.release_backfill_job(account_uuid, provider_chat_key)
            return False
        try:
            adapter = self.provider_adapters(account_uuid)
            anchor = "newest" if job["next_anchor"] is None else int(job["next_anchor"])
            messages = adapter.message_history(provider_chat_key, anchor=anchor)
        except zulip_adapter.ZulipOperationError as exc:
            if not exc.retryable:
                self.store.fail_backfill_job(
                    account_uuid,
                    provider_chat_key,
                    exc.code,
                )
                self.store.mark_health(
                    f"provider:{account_uuid}:{provider_chat_key}",
                    "degraded",
                    exc.code,
                )
                self._queue_account_report(account_uuid, "degraded", exc.code)
                return True
            attempts = int(job.get("retry_count", 0)) + 1
            ceiling = min(300.0, float(2 ** min(attempts - 1, 8)))
            random_source = getattr(self, "provider_random", random)
            delay = random_source.uniform(0.0, ceiling)
            self.store.defer_backfill_job(
                account_uuid,
                provider_chat_key,
                datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(seconds=delay),
                exc.code,
            )
            self.store.mark_health("provider", "degraded", exc.code)
            return True
        cutoff = job["cutoff_at"]
        eligible = messages
        reached_cutoff = False
        if isinstance(cutoff, datetime.datetime):
            eligible = [
                message
                for message in messages
                if datetime.datetime.fromtimestamp(
                    float(message["timestamp"]), datetime.UTC
                )
                >= cutoff
            ]
            reached_cutoff = len(eligible) != len(messages)
        try:
            self.enqueue_backfill(account_uuid, provider_chat_key, eligible)
        except zulip_adapter.ZulipOperationError as exc:
            if not exc.retryable:
                self.store.fail_backfill_job(
                    account_uuid,
                    provider_chat_key,
                    exc.code,
                )
                self.store.mark_health(
                    f"provider:{account_uuid}:{provider_chat_key}",
                    "degraded",
                    exc.code,
                )
                self._queue_account_report(account_uuid, "degraded", exc.code)
                return True
            attempts = int(job.get("retry_count", 0)) + 1
            ceiling = min(300.0, float(2 ** min(attempts - 1, 8)))
            random_source = getattr(self, "provider_random", random)
            delay = random_source.uniform(0.0, ceiling)
            self.store.defer_backfill_job(
                account_uuid,
                provider_chat_key,
                datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=delay),
                exc.code,
            )
            self.store.mark_health("provider", "degraded", exc.code)
            return True
        except ValueError as exc:
            if str(exc) not in {
                "provider_chat_assignment_pending",
                "provider_chat_participants_pending",
            }:
                raise
            # Selecting a chat and receiving the resulting Workspace stream/topic
            # mappings are separate control-plane steps. Keep the history job
            # pending until those mappings arrive instead of crashing the worker.
            self.store.release_backfill_job(account_uuid, provider_chat_key)
            return False
        complete = reached_cutoff or len(messages) < 100 or not messages
        next_anchor = (
            None
            if not messages
            else min(int(message["id"]) for message in messages) - 1
        )
        self.store.advance_backfill_job(
            account_uuid,
            provider_chat_key,
            next_anchor,
            complete,
        )
        return True

    def flush_provider_events(
        self,
        minimum_priority: int = 0,
        maximum_priority: int = 2,
        limit: int = 100,
    ) -> int:
        records = self.store.pending_workspace_deliveries(
            minimum_priority=minimum_priority,
            maximum_priority=maximum_priority,
            limit=limit,
        )
        event_records: list[tuple[dict[str, object], dict[str, object]]] = []
        completed_without_event: list[dict[str, object]] = []
        submitting_record_uuids: list[str] = []
        try:
            for record in records:
                if not self.store.account_is_active(str(record["account_uuid"])):
                    continue
                record_uuid = str(record["record_uuid"])
                if hasattr(self.store, "mark_workspace_delivery_submitting"):
                    if not self.store.mark_workspace_delivery_submitting(record_uuid):
                        continue
                submitting_record_uuids.append(record_uuid)
                event = provider_protocol.event_payload(self.store, record)
                if event is None:
                    completed_without_event.append(record)
                else:
                    event_records.append((record, event))
            if event_records:
                response = self.provider_api.apply_events(
                    [event for _record, event in event_records]
                )
                results = typing.cast(list[dict[str, object]], response["results"])
                expected = [
                    str(event["provider_event_uuid"])
                    for _record, event in event_records
                ]
                actual = [str(result["provider_event_uuid"]) for result in results]
                if actual != expected:
                    raise ValueError(
                        "Provider event response does not match request order"
                    )
                if any(result["status"] != "applied" for result in results):
                    raise ValueError("Provider event batch was not applied atomically")
        except Exception:
            if hasattr(self.store, "release_provider_event_submissions"):
                self.store.release_provider_event_submissions(submitting_record_uuids)
            raise
        for record in [
            *completed_without_event,
            *(record for record, _event in event_records),
        ]:
            result = scheduler.result_record(
                record,
                "committed",
                scheduler.TargetCommit(None, None),
                None,
            )
            self.store.accept_result(result)
        if hasattr(self.store, "finalize_ready_provider_events"):
            self.store.finalize_ready_provider_events()
        return len(completed_without_event) + len(event_records)

    def tick(self) -> bool:
        now = time.monotonic()
        progressed = False
        if now - self.last_certificate_check >= 3600.0:
            progressed |= self._renew_certificate(False)
            self.last_certificate_check = now
        progressed |= self._run_heartbeat(now)
        progressed |= self._run_control_poll(now)
        store = getattr(self, "store", None)
        if store is not None and hasattr(store, "reconcile_participant_sync"):
            store.reconcile_participant_sync()
        if store is not None and hasattr(store, "reconcile_backfill_jobs"):
            store.reconcile_backfill_jobs()
        live_progressed = False
        try:
            live_progressed |= self.poll_provider_operations() > 0
        except (httpx.TransportError, provider_api.ProviderApiRetryableError):
            self.store.mark_health(
                "provider_api", "degraded", "provider_api_unavailable"
            )
        last_provider_poll = getattr(self, "last_provider_poll", 0.0)
        provider_poll_interval = getattr(
            self,
            "provider_poll_interval_seconds",
            self.PROVIDER_POLL_INTERVAL_SECONDS,
        )
        if now - last_provider_poll >= provider_poll_interval:
            live_progressed |= self.poll_provider_events() > 0
            self.last_provider_poll = now
        if store is not None and hasattr(store, "claim_participant_sync"):
            live_progressed |= self.refresh_selected_participants_once()
        if hasattr(self, "store"):
            live_progressed |= self._flush_observed_reports(now) > 0
        live_progressed |= self.process_provider_journal() > 0
        # Provider operations and their HTTP results always outrank history I/O.
        live_progressed |= self.scheduler.reconcile_once()
        live_progressed |= self.scheduler.run_once()
        try:
            live_progressed |= self.flush_provider_results() > 0
        except (httpx.TransportError, provider_api.ProviderApiRetryableError):
            self.store.mark_health(
                "provider_api", "degraded", "provider_api_unavailable"
            )
        # Keep the live event quantum bounded so Provider HTTP cannot monopolize a tick.
        try:
            live_progressed |= (
                self.flush_provider_events(
                    minimum_priority=0, maximum_priority=0, limit=10
                )
                > 0
            )
        except (httpx.TransportError, provider_api.ProviderApiRetryableError):
            self.store.mark_health(
                "provider_api", "degraded", "provider_api_unavailable"
            )
        progressed |= live_progressed
        last_history_quantum = getattr(self, "last_history_quantum", now)
        history_due = (
            now - last_history_quantum >= self.HISTORY_QUANTUM_INTERVAL_SECONDS
        )
        if not live_progressed or history_due:
            if history_due:
                self.last_history_quantum = now
            progressed |= self.run_backfill_once()
            # Live work is handled first. One bounded history delivery then
            # prevents continuous healthy traffic from starving initial sync.
            try:
                progressed |= (
                    self.flush_provider_events(
                        minimum_priority=2, maximum_priority=2, limit=1
                    )
                    > 0
                )
            except (httpx.TransportError, provider_api.ProviderApiRetryableError):
                self.store.mark_health(
                    "provider_api", "degraded", "provider_api_unavailable"
                )
        self.health_file.parent.mkdir(parents=True, exist_ok=True)
        self.health_file.write_text(
            datetime.datetime.now(datetime.UTC).isoformat(),
            encoding="utf-8",
        )
        return progressed

    def run(self) -> None:
        while True:
            progressed = self.tick()
            if not progressed:
                time.sleep(0.5)
