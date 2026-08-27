import concurrent.futures
import contextlib
import copy
import dataclasses
import datetime
import hashlib
import pathlib
import queue
import random
import ssl
import tempfile
import threading
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
        self.validated_ca_digest: str | None = None

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
        digest = hashlib.sha256(custom_pem.encode("ascii")).hexdigest()
        if digest != self.validated_ca_digest:
            try:
                ssl.create_default_context(cadata=custom_pem)
            except ssl.SSLError as exc:
                raise zulip_adapter.ZulipOperationError(
                    "invalid_custom_ca_bundle", False
                ) from exc
            self.validated_ca_digest = digest
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
        generation: int | None = None
        try:
            if resource is None:
                raise ValueError("Zulip provider account is unavailable")
            generation = int(resource["generation"])
            if generation < 1:
                raise ValueError("Account generation must be positive")
            if not resource["synchronization_enabled"]:
                raise ValueError("Zulip provider account is disabled")
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
                "unauthorized_account", False, generation
            ) from exc
        try:
            account_credentials = dataclasses.replace(
                account_credentials,
                cert_bundle=self._cert_bundle(),
            )
            return zulip_adapter.OfficialZulipAdapter(
                account_credentials,
                routing=_AccountRouting(self.store, account_uuid),
                owner_user_uuid=str(resource["owner_user_uuid"]),
                account_uuid=account_uuid,
                account_generation=generation,
                file_client=self.file_client,
                file_limit=lambda: self.store.effective_file_limit(
                    file_api.MAX_FILE_BYTES
                ),
            )
        except zulip_adapter.ZulipOperationError as exc:
            if exc.account_generation is None:
                exc.account_generation = generation
            raise


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


class _BackfillConversionStore:
    """Avoid repeated mapping round trips within one backfill transaction."""

    def __init__(self, store: storage.RestAlchemyStore):
        self.store = store
        self.account_resources: dict[str, dict[str, object] | None] = {}
        self.assignments: dict[tuple[str, str], dict[str, object] | None] = {}
        self.provider_mappings: dict[
            tuple[str, str, str], dict[str, object] | None
        ] = {}
        self.provider_message_mappings: dict[
            tuple[str, str], dict[str, object] | None
        ] = {}
        self.provider_mappings_by_name: dict[
            tuple[str, str, str], dict[str, object] | None
        ] = {}
        self.workspace_mappings: dict[
            tuple[str, str, str], dict[str, object] | None
        ] = {}
        self.remembered_mappings: dict[
            tuple[str, str, str], tuple[str, str | None, dict[str, object]]
        ] = {}

    def __getattr__(self, name: str) -> object:
        return getattr(self.store, name)

    def account_resource(self, account_uuid: str) -> dict[str, object] | None:
        if account_uuid not in self.account_resources:
            self.account_resources[account_uuid] = self.store.account_resource(
                account_uuid
            )
        return self.account_resources[account_uuid]

    def assignment_for_provider_chat(
        self, account_uuid: str, provider_chat_key: str
    ) -> dict[str, object] | None:
        key = (account_uuid, provider_chat_key)
        if key not in self.assignments:
            self.assignments[key] = self.store.assignment_for_provider_chat(*key)
        return self.assignments[key]

    def provider_mapping(
        self, account_uuid: str, entity_kind: str, provider_id: str
    ) -> dict[str, object] | None:
        key = (account_uuid, entity_kind, provider_id)
        if key not in self.provider_mappings:
            self.provider_mappings[key] = self.store.provider_mapping(*key)
        return self.provider_mappings[key]

    def provider_message_mapping(
        self, account_uuid: str, provider_id: str
    ) -> dict[str, object] | None:
        key = (account_uuid, provider_id)
        if key not in self.provider_message_mappings:
            self.provider_message_mappings[key] = (
                self.store.provider_message_mapping(*key)
            )
        return self.provider_message_mappings[key]

    def provider_mapping_by_name(
        self, account_uuid: str, entity_kind: str, name: str
    ) -> dict[str, object] | None:
        # PostgreSQL owns the case-insensitive lookup semantics.  Python
        # casefolding is intentionally not used here: values such as Straße
        # and STRASSE collide in Python but remain distinct under PostgreSQL's
        # LOWER() in the supported database locale.
        key = (account_uuid, entity_kind, name)
        if key not in self.provider_mappings_by_name:
            self.provider_mappings_by_name[key] = self.store.provider_mapping_by_name(
                account_uuid, entity_kind, name
            )
        return self.provider_mappings_by_name[key]

    def workspace_mapping(
        self, account_uuid: str, entity_kind: str, workspace_uuid: str
    ) -> dict[str, object] | None:
        key = (account_uuid, entity_kind, workspace_uuid)
        if key not in self.workspace_mappings:
            self.workspace_mappings[key] = self.store.workspace_mapping(*key)
        return self.workspace_mappings[key]

    def remember_provider_mapping(
        self,
        account_uuid: str,
        entity_kind: str,
        provider_id: str,
        workspace_uuid: str,
        metadata: dict[str, object],
        provider_revision: str | None = None,
    ) -> None:
        key = (account_uuid, entity_kind, provider_id)
        previous = self.remembered_mappings.get(key)
        if previous is not None and previous == (
            workspace_uuid,
            provider_revision,
            metadata,
        ):
            return
        self.store.remember_provider_mapping(
            account_uuid,
            entity_kind,
            provider_id,
            workspace_uuid,
            metadata,
            provider_revision,
        )
        self.remembered_mappings[key] = (
            workspace_uuid,
            provider_revision,
            copy.deepcopy(metadata),
        )
        mapping = self.provider_mappings.get(key)
        if isinstance(mapping, dict):
            self.provider_mappings[key] = {
                **mapping,
                "provider_revision": provider_revision
                if provider_revision is not None
                else mapping.get("provider_revision"),
                "metadata": copy.deepcopy(metadata),
            }
        else:
            self.provider_mappings.pop(key, None)
        if entity_kind == "message":
            self.provider_message_mappings.pop((account_uuid, provider_id), None)
        self.provider_mappings_by_name = {
            cache_key: value
            for cache_key, value in self.provider_mappings_by_name.items()
            if cache_key[:2] != (account_uuid, entity_kind)
        }
        self.workspace_mappings = {
            cache_key: value
            for cache_key, value in self.workspace_mappings.items()
            if cache_key[:2] != (account_uuid, entity_kind)
        }


class BridgeService:
    PROVIDER_AUTH_ERROR_CODES = frozenset(
        {
            "unauthorized",
            "unauthorized_account",
            "bad_api_key",
            "invalid_api_key",
            "user_not_authorized",
        }
    )
    MAX_QUEUE_CATCHUP_PAGES = 20
    MAX_CONTROL_SNAPSHOT_PAGES = 10_000
    MAX_CONTROL_SNAPSHOT_RESOURCES = 2_000_000
    # Keep each control request comfortably below the transport timeout. Large
    # catalogs are still drained continuously, while a completed response can
    # wake assignment-dependent live events after every small batch.
    OBSERVED_REPORT_BATCH_SIZE = 20
    # Idle history uses the full large-profile Provider batch. Once live work is
    # durable, history drops to one small batch per second so people can keep
    # chatting while the import continues in the background without reducing a
    # large import to one event per HTTP round trip.
    HISTORY_DELIVERY_BATCH_SIZE = 100
    HISTORY_LIVE_DELIVERY_BATCH_SIZE = 10
    BACKGROUND_HISTORY_WORKERS = 8
    BACKGROUND_LIVE_WORKERS = 1
    # The main live lane owns journal conversion and outbound provider work.
    # Additional workers only submit already-durable Workspace deliveries, so
    # per-account Zulip event ordering remains single-threaded.
    # A single submitter preserves causal order for live records. Provider
    # journal conversion remains concurrent with delivery and history still
    # runs across independent workers.
    BACKGROUND_LIVE_DELIVERY_WORKERS = 1
    LIVE_DELIVERY_BATCH_SIZE = 100
    LIVE_DELIVERY_DEPENDENCY_RECHECK_SECONDS = 1.0
    LIVE_DELIVERY_STALL_THRESHOLD_SECONDS = 300.0
    PROVIDER_JOURNAL_QUANTUM = 20
    PROVIDER_JOURNAL_WORKERS = 16
    PROVIDER_EVENT_FAILURE_MAX_ATTEMPTS = 5
    # Give a newly discovered chat five minutes to reach desired state, then
    # quarantine the reaction so one missing assignment cannot block the rest
    # of an account's journal forever. The start time is persisted separately
    # from retry backoff so provider failures cannot restart this deadline.
    REACTION_CHAT_ASSIGNMENT_TIMEOUT = datetime.timedelta(minutes=5)
    PARTICIPANT_SYNC_BATCH_SIZE = 50
    # Amortize transaction setup while keeping each history lock set short. The
    # effective size falls back to one as soon as live work becomes durable.
    BACKFILL_ENQUEUE_TRANSACTION_MESSAGES = 10
    RETRYABLE_DATABASE_CONFLICT_CODES = frozenset({"40001", "40P01"})
    PROVIDER_POLL_INTERVAL_SECONDS = 2.0
    ACCOUNT_STATE_RECHECK_INTERVAL_SECONDS = 30.0
    OBSERVED_REPORT_RECHECK_INTERVAL_SECONDS = 300.0
    # Desired-state changes reconcile immediately. This slower sweep is only a
    # recovery guard for state left behind by a prior process interruption.
    CONTROL_STATE_RECONCILE_INTERVAL_SECONDS = 60.0
    HISTORY_QUANTUM_INTERVAL_SECONDS = 1.0
    HISTORY_PROGRESS_YIELD_SECONDS = 0.01
    TERMINAL_STATE_PRUNE_INTERVAL_SECONDS = 10.0
    HISTORY_LEASE_REAP_INTERVAL_SECONDS = 30.0
    PROVIDER_DELIVERY_RETRY_BASE_SECONDS = 1.0
    PROVIDER_DELIVERY_RETRY_CAP_SECONDS = 30.0
    PROVIDER_DELIVERY_RETRY_AFTER_CAP_SECONDS = 300.0

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
        provider_event_long_polling: bool = False,
        provider_lease_seconds: int = 300,
        provider_batch_size: int = 20,
    ):
        if provider_client is None:
            raise ValueError("Provider API client is required")
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
        self.provider_event_long_polling = provider_event_long_polling
        self.provider_lease_seconds = provider_lease_seconds
        self.provider_batch_size = provider_batch_size
        self.provider_lease_request_uuid: uuid.UUID | None = None
        self.last_control = 0.0
        self.last_heartbeat = 0.0
        self.last_certificate_check = 0.0
        self.last_provider_poll = 0.0
        self.last_control_state_reconcile = 0.0
        self.control_state_dirty = True
        self.last_history_quantum = time.monotonic()
        self.history_quantum_lock = threading.Lock()
        self.history_delivery_lock = threading.Lock()
        self.last_terminal_state_prune = time.monotonic()
        self.last_history_lease_reap = time.monotonic()
        self.provider_poll_threads: dict[str, threading.Thread] = {}
        self.provider_poll_stops: dict[str, threading.Event] = {}
        self.provider_poll_results: queue.SimpleQueue[
            tuple[str, int | None, int, zulip_adapter.ZulipOperationError | None]
        ] = queue.SimpleQueue()
        self.provider_failed_accounts: set[str] = set()
        self.provider_successful_accounts: set[str] = set()
        self.provider_retry_attempts: dict[str, int] = {}
        self.provider_retry_after: dict[str, float] = {}
        self.initial_sync_ready_accounts: set[str] = set()
        self.initial_sync_probe_after: dict[str, float] = {}
        self.provider_account_report_states: dict[str, tuple[int, str, str | None]] = {}
        self.provider_account_report_retry_after: dict[
            str, tuple[tuple[int, str, str | None], float]
        ] = {}
        self.provider_random = random.Random()
        self.control_retry_attempts = 0
        self.control_retry_after = 0.0
        self.heartbeat_retry_attempts = 0
        self.heartbeat_retry_after = 0.0
        self.control_random = random.Random()
        self.provider_delivery_retry_attempts = 0
        self.provider_delivery_retry_after = 0.0
        self.provider_delivery_random = random.Random()
        self.provider_delivery_retry_lock = threading.Lock()
        self.provider_delivery_lock = threading.Lock()
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
        self.scheduler.account_failure_handler = self._handle_provider_account_error
        self.scheduler.account_success_handler = self._record_provider_account_success

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
        changes = typing.cast(list[dict[str, object]], batch["changes"])
        try:
            self.store.apply_desired_changes(changes, str(batch["next_cursor"]))
        except (KeyError, TypeError, ValueError):
            self.store.set_blocked_batch(
                cursor,
                str(batch.get("next_cursor", cursor)),
                "unsupported_desired_batch",
            )
            self.store.mark_health("control", "degraded", "unsupported_desired_batch")
            return False
        self.store.clear_blocked_batch()
        if changes:
            reset_stale_deliveries = getattr(
                self.store,
                "reset_stale_workspace_deliveries",
                None,
            )
            if callable(reset_stale_deliveries):
                reset_stale_deliveries()
            self.control_state_dirty = True
            getattr(self, "initial_sync_ready_accounts", set()).clear()
            getattr(self, "initial_sync_probe_after", {}).clear()
            getattr(self, "provider_account_report_states", {}).clear()
            getattr(self, "provider_account_report_retry_after", {}).clear()
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
        reset_stale_deliveries = getattr(
            self.store,
            "reset_stale_workspace_deliveries",
            None,
        )
        if callable(reset_stale_deliveries):
            reset_stale_deliveries()
        self.control_state_dirty = True
        getattr(self, "initial_sync_ready_accounts", set()).clear()
        getattr(self, "initial_sync_probe_after", {}).clear()
        getattr(self, "provider_account_report_states", {}).clear()
        getattr(self, "provider_account_report_retry_after", {}).clear()

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
        # Catalog reports can satisfy the assignment dependency blocking the
        # live journal.  Delivery has its own worker, so a ready delivery must
        # not hold the control-plane report behind it.
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

    def _live_workspace_delivery_pending(self) -> bool:
        store = getattr(self, "store", None)
        pending_provider_events = getattr(store, "has_pending_provider_events", None)
        if callable(pending_provider_events) and pending_provider_events():
            return True
        return self._ready_live_workspace_delivery_pending()

    def _ready_live_workspace_delivery_pending(self) -> bool:
        store = getattr(self, "store", None)
        ready_deliveries = getattr(store, "has_pending_workspace_deliveries", None)
        if callable(ready_deliveries):
            return bool(ready_deliveries(0, 0))
        pending = getattr(store, "pending_workspace_deliveries", None)
        if pending is None:
            return False
        return bool(pending(minimum_priority=0, maximum_priority=0, limit=1))

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
            transport = record.get("transport")
            lease_uuid = (
                str(transport["lease_uuid"])
                if isinstance(transport, dict) and transport.get("lease_uuid")
                else None
            )
            self.store.finalize_provider_result_response(
                str(record["record_uuid"]), status, lease_uuid
            )
            if status in {"applied", "duplicate"}:
                sent += 1
        return sent

    def _poll_provider_account(
        self,
        account_uuid: str,
        adapter: zulip_adapter.OfficialZulipAdapter | None = None,
    ) -> tuple[int, zulip_adapter.ZulipOperationError | None]:
        """Perform one bounded queue poll using an account-thread-owned adapter."""
        try:
            if adapter is None:
                adapter = self.provider_adapters(account_uuid)
            cursor = self.store.provider_event_cursor(account_uuid)
            if cursor is not None and (
                (
                    "provider_realm_uuid" in cursor
                    and cursor["provider_realm_uuid"] is None
                )
                or (
                    "provider_owner_user_id" in cursor
                    and cursor["provider_owner_user_id"] is None
                )
            ):
                self.store.invalidate_provider_event_cursor(account_uuid)
                getattr(self, "initial_sync_ready_accounts", set()).discard(
                    account_uuid
                )
                getattr(self, "initial_sync_probe_after", {}).pop(account_uuid, None)
                cursor = None
            if cursor is None:
                adapter.invalidate_queue()
                queue_id, last_event_id = adapter.ensure_queue()
                # Persist the queue before catalog, participant, or history work.
                # A restart can then resume the same queue instead of opening a
                # gap while the initial synchronization is still in progress.
                self.store.update_provider_event_cursor(
                    account_uuid, queue_id, last_event_id
                )
                registration = adapter.take_registration_snapshot()
                if registration is not None:
                    account = self.store.account_resource(account_uuid)
                    if account is None:
                        raise ValueError("Zulip provider account is unavailable")
                    provider_account_generation = int(account["generation"])
                    provider_realm_uuid = str(
                        uuid.UUID(str(registration["realm_uuid"]))
                    )
                    provider_owner_user_id = str(int(registration["user_id"]))
                    self.store.update_provider_event_cursor(
                        account_uuid,
                        queue_id,
                        last_event_id,
                        provider_realm_uuid,
                        provider_owner_user_id,
                        provider_account_generation,
                    )
                    self._queue_registration_reports(
                        account_uuid,
                        registration,
                        getattr(adapter, "server_url", ""),
                    )
                    self._record_registration_notification_snapshots(
                        account_uuid,
                        queue_id,
                        registration,
                    )
            else:
                queue_id = str(cursor["queue_id"])
                last_event_id = int(cursor["last_event_id"])
                adapter.restore_queue(queue_id, last_event_id)
            if getattr(self, "provider_event_long_polling", False):
                events = adapter.events(queue_id, last_event_id, long_polling=True)
            else:
                events = adapter.events(queue_id, last_event_id)
        except zulip_adapter.ZulipOperationError as exc:
            if exc.code == "bad_event_queue_id":
                self.store.begin_provider_queue_catchup(account_uuid)
                self.store.invalidate_provider_event_cursor(account_uuid)
                getattr(self, "initial_sync_ready_accounts", set()).discard(
                    account_uuid
                )
                getattr(self, "initial_sync_probe_after", {}).pop(account_uuid, None)
                if adapter is not None:
                    adapter.invalidate_queue()
            return 0, exc
        processed = 0
        for event in events:
            event_id = int(event["id"])
            if event.get("type") != "heartbeat":
                persisted_event = event
                if (
                    event.get("type") == "user_settings"
                    and event.get("op") == "update"
                    and event.get("property") == "enable_stream_desktop_notifications"
                ):
                    try:
                        subscriptions = adapter.notification_subscriptions()
                    except zulip_adapter.ZulipOperationError as exc:
                        return processed, exc
                    persisted_event = dict(event)
                    persisted_event["subscriptions"] = subscriptions
                    persisted_event["observed_at"] = datetime.datetime.now(
                        datetime.UTC
                    ).timestamp()
                if (
                    event.get("type") == "subscription"
                    and event.get("op") == "update"
                    and event.get("property") in {"is_muted", "desktop_notifications"}
                    and event.get("observed_at") is None
                ):
                    persisted_event = dict(event)
                    persisted_event["observed_at"] = datetime.datetime.now(
                        datetime.UTC
                    ).timestamp()
                self.store.record_provider_event(
                    account_uuid,
                    queue_id,
                    persisted_event,
                )
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
                processed += 1
            self.store.update_provider_event_cursor(account_uuid, queue_id, event_id)
        initial_sync_ready = self._initial_sync_ready(account_uuid)
        self._queue_account_report(
            account_uuid,
            "live_ready" if initial_sync_ready else "backfill",
        )
        if initial_sync_ready:
            self._queue_ready_assignment_reports(account_uuid)
        return processed, None

    def _record_registration_notification_snapshots(
        self,
        account_uuid: str,
        queue_id: str,
        registration: dict[str, object],
    ) -> int:
        observed_at = datetime.datetime.now(datetime.UTC).timestamp()
        snapshots: list[dict[str, object]] = []
        user_settings = registration.get("user_settings")
        global_desktop_notifications = (
            user_settings.get("enable_stream_desktop_notifications")
            if isinstance(user_settings, dict)
            else None
        )
        if not isinstance(global_desktop_notifications, bool):
            return 0
        for subscription in typing.cast(
            list[dict[str, object]], registration.get("subscriptions", [])
        ):
            stream_id = subscription.get("stream_id")
            is_muted = subscription.get("is_muted")
            desktop_notifications = subscription.get("desktop_notifications")
            if (
                not isinstance(stream_id, int)
                or not isinstance(is_muted, bool)
                or (
                    desktop_notifications is not None
                    and not isinstance(desktop_notifications, bool)
                )
            ):
                continue
            snapshots.append(
                {
                    "type": "subscription",
                    "op": "notification_snapshot",
                    "stream_id": stream_id,
                    "is_muted": is_muted,
                    "desktop_notifications": desktop_notifications,
                    "enable_stream_desktop_notifications": (
                        global_desktop_notifications
                    ),
                    "observed_at": observed_at,
                }
            )
        subscribed_stream_ids = {
            int(subscription["stream_id"])
            for subscription in typing.cast(
                list[dict[str, object]], registration.get("subscriptions", [])
            )
            if isinstance(subscription.get("stream_id"), int)
        }
        user_topics = [
            topic
            for topic in typing.cast(
                list[dict[str, object]], registration.get("user_topics", [])
            )
            if int(topic["stream_id"]) in subscribed_stream_ids
        ]
        snapshots.extend({"type": "user_topic", **topic} for topic in user_topics)
        explicit_topic_ids = {
            converter.channel_topic_provider_id(
                int(topic["stream_id"]), str(topic["topic_name"])
            )
            for topic in user_topics
        }
        for mapping in self.store.provider_topic_mappings(account_uuid):
            provider_topic_id = str(mapping["provider_id"])
            raw_stream_id, separator, topic_name = provider_topic_id.partition(":")
            if not separator or not raw_stream_id.isdigit() or not topic_name:
                continue
            stream_id = int(raw_stream_id)
            if (
                stream_id not in subscribed_stream_ids
                or provider_topic_id in explicit_topic_ids
            ):
                continue
            # Zulip omits inherited/default topics from user_topics. Recreate
            # those authoritative tombstones for every mapped topic so a queue
            # replacement cannot retain an older mute/follow override forever.
            snapshots.append(
                {
                    "type": "user_topic",
                    "stream_id": stream_id,
                    "topic_name": topic_name,
                    "visibility_policy": 0,
                    "observed_at": observed_at,
                }
            )
        inserted = 0
        for index, snapshot in enumerate(snapshots, start=1):
            event = {"id": -index, **snapshot}
            inserted += int(
                self.store.record_provider_event(account_uuid, queue_id, event)
            )
        return inserted

    def _ensure_provider_poll_state(self) -> None:
        """Initialize account-thread state for normal and lightweight test instances."""
        if not hasattr(self, "provider_poll_threads"):
            self.provider_poll_threads = {}
        if not hasattr(self, "provider_poll_stops"):
            self.provider_poll_stops = {}
        if not hasattr(self, "provider_poll_results"):
            self.provider_poll_results = queue.SimpleQueue()
        if not hasattr(self, "provider_failed_accounts"):
            self.provider_failed_accounts = set()
        if not hasattr(self, "provider_successful_accounts"):
            self.provider_successful_accounts = set()

    def _run_provider_account_poll(
        self,
        account_uuid: str,
        stop: threading.Event,
    ) -> None:
        """Continuously capture one account queue outside the main sync thread."""
        try:
            adapter = self.provider_adapters(account_uuid)
            attempted_generation = self._adapter_generation(adapter)
            while not stop.is_set():
                processed, error = self._poll_provider_account(account_uuid, adapter)
                self.provider_poll_results.put(
                    (account_uuid, attempted_generation, processed, error)
                )
                if error is not None:
                    return
                poll_interval = getattr(
                    self,
                    "provider_poll_interval_seconds",
                    self.PROVIDER_POLL_INTERVAL_SECONDS,
                )
                if stop.wait(max(0.1, poll_interval)):
                    return
        except zulip_adapter.ZulipOperationError as exc:
            self.provider_poll_results.put(
                (account_uuid, exc.account_generation, 0, exc)
            )
        except Exception:
            self.provider_poll_results.put(
                (
                    account_uuid,
                    None,
                    0,
                    zulip_adapter.ZulipOperationError(
                        "provider_poll_failed",
                        True,
                    ),
                )
            )

    def poll_provider_events(self) -> int:
        """Supervise one persistent long-poll thread for every eligible account."""
        self._ensure_provider_poll_state()
        now = time.monotonic()
        active_accounts = set(self._eligible_provider_accounts())
        processed = 0
        while True:
            try:
                account_uuid, attempted_generation, account_processed, error = (
                    self.provider_poll_results.get_nowait()
                )
            except queue.Empty:
                break
            if account_uuid not in active_accounts:
                continue
            if error is None:
                self._record_provider_account_success(
                    account_uuid, attempted_generation
                )
                self.provider_failed_accounts.discard(account_uuid)
                self.provider_successful_accounts.add(account_uuid)
                processed += account_processed
            else:
                self.provider_successful_accounts.discard(account_uuid)
                self.provider_failed_accounts.add(account_uuid)
                if not hasattr(self.store, "record_provider_account_failure"):
                    self._defer_provider_account(account_uuid, now)
                self._handle_provider_account_error(
                    account_uuid, error, attempted_generation
                )
                active_accounts.discard(account_uuid)

        for account_uuid, thread in list(self.provider_poll_threads.items()):
            if account_uuid not in active_accounts:
                self.provider_poll_stops[account_uuid].set()
            if not thread.is_alive():
                thread.join(timeout=0)
                del self.provider_poll_threads[account_uuid]
                del self.provider_poll_stops[account_uuid]

        self.provider_failed_accounts.intersection_update(active_accounts)
        self.provider_successful_accounts.intersection_update(active_accounts)
        for account_uuid in sorted(active_accounts):
            if account_uuid in self.provider_poll_threads:
                continue
            if getattr(self, "provider_retry_after", {}).get(account_uuid, 0.0) > now:
                continue
            stop = threading.Event()
            thread = threading.Thread(
                target=self._run_provider_account_poll,
                args=(account_uuid, stop),
                name=f"zulip-live-{account_uuid[:8]}-{account_uuid[-4:]}",
                daemon=True,
            )
            self.provider_poll_stops[account_uuid] = stop
            self.provider_poll_threads[account_uuid] = thread
            thread.start()

        if not active_accounts or (
            not self.provider_failed_accounts
            and active_accounts <= self.provider_successful_accounts
        ):
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
        ensure_durable: bool = False,
        provider_event_marker: tuple[str, str, int] | None = None,
        catalog_deletion: tuple[str, str] | None = None,
    ) -> bool:
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
        if ensure_durable:
            if catalog_deletion is not None:
                ensure_deletion = getattr(
                    self.store, "ensure_catalog_deletion", None
                )
                if callable(ensure_deletion):
                    return bool(
                        ensure_deletion(
                            report,
                            *catalog_deletion,
                            provider_event_marker=provider_event_marker,
                        )
                    )
            if provider_event_marker is not None:
                ensure_event_report = getattr(
                    self.store,
                    "ensure_provider_event_catalog_report",
                    None,
                )
                if callable(ensure_event_report):
                    return bool(ensure_event_report(report, *provider_event_marker))
            ensure_report = getattr(self.store, "ensure_observed_report", None)
            if callable(ensure_report):
                durable = bool(ensure_report(report))
            else:
                durable = bool(self.store.enqueue_observed_report(report))
            if durable and provider_event_marker is not None:
                mark_reported = getattr(
                    self.store,
                    "mark_provider_event_catalog_reported",
                    None,
                )
                if callable(mark_reported):
                    return bool(mark_reported(*provider_event_marker))
            return durable
        return bool(self.store.enqueue_observed_report(report))

    def _queue_account_report(
        self,
        account_uuid: str,
        status: str,
        safe_error_code: str | None = None,
        expected_generation: int | None = None,
    ) -> None:
        account_resource = getattr(self.store, "account_resource", None)
        if not callable(account_resource):
            return
        account = account_resource(account_uuid)
        if account is None:
            return
        generation = int(account["generation"])
        if expected_generation is not None and generation != expected_generation:
            return
        report_state = (generation, status, safe_error_code)
        report_states = getattr(self, "provider_account_report_states", None)
        if report_states is None:
            report_states = {}
            self.provider_account_report_states = report_states
        if report_states.get(account_uuid) == report_state:
            return
        retry_after = getattr(self, "provider_account_report_retry_after", None)
        if retry_after is None:
            retry_after = {}
            self.provider_account_report_retry_after = retry_after
        now = time.monotonic()
        suppressed = retry_after.get(account_uuid)
        if (
            suppressed is not None
            and suppressed[0] == report_state
            and now < suppressed[1]
        ):
            return
        retained = self._queue_observed_report(
            "external_account",
            account_uuid,
            generation,
            status,
            (
                "live"
                if status == "live_ready"
                else "auth_required"
                if status == "auth_required"
                else "retry"
                if status == "degraded"
                else "backfill"
            ),
            safe_error_code=safe_error_code,
        )
        if retained:
            report_states[account_uuid] = report_state
            retry_after.pop(account_uuid, None)
        else:
            retry_after[account_uuid] = (
                report_state,
                now + self.OBSERVED_REPORT_RECHECK_INTERVAL_SECONDS,
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
        assignments: dict[str, dict[str, object]] | None = None,
    ) -> None:
        account = self.store.account_resource(account_uuid)
        if account is None:
            return
        settings = typing.cast(dict[str, object], account["settings"])
        owner_uuid = str(account["owner_user_uuid"])
        generation = int(account["generation"])
        project_uuid = str(settings["default_project_id"])
        provider_realm_uuid_value = registration.get("realm_uuid")
        if provider_realm_uuid_value is None:
            cursor = self.store.provider_event_cursor(account_uuid)
            if cursor is not None:
                provider_realm_uuid_value = cursor.get("provider_realm_uuid")
        provider_realm_uuid = str(uuid.UUID(str(provider_realm_uuid_value)))
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
                if assignments is not None:
                    assignment = assignments.get(chat_key)
                else:
                    assignment_lookup = getattr(
                        self.store, "assignment_for_provider_chat", None
                    )
                    assignment = (
                        assignment_lookup(account_uuid, chat_key)
                        if assignment_lookup is not None
                        else None
                    )
                subscribers = subscription.get("subscribers")
                participant_ids: set[int] = (
                    {provider_user_id} if isinstance(provider_user_id, int) else set()
                )
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
        if assignments is None:
            omitted_channels = getattr(
                self.store, "omitted_cataloged_channels", None
            )
            if callable(omitted_channels):
                current_channel_keys = {
                    chat_key
                    for chat_key, (chat_type, _name, _participants) in catalog.items()
                    if chat_type == "channel"
                }
                for chat_key in omitted_channels(account_uuid, current_channel_keys):
                    mapping = self.store.provider_mapping(
                        account_uuid, "stream", chat_key
                    )
                    metadata = (
                        mapping.get("metadata") if isinstance(mapping, dict) else None
                    )
                    display_name = (
                        metadata.get("name")
                        if isinstance(metadata, dict)
                        and isinstance(metadata.get("name"), str)
                        else chat_key
                    )
                    # A registration snapshot is authoritative for subscribed
                    # channels. Make the Workspace deletion durable before the
                    # local topology is retired so a restart can retry safely.
                    self._queue_catalog_report(
                        account_uuid,
                        owner_uuid,
                        project_uuid,
                        generation,
                        chat_key,
                        "channel",
                        display_name,
                        server_url,
                        "delete",
                        provider_realm_uuid=provider_realm_uuid,
                        provider_owner_user_id=str(int(provider_user_id)),
                    )
        for chat_key, (chat_type, display_name, participants) in catalog.items():
            topics = (
                [
                    {
                        "provider_topic_id": f"{chat_key}:default",
                        "name": converter.ZULIP_DIRECT_TOPIC_NAME,
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
                provider_realm_uuid=provider_realm_uuid,
                provider_owner_user_id=str(int(provider_user_id)),
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
        return bool(checker(account_uuid, chat_key, int(assignment["generation"])))

    def refresh_selected_participants_once(self) -> bool:
        batch_claim = getattr(self.store, "claim_participant_sync_batch", None)
        if callable(batch_claim):
            jobs = batch_claim(self.PARTICIPANT_SYNC_BATCH_SIZE)
        else:
            job = self.store.claim_participant_sync()
            jobs = [] if job is None else [job]
        if not jobs:
            return False
        account_uuid = str(jobs[0]["account_uuid"])
        valid_jobs: list[dict[str, object]] = []
        assignments: dict[str, dict[str, object]] = {}
        invalid_jobs: list[dict[str, object]] = []
        for job in jobs:
            chat_key = str(job["provider_chat_key"])
            generation = int(job["assignment_generation"])
            assignment_value = job.get("assignment")
            assignment = (
                typing.cast(dict[str, object], assignment_value)
                if isinstance(assignment_value, dict)
                else self.store.assignment_for_provider_chat(account_uuid, chat_key)
            )
            if (
                str(job["account_uuid"]) != account_uuid
                or assignment is None
                or int(assignment["generation"]) != generation
                or not bool(assignment.get("selected", True))
            ):
                invalid_jobs.append(job)
                continue
            valid_jobs.append(job)
            assignments[chat_key] = assignment
        self._release_participant_jobs(invalid_jobs)
        if not valid_jobs:
            return False
        if self.store.provider_event_cursor(account_uuid) is None:
            self._release_participant_jobs(valid_jobs)
            return False
        adapter: zulip_adapter.OfficialZulipAdapter | None = None
        try:
            adapter = self.provider_adapters(account_uuid)
            chat_keys = [str(job["provider_chat_key"]) for job in valid_jobs]
            batch_catalog = getattr(adapter, "channel_catalogs", None)
            if callable(batch_catalog):
                registration = batch_catalog(chat_keys)
            elif len(chat_keys) == 1:
                registration = adapter.channel_catalog(chat_keys[0])
            else:
                raise RuntimeError("Adapter does not support participant batches")
            self._queue_registration_reports(
                account_uuid,
                registration,
                getattr(adapter, "server_url", ""),
                assignments,
            )
        except zulip_adapter.ZulipOperationError as exc:
            self._release_participant_jobs(valid_jobs)
            authentication = self._handle_provider_account_error(
                account_uuid, exc, self._adapter_generation(adapter)
            )
            if not authentication and not exc.retryable:
                self.store.mark_health("provider", "degraded", exc.code)
                self._queue_account_report(account_uuid, "degraded", exc.code)
            return False
        subscriptions = {
            f"channel:{int(subscription['stream_id'])}": subscription
            for subscription in typing.cast(
                list[dict[str, object]], registration["subscriptions"]
            )
        }
        completions: list[dict[str, object]] = []
        for job in valid_jobs:
            chat_key = str(job["provider_chat_key"])
            subscription = subscriptions[chat_key]
            provider_user_ids = {
                int(value)
                for value in typing.cast(list[object], subscription["subscribers"])
            }
            provider_user_ids.add(int(registration["user_id"]))
            completions.append(
                {
                    "account_uuid": account_uuid,
                    "provider_chat_key": chat_key,
                    "assignment_generation": int(job["assignment_generation"]),
                    "provider_user_ids": sorted(provider_user_ids),
                    "ready": provider_user_ids
                    == self._projection_participant_ids(assignments[chat_key]),
                }
            )
        batch_complete = getattr(self.store, "complete_participant_sync_batch", None)
        if callable(batch_complete):
            batch_complete(completions)
        else:
            for completion in completions:
                self.store.complete_participant_sync(
                    str(completion["account_uuid"]),
                    str(completion["provider_chat_key"]),
                    int(completion["assignment_generation"]),
                    typing.cast(list[int], completion["provider_user_ids"]),
                    bool(completion["ready"]),
                )
        self._record_provider_account_success(
            account_uuid, self._adapter_generation(adapter)
        )
        return True

    def _release_participant_jobs(self, jobs: list[dict[str, object]]) -> None:
        if not jobs:
            return
        batch_release = getattr(self.store, "release_participant_sync_batch", None)
        if callable(batch_release):
            batch_release(jobs)
            return
        for job in jobs:
            self.store.release_participant_sync(
                str(job["account_uuid"]),
                str(job["provider_chat_key"]),
                int(job["assignment_generation"]),
            )

    @staticmethod
    def _catalog_participant(
        person: dict[str, object], is_owner: bool
    ) -> dict[str, object]:
        provider_user_id = person.get("user_id", person.get("id"))
        if not isinstance(provider_user_id, int):
            raise ValueError("Invalid Zulip catalog participant")
        participant = {
            "provider_user_id": str(provider_user_id),
            "display_name": str(person.get("full_name", provider_user_id)),
            "email": person.get("email"),
            "avatar_urn": None,
            "is_owner": is_owner,
        }
        if isinstance(person.get("is_active"), bool):
            participant["_provider_active"] = person["is_active"]
        return participant

    def _catalog_participants_with_owner(
        self,
        account_uuid: str,
        participants: list[dict[str, object]],
        provider_owner_user_id: str,
    ) -> list[dict[str, object]]:
        normalized: list[dict[str, object]] = []
        owner_present = False
        for participant in participants:
            value = dict(participant)
            is_owner = str(value.get("provider_user_id")) == provider_owner_user_id
            value["is_owner"] = is_owner
            owner_present |= is_owner
            normalized.append(value)
        if owner_present:
            return normalized

        metadata: dict[str, object] = {}
        mapping_loader = getattr(self.store, "provider_mapping", None)
        if callable(mapping_loader):
            mapping = mapping_loader(
                account_uuid,
                "identity",
                provider_owner_user_id,
            )
            if isinstance(mapping, dict) and isinstance(mapping.get("metadata"), dict):
                metadata = typing.cast(dict[str, object], mapping["metadata"])
        display_name = metadata.get("display_name")
        normalized.append(
            {
                "provider_user_id": provider_owner_user_id,
                "display_name": (
                    display_name
                    if isinstance(display_name, str) and display_name.strip()
                    else provider_owner_user_id
                ),
                "email": (
                    metadata.get("email")
                    if isinstance(metadata.get("email"), str)
                    else None
                ),
                "avatar_urn": (
                    metadata.get("avatar_urn")
                    if isinstance(metadata.get("avatar_urn"), str)
                    else None
                ),
                "is_owner": True,
            }
        )
        return normalized

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
        provider_realm_uuid: str | None = None,
        provider_owner_user_id: str | None = None,
        provider_event_marker: tuple[str, str, int] | None = None,
    ) -> bool:
        if provider_realm_uuid is None or provider_owner_user_id is None:
            cursor = self.store.provider_event_cursor(account_uuid)
            if cursor is None:
                raise ValueError("Zulip provider account identity is unavailable")
            if provider_realm_uuid is None:
                provider_realm_uuid = str(
                    uuid.UUID(str(cursor.get("provider_realm_uuid")))
                )
            if provider_owner_user_id is None:
                provider_owner_user_id = str(
                    int(str(cursor.get("provider_owner_user_id")))
                )
        provider_realm_uuid = str(uuid.UUID(provider_realm_uuid))
        provider_owner_user_id = str(int(provider_owner_user_id))
        if operation == "upsert" and hasattr(self.store, "merge_catalog_topology"):
            participants, topics = self.store.merge_catalog_topology(
                account_uuid,
                chat_key,
                participants or [],
                topics or [],
                authoritative_participants=authoritative_participants,
            )
            normalized_participants = self._catalog_participants_with_owner(
                account_uuid,
                participants,
                provider_owner_user_id,
            )
            if normalized_participants != participants:
                participants, topics = self.store.merge_catalog_topology(
                    account_uuid,
                    chat_key,
                    normalized_participants,
                    topics,
                    authoritative_participants=authoritative_participants,
                )
        elif operation == "upsert":
            participants = self._catalog_participants_with_owner(
                account_uuid,
                participants or [],
                provider_owner_user_id,
            )
        if operation == "upsert" and chat_type in {"direct", "group_direct"}:
            peer_names = [
                str(
                    participant.get(
                        "display_name",
                        participant.get("provider_user_id", "User"),
                    )
                )
                for participant in participants or []
                if not bool(participant.get("is_owner"))
            ]
            if peer_names:
                display_name = ", ".join(peer_names)
        common_capabilities = {
            "messenger.chat_catalog",
            "messenger.message.send",
            "messenger.message.edit",
            "messenger.message.delete",
            "messenger.message.read",
            "messenger.reaction.write",
            "messenger.file.transfer",
        }
        if chat_type == "channel":
            common_capabilities.update(
                {
                    "messenger.membership.write",
                    "messenger.notification.write",
                    "messenger.stream.rename",
                    "messenger.topic.rename",
                }
            )
        capabilities = {
            name: {"available": True, **control.CAPABILITIES[name]}
            for name in sorted(common_capabilities)
        }
        external_chat_uuid = converter.stable_entity_uuid(
            account_uuid, "external_chat", chat_key
        )
        report_participants = [
            {
                name: participant.get(name)
                for name in (
                    "provider_user_id",
                    "display_name",
                    "email",
                    "avatar_urn",
                    "is_owner",
                )
                if name in participant
            }
            for participant in participants or []
        ]
        atomic_deletion = operation == "delete" and callable(
            getattr(self.store, "ensure_catalog_deletion", None)
        )
        retained = self._queue_observed_report(
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
                    "provider_realm_uuid": provider_realm_uuid,
                    "provider_owner_user_id": provider_owner_user_id,
                    "original_url": self._catalog_original_url(server_url, chat_key),
                },
                "display_name": display_name,
                "description": "",
                "participants": report_participants,
                "topics": topics or [],
                "capabilities": capabilities,
            },
            ensure_durable=True,
            provider_event_marker=provider_event_marker,
            catalog_deletion=(
                (account_uuid, chat_key) if operation == "delete" else None
            ),
        )
        if (
            retained
            and operation == "delete"
            and not atomic_deletion
            and hasattr(self.store, "delete_catalog_topology")
        ):
            self.store.delete_catalog_topology(account_uuid, chat_key)
        return retained

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

    @staticmethod
    def _reaction_message_context(
        provider_message: dict[str, object],
    ) -> dict[str, object]:
        """Retain only fields needed to resolve and catalog a reaction's chat."""
        return {
            name: provider_message[name]
            for name in (
                "id",
                "type",
                "stream_id",
                "display_recipient",
                "subject",
                "timestamp",
            )
            if name in provider_message
        }

    def _selected_provider_event_lane_changed(
        self,
        row: dict[str, object],
        event: dict[str, object],
    ) -> bool:
        selected_causal_lane = row.get("causal_lane")
        refresh_causal_lane = getattr(
            self.store,
            "refresh_provider_event_causal_lane",
            None,
        )
        return (
            self._provider_event_has_message_dependencies(event)
            and callable(refresh_causal_lane)
            and refresh_causal_lane(
                str(row["account_uuid"]),
                str(row["queue_id"]),
                int(row["event_id"]),
                event,
            )
            != selected_causal_lane
        )

    @staticmethod
    def _provider_event_has_message_dependencies(
        event: dict[str, object],
    ) -> bool:
        if event.get("message_id") is not None:
            return True
        message_ids = event.get("message_ids", event.get("messages"))
        return isinstance(message_ids, list) and bool(message_ids)

    @classmethod
    def _reaction_assignment_wait_expired(cls, row: dict[str, object]) -> bool:
        pending_since = row.get("assignment_pending_since")
        return (
            isinstance(pending_since, datetime.datetime)
            and pending_since + cls.REACTION_CHAT_ASSIGNMENT_TIMEOUT
            <= datetime.datetime.now(datetime.UTC)
        )

    def _queue_event_catalog(
        self,
        account_uuid: str,
        event: dict[str, object],
        server_url: str,
        provider_event_marker: tuple[str, str, int] | None = None,
    ) -> bool:
        account = self.store.account_resource(account_uuid)
        if account is None:
            return False
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
                if isinstance(stream_id, int) and isinstance(subject, str):
                    topic_name = converter.channel_topic_name(subject)
                    topics.append(
                        {
                            "provider_topic_id": (
                                converter.channel_topic_provider_id(
                                    stream_id, topic_name
                                )
                            ),
                            "name": topic_name,
                            "is_default": converter.is_empty_channel_topic(subject),
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
                return False
            if display_name:
                return self._queue_catalog_report(
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
                                "name": converter.ZULIP_DIRECT_TOPIC_NAME,
                                "is_default": True,
                            }
                        ]
                    ),
                    provider_event_marker=provider_event_marker,
                )
            return False
        if event_type == "user_topic":
            stream_id = event.get("stream_id")
            raw_topic_name = event.get("topic_name")
            if not isinstance(stream_id, int) or not isinstance(raw_topic_name, str):
                return False
            chat_key = f"channel:{stream_id}"
            mapping = self.store.provider_mapping(account_uuid, "stream", chat_key)
            if mapping is None:
                return False
            metadata = mapping.get("metadata")
            display_name = metadata.get("name") if isinstance(metadata, dict) else None
            if not isinstance(display_name, str) or not display_name:
                return False
            topic_name = converter.channel_topic_name(raw_topic_name)
            return self._queue_catalog_report(
                account_uuid,
                *common,
                chat_key,
                "channel",
                display_name,
                server_url,
                topics=[
                    {
                        "provider_topic_id": converter.channel_topic_provider_id(
                            stream_id, topic_name
                        ),
                        "name": topic_name,
                        "is_default": converter.is_empty_channel_topic(raw_topic_name),
                    }
                ],
                provider_event_marker=provider_event_marker,
            )
        if event_type != "subscription":
            return False
        operation = str(event.get("op"))
        if operation in {"peer_add", "peer_remove"}:
            stream_ids = event.get("stream_ids")
            if isinstance(stream_ids, list):
                invalidator = getattr(self.store, "invalidate_participant_sync", None)
                if callable(invalidator):
                    invalidator(
                        account_uuid,
                        [
                            f"channel:{stream_id}"
                            for stream_id in stream_ids
                            if isinstance(stream_id, int)
                        ],
                    )
            return False
        subscriptions: list[dict[str, object]] = []
        if operation in {"add", "remove"}:
            subscriptions = typing.cast(
                list[dict[str, object]], event.get("subscriptions", [])
            )
        elif operation == "update" and event.get("property") == "name":
            subscriptions = [
                {"stream_id": event.get("stream_id"), "name": event.get("value")}
            ]
        catalog_changed = False
        for subscription in subscriptions:
            stream_id = subscription.get("stream_id")
            display_name = subscription.get("name")
            if not isinstance(stream_id, int) or not isinstance(display_name, str):
                continue
            catalog_changed = (
                self._queue_catalog_report(
                    account_uuid,
                    *common,
                    f"channel:{stream_id}",
                    "channel",
                    display_name,
                    server_url,
                    "delete" if operation == "remove" else "upsert",
                )
                or catalog_changed
            )
        return catalog_changed

    def _initial_sync_ready(self, account_uuid: str) -> bool:
        ready_accounts = getattr(self, "initial_sync_ready_accounts", None)
        if ready_accounts is None:
            ready_accounts = set()
            self.initial_sync_ready_accounts = ready_accounts
        if account_uuid in ready_accounts:
            return True
        probe_after = getattr(self, "initial_sync_probe_after", None)
        if probe_after is None:
            probe_after = {}
            self.initial_sync_probe_after = probe_after
        now = time.monotonic()
        if now < probe_after.get(account_uuid, 0.0):
            return False
        account = self.store.account_resource(account_uuid)
        if account is None:
            return False
        generation = int(account["generation"])
        cursor = self.store.provider_event_cursor(account_uuid)
        if (
            cursor is None
            or cursor.get("provider_realm_uuid") is None
            or cursor.get("provider_owner_user_id") is None
        ):
            return False
        try:
            provider_account_generation = int(cursor["provider_account_generation"])
        except (KeyError, TypeError, ValueError):
            return False
        if provider_account_generation != generation:
            return False
        ready = (
            self.store.provider_catchup_ready(account_uuid)
            and self.store.catalog_reports_accepted(account_uuid, generation)
            and self.store.catalog_assignments_ready(account_uuid, generation)
        )
        if ready:
            ready_accounts.add(account_uuid)
            probe_after.pop(account_uuid, None)
        else:
            probe_after[account_uuid] = now + max(
                0.0,
                float(
                    getattr(
                        self,
                        "account_state_recheck_interval_seconds",
                        self.ACCOUNT_STATE_RECHECK_INTERVAL_SECONDS,
                    )
                ),
            )
        return ready

    def flush_observed_reports(self) -> int:
        reports = self.store.pending_observed_reports(self.OBSERVED_REPORT_BATCH_SIZE)
        if not reports:
            return 0
        response = self.control.observed_reports(reports)
        results = typing.cast(list[dict[str, object]], response["results"])
        expected = [str(report["report_uuid"]) for report in reports]
        actual = [str(result["report_uuid"]) for result in results]
        if actual != expected:
            raise ValueError("Observed report results do not match request order")
        self.store.apply_observed_report_results(results)
        report_states = getattr(self, "provider_account_report_states", None)
        if report_states is not None:
            for report, result in zip(reports, results, strict=True):
                safe_error = result.get("safe_error")
                retryable = (
                    isinstance(safe_error, dict) and safe_error.get("retryable") is True
                )
                if (
                    result.get("status") != "rejected"
                    or retryable
                    or report.get("resource_type") != "external_account"
                ):
                    continue
                account_uuid = str(report["resource_uuid"])
                report_safe_error = report.get("safe_error")
                safe_error_code = (
                    str(report_safe_error["code"])
                    if isinstance(report_safe_error, dict)
                    else None
                )
                report_state = (
                    int(report["observed_generation"]),
                    str(report["status"]),
                    safe_error_code,
                )
                if report_states.get(account_uuid) == report_state:
                    report_states.pop(account_uuid, None)
        return len(results)

    def _defer_provider_account(self, account_uuid: str, now: float) -> None:
        retry_attempts = getattr(self, "provider_retry_attempts", None)
        if retry_attempts is None:
            retry_attempts = {}
            self.provider_retry_attempts = retry_attempts
        retry_after = getattr(self, "provider_retry_after", None)
        if retry_after is None:
            retry_after = {}
            self.provider_retry_after = retry_after
        attempts = retry_attempts.get(account_uuid, 0) + 1
        self.provider_retry_attempts[account_uuid] = attempts
        ceiling = min(300.0, float(2 ** min(attempts - 1, 8)))
        random_source = getattr(self, "provider_random", random)
        self.provider_retry_after[account_uuid] = now + random_source.uniform(
            0.0, ceiling
        )

    def _clear_provider_retry(self, account_uuid: str) -> None:
        getattr(self, "provider_retry_attempts", {}).pop(account_uuid, None)
        getattr(self, "provider_retry_after", {}).pop(account_uuid, None)

    def _eligible_provider_accounts(self) -> list[str]:
        eligible = getattr(self.store, "eligible_account_uuids", None)
        if callable(eligible):
            return typing.cast(list[str], eligible())
        return self.store.active_account_uuids()

    @classmethod
    def _provider_auth_error(cls, code: str) -> bool:
        return code.lower() in cls.PROVIDER_AUTH_ERROR_CODES

    @staticmethod
    def _adapter_generation(
        adapter: zulip_adapter.OfficialZulipAdapter | object | None,
    ) -> int | None:
        generation = getattr(adapter, "account_generation", None)
        if isinstance(generation, int) and not isinstance(generation, bool):
            return generation
        return None

    def _handle_provider_account_error(
        self,
        account_uuid: str,
        error: zulip_adapter.ZulipOperationError,
        attempted_generation: int | None = None,
    ) -> bool:
        """Persist account-scoped retry or sticky authentication quarantine."""
        authentication = self._provider_auth_error(error.code)
        normalized_code = "unauthorized_account" if authentication else error.code
        if attempted_generation is None:
            attempted_generation = error.account_generation
        recorder = getattr(self.store, "record_provider_account_failure", None)
        recorded: dict[str, object] | None = None
        if (
            callable(recorder)
            and attempted_generation is not None
            and (authentication or error.retryable)
        ):
            recorded = recorder(
                account_uuid,
                attempted_generation,
                normalized_code,
                error.retryable and not authentication,
            )
            if (
                isinstance(recorded, dict)
                and recorded.get("provider_state") == "auth_required"
            ):
                authentication = True
                normalized_code = "unauthorized_account"
        if not authentication and not error.retryable:
            return False
        if callable(recorder) and recorded is None:
            # No desired resource matched the generation used by this request.
            # Classify the old operation, but do not mutate/report a newer one.
            return authentication
        component = (
            storage.provider_account_health_component(account_uuid)
            if callable(recorder)
            else "provider"
        )
        self.store.mark_health(component, "degraded", normalized_code)
        self._queue_account_report(
            account_uuid,
            "auth_required" if authentication else "degraded",
            normalized_code,
            attempted_generation,
        )
        return authentication

    def _record_provider_account_success(
        self, account_uuid: str, attempted_generation: int | None = None
    ) -> None:
        recorder = getattr(self.store, "record_provider_account_success", None)
        if callable(recorder):
            if attempted_generation is None:
                return
            if recorder(account_uuid, attempted_generation) is None:
                return
        self._clear_provider_retry(account_uuid)

    def _enqueue_queue_recovery_delivery(self, record: dict[str, object]) -> None:
        try:
            self.store.enqueue_workspace_delivery(record, 2)
        except ValueError as exc:
            if str(exc) != "Operation UUID reused with a different digest":
                raise
            # Catch-up can rediscover an edit or deletion whose deterministic
            # operation was already accepted from an earlier recovery page.
            # Preserve the first accepted operation as the idempotent result.

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
        complete = reached_checkpoint or len(messages) < zulip_adapter.HISTORY_PAGE_SIZE

        unmapped_messages = []
        for message in converter.newest_first(messages):
            provider_message_id = str(message["id"])
            mapping = converter.provider_message_mapping(
                self.store,
                account_uuid,
                provider_message_id,
            )
            if mapping is None:
                unmapped_messages.append(message)
                continue
            metadata = typing.cast(dict[str, object], mapping["metadata"])
            workspace_delivery_committed = (
                mapping.get("convergent_alias") is True
                or metadata.get("mapping_origin") == "workspace"
                or metadata.get("workspace_delivery_state") == "committed"
            )
            if not workspace_delivery_committed:
                unmapped_messages.append(message)
                continue
            provider_content_sha256 = hashlib.sha256(
                str(message["content"]).encode("utf-8")
            ).hexdigest()
            current_subject = converter.channel_topic_name(
                str(message.get("subject", ""))
            )
            mapped_subject = converter.channel_topic_name(
                str(metadata.get("subject", ""))
            )
            if (
                metadata.get("provider_content_sha256") == provider_content_sha256
                and mapped_subject == current_subject
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
                "orig_subject": mapped_subject,
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
                self._enqueue_queue_recovery_delivery(record)

        if unmapped_messages:
            try:
                self.enqueue_backfill(account_uuid, chat_key, unmapped_messages)
            except ValueError as exc:
                if str(exc) not in {
                    "provider_chat_assignment_pending",
                    "provider_chat_participants_pending",
                }:
                    raise
                # Queue recovery can overlap the Workspace control-plane work
                # that creates stream/topic mappings or projects the complete
                # participant set for a newly selected chat. Leave the catch-up
                # checkpoint untouched and retry after both gates are ready.
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
                    self._enqueue_queue_recovery_delivery(record)

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

    def run_provider_catchup_once(self) -> bool:
        store = getattr(self, "store", None)
        if (
            store is None
            or not hasattr(store, "active_account_uuids")
            or not hasattr(store, "provider_catchup_ready")
        ):
            return False
        for account_uuid in self._eligible_provider_accounts():
            if store.provider_catchup_ready(account_uuid):
                continue
            adapter: zulip_adapter.OfficialZulipAdapter | None = None
            try:
                adapter = self.provider_adapters(account_uuid)
                self._run_provider_queue_catchup(account_uuid, adapter)
            except zulip_adapter.ZulipOperationError as exc:
                authentication = self._handle_provider_account_error(
                    account_uuid, exc, self._adapter_generation(adapter)
                )
                if not authentication and not exc.retryable:
                    self.store.mark_health("provider", "degraded", exc.code)
                    self._queue_account_report(account_uuid, "degraded", exc.code)
                continue
            self._record_provider_account_success(
                account_uuid, self._adapter_generation(adapter)
            )
            return True
        return False

    def _run_history_quantum_once(self) -> bool:
        if self.run_provider_catchup_once():
            return True
        return self.run_backfill_once()

    def _file_resolver(
        self,
        adapter: zulip_adapter.OfficialZulipAdapter,
        account_uuid: str,
        external_chat_uuid: str,
    ) -> converter.FileResolver | None:
        if self.file_client is None:
            return None

        def resolve(provider_url: str, display_name: str) -> str:
            max_bytes = self.store.effective_file_limit(file_api.MAX_FILE_BYTES)
            downloaded = adapter.download_file(provider_url, max_bytes=max_bytes)
            incoming_uuid = uuid.uuid5(
                converter.ENTITY_NAMESPACE,
                f"zulip-file:{account_uuid}:{external_chat_uuid}:{provider_url}",
            )
            transfer_operation_uuid = uuid.uuid5(
                converter.OPERATION_NAMESPACE,
                f"zulip-file-import:{account_uuid}:{external_chat_uuid}:{provider_url}",
            )
            try:
                return self.file_client.import_file(
                    transfer_operation_uuid,
                    uuid.UUID(account_uuid),
                    uuid.UUID(external_chat_uuid),
                    file_api.IncomingFile(
                        incoming_uuid,
                        # The UUID and operation UUID are URL-derived, so the
                        # immutable descriptor must not depend on a mutable
                        # Markdown label for that URL.
                        downloaded.name,
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
        conversion_store: converter.ConversionStore | None = None,
    ) -> list[dict[str, object]]:
        file_resolver = self._file_resolver(
            adapter,
            account_uuid,
            external_chat_uuid,
        )
        if file_resolver is not None and delivery_class == "backfill":
            transfer_file = file_resolver

            def resolve_historical_file(
                provider_url: str, display_name: str
            ) -> str | None:
                try:
                    return transfer_file(provider_url, display_name)
                except zulip_adapter.ZulipOperationError as exc:
                    if exc.code == "provider_file_unavailable" and not exc.retryable:
                        return None
                    raise

            file_resolver = resolve_historical_file
        return converter.event_records(
            self.store if conversion_store is None else conversion_store,
            account_uuid,
            queue_id,
            event,
            delivery_class,
            adapter.server_url,
            file_resolver,
        )

    def _pending_delete_recreation_messages(
        self,
        adapter: zulip_adapter.OfficialZulipAdapter,
        row: dict[str, object],
        event: dict[str, object],
    ) -> tuple[dict[str, dict[str, object]], set[str]]:
        if event.get("type") != "update_message":
            return {}, set()
        destination_stream_id = event.get("new_stream_id")
        if destination_stream_id is None:
            return {}, set()
        account_uuid = str(row["account_uuid"])
        destination_chat_key = f"channel:{int(destination_stream_id)}"
        try:
            converter.provider_chat_assignment(
                self.store,
                account_uuid,
                destination_chat_key,
            )
        except ValueError as exc:
            if str(exc) == "provider_chat_not_selected":
                return {}, set()
            raise
        message_ids = event.get("message_ids")
        if message_ids is None and event.get("message_id") is not None:
            message_ids = [event["message_id"]]
        provider_message_ids = [
            str(value) for value in typing.cast(list[object], message_ids or [])
        ]
        cached_context = row.get("provider_message_context")
        if (
            isinstance(cached_context, dict)
            and cached_context.get("context_kind") == "pending_delete_recreations"
        ):
            return self._validated_pending_delete_recreations(
                cached_context,
                provider_message_ids,
            )
        pending_context_lookup = getattr(
            self.store,
            "pending_provider_message_context",
            None,
        )
        tombstone_lookup = getattr(
            self.store,
            "provider_message_tombstone",
            None,
        )
        if not callable(pending_context_lookup) and not callable(tombstone_lookup):
            return {}, set()
        pending_ids = []
        for provider_message_id in provider_message_ids:
            mapping = converter.provider_message_mapping(
                self.store,
                account_uuid,
                provider_message_id,
            )
            if mapping is None and callable(tombstone_lookup):
                mapping = tombstone_lookup(account_uuid, provider_message_id)
                if mapping is not None:
                    pending_ids.append(provider_message_id)
                    continue
            if mapping is None:
                continue
            if not callable(pending_context_lookup):
                continue
            pending_context = pending_context_lookup(
                account_uuid,
                str(mapping["workspace_uuid"]),
            )
            if pending_context is not None and pending_context.get("deleted") is True:
                pending_ids.append(provider_message_id)
        if not pending_ids:
            return {}, set()

        snapshots = {}
        missing_message_ids = []
        for provider_message_id in pending_ids:
            snapshot = adapter.message_by_id(int(provider_message_id))
            if snapshot is None:
                missing_message_ids.append(provider_message_id)
                continue
            snapshots[provider_message_id] = snapshot
        cache_context = getattr(
            self.store,
            "cache_provider_event_message_context",
            None,
        )
        cached_context = {
            "context_kind": "pending_delete_recreations",
            "messages": snapshots,
            "missing_message_ids": missing_message_ids,
        }
        if callable(cache_context):
            cached_context = cache_context(
                account_uuid,
                str(row["queue_id"]),
                int(row["event_id"]),
                cached_context,
            )
        return self._validated_pending_delete_recreations(
            cached_context,
            provider_message_ids,
        )

    @staticmethod
    def _validated_pending_delete_recreations(
        cached_context: dict[str, object],
        provider_message_ids: list[str],
    ) -> tuple[dict[str, dict[str, object]], set[str]]:
        cached_messages = cached_context.get("messages")
        if not isinstance(cached_messages, dict):
            raise ValueError("provider_event_replay_incomplete")
        cached_missing_message_ids = cached_context.get("missing_message_ids", [])
        if not isinstance(cached_missing_message_ids, list) or any(
            not isinstance(provider_message_id, str)
            for provider_message_id in cached_missing_message_ids
        ):
            raise ValueError("provider_event_replay_incomplete")
        missing_message_ids = set(cached_missing_message_ids)
        cached_message_ids = set(cached_messages)
        unexpected_ids = (cached_message_ids | missing_message_ids).difference(
            provider_message_ids
        )
        if unexpected_ids:
            raise ValueError("provider_event_replay_incomplete")
        if cached_message_ids & missing_message_ids:
            raise ValueError("provider_event_replay_incomplete")
        recreations = {}
        for provider_message_id in provider_message_ids:
            snapshot = cached_messages.get(provider_message_id)
            if snapshot is None:
                continue
            if not isinstance(snapshot, dict) or str(snapshot.get("id")) != (
                provider_message_id
            ):
                raise ValueError("provider_event_replay_incomplete")
            recreations[provider_message_id] = typing.cast(
                dict[str, object],
                snapshot,
            )
        return recreations, missing_message_ids

    def _event_records_with_pending_delete_recreations(
        self,
        adapter: zulip_adapter.OfficialZulipAdapter,
        row: dict[str, object],
        external_chat_uuid: str,
        event: dict[str, object],
        delivery_class: str,
    ) -> list[dict[str, object]]:
        account_uuid = str(row["account_uuid"])
        queue_id = str(row["queue_id"])
        recreations, missing_message_ids = self._pending_delete_recreation_messages(
            adapter,
            row,
            event,
        )
        conversion_events: list[tuple[dict[str, object], str]] = []
        if recreations or missing_message_ids:
            message_ids = event.get("message_ids")
            if message_ids is None and event.get("message_id") is not None:
                message_ids = [event["message_id"]]
            remaining_ids = [
                value
                for value in typing.cast(list[object], message_ids or [])
                if str(value) not in recreations
                and str(value) not in missing_message_ids
            ]
            if remaining_ids:
                mapped_event = dict(event)
                mapped_event["message_ids"] = remaining_ids
                conversion_events.append((mapped_event, external_chat_uuid))
            for provider_message_id_raw in typing.cast(list[object], message_ids or []):
                provider_message_id = str(provider_message_id_raw)
                snapshot = recreations.get(provider_message_id)
                if snapshot is None:
                    continue
                _chat_type, chat_key = converter.provider_chat_reference(snapshot)
                snapshot_chat_uuid = _AccountRouting(
                    self.store,
                    account_uuid,
                ).external_chat_uuid(chat_key)
                conversion_events.append(
                    (
                        {
                            "id": event["id"],
                            "type": "message",
                            "message": snapshot,
                        },
                        snapshot_chat_uuid,
                    )
                )
        else:
            conversion_events.append((event, external_chat_uuid))

        records: list[dict[str, object]] = []
        setup_operations: set[tuple[str, str, str]] = set()
        for conversion_event, target_chat_uuid in conversion_events:
            converted = self._event_records_with_file_fallback(
                adapter,
                account_uuid,
                target_chat_uuid,
                queue_id,
                conversion_event,
                delivery_class,
            )
            for record in converted:
                operation = typing.cast(
                    dict[str, object] | None,
                    record.get("operation"),
                )
                if operation is not None and operation.get("kind") in {
                    "identity.upsert",
                    "topic.upsert",
                }:
                    setup_key = (
                        str(operation["kind"]),
                        str(record["project_uuid"]),
                        str(operation["entity_uuid"]),
                    )
                    if setup_key in setup_operations:
                        continue
                    setup_operations.add(setup_key)
                records.append(record)
        return records

    def _recover_interrupted_workspace_deliveries_once(self) -> None:
        """Recover prior-process submissions before concurrent delivery starts."""
        if getattr(self, "_workspace_delivery_recovery_done", False):
            return
        if hasattr(self.store, "mark_interrupted_workspace_deliveries_ambiguous"):
            self.store.mark_interrupted_workspace_deliveries_ambiguous()
        if hasattr(self.store, "reset_stale_workspace_deliveries"):
            self.store.reset_stale_workspace_deliveries()
        if hasattr(self.store, "finalize_ready_provider_events"):
            self.store.finalize_ready_provider_events()
        self._workspace_delivery_recovery_done = True

    def _contain_provider_event_failure(
        self,
        row: dict[str, object],
        error: Exception,
    ) -> int:
        """Retry one failed journal row without terminating the live lane."""
        if self._is_retryable_database_conflict(error):
            raise error
        account_uuid = str(row["account_uuid"])
        queue_id = str(row["queue_id"])
        event_id = int(row["event_id"])
        reason = "provider_event_processing_failed"
        attempts = 1
        if row.get("processing_reason") == reason:
            attempts = int(row.get("retry_count") or 0) + 1
        self.store.mark_health("provider", "degraded", reason)
        if attempts >= self.PROVIDER_EVENT_FAILURE_MAX_ATTEMPTS:
            self.store.mark_provider_event_invalid(
                account_uuid,
                queue_id,
                event_id,
                reason,
            )
            return 1
        self.store.retry_provider_event(
            account_uuid,
            queue_id,
            event_id,
            reason,
        )
        return 0

    def process_provider_journal(
        self,
        rows: list[dict[str, object]] | None = None,
    ) -> int:
        self._recover_interrupted_workspace_deliveries_once()
        if rows is None:
            rows = self.store.pending_provider_events(self.PROVIDER_JOURNAL_QUANTUM)
            if (
                getattr(self, "provider_journal_parallel_enabled", False)
                and len(rows) > 1
            ):
                executor = getattr(self, "provider_journal_executor", None)
                if executor is None:
                    executor = concurrent.futures.ThreadPoolExecutor(
                        max_workers=self._provider_journal_worker_count(),
                        thread_name_prefix="workspace-zulip-journal",
                    )
                    self.provider_journal_executor = executor
                return sum(
                    executor.map(
                        lambda row: self.process_provider_journal([row]),
                        rows,
                    )
                )
        processed = 0
        supported_types = {
            "message",
            "update_message",
            "delete_message",
            "update_message_flags",
            "reaction",
            "subscription",
            "user_topic",
            "user_settings",
            "realm_user",
        }
        for row in rows:
            account_uuid = str(row["account_uuid"])
            queue_id = str(row["queue_id"])
            event_id = int(row["event_id"])
            event = typing.cast(dict[str, object], row["body"])
            if not self.store.account_is_active(account_uuid):
                ignore_inactive = getattr(
                    self.store,
                    "ignore_provider_event_for_inactive_account",
                    None,
                )
                if callable(ignore_inactive) and ignore_inactive(
                    account_uuid, queue_id, event_id
                ):
                    processed += 1
                continue
            try:
                adapter = self.provider_adapters(account_uuid)
            except zulip_adapter.ZulipOperationError as exc:
                authentication = self._handle_provider_account_error(
                    account_uuid, exc, exc.account_generation
                )
                if not authentication and not exc.retryable:
                    self.store.mark_health("provider", "degraded", exc.code)
                continue
            try:
                supported = str(event["type"]) in supported_types
                if row.get("assignment_catalog_reported_at") is None:
                    self._queue_event_catalog(
                        account_uuid,
                        event,
                        adapter.server_url,
                        (
                            (account_uuid, queue_id, event_id)
                            if event["type"] in {"message", "user_topic"}
                            else None
                        ),
                    )
                external_chat_uuid = uuid.UUID(int=0)
                if event["type"] == "message":
                    message = typing.cast(dict[str, object], event["message"])
                    _, chat_key = converter.provider_chat_reference(message)
                    # A live channel message contains enough identity data to
                    # create its author before the authoritative participant
                    # snapshot has converged. The converter still requires the
                    # selected stream/topic assignment and its owner; only bulk
                    # history must wait for every participant to be projected.
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
                    destination_stream_id = event.get("new_stream_id")
                    chat_key = (
                        f"channel:{int(destination_stream_id)}"
                        if destination_stream_id is not None
                        else None
                    )
                    if chat_key is None:
                        message_ids = event.get("message_ids", event.get("messages"))
                        if message_ids is None and event.get("message_id") is not None:
                            message_ids = [event["message_id"]]
                        first_message_id = next(
                            iter(typing.cast(list[object], message_ids or [])), None
                        )
                        if first_message_id is not None:
                            mapping = converter.provider_message_mapping(
                                self.store,
                                account_uuid,
                                str(first_message_id),
                            )
                            if mapping is not None:
                                metadata = typing.cast(
                                    dict[str, object], mapping["metadata"]
                                )
                                chat_key = str(metadata["chat_key"])
                    if chat_key is not None:
                        external_chat_uuid = uuid.UUID(
                            _AccountRouting(
                                self.store, account_uuid
                            ).external_chat_uuid(chat_key)
                        )
                elif event["type"] == "reaction":
                    provider_message_id = int(str(event["message_id"]))
                    mapping = converter.provider_message_mapping(
                        self.store,
                        account_uuid,
                        str(provider_message_id),
                    )
                    if mapping is None:
                        cached_message = row.get("provider_message_context")
                        if isinstance(cached_message, dict):
                            provider_message = typing.cast(
                                dict[str, object], cached_message
                            )
                        else:
                            fetched_message = adapter.message_by_id(provider_message_id)
                            if fetched_message is None:
                                raise ValueError("provider_message_not_found")
                            provider_message = self._reaction_message_context(
                                fetched_message
                            )
                            cache_message = getattr(
                                self.store,
                                "cache_provider_event_message_context",
                                None,
                            )
                            if callable(cache_message):
                                provider_message = cache_message(
                                    account_uuid,
                                    queue_id,
                                    event_id,
                                    provider_message,
                                )
                        if self._selected_provider_event_lane_changed(row, event):
                            # The fetched message moved this already-selected
                            # reaction into a chat lane whose older head may
                            # currently block it. Let the selector enforce
                            # that order before conversion starts.
                            continue
                        _, chat_key = converter.provider_chat_reference(
                            provider_message
                        )
                        try:
                            converter.provider_chat_assignment(
                                self.store, account_uuid, chat_key
                            )
                        except ValueError as exc:
                            if (
                                str(exc) == "provider_chat_assignment_pending"
                                and row.get("assignment_catalog_reported_at") is None
                            ):
                                # A reaction can be the first live event that
                                # exposes an older direct chat omitted from
                                # Zulip's registration snapshot. Publish it
                                # once and persist the publication boundary in
                                # the same storage transaction.
                                self._queue_event_catalog(
                                    account_uuid,
                                    {
                                        "type": "message",
                                        "message": provider_message,
                                    },
                                    adapter.server_url,
                                    (account_uuid, queue_id, event_id),
                                )
                            raise
                        provider_timestamp = provider_message.get("timestamp")
                        provider_message_time = None
                        if not isinstance(provider_timestamp, bool):
                            try:
                                provider_message_time = datetime.datetime.fromtimestamp(
                                    float(typing.cast(object, provider_timestamp)),
                                    datetime.UTC,
                                )
                            except (TypeError, ValueError, OverflowError):
                                pass
                        ignore_outside_history = getattr(
                            self.store,
                            "ignore_provider_reaction_outside_history_window",
                            None,
                        )
                        if (
                            provider_message_time is not None
                            and callable(ignore_outside_history)
                            and ignore_outside_history(
                                account_uuid,
                                chat_key,
                                str(provider_message_id),
                                provider_message_time,
                                queue_id,
                                event_id,
                            )
                        ):
                            processed += 1
                            continue
                        external_chat_uuid = uuid.UUID(
                            converter.stable_entity_uuid(
                                account_uuid, "external_chat", chat_key
                            )
                        )
                if self._selected_provider_event_lane_changed(row, event):
                    # Resolving the message moved this already-selected event
                    # into a chat lane whose older head may currently block it.
                    # Leave it pending so the selector can enforce that order.
                    continue
                records = self._event_records_with_pending_delete_recreations(
                    adapter,
                    row,
                    str(external_chat_uuid),
                    event,
                    "live",
                )
                if self._selected_provider_event_lane_changed(row, event):
                    # Conversion can observe a message mapping committed after
                    # the pre-conversion refresh. Re-select before persisting
                    # records so an older head in the resolved lane stays first.
                    continue
            except zulip_adapter.ZulipOperationError as exc:
                authentication = self._handle_provider_account_error(
                    account_uuid, exc, self._adapter_generation(adapter)
                )
                if authentication:
                    # Keep the durable journal head intact until reconnecting
                    # advances the account generation and opens the breaker.
                    continue
                if exc.retryable:
                    self.store.retry_provider_event(
                        account_uuid, queue_id, event_id, exc.code
                    )
                    continue
                self.store.mark_provider_event_invalid(
                    account_uuid, queue_id, event_id, exc.code
                )
                self.store.mark_health("provider", "degraded", exc.code)
                processed += 1
                continue
            except ValueError as exc:
                reason = str(exc)
                if reason == "provider_reaction_chat_assignment_timeout" or (
                    reason == "provider_chat_assignment_pending"
                    and event.get("type") == "reaction"
                    and self._reaction_assignment_wait_expired(row)
                ):
                    self.store.mark_provider_event_invalid(
                        account_uuid,
                        queue_id,
                        event_id,
                        "provider_reaction_chat_assignment_timeout",
                    )
                    processed += 1
                    continue
                if reason in {
                    "provider_chat_assignment_pending",
                    "provider_chat_participants_pending",
                    "provider_event_replay_incomplete",
                    "reaction_mapping_plan_changed",
                }:
                    self.store.retry_provider_event(
                        account_uuid,
                        queue_id,
                        event_id,
                        reason,
                    )
                    continue
                if reason in {
                    "provider_chat_not_selected",
                    "provider_message_not_found",
                }:
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
            except Exception as exc:
                processed += self._contain_provider_event_failure(row, exc)
                continue
            lane_guard = getattr(self.store, "provider_event_lane_guard", None)
            try:
                guard = (
                    lane_guard(
                        account_uuid,
                        queue_id,
                        event_id,
                        event,
                        row.get("causal_lane"),
                    )
                    if callable(lane_guard)
                    else contextlib.nullcontext(True)
                )
                with guard as lane_current:
                    if not lane_current:
                        continue
                    prepare_records = getattr(
                        self.store, "prepare_provider_event_records", None
                    )
                    if records and callable(prepare_records):
                        records = prepare_records(
                            account_uuid,
                            queue_id,
                            event_id,
                            records,
                        )
                    enqueue_event_records = getattr(
                        self.store, "enqueue_provider_event_records", None
                    )
                    event_enqueued_atomically = False
                    if records and callable(enqueue_event_records):
                        enqueue_event_records(
                            records,
                            0,
                            account_uuid,
                            queue_id,
                            event_id,
                        )
                        event_enqueued_atomically = True
                    else:
                        for record in records:
                            if hasattr(
                                self.store, "mark_provider_event_delivering"
                            ):
                                self.store.enqueue_workspace_delivery(
                                    record, 0, queue_id, event_id
                                )
                            else:
                                self.store.enqueue_workspace_delivery(record, 0)
                    deleted_message_ids: list[str] = []
                    if event.get("type") == "delete_message":
                        raw_ids = event.get("message_ids")
                        if raw_ids is None and event.get("message_id") is not None:
                            raw_ids = [event["message_id"]]
                        deleted_message_ids = [
                            str(value)
                            for value in typing.cast(list[object], raw_ids or [])
                        ]
                    if records and event_enqueued_atomically:
                        pass
                    elif records and hasattr(
                        self.store, "mark_provider_event_delivering"
                    ):
                        self.store.mark_provider_event_delivering(
                            account_uuid, queue_id, event_id
                        )
                    elif callable(lane_guard):
                        self.store.finalize_provider_event(
                            account_uuid,
                            queue_id,
                            event_id,
                            supported,
                            deleted_message_ids,
                        )
                    else:
                        finalize_if_lane_current = getattr(
                            self.store,
                            "finalize_provider_event_if_lane_current",
                            None,
                        )
                        if callable(finalize_if_lane_current):
                            finalized = finalize_if_lane_current(
                                account_uuid,
                                queue_id,
                                event_id,
                                event,
                                row.get("causal_lane"),
                                supported,
                                deleted_message_ids,
                            )
                            if not finalized:
                                continue
                        else:
                            self.store.finalize_provider_event(
                                account_uuid,
                                queue_id,
                                event_id,
                                supported,
                                deleted_message_ids,
                            )
            except ValueError as exc:
                if str(exc) in {
                    "provider_message_mapping_changed",
                    "reaction_mapping_plan_changed",
                }:
                    self.store.retry_provider_event(
                        account_uuid,
                        queue_id,
                        event_id,
                        str(exc),
                    )
                    continue
                if str(exc) != "provider_chat_assignment_pending":
                    processed += self._contain_provider_event_failure(row, exc)
                    continue
                finalize_assignment_change = getattr(
                    self.store,
                    "finalize_provider_event_assignment_changed",
                    None,
                )
                if callable(finalize_assignment_change):
                    requeued = finalize_assignment_change(
                        account_uuid,
                        queue_id,
                        event_id,
                    )
                    if requeued is True:
                        # The recovered create must run before any later edit
                        # that was already present in this fetched journal page.
                        break
                    processed += 1
                else:
                    self.store.retry_provider_event(
                        account_uuid,
                        queue_id,
                        event_id,
                        str(exc),
                    )
                continue
            except Exception as exc:
                processed += self._contain_provider_event_failure(row, exc)
                continue
            processed += 1
        return processed

    def _provider_journal_worker_count(self) -> int:
        """Scale conversion concurrency with the deployment batch profile."""
        configured_batch = max(1, int(getattr(self, "provider_batch_size", 20)))
        return max(
            1,
            min(self.PROVIDER_JOURNAL_WORKERS, configured_batch // 5),
        )

    def _live_delivery_batch_size(self) -> int:
        """Keep small defaults while letting large profiles amortize HTTP commits."""
        configured_batch = max(1, int(getattr(self, "provider_batch_size", 20)))
        return min(self.LIVE_DELIVERY_BATCH_SIZE, configured_batch)

    def _history_worker_count(self) -> int:
        """Scale history discovery without oversubscribing small deployments."""
        configured_batch = max(1, int(getattr(self, "provider_batch_size", 20)))
        return max(
            2,
            min(self.BACKGROUND_HISTORY_WORKERS, (configured_batch + 9) // 10),
        )

    def _history_delivery_batch_size(self, live_pending: bool) -> int:
        configured_batch = max(1, int(getattr(self, "provider_batch_size", 20)))
        ceiling = (
            self.HISTORY_LIVE_DELIVERY_BATCH_SIZE
            if live_pending
            else self.HISTORY_DELIVERY_BATCH_SIZE
        )
        return min(ceiling, configured_batch)

    def _history_message_uses_file_transfer(
        self, message: dict[str, object]
    ) -> bool:
        return (
            getattr(self, "file_client", None) is not None
            and "/user_uploads/" in str(message.get("content", ""))
        )

    @staticmethod
    def _backfill_catalog_key(message: dict[str, object]) -> tuple[object, ...]:
        chat_type, chat_key = converter.provider_chat_reference(message)
        recipient = message.get("display_recipient")
        if chat_type == "channel":
            subject = message.get("subject")
            topic_name = (
                converter.channel_topic_name(subject)
                if isinstance(subject, str)
                else None
            )
            return (chat_key, str(recipient), topic_name)
        if isinstance(recipient, list):
            participants = tuple(
                (
                    str(person.get("id")),
                    str(person.get("full_name")),
                    str(person.get("email")),
                    bool(person.get("is_me")),
                    str(person.get("is_active")),
                )
                for person in recipient
                if isinstance(person, dict) and isinstance(person.get("id"), int)
            )
            return (chat_key, participants)
        return (chat_key, str(recipient))

    def _backfill_message_chunks(
        self,
        messages: list[dict[str, object]],
        *,
        isolate_file_transfers: bool = False,
    ) -> typing.Iterator[list[dict[str, object]]]:
        offset = 0
        while offset < len(messages):
            transaction_size = (
                1
                if self._live_workspace_delivery_pending()
                else self.BACKFILL_ENQUEUE_TRANSACTION_MESSAGES
            )
            chunk_end = min(len(messages), offset + transaction_size)
            if isolate_file_transfers:
                if self._history_message_uses_file_transfer(messages[offset]):
                    chunk_end = offset + 1
                else:
                    for candidate in range(offset + 1, chunk_end):
                        if self._history_message_uses_file_transfer(
                            messages[candidate]
                        ):
                            chunk_end = candidate
                            break
            chunk = messages[offset:chunk_end]
            yield chunk
            offset += len(chunk)

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
        topic_cache = getattr(self, "backfill_topic_cache", set())
        self.backfill_topic_cache = topic_cache
        ordered_messages = converter.newest_first(messages)
        transaction = getattr(self.store, "transaction", contextlib.nullcontext)
        catalog_keys: set[tuple[object, ...]] = set()
        for message_chunk in self._backfill_message_chunks(ordered_messages):
            with transaction():
                for message in message_chunk:
                    catalog_key = self._backfill_catalog_key(message)
                    if catalog_key in catalog_keys:
                        continue
                    self._queue_event_catalog(
                        account_uuid,
                        {
                            "id": int(message["id"]),
                            "type": "message",
                            "message": message,
                        },
                        adapter.server_url,
                    )
                    catalog_keys.add(catalog_key)
        for message_chunk in self._backfill_message_chunks(
            ordered_messages,
            isolate_file_transfers=True,
        ):
            durable_topic_cache_keys = set()
            with transaction():
                conversion_store = _BackfillConversionStore(self.store)
                for message in message_chunk:
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
                        conversion_store,
                    )
                    for record in records:
                        operation = typing.cast(
                            dict[str, object],
                            record.get("operation", {}),
                        )
                        topic_cache_key = None
                        if operation.get("kind") == "topic.upsert":
                            topic_cache_key = (
                                account_uuid,
                                str(assignment["uuid"]),
                                int(assignment["generation"]),
                                str(operation["entity_uuid"]),
                            )
                            if (
                                topic_cache_key in topic_cache
                                or topic_cache_key in durable_topic_cache_keys
                            ):
                                continue
                        try:
                            enqueued += int(
                                self.store.enqueue_workspace_delivery(record, 2)
                            )
                        except ValueError as exc:
                            if (
                                str(exc)
                                != "Operation UUID reused with a different digest"
                            ):
                                raise
                            # A repeated history page can contain the current
                            # revision of a message whose deterministic backfill
                            # operation was already accepted from an earlier
                            # snapshot. Keep that first operation canonical; live
                            # queue recovery carries later edits separately.
                        if topic_cache_key is not None:
                            durable_topic_cache_keys.add(topic_cache_key)
            # Cache only keys committed by the transaction. A retry after rollback
            # must still be able to enqueue the first durable topic projection.
            topic_cache.update(durable_topic_cache_keys)
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
        adapter: zulip_adapter.OfficialZulipAdapter | None = None
        try:
            adapter = self.provider_adapters(account_uuid)
            anchor = "newest" if job["next_anchor"] is None else int(job["next_anchor"])
            messages = adapter.message_history(provider_chat_key, anchor=anchor)
        except zulip_adapter.ZulipOperationError as exc:
            authentication = self._handle_provider_account_error(
                account_uuid, exc, self._adapter_generation(adapter)
            )
            if authentication:
                self.store.release_backfill_job(account_uuid, provider_chat_key)
                return True
            if not exc.retryable:
                self.store.fail_backfill_job(
                    account_uuid,
                    provider_chat_key,
                    exc.code,
                )
                self.store.mark_health(
                    storage.backfill_health_component(account_uuid, provider_chat_key),
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
            authentication = self._handle_provider_account_error(
                account_uuid, exc, self._adapter_generation(adapter)
            )
            if authentication:
                self.store.release_backfill_job(account_uuid, provider_chat_key)
                return True
            if not exc.retryable:
                self.store.fail_backfill_job(
                    account_uuid,
                    provider_chat_key,
                    exc.code,
                )
                self.store.mark_health(
                    storage.backfill_health_component(account_uuid, provider_chat_key),
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
        except Exception as exc:
            if not self._is_retryable_database_conflict(exc):
                raise
            # Concurrent live and history projections can update the same
            # provider mapping in opposite order. PostgreSQL rolls one whole
            # transaction back; release the durable job so another lane can
            # retry it instead of restarting the bridge process.
            self.store.release_backfill_job(account_uuid, provider_chat_key)
            return True
        complete = (
            reached_cutoff
            or len(messages) < zulip_adapter.HISTORY_PAGE_SIZE
            or not messages
        )
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
        self._record_provider_account_success(
            account_uuid, self._adapter_generation(adapter)
        )
        return True

    def _provider_delivery_delay(self, now: float | None = None) -> float:
        if now is None:
            now = time.monotonic()
        return max(0.0, getattr(self, "provider_delivery_retry_after", 0.0) - now)

    def _defer_provider_delivery(
        self,
        error: BaseException,
        now: float,
    ) -> None:
        lock = getattr(self, "provider_delivery_retry_lock", None)
        if lock is None:
            lock = threading.Lock()
            self.provider_delivery_retry_lock = lock
        with lock:
            attempts = getattr(self, "provider_delivery_retry_attempts", 0) + 1
            self.provider_delivery_retry_attempts = attempts
            base = self.PROVIDER_DELIVERY_RETRY_BASE_SECONDS
            cap = self.PROVIDER_DELIVERY_RETRY_CAP_SECONDS
            ceiling = min(cap, base * (2 ** min(attempts - 1, 30)))
            retry_random = getattr(self, "provider_delivery_random", random)
            delay = retry_random.uniform(ceiling / 2, ceiling)
            if isinstance(error, provider_api.ProviderApiRetryableError):
                retry_after = error.retry_after_seconds
                if retry_after is not None:
                    delay = max(
                        delay,
                        min(
                            retry_after, self.PROVIDER_DELIVERY_RETRY_AFTER_CAP_SECONDS
                        ),
                    )
            self.provider_delivery_retry_after = max(
                getattr(self, "provider_delivery_retry_after", 0.0),
                now + delay,
            )

    def _clear_provider_delivery_retry(self) -> None:
        lock = getattr(self, "provider_delivery_retry_lock", None)
        if lock is None:
            lock = threading.Lock()
            self.provider_delivery_retry_lock = lock
        with lock:
            self.provider_delivery_retry_attempts = 0
            self.provider_delivery_retry_after = 0.0

    def _provider_delivery_mutex(self) -> threading.Lock:
        lock = getattr(self, "provider_delivery_lock", None)
        if lock is None:
            lock = threading.Lock()
            self.provider_delivery_lock = lock
        return lock

    def flush_provider_events(
        self,
        minimum_priority: int = 0,
        maximum_priority: int = 2,
        limit: int = 100,
    ) -> int:
        with self._provider_delivery_mutex():
            if self._provider_delivery_delay() > 0:
                return 0
            return self._flush_provider_events_locked(
                minimum_priority=minimum_priority,
                maximum_priority=maximum_priority,
                limit=limit,
            )

    def _flush_history_events(self) -> tuple[int, int, bool]:
        """Submit history only after rechecking live work under the HTTP mutex."""
        with self._provider_delivery_mutex():
            live_pending = self._live_workspace_delivery_pending()
            history_batch_size = self._history_delivery_batch_size(live_pending)
            if live_pending and not self._claim_history_quantum():
                return 0, history_batch_size, False
            if self._provider_delivery_delay() > 0:
                return 0, history_batch_size, False
            return (
                self._flush_provider_events_locked(
                    minimum_priority=2,
                    maximum_priority=2,
                    limit=history_batch_size,
                    prioritize_live_between_rejections=True,
                ),
                history_batch_size,
                True,
            )

    def _flush_provider_events_locked(
        self,
        minimum_priority: int,
        maximum_priority: int,
        limit: int,
        *,
        prioritize_live_between_rejections: bool = False,
        defer_retryable_failure: bool = True,
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
                committed = self._apply_provider_event_records(
                    event_records,
                    prioritize_live_between_rejections=(
                        prioritize_live_between_rejections
                    ),
                )
            else:
                committed = 0
        except (httpx.TransportError, provider_api.ProviderApiRetryableError) as error:
            if hasattr(self.store, "release_provider_event_submissions"):
                self.store.release_provider_event_submissions(submitting_record_uuids)
            if defer_retryable_failure:
                self._defer_provider_delivery(error, time.monotonic())
            raise
        except Exception:
            if hasattr(self.store, "release_provider_event_submissions"):
                self.store.release_provider_event_submissions(submitting_record_uuids)
            raise
        for record in completed_without_event:
            result = scheduler.result_record(
                record,
                "committed",
                scheduler.TargetCommit(None, None),
                None,
            )
            self.store.accept_result(result)
        if hasattr(self.store, "finalize_ready_provider_events"):
            self.store.finalize_ready_provider_events()
        self._clear_provider_delivery_retry()
        return len(completed_without_event) + committed

    def _apply_provider_event_records(
        self,
        event_records: list[tuple[dict[str, object], dict[str, object]]],
        *,
        prioritize_live_between_rejections: bool = False,
    ) -> int:
        """Apply in order, isolating only permanently rejected records."""
        try:
            response = self.provider_api.apply_events(
                [event for _record, event in event_records]
            )
        except provider_api.ProviderEventRejectedError as exc:
            if len(event_records) > 1:
                midpoint = len(event_records) // 2
                committed = 0
                for subset in (
                    event_records[:midpoint],
                    event_records[midpoint:],
                ):
                    if (
                        prioritize_live_between_rejections
                        and self._ready_live_workspace_delivery_pending()
                    ):
                        self._flush_provider_events_locked(
                            minimum_priority=0,
                            maximum_priority=0,
                            limit=self._live_delivery_batch_size(),
                            defer_retryable_failure=False,
                        )
                    committed += self._apply_provider_event_records(
                        subset,
                        prioritize_live_between_rejections=(
                            prioritize_live_between_rejections
                        ),
                    )
                return committed
            record, _event = event_records[0]
            error_code = f"provider_api_http_{exc.status_code}"
            rejected = self.store.reject_provider_event_submission(
                str(record["record_uuid"]),
                error_code,
            )
            if not rejected:
                raise RuntimeError(
                    "Provider event rejection could not be quarantined"
                ) from exc
            self.store.mark_health(
                "provider_api",
                "degraded",
                "provider_event_rejected",
            )
            return 0
        results = typing.cast(list[dict[str, object]], response["results"])
        expected = [
            str(event["provider_event_uuid"]) for _record, event in event_records
        ]
        actual = [str(result["provider_event_uuid"]) for result in results]
        if actual != expected:
            raise ValueError("Provider event response does not match request order")
        if any(result["status"] != "applied" for result in results):
            raise ValueError("Provider event batch was not applied atomically")
        for record, _event in event_records:
            result = scheduler.result_record(
                record,
                "committed",
                scheduler.TargetCommit(None, None),
                None,
            )
            self.store.accept_result(result)
        return len(event_records)

    def tick(self) -> bool:
        background_heartbeat_error = getattr(self, "background_heartbeat_error", None)
        if background_heartbeat_error is not None:
            raise RuntimeError("Background heartbeat lane failed") from (
                background_heartbeat_error
            )
        background_error = getattr(self, "background_history_error", None)
        if background_error is not None:
            raise RuntimeError("Background history lane failed") from background_error
        background_live_error = getattr(self, "background_live_error", None)
        if background_live_error is not None:
            raise RuntimeError("Background live lane failed") from background_live_error
        background_live_delivery_error = getattr(
            self, "background_live_delivery_error", None
        )
        if background_live_delivery_error is not None:
            raise RuntimeError("Background live delivery lane failed") from (
                background_live_delivery_error
            )
        now = time.monotonic()
        progressed = False
        if now - self.last_certificate_check >= 3600.0:
            progressed |= self._renew_certificate(False)
            self.last_certificate_check = now
        if not getattr(self, "background_heartbeat_enabled", False):
            progressed |= self._run_heartbeat(now)
        progressed |= self._run_control_poll(now)
        store = getattr(self, "store", None)
        last_history_lease_reap = getattr(self, "last_history_lease_reap", 0.0)
        if (
            store is not None
            and hasattr(store, "reap_expired_history_leases")
            and now - last_history_lease_reap
            >= self.HISTORY_LEASE_REAP_INTERVAL_SECONDS
        ):
            progressed |= store.reap_expired_history_leases() > 0
            self.last_history_lease_reap = now
        last_reconcile = getattr(self, "last_control_state_reconcile", 0.0)
        control_state_dirty = getattr(self, "control_state_dirty", True)
        if control_state_dirty or (
            now - last_reconcile >= self.CONTROL_STATE_RECONCILE_INTERVAL_SECONDS
        ):
            if store is not None and hasattr(store, "reconcile_participant_sync"):
                store.reconcile_participant_sync()
            if store is not None and hasattr(store, "reconcile_backfill_jobs"):
                store.reconcile_backfill_jobs()
            self.last_control_state_reconcile = now
            self.control_state_dirty = False
        live_progressed = False
        if not getattr(self, "background_live_enabled", False):
            live_progressed |= self._run_live_lane_once(before_provider_poll=True)
        last_provider_poll = getattr(self, "last_provider_poll", 0.0)
        provider_poll_interval = getattr(
            self,
            "provider_poll_interval_seconds",
            self.PROVIDER_POLL_INTERVAL_SECONDS,
        )
        if now - last_provider_poll >= provider_poll_interval:
            live_progressed |= self.poll_provider_events() > 0
            self.last_provider_poll = now
        if hasattr(self, "store"):
            live_progressed |= self._flush_observed_reports(now) > 0
        if store is not None and hasattr(store, "claim_participant_sync"):
            live_progressed |= self.refresh_selected_participants_once()
        if not getattr(self, "background_live_enabled", False):
            live_progressed |= self._run_live_lane_once(before_provider_poll=False)
        progressed |= live_progressed
        last_history_quantum = getattr(self, "last_history_quantum", now)
        history_due = (
            now - last_history_quantum >= self.HISTORY_QUANTUM_INTERVAL_SECONDS
        )
        if not getattr(self, "background_history_enabled", False) and (
            not live_progressed or history_due
        ):
            progressed |= self._run_history_lane_once()
        if (
            store is not None
            and hasattr(store, "prune_terminal_delivery_state")
            and now - self.last_terminal_state_prune
            >= self.TERMINAL_STATE_PRUNE_INTERVAL_SECONDS
        ):
            deleted_deliveries, deleted_events = store.prune_terminal_delivery_state()
            progressed |= deleted_deliveries + deleted_events > 0
            self.last_terminal_state_prune = now
        self.health_file.parent.mkdir(parents=True, exist_ok=True)
        self.health_file.write_text(
            datetime.datetime.now(datetime.UTC).isoformat(),
            encoding="utf-8",
        )
        return progressed

    def _run_live_lane_once(self, *, before_provider_poll: bool | None = None) -> bool:
        """Run live provider traffic independently from catalog and history I/O."""
        progressed = False
        if before_provider_poll is None:
            # The dedicated background lane must not let a slow outbound lease
            # postpone already-journaled Zulip events. Bound the inbound quantum,
            # then give Workspace-to-Zulip operations their turn below.
            progressed |= self.process_provider_journal() > 0
        if before_provider_poll is not False:
            try:
                progressed |= self.poll_provider_operations() > 0
            except (httpx.TransportError, provider_api.ProviderApiRetryableError):
                self.store.mark_health(
                    "provider_api", "degraded", "provider_api_unavailable"
                )
        if before_provider_poll is True:
            return progressed
        if before_provider_poll is not None:
            progressed |= self.process_provider_journal() > 0
        # Provider operations and their HTTP results always outrank history I/O.
        progressed |= self.scheduler.reconcile_once()
        progressed |= self.scheduler.run_once()
        try:
            progressed |= self.flush_provider_results() > 0
        except (httpx.TransportError, provider_api.ProviderApiRetryableError):
            self.store.mark_health(
                "provider_api", "degraded", "provider_api_unavailable"
            )
        # Keep the live event quantum bounded so Provider HTTP cannot monopolize a tick.
        store = getattr(self, "store", None)
        readiness_probe = getattr(store, "has_pending_workspace_deliveries", None)
        legacy_pending = getattr(store, "pending_workspace_deliveries", None)
        readiness_unknown = not callable(readiness_probe) and legacy_pending is None
        if not getattr(self, "background_live_delivery_enabled", False) and (
            readiness_unknown or self._ready_live_workspace_delivery_pending()
        ):
            try:
                progressed |= (
                    self.flush_provider_events(
                        minimum_priority=0,
                        maximum_priority=0,
                        limit=self._live_delivery_batch_size(),
                    )
                    > 0
                )
            except (httpx.TransportError, provider_api.ProviderApiRetryableError):
                self.store.mark_health(
                    "provider_api", "degraded", "provider_api_unavailable"
                )
        return progressed

    def _run_history_lane_once(self) -> bool:
        """Drain one durable history batch, then discover at most one page."""
        history_delivered = 0
        history_batch_size = 1
        history_permitted = False
        history_delivery_lock = getattr(self, "history_delivery_lock", None)
        if history_delivery_lock is None:
            history_delivery_lock = threading.Lock()
            self.history_delivery_lock = history_delivery_lock
        with history_delivery_lock:
            try:
                (
                    history_delivered,
                    history_batch_size,
                    history_permitted,
                ) = self._flush_history_events()
                if not history_permitted:
                    return False
            except (httpx.TransportError, provider_api.ProviderApiRetryableError):
                self.store.mark_health(
                    "provider_api", "degraded", "provider_api_unavailable"
                )
        progressed = history_delivered > 0
        if history_delivered < history_batch_size:
            progressed |= self._run_history_quantum_once()
        return progressed

    def _claim_history_quantum(self) -> bool:
        """Let exactly one history worker progress during sustained live load."""
        lock = getattr(self, "history_quantum_lock", None)
        if lock is None:
            lock = threading.Lock()
            self.history_quantum_lock = lock
        with lock:
            now = time.monotonic()
            if (
                now - getattr(self, "last_history_quantum", now)
                < self.HISTORY_QUANTUM_INTERVAL_SECONDS
            ):
                return False
            self.last_history_quantum = now
            return True

    @classmethod
    def _is_retryable_database_conflict(cls, error: BaseException) -> bool:
        current: BaseException | None = error
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if (
                getattr(current, "sqlstate", None)
                in cls.RETRYABLE_DATABASE_CONFLICT_CODES
                or getattr(current, "code", None)
                in cls.RETRYABLE_DATABASE_CONFLICT_CODES
            ):
                return True
            current = current.__cause__ or current.__context__
        return False

    def _run_background_history_lane(self) -> None:
        while True:
            try:
                if self._run_history_lane_once():
                    # Let live threads acquire the GIL and shared Provider mutex
                    # between otherwise continuous idle-history batches.
                    time.sleep(self.HISTORY_PROGRESS_YIELD_SECONDS)
                else:
                    time.sleep(0.5)
            except Exception as error:
                if self._is_retryable_database_conflict(error):
                    time.sleep(random.uniform(0.05, 0.25))
                    continue
                self.background_history_error = error
                return

    def _run_background_heartbeat_lane(self) -> None:
        """Keep provider capability leases fresh during slow catalog work."""
        while True:
            try:
                self._run_heartbeat(time.monotonic())
            except Exception as error:
                self.background_heartbeat_error = error
                return
            time.sleep(0.5)

    def _run_background_live_lane(self) -> None:
        while True:
            try:
                if not self._run_live_lane_once():
                    time.sleep(0.1)
            except Exception as error:
                if self._is_retryable_database_conflict(error):
                    time.sleep(random.uniform(0.05, 0.25))
                    continue
                self.background_live_error = error
                return

    def _record_live_delivery_stall(self, now: float | None = None) -> None:
        """Expose a durable dependency stall without hot-looping health writes."""
        if now is None:
            now = time.monotonic()
        stalled_since = getattr(self, "live_delivery_stalled_since", None)
        if stalled_since is None:
            self.live_delivery_stalled_since = now
            return
        if getattr(self, "live_delivery_stall_reported", False):
            return
        if now - stalled_since < self.LIVE_DELIVERY_STALL_THRESHOLD_SECONDS:
            return
        self.store.mark_health(
            "provider_delivery",
            "degraded",
            "workspace_delivery_dependency_stalled",
        )
        self.live_delivery_stall_reported = True

    def _clear_live_delivery_stall(self) -> None:
        if getattr(self, "live_delivery_stall_reported", False):
            self.store.mark_health("provider_delivery", "healthy")
        self.live_delivery_stalled_since = None
        self.live_delivery_stall_reported = False

    def _run_background_live_delivery_lane(self) -> None:
        """Submit durable live records without racing Zulip journal conversion."""
        while True:
            try:
                retry_delay = self._provider_delivery_delay()
                if retry_delay > 0:
                    time.sleep(min(retry_delay, 1.0))
                    continue
                pending = getattr(self.store, "has_pending_workspace_deliveries", None)
                if callable(pending) and not pending(0, 0):
                    self._clear_live_delivery_stall()
                    time.sleep(0.05)
                    continue
                delivered = self.flush_provider_events(
                    minimum_priority=0,
                    maximum_priority=0,
                    limit=self._live_delivery_batch_size(),
                )
                if delivered:
                    self._clear_live_delivery_stall()
                else:
                    self._record_live_delivery_stall()
                    # The cheap readiness probe intentionally ignores causal
                    # dependencies.  Avoid hot-looping the full selector while
                    # a read or reaction waits for its message mapping.
                    time.sleep(self.LIVE_DELIVERY_DEPENDENCY_RECHECK_SECONDS)
            except (httpx.TransportError, provider_api.ProviderApiRetryableError):
                self.store.mark_health(
                    "provider_api", "degraded", "provider_api_unavailable"
                )
                time.sleep(0.1)
            except Exception as error:
                if self._is_retryable_database_conflict(error):
                    time.sleep(random.uniform(0.05, 0.25))
                    continue
                self.background_live_delivery_error = error
                return

    def run(self) -> None:
        release_dependency_gates = getattr(
            self.store,
            "release_dependency_gated_provider_events",
            None,
        )
        if callable(release_dependency_gates):
            release_dependency_gates()
        self._recover_interrupted_workspace_deliveries_once()
        self.provider_journal_parallel_enabled = True
        self.background_live_enabled = True
        self.background_live_delivery_enabled = True
        self.background_heartbeat_enabled = True
        self.background_heartbeat_error = None
        threading.Thread(
            target=self._run_background_heartbeat_lane,
            name="workspace-zulip-heartbeat",
            daemon=True,
        ).start()
        self.background_live_error = None
        for worker_index in range(self.BACKGROUND_LIVE_WORKERS):
            threading.Thread(
                target=self._run_background_live_lane,
                name=f"workspace-zulip-live-{worker_index}",
                daemon=True,
            ).start()
        self.background_live_delivery_error = None
        for worker_index in range(self.BACKGROUND_LIVE_DELIVERY_WORKERS):
            threading.Thread(
                target=self._run_background_live_delivery_lane,
                name=f"workspace-zulip-live-delivery-{worker_index}",
                daemon=True,
            ).start()
        self.background_history_enabled = True
        self.background_history_error = None
        for worker_index in range(self._history_worker_count()):
            threading.Thread(
                target=self._run_background_history_lane,
                name=f"workspace-zulip-history-{worker_index}",
                daemon=True,
            ).start()
        while True:
            try:
                progressed = self.tick()
            except Exception as error:
                if not self._is_retryable_database_conflict(error):
                    raise
                time.sleep(random.uniform(0.05, 0.25))
                continue
            if not progressed:
                time.sleep(0.5)
