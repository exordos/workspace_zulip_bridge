import copy
import dataclasses
import datetime
import uuid

from workspace_zulip_bridge import scheduler, storage, zulip_adapter


def test_result_record_maps_internal_adapter_codes_to_wire_safe_error(operation_record):
    result = scheduler.result_record(
        operation_record, "rejected", None, "missing_send_correlation"
    )
    assert result["result"]["safe_error"] == {
        "code": "internal_error",
        "message": "The provider operation failed.",
    }


class FakeStore:
    def __init__(self, record):
        self.item = storage.QueuedOperation(uuid.uuid4(), record, 0, 0)
        self.completed = []
        self.retries = []
        self.correlations = []
        self.uncertain = []
        self.reap_count = 0
        self.terminal = None

    def reap_expired_running(self):
        self.reap_count += 1
        return 0

    def claim(self, worker_id, lease_seconds=60):
        item, self.item = self.item, None
        return item

    def claim_terminal(self, worker_id, lease_seconds=60):
        terminal, self.terminal = self.terminal, None
        return terminal

    def provider_event_cursor(self, account_uuid):
        return {"queue_id": "queue-1", "last_event_id": 7}

    def complete(self, item, result, outcome):
        self.completed.append((item, result, outcome))

    def retry(self, item, available_at, code):
        self.retries.append((item, available_at, code))

    def record_provider_attempt(
        self, item, queue_id, local_id, last_event_id, provider_rendered_content
    ):
        self.correlations.append(
            (queue_id, local_id, last_event_id, provider_rendered_content)
        )

    def mark_uncertain(self, item, code):
        self.uncertain.append((item, code))

    def uncertain_by_local_id(self, account_uuid, queue_id, local_id):
        return self.item

    def claim_uncertain(self, worker_id):
        item, self.item = self.item, None
        return item

    def schedule_reconciliation_check(self, item, after, evidence):
        self.reconciliation_check = (item, after, evidence)

    def schedule_single_resend(self, item, evidence):
        self.resend = (item, evidence)

    def require_operation_manual_reconciliation(self, item, code, evidence):
        self.manual = (item, code, evidence)


@dataclasses.dataclass
class FakeAdapter:
    outcome: str = "success"

    def restore_queue(self, queue_id, last_event_id):
        self.queue = (queue_id, last_event_id)

    def prepare(self, operation, operation_uuid, provider_rendered_content=None):
        return zulip_adapter.SendCorrelation(
            "queue-1", operation_uuid, 7, provider_rendered_content or "rendered"
        )

    def apply(self, operation, correlation):
        if self.outcome == "ambiguous":
            raise zulip_adapter.ZulipAmbiguousOutcome("provider_send_outcome_unknown")
        if self.outcome == "retry":
            raise zulip_adapter.ZulipOperationError("rate_limit_hit", True)
        return "99", "1"

    def reconcile_message(
        self, operation, attempted_at, provider_rendered_content=None
    ):
        if self.outcome == "match":
            return zulip_adapter.ReconciliationEvidence("now", ("99",), 1, "99")
        if self.outcome == "equivalent_matches":
            return zulip_adapter.ReconciliationEvidence("now", ("98", "99"), 2, "98")
        return zulip_adapter.ReconciliationEvidence("now", (), 0, None)


def test_zb_msg_003_provider_correlation_is_durable_before_commit(operation_record):
    store = FakeStore(operation_record)
    worker = scheduler.Scheduler(store, lambda _: FakeAdapter(), "worker")
    assert worker.run_once()
    assert store.correlations == [
        ("queue-1", operation_record["operation_uuid"], 7, "rendered")
    ]
    assert store.completed[0][2] == "committed"
    assert store.reap_count == 1


def test_zb_msg_003_ambiguous_send_is_not_automatically_retried(operation_record):
    store = FakeStore(operation_record)
    worker = scheduler.Scheduler(store, lambda _: FakeAdapter("ambiguous"), "worker")
    assert worker.run_once()
    assert store.uncertain
    assert not store.retries
    assert not store.completed


def test_zb_rel_002_retryable_failure_uses_pending_retry(operation_record):
    store = FakeStore(operation_record)
    worker = scheduler.Scheduler(store, lambda _: FakeAdapter("retry"), "worker")
    assert worker.run_once()
    assert store.retries[0][2] == "rate_limit_hit"


def test_zb_rel_002_expired_work_never_reaches_provider(operation_record):
    record = copy.deepcopy(operation_record)
    record["expires_at"] = (
        datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=1)
    ).isoformat()
    store = FakeStore(record)
    called = []
    worker = scheduler.Scheduler(store, lambda _: called.append(True), "worker")
    assert worker.run_once()
    assert store.completed[0][2] == "expired"
    assert not called


def test_expired_pending_is_terminally_swept_before_normal_claim(operation_record):
    store = FakeStore(operation_record)
    item = store.item
    store.item = None
    store.terminal = (item, "expired")
    worker = scheduler.Scheduler(store, lambda _: None, "worker")

    assert worker.run_once()

    assert store.completed[0][2] == "expired"
    assert store.completed[0][1]["result"]["manual_retry_allowed"] is True


def _uncertain_item(record, checks=0, resends=0):
    return storage.QueuedOperation(
        uuid.uuid4(),
        record,
        0,
        provider_attempted_at=datetime.datetime.now(datetime.UTC),
        auto_resend_count=resends,
        reconciliation_check_count=checks,
        provider_rendered_content="rendered",
    )


def test_zb_msg_003_late_exact_match_commits_without_resend(operation_record):
    store = FakeStore(operation_record)
    store.item = _uncertain_item(operation_record, checks=1)
    worker = scheduler.Scheduler(store, lambda _: FakeAdapter("match"), "worker")
    assert worker.reconcile_once()
    assert store.completed[0][2] == "committed"
    assert not hasattr(store, "resend")


def test_zb_msg_003_no_match_after_three_checks_allows_one_resend(operation_record):
    store = FakeStore(operation_record)
    store.item = _uncertain_item(operation_record, checks=2)
    worker = scheduler.Scheduler(store, lambda _: FakeAdapter("no_match"), "worker")
    assert worker.reconcile_once()
    assert store.resend[1]["match_count"] == 0


def test_zb_msg_003_equivalent_matches_commit_deterministic_choice(operation_record):
    store = FakeStore(operation_record)
    store.item = _uncertain_item(operation_record)
    worker = scheduler.Scheduler(
        store, lambda _: FakeAdapter("equivalent_matches"), "worker"
    )
    assert worker.reconcile_once()
    assert store.completed[0][1]["result"]["provider_entity_id"] == "98"
    assert not hasattr(store, "manual")


def test_zb_msg_003_second_no_match_never_auto_resends_again(operation_record):
    store = FakeStore(operation_record)
    store.item = _uncertain_item(operation_record, checks=2, resends=1)
    worker = scheduler.Scheduler(store, lambda _: FakeAdapter("no_match"), "worker")
    assert worker.reconcile_once()
    assert store.manual[1] == "no_match_after_auto_resend"


def test_manual_reconciliation_reasons_match_public_backend_contract(
    operation_record,
):
    emitted = set()

    missing_store = FakeStore(operation_record)
    missing_store.item = storage.QueuedOperation(
        uuid.uuid4(), operation_record, 0, provider_attempted_at=None
    )
    scheduler.Scheduler(
        missing_store, lambda _: FakeAdapter("no_match"), "worker"
    ).reconcile_once()
    emitted.add(missing_store.manual[1])

    unavailable_store = FakeStore(operation_record)
    unavailable_store.item = _uncertain_item(operation_record)

    class UnavailableAdapter:
        def reconcile_message(
            self, operation, attempted_at, provider_rendered_content=None
        ):
            raise zulip_adapter.ZulipOperationError("provider_unavailable", False)

    scheduler.Scheduler(
        unavailable_store, lambda _: UnavailableAdapter(), "worker"
    ).reconcile_once()
    emitted.add(unavailable_store.manual[1])

    exhausted_store = FakeStore(operation_record)
    exhausted_store.item = _uncertain_item(operation_record, checks=2, resends=1)
    scheduler.Scheduler(
        exhausted_store, lambda _: FakeAdapter("no_match"), "worker"
    ).reconcile_once()
    emitted.add(exhausted_store.manual[1])

    assert emitted == scheduler.Scheduler.RECONCILIATION_REASONS
