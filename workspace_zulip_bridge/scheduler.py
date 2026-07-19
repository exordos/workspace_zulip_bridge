import dataclasses
import datetime
import random
import typing
import uuid

from workspace_zulip_bridge import storage, zulip_adapter

SAFE_ERROR_CODES = {
    "invalid_record",
    "unauthorized_account",
    "project_mismatch",
    "chat_not_selected",
    "capability_missing",
    "unsupported_operation",
    "not_found",
    "permission_denied",
    "conflict",
    "rate_limited",
    "provider_unavailable",
    "workspace_unavailable",
    "expired",
    "cancelled",
    "internal_error",
}


@dataclasses.dataclass(frozen=True)
class TargetCommit:
    entity_id: str | None
    revision: str | None


def result_record(
    operation: dict[str, object],
    outcome: str,
    commit: TargetCommit | None,
    code: str | None,
) -> dict[str, object]:
    now = datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
    safe_code = code if code in SAFE_ERROR_CODES else "internal_error"
    record = {
        "schema": operation["schema"],
        "schema_version": operation["schema_version"],
        "record_kind": "result",
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": operation["operation_uuid"],
        "attempt": operation["attempt"],
        "operation_sha256": operation["operation_sha256"],
        "account_uuid": operation["account_uuid"],
        "project_uuid": operation["project_uuid"],
        "origin": operation["origin"],
        "causal_lane": operation["causal_lane"],
        "sequence": operation["sequence"],
        "predecessor_operation_uuid": operation["predecessor_operation_uuid"],
        "created_at": now,
        "expires_at": operation["expires_at"],
        "in_reply_to_record_uuid": operation["record_uuid"],
        "result": {
            "outcome": outcome,
            "committed_at": now if outcome == "committed" else None,
            "provider_entity_id": commit.entity_id if commit else None,
            "provider_revision": commit.revision if commit else None,
            "safe_error": None
            if code is None
            else {"code": safe_code, "message": "The provider operation failed."},
            "manual_retry_allowed": outcome in {"rejected", "expired"},
        },
    }
    if "transport" in operation:
        record["transport"] = operation["transport"]
    return record


class AdapterResolver(typing.Protocol):
    def __call__(self, account_uuid: str) -> zulip_adapter.OfficialZulipAdapter: ...


class Scheduler:
    RECONCILIATION_DELAYS_SECONDS = (5, 15, 30)
    MAX_AUTO_RESENDS = 1
    RECONCILIATION_REASONS = frozenset(
        {
            "provider_history_unavailable",
            "no_match_after_auto_resend",
            "unsafe_provider_state",
        }
    )

    def __init__(
        self,
        store: storage.QueueStore,
        adapters: AdapterResolver,
        worker_id: str,
        random_source: random.Random | None = None,
    ):
        self.store = store
        self.adapters = adapters
        self.worker_id = worker_id
        self.random = random_source or random.Random()

    def run_once(self) -> bool:
        self.store.reap_expired_running()
        terminal = self.store.claim_terminal(self.worker_id)
        if terminal is not None:
            item, outcome = terminal
            result = result_record(item.record, outcome, None, outcome)
            self.store.complete(item, result, outcome)
            return True
        item = self.store.claim(self.worker_id)
        if item is None:
            return False
        record = item.record
        expires_at = record["expires_at"]
        if isinstance(expires_at, str):
            deadline = datetime.datetime.fromisoformat(
                expires_at.replace("Z", "+00:00")
            )
            if deadline <= datetime.datetime.now(datetime.UTC):
                result = result_record(record, "expired", None, "expired")
                self.store.complete(item, result, "expired")
                return True
        try:
            operation = typing.cast(dict[str, object], record["operation"])
            adapter = self.adapters(str(record["account_uuid"]))
            cursor = self.store.provider_event_cursor(str(record["account_uuid"]))
            if cursor is not None:
                adapter.restore_queue(
                    str(cursor["queue_id"]), int(cursor["last_event_id"])
                )
            correlation = adapter.prepare(
                operation,
                str(record["operation_uuid"]),
                item.provider_rendered_content,
            )
            if correlation is not None:
                self.store.record_provider_attempt(
                    item,
                    correlation.queue_id,
                    correlation.local_id,
                    correlation.last_event_id,
                    correlation.provider_rendered_content,
                )
            entity_id, revision = adapter.apply(operation, correlation)
        except zulip_adapter.ZulipAmbiguousOutcome as exc:
            # Zulip echoes local_id but does not deduplicate by it. Keep the
            # operation actionable and do not risk an automatic duplicate.
            self.store.mark_uncertain(item, str(exc))
            return True
        except zulip_adapter.ZulipOperationError as exc:
            if exc.retryable:
                exponent = min(item.attempts, 5)
                maximum = min(30.0, float(2**exponent))
                delay = self.random.uniform(0.0, maximum)
                self.store.retry(
                    item,
                    datetime.datetime.now(datetime.UTC)
                    + datetime.timedelta(seconds=delay),
                    exc.code,
                )
            else:
                result = result_record(record, "rejected", None, exc.code)
                self.store.complete(item, result, "rejected")
            return True
        commit = TargetCommit(entity_id, revision)
        result = result_record(record, "committed", commit, None)
        self.store.complete(item, result, "committed")
        return True

    def reconcile_local_echo(
        self,
        account_uuid: str,
        queue_id: str,
        local_id: str,
        provider_message_id: str,
    ) -> bool:
        item = self.store.uncertain_by_local_id(account_uuid, queue_id, local_id)
        if item is None:
            return False
        commit = TargetCommit(provider_message_id, None)
        result = result_record(item.record, "committed", commit, None)
        self.store.complete(item, result, "committed")
        return True

    def reconcile_once(self) -> bool:
        self.store.reap_expired_running()
        item = self.store.claim_uncertain(self.worker_id)
        if item is None:
            return False
        if item.provider_attempted_at is None:
            self.store.require_operation_manual_reconciliation(
                item, "unsafe_provider_state", {"match_count": None}
            )
            return True
        operation = typing.cast(dict[str, object], item.record["operation"])
        adapter = self.adapters(str(item.record["account_uuid"]))
        try:
            evidence = adapter.reconcile_message(
                operation,
                item.provider_attempted_at,
                item.provider_rendered_content,
            )
        except zulip_adapter.ZulipOperationError as exc:
            reason = (
                "provider_history_unavailable"
                if exc.code == "provider_unavailable"
                else "unsafe_provider_state"
            )
            self.store.require_operation_manual_reconciliation(
                item,
                reason,
                {
                    "match_count": None,
                    "provider_available": False,
                    "provider_error_class": exc.code,
                },
            )
            return True
        stored_evidence = {
            "checked_at": evidence.checked_at,
            "candidate_ids": list(evidence.candidate_ids),
            "match_count": evidence.exact_match_count,
            "selected_provider_id": evidence.selected_provider_id,
        }
        if evidence.exact_match_count >= 1:
            assert evidence.selected_provider_id is not None
            commit = TargetCommit(evidence.selected_provider_id, None)
            result = result_record(item.record, "committed", commit, None)
            self.store.complete(item, result, "committed")
            return True
        next_check = item.reconciliation_check_count + 1
        if next_check < len(self.RECONCILIATION_DELAYS_SECONDS):
            previous = self.RECONCILIATION_DELAYS_SECONDS[next_check - 1]
            target = self.RECONCILIATION_DELAYS_SECONDS[next_check]
            self.store.schedule_reconciliation_check(
                item,
                datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(seconds=target - previous),
                stored_evidence,
            )
            return True
        if item.auto_resend_count < self.MAX_AUTO_RESENDS:
            self.store.schedule_single_resend(item, stored_evidence)
        else:
            self.store.require_operation_manual_reconciliation(
                item, "no_match_after_auto_resend", stored_evidence
            )
        return True
