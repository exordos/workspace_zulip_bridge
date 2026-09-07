"""Real durable state with synthetic provider snapshots and import receipts."""

import copy
import types
import uuid

import pytest

from workspace_zulip_bridge import (
    canonical,
    converter,
    history_delivery,
    scheduler,
    service,
    storage,
)
from workspace_zulip_bridge import missing_message_recovery as recovery
from workspace_zulip_bridge.tests import test_converter
from workspace_zulip_bridge.tests import test_postgres_integration as pg_tests

migrated_postgres_dsn = pg_tests.migrated_postgres_dsn
postgres_store = pg_tests.postgres_store


@pytest.fixture
def missing(postgres_store):
    store = postgres_store
    project, realm = str(uuid.uuid4()), str(uuid.uuid4())
    account = pg_tests._history_source(store, 1, realm, project)
    owner = store.account_resource(account)["owner_user_uuid"]
    fake = test_converter.FakeStore(
        account_uuid=account, owner_uuid=owner, project_uuid=project
    )
    converter.event_records(
        fake,
        account,
        "queue",
        {"id": 10, "type": "message", "message": test_converter._stream_message()},
    )
    fake.auto_materialize = False
    records = converter.event_records(
        fake,
        account,
        "queue",
        {
            "id": 11,
            "type": "update_message",
            "message_id": 601,
            "message_ids": [601],
            "stream_id": 42,
            "orig_subject": "Topic",
            "subject": "Moved",
            "propagate_mode": "change_one",
            "edit_timestamp": 1_700_000_010,
        },
        original_url="https://zulip.example",
    )
    for (kind, provider_id), mapping in fake.mappings.items():
        store.remember_provider_mapping(
            account,
            kind,
            provider_id,
            mapping["workspace_uuid"],
            mapping["metadata"],
            mapping["provider_revision"],
        )
    record = next(
        record for record in records if record["operation"]["kind"] == "message.update"
    )
    # Allocate the rejected operation through the real producer lane too, so
    # recovery is checked after an actual terminal predecessor.
    record["sequence"] = 0
    record["operation_sha256"] = canonical.operation_digest(record)
    assert store.enqueue_workspace_delivery(record, 0)
    assert store.mark_workspace_delivery_submitting(record["record_uuid"])
    original = copy.deepcopy(record)
    assert store.reject_provider_event_submission(record["record_uuid"], recovery.ERROR)
    return types.SimpleNamespace(**locals())


def row(store):
    with store.session() as session:
        return dict(
            session.execute("SELECT * FROM zulip_missing_message_recovery").fetchone()
        )


def make_due(store):
    with store.session() as session:
        session.execute(
            "UPDATE zulip_missing_message_recovery SET available_at=now(),lease_until=NULL"
        )


def publish(missing, topic="Topic"):
    s = missing
    job = s.store.claim_backfill_job()
    assert s.store.save_history_batch(
        job, pg_tests._history_job_batch(job, 1), None, True
    )
    receipt = history_delivery.claim(s.store)
    assert receipt is not None
    receipt["import_uuid"] = uuid.uuid4()
    reference = {
        "kind": "message",
        "provider_id": "601",
        "workspace_uuid": str(uuid.uuid4()),
        "metadata": {
            "workspace_delivery_state": "committed",
            "chat_key": "channel:42",
            "topic_provider_id": "42:" + topic,
            "stream_uuid": s.record["operation"]["payload"]["stream_uuid"],
            "topic_uuid": s.record["operation"]["payload"]["topic_uuid"],
            "author_uuid": s.owner,
            "provider_timestamp": 1_700_000_000,
        },
    }
    history_delivery.save_references(
        s.store, receipt, s.account, [reference], {"account": 1}, True
    )
    return reference


def test_missing_base_intent_is_atomic_and_survives_restart_cleanup(
    missing, migrated_postgres_dsn
):
    s = missing
    assert row(s.store)["state"] == "pending"
    assert s.store.reject_provider_event_submission(
        s.record["record_uuid"], recovery.ERROR
    )
    s.store.prune_terminal_delivery_state()
    restarted = storage.RestAlchemyStore(migrated_postgres_dsn)
    assert row(restarted)["operation_uuid"] == uuid.UUID(s.record["operation_uuid"])
    with restarted.session() as session:
        saved = session.execute(
            "SELECT record FROM workspace_delivery_outbox WHERE record_uuid=%s",
            (s.record["record_uuid"],),
        ).fetchone()["record"]
        idempotency = session.execute(
            "SELECT terminal_outcome,operation_sha256 FROM operation_idempotency WHERE operation_uuid=%s",
            (s.record["operation_uuid"],),
        ).fetchone()
    assert saved == s.original
    assert idempotency == {
        "terminal_outcome": "rejected",
        "operation_sha256": s.original["operation_sha256"],
    }
    assert not restarted.enqueue_workspace_delivery(s.original, 0)


def test_capture_and_publication_gate_recovery_then_reference_converges(missing):
    s = missing
    item = s.store.claim_missing_message_recovery()
    assert recovery.context(s.store, item)[0] == "pending"
    assert recovery.finish(s.store, item, "pending", delay=0)
    publish(s, "Moved")
    called = []
    instance = types.SimpleNamespace(
        store=s.store, provider_adapters=lambda _: called.append(True)
    )
    assert recovery.run_once(instance)
    assert row(s.store)["state"] == "complete"
    assert called == []


@pytest.mark.parametrize(
    "change,expected",
    [
        ("generation", "superseded"),
        ("assignment_generation", "superseded"),
        ("deselected", "superseded"),
        ("tombstone", "tombstoned"),
    ],
)
def test_recovery_fences_generation_selection_and_tombstones(missing, change, expected):
    s = missing
    publish(s)
    with s.store.session() as session:
        if change == "generation":
            session.execute(
                "UPDATE desired_resources SET generation=2,body=jsonb_set(body,'{generation}','2') WHERE resource_type='external_account'"
            )
        elif change == "assignment_generation":
            session.execute(
                "UPDATE desired_resources SET generation=2,body=jsonb_set(body,'{generation}','2') WHERE resource_type='external_chat_assignment'"
            )
        elif change == "deselected":
            session.execute(
                "UPDATE desired_resources SET body=jsonb_set(body,'{selected}','false') WHERE resource_type='external_chat_assignment'"
            )
        else:
            s.store.mark_provider_mapping_deleted(s.account, "message", "601")
    assert recovery.run_once(types.SimpleNamespace(store=s.store))
    assert row(s.store)["state"] == expected


def test_fresh_snapshot_uses_separate_immutable_outbox_operation(
    missing, migrated_postgres_dsn
):
    s = missing
    publish(s)
    s.store.remember_provider_mapping(
        s.account, "topic", "42:Newer", str(uuid.uuid4()), {"chat_key": "channel:42"}
    )
    current = {
        **test_converter._stream_message(subject="Newer"),
        "content": "latest body",
        "last_edit_timestamp": 1_700_000_020,
    }
    adapter = types.SimpleNamespace(
        server_url="https://zulip.example",
        message_by_id=lambda _: copy.deepcopy(current),
    )
    instance = object.__new__(service.BridgeService)
    instance.store = s.store
    instance.provider_adapters = lambda _: adapter
    instance._file_resolver = lambda *args: None
    assert recovery.run_once(instance)
    intent = row(s.store)
    assert intent["state"] == "delivering", intent["error_code"]
    with s.store.session() as session:
        deliveries = session.execute(
            "SELECT record FROM workspace_delivery_outbox WHERE operation_uuid=ANY(%s::uuid[]) ORDER BY created_at,record_uuid",
            (intent["recovery_operations"],),
        ).fetchall()
    records = [entry["record"] for entry in deliveries]
    message = next(
        record
        for record in records
        if record["operation"]["kind"].startswith("message.")
    )
    assert message["operation_uuid"] != s.original["operation_uuid"]
    assert message["operation"]["payload"]["payload"]["content"] == "latest body"
    assert message["operation"]["provider"]["revision"] == "1700000020"
    assert message["operation"]["extensions"]["missing_base_recovery"] is True
    assert message["operation_sha256"] == canonical.operation_digest(message)
    for record in records:
        result = scheduler.result_record(
            record, "committed", scheduler.TargetCommit(None, None), None
        )
        s.store.accept_result(result)
    with s.store.session() as session:
        session.execute(
            "UPDATE workspace_delivery_outbox SET sent_at=now()-interval '1 hour' WHERE submission_state='sent'"
        )
    s.store.prune_terminal_delivery_state()
    restarted = storage.RestAlchemyStore(migrated_postgres_dsn)
    make_due(restarted)
    assert recovery.run_once(types.SimpleNamespace(store=restarted))
    assert row(restarted)["state"] == "complete"
    with restarted.session() as session:
        original = session.execute(
            "SELECT record FROM workspace_delivery_outbox WHERE record_uuid=%s",
            (s.original["record_uuid"],),
        ).fetchone()["record"]
    assert original == s.original


def test_failure_to_persist_intent_rolls_back_rejection(missing, monkeypatch):
    s = missing
    second = copy.deepcopy(s.original)
    second["record_uuid"] = str(uuid.uuid4())
    second["operation_uuid"] = str(uuid.uuid4())
    second["operation_sha256"] = canonical.operation_digest(second)
    assert s.store.enqueue_workspace_delivery(second, 0)
    assert s.store.mark_workspace_delivery_submitting(second["record_uuid"])

    def fail(*args):
        raise RuntimeError("synthetic persistence failure")

    monkeypatch.setattr(recovery, "remember", fail)
    with pytest.raises(RuntimeError):
        s.store.reject_provider_event_submission(second["record_uuid"], recovery.ERROR)
    with s.store.session() as session:
        result = session.execute(
            "SELECT submission_state FROM workspace_delivery_outbox WHERE record_uuid=%s",
            (second["record_uuid"],),
        ).fetchone()
    assert result["submission_state"] == "submitting"


def snapshot_service(s, on_fetch=None, subject="Moved"):
    def fetch(_message_id):
        if on_fetch is not None:
            on_fetch()
        return {
            **test_converter._stream_message(subject=subject),
            "content": "Current provider snapshot",
            "last_edit_timestamp": 1_700_000_020,
        }

    instance = object.__new__(service.BridgeService)
    instance.store = s.store
    instance.provider_adapters = lambda _: types.SimpleNamespace(
        server_url="https://zulip.example", message_by_id=fetch
    )
    instance._file_resolver = lambda *args: None
    return instance


@pytest.mark.parametrize(
    "change,expected",
    [
        ("generation", "superseded"),
        ("assignment_generation", "superseded"),
        ("tombstone", "tombstoned"),
        ("journal", "pending"),
        ("other_source", "pending"),
    ],
)
def test_context_change_during_provider_fetch_never_enqueues_stale_snapshot(
    missing, change, expected
):
    s = missing
    publish(s)

    def changed():
        with s.store.session() as session:
            if change == "generation":
                session.execute(
                    "UPDATE desired_resources SET generation=2,body=jsonb_set(body,'{generation}','2') WHERE resource_type='external_account'"
                )
            elif change == "assignment_generation":
                session.execute(
                    "UPDATE desired_resources SET generation=2,body=jsonb_set(body,'{generation}','2') WHERE resource_type='external_chat_assignment'"
                )
            elif change == "tombstone":
                s.store.mark_provider_mapping_deleted(s.account, "message", "601")
            elif change == "journal":
                session.execute(
                    "INSERT INTO zulip_provider_events(account_uuid,queue_id,event_id,event_type,body,processing_state) VALUES (%s,'new-queue',99,'update_message','{\"message_id\":601}'::jsonb,'pending')",
                    (s.account,),
                )
            else:
                pg_tests._history_source(s.store, 2, s.realm, s.project)

    assert recovery.run_once(snapshot_service(s, changed))
    assert row(s.store)["state"] == expected
    with s.store.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM workspace_delivery_outbox"
            ).fetchone()["n"]
            == 1
        )


def test_missing_topic_projection_waits_without_mutating_mappings(missing):
    s = missing
    publish(s)
    with s.store.session() as session:
        before = session.execute(
            "SELECT * FROM provider_mappings ORDER BY entity_kind,provider_id"
        ).fetchall()
    assert recovery.run_once(snapshot_service(s, subject="Not cataloged yet"))
    assert row(s.store)["state"] == "pending"
    assert row(s.store)["error_code"] == "provider_chat_assignment_pending"
    with s.store.session() as session:
        assert (
            session.execute(
                "SELECT * FROM provider_mappings ORDER BY entity_kind,provider_id"
            ).fetchall()
            == before
        )


def test_expired_lease_cannot_complete_or_steal_recovery(missing):
    s = missing
    first = s.store.claim_missing_message_recovery()
    assert s.store.claim_missing_message_recovery() is None
    make_due(s.store)
    second = s.store.claim_missing_message_recovery()
    assert second["lease_uuid"] != first["lease_uuid"]
    assert not recovery.finish(s.store, first, "complete")
    assert recovery.finish(s.store, second, "pending")


def test_assignment_reset_retires_children_without_replaying_old_move(missing):
    s = missing
    publish(s)
    assert recovery.run_once(snapshot_service(s))
    assert row(s.store)["state"] == "delivering"
    with s.store.session() as session:
        session.execute(
            "UPDATE desired_resources SET generation=2,body=jsonb_set(body,'{generation}','2') WHERE resource_type='external_chat_assignment'"
        )
    s.store.reset_stale_workspace_deliveries()
    make_due(s.store)
    assert recovery.run_once(types.SimpleNamespace(store=s.store))
    assert row(s.store)["state"] in {"superseded", "blocked"}
    with s.store.session() as session:
        original = session.execute(
            "SELECT record FROM workspace_delivery_outbox WHERE record_uuid=%s",
            (s.original["record_uuid"],),
        ).fetchone()
    assert original["record"] == s.original


def test_capture_finished_but_import_unpublished_is_not_ready(missing):
    s = missing
    job = s.store.claim_backfill_job()
    assert s.store.save_history_batch(
        job, pg_tests._history_job_batch(job, 1), None, True
    )
    item = s.store.claim_missing_message_recovery()
    assert recovery.context(s.store, item)[0] == "pending"


def test_final_lease_failure_rolls_back_new_outbox_and_mapping_writes(
    missing, monkeypatch
):
    s = missing
    publish(s)
    with s.store.session() as session:
        before = session.execute(
            "SELECT * FROM provider_mappings ORDER BY entity_kind,provider_id"
        ).fetchall()
    finish = recovery.finish
    monkeypatch.setattr(
        recovery,
        "finish",
        lambda store, item, state, *args, **kwargs: (
            False
            if state == "delivering"
            else finish(store, item, state, *args, **kwargs)
        ),
    )
    assert not recovery.run_once(snapshot_service(s))
    with s.store.session() as session:
        assert (
            session.execute(
                "SELECT * FROM provider_mappings ORDER BY entity_kind,provider_id"
            ).fetchall()
            == before
        )
        assert (
            session.execute(
                "SELECT count(*) AS n FROM workspace_delivery_outbox"
            ).fetchone()["n"]
            == 1
        )
    assert row(s.store)["state"] == "pending"


def recovered_records(s):
    intent = row(s.store)
    assert intent["state"] == "delivering", intent["error_code"]
    with s.store.session() as session:
        return [
            entry["record"]
            for entry in session.execute(
                "SELECT record FROM workspace_delivery_outbox WHERE operation_uuid=ANY(%s::uuid[]) ORDER BY (record->>'sequence')::bigint",
                (intent["recovery_operations"],),
            ).fetchall()
        ]


def test_recovery_uses_imported_canonical_uuid_instead_of_stale_mapping(missing):
    s = missing
    reference = publish(s)
    old_uuid = s.store.provider_mapping(s.account, "message", "601")["workspace_uuid"]
    assert str(old_uuid) != reference["workspace_uuid"]
    assert recovery.run_once(snapshot_service(s))
    message = next(
        record
        for record in recovered_records(s)
        if record["operation"]["kind"].startswith("message.")
    )
    assert message["operation"]["entity_uuid"] == reference["workspace_uuid"]
    assert (
        str(s.store.provider_mapping(s.account, "message", "601")["workspace_uuid"])
        == reference["workspace_uuid"]
    )


def test_recovery_allocates_fresh_causal_lane_position(missing):
    s = missing
    publish(s)
    previous = copy.deepcopy(s.original)
    previous.update(
        operation_uuid=str(uuid.uuid4()), record_uuid=str(uuid.uuid4()), sequence=0
    )
    previous["operation_sha256"] = canonical.operation_digest(previous)
    assert s.store.enqueue_workspace_delivery(previous, 0)
    s.store.accept_result(
        scheduler.result_record(
            previous, "committed", scheduler.TargetCommit(None, None), None
        )
    )
    assert recovery.run_once(snapshot_service(s))
    records = [
        record
        for record in recovered_records(s)
        if record["causal_lane"] == previous["causal_lane"]
    ]
    assert records
    predecessor, sequence = previous["operation_uuid"], previous["sequence"]
    for record in records:
        assert record["sequence"] == sequence + 1
        assert record["predecessor_operation_uuid"] == predecessor
        assert record["operation_sha256"] == canonical.operation_digest(record)
        predecessor, sequence = record["operation_uuid"], record["sequence"]
    with s.store.session() as session:
        counter = session.execute(
            "SELECT last_sequence,last_operation_uuid FROM producer_lane_counters WHERE origin='zulip' AND causal_lane=%s",
            (previous["causal_lane"],),
        ).fetchone()
    assert counter == {
        "last_sequence": sequence,
        "last_operation_uuid": uuid.UUID(predecessor),
    }


def test_equivalent_pending_topic_does_not_leave_recovery_delivering_forever(missing):
    s = missing
    publish(s)
    instance = snapshot_service(s)
    current = instance.provider_adapters(s.account).message_by_id(601)
    queue_id, event = recovery.snapshot_event(row(s.store), current)
    records = instance._event_records_with_file_fallback(
        instance.provider_adapters(s.account),
        s.account,
        str(row(s.store)["assignment_uuid"]),
        queue_id,
        event,
        "live",
        recovery.SnapshotStore(s.store),
    )
    topic = copy.deepcopy(
        next(
            record
            for record in records
            if record["operation"]["kind"] == "topic.upsert"
        )
    )
    topic.update(
        operation_uuid=str(uuid.uuid4()), record_uuid=str(uuid.uuid4()), sequence=0
    )
    topic["operation_sha256"] = canonical.operation_digest(topic)
    assert s.store.enqueue_workspace_delivery(topic, 1)
    assert recovery.run_once(instance)
    for record in [topic, *recovered_records(s)]:
        s.store.accept_result(
            scheduler.result_record(
                record, "committed", scheduler.TargetCommit(None, None), None
            )
        )
    make_due(s.store)
    assert recovery.run_once(instance)
    assert row(s.store)["state"] == "complete"


def test_changed_import_reference_during_fetch_does_not_target_old_uuid(missing):
    s = missing
    publish(s)
    before = s.store.provider_mapping(s.account, "message", "601")

    def changed():
        with s.store.session() as session:
            session.execute(
                "UPDATE zulip_missing_message_recovery SET reference=jsonb_set(reference,'{workspace_uuid}',to_jsonb(%s::text))",
                (str(uuid.uuid4()),),
            )

    assert not recovery.run_once(snapshot_service(s, changed))
    assert row(s.store)["state"] == "pending"
    assert s.store.provider_mapping(s.account, "message", "601") == before
    with s.store.session() as session:
        assert (
            session.execute(
                "SELECT count(*) AS n FROM workspace_delivery_outbox"
            ).fetchone()["n"]
            == 1
        )


def test_production_live_lane_dispatches_recovery_and_completes_intent(missing):
    s = missing
    reference = publish(s)
    instance = snapshot_service(s)
    submitted = []

    def apply_commands(commands):
        submitted.extend(commands)
        return {
            "results": [
                {
                    "provider_event_key": command["provider_event_key"],
                    "status": "applied",
                }
                for command in commands
            ]
        }

    instance.provider_api = types.SimpleNamespace(apply_commands=apply_commands)
    instance.scheduler = types.SimpleNamespace(
        reconcile_once=lambda: False, run_once=lambda: False
    )
    instance.process_provider_journal = lambda: 0
    instance.flush_provider_results = lambda: 0
    assert recovery.run_once(instance)
    for _ in range(5):
        instance._run_live_lane_once(before_provider_poll=False)
        make_due(s.store)
        recovery.run_once(instance)
        if row(s.store)["state"] == "complete":
            break
    assert row(s.store)["state"] == "complete"
    message = next(
        command for command in submitted if command["kind"] == "message.upsert"
    )
    assert message["provider_object"] == {"kind": "message", "id": "601"}
    assert (
        str(s.store.provider_mapping(s.account, "message", "601")["workspace_uuid"])
        == reference["workspace_uuid"]
    )
    assert message["provider_sequence"] == "1700000020"
    assert message["payload"]["payload"]["content"] == "Current provider snapshot"
