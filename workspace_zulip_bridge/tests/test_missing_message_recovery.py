import copy
import types
import uuid

import httpx
import pytest

from workspace_zulip_bridge import missing_message_recovery as recovery
from workspace_zulip_bridge import provider_api
from workspace_zulip_bridge.tests import test_converter, test_provider_api


@pytest.mark.parametrize(
    "status,body,expected",
    [
        (422, {"type": "ProviderProblem", "error": recovery.ERROR}, recovery.ERROR),
        (
            422,
            {"type": "ProviderProblem", "error": "provider_event_batch_rejected"},
            None,
        ),
        (422, {"message": recovery.ERROR}, None),
        (422, [recovery.ERROR], None),
        (400, {"type": "ProviderProblem", "error": recovery.ERROR}, None),
    ],
)
def test_only_typed_missing_base_is_recoverable(status, body, expected):
    client = provider_api.ProviderApiClient(
        test_provider_api._settings(),
        httpx.Client(
            base_url="https://provider.invalid",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(status, json=body)
            ),
        ),
    )
    with pytest.raises(provider_api.ProviderEventRejectedError) as raised:
        client.apply_commands([{"provider_event_key": "sample"}])
    assert raised.value.error_code == expected


def test_reconciliation_has_stable_identity_for_one_snapshot():
    item = {"operation_uuid": uuid.uuid4(), "provider_message_id": "601"}
    message = test_converter._stream_message()
    original = copy.deepcopy(message)
    assert recovery.snapshot_event(item, message) == recovery.snapshot_event(
        item, message
    )
    changed = {**message, "subject": "Newer destination"}
    assert (
        recovery.snapshot_event(item, message)[0]
        != recovery.snapshot_event(item, changed)[0]
    )
    assert message == original


@pytest.mark.parametrize("state", ["pending", "superseded", "tombstoned"])
def test_gated_recovery_never_fetches_or_replays(monkeypatch, state):
    item = {"state": "pending"}
    service = types.SimpleNamespace(
        store=types.SimpleNamespace(claim_missing_message_recovery=lambda: item)
    )
    monkeypatch.setattr(recovery, "context", lambda *args: (state, None))
    calls = []
    monkeypatch.setattr(
        recovery, "finish", lambda *args, **kwargs: calls.append((args, kwargs)) or True
    )
    assert recovery.run_once(service)
    assert calls[0][0][2] == state


def test_imported_destination_converges_without_provider_read(monkeypatch):
    item = {
        "state": "pending",
        "provider_chat_key": "channel:42",
        "provider_topic_id": "42:Moved",
        "reference": {
            "metadata": {"chat_key": "channel:42", "topic_provider_id": "42:Moved"}
        },
    }
    service = types.SimpleNamespace(
        store=types.SimpleNamespace(claim_missing_message_recovery=lambda: item)
    )
    monkeypatch.setattr(recovery, "context", lambda *args: ("ready", {}))
    calls = []
    monkeypatch.setattr(
        recovery, "finish", lambda *args, **kwargs: calls.append((args, kwargs)) or True
    )
    assert recovery.run_once(service)
    assert calls[0][0][2] == "complete"


def test_snapshot_conversion_does_not_mutate_mappings_before_enqueue():
    store = test_converter.FakeStore()
    staged = recovery.SnapshotStore(store)
    account = test_converter.ACCOUNT_UUID
    staged.remember_provider_mapping(
        account, "message", "601", str(uuid.uuid4()), {"content_sha256": "hash"}
    )
    assert ("message", "601") not in store.mappings
    assert staged.producer_lane_position("operation", "zulip", "lane") == (0, None)
    staged.persist()
    assert ("message", "601") in store.mappings
