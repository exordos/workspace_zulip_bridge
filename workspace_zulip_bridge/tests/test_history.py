import copy
import datetime

import pytest

from workspace_zulip_bridge import history, service, zulip_adapter


def source_message(message_id=205, flags=None):
    return {
        "id": message_id,
        "type": "stream",
        "sender_id": 17,
        "stream_id": 42,
        "display_recipient": "Example channel",
        "subject": "Topic",
        "timestamp": 1788638060,
        "content": "Synthetic history message",
        "flags": ["read"] if flags is None else flags,
        "reactions": [
            {
                "user_id": 28,
                "reaction_type": "unicode_emoji",
                "emoji_code": "1f44d",
                "emoji_name": "+1",
            }
        ],
    }


def source_users():
    return [
        {"user_id": 17, "full_name": "Example author", "is_active": True},
        {"user_id": 28, "full_name": "Inactive user", "is_active": False},
        {"user_id": 39, "full_name": "Unused bot", "is_bot": True},
    ]


def batch(observer=17, flags=None, messages=None):
    return history.make_batch(
        history.HistoryRange(
            1, 5000, [source_message(flags=flags)] if messages is None else messages
        ),
        source_users(),
        observer,
    )


def test_union_deduplicates_by_id_and_retains_independent_personal_state():
    first = batch()
    second = batch(observer=28, flags=["starred"])
    merged = history.merge_batch(first, second)
    assert len(merged["messages"]) == 1
    assert merged["messages"][0]["access"] == [
        {"user_id": 17, "read": True, "starred": False},
        {"user_id": 28, "read": False, "starred": True},
    ]
    assert [user["id"] for user in merged["users"]] == [17, 28, 39]
    assert history.merge_batch(merged, second) == merged
    assert first["messages"][0]["access"] == [
        {"user_id": 17, "read": True, "starred": False}
    ]


@pytest.mark.parametrize("flag", ["read", "starred"])
def test_each_flag_round_trip_changes_hash_without_changing_other_user(flag):
    initial = history.merge_batch(batch(flags=[]), batch(observer=28, flags=["read"]))
    changed = history.merge_batch(initial, batch(flags=[flag]))
    assert changed["hash"] != initial["hash"]
    assert changed["messages"][0]["hash"] != initial["messages"][0]["hash"]
    assert changed["messages"][0]["access"][1] == initial["messages"][0]["access"][1]
    assert history.merge_batch(changed, batch(flags=[])) == initial


def test_complete_reaction_snapshot_replaces_removed_reaction():
    current = batch()
    raw = source_message()
    raw["reactions"] = []
    changed = history.merge_batch(current, batch(messages=[raw]))
    assert changed["messages"][0]["reactions"] == []
    assert history.merge_batch(changed, current) == current


def test_catalog_includes_unused_users_and_changes_batch_hash():
    current = batch()
    users = source_users()
    users[-1]["full_name"] = "Renamed unused bot"
    updated = history.make_batch(history.HistoryRange(1, 5000, []), users, 17)
    merged = history.merge_batch(current, updated)
    assert merged["hash"] != current["hash"]
    assert merged["messages"] == current["messages"]


def test_identical_text_different_ids_remain_distinct_and_order_is_stable():
    raw = [source_message(206), source_message(205)]
    current = batch(messages=raw)
    reordered = batch(messages=list(reversed(raw)))
    assert current == reordered
    assert [message["id"] for message in current["messages"]] == [205, 206]
    assert current["messages"][0]["hash"] != current["messages"][1]["hash"]


def test_hash_preserves_exact_source_unicode_and_newlines():
    raw = source_message()
    raw["content"] = "e\u0301\n"
    first = batch(messages=[raw])
    raw["content"] = "\u00e9\n"
    assert first["hash"] != batch(messages=[raw])["hash"]
    assert first["messages"][0]["content"] == "e\u0301\n"


@pytest.mark.parametrize("field", ["flags", "reactions"])
def test_missing_user_state_or_reaction_snapshot_is_not_silently_empty(field):
    raw = source_message()
    del raw[field]
    with pytest.raises(ValueError):
        batch(messages=[raw])


def test_direct_messages_retain_participants_without_inventing_a_channel():
    raw = source_message()
    raw.update(type="private", display_recipient=[{"id": 28}, {"id": 17}])
    result = batch(messages=[raw])["messages"][0]
    assert result["recipient_ids"] == [17, 28]
    assert result["channel_id"] is result["channel_name"] is result["topic"] is None


class RangeClient:
    def __init__(self, ids):
        self.ids = ids
        self.requests = []

    def get_messages(self, request):
        self.requests.append(request)
        anchor = request["anchor"]
        if anchor == "newest":
            selected = self.ids[-1:]
            found_oldest = len(self.ids) <= 1
            found_newest = True
        else:
            before = [value for value in self.ids if value < anchor]
            after = [value for value in self.ids if value > anchor]
            selected = (
                (before[-request["num_before"] :] if request["num_before"] else [])
                + (
                    [anchor]
                    if request.get("include_anchor", True) and anchor in self.ids
                    else []
                )
                + after[: request["num_after"]]
            )
            found_oldest = not before or bool(selected and selected[0] == self.ids[0])
            found_newest = not after or bool(selected and selected[-1] == self.ids[-1])
        return {
            "result": "success",
            "messages": [{"id": value} for value in selected],
            "found_newest": found_newest,
            "found_oldest": found_oldest,
        }

    def get_users(self, request):
        assert request == {"include_deactivated": True}
        return {"result": "success", "members": source_users()}


def test_range_reads_every_visible_id_across_api_pages():
    client = RangeClient(list(range(1, 5002)))
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)
    page = adapter.history_range("channel:42", 5000)
    assert (page.from_id, page.to_id) == (1, 5000)
    assert [message["id"] for message in page.messages] == list(range(1, 5001))
    assert [request["anchor"] for request in client.requests] == [
        1,
        1001,
        2001,
        3001,
        4001,
    ]
    assert all(
        request["narrow"] == [{"operator": "channel", "operand": 42}]
        for request in client.requests
    )
    assert all(request["apply_markdown"] is False for request in client.requests)


def test_range_preserves_sparse_id_boundaries_and_empty_gaps():
    adapter = zulip_adapter.OfficialZulipAdapter(client=RangeClient([1, 5000, 10001]))
    newest = adapter.history_range("channel:42")
    assert (newest.from_id, newest.to_id) == (10001, 15000)
    assert [message["id"] for message in newest.messages] == [10001]
    assert newest.next_anchor == 5000
    gap = adapter.history_range("channel:42", 10000)
    assert (gap.from_id, gap.to_id, gap.messages) == (5001, 10000, [])
    assert gap.next_anchor == 5000
    oldest = adapter.history_range("channel:42", 5000)
    assert [message["id"] for message in oldest.messages] == [1, 5000]
    assert oldest.next_anchor is None


def test_full_middle_range_retains_every_page_before_returning_older_anchor():
    client = RangeClient(list(range(1, 10002)))
    page = zulip_adapter.OfficialZulipAdapter(client=client).history_range(
        "channel:42", 10000
    )
    assert [message["id"] for message in page.messages] == list(range(5001, 10001))
    assert page.next_anchor == 5000
    assert len(client.requests) == 6
    assert client.requests[-1] == {
        "anchor": 5001,
        "include_anchor": False,
        "num_before": 1,
        "num_after": 0,
        "narrow": [{"operator": "channel", "operand": 42}],
        "apply_markdown": False,
    }


def test_empty_source_still_has_one_empty_range_and_full_directory():
    adapter = zulip_adapter.OfficialZulipAdapter(client=RangeClient([]))
    assert adapter.history_range("channel:42") == history.HistoryRange(1, 5000, [])
    assert adapter.history_users() == source_users()


def test_direct_range_uses_participant_narrow():
    client = RangeClient([205])
    zulip_adapter.OfficialZulipAdapter(client=client).history_range("direct:17,28")
    assert all(
        request["narrow"] == [{"operator": "dm", "operand": [17, 28]}]
        for request in client.requests
    )


class CaptureStore:
    def __init__(self):
        self.job = {
            "account_uuid": "account",
            "provider_chat_key": "channel:42",
            "next_anchor": None,
            "cutoff_at": None,
            "retry_count": 0,
        }
        self.saved = []
        self.released = []
        self.failed = []
        self.deferred = []

    def claim_backfill_job(self):
        return self.job

    def account_is_active(self, account_uuid):
        return True

    def provider_event_cursor(self, account_uuid):
        return {"provider_owner_user_id": "17"}

    def save_history_batch(self, *args):
        self.saved.append(copy.deepcopy(args))
        return True

    def release_backfill_job(self, *args):
        self.released.append(args)

    def release_history_capture(self, job):
        self.release_backfill_job(job["account_uuid"], job["provider_chat_key"])

    def fail_backfill_job(self, *args):
        self.failed.append(args)

    def defer_backfill_job(self, *args):
        self.deferred.append(args)

    def mark_health(self, *args):
        pass

    def enqueue_workspace_delivery(self, *args, **kwargs):
        pytest.fail("History capture must not enqueue Workspace delivery")


class CaptureAdapter:
    def __init__(self, page):
        self.page = page

    def history_range(self, chat_key, anchor):
        return self.page

    def history_users(self, messages=None):
        return source_users()


def capture_service(page, store=None):
    instance = object.__new__(service.BridgeService)
    instance.store = store or CaptureStore()
    instance.provider_adapters = lambda account: CaptureAdapter(page)
    instance._record_interval_stat = lambda *args: None
    instance._record_provider_account_success = lambda *args: None
    instance._handle_provider_account_error = lambda *args: False
    instance._queue_account_report = lambda *args: None
    return instance


def test_capture_saves_snapshot_without_workspace_or_file_plane_dependencies():
    instance = capture_service(history.HistoryRange(1, 5000, [source_message()]))
    assert instance.run_backfill_once()
    _, saved, next_anchor, complete = instance.store.saved[0]
    assert saved == batch()
    assert next_anchor is None and complete


def test_empty_middle_range_does_not_prematurely_finish_history():
    instance = capture_service(history.HistoryRange(5001, 10000, [], next_anchor=5000))
    assert instance.run_backfill_once()
    _, saved, next_anchor, complete = instance.store.saved[0]
    assert saved["messages"] == []
    assert next_anchor == 5000 and not complete


def test_capture_applies_saved_history_depth():
    instance = capture_service(
        history.HistoryRange(5001, 10000, [source_message(5001)])
    )
    instance.store.job["cutoff_at"] = datetime.datetime.fromtimestamp(
        1788638061, datetime.UTC
    )
    assert instance.run_backfill_once()
    assert instance.store.saved[0][1]["messages"] == []
    assert instance.store.saved[0][-1] is True


def test_invalid_flags_fail_capture_without_advancing_or_saving():
    raw = source_message()
    raw.pop("flags")
    instance = capture_service(history.HistoryRange(1, 5000, [raw]))
    assert instance.run_backfill_once()
    assert instance.store.saved == []
    assert instance.store.failed == [
        ("account", "channel:42", "invalid_history_snapshot")
    ]


@pytest.mark.parametrize("code", ["40001", "40P01", "55P03", "57014"])
def test_database_conflict_releases_capture_for_retry_without_an_early_checkpoint(code):
    class Conflict(Exception):
        sqlstate = code

    instance = capture_service(history.HistoryRange(1, 5000, [source_message()]))
    instance.store.save_history_batch = lambda *args: (_ for _ in ()).throw(Conflict())
    assert instance.run_backfill_once()
    assert instance.store.saved == []
    assert instance.store.released == [("account", "channel:42")]


@pytest.mark.parametrize("code", ["40001", "40P01", "55P03"])
def test_provider_error_bookkeeping_conflict_keeps_capture_lane_alive(code):
    class Conflict(Exception):
        sqlstate = code

    instance = capture_service(history.HistoryRange(1, 5000, []))
    instance.provider_adapters = lambda account: (_ for _ in ()).throw(
        zulip_adapter.ZulipOperationError("rate_limit_hit", True)
    )
    instance.store.defer_backfill_job = lambda *args: (_ for _ in ()).throw(
        Conflict()
    )

    assert instance.run_backfill_once()
    assert instance.store.saved == []
    assert instance.store.released == []


def test_provider_error_post_transition_conflict_is_not_silently_swallowed():
    class Conflict(Exception):
        sqlstate = "55P03"

    instance = capture_service(history.HistoryRange(1, 5000, []))
    instance.provider_adapters = lambda account: (_ for _ in ()).throw(
        zulip_adapter.ZulipOperationError("history_batch_rejected", False)
    )
    instance._queue_account_report = lambda *args: (_ for _ in ()).throw(Conflict())

    with pytest.raises(Conflict):
        instance.run_backfill_once()
    assert instance.store.failed == [
        ("account", "channel:42", "history_batch_rejected")
    ]


def test_published_golden_batch_and_markdown():
    import json
    import pathlib

    from workspace_zulip_bridge import converter

    directory = pathlib.Path(__file__).parent / "fixtures"
    fixture = json.loads((directory / "history_batch_v1.json").read_text())
    body = fixture["envelope"]["batch"]
    assert (
        history.make_batch(
            history.HistoryRange(
                body["from_id"], body["to_id"], fixture["provider_messages"]
            ),
            fixture["provider_users"],
            fixture["observer_user_id"],
        )
        == body
    )
    for case in json.loads((directory / "history_markdown_v1.json").read_text()):
        converted, lossy = converter.convert_markdown(
            case["content"],
            case["mention_uuids"],
            case["original_url"],
            file_resolver=lambda path, label: case["files"].get(path),
        )
        assert (converted, lossy) == (case["converted"], case["lossy"])


def test_history_directory_hydrates_old_authors_and_retains_unavailable_ids():
    client = RangeClient([])
    looked_up = []

    def get_user(user_id):
        looked_up.append(user_id)
        if user_id == 50:
            return {
                "result": "success",
                "user": {"user_id": 50, "full_name": "Historical author"},
            }
        return {"result": "error", "code": "BAD_REQUEST", "msg": "No such user"}

    client.get_user_by_id = get_user
    message = source_message()
    message["sender_id"] = 50
    message["reactions"][0]["user_id"] = 51
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)
    users = adapter.history_users([message])
    assert looked_up == [50, 51]
    assert {user["user_id"] for user in users} == {17, 28, 39, 50, 51}
    assert (
        next(user for user in users if user["user_id"] == 50)["full_name"]
        == "Historical author"
    )
    assert next(user for user in users if user["user_id"] == 51)["is_active"] is False
    snapshot = history.make_batch(history.HistoryRange(1, 5000, [message]), users, 17)
    assert {user["id"] for user in snapshot["users"]} == {17, 28, 39, 50, 51}


@pytest.mark.parametrize("lease_current", [True, False])
def test_slow_capture_renews_lease_before_saving(monkeypatch, lease_current):
    import threading
    import time

    instance = capture_service(history.HistoryRange(1, 5000, [source_message()]))
    renewed = threading.Event()
    calls = []

    def renew(job):
        calls.append(job)
        renewed.set()
        return lease_current

    instance.store.renew_history_capture_lease = renew
    adapter = CaptureAdapter(history.HistoryRange(1, 5000, [source_message()]))

    def users(messages):
        assert renewed.wait(1)
        time.sleep(0.02)
        return source_users()

    adapter.history_users = users
    instance.provider_adapters = lambda account: adapter
    monkeypatch.setattr(history, "CAPTURE_LEASE_RENEW_SECONDS", 0.01)
    assert instance.run_backfill_once() is lease_current
    assert bool(instance.store.saved) is lease_current
    count = len(calls)
    time.sleep(0.02)
    assert len(calls) == count


@pytest.mark.parametrize("field", ["reactions", "display_recipient"])
@pytest.mark.parametrize("value", [None, {}, "invalid", [None], [17]])
def test_malformed_history_collections_fail_only_the_capture_job(field, value):
    message = source_message()
    if field == "display_recipient":
        message["type"] = "private"
    message[field] = value
    page = history.HistoryRange(1, 5000, [message])
    adapter = zulip_adapter.OfficialZulipAdapter(client=RangeClient([]))
    adapter.history_range = lambda *args, **kwargs: page
    instance = capture_service(page)
    instance.provider_adapters = lambda account: adapter
    assert instance.run_backfill_once()
    assert instance.store.failed == [("account", "channel:42", "invalid_record")]
    assert instance.store.saved == []
    if field == "display_recipient":
        del message[field]
        assert instance.run_backfill_once()
        assert instance.store.failed[-1][-1] == "invalid_record"


@pytest.mark.parametrize("older_id, expected_requests", [(None, 3), (17, 4)])
def test_sparse_chat_at_three_million_ids_skips_empty_numeric_ranges(
    older_id, expected_requests
):
    ids = ([older_id] if older_id is not None else []) + [3_000_001]
    client = RangeClient(ids)
    requests = client.get_messages

    def messages(request):
        result = requests(request)
        result["messages"] = [
            source_message(message["id"]) for message in result["messages"]
        ]
        return result

    client.get_messages = messages
    instance = capture_service(None)
    instance.provider_adapters = lambda account: zulip_adapter.OfficialZulipAdapter(
        client=client
    )
    while True:
        assert instance.run_backfill_once()
        _, _, next_anchor, complete = instance.store.saved[-1]
        if complete:
            break
        assert len(instance.store.saved) < 3
        instance.store.job["next_anchor"] = next_anchor
    saved_ids = sorted(
        message["id"]
        for _, batch, _, _ in instance.store.saved
        for message in batch["messages"]
    )
    assert saved_ids == ids
    assert len(client.requests) == expected_requests
    assert [item[1]["from_id"] for item in instance.store.saved] == (
        [3_000_001, 1] if older_id is not None else [3_000_001]
    )
    assert all(
        request["narrow"] == [{"operator": "channel", "operand": 42}]
        for request in client.requests
    )


@pytest.mark.parametrize("oldest", [True, False, None, 1, "false"])
@pytest.mark.parametrize("nonempty", [False, True])
def test_backward_probe_requires_provider_confirmation_before_finishing(
    oldest, nonempty
):
    client = RangeClient(([1] if nonempty else []) + [3_000_001])
    original = client.get_messages

    def incomplete_result(request):
        result = original(request)
        if request.get("include_anchor") is False:
            if oldest is None:
                result.pop("found_oldest")
            else:
                result["found_oldest"] = oldest
        return result

    client.get_messages = incomplete_result
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)
    if nonempty or oldest is True:
        assert adapter.history_range("channel:42").next_anchor == (
            1 if nonempty else None
        )
    else:
        with pytest.raises(zulip_adapter.ZulipOperationError) as error:
            adapter.history_range("channel:42")
        assert error.value.code == "history_pagination_stalled"
        assert error.value.retryable


@pytest.mark.parametrize("value", [True, 0, -1, 2**53, "17", None])
def test_public_history_id_validator_rejects_unsafe_values(value):
    with pytest.raises(ValueError, match="invalid_history_id"):
        history.validated_id(value)


def test_public_history_id_validator_accepts_the_full_safe_range():
    assert history.validated_id(1) == 1
    assert history.validated_id(2**53 - 1) == 2**53 - 1


@pytest.mark.parametrize("newest", [True, False, None, 1, "false"])
@pytest.mark.parametrize("oldest", [True, False, None, 1, "false"])
@pytest.mark.parametrize("nonempty", [False, True])
def test_initial_newest_requires_explicit_boundary_confirmation(
    newest, oldest, nonempty
):
    client = RangeClient([205] if nonempty else [])
    original = client.get_messages

    def unconfirmed(request):
        result = original(request)
        result["messages"] = [
            source_message(message["id"]) for message in result["messages"]
        ]
        if request["anchor"] == "newest":
            for field, value in (("found_oldest", oldest), ("found_newest", newest)):
                if value is None:
                    result.pop(field)
                else:
                    result[field] = value
        return result

    client.get_messages = unconfirmed
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)
    instance = capture_service(None)
    instance.provider_adapters = lambda _: adapter
    assert instance.run_backfill_once()
    if newest is True and (nonempty or oldest is True):
        assert len(instance.store.saved) == 1
        assert instance.store.saved[0][-1] is True
        assert [
            message["id"] for message in instance.store.saved[0][1]["messages"]
        ] == ([205] if nonempty else [])
        assert instance.store.deferred == []
    else:
        assert instance.store.saved == []
        assert len(instance.store.deferred) == 1
        assert instance.store.deferred[0][-1] == "history_pagination_stalled"
        assert len(client.requests) == 1 and client.requests[0]["anchor"] == "newest"


@pytest.mark.parametrize("newest", [True, False, None, 1, "false"])
@pytest.mark.parametrize("at_last_id", [False, True])
def test_empty_forward_page_requires_confirmation_except_anchor_only_query(
    newest, at_last_id
):
    client = RangeClient([])

    def page(request):
        client.requests.append(request)
        if at_last_id and request["anchor"] == 1:
            return {
                "result": "success",
                "messages": [{"id": 4999}],
                "found_newest": False,
            }
        result = {"result": "success", "messages": []}
        if newest is not None:
            result["found_newest"] = newest
        return result

    client.get_messages = page
    adapter = zulip_adapter.OfficialZulipAdapter(client=client)
    if newest is True or at_last_id:
        result = adapter.history_range("channel:42", 5000)
        assert [message["id"] for message in result.messages] == (
            [4999] if at_last_id else []
        )
        assert result.next_anchor is None
        if at_last_id:
            assert client.requests[-1]["anchor"] == 5000
            assert client.requests[-1]["num_after"] == 0
    else:
        with pytest.raises(zulip_adapter.ZulipOperationError) as error:
            adapter.history_range("channel:42", 5000)
        assert (
            error.value.code == "history_pagination_stalled" and error.value.retryable
        )


def test_capture_cleanup_contention_yields_without_failure_or_checkpoint():
    class Conflict(Exception):
        sqlstate = "55P03"

    instance = capture_service(history.HistoryRange(1, 5000, [source_message()]))
    instance.store.save_history_batch = lambda *args: (_ for _ in ()).throw(Conflict())
    instance.store.release_history_capture = lambda *args: (_ for _ in ()).throw(Conflict())
    assert instance.run_backfill_once()
    assert instance.store.saved == instance.store.failed == []
