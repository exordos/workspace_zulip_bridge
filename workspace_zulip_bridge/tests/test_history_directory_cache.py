import copy

import pytest

from workspace_zulip_bridge import zulip_adapter


class DirectoryClient:
    base_url = "https://example.invalid/api/"

    def __init__(self):
        self.directory = [
            {"user_id": 17, "full_name": "Author", "is_active": True},
            {"user_id": 28, "full_name": "Inactive user", "is_active": False},
            {"user_id": 39, "full_name": "Unused bot", "is_bot": True},
        ]
        self.directory_calls = 0
        self.historical_calls = []

    def get_users(self, request):
        assert request == {"include_deactivated": True}
        self.directory_calls += 1
        return {"result": "success", "members": copy.deepcopy(self.directory)}

    def get_user_by_id(self, user_id):
        self.historical_calls.append(user_id)
        if user_id == 99:
            return {"result": "error", "code": "BAD_REQUEST", "msg": "No such user"}
        return {
            "result": "success",
            "user": {"user_id": user_id, "full_name": f"Historical user {user_id}"},
        }


def adapter(client, cache, account="account", generation=1):
    return zulip_adapter.OfficialZulipAdapter(
        client=client,
        account_uuid=account,
        account_generation=generation,
        history_user_cache=cache,
    )


def message(sender=17, reaction=28, recipients=None):
    return {
        "sender_id": sender,
        "reactions": [{"user_id": reaction}],
        "type": "private" if recipients is not None else "stream",
        "display_recipient": [{"id": user_id} for user_id in recipients or []],
    }


def test_directory_shared_by_short_lived_adapters_includes_inactive_users_and_bots():
    cache = zulip_adapter.HistoryUserCache()
    client = DirectoryClient()
    for _ in range(20):
        users = adapter(client, cache).history_users([message()])
        assert users == client.directory
        # A batch builder must not be able to mutate the reusable directory.
        users[0]["full_name"] = "Local copy"
    assert client.directory_calls == 1
    assert client.historical_calls == []


def test_account_generation_and_directory_event_refresh_the_cached_directory():
    cache = zulip_adapter.HistoryUserCache()
    client = DirectoryClient()
    adapter(client, cache).history_users()
    client.directory[0]["full_name"] = "Updated author"
    cache.invalidate("account")
    assert adapter(client, cache).history_users()[0]["full_name"] == "Updated author"
    assert client.directory_calls == 2
    adapter(client, cache, generation=2).history_users()
    assert client.directory_calls == 3
    adapter(client, cache, generation=2).history_users()
    assert client.directory_calls == 3


def test_forced_refresh_after_queue_registration_sees_directory_changes():
    cache = zulip_adapter.HistoryUserCache()
    client = DirectoryClient()
    first = adapter(client, cache)
    first.history_users()
    client.directory.append({"user_id": 45, "full_name": "New user"})
    second = adapter(client, cache)
    assert second.history_users(force_refresh=True) == client.directory
    assert first.history_users() == client.directory
    assert client.directory_calls == 2


def test_directory_cache_ttl_expires_without_waiting_for_a_provider_event():
    now = [10.0]
    cache = zulip_adapter.HistoryUserCache(ttl_seconds=30, clock=lambda: now[0])
    client = DirectoryClient()
    adapter(client, cache).history_users()
    now[0] = 39
    adapter(client, cache).history_users()
    assert client.directory_calls == 1
    now[0] = 40
    adapter(client, cache).history_users()
    assert client.directory_calls == 2


def test_directory_cache_account_count_is_bounded_and_keeps_accounts_isolated():
    cache = zulip_adapter.HistoryUserCache(max_accounts=2)
    client = DirectoryClient()
    for account in ["first", "second", "third"]:
        adapter(client, cache, account=account).history_users()
    assert client.directory_calls == 3
    adapter(client, cache, account="second").history_users()
    assert client.directory_calls == 3
    adapter(client, cache, account="first").history_users()
    assert client.directory_calls == 4


def test_cache_hydrates_referenced_historical_users_once_and_keeps_them_bounded():
    cache = zulip_adapter.HistoryUserCache(max_historical_users=2)
    client = DirectoryClient()
    referenced = [message(sender=50, reaction=99, recipients=[17, 50, 99])]
    for _ in range(3):
        users = adapter(client, cache).history_users(referenced)
        assert {user["user_id"] for user in users} == {17, 28, 39, 50, 99}
        assert (
            next(user for user in users if user["user_id"] == 99)["is_active"] is False
        )
    assert client.directory_calls == 1
    assert client.historical_calls == [50, 99]
    adapter(client, cache).history_users([message(sender=60, reaction=61)])
    adapter(client, cache).history_users(referenced)
    assert client.historical_calls == [50, 99, 60, 61, 50, 99]
    assert adapter(client, cache).history_users() == client.directory


def test_single_range_retains_all_historical_references_even_over_cache_limit():
    cache = zulip_adapter.HistoryUserCache(max_historical_users=1)
    client = DirectoryClient()
    users = adapter(client, cache).history_users(
        [message(sender=50, reaction=51, recipients=[17, 50, 51, 52])]
    )
    assert {user["user_id"] for user in users} == {17, 28, 39, 50, 51, 52}


def test_invalidation_during_provider_request_does_not_publish_stale_directory():
    cache = zulip_adapter.HistoryUserCache()
    client = DirectoryClient()
    original = client.get_users

    def interrupted(request):
        response = original(request)
        cache.invalidate("account")
        client.directory[0]["full_name"] = "New name during fetch"
        return response

    client.get_users = interrupted
    with pytest.raises(zulip_adapter.ZulipOperationError) as error:
        adapter(client, cache).history_users()
    assert error.value.code == "history_directory_changed"
    assert error.value.retryable
    client.get_users = original
    assert (
        adapter(client, cache).history_users()[0]["full_name"]
        == "New name during fetch"
    )
    assert client.directory_calls == 2


def test_total_cached_user_limit_evicts_least_recently_used_account():
    cache = zulip_adapter.HistoryUserCache(max_cached_users=6)
    client = DirectoryClient()
    for account in ['first', 'second', 'third']:
        assert adapter(client, cache, account=account).history_users() == client.directory
    assert client.directory_calls == 3
    adapter(client, cache, account='second').history_users()
    assert client.directory_calls == 3
    adapter(client, cache, account='first').history_users()
    assert client.directory_calls == 4


def test_directory_larger_than_total_cache_limit_is_returned_but_not_retained():
    cache = zulip_adapter.HistoryUserCache(max_cached_users=2)
    client = DirectoryClient()
    for _ in range(2):
        assert adapter(client, cache).history_users() == client.directory
    assert client.directory_calls == 2
