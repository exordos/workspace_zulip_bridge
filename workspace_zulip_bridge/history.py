"""Provider-native history snapshots stored locally, never Provider API events."""

import contextlib
import copy
import dataclasses
import hashlib
import json
import threading

BATCH_SIZE = 5000
CAPTURE_LEASE_RENEW_SECONDS = 30
CAPTURE_WRITE_MAX_ATTEMPTS = 8
DIRECTORY_WRITE_MAX_ATTEMPTS = 8


def capture_write_budget_ms(encoded_bytes):
    """Bound JSONB/TOAST work without granting an unbounded control lock."""
    return min(5000, 500 + (encoded_bytes + 16383) // 16384)


class CaptureWriteTimeout(Exception):
    """A rolled-back body write, retaining only its authority fence."""

    def __init__(self, scope, generation):
        super().__init__("history_capture_write_timeout")
        self.scope = scope
        self.generation = generation


def is_statement_timeout(error):
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if (
            getattr(error, "sqlstate", None) == "57014"
            or getattr(error, "code", None) == "57014"
        ):
            return True
        error = error.__cause__ or error.__context__
    return False


@contextlib.contextmanager
def keep_capture_lease_alive(store, job):
    stopped = threading.Event()
    state = {"current": True}

    def renew():
        while not stopped.wait(CAPTURE_LEASE_RENEW_SECONDS):
            try:
                if not store.renew_history_capture_lease(job):
                    state["current"] = False
                    return
            except Exception:
                # A transient database failure can recover on the next tick;
                # the final save still checks the exact lease and authority.
                continue

    thread = threading.Thread(target=renew, name="history-capture-lease", daemon=True)
    thread.start()
    try:
        yield state
    finally:
        stopped.set()
        thread.join(timeout=1)
        if thread.is_alive():
            state["current"] = False


@dataclasses.dataclass(frozen=True)
class HistoryRange:
    from_id: int
    to_id: int
    messages: list[dict[str, object]]
    # An actual older visible message ID, not the next numeric range boundary.
    # None means the provider has confirmed there are no older matching messages.
    next_anchor: int | None = None


def validated_id(value: object) -> int:
    """Validate a provider ID using the shared history wire-format bounds."""
    if type(value) is not int or not 0 < value <= 2**53 - 1:
        raise ValueError("invalid_history_id")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("invalid_history_text")
    # PostgreSQL text/jsonb cannot represent NUL or unpaired surrogates.
    if "\x00" in value:
        raise ValueError("invalid_history_text")
    value.encode("utf-8")
    return value


def digest(value: dict[str, object]) -> str:
    """Hash the fixed schema without changing the source text's Unicode form.

    Schema keys are ASCII and numbers are safe integers, so this serialization
    is JCS-compatible for these records. Arrays are ordered by the builder.
    """
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def user_catalog(users: list[dict[str, object]]) -> list[dict[str, object]]:
    catalog = {}
    for user in users:
        user_id = validated_id(user.get("user_id"))
        catalog[user_id] = {"id": user_id, "name": _text(user.get("full_name"))}
    return [catalog[user_id] for user_id in sorted(catalog)]


def message_snapshot(
    message: dict[str, object], observer_user_id: int
) -> dict[str, object]:
    flags = message.get("flags")
    reactions = message.get("reactions")
    if not isinstance(flags, list) or not all(isinstance(flag, str) for flag in flags):
        raise ValueError("missing_history_user_flags")
    if not isinstance(reactions, list):
        raise ValueError("missing_history_reactions")
    message_type = message.get("type")
    if message_type == "stream":
        location = {
            "channel_id": validated_id(message.get("stream_id")),
            "channel_name": _text(message.get("display_recipient")),
            "topic": _text(message.get("subject")),
        }
    elif message_type == "private":
        recipients = message.get("display_recipient")
        if (
            not isinstance(recipients, list)
            or not recipients
            or not all(isinstance(person, dict) for person in recipients)
        ):
            raise ValueError("invalid_history_recipients")
        # The agreed channel example has no channel/topic for direct messages.
        # Participants are the only extra field needed to retain those messages.
        location = {
            "channel_id": None,
            "channel_name": None,
            "topic": None,
            "recipient_ids": sorted(
                {validated_id(person.get("id")) for person in recipients}
            ),
        }
    else:
        raise ValueError("invalid_history_message_type")
    reaction_items = {}
    for reaction in reactions:
        if not isinstance(reaction, dict):
            raise ValueError("invalid_history_reaction")
        item = {
            "user_id": validated_id(reaction.get("user_id")),
            "reaction_type": _text(reaction.get("reaction_type")),
            "emoji_code": _text(reaction.get("emoji_code")),
            "emoji_name": _text(reaction.get("emoji_name")),
        }
        key = (item["user_id"], item["reaction_type"], item["emoji_code"])
        if key in reaction_items and reaction_items[key] != item:
            raise ValueError("conflicting_history_reaction")
        reaction_items[key] = item
    result = {
        "id": validated_id(message.get("id")),
        "sender_id": validated_id(message.get("sender_id")),
        **location,
        "sent_at": validated_id(message.get("timestamp")),
        "content": _text(message.get("content")),
        "reactions": [reaction_items[key] for key in sorted(reaction_items)],
        "access": [
            {
                "user_id": validated_id(observer_user_id),
                "read": "read" in flags,
                "starred": "starred" in flags,
            }
        ],
    }
    result["hash"] = digest(result)
    return result


def make_batch(
    page: HistoryRange,
    users: list[dict[str, object]],
    observer_user_id: int,
) -> dict[str, object]:
    if page.from_id < 1 or (page.from_id - 1) % BATCH_SIZE:
        raise ValueError("invalid_history_range")
    if page.to_id != page.from_id + BATCH_SIZE - 1:
        raise ValueError("invalid_history_range")
    messages = {}
    for raw in page.messages:
        message = message_snapshot(raw, observer_user_id)
        message_id = message["id"]
        if not page.from_id <= message_id <= page.to_id:
            raise ValueError("history_message_outside_range")
        if message_id in messages and messages[message_id] != message:
            raise ValueError("conflicting_history_message")
        messages[message_id] = message
    result = {
        "from_id": page.from_id,
        "to_id": page.to_id,
        "users": user_catalog(users),
        "messages": [messages[message_id] for message_id in sorted(messages)],
    }
    result["hash"] = digest(result)
    return result


def merge_batch(
    existing: dict[str, object] | None, incoming: dict[str, object]
) -> dict[str, object]:
    """Merge positive observations; absent users are not an access revocation.

    A new observation replaces that user's flags and the common message state,
    including the complete reaction set. Other observers' flags are retained.
    """
    if existing is None:
        return copy.deepcopy(incoming)
    if any(existing[key] != incoming[key] for key in ("from_id", "to_id")):
        raise ValueError("history_range_mismatch")
    users = {user["id"]: user for user in existing["users"]}
    users.update({user["id"]: user for user in incoming["users"]})
    messages = {message["id"]: message for message in existing["messages"]}
    for message in incoming["messages"]:
        prior = messages.get(message["id"], {"access": []})
        access = {item["user_id"]: item for item in prior["access"]}
        access.update({item["user_id"]: item for item in message["access"]})
        merged = {key: value for key, value in message.items() if key != "hash"}
        merged["access"] = [access[user_id] for user_id in sorted(access)]
        merged["hash"] = digest(merged)
        messages[message["id"]] = merged
    result = {
        "from_id": incoming["from_id"],
        "to_id": incoming["to_id"],
        "users": [users[user_id] for user_id in sorted(users)],
        "messages": [messages[message_id] for message_id in sorted(messages)],
    }
    result["hash"] = digest(result)
    return copy.deepcopy(result)
