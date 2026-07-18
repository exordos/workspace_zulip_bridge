import datetime
import uuid

import pytest

from workspace_zulip_bridge import canonical


@pytest.fixture
def operation_record() -> dict[str, object]:
    record: dict[str, object] = {
        "schema": "workspace.provider",
        "schema_version": 1,
        "record_kind": "operation",
        "record_uuid": str(uuid.uuid4()),
        "operation_uuid": str(uuid.uuid4()),
        "attempt": 1,
        "operation_sha256": "",
        "account_uuid": str(uuid.uuid4()),
        "project_uuid": str(uuid.uuid4()),
        "origin": "workspace",
        "causal_lane": f"chat:{uuid.uuid4()}:{uuid.uuid4()}",
        "sequence": 1,
        "predecessor_operation_uuid": None,
        "created_at": datetime.datetime.now(datetime.UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "expires_at": (
            datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=1)
        )
        .isoformat()
        .replace("+00:00", "Z"),
        "operation": {
            "kind": "message.create",
            "entity_uuid": str(uuid.uuid4()),
            "actor_uuid": str(uuid.uuid4()),
            "occurred_at": datetime.datetime.now(datetime.UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "provider": {
                "kind": "zulip",
                "chat_id": "42",
                "entity_id": None,
                "revision": None,
            },
            "payload": {
                "stream_uuid": str(uuid.uuid4()),
                "topic_uuid": str(uuid.uuid4()),
                "author_uuid": str(uuid.uuid4()),
                "payload": {"kind": "markdown", "content": "hello"},
                "reply_to_message_uuid": None,
            },
            "extensions": {},
        },
    }
    record["operation_sha256"] = canonical.operation_digest(record)
    return record
