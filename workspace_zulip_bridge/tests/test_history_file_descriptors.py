"""Malformed waiting-file metadata is terminal before any provider access."""

import types
import uuid

import httpx
import pytest

from workspace_zulip_bridge import history_delivery, service, storage
from workspace_zulip_bridge.tests import test_history_refresh_budgets as helpers
from workspace_zulip_bridge.tests import test_postgres_integration as pg

migrated_postgres_dsn = pg.migrated_postgres_dsn
postgres_store = pg.postgres_store


@pytest.mark.parametrize(
    "case",
    [
        "missing_uuid",
        "null_uuid",
        "integer_uuid",
        "object_uuid",
        "array_uuid",
        "bad_uuid",
        "null_descriptor",
        "string_descriptor",
        "array_descriptor",
        "missing_path",
        "null_path",
        "integer_path",
        "object_path",
        "missing_files",
        "null_files",
        "object_files",
        "string_files",
    ],
)
def test_invalid_file_descriptor_is_durable_and_never_downloaded(postgres_store, case):
    account = helpers.captured(postgres_store)
    descriptor = {
        "uuid": str(uuid.uuid4()),
        "source_path": "/user_uploads/1/file",
        "account_uuids": [account],
    }
    response = {
        "uuid": str(uuid.uuid4()),
        "status": "waiting_files",
        "files": [descriptor],
        "safe_error": None,
    }
    if case == "missing_uuid":
        del descriptor["uuid"]
    elif case.endswith("_uuid"):
        descriptor["uuid"] = {
            "null": None,
            "integer": 1,
            "object": {},
            "array": [],
            "bad": "invalid",
        }[case.removesuffix("_uuid")]
    elif case.endswith("_descriptor"):
        response["files"] = [
            {"null": None, "string": "invalid", "array": []}[
                case.removesuffix("_descriptor")
            ]
        ]
    elif case == "missing_path":
        del descriptor["source_path"]
    elif case.endswith("_path"):
        descriptor["source_path"] = {"null": None, "integer": 1, "object": {}}[
            case.removesuffix("_path")
        ]
    elif case == "missing_files":
        del response["files"]
    else:
        response["files"] = {"null": None, "object": {}, "string": "invalid"}[
            case.removesuffix("_files")
        ]
    requests = []

    def transport(request):
        requests.append(request)
        return httpx.Response(202, json=response)

    with httpx.Client(
        transport=httpx.MockTransport(transport),
        base_url="https://workspace.example.test",
    ) as client:
        publisher = history_delivery.HistoryPublisher(
            postgres_store,
            types.SimpleNamespace(client=client),
            lambda _: pytest.fail("Malformed descriptor reached provider"),
        )
        publisher.supported = True
        assert not publisher.run_once()
    assert len(requests) == 1
    code = (
        "invalid_provider_file_url"
        if case.endswith("_path")
        else "invalid_history_file_request"
    )
    with postgres_store.session() as session:
        batch = session.execute(
            "SELECT import_status,import_error,import_lease FROM zulip_history_batches"
        ).fetchone()
        assert batch["import_status"] == "failed" and batch["import_error"] == code
        assert batch["import_lease"] is None
    restarted = storage.RestAlchemyStore(postgres_store.connection_url)
    instance = object.__new__(service.BridgeService)
    instance.store = restarted
    pg._drain_history_failure_reports(restarted, instance._queue_history_failure_report)
    with restarted.session() as session:
        report = session.execute("SELECT body FROM observed_report_outbox").fetchone()[
            "body"
        ]
        assert report["resource_uuid"] == account
        assert report["safe_error"]["code"] == code
    assert history_delivery.claim(restarted) is None


@pytest.mark.parametrize("spelling", ["uppercase", "braced"])
def test_file_uuid_is_normalized_once_before_download(postgres_store, spelling):
    account = helpers.captured(postgres_store)
    file_uuid = uuid.uuid4()
    value = (
        str(file_uuid).upper()
        if spelling == "uppercase"
        else "{" + str(file_uuid) + "}"
    )
    with postgres_store.session() as session:
        session.execute(
            "UPDATE desired_resources SET body=body || '{\"limits\":{\"max_file_bytes\":1024}}'::jsonb WHERE resource_type='external_provider_policy'"
        )
    requests = []

    def transport(request):
        requests.append(request)
        if request.method == "PUT":
            assert request.url.path.endswith("/files/" + str(file_uuid))
            return httpx.Response(200)
        return httpx.Response(
            202,
            json={
                "uuid": str(uuid.uuid4()),
                "status": "waiting_files",
                "safe_error": None,
                "files": [
                    {
                        "uuid": value,
                        "source_path": "/user_uploads/1/file",
                        "account_uuids": [account],
                    }
                ],
            },
        )

    with httpx.Client(
        transport=httpx.MockTransport(transport),
        base_url="https://workspace.example.test",
    ) as client:
        adapter = types.SimpleNamespace(
            download_file=lambda *_, **__: types.SimpleNamespace(
                content=b"file", content_type="text/plain", name="file"
            )
        )
        publisher = history_delivery.HistoryPublisher(
            postgres_store, types.SimpleNamespace(client=client), lambda _: adapter
        )
        publisher.supported = True
        assert publisher.run_once()
    assert [r.method for r in requests] == ["POST", "PUT"]
