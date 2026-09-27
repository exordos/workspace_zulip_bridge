# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from pathlib import Path
from uuid import UUID

import httpx
import pytest

from workspace_zulip_bridge import workspace_file_transfer
from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.stable_ids import stable_external_chat_uuid
from workspace_zulip_bridge.workspace_file_transfer import WorkspaceFileTransferWorker
from workspace_zulip_bridge.workspace_file_transfer import replace_source_file_urn
from workspace_zulip_bridge.workspace_file_transfer import workspace_file_name

SOURCE_UUID = UUID("10000000-0000-0000-0000-000000000001")
TARGET_UUID = UUID("20000000-0000-0000-0000-000000000002")


def test_external_chat_identity_stays_compatible_with_existing_catalogs() -> None:
    assert stable_external_chat_uuid(
        UUID("10000000-0000-0000-0000-000000000003"),
        "channel:42",
    ) == UUID("2a239a52-7e3f-5db9-9631-996c5d7581c4")


@pytest.mark.parametrize("kind", ("file", "image", "video"))
def test_source_placeholder_is_replaced_with_native_workspace_urn(kind: str) -> None:
    content = f"before ![asset](urn:file:{SOURCE_UUID}?name=asset.png) after"
    target = f"urn:{kind}:{TARGET_UUID}"

    assert replace_source_file_urn(content, SOURCE_UUID, target) == (
        f"before ![asset]({target}?name=asset.png) after"
    )


def test_unrelated_file_reference_is_unchanged() -> None:
    content = f"[asset](urn:file:{TARGET_UUID})"
    assert (
        replace_source_file_urn(
            content,
            SOURCE_UUID,
            f"urn:file:{TARGET_UUID}",
        )
        == content
    )


def test_invalid_native_urn_is_rejected() -> None:
    with pytest.raises(ValueError, match="invalid Workspace file URN"):
        replace_source_file_urn("content", SOURCE_UUID, "https://example.invalid")


def test_workspace_file_name_is_normalized_and_bounded() -> None:
    assert workspace_file_name("  a/b\\c\x00e\u0301.png  ") == "a_b_c_é.png"
    assert len(workspace_file_name("я" * 200).encode("utf-8")) <= 255
    assert workspace_file_name("\x00/\\") == "___"


def test_control_client_loads_bridge_identity_into_tls_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = tmp_path / "control"
    state.mkdir()
    for name in ("control-ca.pem", "bridge.crt", "bridge.key"):
        (state / name).write_text("placeholder", encoding="utf-8")

    class FakeContext:
        loaded_chain: tuple[str, str] | None = None

        def load_cert_chain(self, certificate: str, key: str) -> None:
            self.loaded_chain = (certificate, key)

    context = FakeContext()
    captured: dict[str, object] = {}
    client = object()

    def create_default_context(*, cafile: str) -> FakeContext:
        captured["cafile"] = cafile
        return context

    def async_client(**options: object) -> object:
        captured.update(options)
        return client

    monkeypatch.setattr(
        workspace_file_transfer.ssl,
        "create_default_context",
        create_default_context,
    )
    monkeypatch.setattr(workspace_file_transfer.httpx, "AsyncClient", async_client)

    worker = WorkspaceFileTransferWorker(
        object(),  # type: ignore[arg-type]
        Settings(
            database_dsn="postgresql:///unused",
            workspace_control_url="https://control.example.invalid",
            workspace_control_state_dir=state,
        ),
    )

    assert worker._control_client() is client
    assert captured["cafile"] == str(state / "control-ca.pem")
    assert captured["verify"] is context
    assert "cert" not in captured
    assert captured["headers"] == {"Accept": "application/json"}
    assert context.loaded_chain == (
        str(state / "bridge.crt"),
        str(state / "bridge.key"),
    )
    assert isinstance(captured["timeout"], httpx.Timeout)
