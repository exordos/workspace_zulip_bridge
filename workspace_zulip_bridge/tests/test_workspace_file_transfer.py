# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from uuid import UUID

import pytest

from workspace_zulip_bridge.workspace_file_transfer import replace_source_file_urn
from workspace_zulip_bridge.workspace_file_transfer import workspace_file_name

SOURCE_UUID = UUID("10000000-0000-0000-0000-000000000001")
TARGET_UUID = UUID("20000000-0000-0000-0000-000000000002")


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
