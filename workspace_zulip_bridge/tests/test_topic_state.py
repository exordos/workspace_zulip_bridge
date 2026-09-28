# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from workspace_zulip_bridge.topic_state import topic_display_name
from workspace_zulip_bridge.topic_state import topic_state_after_display_change
from workspace_zulip_bridge.topic_state import topic_state_from_display_name


def test_done_and_reopen_are_reversible() -> None:
    assert topic_state_after_display_change("Topic", False, "✔ Topic") == (
        "Topic",
        True,
    )
    assert topic_state_after_display_change("Topic", True, "Topic") == (
        "Topic",
        False,
    )


def test_done_topic_rename_keeps_canonical_identity_state() -> None:
    assert topic_state_after_display_change("Old", True, "✔ New") == (
        "New",
        True,
    )


def test_every_leading_prefix_is_authoritative_done_state() -> None:
    assert topic_state_after_display_change("Old", False, "✔ Literal") == (
        "Literal",
        True,
    )
    assert topic_state_from_display_name("✔ ✔ Literal") == ("✔ Literal", True)
    assert topic_display_name("✔ Literal", True) == "✔ ✔ Literal"


def test_repeated_prefix_rename_and_reopen_follow_source_titles() -> None:
    assert topic_state_after_display_change("✔ Literal", True, "✔ Renamed") == (
        "Renamed",
        True,
    )
    assert topic_state_after_display_change("Renamed", True, "Renamed") == (
        "Renamed",
        False,
    )
