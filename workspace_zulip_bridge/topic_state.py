# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Canonical topic state and reversible Zulip display naming."""

from __future__ import annotations

DONE_PREFIX = "✔ "


def topic_display_name(name: str, is_done: bool) -> str:
    """Render the canonical topic state as a Zulip topic name."""

    return f"{DONE_PREFIX}{name}" if is_done else name


def topic_state_from_display_name(display_name: str) -> tuple[str, bool]:
    """Parse Zulip's leading resolved marker, removing exactly one prefix."""

    if display_name.startswith(DONE_PREFIX):
        return display_name.removeprefix(DONE_PREFIX), True
    return display_name, False


def topic_state_after_display_change(
    name: str,
    is_done: bool,
    display_name: str,
) -> tuple[str, bool]:
    """Resolve a Zulip rename using its authoritative prefix semantics."""

    del name, is_done
    return topic_state_from_display_name(display_name)
