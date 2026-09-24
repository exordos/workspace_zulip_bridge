# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import pytest

from workspace_zulip_bridge import workspace_entities


def test_workspace_stream_description_uses_ten_thousand_character_limit() -> None:
    workspace_entities.validate_entity(
        "streams",
        {"description": "x" * workspace_entities.WORKSPACE_DESCRIPTION_MAX_LENGTH},
    )

    with pytest.raises(ValueError, match="must not exceed 10000 characters"):
        workspace_entities.validate_entity(
            "streams",
            {
                "description": "x"
                * (workspace_entities.WORKSPACE_DESCRIPTION_MAX_LENGTH + 1)
            },
        )


@pytest.mark.parametrize("description", [None, 100])
def test_workspace_stream_description_must_be_a_string(description: object) -> None:
    with pytest.raises(ValueError, match="must be a string"):
        workspace_entities.validate_entity(
            "streams",
            {"description": description},
        )


def test_other_workspace_entities_do_not_gain_a_description_field() -> None:
    workspace_entities.validate_entity("messages", {})


def test_workspace_stream_projection_bounds_derived_group_name() -> None:
    original = {
        "name": "x" * (workspace_entities.WORKSPACE_STREAM_NAME_MAX_LENGTH + 117),
        "description": "description",
    }

    projected = workspace_entities.project_entity("streams", original)

    assert (
        projected["name"] == "x" * workspace_entities.WORKSPACE_STREAM_NAME_MAX_LENGTH
    )
    assert projected["description"] == "description"
    assert original["name"] == "x" * 372


def test_other_workspace_entity_projection_is_unchanged() -> None:
    original = {"name": "x" * 300}

    assert workspace_entities.project_entity("topics", original) == original
