# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from collections.abc import Mapping
from typing import Any

WORKSPACE_DESCRIPTION_MAX_LENGTH = 10_000


def validate_description(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("Workspace description must be a string")
    if len(value) > WORKSPACE_DESCRIPTION_MAX_LENGTH:
        raise ValueError(
            "Workspace description must not exceed "
            f"{WORKSPACE_DESCRIPTION_MAX_LENGTH} characters"
        )
    return value


def validate_entity(entity_type: str, data: Mapping[str, Any]) -> None:
    if entity_type == "streams":
        validate_description(data.get("description", ""))
