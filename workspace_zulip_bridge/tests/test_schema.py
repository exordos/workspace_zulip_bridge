# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import re
from importlib import resources
from pathlib import Path


def test_v4_schema_only_creates_prefixed_tables() -> None:
    schema = (
        resources.files("workspace_zulip_bridge")
        .joinpath("schema_v4.sql")
        .read_text(encoding="utf-8")
    )

    assert "v4_external_accounts" in schema
    assert "v4_zulip_queues" in schema
    assert "v4_workspace_event_cursors" in schema
    assert "DROP TABLE" not in schema
    assert "ALTER TABLE" not in schema
    for line in schema.splitlines():
        if line.startswith("CREATE TABLE"):
            assert "workspace_zulip_bridge.v4_" in line


def test_runtime_never_loads_legacy_schema_files() -> None:
    source = (Path(__file__).parents[1] / "database.py").read_text(encoding="utf-8")

    assert 'joinpath("schema_v4.sql")' in source
    assert 'joinpath("schema.sql")' not in source
    assert 'joinpath("schema_upgrades.sql")' not in source


def test_runtime_sql_only_accesses_v4_tables() -> None:
    package = Path(__file__).parents[1]
    table_pattern = re.compile(
        r"\b(?:FROM|JOIN|UPDATE|INSERT\s+INTO|DELETE\s+FROM)\s+"
        r"workspace_zulip_bridge\.([a-z][a-z0-9_]*)",
    )
    tables = {
        table
        for source_path in package.glob("*.py")
        for table in table_pattern.findall(source_path.read_text(encoding="utf-8"))
    }

    assert tables
    assert all(table.startswith("v4_") for table in tables)
