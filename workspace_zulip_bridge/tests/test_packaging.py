# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from pathlib import Path

import yaml
from jinja2 import Environment
from jinja2 import StrictUndefined

ROOT = Path(__file__).parents[2]


def test_tox_passes_the_postgresql_test_dsn() -> None:
    tox_config = (ROOT / "tox.ini").read_text(encoding="utf-8")

    assert "pass_env = WZB_TEST_DATABASE_DSN" in tox_config


def test_exordos_configuration_selects_the_bridge_image() -> None:
    config = yaml.safe_load((ROOT / "exordos/exordos.yaml").read_text())

    dependency = config["build"]["deps"][0]
    element = config["build"]["elements"][0]
    assert "workspace_zulip_bridge.egg-info" in dependency["exclude"]
    assert element["manifest"] == "manifests/workspace_zulip_bridge.yaml.j2"
    assert element["images"][0]["name"] == "workspace-zulip-bridge"
    assert element["images"][0]["profile"] == "exordos_base"


def test_runtime_configuration_survives_image_replacement() -> None:
    service = (ROOT / "etc/systemd/workspace-zulip-bridge.service").read_text()
    bootstrap = (ROOT / "exordos/images/bootstrap.sh").read_text()

    assert "EnvironmentFile=-/var/lib/workspace_zulip_bridge/runtime.env" in service
    assert '"/var/lib/workspace_zulip_bridge"' in bootstrap
    assert '"${PERSISTENT_MOUNT}/var/lib/workspace_zulip_bridge"' in bootstrap
    assert '"$DATABASE_ROLE"' in bootstrap


def test_exordos_manifest_renders_without_implicit_values() -> None:
    source = (ROOT / "exordos/manifests/workspace_zulip_bridge.yaml.j2").read_text()
    rendered = (
        Environment(undefined=StrictUndefined)
        .from_string(source)
        .render(
            version="0.1.0",
            project_id="00000000-0000-0000-0000-000000000001",
            images={
                "workspace_zulip_bridge": (
                    "urn:images:00000000-0000-0000-0000-000000000000"
                )
            },
        )
    )
    manifest = yaml.safe_load(rendered)

    node = manifest["resources"]["$core.compute.nodes"]["bridge_node"]
    assert manifest["name"] == "workspace_zulip_bridge"
    assert node["cores"] == 2
    assert node["ram"] == 4096
    assert node["disk_spec"] == {
        "kind": "disks",
        "disks": [
            {
                "size": 6,
                "image": "urn:images:00000000-0000-0000-0000-000000000000",
                "label": "root",
            },
            {"size": 20, "label": "data"},
        ],
    }
    assert node["project_id"] == "00000000-0000-0000-0000-000000000001"
    assert "$core.compute.volumes" not in manifest["resources"]
    assert manifest["exports"]["bridge_node"] == {
        "kind": "resource",
        "link": "$core.compute.nodes.$bridge_node",
    }
