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


def test_persistent_data_disk_grows_with_the_manifest() -> None:
    bootstrap = (ROOT / "exordos/images/bootstrap.sh").read_text()

    assert 'growpart "$PERSISTENT_DISK" 1' in bootstrap
    assert 'resize2fs "$PERSISTENT_PARTITION"' in bootstrap


def test_incomplete_first_boot_migration_preserves_valid_postgresql() -> None:
    bootstrap = (ROOT / "exordos/images/bootstrap.sh").read_text()

    assert 'if [[ ! -f "$PERSIST_MIGRATE_MARKER" ]]' in bootstrap
    assert 'postgres_cluster_valid "$PERSISTENT_POSTGRESQL_DIR"' in bootstrap
    assert 'if mountpoint -q "/var/lib/postgresql"' in bootstrap
    assert 'umount "/var/lib/postgresql"' in bootstrap
    assert "rebuild_image_postgres_cluster" in bootstrap
    assert 'rm -rf -- "$PERSISTENT_RUNTIME_DIR"' not in bootstrap


def test_exordos_manifest_renders_without_implicit_values() -> None:
    source = (ROOT / "exordos/manifests/workspace_zulip_bridge.yaml.j2").read_text()
    rendered = (
        Environment(undefined=StrictUndefined)
        .from_string(source)
        .render(
            version="0.1.0",
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
            {"size": 64, "label": "data"},
        ],
    }
    infrastructure_project_id = "12345678-c625-4fee-81d5-f691897b8142"
    workspace_project_id = "$workspace_zulip_bridge.imports.$workspace_project_id:value"
    assert node["project_id"] == infrastructure_project_id
    assert "$core.compute.volumes" not in manifest["resources"]
    assert manifest["exports"]["bridge_node"] == {
        "kind": "resource",
        "link": "$core.compute.nodes.$bridge_node",
    }
    assert manifest["requirements"]["workspace"]["from_version"] == "1.2.5-dev"
    assert manifest["imports"]["workspace_provider_sync_role"]["element"] == (
        "$workspace"
    )
    assert manifest["imports"]["workspace_project_id"] == {
        "element": "$workspace",
        "kind": "resource",
        "link": "$core.vs.variables.$workspace_project_id",
    }
    assert (
        manifest["resources"]["$core.iam.rolebinding"][
            "workspace_zulip_bridge_provider_sync"
        ]["project"]
        == workspace_project_id
    )
    config = manifest["resources"]["$core.config.configs"][
        "workspace_zulip_bridge_config"
    ]["body"]["content"]
    assert "WZB_WORKSPACE_CONTROL_URL=" in config
    assert f"WZB_WORKSPACE_PROJECT_ID={{{workspace_project_id}}}" in config
    assert "WZB_WORKSPACE_USERNAME=" in config
    assert "WZB_WORKSPACE_PASSWORD_FILE=" in config
    on_change = manifest["resources"]["$core.config.configs"][
        "workspace_zulip_bridge_config"
    ]["on_change"]["command"]
    assert "rm -f /var/lib/workspace_zulip_bridge/workspace.token" in on_change
    assert "workspace.refresh-token" in on_change
    assert "control/desired-state-v4-cursor" in on_change
    assert "control/desired-state-cursor" not in on_change
