import pathlib
import re

ROOT = pathlib.Path(__file__).parents[2]
MANIFEST = ROOT / "exordos/manifests/workspace_zulip_bridge.yaml.j2"
BUILD_CONFIG = ROOT / "exordos/exordos.yaml"


def test_build_manifest_filename_matches_element_name():
    text = MANIFEST.read_text(encoding="utf-8")
    name = re.search(r'^name: "([^"]+)"$', text, re.MULTILINE)

    assert name is not None
    assert MANIFEST.name == f"{name.group(1)}.yaml.j2"


def test_workspace_imports_use_underlying_exported_resource_links():
    text = MANIFEST.read_text(encoding="utf-8")
    imports = text.split("imports:", maxsplit=1)[1].split("resources:", maxsplit=1)[0]
    expected_links = {
        "workspace_backend_node": "$core.compute.nodes.$workspace_backend",
        "workspace_zulip_bridge_enrollment_secret": (
            "$core.secret.passwords.$workspace_zulip_bridge_enrollment_secret"
        ),
    }

    assert ".exports." not in imports
    for import_name, link in expected_links.items():
        block = imports.split(f"  {import_name}:\n", maxsplit=1)[1]
        next_import = re.search(r"^  [a-z0-9_]+:\n", block, re.MULTILINE)
        if next_import is not None:
            block = block[: next_import.start()]
        assert 'element: "$workspace"' in block
        assert f'link: "{link}"' in block


def test_zb_deploy_002_root_is_replaceable_and_data_disk_is_persistent():
    text = MANIFEST.read_text(encoding="utf-8")
    assert 'kind: "disks"' in text
    assert "label: root" in text
    assert "label: data" in text
    assert text.index("label: root") < text.index("label: data")
    assert "workspace_zulip_bridge']" in text


def test_zb_deploy_001_is_one_private_node_without_public_load_balancer():
    text = MANIFEST.read_text(encoding="utf-8")
    assert text.count("workspace_zulip_bridge:\n") >= 1
    assert "$core.compute.nodes:" in text
    assert "load_balancer" not in text.lower()
    assert "public:" not in text.lower()


def test_zb_deploy_002_runtime_state_and_identity_paths_are_persistent():
    bootstrap = (ROOT / "exordos/images/bootstrap.sh").read_text(encoding="utf-8")
    helpers = (ROOT / "exordos/images/bootstrap-persistence.sh").read_text(
        encoding="utf-8"
    )
    assert "find_persistent_disk" in helpers
    assert "/var/lib/postgresql" in bootstrap
    assert "/var/lib/workspace-zulip-bridge" in bootstrap
    assert "bridge_prepare_persistent_mount" in bootstrap
    assert "bridge_make_persistent_mount_private" in bootstrap
    assert "mount --make-rprivate" in helpers
    assert bootstrap.count("bridge_migrate_to_persistent") == 2
    assert "stat -L -c '%d'" in helpers
    assert "stat -L -c '%d:%i'" in helpers
    assert "migrate_to_persistent" in helpers


def test_repeated_bootstrap_keeps_running_postgresql_available():
    bootstrap = (ROOT / "exordos/images/bootstrap.sh").read_text(encoding="utf-8")
    migration_guard = "if bridge_persistence_migration_is_required"
    stop_postgres = "systemctl stop postgresql.service"
    start_postgres = "systemctl start postgresql.service"

    assert migration_guard in bootstrap
    guarded_body = bootstrap.split(migration_guard, maxsplit=1)[1].split(
        "\nfi", maxsplit=1
    )[0]
    assert stop_postgres in guarded_body
    assert bootstrap.count(stop_postgres) == 1
    assert start_postgres in bootstrap
    assert bootstrap.index(start_postgres) < bootstrap.index(
        "bridge_wait_for_postgresql"
    )
    assert bootstrap.index("bridge_wait_for_postgresql") < bootstrap.index(
        "workspace-zulip-bridge-enroll"
    )


def test_database_peer_identity_matches_runtime_service_user():
    text = MANIFEST.read_text(encoding="utf-8")
    bootstrap = (ROOT / "exordos/images/bootstrap.sh").read_text(encoding="utf-8")

    assert "[db]" in text
    assert "[database]" not in text
    assert "connection_url = postgresql:///workspace_zulip_bridge" in text
    assert "user: workspace-zulip" in text
    assert "DATABASE_ROLE=workspace-zulip" in bootstrap
    assert 'createuser "$DATABASE_ROLE"' in bootstrap
    assert 'createdb -O "$DATABASE_ROLE" "$DATABASE_NAME"' in bootstrap
    assert "rolname='workspace_zulip'" not in bootstrap
    assert "createuser workspace_zulip" not in bootstrap
    assert "createdb -O workspace_zulip" not in bootstrap


def test_runtime_health_directory_is_owned_by_service_user():
    bootstrap = (ROOT / "exordos/images/bootstrap.sh").read_text(encoding="utf-8")
    install = (ROOT / "exordos/images/install.sh").read_text(encoding="utf-8")
    expected = "install -d -m 0755 -o workspace-zulip -g workspace-zulip"

    assert f'{expected} "$RUN_DIR"' in bootstrap
    assert bootstrap.index(expected) < bootstrap.index(
        'exec 9>"$RUN_DIR/bootstrap.lock"'
    )
    assert expected in install
    assert "/run/workspace-zulip-bridge" in install
    assert 'install -d -m 0755 "$RUN_DIR"' not in bootstrap


def test_manifest_has_no_plaintext_provider_credentials():
    text = MANIFEST.read_text(encoding="utf-8").lower()
    assert "api_key" not in text
    assert "zulip_email" not in text
    assert "zulip_server" not in text
    assert "credential_private_key_file" in text


def test_all_config_inline_references_use_core_fstring_rendering():
    text = MANIFEST.read_text(encoding="utf-8")
    config_resources = text.split("  $core.config.configs:", maxsplit=1)[1].split(
        "\n  $workspace_zulip_bridge.imports.", maxsplit=1
    )[0]
    rendered_values = []
    for resource in re.split(r"\n(?=    \S)", config_resources):
        content = re.search(r"^        content: (.*)$", resource, re.MULTILINE)
        if content is None:
            continue
        scalar = content.group(1)
        value = resource[content.end() + 1 :] if scalar == "|" else scalar
        value = value[1:-1] if value.startswith("'") else value
        if "{$" not in value:
            continue
        rendered_values.append(value)
        assert value.lstrip().startswith('f"')
        assert value.rstrip().endswith('"')

    assert len(rendered_values) == 2


def test_runtime_manifest_has_no_mail_data_plane_resources():
    text = MANIFEST.read_text(encoding="utf-8")
    assert "workspace_mail" not in text
    assert "workspace-mail" not in text
    assert "imap" not in text.lower()
    assert "smtp" not in text.lower()
    assert "[provider_api]" in text


def test_realm_identity_and_backend_node_are_cross_element_resources():
    text = MANIFEST.read_text(encoding="utf-8")
    canonical_control = (
        "workspace-bridge-control."
        "{$workspace_zulip_bridge.imports.$core_local_domain:name}"
    )
    assert 'link: "$core.compute.nodes.$workspace_backend"' in text
    assert (
        "realm_uuid = {$workspace_zulip_bridge.imports.$core_local_domain:uuid}" in text
    )
    assert "{{ realm_uuid }}" not in text
    assert (
        "address: $workspace_zulip_bridge.imports.$workspace_backend_node:"
        "default_network:ipv4"
    ) in text
    assert f"base_url = https://{canonical_control}:21443" in text
    assert f"bootstrap_url = http://{canonical_control}:21085" in text
    assert f"hostname = {canonical_control}" in text
    assert "name: workspace-bridge-control" in text
    assert text.count(f"base_url = https://{canonical_control}:21443") == 3
    assert "{{ workspace_control_url }}" not in text


def test_enrollment_identity_uses_workspace_export_and_readable_secret_files():
    text = MANIFEST.read_text(encoding="utf-8")
    imported = (
        "$workspace_zulip_bridge.imports.$workspace_zulip_bridge_enrollment_secret"
    )
    assert (
        'link: "$core.secret.passwords.$workspace_zulip_bridge_enrollment_secret"'
        in text
    )
    assert f"bridge_instance_uuid = {{{imported}:uuid}}" in text
    assert f"content: 'f\"{{{imported}:value}}\"'" in text
    resources = text.split("resources:", maxsplit=1)[1]
    assert (
        "$core.secret.passwords.$workspace_zulip_bridge_enrollment_secret"
        not in resources
    )

    configs_start = text.index("  $core.config.configs:")
    for resource_name in (
        "workspace_zulip_bridge_config:",
        "workspace_zulip_bridge_enrollment_secret:",
    ):
        start = text.index(f"    {resource_name}", configs_start)
        first_body_line = text.index("\n", start) + 1
        next_resource = re.search(r"^    \S", text[first_body_line:], re.MULTILINE)
        assert next_resource is not None
        body = text[start : first_body_line + next_resource.start()]
        assert 'mode: "0640"' in body
        assert "owner: root" in body
        assert "group: workspace-zulip" in body


def test_control_transport_retry_policy_is_explicit_in_runtime_config():
    text = MANIFEST.read_text(encoding="utf-8")
    assert "poll_interval_seconds = 2" in text
    assert "heartbeat_interval_seconds = 10" in text
    assert "retry_base_seconds = 1" in text
    assert "retry_cap_seconds = 30" in text
    assert "retry_after_cap_seconds = 300" in text


def test_provider_api_reuses_enrolled_control_mtls_identity():
    text = MANIFEST.read_text(encoding="utf-8")
    bootstrap = (ROOT / "exordos/images/bootstrap.sh").read_text(encoding="utf-8")
    install = (ROOT / "exordos/images/install.sh").read_text(encoding="utf-8")
    provider = text.split("          [provider_api]", maxsplit=1)[1].split(
        "          [file_api]", maxsplit=1
    )[0]
    assert "control-ca.crt" in provider
    assert "bridge.crt" in provider
    assert "bridge.key" in provider
    assert "workspace-mail" not in install
    assert "workspace-mail" not in bootstrap


def test_image_uses_isolated_venv_for_install_and_all_python_entrypoints():
    text = MANIFEST.read_text(encoding="utf-8")
    install = (ROOT / "exordos/images/install.sh").read_text(encoding="utf-8")
    bootstrap = (ROOT / "exordos/images/bootstrap.sh").read_text(encoding="utf-8")
    venv = "/opt/workspace-zulip-bridge-venv"

    assert f"path: {venv}/bin/workspace-zulip-bridge --config" in text
    assert "path: /usr/local/bin/workspace-zulip-bridge --config" not in text
    assert f"VENV={venv}" in install
    assert 'sudo python3 -m venv "$VENV"' in install
    assert 'sudo "$VENV/bin/python" -m pip install "$SOURCE"' in install
    assert "--break-system-packages" not in install
    assert "python3 -m pip install" not in install
    assert f"VENV={venv}" in bootstrap
    assert '"$VENV/bin/workspace-zulip-bridge-enroll"' in bootstrap
    assert '"$VENV/bin/ra-apply-migration"' in bootstrap
    assert '--config-file "$CONFIG"' in bootstrap
    assert '--path "$SOURCE/migrations"' in bootstrap


def test_image_dependency_source_is_only_this_repository_and_excludes_bytecode():
    text = BUILD_CONFIG.read_text(encoding="utf-8")
    source = re.search(r"^\s+src: (\S+)$", text, re.MULTILINE)

    assert source is not None
    declared_path = pathlib.PurePosixPath(source.group(1))
    resolved_path = (BUILD_CONFIG.parent / declared_path).resolve()
    assert declared_path.name == ROOT.name
    assert declared_path.name not in {"", ".", ".."}
    assert resolved_path == ROOT.resolve()
    assert '        - "*/__pycache__"' in text
    assert '        - "*.pyc"' in text
