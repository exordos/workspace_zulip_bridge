import pathlib
import subprocess

ROOT = pathlib.Path(__file__).parents[2]
HELPERS = ROOT / "exordos/images/bootstrap-persistence.sh"


def run_bash(script: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-ceu", script, "bootstrap-test", str(HELPERS), *args],
        check=False,
        text=True,
        capture_output=True,
        timeout=5,
    )


def test_mounted_persistent_filesystem_skips_disk_discovery_and_mount_tools(
    tmp_path,
):
    mount_path = tmp_path / "persist"
    mount_path.mkdir()
    result = run_bash(
        r"""
        source "$1"
        mount_path="$2"
        stat() {
            case "${*: -1}" in
                /) printf '101\n' ;;
                "$mount_path") printf '202\n' ;;
                *) command stat "$@" ;;
            esac
        }
        find_persistent_disk() {
            echo "disk discovery must not run" >&2
            return 91
        }
        prepare_persistent_disk() {
            echo "persistent disk preparation must not run" >&2
            return 92
        }

        bridge_prepare_persistent_mount "$mount_path"
        """,
        str(mount_path),
    )

    assert result.returncode == 0, result.stderr
    assert "already mounted" in result.stdout
    assert "must not run" not in result.stderr


def test_unmounted_persistent_path_keeps_first_run_disk_preparation(tmp_path):
    mount_path = tmp_path / "persist"
    mount_path.mkdir()
    trace = tmp_path / "prepare-trace"
    result = run_bash(
        r"""
        source "$1"
        mount_path="$2"
        trace_file="$3"
        stat() {
            case "${*: -1}" in
                /|"$mount_path") printf '101\n' ;;
                *) command stat "$@" ;;
            esac
        }
        find_persistent_disk() { printf '/dev/vdb\n'; }
        prepare_persistent_disk() { printf '%s|%s\n' "$1" "$2" > "$trace_file"; }

        bridge_prepare_persistent_mount "$mount_path"
        """,
        str(mount_path),
        str(trace),
    )

    assert result.returncode == 0, result.stderr
    assert trace.read_text(encoding="utf-8") == f"/dev/vdb|{mount_path}\n"


def test_persistent_mount_is_made_recursively_private_before_bind_migration(
    tmp_path,
):
    mount_path = tmp_path / "persist"
    mount_path.mkdir()
    trace = tmp_path / "mount-trace"
    result = run_bash(
        r"""
        source "$1"
        trace_file="$3"
        mount() { printf '%s\n' "$*" > "$trace_file"; }

        bridge_make_persistent_mount_private "$2"
        """,
        str(mount_path),
        str(trace),
    )

    assert result.returncode == 0, result.stderr
    assert trace.read_text(encoding="utf-8") == f"--make-rprivate {mount_path}\n"


def test_existing_bind_target_skips_shared_migration_helper(tmp_path):
    persistent_dir = tmp_path / "persistent"
    old_data_dir = tmp_path / "old"
    persistent_dir.mkdir()
    old_data_dir.symlink_to(persistent_dir, target_is_directory=True)
    result = run_bash(
        r"""
        source "$1"
        migrate_to_persistent() {
            echo "shared migration helper must not run" >&2
            return 93
        }

        bridge_migrate_to_persistent "$2" "$3"
        """,
        str(old_data_dir),
        str(persistent_dir),
    )

    assert result.returncode == 0, result.stderr
    assert "already active; skipping" in result.stdout
    assert "must not run" not in result.stderr


def test_distinct_paths_keep_first_run_data_migration(tmp_path):
    old_data_dir = tmp_path / "old"
    persistent_dir = tmp_path / "persistent"
    trace = tmp_path / "migration-trace"
    old_data_dir.mkdir()
    result = run_bash(
        r"""
        source "$1"
        trace_file="$4"
        migrate_to_persistent() { printf '%s|%s\n' "$1" "$2" > "$trace_file"; }

        bridge_migrate_to_persistent "$2" "$3"
        """,
        str(old_data_dir),
        str(persistent_dir),
        str(trace),
    )

    assert result.returncode == 0, result.stderr
    assert trace.read_text(encoding="utf-8") == (f"{old_data_dir}|{persistent_dir}\n")


def test_completed_persistence_does_not_require_disruptive_migration(tmp_path):
    persistent_postgres = tmp_path / "persistent-postgres"
    persistent_bridge = tmp_path / "persistent-bridge"
    persistent_postgres.mkdir()
    persistent_bridge.mkdir()
    postgres_data = tmp_path / "postgres"
    bridge_data = tmp_path / "bridge"
    postgres_data.symlink_to(persistent_postgres, target_is_directory=True)
    bridge_data.symlink_to(persistent_bridge, target_is_directory=True)
    result = run_bash(
        r"""
        source "$1"
        if bridge_persistence_migration_is_required "$2" "$3" "$4" "$5"; then
            echo "migration required" >&2
            exit 94
        fi
        """,
        str(postgres_data),
        str(persistent_postgres),
        str(bridge_data),
        str(persistent_bridge),
    )

    assert result.returncode == 0, result.stderr
    assert "migration required" not in result.stderr


def test_incomplete_persistence_requires_migration(tmp_path):
    persistent_postgres = tmp_path / "persistent-postgres"
    persistent_bridge = tmp_path / "persistent-bridge"
    persistent_postgres.mkdir()
    persistent_bridge.mkdir()
    postgres_data = tmp_path / "postgres"
    bridge_data = tmp_path / "bridge"
    postgres_data.symlink_to(persistent_postgres, target_is_directory=True)
    bridge_data.mkdir()
    result = run_bash(
        r"""
        source "$1"
        bridge_persistence_migration_is_required "$2" "$3" "$4" "$5"
        """,
        str(postgres_data),
        str(persistent_postgres),
        str(bridge_data),
        str(persistent_bridge),
    )

    assert result.returncode == 0, result.stderr


def test_postgresql_readiness_retries_until_socket_accepts_connections():
    result = run_bash(
        r"""
        source "$1"
        readiness_calls=0
        runuser() {
            readiness_calls=$((readiness_calls + 1))
            [[ "$readiness_calls" -eq 3 ]]
        }
        sleep() { :; }

        bridge_wait_for_postgresql 4 0
        printf '%s\n' "$readiness_calls"
        """,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "3\n"


def test_postgresql_readiness_fails_after_bounded_attempts():
    result = run_bash(
        r"""
        source "$1"
        readiness_calls=0
        runuser() {
            readiness_calls=$((readiness_calls + 1))
            return 1
        }
        sleep() { :; }

        bridge_wait_for_postgresql 3 0
        """,
    )

    assert result.returncode == 1
    assert "did not become ready after 3 attempts" in result.stderr
