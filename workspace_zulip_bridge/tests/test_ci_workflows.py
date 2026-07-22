import os
import pathlib
import re
import shutil
import stat
import subprocess
import textwrap

import pytest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOWS = PROJECT_ROOT / ".github" / "workflows"
TOX_CONFIG = PROJECT_ROOT / "tox.ini"


def _read(name: str) -> str:
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def test_test_workflow_runs_quality_and_postgres_integration() -> None:
    workflow = _read("tests.yaml")

    assert 'python-version: ["3.11"]' in workflow
    assert "uv tool install tox --with tox-uv" in workflow
    assert "tox -e ruff" in workflow
    assert "tox -e py311" in workflow
    assert "postgres:16-alpine" in workflow
    assert "WORKSPACE_BRIDGE_TEST_POSTGRES_DSN" in workflow
    assert "pass_env = WORKSPACE_BRIDGE_TEST_POSTGRES_DSN" in TOX_CONFIG.read_text(
        encoding="utf-8"
    )


def test_element_workflow_pins_cli_and_always_publishes_build() -> None:
    workflow = _read("exordos-element.yml")

    assert "runs-on: [self-hosted, vm]" in workflow
    assert "releases/download/3.1.3/exordos-linux" in workflow
    assert (
        "166c193394263996723393ad79f0133e956bae1c3b7bb8d5bcb02ee532bd91ee" in workflow
    )
    assert '"${EXORDOS_BIN}" build .' in workflow
    assert '"${EXORDOS_BIN}" push .' in workflow
    assert "PUSH_CFG: ${{ secrets.PUSH_CFG }}" in workflow

    publish_step = workflow.split("- name: Publish element", 1)[1]
    publish_step = publish_step.split(
        "- name: Build, verify, and publish immutable production bridge", 1
    )[0]
    assert re.search(r"^\s+if:", publish_step, flags=re.MULTILINE) is None


def _production_release_step() -> str:
    return _read("exordos-element.yml").split(
        "- name: Build, verify, and publish immutable production bridge", 1
    )[1]


def _production_release_shell() -> str:
    workflow = _read("exordos-element.yml")
    step = workflow.split(
        "- name: Build, verify, and publish immutable production bridge", 1
    )[1]
    run = step.split("        run: |\n", 1)[1]
    lines: list[str] = []
    for line in run.splitlines():
        if line and not line.startswith("          "):
            break
        lines.append(line[10:] if line.startswith("          ") else line)
    return "\n".join(lines)


def _production_release_version_block() -> str:
    shell = _production_release_shell()
    return shell.split('commit_list="$(git rev-list "${original_head}")"', 1)[
        1
    ].split(
        "\ngit commit --quiet --allow-empty", 1
    )[0]


def _final_evidence_function() -> str:
    shell = _production_release_shell()
    body = shell.split("verify_final_evidence() {", 1)[1].split("\n}", 1)[0]
    return "verify_final_evidence() {" + body + "\n}"


def test_production_release_is_manual_unique_and_immutable() -> None:
    workflow = _read("exordos-element.yml")
    release = _production_release_step()

    assert "- production_release" in workflow
    assert "github.event_name == 'workflow_dispatch'" in release
    assert "inputs.profile == 'production_release'" in release
    assert "git commit --quiet --allow-empty" in release
    assert "git tag \"${release_version}\"" in release
    assert 'test ! -e "${release_root}"' in release
    assert 'test -z "$(git tag --list "${release_version}")"' in release
    assert 'git tag --points-at "${commit}" --list' in release
    assert 'commit_list="$(git rev-list "${original_head}")"' in release
    assert 'done <<< "${commit_list}"' in release
    assert "done < <(git rev-list" not in release
    version_block = _production_release_version_block()
    assert "show-ref" not in version_block
    assert "|| true" not in version_block
    push = release.split('"${EXORDOS_BIN}" push .', 1)[1]
    assert "--force" not in push
    assert 'grep -Fq " already exists."' in release


def test_production_release_evidence_is_private_prepared_and_verifiable() -> None:
    release = _production_release_step()

    assert (
        "EVIDENCE_ARCHIVE_ROOT: "
        "${{ secrets.WORKSPACE_BRIDGE_RELEASE_EVIDENCE_DIR }}" in release
    )
    assert ': "${EVIDENCE_ARCHIVE_ROOT:?' in release
    assert 'chmod 0700 "${EVIDENCE_ARCHIVE_ROOT}"' in release
    assert 'chmod 0600 "${archive_tmp}/evidence.sha256"' in release
    assert 'mv "${archive_tmp}" "${archive_final}"' in release
    assert release.index('printf \'%s\\n\' "prepared"') < release.index(
        '"${EXORDOS_BIN}" push .'
    )
    assert "published|publication_failed" in release
    assert 'test "$(cat "${archive_final}/state.txt")" = prepared' in release
    assert 'push_immutable "${archive_final}/push.log"' in release
    assert "update_evidence_state publication_failed" in release
    assert "update_evidence_state published" in release
    assert "verify_final_evidence publication_failed" in release
    assert "verify_final_evidence published" in release
    assert release.index("verify_final_evidence published") < release.index(
        "printf 'version=%s\\n'"
    )
    assert "actions/upload-artifact" not in release
    assert 'echo "${EVIDENCE_ARCHIVE_ROOT}"' not in release


def test_production_release_records_exact_build_identity_and_integrity() -> None:
    release = _production_release_step()

    for evidence_name in (
        "source-commit.txt",
        "source-tree.txt",
        "release-commit.txt",
        "version.txt",
        "exordos-download.sha256",
        "exordos-version.txt",
        "workspace_zulip_bridge.yaml",
        "inventory.json",
        "compression-check.txt",
        "artifact.sha256",
        "evidence.sha256",
    ):
        assert evidence_name in release
    assert "zstd -t -q" in release
    assert "xargs -0 sha256sum" in release
    assert 'inventory.get("version") != expected' in release
    assert '> "${archive_tmp}/build.log" 2>&1' in release
    assert 'echo "${EVIDENCE_ARCHIVE_ROOT}"' not in release
    assert "actions/upload-artifact" not in release
    assert "version=%s\\n" in release
    assert "Production bridge version: %s\\n" in release


@pytest.mark.parametrize("failed_command", ("rev-list", "tag"))
def test_production_release_version_lookup_fails_on_injected_git_error(
    tmp_path: pathlib.Path,
    failed_command: str,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    subprocess.run(["git", "init", "--quiet"], cwd=checkout, check=True)
    subprocess.run(
        ["git", "config", "user.name", "CASSI test"], cwd=checkout, check=True
    )
    subprocess.run(
        ["git", "config", "user.email", "cassi@exordos.com"],
        cwd=checkout,
        check=True,
    )
    subprocess.run(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "source"],
        cwd=checkout,
        check=True,
    )
    real_git = shutil.which("git")
    assert real_git is not None
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_git = fake_bin / "git"
    fake_git.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env bash
            if [[ "${{FAKE_GIT_FAILURE}}" == "rev-list" \
              && "${{1:-}}" == "rev-list" ]]; then
              echo "injected rev-list failure" >&2
              exit 73
            fi
            if [[ "${{FAKE_GIT_FAILURE}}" == "tag" \
              && "${{1:-}}" == "tag" && "${{2:-}}" == "--points-at" ]]; then
              echo "injected tag failure" >&2
              exit 73
            fi
            exec {real_git} "$@"
            """
        ),
        encoding="utf-8",
    )
    fake_git.chmod(0o755)
    script = (
        "set -euo pipefail\n"
        'original_head="$(git rev-parse HEAD)"\n'
        'commit_list="$(git rev-list "${original_head}")"'
        f"{_production_release_version_block()}\n"
    )
    result = subprocess.run(
        ["bash", "-c", script],
        cwd=checkout,
        env={
            **os.environ,
            "FAKE_GIT_FAILURE": failed_command,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
        },
        text=True,
        capture_output=True,
    )

    assert result.returncode == 73
    assert f"injected {failed_command} failure" in result.stderr


def test_final_evidence_readback_requires_digest_and_source_binding(
    tmp_path: pathlib.Path,
) -> None:
    archive = tmp_path / "evidence"
    archive.mkdir(mode=0o700)
    expected = {
        "state.txt": "published\n",
        "version.txt": "1.2.3-rc.20260719010101+abcdef12\n",
        "source-commit.txt": f"{'a' * 40}\n",
        "source-tree.txt": f"{'b' * 40}\n",
        "release-commit.txt": f"{'c' * 40}\n",
    }
    for name, value in expected.items():
        (archive / name).write_text(value, encoding="utf-8")

    def write_digest() -> None:
        subprocess.run(
            [
                "bash",
                "-c",
                "find . -type f ! -name evidence.sha256 -print0 "
                "| sort -z | xargs -0 sha256sum > evidence.sha256",
            ],
            cwd=archive,
            check=True,
        )

    env_script = (
        f"archive_final={archive!s}\n"
        f"release_version={expected['version.txt'].strip()}\n"
        f"original_head={expected['source-commit.txt'].strip()}\n"
        f"source_tree={expected['source-tree.txt'].strip()}\n"
        f"release_head={expected['release-commit.txt'].strip()}\n"
        f"{_final_evidence_function()}\n"
    )
    write_digest()
    valid = subprocess.run(
        ["bash", "-c", env_script + "verify_final_evidence published\n"],
        text=True,
        capture_output=True,
    )
    assert valid.returncode == 0, valid.stderr

    (archive / "version.txt").write_text("9.9.9\n", encoding="utf-8")
    write_digest()
    mismatched_binding = subprocess.run(
        ["bash", "-c", env_script + "verify_final_evidence published\n"],
        text=True,
        capture_output=True,
    )
    assert mismatched_binding.returncode != 0

    (archive / "version.txt").write_text(expected["version.txt"], encoding="utf-8")
    write_digest()
    (archive / "source-tree.txt").write_text("corrupted\n", encoding="utf-8")
    invalid_digest = subprocess.run(
        ["bash", "-c", env_script + "verify_final_evidence published\n"],
        text=True,
        capture_output=True,
    )
    assert invalid_digest.returncode != 0


@pytest.mark.parametrize(
    ("push_result", "expected_state", "expected_returncode"),
    (
        ("success", "published", 0),
        ("failure", "publication_failed", 1),
        ("collision", "publication_failed", 1),
    ),
)
def test_production_release_preserves_verifiable_evidence_for_push_outcome(
    tmp_path: pathlib.Path,
    push_result: str,
    expected_state: str,
    expected_returncode: int,
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "source.txt").write_text("immutable source\n", encoding="utf-8")
    subprocess.run(["git", "init", "--quiet"], cwd=checkout, check=True)
    subprocess.run(
        ["git", "config", "user.name", "CASSI test"], cwd=checkout, check=True
    )
    subprocess.run(
        ["git", "config", "user.email", "cassi@exordos.com"],
        cwd=checkout,
        check=True,
    )
    subprocess.run(["git", "add", "source.txt"], cwd=checkout, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "test source"],
        cwd=checkout,
        check=True,
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_exordos = fake_bin / "exordos"
    fake_exordos.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            command="$1"
            shift
            case "$command" in
              version)
                printf '%s\\n' 'exordos 3.0.2'
                ;;
              build)
                output_dir=''
                while (($#)); do
                  if [[ "$1" == '--output-dir' ]]; then
                    output_dir="$2"
                    shift 2
                  else
                    shift
                  fi
                done
                version="$(git describe --tags --exact-match HEAD)"
                element_dir="${output_dir}/exordos-elements/workspace_zulip_bridge/${version}"
                mkdir -p "${element_dir}/manifests" "${element_dir}/images"
                printf 'name: "workspace_zulip_bridge"\\nversion: "%s"\\n' "$version" \\
                  > "${element_dir}/manifests/workspace_zulip_bridge.yaml"
                printf '{"version":"%s"}\\n' "$version" \\
                  > "${element_dir}/inventory.json"
                printf '%s\\n' 'compressed-image' \\
                  > "${element_dir}/images/workspace-zulip-bridge.raw.zst"
                ;;
              push)
                case "${FAKE_PUSH_RESULT}" in
                  success) printf '%s\\n' 'published' ;;
                  failure) printf '%s\\n' 'repository unavailable'; exit 2 ;;
                  collision) printf '%s\\n' 'Version already exists.' ;;
                esac
                ;;
            esac
            """
        ),
        encoding="utf-8",
    )
    fake_exordos.chmod(fake_exordos.stat().st_mode | stat.S_IXUSR)
    fake_zstd = fake_bin / "zstd"
    fake_zstd.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_zstd.chmod(fake_zstd.stat().st_mode | stat.S_IXUSR)

    archive_root = tmp_path / "private-evidence"
    github_output = tmp_path / "github-output"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "ARTIFACTS_PATH": "output",
        "EVIDENCE_ARCHIVE_ROOT": str(archive_root),
        "EXORDOS_BIN": str(fake_exordos),
        "EXORDOS_CFG": "exordos/exordos.yaml",
        "EXORDOS_RELEASE_SHA256": "a" * 64,
        "FAKE_PUSH_RESULT": push_result,
        "GITHUB_OUTPUT": str(github_output),
        "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_RUN_ID": "12345",
        "PUSH_CFG": "cHVzaDoge30K",
        "RUNNER_TEMP": str(tmp_path / "runner-temp"),
    }
    pathlib.Path(env["RUNNER_TEMP"]).mkdir()
    result = subprocess.run(
        ["bash", "-s"],
        cwd=checkout,
        env=env,
        input=_production_release_shell(),
        text=True,
        capture_output=True,
    )

    assert result.returncode == expected_returncode, result.stderr
    archive = archive_root / "workspace-zulip-bridge-release-12345-1"
    assert (archive / "state.txt").read_text(
        encoding="utf-8"
    ).strip() == expected_state
    assert (archive / "prepared-at.txt").is_file()
    assert (archive / "push.log").is_file()
    assert stat.S_IMODE(archive.stat().st_mode) == 0o700
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o600
        for path in archive.iterdir()
        if path.is_file()
    )
    subprocess.run(
        ["sha256sum", "--check", "evidence.sha256"], cwd=archive, check=True
    )
    combined_output = result.stdout + result.stderr
    assert str(archive_root) not in combined_output
    assert "Version already exists." not in combined_output
    if expected_state == "published":
        assert "version=" in github_output.read_text(encoding="utf-8")
    else:
        assert not github_output.exists()


def test_production_release_evidence_state_rejects_invalid_or_repeated_update(
    tmp_path: pathlib.Path,
) -> None:
    shell = _production_release_shell()
    function = shell.split("update_evidence_state() {", 1)[1].split("\n}", 1)[0]
    function = "update_evidence_state() {" + function + "\n}"
    archive = tmp_path / "evidence"
    archive.mkdir(mode=0o700)
    (archive / "state.txt").write_text("prepared\n", encoding="utf-8")
    env_script = f"archive_final={archive!s}\n{function}\n"

    invalid = subprocess.run(
        ["bash", "-c", env_script + "update_evidence_state invalid\n"],
        text=True,
        capture_output=True,
    )
    assert invalid.returncode != 0
    assert (archive / "state.txt").read_text(encoding="utf-8") == "prepared\n"

    published = subprocess.run(
        ["bash", "-c", env_script + "update_evidence_state published\n"],
        text=True,
        capture_output=True,
    )
    repeated = subprocess.run(
        [
            "bash",
            "-c",
            env_script + "update_evidence_state publication_failed\n",
        ],
        text=True,
        capture_output=True,
    )
    assert published.returncode == 0, published.stderr
    assert repeated.returncode != 0
    assert (archive / "state.txt").read_text(encoding="utf-8") == "published\n"
    subprocess.run(
        ["sha256sum", "--check", "evidence.sha256"], cwd=archive, check=True
    )


def test_workflows_do_not_disclose_private_infrastructure() -> None:
    workflow = "\n".join((_read("tests.yaml"), _read("exordos-element.yml")))

    assert (
        re.search(
            r"\b(?:192\.168|10\.|172\.(?:1[6-9]|2\d|3[01]))\.\d+\.\d+\b", workflow
        )
        is None
    )
