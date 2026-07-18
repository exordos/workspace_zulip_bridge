import pathlib
import re

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
    assert "releases/download/3.0.2/exordos-linux" in workflow
    assert (
        "469007b01253f69b5fcf540b8f6605a360c2539019a5b148fbabb0353bee6a5b" in workflow
    )
    assert '"${EXORDOS_BIN}" build .' in workflow
    assert '"${EXORDOS_BIN}" push .' in workflow
    assert "PUSH_CFG: ${{ secrets.PUSH_CFG }}" in workflow

    publish_step = workflow.split("- name: Publish element", 1)[1]
    assert re.search(r"^\s+if:", publish_step, flags=re.MULTILINE) is None


def test_workflows_do_not_disclose_private_infrastructure() -> None:
    workflow = "\n".join((_read("tests.yaml"), _read("exordos-element.yml")))

    assert (
        re.search(
            r"\b(?:192\.168|10\.|172\.(?:1[6-9]|2\d|3[01]))\.\d+\.\d+\b", workflow
        )
        is None
    )
