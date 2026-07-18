import argparse
import datetime
import pathlib

from workspace_zulip_bridge import config, storage


def check(runtime: config.RuntimeConfig) -> None:
    timestamp = datetime.datetime.fromisoformat(
        runtime.health_file.read_text(encoding="utf-8")
    )
    age = datetime.datetime.now(datetime.UTC) - timestamp
    if age > datetime.timedelta(seconds=60):
        raise RuntimeError("Bridge worker has not progressed in 60 seconds")
    rows = storage.PostgresStore(runtime.database.dsn).health()
    states = {str(row["component"]): str(row["status"]) for row in rows}
    if states.get("control") != "healthy":
        raise RuntimeError("Bridge control heartbeat is not healthy")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        default=pathlib.Path("/etc/workspace-zulip-bridge/bridge.conf"),
    )
    arguments = parser.parse_args()
    check(config.load(arguments.config))
