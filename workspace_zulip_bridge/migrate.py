import argparse
import pathlib

from workspace_zulip_bridge import config, storage


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        default=pathlib.Path("/etc/workspace-zulip-bridge/bridge.conf"),
    )
    parser.add_argument(
        "--migrations",
        type=pathlib.Path,
        default=pathlib.Path("/opt/workspace-zulip-bridge/migrations"),
    )
    arguments = parser.parse_args()
    runtime = config.load(arguments.config)
    storage.PostgresStore(runtime.database.dsn).migrate(arguments.migrations)
