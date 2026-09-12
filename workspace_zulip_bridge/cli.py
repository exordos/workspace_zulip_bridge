# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import asyncio
import logging
import signal

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.service import BridgeService


async def _run(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)

    await BridgeService(settings).run(stop)


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    try:
        import uvloop
    except ImportError:
        asyncio.run(_run(settings))
    else:
        uvloop.run(_run(settings))


if __name__ == "__main__":
    main()
