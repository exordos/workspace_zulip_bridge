from workspace_zulip_bridge import cli


def test_logging_keeps_http_request_urls_above_info(monkeypatch):
    basic_config = []
    levels = {}
    original_get_logger = cli.logging.getLogger

    class Logger:
        def __init__(self, name):
            self.name = name

        def setLevel(self, level):
            levels[self.name] = level

    monkeypatch.setattr(
        cli.logging,
        "basicConfig",
        lambda **kwargs: basic_config.append(kwargs),
    )
    def get_logger(name=None):
        if name in {"workspace_zulip_bridge", "httpx", "httpcore"}:
            return Logger(name)
        return original_get_logger(name)

    monkeypatch.setattr(cli.logging, "getLogger", get_logger)

    cli.configure_logging()

    assert basic_config == [
        {
            "level": cli.logging.WARNING,
            "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
        }
    ]
    assert levels == {
        "workspace_zulip_bridge": cli.logging.INFO,
        "httpx": cli.logging.WARNING,
        "httpcore": cli.logging.WARNING,
    }
