# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import pytest

from workspace_zulip_bridge.zulip_outbound import _json_object


def test_json_object_decodes_asyncpg_jsonb_text() -> None:
    assert _json_object('{"name":"general"}') == {"name": "general"}


def test_json_object_rejects_non_objects() -> None:
    with pytest.raises(ValueError, match="expected a JSON object"):
        _json_object('["general"]')
