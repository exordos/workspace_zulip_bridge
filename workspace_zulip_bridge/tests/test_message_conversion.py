# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

import dataclasses

import pytest

from workspace_zulip_bridge.message_conversion import ConvertedMessage
from workspace_zulip_bridge.message_conversion import MessageFormat
from workspace_zulip_bridge.message_conversion import WorkspaceMessageReference
from workspace_zulip_bridge.message_conversion import WorkspaceStreamReference
from workspace_zulip_bridge.message_conversion import WorkspaceTopicReference
from workspace_zulip_bridge.message_conversion import WorkspaceToZulipContext
from workspace_zulip_bridge.message_conversion import WorkspaceUserReference
from workspace_zulip_bridge.message_conversion import ZulipToWorkspaceContext
from workspace_zulip_bridge.message_conversion import workspace_to_zulip
from workspace_zulip_bridge.message_conversion import zulip_to_workspace
from workspace_zulip_bridge.stable_ids import stable_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_file_uuid
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid

ENDPOINT = "https://zulip.example.invalid"
OWN_USER_ID = 1
OTHER_USER_ID = 2
OTHER_USER_UUID = stable_user_uuid(ENDPOINT, OTHER_USER_ID)
STREAM_UUID = stable_chat_uuid(ENDPOINT, "channel:42")
DIRECT_STREAM_UUID = stable_chat_uuid(ENDPOINT, "direct:1,2")
TOPIC_UUID = stable_topic_uuid(STREAM_UUID, "bridge")
MESSAGE_UUID = stable_message_uuid(ENDPOINT, 99)
FILE_UUID = stable_file_uuid(ENDPOINT, "/user_uploads/a/report.pdf")


def _inbound() -> ZulipToWorkspaceContext:
    return ZulipToWorkspaceContext(
        endpoint=ENDPOINT,
        own_user_id=OWN_USER_ID,
        user_uuids={
            OWN_USER_ID: stable_user_uuid(ENDPOINT, OWN_USER_ID),
            OTHER_USER_ID: OTHER_USER_UUID,
        },
        stream_ids_by_name={"engineering": 42},
        message_contents={99: "original text"},
    )


def _outbound() -> WorkspaceToZulipContext:
    return WorkspaceToZulipContext(
        endpoint=ENDPOINT,
        users={
            OTHER_USER_UUID: WorkspaceUserReference(
                zulip_user_id=OTHER_USER_ID,
                full_name="Other User",
            )
        },
        streams={
            STREAM_UUID: WorkspaceStreamReference(
                chat_key="channel:42",
                name="engineering",
            ),
            DIRECT_STREAM_UUID: WorkspaceStreamReference(
                chat_key="direct:1,2",
                name="Direct message",
            ),
        },
        topics={
            TOPIC_UUID: WorkspaceTopicReference(
                stream_uuid=STREAM_UUID,
                name="bridge",
            )
        },
        messages={
            MESSAGE_UUID: WorkspaceMessageReference(
                zulip_message_id=99,
                sender_name="Other User",
                content="original text",
                stream_uuid=STREAM_UUID,
                topic_name="bridge",
            )
        },
        files={FILE_UUID: "/user_uploads/a/report.pdf"},
    )


@pytest.mark.parametrize(
    "content",
    (
        "",
        "plain ASCII",
        "кириллица 👩🏽‍💻 e\u0301 é",
        "line one\nline two\n",
        "line one\r\nline two\r\n",
        "\x00 valid JSON text after escaping",
        "<!-- possible sidecar-looking text -->",
        "`[literal](urn:user:00000000-0000-4000-8000-000000000000)`",
        "````markdown\n```quote\nliteral\n```\n````",
        "x" * 50_000,
    ),
)
def test_unchanged_projection_round_trips_exact_utf8(content: str) -> None:
    workspace = zulip_to_workspace(content, context=_inbound())
    restored_zulip = workspace_to_zulip(workspace, context=_outbound())
    assert restored_zulip.content_utf8 == content.encode("utf-8")

    zulip = workspace_to_zulip(content, context=_outbound())
    restored_workspace = zulip_to_workspace(zulip, context=_inbound())
    assert restored_workspace.content_utf8 == content.encode("utf-8")


@pytest.mark.parametrize(
    ("source", "expected"),
    (
        ("plain\r\ntext", "plain\r\ntext"),
        ("@**Other User|2**", f"[Other User](urn:user:{OTHER_USER_UUID})"),
        (
            '[docs](https://example.invalid/a?x=1#part "Docs")',
            '[docs](urn:url:https://example.invalid/a?x=1#part "Docs")',
        ),
        (
            "https://example.invalid/a_(b).",
            "[https://example.invalid/a_(b)](urn:url:https://example.invalid/a_(b)).",
        ),
        ("#**engineering**", f"[#engineering](urn:stream:{STREAM_UUID})"),
        (
            "#**engineering>bridge**",
            f"[#engineering > bridge](urn:topic:{TOPIC_UUID})",
        ),
        (
            "#**engineering>bridge@99**",
            f"[#engineering > bridge @ 💬](urn:message:{MESSAGE_UUID})",
        ),
        (
            "[dm](https://zulip.example.invalid/#narrow/dm/2-user)",
            f"[dm](urn:stream:{DIRECT_STREAM_UUID})",
        ),
        (
            "[report.pdf](/user_uploads/a/report.pdf)",
            f"[report.pdf](urn:file:{FILE_UUID})",
        ),
        (
            "@_**Other User|2** "
            "[said](https://zulip.example.invalid/#narrow/near/99):\n"
            "```quote\noriginal text\n```\n\nreply",
            f"[Other User](urn:user:{OTHER_USER_UUID}) "
            "[said](urn:url:https://zulip.example.invalid/#narrow/near/99):\n"
            "> original text\n\nreply",
        ),
        ("```quote\nordinary quote\n```", "> ordinary quote"),
        (
            "`@**Other User|2** https://example.invalid`\n"
            "```markdown\n#**engineering>bridge**\n```",
            "`@**Other User|2** https://example.invalid`\n"
            "```markdown\n#**engineering>bridge**\n```",
        ),
        (
            "![diagram](https://example.invalid/diagram.png)",
            "![diagram](urn:url:https://example.invalid/diagram.png)",
        ),
        (
            "<https://example.invalid/autolink>",
            "[https://example.invalid/autolink]"
            "(urn:url:https://example.invalid/autolink)",
        ),
    ),
)
def test_zulip_differences_are_projected_in_bridge(
    source: str,
    expected: str,
) -> None:
    converted = zulip_to_workspace(source, context=_inbound())
    assert converted.content == expected
    assert workspace_to_zulip(converted, context=_outbound()).content == source


@pytest.mark.parametrize(
    ("source", "expected"),
    (
        ("plain\r\ntext", "plain\r\ntext"),
        (f"[Alias](urn:user:{OTHER_USER_UUID})", "@**Other User|2**"),
        (f"[channel](urn:stream:{STREAM_UUID})", "#**engineering**"),
        (f"[topic](urn:topic:{TOPIC_UUID})", "#**engineering>bridge**"),
        (
            f"[message](urn:message:{MESSAGE_UUID})",
            "#**engineering>bridge@99**",
        ),
        (
            f"[dm](urn:stream:{DIRECT_STREAM_UUID})",
            "[dm](https://zulip.example.invalid/#narrow/dm/1,2)",
        ),
        (
            "[site](urn:url:https://example.invalid/a?x=1#part)",
            "[site](https://example.invalid/a?x=1#part)",
        ),
        (
            f"[report.pdf](urn:file:{FILE_UUID})",
            "[report.pdf](/user_uploads/a/report.pdf)",
        ),
        (
            f"![report](urn:image:{FILE_UUID})",
            "![report](/user_uploads/a/report.pdf)",
        ),
        (
            f"[Other User](urn:quote:{MESSAGE_UUID})\n\nreply",
            "@_**Other User** "
            "[said](https://zulip.example.invalid/#narrow/near/99):\n"
            "```quote\noriginal text\n```\n\nreply",
        ),
        (
            f"`[Alias](urn:user:{OTHER_USER_UUID})`\n"
            f"```markdown\n[topic](urn:topic:{TOPIC_UUID})\n```",
            f"`[Alias](urn:user:{OTHER_USER_UUID})`\n"
            f"```markdown\n[topic](urn:topic:{TOPIC_UUID})\n```",
        ),
        (
            "[docs][reference]\n\n"
            "[reference]: urn:url:https://example.invalid/reference",
            "[docs](https://example.invalid/reference)\n\n",
        ),
    ),
)
def test_workspace_differences_are_projected_in_bridge(
    source: str,
    expected: str,
) -> None:
    converted = workspace_to_zulip(source, context=_outbound())
    assert converted.content == expected
    assert zulip_to_workspace(converted, context=_inbound()).content == source


def test_edit_invalidates_the_old_source_snapshot() -> None:
    projected = zulip_to_workspace(
        "original Zulip bytes\r\n",
        context=_inbound(),
    )
    edited = dataclasses.replace(
        projected,
        content="Workspace edit\r\nwith exact line endings\r\n",
    )
    zulip = workspace_to_zulip(edited, context=_outbound())
    assert zulip.content == edited.content
    assert zulip_to_workspace(zulip, context=_inbound()).content_utf8 == (
        edited.content_utf8
    )


def test_direction_is_checked() -> None:
    workspace = ConvertedMessage(MessageFormat.WORKSPACE, "content")
    with pytest.raises(ValueError, match="expected zulip message"):
        zulip_to_workspace(workspace, context=_inbound())
