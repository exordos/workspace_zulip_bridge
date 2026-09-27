# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Lossless message-text conversion owned by the v3 bridge.

The visible projection and the original text are deliberately separate.  A
``ConvertedMessage`` keeps the source bytes out of band so an unchanged value
can make the inverse trip byte for byte.  Production persistence stores the
same pair in separate columns; no bridge metadata is embedded in Markdown.
"""

from __future__ import annotations

import dataclasses
import enum
import hashlib
import re
from collections.abc import Callable
from collections.abc import Mapping
from urllib.parse import parse_qs
from urllib.parse import unquote
from urllib.parse import urljoin
from urllib.parse import urlsplit
from uuid import UUID

from workspace_zulip_bridge.stable_ids import stable_chat_uuid
from workspace_zulip_bridge.stable_ids import stable_file_uuid
from workspace_zulip_bridge.stable_ids import stable_message_uuid
from workspace_zulip_bridge.stable_ids import stable_topic_uuid

CONVERTER_VERSION = 1
WORKSPACE_MARKDOWN_LIMIT = 40_000

_MENTION = re.compile(
    r"@_?\*\*(?:(?P<name>[^*|]+)\|(?P<id>[0-9]+)|"
    r"\|(?P<id_only>[0-9]+)|(?P<name_only>[^*]+))\*\*"
)
_NATIVE_LINK = re.compile(r"#\*\*(?P<reference>[^*]+)\*\*")
_TEXT_URL = re.compile(
    r"<(?P<autolink>https?://[^>\s]+)>|"
    r"(?<![\w:/])(?P<bare>https?://[^\s<>]+|www\.[^\s<>]+)"
)
_FENCE = re.compile(
    r"(?ms)^(?P<indent>[ \t]*)(?P<marker>`{3,}|~{3,})[^\r\n]*\r?\n.*?"
    r"^(?P=indent)(?P=marker)[ \t]*(?:\r?\n|$)"
)
_SEMANTIC_QUOTE = re.compile(
    r"@_?\*\*(?P<name>[^*|]+)(?:\|(?P<user_id>[0-9]+))?\*\*\s+"
    r"\[[^\]]+\]\((?P<link>[^)]+)\):\r?\n"
    r"```quote\r?\n(?P<quoted>.*?)\r?\n```(?:\r?\n){0,2}",
    re.DOTALL,
)
_QUOTE_FENCE = re.compile(r"```quote\r?\n(?P<body>.*?)\r?\n```", re.DOTALL)
_URN = re.compile(
    r"^urn:(?P<kind>user|stream|topic|message|file|image|video|quote):"
    r"(?P<uuid>[0-9a-fA-F-]{36})(?P<query>\?.*)?$"
)
_WORKSPACE_URN_REFERENCE = re.compile(
    r"urn:(?P<kind>user|stream|topic|message|file|image|video|quote):"
    r"(?P<uuid>[0-9a-fA-F-]{36})"
)


class MessageFormat(enum.StrEnum):
    ZULIP = "zulip"
    WORKSPACE = "workspace"


@dataclasses.dataclass(frozen=True, slots=True)
class _Source:
    message_format: MessageFormat
    content_utf8: bytes
    projection_sha256: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class ConvertedMessage:
    message_format: MessageFormat
    content: str
    _source: _Source | None = dataclasses.field(
        default=None,
        repr=False,
        compare=False,
    )

    @property
    def content_utf8(self) -> bytes:
        return self.content.encode("utf-8")


@dataclasses.dataclass(frozen=True, slots=True)
class ZulipToWorkspaceContext:
    endpoint: str
    own_user_id: int
    user_uuids: Mapping[int, UUID]
    stream_ids_by_name: Mapping[str, int]
    message_uuids: Mapping[int, UUID] = dataclasses.field(default_factory=dict)
    message_contents: Mapping[int, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True, slots=True)
class WorkspaceUserReference:
    zulip_user_id: int
    full_name: str


@dataclasses.dataclass(frozen=True, slots=True)
class WorkspaceStreamReference:
    chat_key: str
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class WorkspaceTopicReference:
    stream_uuid: UUID
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class WorkspaceMessageReference:
    zulip_message_id: int
    sender_name: str
    content: str
    stream_uuid: UUID
    topic_name: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class WorkspaceToZulipContext:
    endpoint: str
    users: Mapping[UUID, WorkspaceUserReference] = dataclasses.field(
        default_factory=dict
    )
    streams: Mapping[UUID, WorkspaceStreamReference] = dataclasses.field(
        default_factory=dict
    )
    topics: Mapping[UUID, WorkspaceTopicReference] = dataclasses.field(
        default_factory=dict
    )
    messages: Mapping[UUID, WorkspaceMessageReference] = dataclasses.field(
        default_factory=dict
    )
    files: Mapping[UUID, str] = dataclasses.field(default_factory=dict)


MessageInput = str | ConvertedMessage


def workspace_reference_uuids(content: str) -> dict[str, frozenset[UUID]]:
    """Return entity UUIDs needed to render a Workspace message for Zulip."""

    references: dict[str, set[UUID]] = {}
    for match in _WORKSPACE_URN_REFERENCE.finditer(content):
        try:
            entity_uuid = UUID(match.group("uuid"))
        except ValueError:
            continue
        references.setdefault(match.group("kind"), set()).add(entity_uuid)
    return {kind: frozenset(entity_uuids) for kind, entity_uuids in references.items()}


def _sha256(content: str) -> bytes:
    return hashlib.sha256(content.encode("utf-8")).digest()


def _coerce(message: MessageInput, expected: MessageFormat) -> ConvertedMessage:
    if isinstance(message, str):
        return ConvertedMessage(expected, message)
    if not isinstance(message, ConvertedMessage):
        raise TypeError("message must be str or ConvertedMessage")
    if message.message_format is not expected:
        raise ValueError(
            f"expected {expected.value} message, got {message.message_format.value}"
        )
    return message


def _convert(
    message: MessageInput,
    *,
    source_format: MessageFormat,
    target_format: MessageFormat,
    render: Callable[[str], str],
) -> ConvertedMessage:
    source = _coerce(message, source_format)
    preserved = source._source
    if (
        preserved is not None
        and preserved.message_format is target_format
        and preserved.projection_sha256 == _sha256(source.content)
    ):
        return ConvertedMessage(
            target_format,
            preserved.content_utf8.decode("utf-8", errors="strict"),
        )
    rendered = render(source.content)
    if not isinstance(rendered, str):
        raise TypeError("message renderer must return str")
    return ConvertedMessage(
        target_format,
        rendered,
        _Source(source_format, source.content_utf8, _sha256(rendered)),
    )


def zulip_to_workspace(
    message: MessageInput,
    *,
    context: ZulipToWorkspaceContext,
) -> ConvertedMessage:
    """Project Zulip Markdown into Workspace Markdown.

    The returned object keeps the exact Zulip UTF-8 source for an unchanged
    inverse conversion.  Callers persist ``content`` and the source separately.
    """

    return _convert(
        message,
        source_format=MessageFormat.ZULIP,
        target_format=MessageFormat.WORKSPACE,
        render=lambda content: _render_zulip(content, context),
    )


def workspace_to_zulip(
    message: MessageInput,
    *,
    context: WorkspaceToZulipContext,
) -> ConvertedMessage:
    """Project Workspace Markdown into Zulip Markdown."""

    return _convert(
        message,
        source_format=MessageFormat.WORKSPACE,
        target_format=MessageFormat.ZULIP,
        render=lambda content: _render_workspace(content, context),
    )


def _split_protected(content: str) -> list[tuple[bool, str]]:
    """Split fenced and inline code from text without normalizing bytes."""

    result: list[tuple[bool, str]] = []
    cursor = 0
    for fence in _FENCE.finditer(content):
        if fence.start() > cursor:
            result.extend(_split_inline_code(content[cursor : fence.start()]))
        result.append((True, fence.group(0)))
        cursor = fence.end()
    if cursor < len(content):
        result.extend(_split_inline_code(content[cursor:]))
    if not result:
        result.append((False, ""))
    return result


def _split_inline_code(content: str) -> list[tuple[bool, str]]:
    result: list[tuple[bool, str]] = []
    cursor = 0
    while cursor < len(content):
        opening = content.find("`", cursor)
        if opening < 0:
            result.append((False, content[cursor:]))
            break
        marker_end = opening
        while marker_end < len(content) and content[marker_end] == "`":
            marker_end += 1
        marker = content[opening:marker_end]
        closing = content.find(marker, marker_end)
        if closing < 0:
            result.append((False, content[cursor:]))
            break
        if opening > cursor:
            result.append((False, content[cursor:opening]))
        result.append((True, content[opening : closing + len(marker)]))
        cursor = closing + len(marker)
    if not content:
        result.append((False, ""))
    return result


def _find_closing(value: str, start: int, opening: str, closing: str) -> int | None:
    depth = 1
    escaped = False
    for index in range(start, len(value)):
        character = value[index]
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if character == opening:
            depth += 1
        elif character == closing:
            depth -= 1
            if depth == 0:
                return index
    return None


def _link_parts(raw: str) -> tuple[str, str]:
    stripped = raw.lstrip()
    leading = raw[: len(raw) - len(stripped)]
    if stripped.startswith("<"):
        end = stripped.find(">")
        if end >= 0:
            return stripped[1:end], leading + stripped[end + 1 :]
    depth = 0
    escaped = False
    for index, character in enumerate(stripped):
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if character == "(":
            depth += 1
        elif character == ")" and depth:
            depth -= 1
        elif character.isspace() and depth == 0:
            return stripped[:index], leading + stripped[index:]
    return stripped, leading


LinkTransform = Callable[[str, str, bool, str], str]
TextTransform = Callable[[str], str]


def _rewrite_inline(
    content: str,
    *,
    text_transform: TextTransform,
    link_transform: LinkTransform,
) -> str:
    output: list[str] = []
    text_start = 0
    cursor = 0
    while cursor < len(content):
        image = content.startswith("![", cursor)
        if not image and content[cursor] != "[":
            cursor += 1
            continue
        label_open = cursor + 1 if image else cursor
        label_close = _find_closing(content, label_open + 1, "[", "]")
        if label_close is None or label_close + 1 >= len(content):
            cursor += 1
            continue
        if content[label_close + 1] != "(":
            cursor += 1
            continue
        destination_close = _find_closing(content, label_close + 2, "(", ")")
        if destination_close is None:
            cursor += 1
            continue
        output.append(text_transform(content[text_start:cursor]))
        label = content[label_open + 1 : label_close]
        destination, suffix = _link_parts(content[label_close + 2 : destination_close])
        rewritten_label = _rewrite_inline(
            label,
            text_transform=text_transform,
            link_transform=link_transform,
        )
        output.append(link_transform(rewritten_label, destination, image, suffix))
        cursor = destination_close + 1
        text_start = cursor
    output.append(text_transform(content[text_start:]))
    return "".join(output)


def _message_uuid(context: ZulipToWorkspaceContext, message_id: int) -> UUID:
    return context.message_uuids.get(
        message_id,
        stable_message_uuid(context.endpoint, message_id),
    )


def _channel_id(context: ZulipToWorkspaceContext, name_or_slug: str) -> int | None:
    first = name_or_slug.split("-", 1)[0]
    if first.isdigit():
        return int(first)
    decoded = unquote(name_or_slug.replace(".", "%"))
    for name, stream_id in context.stream_ids_by_name.items():
        if name.casefold() == decoded.casefold():
            return stream_id
    return None


def _provider_urn(
    target: str,
    context: ZulipToWorkspaceContext,
) -> str | None:
    parsed = urlsplit(target)
    endpoint = urlsplit(context.endpoint)
    if parsed.scheme and (
        parsed.scheme.casefold() != endpoint.scheme.casefold()
        or parsed.netloc.casefold() != endpoint.netloc.casefold()
    ):
        return None
    fragment = parsed.fragment
    if fragment.startswith("user/"):
        user_id = fragment.removeprefix("user/").split("/", 1)[0]
        user_uuid = context.user_uuids.get(int(user_id)) if user_id.isdigit() else None
        return None if user_uuid is None else f"urn:user:{user_uuid}"
    if not fragment.startswith("narrow/"):
        return None
    parts = fragment.split("/")
    terms: dict[str, str] = {}
    for index in range(1, len(parts) - 1, 2):
        key = unquote(parts[index].replace(".", "%")).casefold().removeprefix("-")
        key = {"stream": "channel", "pm": "dm", "pm-with": "dm"}.get(key, key)
        terms[key] = parts[index + 1]
    near = terms.get("near")
    if near and near.isdigit():
        return f"urn:message:{_message_uuid(context, int(near))}"
    channel = terms.get("channel")
    if channel is not None:
        stream_id = _channel_id(context, channel)
        if stream_id is None:
            return None
        stream_uuid = stable_chat_uuid(context.endpoint, f"channel:{stream_id}")
        topic = terms.get("topic")
        if topic is None:
            return f"urn:stream:{stream_uuid}"
        topic_name = unquote(topic.replace(".", "%"))
        return f"urn:topic:{stable_topic_uuid(stream_uuid, topic_name)}"
    direct = terms.get("dm")
    if direct is not None:
        raw_ids = direct.split("-", 1)[0].split(",")
        if raw_ids and all(value.isdigit() for value in raw_ids):
            participants = {context.own_user_id, *(int(value) for value in raw_ids)}
            chat_key = "direct:" + ",".join(map(str, sorted(participants)))
            return f"urn:stream:{stable_chat_uuid(context.endpoint, chat_key)}"
    return None


def _render_zulip(content: str, context: ZulipToWorkspaceContext) -> str:
    lossy = False

    def semantic_quote(match: re.Match[str]) -> str:
        nonlocal lossy
        parsed = urlsplit(match.group("link"))
        message_id = None
        parts = parsed.fragment.split("/")
        for index, value in enumerate(parts[:-1]):
            if value == "near" and parts[index + 1].isdigit():
                message_id = int(parts[index + 1])
                break
        if message_id is None:
            lossy = True
            return match.group(0)
        quoted = match.group("quoted")
        user_id = match.group("user_id")
        user_uuid = context.user_uuids.get(int(user_id)) if user_id else None
        author = match.group("name")
        if user_uuid is not None:
            author = f"[{author}](urn:user:{user_uuid})"
        source = f"[said](urn:url:{match.group('link')})"
        body = "\n".join(f"> {line}" for line in quoted.splitlines())
        return f"{author} {source}:\n{body}\n\n"

    content = _SEMANTIC_QUOTE.sub(semantic_quote, content)

    def quote_fence(match: re.Match[str]) -> str:
        return "\n".join(f"> {line}" for line in match.group("body").splitlines())

    content = _QUOTE_FENCE.sub(quote_fence, content)

    def text_transform(value: str) -> str:
        nonlocal lossy

        def mention(match: re.Match[str]) -> str:
            nonlocal lossy
            raw_id = match.group("id") or match.group("id_only")
            name = match.group("name") or match.group("name_only") or "User"
            user_uuid = context.user_uuids.get(int(raw_id)) if raw_id else None
            if user_uuid is None:
                lossy = True
                return f"@{name}"
            return f"[{name}](urn:user:{user_uuid})"

        def native(match: re.Match[str]) -> str:
            nonlocal lossy
            reference = match.group("reference")
            channel_name, separator, topic_reference = reference.partition(">")
            stream_id = _channel_id(context, channel_name)
            if stream_id is None:
                lossy = True
                return match.group(0)
            stream_uuid = stable_chat_uuid(context.endpoint, f"channel:{stream_id}")
            if not separator:
                return f"[#{channel_name}](urn:stream:{stream_uuid})"
            topic_name, message_separator, raw_message_id = topic_reference.rpartition(
                "@"
            )
            if message_separator and raw_message_id.isdigit():
                message_uuid = _message_uuid(context, int(raw_message_id))
                return (
                    f"[#{channel_name} > {topic_name} @ 💬](urn:message:{message_uuid})"
                )
            topic_uuid = stable_topic_uuid(stream_uuid, topic_reference)
            return f"[#{channel_name} > {topic_reference}](urn:topic:{topic_uuid})"

        value = _MENTION.sub(mention, value)
        value = _NATIVE_LINK.sub(native, value)

        def url(match: re.Match[str]) -> str:
            autolink = match.group("autolink")
            raw = autolink or match.group("bare")
            assert raw is not None
            if autolink is not None:
                return f"[{raw}](urn:url:{raw})"
            suffix = ""
            while raw and raw[-1] in ".,;:!?":
                suffix = raw[-1] + suffix
                raw = raw[:-1]
            while raw.endswith(")") and raw.count("(") < raw.count(")"):
                suffix = ")" + suffix
                raw = raw[:-1]
            target = (
                raw if raw.startswith(("http://", "https://")) else f"https://{raw}"
            )
            return f"[{raw}](urn:url:{target}){suffix}"

        return _TEXT_URL.sub(url, value)

    def link_transform(label: str, target: str, image: bool, suffix: str) -> str:
        nonlocal lossy
        marker = "!" if image else ""
        if target.startswith("urn:"):
            return f"{marker}[{label}]({target}{suffix})"
        parsed = urlsplit(target)
        path = parsed.path
        if path.startswith("/user_uploads/"):
            file_uuid = stable_file_uuid(context.endpoint, path)
            return f"{marker}[{label}](urn:file:{file_uuid}{suffix})"
        provider = _provider_urn(target, context)
        if provider is not None:
            return f"{marker}[{label}]({provider}{suffix})"
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return f"{marker}[{label}](urn:url:{target}{suffix})"
        if target.startswith(("#", "/")):
            absolute = urljoin(context.endpoint.rstrip("/") + "/", target)
            return f"{marker}[{label}](urn:url:{absolute}{suffix})"
        lossy = True
        return f"{marker}[{label}]({target}{suffix})"

    converted = "".join(
        segment
        if protected
        else _rewrite_inline(
            segment,
            text_transform=text_transform,
            link_transform=link_transform,
        )
        for protected, segment in _split_protected(content)
    )
    original_url = context.endpoint.rstrip("/")
    if lossy and original_url:
        converted += f"\n\n[Open original](urn:url:{original_url})"
    if len(converted) > WORKSPACE_MARKDOWN_LIMIT:
        marker = "\n\n[Message truncated]"
        converted = converted[: WORKSPACE_MARKDOWN_LIMIT - len(marker)] + marker
    return converted


def _provider_url(context: WorkspaceToZulipContext, fragment: str) -> str:
    return context.endpoint.rstrip("/") + "/#" + fragment.lstrip("#")


def _render_workspace(content: str, context: WorkspaceToZulipContext) -> str:
    references: dict[str, str] = {}
    lines: list[str] = []
    definition = re.compile(r"^\[([^\]]+)\]:\s*(\S+)\s*$")
    for line in content.splitlines(keepends=True):
        body = line.rstrip("\r\n")
        match = definition.match(body)
        if match is not None:
            references[match.group(1).casefold()] = match.group(2)
            continue
        lines.append(line)
    content = "".join(lines)
    for label, target in references.items():

        def inline_reference(
            match: re.Match[str],
            destination: str = target,
        ) -> str:
            return f"[{match.group(1)}]({destination})"

        content = re.sub(
            rf"\[([^\]]+)\]\[{re.escape(label)}\]",
            inline_reference,
            content,
            flags=re.IGNORECASE,
        )

    def text_transform(value: str) -> str:
        return value

    def link_transform(label: str, target: str, image: bool, suffix: str) -> str:
        marker = "!" if image else ""
        if target.startswith("urn:url:"):
            return f"{marker}[{label}]({target.removeprefix('urn:url:')}{suffix})"
        match = _URN.match(target)
        if match is None:
            return f"{marker}[{label}]({target}{suffix})"
        kind = match.group("kind")
        entity_uuid = UUID(match.group("uuid"))
        if kind == "user":
            user = context.users.get(entity_uuid)
            return (
                f"@{label}"
                if user is None
                else f"@**{user.full_name}|{user.zulip_user_id}**"
            )
        if kind == "stream":
            stream = context.streams.get(entity_uuid)
            if stream is None:
                return label
            if stream.chat_key.startswith("channel:"):
                return f"#**{stream.name}**"
            participant_ids = stream.chat_key.partition(":")[2].split(",")
            return f"[{label}]({_provider_url(context, 'narrow/dm/' + ','.join(participant_ids))})"
        if kind == "topic":
            topic = context.topics.get(entity_uuid)
            if topic is None:
                return label
            stream = context.streams.get(topic.stream_uuid)
            return label if stream is None else f"#**{stream.name}>{topic.name}**"
        if kind == "message":
            message = context.messages.get(entity_uuid)
            if message is None:
                return label
            stream = context.streams.get(message.stream_uuid)
            if stream is None:
                return label
            if stream.chat_key.startswith("channel:") and message.topic_name:
                return f"#**{stream.name}>{message.topic_name}@{message.zulip_message_id}**"
            return f"[{label}]({_provider_url(context, 'narrow/near/' + str(message.zulip_message_id))})"
        if kind in {"file", "image", "video"}:
            source_path = context.files.get(entity_uuid)
            return label if source_path is None else f"{marker}[{label}]({source_path})"
        if kind == "quote":
            message = context.messages.get(entity_uuid)
            if message is None:
                return f"@{label}"
            query = parse_qs((match.group("query") or "").removeprefix("?"))
            selected = query.get("text", [message.content])[0]
            link = _provider_url(
                context,
                "narrow/near/" + str(message.zulip_message_id),
            )
            return (
                f"@_**{message.sender_name}** [said]({link}):\n"
                f"```quote\n{selected}\n```"
            )
        return f"{marker}[{label}]({target}{suffix})"

    return "".join(
        segment
        if protected
        else _rewrite_inline(
            segment,
            text_transform=text_transform,
            link_transform=link_transform,
        )
        for protected, segment in _split_protected(content)
    )
