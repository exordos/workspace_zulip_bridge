"""Source-preserving CommonMark conversion for bridge message content."""

import dataclasses
import typing

from markdown_it import MarkdownIt, rules_inline
from markdown_it.token import Token


@dataclasses.dataclass(frozen=True)
class MarkdownLink:
    raw: str
    image: bool
    label: str
    destination: str
    destination_prefix: str
    destination_suffix: str
    autolink: bool = False
    reference: bool = False

    def with_destination(self, destination: str) -> str:
        if self.reference:
            marker = "!" if self.image else ""
            return (
                f"{marker}[{self.label}]"
                f"({destination}{self.destination_suffix})"
            )
        if self.autolink and destination.casefold().startswith("urn:"):
            return f"[{self.label}]({destination})"
        return f"{self.destination_prefix}{destination}{self.destination_suffix}"


TextTransform = typing.Callable[[str], str]
LinkTransform = typing.Callable[[MarkdownLink], str]
StandaloneLinkTransform = typing.Callable[[MarkdownLink], str | None]
SemanticQuoteTransform = typing.Callable[[MarkdownLink, str], str | None]


@dataclasses.dataclass(frozen=True)
class _Source:
    content: str
    lines: tuple[str, ...]
    offsets: tuple[int, ...]

    @classmethod
    def from_content(cls, content: str) -> "_Source":
        lines = tuple(content.splitlines(keepends=True))
        if content and not lines:
            lines = (content,)
        offsets: list[int] = []
        offset = 0
        for line in lines:
            offsets.append(offset)
            offset += len(line)
        return cls(content, lines, tuple(offsets))

    def line_range(self, line_map: list[int]) -> tuple[int, int]:
        start_line, end_line = line_map
        start = (
            self.offsets[start_line]
            if start_line < len(self.offsets)
            else len(self.content)
        )
        end = (
            self.offsets[end_line]
            if end_line < len(self.offsets)
            else len(self.content)
        )
        return start, end


@dataclasses.dataclass(frozen=True)
class _Edit:
    start: int
    end: int
    replacement: str | None
    priority: int


def _split_line_ending(line: str) -> tuple[str, str]:
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith(("\n", "\r")):
        return line[:-1], line[-1:]
    return line, ""


def _inline_link_details(
    state: rules_inline.StateInline,
    start: int,
    *,
    image: bool,
) -> dict[str, object]:
    opening = start + 1 if image else start
    label_start = opening + 1
    label_end = state.md.helpers.parseLinkLabel(
        state,
        opening,
        not image,
    )
    details: dict[str, object] = {
        "source_start": start,
        "source_end": state.pos,
        "label_start": label_start,
        "label_end": label_end,
        "image": image,
        "reference": True,
    }
    if label_end < 0:
        return details

    position = label_end + 1
    if position >= state.posMax or state.src[position] != "(":
        return details

    position += 1
    while position < state.posMax and state.src[position] in " \t\r\n":
        position += 1
    destination_start = position
    parsed = state.md.helpers.parseLinkDestination(
        state.src,
        position,
        state.posMax,
    )
    if not parsed.ok:
        return details

    destination_end = parsed.pos
    if (
        destination_start < destination_end
        and state.src[destination_start] == "<"
    ):
        destination_start += 1
        destination_end -= 1
    details.update(
        {
            "destination_start": destination_start,
            "destination_end": destination_end,
            "reference": False,
        }
    )
    return details


def _annotate_inline_rule(
    markdown: MarkdownIt,
    name: str,
    original: typing.Callable[[rules_inline.StateInline, bool], bool],
    token_type: str,
) -> None:
    def annotated(state: rules_inline.StateInline, silent: bool) -> bool:
        start = state.pos
        token_count = len(state.tokens)
        matched = original(state, silent)
        if not matched or silent:
            return matched

        candidates = [
            token
            for token in state.tokens[token_count:]
            if token.type == token_type
        ]
        if not candidates:
            return matched
        token = candidates[0]
        if name in {"link", "image"}:
            token.meta.update(
                _inline_link_details(state, start, image=name == "image")
            )
        else:
            token.meta.update(
                {
                    "source_start": start,
                    "source_end": state.pos,
                }
            )
            if name == "autolink":
                token.meta.update(
                    {
                        "destination_start": start + 1,
                        "destination_end": state.pos - 1,
                        "label_start": start + 1,
                        "label_end": state.pos - 1,
                        "image": False,
                        "autolink": True,
                        "reference": False,
                    }
                )
        return matched

    markdown.inline.ruler.at(name, annotated)


def _commonmark() -> MarkdownIt:
    markdown = MarkdownIt("commonmark", {"store_labels": True})
    for name, original, token_type in (
        ("backticks", rules_inline.backtick, "code_inline"),
        ("link", rules_inline.link, "link_open"),
        ("image", rules_inline.image, "image"),
        ("autolink", rules_inline.autolink, "link_open"),
        ("html_inline", rules_inline.html_inline, "html_inline"),
    ):
        _annotate_inline_rule(markdown, name, original, token_type)
    return markdown


_COMMONMARK = _commonmark()


def _parse(content: str) -> tuple[list[Token], dict[str, object], _Source]:
    environment: dict[str, object] = {}
    tokens = _COMMONMARK.parse(content, environment)
    return tokens, environment, _Source.from_content(content)


def _inline_source_boundaries(
    inline: Token,
    source: _Source,
) -> list[int | None]:
    """Map normalized inline-content boundaries back to original source."""
    boundaries: list[int | None] = [None] * (len(inline.content) + 1)
    if inline.map is None:
        return boundaries

    source_line = inline.map[0]
    end_line = min(inline.map[1], len(source.lines))
    content_position = 0
    content_lines = inline.content.split("\n")
    for part_index, content_line in enumerate(content_lines):
        if part_index == len(content_lines) - 1 and not content_line:
            boundaries[content_position] = (
                boundaries[content_position]
                if boundaries[content_position] is not None
                else (
                    source.offsets[source_line]
                    if source_line < len(source.offsets)
                    else len(source.content)
                )
            )
            break

        match_position = -1
        while source_line < end_line:
            source_body, _ending = _split_line_ending(source.lines[source_line])
            match_position = source_body.rfind(content_line)
            if match_position >= 0:
                break
            source_line += 1
        if source_line >= end_line or match_position < 0:
            return boundaries

        absolute = source.offsets[source_line] + match_position
        for index in range(len(content_line) + 1):
            boundaries[content_position + index] = absolute + index
        content_position += len(content_line)

        if part_index < len(content_lines) - 1:
            content_position += 1
            source_line += 1

    return boundaries


def _absolute_span(
    boundaries: list[int | None],
    start: object,
    end: object,
) -> tuple[int, int] | None:
    if not isinstance(start, int) or not isinstance(end, int):
        return None
    if start < 0 or end < start or end >= len(boundaries):
        return None
    absolute_start = boundaries[start]
    absolute_end = boundaries[end]
    if absolute_start is None or absolute_end is None:
        return None
    return absolute_start, absolute_end


def _link_from_token(
    token: Token,
    boundaries: list[int | None],
    source: _Source,
) -> MarkdownLink | None:
    source_span = _absolute_span(
        boundaries,
        token.meta.get("source_start"),
        token.meta.get("source_end"),
    )
    label_span = _absolute_span(
        boundaries,
        token.meta.get("label_start"),
        token.meta.get("label_end"),
    )
    if source_span is None or label_span is None:
        return None

    destination = token.attrGet("src" if token.type == "image" else "href")
    if destination is None:
        return None
    raw = source.content[source_span[0] : source_span[1]]
    label = source.content[label_span[0] : label_span[1]]
    reference = bool(token.meta.get("reference"))
    autolink = bool(token.meta.get("autolink"))

    destination_span = _absolute_span(
        boundaries,
        token.meta.get("destination_start"),
        token.meta.get("destination_end"),
    )
    if destination_span is not None:
        destination_prefix = source.content[source_span[0] : destination_span[0]]
        destination_suffix = source.content[destination_span[1] : source_span[1]]
    elif reference:
        title = token.attrGet("title")
        destination_prefix = f"{'!' if token.type == 'image' else ''}[{label}]("
        destination_suffix = f' "{title}"' if title else ""
    else:
        return None

    return MarkdownLink(
        raw=raw,
        image=token.type == "image",
        label=label,
        destination=destination,
        destination_prefix=destination_prefix,
        destination_suffix=destination_suffix,
        autolink=autolink,
        reference=reference,
    )


def _inline_links(
    inline: Token,
    source: _Source,
) -> list[tuple[Token, MarkdownLink, tuple[int, int]]]:
    boundaries = _inline_source_boundaries(inline, source)
    links: list[tuple[Token, MarkdownLink, tuple[int, int]]] = []
    for child in inline.children or ():
        if child.type not in {"link_open", "image"}:
            continue
        link = _link_from_token(child, boundaries, source)
        source_span = _absolute_span(
            boundaries,
            child.meta.get("source_start"),
            child.meta.get("source_end"),
        )
        if link is not None and source_span is not None:
            links.append((child, link, source_span))
    return links


def _standalone_link(
    inline: Token,
    links: list[tuple[Token, MarkdownLink, tuple[int, int]]],
) -> tuple[MarkdownLink, tuple[int, int]] | None:
    if len(links) != 1 or links[0][1].image:
        return None
    children = inline.children or ()
    link_token = links[0][0]
    try:
        opening = children.index(link_token)
    except ValueError:
        return None

    level = 1
    closing = opening + 1
    while closing < len(children) and level:
        if children[closing].type == "link_open":
            level += 1
        elif children[closing].type == "link_close":
            level -= 1
        closing += 1
    if level:
        return None

    outside = children[:opening] + children[closing:]
    if any(token.type != "text" or token.content.strip() for token in outside):
        return None
    return links[0][1], links[0][2]


def _quote_fence(token: Token) -> bool:
    return (
        token.type == "fence"
        and token.map is not None
        and token.map[1] - token.map[0] == len(token.content.splitlines()) + 2
        and token.info.strip().casefold() == "quote"
    )


def _preferred_line_ending(lines: tuple[str, ...]) -> str:
    endings = [_split_line_ending(line)[1] for line in lines]
    endings = [ending for ending in endings if ending]
    if endings and all(ending == "\r\n" for ending in endings):
        return "\r\n"
    return "\n"


def _without_final_line_ending(content: str) -> str:
    if content.endswith("\r\n"):
        return content[:-2]
    if content.endswith(("\n", "\r")):
        return content[:-1]
    return content


def _blockquote(content: str, prefix: str) -> str:
    if not content:
        return f"{prefix}>"
    return "".join(f"{prefix}> {line}" for line in content.splitlines(keepends=True))


def _quote_replacement(
    token: Token,
    source: _Source,
    *,
    text_transform: TextTransform,
    link_transform: LinkTransform,
    standalone_link_transform: StandaloneLinkTransform | None,
) -> str:
    assert token.map is not None
    start_line, end_line = token.map
    opening_body, _opening_ending = _split_line_ending(source.lines[start_line])
    marker_position = opening_body.find(token.markup)
    prefix = opening_body[:marker_position] if marker_position >= 0 else ""

    quote_lines = source.lines[start_line:end_line]
    line_ending = _preferred_line_ending(quote_lines)
    closing_ending = _split_line_ending(quote_lines[-1])[1]
    body = transform_markdown(
        _without_final_line_ending(token.content),
        text_transform=text_transform,
        link_transform=link_transform,
        standalone_link_transform=standalone_link_transform,
        convert_semantic_quotes=True,
    )
    if line_ending != "\n":
        body = body.replace("\n", line_ending)
    return _blockquote(body, prefix) + closing_ending


def _select_edits(edits: list[_Edit]) -> list[_Edit]:
    selected: list[_Edit] = []
    for edit in sorted(
        edits,
        key=lambda candidate: (
            candidate.start,
            -candidate.priority,
            -(candidate.end - candidate.start),
        ),
    ):
        if edit.end < edit.start:
            continue
        if selected and edit.start < selected[-1].end:
            continue
        selected.append(edit)
    return selected


def semantic_quote_links(content: str) -> tuple[MarkdownLink, ...]:
    """Return links that introduce real CommonMark ``quote`` fences."""
    tokens, _environment, source = _parse(content)
    links: list[MarkdownLink] = []
    for fence_index, fence in enumerate(tokens):
        if not _quote_fence(fence) or fence.map is None:
            continue
        for candidate in reversed(tokens[:fence_index]):
            if candidate.type != "inline" or candidate.map is None:
                continue
            if candidate.map[1] != fence.map[0]:
                break
            if candidate.level != fence.level + 1:
                continue
            candidate_links = _inline_links(candidate, source)
            for _token, link, span in reversed(candidate_links):
                if link.image:
                    continue
                fence_start, _fence_end = source.line_range(fence.map)
                if source.content[span[1] : fence_start].strip() == ":":
                    links.append(link)
                    break
            break
    return tuple(links)


def transform_semantic_quotes(
    content: str,
    transform: SemanticQuoteTransform,
) -> str:
    """Replace complete Zulip reply quote blocks while preserving other source."""
    tokens, _environment, source = _parse(content)
    edits: list[_Edit] = []
    for fence_index, fence in enumerate(tokens):
        if not _quote_fence(fence) or fence.map is None:
            continue
        for candidate in reversed(tokens[:fence_index]):
            if candidate.type != "inline" or candidate.map is None:
                continue
            if candidate.map[1] != fence.map[0]:
                break
            if candidate.level != fence.level + 1:
                continue
            candidate_links = _inline_links(candidate, source)
            for _token, link, span in reversed(candidate_links):
                if link.image:
                    continue
                fence_start, fence_end = source.line_range(fence.map)
                if source.content[span[1] : fence_start].strip() != ":":
                    continue
                replacement = transform(
                    link,
                    _without_final_line_ending(fence.content),
                )
                if replacement is not None:
                    header_start, _header_end = source.line_range(candidate.map)
                    closing_ending = _split_line_ending(
                        source.lines[fence.map[1] - 1]
                    )[1]
                    edits.append(
                        _Edit(
                            header_start,
                            fence_end,
                            replacement + closing_ending,
                            120,
                        )
                    )
                break
            break

    transformed: list[str] = []
    cursor = 0
    for edit in _select_edits(edits):
        transformed.append(content[cursor : edit.start])
        transformed.append(
            content[edit.start : edit.end]
            if edit.replacement is None
            else edit.replacement
        )
        cursor = edit.end
    transformed.append(content[cursor:])
    return "".join(transformed)


def _literal_reference_labels(
    content: str,
    references: dict[str, object],
) -> set[str]:
    """Return CommonMark reference labels used by preserved literal code."""
    labels: set[str] = set()
    for token in _COMMONMARK.parseInline(
        content,
        {"references": references},
    ):
        for child in token.children or ():
            label = child.meta.get("label")
            if child.meta.get("reference") and isinstance(label, str):
                labels.add(label)
    return labels


def transform_markdown(
    content: str,
    *,
    text_transform: TextTransform,
    link_transform: LinkTransform,
    standalone_link_transform: StandaloneLinkTransform | None = None,
    convert_semantic_quotes: bool = False,
) -> str:
    tokens, environment, source = _parse(content)
    edits: list[_Edit] = []
    reference_results: dict[str, list[bool]] = {}
    references = environment.get("references")
    reference_records = references if isinstance(references, dict) else {}
    literal_reference_labels: set[str] = set()

    for token in tokens:
        if token.map is not None and token.type in {"code_block", "html_block"}:
            start, end = source.line_range(token.map)
            edits.append(_Edit(start, end, None, 100))
            if token.type == "code_block":
                literal_reference_labels.update(
                    _literal_reference_labels(token.content, reference_records)
                )
            continue
        if token.type == "fence" and token.map is not None:
            start, end = source.line_range(token.map)
            if convert_semantic_quotes and _quote_fence(token):
                replacement = _quote_replacement(
                    token,
                    source,
                    text_transform=text_transform,
                    link_transform=link_transform,
                    standalone_link_transform=standalone_link_transform,
                )
                edits.append(_Edit(start, end, replacement, 110))
            else:
                edits.append(_Edit(start, end, None, 100))
                literal_reference_labels.update(
                    _literal_reference_labels(token.content, reference_records)
                )
            continue
        if token.type != "inline":
            continue

        boundaries = _inline_source_boundaries(token, source)
        token_links = _inline_links(token, source)
        standalone = _standalone_link(token, token_links)
        standalone_replacement: str | None = None
        if standalone is not None and standalone_link_transform is not None:
            standalone_replacement = standalone_link_transform(standalone[0])

        parents: dict[int, int] = {}
        for child_index, (_child, _link, child_span) in enumerate(token_links):
            containers = [
                parent_index
                for parent_index, (_parent, _parent_link, parent_span) in enumerate(
                    token_links
                )
                if parent_span[0] <= child_span[0]
                and child_span[1] <= parent_span[1]
                and parent_span != child_span
            ]
            if containers:
                parents[child_index] = min(
                    containers,
                    key=lambda index: (
                        token_links[index][2][1] - token_links[index][2][0]
                    ),
                )

        replacements: dict[int, str] = {}
        for link_index in sorted(
            range(len(token_links)),
            key=lambda index: (
                token_links[index][2][1] - token_links[index][2][0]
            ),
        ):
            child, link, span = token_links[link_index]
            if (
                standalone is not None
                and standalone[1] == span
                and standalone_replacement is not None
            ):
                replacement = standalone_replacement
            else:
                replacement = link_transform(link)
            for nested_index, parent_index in parents.items():
                if parent_index != link_index:
                    continue
                nested_link = token_links[nested_index][1]
                replacement = replacement.replace(
                    nested_link.raw,
                    replacements[nested_index],
                    1,
                )
            replacements[link_index] = replacement
            if link_index not in parents:
                edits.append(_Edit(span[0], span[1], replacement, 80))
            label = child.meta.get("label")
            if link.reference and isinstance(label, str):
                reference_results.setdefault(label, []).append(
                    replacement != link.raw
                )

        for child in token.children or ():
            if child.type not in {"code_inline", "html_inline"}:
                continue
            span = _absolute_span(
                boundaries,
                child.meta.get("source_start"),
                child.meta.get("source_end"),
            )
            if span is not None:
                edits.append(_Edit(span[0], span[1], None, 70))
            if child.type == "code_inline":
                literal_reference_labels.update(
                    _literal_reference_labels(child.content, reference_records)
                )

    if isinstance(references, dict):
        for label, record in references.items():
            if (
                not isinstance(label, str)
                or not isinstance(record, dict)
                or not isinstance(record.get("map"), list)
            ):
                continue
            start, end = source.line_range(record["map"])
            results = reference_results.get(label, [])
            if (
                results
                and all(results)
                and label not in literal_reference_labels
            ):
                edits.append(_Edit(start, end, "", 60))
            else:
                edits.append(_Edit(start, end, None, 60))

    transformed: list[str] = []
    cursor = 0
    for edit in _select_edits(edits):
        transformed.append(text_transform(content[cursor : edit.start]))
        transformed.append(
            content[edit.start : edit.end]
            if edit.replacement is None
            else edit.replacement
        )
        cursor = edit.end
    transformed.append(text_transform(content[cursor:]))
    return "".join(transformed)
