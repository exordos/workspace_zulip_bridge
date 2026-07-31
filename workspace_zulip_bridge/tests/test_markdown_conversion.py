import pytest

from workspace_zulip_bridge import markdown_conversion


@pytest.mark.parametrize(
    ("content", "destinations"),
    (
        (
            '[docs](https://example.com "Title")',
            ("https://example.com",),
        ),
        (
            "`[inline](https://example.com)`",
            (),
        ),
        (
            "````markdown\n[code](https://example.com)\n`````",
            (),
        ),
        (
            "[nested [label]](https://example.com/a_(b))",
            ("https://example.com/a_(b)",),
        ),
    ),
)
def test_structured_markdown_pass_distinguishes_links_and_code(
    content,
    destinations,
):
    parsed_destinations = []

    def link_transform(link):
        parsed_destinations.append(link.destination)
        return link.raw

    converted = markdown_conversion.transform_markdown(
        content,
        text_transform=lambda value: value,
        link_transform=link_transform,
    )

    assert converted == content
    assert tuple(parsed_destinations) == destinations


def test_semantic_quote_links_only_returns_real_quote_headers():
    reply = (
        "@_**Other User|2** "
        "[said](https://zulip.example.invalid/#narrow/near/601):\n"
        "```quote\noriginal\n```\n"
    )
    literal = (
        "````markdown\n"
        "[said](https://zulip.example.invalid/#narrow/near/999):\n"
        "```quote\nliteral\n```\n"
        "````\n"
    )

    links = markdown_conversion.semantic_quote_links(literal + reply)

    assert [link.destination for link in links] == [
        "https://zulip.example.invalid/#narrow/near/601"
    ]


def test_semantic_quote_link_label_may_be_localized():
    reply = (
        "@_**Other User|2** "
        "[сказал/а](https://zulip.example.invalid/#narrow/near/601):\n"
        "```quote\noriginal\n```\n"
    )

    links = markdown_conversion.semantic_quote_links(reply)

    assert [link.destination for link in links] == [
        "https://zulip.example.invalid/#narrow/near/601"
    ]


def test_semantic_quote_transform_replaces_header_and_body_but_preserves_reply():
    reply = (
        "@_**Other User|2** "
        "[said](https://zulip.example.invalid/#narrow/near/601):\n"
        "```quote\noriginal\n```\n\nreply"
    )

    converted = markdown_conversion.transform_semantic_quotes(
        reply,
        lambda _link, body: f"[Other User](urn:quote:message-uuid?text={body})",
    )

    assert converted == (
        "[Other User](urn:quote:message-uuid?text=original)\n\nreply"
    )


def test_semantic_quote_transform_preserves_literal_example_fence():
    literal = (
        "````markdown\n"
        "[said](https://zulip.example.invalid/#narrow/near/999):\n"
        "```quote\nliteral\n```\n"
        "````\n"
    )

    converted = markdown_conversion.transform_semantic_quotes(
        literal,
        lambda _link, _body: "replacement",
    )

    assert converted == literal
