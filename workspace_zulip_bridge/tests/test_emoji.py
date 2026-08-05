import pytest

from workspace_zulip_bridge import emoji


@pytest.mark.parametrize(
    ("emoji_code", "glyph", "canonical_code"),
    [
        ("270d", "✍", "270d"),
        ("2764-FE0F", "❤", "2764"),
        ("1f469-200d-1f4bb", "👩‍💻", "1f469-200d-1f4bb"),
        ("1f44d-1f3fd", "👍🏽", "1f44d-1f3fd"),
    ],
)
def test_zulip_unicode_emoji_codes_are_normalized(
    emoji_code, glyph, canonical_code
):
    assert emoji.unicode_emoji_from_code(emoji_code) == glyph
    assert emoji.canonical_unicode_emoji_code(emoji_code) == canonical_code
    assert emoji.unicode_emoji_code(glyph + "\ufe0f") == canonical_code


@pytest.mark.parametrize(
    "emoji_code",
    ["", "not-hex", "1f44d--1f3fd", "110000", "d800"],
)
def test_invalid_zulip_unicode_emoji_codes_are_rejected(emoji_code):
    with pytest.raises(ValueError, match="Zulip emoji code"):
        emoji.unicode_emoji_from_code(emoji_code)
