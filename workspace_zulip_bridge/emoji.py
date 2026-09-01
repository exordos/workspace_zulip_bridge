import re
import unicodedata

_CODEPOINT_RE = re.compile(r"^[0-9a-fA-F]{1,6}$")
_PRESENTATION_SELECTORS = {"\ufe0e", "\ufe0f"}


def _unicode_scalar(raw_codepoint: str) -> int:
    if _CODEPOINT_RE.fullmatch(raw_codepoint) is None:
        raise ValueError("Zulip emoji code contains an invalid code point")
    codepoint = int(raw_codepoint, 16)
    if codepoint > 0x10FFFF or 0xD800 <= codepoint <= 0xDFFF:
        raise ValueError("Zulip emoji code contains an invalid Unicode scalar")
    return codepoint


def unicode_emoji_code(value: str) -> str:
    codepoints = [
        ord(character)
        for character in unicodedata.normalize("NFC", value)
        if character not in _PRESENTATION_SELECTORS
    ]
    if not codepoints:
        raise ValueError("Unicode emoji is empty after normalization")
    invalid_scalar = any(
        codepoint > 0x10FFFF or 0xD800 <= codepoint <= 0xDFFF
        for codepoint in codepoints
    )
    if invalid_scalar:
        raise ValueError("Unicode emoji contains an invalid Unicode scalar")
    return "-".join(f"{codepoint:x}" for codepoint in codepoints)


def unicode_emoji_from_code(value: str) -> str:
    if not value:
        raise ValueError("Zulip emoji code is empty")
    characters = [
        chr(_unicode_scalar(raw_codepoint))
        for raw_codepoint in value.split("-")
    ]
    glyph = unicodedata.normalize(
        "NFC",
        "".join(
            character
            for character in characters
            if character not in _PRESENTATION_SELECTORS
        ),
    )
    if not glyph:
        raise ValueError("Zulip emoji code is empty after normalization")
    return glyph


def canonical_unicode_emoji_code(value: str) -> str:
    return unicode_emoji_code(unicode_emoji_from_code(value))


def normalized_reaction_code(reaction_type: str, emoji_code: str) -> str:
    """Return the collision-safe code used by confirmed reaction state keys."""

    normalized_type = unicodedata.normalize("NFC", str(reaction_type))
    normalized_code = unicodedata.normalize("NFC", str(emoji_code))
    if normalized_type == "unicode_emoji":
        normalized_code = canonical_unicode_emoji_code(normalized_code)
    return f"{normalized_type}:{normalized_code}"
