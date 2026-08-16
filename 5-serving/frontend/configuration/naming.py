"""
Rules for the name fields (the sign-in username and the profile display name).

"No special characters" is read as "the characters Python allows in a variable
name": Roman letters, Arabic numerals and the underscore. The underscore has to
stay — the existing accounts (radar_electronics, radar_pharma, radar_agrifood)
use it, and rejecting it would lock everyone out.
"""
import re

NAME_MAX_CHARS = 20

_NAME_RE = re.compile(r"^[A-Za-z0-9_]{1,%d}$" % NAME_MAX_CHARS)

NAME_HELP = (
    "Necessary to link you, within our database, to the news you might want. "
    "For this reason, whatever username you set could only have had Roman letters, Arabic numerals and underscores (no spaces). "
    f"Max characters: {NAME_MAX_CHARS}"
)


def is_valid_name(value: str) -> bool:
    """True if `value` is a legal name (letters/digits/underscore, 1-20 chars)."""
    return bool(_NAME_RE.match(value or ""))
