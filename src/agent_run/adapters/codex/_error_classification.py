"""Normalize structured Codex provider failure categories."""

from __future__ import annotations

from collections.abc import Mapping


def _structured_failure_kind(error: Mapping[str, object]) -> str | None:
    """Return explicit kind/code or a safe bounded Codex structured code.

    Existing non-null ``kind``/``code`` precedence is preserved. The known
    ``serverOverloaded`` provider code maps to the stable
    ``provider_overloaded`` category. Other nonblank structured codes retain a
    bounded ASCII identifier for diagnostics; provider detail objects are ignored.
    """

    explicit = error.get("kind") or error.get("code")
    if isinstance(explicit, str):
        return explicit
    value = error.get("codexErrorInfo")
    if not isinstance(value, str) or not value.strip():
        return None
    code = value.strip()
    if code == "serverOverloaded":
        return "provider_overloaded"
    safe = "".join(
        character
        if character.isascii() and (character.isalnum() or character in "._-")
        else "_"
        for character in code
    )[:58].strip("._-")
    return f"codex_{safe or 'error'}"
