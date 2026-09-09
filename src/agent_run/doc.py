"""Operator guide loader for packaged Markdown topics."""

from __future__ import annotations

from pathlib import Path

from .errors import ValidationError

_GUIDE_DIR = Path(__file__).with_name("operator_guide")

TOPICS: tuple[str, ...] = (
    "config",
    "skills",
    "mcp-servers",
    "plugins",
    "models",
    "releases",
    "migrations",
    "troubleshoot",
)


def topic_text(topic: str | None = None) -> str:
    """Return Markdown for a str topic, with None selecting the index.

    Unknown names raise ValidationError; missing or malformed package resources
    propagate errors.
    """

    name = "index" if topic is None else topic
    if topic is not None and topic != "index" and topic not in TOPICS:
        raise ValidationError(
            f"unknown operator guide topic: {topic!r}; valid topics: "
            f"{', '.join(TOPICS)}"
        )

    return (_GUIDE_DIR / f"{name}.md").read_text(encoding="utf-8")
