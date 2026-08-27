"""Operator guide loader: package-data Markdown topics for maintainers.

Content lives in :mod:`agent_run.operator_guide` as one Markdown file per
topic, served by ``agent-run doc [topic]`` and the MCP ``doc`` tool.
"""

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
    "service",
    "releases",
    "migrations",
    "troubleshoot",
)


def topic_text(topic: str | None = None) -> str:
    """Return one topic's Markdown, or the index when ``topic`` is omitted."""

    name = "index" if topic is None else topic
    if topic is not None and topic != "index" and topic not in TOPICS:
        raise ValidationError(
            f"unknown operator guide topic: {topic!r}; valid topics: "
            f"{', '.join(TOPICS)}"
        )
    return (_GUIDE_DIR / f"{name}.md").read_text(encoding="utf-8")
