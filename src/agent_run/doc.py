"""Operator guide loader: package-data Markdown topics for maintainers.

Most topics are Markdown in :mod:`agent_run.operator_guide`; completion uses
the shared delivery contract. CLI and MCP serve the same content.
"""

from __future__ import annotations

from pathlib import Path

from .errors import ValidationError
from .delivery.completion_notice_contract import completion_notice_contract_text

_GUIDE_DIR = Path(__file__).with_name("operator_guide")

TOPICS: tuple[str, ...] = (
    "completion",
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
    """Return Markdown for a str topic, with None selecting the index.

    Completion is composed from the cached packaged delivery contract;
    other topics read their packaged Markdown file. Unknown names raise
    ValidationError; missing or malformed package resources propagate errors.
    """

    name = "index" if topic is None else topic
    if topic == "completion":
        return completion_notice_contract_text()

    if topic is not None and topic != "index" and topic not in TOPICS:
        raise ValidationError(
            f"unknown operator guide topic: {topic!r}; valid topics: "
            f"{', '.join(TOPICS)}"
        )

    return (_GUIDE_DIR / f"{name}.md").read_text(encoding="utf-8")
