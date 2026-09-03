"""Shared completion-notice contract loader and formatter."""

from __future__ import annotations

import json
from importlib import resources

# Packaged resource shared with the adjacent Node relay.
_CONTRACT_NAME = "completion_notice_contract.json"
# Cached package-owned strings; callers must not mutate this mapping.
_CONTRACT: dict[str, str] | None = None


def _load_contract() -> dict[str, str]:
    """Return cached package strings as dict[str, str], borrowed read-only.

    The first call reads UTF-8 package JSON; concurrent first callers may
    perform equivalent reads. I/O/JSON errors propagate, while invalid field
    types raise RuntimeError. The installed contract is immutable for a process.
    """

    global _CONTRACT
    if _CONTRACT is not None:
        return _CONTRACT

    resource = json.loads(
        resources.files("agent_run.delivery").joinpath(_CONTRACT_NAME).read_text(encoding="utf-8")
    )
    if not isinstance(resource, dict):
        raise RuntimeError(f"completion contract is not a mapping: {_CONTRACT_NAME!r}")
    template = resource.get("template")
    handling = resource.get("handling")
    resumed = resource.get("handled_by_resumed_sessions")
    if not isinstance(template, str) or not isinstance(handling, str) or not isinstance(
        resumed, str
    ):
        raise RuntimeError(f"completion contract is malformed: {_CONTRACT_NAME!r}")

    _CONTRACT = {
        "template": template,
        "handling": handling,
        "handled_by_resumed_sessions": resumed,
    }
    return _CONTRACT


def format_notice_message(
    *,
    agent_id: str,
    status: str,
    runtime: str,
    model: str,
    effort: str,
    version: int | str,
    notification_id: str,
) -> str:
    """Return str notice text from validated, display-safe lifecycle fields.

    agent_id and notification_id are trusted str identifiers; status is a
    terminal status string. runtime/model/effort are already escaped strings
    or explicit missing-value labels. version is the notice version (int/str),
    never the relay wire version. Substitution does not reinterpret braces in
    values. No inputs are mutated. Package load and format errors propagate.
    """

    contract = _load_contract()
    return contract["template"].format(
        agent_id=agent_id,
        status=status,
        runtime=runtime,
        model=model,
        effort=effort,
        version=version,
        notification_id=notification_id,
    )


def completion_handling_contract() -> str:
    """Return str handling prose from cached package strings; load errors propagate."""

    contract = _load_contract()
    return f"{contract['handling']} {contract['handled_by_resumed_sessions']}".strip()


def completion_notice_contract_text() -> str:
    """Return the same str instructions and fenced format for MCP start and docs.

    Placeholders describe lifecycle fields; the fence is documentation only
    and is never part of a delivered notice. Package load errors propagate.
    """

    contract = _load_contract()
    return f"{completion_handling_contract()}\n\nNotice format:\n```\n{contract['template']}\n```"
