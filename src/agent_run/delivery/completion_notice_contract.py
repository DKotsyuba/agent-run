"""Shared completion-notice contract loader and formatter."""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib import resources
from types import MappingProxyType
from typing import Mapping

# Packaged resource shared with the adjacent Node relay.
_CONTRACT_NAME = "completion_notice_contract.json"
# Cached package-owned strings; callers must not mutate this mapping.
@dataclass(frozen=True, slots=True)
class NoticeContract:
    """Validated package text shared by Python and the Node relay.

    ``template`` is the final notice skeleton; ``handling`` and ``resumed`` are
    operator instructions. ``default_failure`` is the unknown-kind fallback,
    while ``status_guidance`` and ``failure_guidance`` map trusted status/kind
    keys to immutable ``(reason, advice)`` pairs.
    """

    template: str
    handling: str
    resumed: str
    default_failure: tuple[str, str]
    status_guidance: Mapping[str, tuple[str, str]]
    failure_guidance: Mapping[str, tuple[str, str]]


_CONTRACT: NoticeContract | None = None


def _guidance(value: object, name: str) -> tuple[str, str]:
    """Validate one named reason/advice mapping and return its string pair."""

    if not isinstance(value, dict):
        raise RuntimeError(f"completion contract {name} must be a mapping")
    reason, advice = value.get("reason"), value.get("advice")
    if not isinstance(reason, str) or not reason.strip():
        raise RuntimeError(f"completion contract {name}.reason must be nonblank")
    if not isinstance(advice, str) or not advice.strip():
        raise RuntimeError(f"completion contract {name}.advice must be nonblank")
    return reason, advice


def _guidance_table(value: object, name: str) -> Mapping[str, tuple[str, str]]:
    """Validate a string-keyed guidance mapping and freeze its reason/advice pairs."""

    if not isinstance(value, dict):
        raise RuntimeError(f"completion contract {name} must be a mapping")
    table: dict[str, tuple[str, str]] = {}
    for key, entry in value.items():
        if not isinstance(key, str) or not key.strip():
            raise RuntimeError(f"completion contract {name} keys must be nonblank strings")
        table[key] = _guidance(entry, f"{name}.{key}")
    return MappingProxyType(table)


def _load_contract() -> NoticeContract:
    """Return the cached validated immutable notice contract.

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

    _CONTRACT = NoticeContract(
        template=template,
        handling=handling,
        resumed=resumed,
        default_failure=_guidance(resource.get("default_failure"), "default_failure"),
        status_guidance=_guidance_table(resource.get("status_guidance"), "status_guidance"),
        failure_guidance=_guidance_table(resource.get("failure_guidance"), "failure_guidance"),
    )
    return _CONTRACT


def failure_notice_block(status: str, failure_kind: str | None) -> str:
    """Return optional safe Failure/Advice lines for one terminal status and kind."""

    if status not in {"failed", "timed_out", "lost"}:
        return ""
    contract = _load_contract()
    kind = failure_kind or "unknown"
    if status == "timed_out":
        reason, advice = contract.status_guidance["timed_out"]
    else:
        reason, advice = contract.failure_guidance.get(
            kind, contract.status_guidance.get(status, contract.default_failure)
        )
    return f"\n- Failure: {kind} — {reason}\n- Advice: {advice}"


def format_notice_message(
    *,
    agent_id: str,
    status: str,
    runtime: str,
    model: str,
    effort: str,
    failure_block: str,
    version: int | str,
    notification_id: str,
) -> str:
    """Return str notice text from validated, display-safe lifecycle fields.

    agent_id and notification_id are trusted str identifiers; status is a
    terminal status string. runtime/model/effort are already escaped strings
    or explicit missing-value labels. failure_block is empty or package-owned
    safe guidance. version is the notice version (int/str), never the relay
    wire version. Substitution does not reinterpret braces in values. No inputs
    are mutated. Package load and format errors propagate.
    """

    contract = _load_contract()
    return contract.template.format(
        agent_id=agent_id,
        status=status,
        runtime=runtime,
        model=model,
        effort=effort,
        failure_block=failure_block,
        version=version,
        notification_id=notification_id,
    )


def completion_handling_contract() -> str:
    """Return str handling prose from cached package strings; load errors propagate."""

    contract = _load_contract()
    return f"{contract.handling} {contract.resumed}".strip()


def completion_notice_contract_text() -> str:
    """Return the same str instructions and fenced format for MCP start and docs.

    Placeholders describe lifecycle fields; the fence is documentation only
    and is never part of a delivered notice. Package load errors propagate.
    """

    contract = _load_contract()
    return f"{completion_handling_contract()}\n\nNotice format:\n```\n{contract.template}\n```"
