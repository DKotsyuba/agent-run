"""Bounded, session-scoped UserPromptSubmit context: capacity plus active agents.

Session lookup and the active-agent listing are plain SELECTs against
existing tables; the only write is the per-session dedup receipt in
``context_receipts``, which exists precisely so unchanged context is not
re-injected. Nothing here performs a network or provider call.
"""

from __future__ import annotations

import hashlib
import math
import time
from dataclasses import dataclass

from ..config import Config
from ..domain import OrchestratorRef
from ..errors import ValidationError
from ..state import StateStore, context_agents


CONTEXT_HARD_LIMIT_CHARS = 2500
ACTIVE_BLOCK_MAX_CHARS = 600
_MAX_LISTED_AGENTS = 5
_SILENCE_THRESHOLD_SECONDS = 60.0
#: Presentation order for capacity lines. Every lane is rendered -- filtering
#: healthy rows got a healthy lane read as "no data" twice -- but the worst
#: known risk leads, so the char budget can only ever clip the tail.


@dataclass(frozen=True)
class ContextResult:
    orchestrator_session_id: str | None
    context_key: str
    text: str
    injected: bool


def build_context(
    store: StateStore,
    ref: OrchestratorRef,
    *,
    config: Config,
    now: float | None = None,
) -> ContextResult:
    """Return newly changed visible context blocks for one orchestrator ref.

    ``store`` is read and receipt-written on its owning thread; ``ref`` scopes
    deduplication, ``config`` supplies the bounded budget, and ``now`` is an
    optional finite epoch. A zero budget returns an empty, non-injected result
    without recording a receipt.
    """
    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    if not isinstance(ref, OrchestratorRef):
        raise ValidationError("ref must be an OrchestratorRef")
    if not isinstance(config, Config):
        raise ValidationError("config must be a Config")
    at = time.time() if now is None else now
    if isinstance(at, bool) or not isinstance(at, (int, float)) or not math.isfinite(at):
        raise ValidationError("now must be a finite number")
    at = float(at)
    session_id = store.find_orchestrator_session(ref)

    capacity_text = _capacity_block(store, config, at)
    agents = () if session_id is None else _active_agents(store, session_id, at)
    active_text, active_key = _active_block(agents, at)
    budget = min(max(config.capacity.context_max_chars, 0), CONTEXT_HARD_LIMIT_CHARS)
    priority_text, active_text, active_slot = _assemble(capacity_text, active_text, budget)
    components = {}
    if priority_text:
        components["priority"] = hashlib.sha256(priority_text.encode()).hexdigest()
    if active_text:
        components["active"] = hashlib.sha256(f"{active_slot}:{active_key}".encode()).hexdigest()
    if not components:
        return ContextResult(session_id, _combine_key("", active_key), "", False)
    session_id, changed = store.record_context_components_for_ref(ref, components, at=at)
    text = "\n".join(
        value for name, value in (("priority", priority_text), ("active", active_text))
        if name in changed
    )
    return ContextResult(session_id, _combine_key(components.get("priority", ""), components.get("active", "")), text, bool(text))


def _capacity_block(store: StateStore, config: Config, at: float) -> str:
    """Render ordered, JSON-safe route selectors without raw measurements."""
    from ..capacity.order import build_capacity_order
    import json

    order = build_capacity_order(store, config, now=at)
    lines = [
        "Runtime priorities (highest first). Choose the first compatible subagent using the role/model table. A route applies only to a model belonging to its quota lane; if incompatible, skip the entire entry. account=null means omit the account selector. Do not recheck raw limits."
    ]
    if not order.routes:
        lines.append("No currently available routes.")
    for index, route in enumerate(order.routes, 1):
        selectors = []
        for alias in route.aliases:
            selector = {"quota_lane": alias.quota_lane}
            if alias.account is not None:
                selector["account"] = alias.account
            selectors.append(selector)
        lines.append(
            f"{index}. runtime={json.dumps(route.runtime)}; "
            f"selectors={json.dumps(selectors, separators=(',', ':'))}; "
            f"priority={route.priority:.3f}"
        )
    return "\n".join(lines)


def _active_agents(
    store: StateStore, session_id: str, at: float
) -> tuple[dict, ...]:
    return context_agents(store, session_id, at=at)


def _material_silence(agent: dict) -> bool:
    silent = agent["silent_seconds"]
    return isinstance(silent, (int, float)) and silent >= _SILENCE_THRESHOLD_SECONDS


def _safe_summary(value: object) -> str:
    printable = "".join(char if char.isprintable() else " " for char in str(value))
    return _truncate(" ".join(printable.split()), 48)


def _active_block(agents: tuple[dict, ...], at: float) -> tuple[str, str]:
    total = len(agents)
    if total == 0:
        return "", f"0:{total}"
    listed = agents[:_MAX_LISTED_AGENTS]
    entries = []
    key_parts = [str(total)]
    for agent in listed:
        started = agent["started_at"] or agent["created_at"]
        elapsed_minutes = max(int((at - started) // 60), 0)
        warned = bool(agent["warned"])
        silent = _material_silence(agent)
        flags = "".join(
            (" warn" if warned else "", " silent" if silent else "")
        )
        entries.append(
            f"{agent['id']} {agent['runtime']}/{agent['model']} {agent['profile']} "
            f"{_safe_summary(agent['task_summary'])} {agent['status']} "
            f"{elapsed_minutes}m{flags}"
        )
        key_parts.append(f"{agent['id']}:{agent['status']}:{warned}:{silent}")
    more = total - len(listed)
    suffix = f"; +{more} more" if more > 0 else ""
    guidance = " Use agent-run status/transcript; do not start replacements for existing ids."
    body = f"Active agents ({total}): " + "; ".join(entries) + suffix + "."
    text = _truncate(body, ACTIVE_BLOCK_MAX_CHARS - len(guidance)) + guidance
    return text, "|".join(key_parts)


def _combine_key(capacity_key: str, active_key: str) -> str:
    return hashlib.sha256(f"{capacity_key}::{active_key}".encode("utf-8")).hexdigest()[:32]


def _truncate(text: str, limit: int) -> str:
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    if limit <= 1:
        return text[:limit]
    return text[: limit - 1].rstrip() + "…"


def _truncate_priority(text: str, limit: int) -> str:
    """Keep complete priority lines and an omission instruction within ``limit``."""
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    lines = text.splitlines()
    hint = "More routes omitted; use capacity_order if needed"
    if not lines or len(lines[0]) + 1 + len(hint) > limit:
        return ""
    kept = [lines[0]]
    for line in lines[1:]:
        if len("\n".join((*kept, line, hint))) > limit:
            break
        kept.append(line)
    return "\n".join((*kept, hint))


def _assemble(capacity_text: str, active_text: str, budget: int) -> tuple[str, str, int]:
    """Reserve fixed slots so active-agent changes cannot resize priorities."""
    active_slot = min(ACTIVE_BLOCK_MAX_CHARS, budget)
    priority_slot = max(budget - active_slot - 1, 0)
    active = _truncate(active_text, active_slot)
    priority = _truncate_priority(capacity_text, priority_slot)
    return priority, active, active_slot
