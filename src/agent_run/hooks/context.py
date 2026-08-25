"""Bounded, session-scoped UserPromptSubmit context: capacity plus active agents.

Session lookup and the active-agent listing are plain SELECTs against
existing tables; the only write is the per-session dedup receipt in
``context_receipts``, which exists precisely so unchanged context is not
re-injected. Nothing here performs a network or provider call.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass

from ..capacity import advice as advice_module
from ..capacity import forecast as forecast_module
from ..capacity import history as history_module
from ..config import Config
from ..domain import ACTIVE, OrchestratorRef
from ..errors import ValidationError
from ..state import StateStore
from ..state.db import immediate, timestamp


CONTEXT_HARD_LIMIT_CHARS = 2500
ACTIVE_BLOCK_MAX_CHARS = 600
_MAX_LISTED_AGENTS = 5
_SILENCE_THRESHOLD_SECONDS = 60.0


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
    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    if not isinstance(ref, OrchestratorRef):
        raise ValidationError("ref must be an OrchestratorRef")
    if not isinstance(config, Config):
        raise ValidationError("config must be a Config")
    at = timestamp(time.time() if now is None else now)

    session_row = store.connection.execute(
        "SELECT id FROM orchestrator_sessions WHERE transport = ? AND external_session_id = ?",
        (ref.transport, ref.external_session_id),
    ).fetchone()
    session_id = None if session_row is None else str(session_row["id"])

    capacity_text, capacity_key = _capacity_block(store, at)
    agents = () if session_id is None else _active_agents(store, session_id)
    active_text, active_key = _active_block(agents, at)
    context_key = _combine_key(capacity_key, active_key)
    budget = min(max(config.capacity.context_max_chars, 0), CONTEXT_HARD_LIMIT_CHARS)
    text = _assemble(capacity_text, active_text, budget)

    if session_id is None:
        return ContextResult(None, context_key, text, True)

    with immediate(store.connection):
        previous = store.connection.execute(
            "SELECT context_key FROM context_receipts WHERE orchestrator_session_id = ?",
            (session_id,),
        ).fetchone()
        if previous is not None and previous["context_key"] == context_key:
            return ContextResult(session_id, context_key, "", False)
        store.connection.execute(
            """INSERT INTO context_receipts (orchestrator_session_id, context_key, injected_at)
               VALUES (?, ?, ?)
               ON CONFLICT(orchestrator_session_id) DO UPDATE SET
                 context_key = excluded.context_key, injected_at = excluded.injected_at""",
            (session_id, context_key, at),
        )
    return ContextResult(session_id, context_key, text, True)


def _capacity_block(store: StateStore, at: float) -> tuple[str, str]:
    series = history_module.load_series(store)
    forecasts = forecast_module.build_forecasts(series, now=at)
    items = advice_module.build_advice(forecasts)
    key = advice_module.advice_key(items)
    lines = [
        (
            f"{item.key.runtime}/{item.key.lane} {item.key.window}: "
            f"{'unknown' if item.remaining_percent is None else f'{item.remaining_percent:.0f}%'} "
            f"remaining, risk={item.risk}"
        )
        for item in sorted(items, key=lambda item: (item.key.runtime, item.key.lane, item.key.window))
        if item.risk != forecast_module.RISK_LOW
    ]
    text = "Capacity: " + ("; ".join(lines) if lines else "nominal") + "."
    return text, key


def _active_agents(store: StateStore, session_id: str) -> tuple[dict, ...]:
    statuses = tuple(status.value for status in ACTIVE)
    placeholders = ",".join("?" for _ in statuses)
    rows = store.connection.execute(
        f"""SELECT id, runtime, model, profile, task_summary, status, created_at,
                   started_at, warned, silent_seconds
            FROM agents
            WHERE orchestrator_session_id = ? AND status IN ({placeholders})
            ORDER BY created_at""",
        (session_id, *statuses),
    ).fetchall()
    return tuple(dict(row) for row in rows)


def _material_silence(agent: dict) -> bool:
    silent = agent["silent_seconds"]
    return isinstance(silent, (int, float)) and silent >= _SILENCE_THRESHOLD_SECONDS


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
            f"{agent['status']} {elapsed_minutes}m{flags}"
        )
        key_parts.append(f"{agent['id']}:{agent['status']}:{warned}:{silent}")
    more = total - len(listed)
    suffix = f"; +{more} more" if more > 0 else ""
    text = f"Active agents ({total}): " + "; ".join(entries) + suffix + "."
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


def _assemble(capacity_text: str, active_text: str, budget: int) -> str:
    active_text = _truncate(active_text, ACTIVE_BLOCK_MAX_CHARS)
    remaining_for_capacity = max(budget - len(active_text) - 1, 0)
    capacity_text = _truncate(capacity_text, remaining_for_capacity)
    parts = [part for part in (capacity_text, active_text) if part]
    return _truncate("\n".join(parts), budget)
