"""Normalized per-run statistics: one queryable row per agent.

Token and timing data arrives inside runtime-specific event payloads -- one
``runtime_result`` event for the claude/glm/qwen family, a stream of
``thread/tokenUsage/updated`` events for codex whose last entry is cumulative.
``record_run_stats`` reads the agents row plus that one agent's events,
normalizes both shapes, and keeps a single ``run_stats`` row per agent.

A measurement the runtime never reported stays NULL; it is never fabricated
as zero.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any, Mapping

from agent_run.domain import TERMINAL, AgentId, validate_agent_id

from .db import agent_row, immediate, timestamp

_logger = logging.getLogger("agent_run.state")

_USAGE_EVENT_KINDS = ("runtime_result", "thread/tokenUsage/updated")

_INSERT = """INSERT OR REPLACE INTO run_stats
   (agent_id, runtime, model, profile, status, failure_kind,
    started_at, finished_at, duration_seconds,
    input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
    reasoning_tokens, total_tokens, num_turns, ttft_ms, api_duration_ms,
    cost_usd, usage_source, recorded_at)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""


def _number(payload: Mapping[str, Any], key: str) -> float | None:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _nested_number(payload: Mapping[str, Any], *keys: str) -> float | None:
    value: Any = payload
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _runtime_result_stats(payload: Mapping[str, Any]) -> dict[str, object]:
    return {
        "usage_source": "runtime_result",
        "input_tokens": _nested_number(payload, "usage", "input_tokens"),
        "output_tokens": _nested_number(payload, "usage", "output_tokens"),
        "cache_read_tokens": _nested_number(
            payload, "usage", "cache_read_input_tokens"
        ),
        "cache_write_tokens": _nested_number(
            payload, "usage", "cache_creation_input_tokens"
        ),
        "reasoning_tokens": _nested_number(
            payload, "usage", "output_tokens_details", "thinking_tokens"
        ),
        "total_tokens": None,
        "num_turns": _number(payload, "num_turns"),
        "ttft_ms": _number(payload, "ttft_ms"),
        "api_duration_ms": _number(payload, "duration_api_ms"),
        "cost_usd": _number(payload, "total_cost_usd"),
    }


def _token_usage_stats(payload: Mapping[str, Any]) -> dict[str, object]:
    total = payload.get("tokenUsage")
    if not isinstance(total, Mapping):
        total = {}
    total = total.get("total")
    if not isinstance(total, Mapping):
        total = {}
    return {
        "usage_source": "token_usage_updated",
        "input_tokens": _number(total, "inputTokens"),
        "output_tokens": _number(total, "outputTokens"),
        "cache_read_tokens": _number(total, "cachedInputTokens"),
        "cache_write_tokens": _number(total, "cacheWriteInputTokens"),
        "reasoning_tokens": _number(total, "reasoningOutputTokens"),
        "total_tokens": _number(total, "totalTokens"),
        "num_turns": None,
        "ttft_ms": None,
        "api_duration_ms": None,
        "cost_usd": None,
    }


def _empty_stats() -> dict[str, object]:
    return {
        "usage_source": "none",
        "input_tokens": None,
        "output_tokens": None,
        "cache_read_tokens": None,
        "cache_write_tokens": None,
        "reasoning_tokens": None,
        "total_tokens": None,
        "num_turns": None,
        "ttft_ms": None,
        "api_duration_ms": None,
        "cost_usd": None,
    }


def _usage_stats(events: list[sqlite3.Row]) -> dict[str, object]:
    """Normalize the most informative usage payload this agent recorded.

    The claude family's single ``runtime_result`` wins over the codex stream;
    within the codex stream the last event carries the cumulative total.
    """

    parsed: dict[str, list[Mapping[str, Any]]] = {kind: [] for kind in _USAGE_EVENT_KINDS}
    for event in events:
        try:
            payload = json.loads(str(event["data_json"]))
        except (TypeError, ValueError):
            continue
        if isinstance(payload, Mapping):
            bucket = parsed.get(str(event["kind"]))
            if bucket is not None:
                bucket.append(payload)
    if parsed["runtime_result"]:
        return _runtime_result_stats(parsed["runtime_result"][-1])
    if parsed["thread/tokenUsage/updated"]:
        return _token_usage_stats(parsed["thread/tokenUsage/updated"][-1])
    return _empty_stats()


_TERMINAL_VALUES = frozenset(status.value for status in TERMINAL)


def _transition_times(events: list[sqlite3.Row]) -> tuple[float | None, float | None]:
    """Start/finish instants from the lifecycle transition events' ``at``."""

    started_at = None
    finished_at = None
    for event in events:
        to_status = event["to_status"]
        if started_at is None and to_status == "running":
            started_at = float(event["at"])
        if finished_at is None and to_status in _TERMINAL_VALUES:
            finished_at = float(event["at"])
    return started_at, finished_at


def _transition_events(
    connection: sqlite3.Connection, agent_id: str
) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in _USAGE_EVENT_KINDS)
    return list(
        connection.execute(
            f"""SELECT seq, at, kind, to_status, data_json FROM events
                WHERE agent_id = ?
                  AND (to_status IS NOT NULL OR kind IN ({placeholders}))
                ORDER BY seq""",
            (agent_id, *_USAGE_EVENT_KINDS),
        )
    )


def record_run_stats(
    store: Any, agent_id: str | AgentId, *, at: float | None = None
) -> dict[str, object]:
    """Snapshot one agent's usage/timing events into its ``run_stats`` row.

    Idempotent: re-recording replaces the row from the same durable sources.
    """

    connection = store.connection
    agent_id = validate_agent_id(agent_id)
    recorded_at = timestamp(at)
    with immediate(connection):
        agent = agent_row(connection, agent_id)
        events = _transition_events(connection, agent_id)
        started_at, finished_at = _transition_times(events)
        stats = _usage_stats(events)
        duration_seconds = None
        if started_at is not None and finished_at is not None:
            duration_seconds = max(0.0, finished_at - started_at)
        connection.execute(
            _INSERT,
            (
                agent_id,
                str(agent["runtime"]),
                str(agent["model"]),
                str(agent["profile"]),
                str(agent["status"]),
                agent["failure_kind"],
                started_at,
                finished_at,
                duration_seconds,
                stats["input_tokens"],
                stats["output_tokens"],
                stats["cache_read_tokens"],
                stats["cache_write_tokens"],
                stats["reasoning_tokens"],
                stats["total_tokens"],
                stats["num_turns"],
                stats["ttft_ms"],
                stats["api_duration_ms"],
                stats["cost_usd"],
                stats["usage_source"],
                recorded_at,
            ),
        )
    row = connection.execute(
        "SELECT * FROM run_stats WHERE agent_id = ?", (agent_id,)
    ).fetchone()
    return dict(row)


def backfill_run_stats(store: Any, *, at: float | None = None) -> dict[str, int]:
    """Record stats for every agent that has no ``run_stats`` row yet."""

    connection = store.connection
    missing = list(
        connection.execute(
            """SELECT agents.id FROM agents
               LEFT JOIN run_stats ON run_stats.agent_id = agents.id
               WHERE run_stats.agent_id IS NULL
               ORDER BY agents.created_at, agents.id"""
        )
    )
    backfilled = 0
    skipped = 0
    for row in missing:
        try:
            record_run_stats(store, str(row["id"]), at=at)
        except Exception as error:
            skipped += 1
            _logger.warning(
                "agent_id=%s stage=run_stats_backfill failed error_kind=%s",
                row["id"], type(error).__name__,
            )
        else:
            backfilled += 1
    return {"backfilled": backfilled, "skipped": skipped}


def record_run_stats_best_effort(
    store: Any, agent_id: str | AgentId, *, at: float | None = None
) -> None:
    """The supervisor's terminal-path hook: one WARNING, never a failed run."""

    try:
        record_run_stats(store, agent_id, at=at)
    except Exception as error:
        _logger.warning(
            "agent_id=%s stage=run_stats failed error_kind=%s",
            agent_id, type(error).__name__,
        )
