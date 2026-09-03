"""Frozen view contract between the dashboard reader and the curses screens.

The reader (``agent_run_tui.snapshot``) produces a ``Snapshot``; the screens
(``agent_run_tui.app``) only consume it. Neither side may add behaviour here:
this module holds plain data so both halves can be built and tested apart.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class AgentCard:
    """One child agent as shown on a card.

    ``active`` is true for the non-terminal statuses ``created``, ``starting``,
    ``running`` and ``cancelling``. ``summary`` is ``task_summary`` when
    present, otherwise the first line of the task truncated by the reader.
    ``last_event`` is a one-line hint of the newest transcript message
    (``"Bash: git status"`` for a tool call, or the head of the latest
    assistant text); ``None`` when there are no messages yet.
    ``elapsed_seconds`` is measured against the snapshot's ``observed_at`` for
    running agents and against ``finished_at`` otherwise.
    """

    agent_id: str
    runtime: str
    model: str | None
    effort: str | None
    status: str
    active: bool
    summary: str
    started_at: float | None
    finished_at: float | None
    elapsed_seconds: float | None
    last_event: str | None
    failure_kind: str | None = None


@dataclass(frozen=True, slots=True)
class SessionCard:
    """One orchestrator session (a parent) with its child counts.

    ``session_id`` is the orchestrator session id (``ors_…``) or the literal
    ``"unbound"`` for agents launched without a parent. ``title`` is resolved
    from the host runtime (Claude Code custom title, Codex thread name) and
    falls back to a short form of ``external_session_id``. ``active`` counts
    children with a non-terminal status; ``total`` counts all children.
    """

    session_id: str
    transport: str
    external_session_id: str
    title: str
    cwd: str | None
    active: int
    total: int
    last_seen_at: float


@dataclass(frozen=True, slots=True)
class Snapshot:
    """Everything one dashboard refresh needs, read at ``observed_at``.

    ``sessions`` is sorted by ``active`` descending, then ``last_seen_at``
    descending. ``agents`` maps ``session_id`` to that session's cards: active
    ones first (newest ``started_at`` first), then finished ones (newest
    ``finished_at`` first), the finished part capped by the reader's limit.
    """

    observed_at: float
    sessions: tuple[SessionCard, ...] = ()
    agents: dict[str, tuple[AgentCard, ...]] = field(default_factory=dict)
