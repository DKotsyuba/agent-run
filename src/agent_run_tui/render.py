"""Pure text rendering for the agent-run terminal dashboard."""

from __future__ import annotations

from .model import AgentCard, Snapshot


MAX_WIDTH = 72
SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def truncate(text: str, width: int) -> str:
    """Return ``text`` clipped to ``width`` character cells with an ellipsis.

    The renderer treats every Python character as one cell so it remains safe
    for terminals with differing wide-character support.  A non-positive
    width produces an empty string.
    """
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width == 1:
        return "…"
    return text[: width - 1] + "…"


def format_elapsed(seconds: float | None) -> str:
    """Format optional elapsed seconds as a compact dashboard duration."""
    if seconds is None:
        return "—"
    whole = max(0, int(seconds))
    hours, remainder = divmod(whole, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def spinner(tick: int) -> str:
    """Return the deterministic braille spinner frame for a redraw tick."""
    return SPINNER_FRAMES[tick % len(SPINNER_FRAMES)]


def _width(width: int) -> int:
    """Clamp a terminal width to the dashboard's readable content limit."""
    return min(max(width, 0), MAX_WIDTH)


def _card(title: str, detail: str, width: int, selected: bool) -> list[str]:
    """Build a two-line left-aligned card with a visible selection marker."""
    prefix = "> " if selected else "  "
    return [truncate(prefix + title, width), truncate("  " + detail, width)]


def render_sessions(snapshot: Snapshot, width: int, selected: int) -> list[str]:
    """Render session cards, preserving ``snapshot.sessions`` ordering.

    ``selected`` is the zero-based session card index; an empty snapshot has a
    single explanatory line.  All output is bounded by the supplied terminal
    width and the dashboard maximum width.
    """
    width = _width(width)
    if not snapshot.sessions:
        return [truncate("no sessions", width)] if width else []
    lines: list[str] = []
    for index, card in enumerate(snapshot.sessions):
        cwd = card.cwd.rsplit("/", 1)[-1] if card.cwd else ""
        details = f"active {card.active} · total {card.total} · {card.transport}"
        if cwd:
            details += f" · {cwd}"
        lines.extend(_card(card.title, details, width, index == selected))
    return lines


def _agent_lines(agent: AgentCard, width: int, selected: bool, tick: int) -> list[str]:
    """Render one active agent card with its current activity hint."""
    parts = [part for part in (agent.runtime, agent.model, agent.effort) if part]
    event = agent.last_event or "waiting"
    detail = f"{spinner(tick)} {format_elapsed(agent.elapsed_seconds)}  {event}"
    return _card(agent.summary, " · ".join(parts), width, selected) + [
        truncate("  " + detail, width)
    ]


def _finished_lines(agent: AgentCard, width: int, selected: bool) -> list[str]:
    """Render one finished agent card with its terminal status evidence."""
    parts = [part for part in (agent.runtime, agent.model, agent.effort) if part]
    detail = f"{agent.status} {format_elapsed(agent.elapsed_seconds)}"
    if agent.failure_kind:
        detail += f" · {agent.failure_kind}"
    return _card(agent.summary, " · ".join(parts), width, selected) + [
        truncate("  " + detail, width)
    ]


def render_agents(
    snapshot: Snapshot,
    session_id: str,
    width: int,
    selected: int,
    finished_expanded: bool,
    tick: int,
) -> list[str]:
    """Render active and optionally finished cards for one session.

    The selection index spans active cards followed by finished cards.  The
    divider always follows active cards; finished cards appear only when the
    caller has expanded that section.
    """
    width = _width(width)
    agents = snapshot.agents.get(session_id, ())
    active = [agent for agent in agents if agent.active]
    finished = [agent for agent in agents if not agent.active]
    lines: list[str] = []
    for index, agent in enumerate(active):
        lines.extend(_agent_lines(agent, width, index == selected, tick))
    lines.append(truncate(f"── finished ({len(finished)}) ─ [tab] ──", width))
    if finished_expanded:
        offset = len(active)
        for index, agent in enumerate(finished):
            lines.extend(_finished_lines(agent, width, offset + index == selected))
    return lines
