"""Pure text rendering for the agent-run terminal dashboard.

Every renderer returns :class:`Line` rows whose ``text`` fits the requested
width in terminal cells; the curses layer only maps ``style`` to attributes.
"""

from __future__ import annotations

from dataclasses import dataclass
from unicodedata import east_asian_width

from .model import AgentCard, SessionCard, Snapshot


MAX_WIDTH = 72
SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
STYLES = frozenset({"normal", "selected", "dim", "ok", "err", "title"})
#: Terminal cells reserved for the left part of a row before its right-aligned
#: part is dropped.
MIN_LEFT = 20
STATUS_GLYPHS = {"succeeded": "✔", "failed": "✘", "timed_out": "⏰", "cancelled": "⊘", "lost": "⚠"}
_HOST_RUNTIMES = {"claude_uds": "claude", "codex_queue": "codex", "unbound": "—", "": "—"}


@dataclass(frozen=True, slots=True)
class Line:
    """One rendered row: display ``text`` plus a style name from ``STYLES``.

    ``card`` is the zero-based selectable card the row belongs to, or ``None``
    for labels, borders' surroundings and blank separators.
    """

    text: str
    style: str = "normal"
    card: int | None = None


def display_width(text: str) -> int:
    """Return the terminal cell count of ``text`` (east-asian wide chars are 2)."""
    return sum(2 if east_asian_width(char) in "WF" else 1 for char in text)


def truncate(text: str, width: int) -> str:
    """Clip ``text`` to ``width`` cells, ending with an ellipsis when cut."""
    if width <= 0:
        return ""
    if display_width(text) <= width:
        return text
    kept, used = [], 0
    for char in text:
        cells = display_width(char)
        if used + cells > width - 1:
            break
        kept.append(char)
        used += cells
    return "".join(kept) + "…"


def pad(text: str, width: int) -> str:
    """Return ``text`` clipped and space-padded to exactly ``width`` cells."""
    text = truncate(text, width)
    return text + " " * (width - display_width(text))


def spread(left: str, right: str, width: int) -> str:
    """Lay ``left`` and a right-aligned ``right`` on one row of ``width`` cells.

    The right part is dropped when keeping it would leave the left part fewer
    than ``MIN_LEFT`` cells (or fewer than it needs, if shorter).
    """
    room = width - display_width(right) - 2
    if not right or room < min(display_width(left), MIN_LEFT):
        return truncate(left, width)
    return pad(left, room) + "  " + right


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


def host_runtime(transport: str) -> str:
    """Name the host runtime a session came from, given its API transport."""
    return _HOST_RUNTIMES.get(transport, transport)


def render_header(left: str, hint: str, width: int) -> tuple[str, str]:
    """Fit a header title and its key hint into ``width`` cells.

    Returns ``(left, hint)`` to draw at the left and right edges.  Hint parts
    (separated by `` · ``) are dropped from the right until the whole title
    fits before a one-cell gap; the first part always stays and the title is
    truncated to whatever room remains.
    """
    width = _width(width)
    parts = hint.split(" · ") if hint else []
    while len(parts) > 1 and width - display_width(" · ".join(parts)) - 1 < display_width(left):
        parts.pop()
    hint = " · ".join(parts)
    return truncate(left, width - display_width(hint) - (1 if hint else 0)), hint


def _width(width: int) -> int:
    """Clamp a terminal width to the dashboard's readable content limit."""
    return min(max(width, 0), MAX_WIDTH)


def _box(rows: list[tuple[str, str]], width: int, selected: bool, card: int) -> list[Line]:
    """Frame ``(text, style)`` rows in a light box after a one-cell left margin.

    The selected card gets a bold border; every emitted row fits ``width``.
    """
    inner = max(1, width - 5)
    border = "selected" if selected else "dim"
    lines = [Line(truncate(" ┌" + "─" * (inner + 2) + "┐", width), border, card)]
    lines += [Line(truncate(" │ " + pad(text, inner) + " │", width), style, card) for text, style in rows]
    lines.append(Line(truncate(" └" + "─" * (inner + 2) + "┘", width), border, card))
    return lines


def _title_row(title: str, selected: bool) -> tuple[str, str]:
    """Return the first card row, marking the selected card with ``▶``."""
    return ("▶ " if selected else "") + title, "title"


def _session_rows(card: SessionCard, selected: bool) -> list[tuple[str, str]]:
    """Return the three content rows of one session card."""
    origin = host_runtime(card.transport)
    if card.cwd:
        origin += " · " + card.cwd.rstrip("/").rsplit("/", 1)[-1]
    counts = f"● {card.active} active   ○ {max(0, card.total - card.active)} done"
    return [_title_row(card.title, selected), (origin, "dim"), (counts, "ok" if card.active > 0 else "dim")]


def render_sessions(snapshot: Snapshot, width: int, selected: int) -> list[Line]:
    """Render session cards, preserving ``snapshot.sessions`` ordering.

    ``selected`` is the zero-based session card index; an empty snapshot has a
    single explanatory line.  Cards are separated by one blank row.
    """
    width = _width(width)
    if not snapshot.sessions:
        return [Line(truncate(" no sessions", width), "dim")]
    lines: list[Line] = []
    for index, card in enumerate(snapshot.sessions):
        if index:
            lines.append(Line(""))
        lines += _box(_session_rows(card, index == selected), width, index == selected, index)
    return lines


def _agent_rows(agent: AgentCard, inner: int, selected: bool, tick: int) -> list[tuple[str, str]]:
    """Return the content rows of one agent card, active or finished."""
    if agent.active:
        head = f"{spinner(tick)} {agent.summary}", "title"
    else:
        head = f"{STATUS_GLYPHS.get(agent.status, '?')} {agent.summary}", _status_style(agent.status)
    if selected:
        head = "▶ " + head[0], head[1]
    origin = " · ".join(part for part in (agent.runtime, agent.model, agent.effort) if part)
    rows = [head, (spread(origin, f"⏱ {format_elapsed(agent.elapsed_seconds)}", inner), "dim")]
    if agent.active and agent.last_event is not None:
        rows.append((f"↳ {agent.last_event}", "dim"))
    elif not agent.active and agent.failure_kind:
        rows.append((f"✘ {agent.failure_kind}", "err"))
    return rows


def _status_style(status: str) -> str:
    """Pick the accent style for a finished agent's headline."""
    if status == "succeeded":
        return "ok"
    if status in ("failed", "timed_out"):
        return "err"
    return "normal"


def render_agents(
    snapshot: Snapshot,
    session_id: str,
    width: int,
    selected: int,
    finished_expanded: bool,
    tick: int,
) -> list[Line]:
    """Render the RUNNING section and the collapsible FINISHED section.

    The selection index spans active cards followed by finished cards; the
    finished cards appear only when the caller has expanded that section.
    """
    width = _width(width)
    inner = max(1, width - 5)
    agents = snapshot.agents.get(session_id, ())
    active = [agent for agent in agents if agent.active]
    finished = [agent for agent in agents if not agent.active]
    lines = [Line(truncate(f" RUNNING ({len(active)})", width), "title")]
    if not active:
        lines.append(Line(truncate(" no running agents", width), "dim"))
    for index, agent in enumerate(active):
        if index:
            lines.append(Line(""))
        lines += _box(_agent_rows(agent, inner, index == selected, tick), width, index == selected, index)
    lines.append(Line(""))
    toggle = "▾ tab to collapse" if finished_expanded else "▸ tab to expand"
    lines.append(Line(truncate(f" FINISHED ({len(finished)})  {toggle}", width), "title"))
    if finished_expanded:
        offset = len(active)
        for index, agent in enumerate(finished):
            if index:
                lines.append(Line(""))
            chosen = offset + index == selected
            lines += _box(_agent_rows(agent, inner, chosen, tick), width, chosen, offset + index)
    return lines
