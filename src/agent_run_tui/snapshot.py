"""Dashboard snapshot construction from JSON-RPC views."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from .api import ApiClient, default_socket_path
from .model import AgentCard, SessionCard, Snapshot
from .titles import TitleResolver

_ACTIVE = {"created", "starting", "running", "cancelling"}


class SnapshotState:
    """Mutable refresh cache holding active transcript cursors and last events."""

    def __init__(self) -> None:
        """Start with no observed transcript positions or event summaries."""
        self.cursors: dict[str, Any] = {}
        self.events: dict[str, str | None] = {}


def _event(messages: list[dict[str, Any]], previous: str | None) -> str | None:
    """Return the newest displayable assistant or tool-call event from messages."""
    for message in reversed(messages):
        content = " ".join(str(message.get("content", "")).split())
        if message.get("role") == "tool_call":
            return f"{message.get('name', '')}: {content[:60]}"
        if message.get("role") == "assistant":
            return content[:80]
    return previous


def _card(item: dict[str, Any], now: float, event: str | None) -> AgentCard:
    """Translate an API agent view into the immutable screen-card contract."""
    active = item.get("status") in _ACTIVE
    ended = item.get("finished_at")
    started = item.get("started_at")
    elapsed_end = now if active else ended
    elapsed = None if started is None or elapsed_end is None else max(0.0, elapsed_end - started)
    summary = item.get("task_summary") or ""
    return AgentCard(item["agent_id"], item.get("runtime", ""), item.get("model"), item.get("effort"),
                     item.get("status", ""), active, str(summary).split("\n", 1)[0], started, ended,
                     elapsed, event, item.get("failure_kind"))


def load_snapshot(now: float, *, client: ApiClient, titles: TitleResolver, finished_limit: int = 50,
                  state: SnapshotState | None = None) -> Snapshot:
    """Load cards at ``now`` with one unfiltered agent sweep capped at 1,000.

    Agent pages contain at most 200 items and are followed through
    ``next_offset`` until complete.  The hard cap prevents an unbounded UI
    refresh; agents are grouped locally by delivery session, including a
    synthetic ``unbound`` group only when it has agents.  Active transcripts
    alone are fetched and resume at the final observed sequence number.
    """
    state = state or SnapshotState()
    listing = client.call("list_orchestrators", {"limit": 200})
    sessions: list[SessionCard] = []
    for item in listing.get("items", []):
        session_id = item.get("session_id")
        if not session_id:
            continue
        transport, external_id = item.get("transport", ""), item.get("external_session_id", "")
        title, cwd = ("unbound", None) if session_id == "unbound" else titles.resolve(transport, external_id)
        sessions.append(SessionCard(session_id, transport, external_id, title, cwd, item.get("active", 0), item.get("total", 0), item.get("last_seen_at", 0.0)))
    sessions.sort(key=lambda card: (-card.active, -card.last_seen_at))
    items: list[dict[str, Any]] = []
    offset = 0
    while len(items) < 1000:
        reply = client.call("list_agents", {"offset": offset, "limit": min(200, 1000 - len(items))})
        page = reply.get("items", [])
        items.extend(page[:1000 - len(items)])
        if reply.get("complete"):
            break
        next_offset = reply.get("next_offset")
        if not isinstance(next_offset, int) or next_offset <= offset:
            break
        offset = next_offset
    raw_groups: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        delivery = item.get("delivery")
        session_id = delivery.get("orchestrator_session_id") if isinstance(delivery, dict) else None
        raw_groups.setdefault(session_id or "unbound", []).append(item)
    if "unbound" in raw_groups and not any(card.session_id == "unbound" for card in sessions):
        unbound = raw_groups["unbound"]
        sessions.append(SessionCard("unbound", "", "", "unbound", None,
                                    sum(item.get("status") in _ACTIVE for item in unbound), len(unbound), 0.0))
        sessions.sort(key=lambda card: (-card.active, -card.last_seen_at))
    grouped: dict[str, tuple[AgentCard, ...]] = {}
    for session_id, group_items in raw_groups.items():
        cards = []
        for item in group_items:
            agent_id = item["agent_id"]
            event = state.events.get(agent_id)
            if item.get("status") in _ACTIVE:
                cursor = state.cursors.get(agent_id, 0)
                reply = client.call("transcript", {"agent_id": agent_id, "cursor": cursor, "limit": 200})
                messages = reply.get("messages", [])
                event = _event(messages, event)
                # The API answers ``next_cursor: null`` once the page is complete, even when it
                # carried messages; resume from the last seen ``seq`` so nothing is re-fetched.
                next_cursor = reply.get("next_cursor")
                if next_cursor is None:
                    next_cursor = messages[-1]["seq"] if messages else cursor
                state.events[agent_id], state.cursors[agent_id] = event, next_cursor
            cards.append(_card(item, now, event))
        active = sorted((card for card in cards if card.active), key=lambda card: -(card.started_at or 0))
        finished = sorted((card for card in cards if not card.active), key=lambda card: -(card.finished_at or 0))[:finished_limit]
        grouped[session_id] = tuple(active + finished)
    return Snapshot(now, tuple(sessions), grouped)


def make_loader(socket_path: Path | None = None, **kw: Any) -> Callable[[float], Snapshot]:
    """Build reusable dependencies and return the app's ``loader(now)``.

    A supplied ``client`` is retained for tests or alternate transports;
    otherwise a socket client is made once for the loader lifetime.
    """
    client = kw.pop("client", ApiClient(socket_path or default_socket_path()))
    titles = kw.pop("titles", TitleResolver())
    state = kw.pop("state", SnapshotState())
    return lambda now: load_snapshot(now, client=client, titles=titles, state=state, **kw)
