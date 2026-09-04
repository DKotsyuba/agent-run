"""Dashboard snapshot construction from JSON-RPC views."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from .api import ApiClient, default_socket_path
from .model import AgentCard, SessionCard, Snapshot
from .titles import TitleResolver

_ACTIVE = {"created", "starting", "running", "cancelling"}
#: Runtimes whose transcript only appears once the agent ends, so polling it is wasted.
_NO_STREAM = {"codex"}
_PAGE = 200
_CAP = 1000


class SnapshotState:
    """Mutable refresh cache: transcript cursors, last events, and finished pages.

    ``finished`` maps a session id to ``(fetched_at, items)``, the newest
    finished agents read while that session was focused.  ``focus`` is the
    session served by the previous refresh, so a focus change refetches at
    once; ``active_ids`` holds each session's active agents from the previous
    refresh, so an agent that just finished refreshes its session's page.
    """

    def __init__(self) -> None:
        """Start with no cursors, events, cached pages, or focus."""
        self.cursors: dict[str, Any] = {}
        self.events: dict[str, str | None] = {}
        self.finished: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self.focus: str | None = None
        self.active_ids: dict[str, set[str]] = {}


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


def _session_of(item: dict[str, Any]) -> str:
    """Return the delivery session id of an agent view, ``"unbound"`` without one."""
    delivery = item.get("delivery")
    session_id = delivery.get("orchestrator_session_id") if isinstance(delivery, dict) else None
    return session_id or "unbound"


def _sessions(listing: dict[str, Any], titles: TitleResolver) -> list[SessionCard]:
    """Build session cards from ``list_orchestrators``, keeping the null row as ``unbound``."""
    sessions: list[SessionCard] = []
    for item in listing.get("items", []):
        session_id = item.get("session_id") or "unbound"
        transport, external_id = item.get("transport", ""), item.get("external_session_id", "")
        if session_id == "unbound":
            if not item.get("total"):
                continue
            title, cwd = "unbound", None
        else:
            title, cwd = titles.resolve(transport, external_id)
        sessions.append(SessionCard(session_id, transport, external_id, title, cwd, item.get("active", 0),
                                    item.get("total", 0), item.get("last_seen_at", 0.0)))
    return sessions


def _list_active(client: ApiClient) -> list[dict[str, Any]]:
    """Read every active agent, following ``next_offset`` only until ``complete``, capped at 1,000."""
    items: list[dict[str, Any]] = []
    offset = 0
    while len(items) < _CAP:
        reply = client.call("list_agents", {"active": True, "offset": offset, "limit": min(_PAGE, _CAP - len(items))})
        page = reply.get("items", [])
        items.extend(page[:_CAP - len(items)])
        if reply.get("complete"):
            break
        next_offset = reply.get("next_offset")
        if not isinstance(next_offset, int) or next_offset <= offset:
            break
        offset = next_offset
    return items


def _fetch_finished(client: ApiClient, session: SessionCard, limit: int, active: int) -> list[dict[str, Any]]:
    """Return the newest finished agents of ``session`` from one ``list_agents`` page.

    Bound sessions are filtered by the server through their orchestrator
    reference; the page is widened by the session's ``active`` agents, which
    the server lists first.  The API cannot filter for "no orchestrator", so
    ``unbound`` reads one unfiltered page of 200 and keeps the unbound finished
    agents in it: older unbound agents beyond the newest 200 overall are not shown.
    """
    if session.session_id == "unbound":
        reply = client.call("list_agents", {"offset": 0, "limit": _PAGE})
        found = [item for item in reply.get("items", []) if _session_of(item) == "unbound"]
    else:
        ref = {"transport": session.transport, "external_session_id": session.external_session_id}
        params = {"orchestrator": ref, "offset": 0, "limit": min(_PAGE, limit + active)}
        found = client.call("list_agents", params).get("items", [])
    return [item for item in found if item.get("status") not in _ACTIVE]


def _observe(client: ApiClient, state: SnapshotState, item: dict[str, Any]) -> str | None:
    """Return the agent's newest event, polling the transcript only when it is active and streams."""
    agent_id = item["agent_id"]
    event = state.events.get(agent_id)
    if item.get("status") not in _ACTIVE or item.get("runtime") in _NO_STREAM:
        return event
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
    return event


def load_snapshot(now: float, *, client: ApiClient, titles: TitleResolver, finished_limit: int = 50,
                  finished_refresh_seconds: float = 15.0, state: SnapshotState | None = None,
                  focus: str | None = None) -> Snapshot:
    """Load cards at ``now`` from the session list, the active agents, and one focused finished page.

    Every refresh issues ``list_orchestrators`` and one active-only
    ``list_agents`` (paged only while incomplete), grouping agents locally by
    delivery session.  Finished agents are read only for the ``focus`` session,
    cached in ``state`` and refreshed after ``finished_refresh_seconds`` of
    ``now``, when the focus changes, or when one of its active agents has just
    finished.  Transcripts are read from the stored cursor for active agents of
    streaming runtimes only.
    """
    state = state or SnapshotState()
    sessions = _sessions(client.call("list_orchestrators", {"limit": 200}), titles)
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in _list_active(client):
        groups.setdefault(_session_of(item), []).append(item)
    if "unbound" in groups and all(card.session_id != "unbound" for card in sessions):
        count = len(groups["unbound"])
        sessions.append(SessionCard("unbound", "", "", "unbound", None, count, count, 0.0))
    sessions.sort(key=lambda card: (-card.active, -card.last_seen_at))
    active_ids = {session_id: {item["agent_id"] for item in items} for session_id, items in groups.items()}
    focused = next((card for card in sessions if card.session_id == focus), None)
    if focused is not None and focus is not None:
        cached = state.finished.get(focus)
        just_finished = not state.active_ids.get(focus, set()) <= active_ids.get(focus, set())
        if cached is None or focus != state.focus or now - cached[0] >= finished_refresh_seconds or just_finished:
            cached = (now, _fetch_finished(client, focused, finished_limit, len(active_ids.get(focus, ()))))
            state.finished[focus] = cached
        seen = active_ids.get(focus, set())
        groups.setdefault(focus, []).extend(item for item in cached[1] if item["agent_id"] not in seen)
    state.focus, state.active_ids = focus, active_ids
    grouped: dict[str, tuple[AgentCard, ...]] = {}
    for session_id, items in groups.items():
        cards = [_card(item, now, _observe(client, state, item)) for item in items]
        active = sorted((card for card in cards if card.active), key=lambda card: -(card.started_at or 0))
        finished = sorted((card for card in cards if not card.active), key=lambda card: -(card.finished_at or 0))[:finished_limit]
        grouped[session_id] = tuple(active + finished)
    return Snapshot(now, tuple(sessions), grouped)


class LoaderHandle:
    """The dashboard's ``loader(now)`` with a settable focused session.

    ``set_focus(session_id)`` names the session whose finished agents the next
    refresh reads; ``None`` reads none.  The focus is a plain attribute, so
    setting it from the key loop while the loader thread runs is safe.
    """

    def __init__(self, load: Callable[[float, str | None], Snapshot]) -> None:
        """Wrap ``load(now, focus)`` with no focused session."""
        self.focus: str | None = None
        self._load = load

    def set_focus(self, session_id: str | None) -> None:
        """Focus ``session_id`` (or nothing) for subsequent refreshes."""
        self.focus = session_id

    def __call__(self, now: float) -> Snapshot:
        """Load one snapshot at ``now`` for the current focus."""
        return self._load(now, self.focus)


def make_loader(socket_path: Path | None = None, **kw: Any) -> LoaderHandle:
    """Build reusable dependencies and return the app's focusable ``loader(now)``.

    A supplied ``client`` is retained for tests or alternate transports;
    otherwise a socket client is made once for the loader lifetime.
    """
    client = kw.pop("client", None) or ApiClient(socket_path or default_socket_path())
    titles = kw.pop("titles", None) or TitleResolver()
    state = kw.pop("state", None) or SnapshotState()
    return LoaderHandle(lambda now, focus: load_snapshot(now, client=client, titles=titles, state=state, focus=focus, **kw))
