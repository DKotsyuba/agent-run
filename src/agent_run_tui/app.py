"""Curses interaction loop for the isolated agent-run dashboard."""

from __future__ import annotations

import curses
import threading
import time
from collections.abc import Callable, Iterable

from .model import AgentCard, Snapshot
from .render import MAX_WIDTH, Line, display_width, host_runtime, render_agents, render_header, render_sessions, truncate


_KEY_CODES: dict[str, set[int]] = {
    "quit": {ord(char) for char in "qQйЙ"},
    "reload": {ord(char) for char in "rRкК"},
    "back": {ord(char) for char in "hHрР"} | {curses.KEY_LEFT, curses.KEY_BACKSPACE, 8, 127, 27},
    "down": {ord(char) for char in "jJоО"} | {curses.KEY_DOWN},
    "up": {ord(char) for char in "kKлЛ"} | {curses.KEY_UP},
    "open": {ord(char) for char in "lLдД"} | {curses.KEY_RIGHT, curses.KEY_ENTER, 10, 13},
    "toggle": {9, ord(" ")},
    "resize": {getattr(curses, "KEY_RESIZE", -2)},
    "mouse": {curses.KEY_MOUSE},
}
_ACTIONS = {code: action for action, codes in _KEY_CODES.items() for code in codes}
#: Attributes usable before curses is initialised (and in tests).
_PLAIN_STYLES = {"normal": 0, "selected": curses.A_BOLD, "title": curses.A_BOLD, "dim": curses.A_DIM, "ok": 0, "err": 0}


def normalize_key(key: int | str) -> str:
    """Map a raw ``get_wch``/``getch`` value to a dashboard action name.

    Latin and Russian-layout letters share actions (``q``/``й`` quit,
    ``r``/``к`` reload, ``h``/``р`` back, ``j``/``о`` down, ``k``/``л`` up,
    ``l``/``д`` open); unknown keys map to ``"none"``.
    """
    if isinstance(key, str):
        key = ord(key) if len(key) == 1 else -1
    return _ACTIONS.get(key, "none")


def init_styles() -> dict[str, int]:
    """Return the style-to-attribute map, adding colour accents when supported.

    Colours use the terminal's default background (``use_default_colors``) so
    nothing is ever filled; text colour stays the terminal's own except for
    the green/red accents.
    """
    styles = dict(_PLAIN_STYLES)
    try:
        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_GREEN, -1)
            curses.init_pair(2, curses.COLOR_RED, -1)
            styles["ok"] = curses.color_pair(1)
            styles["err"] = curses.color_pair(2)
    except curses.error:
        pass
    return styles


def read_key(stdscr: curses.window) -> int | str:
    """Read one key as a wide character, falling back to ``getch`` codes.

    ``get_wch`` raises ``curses.error`` when the poll times out, so the
    fallback ``getch`` runs without waiting and yields ``-1`` in that case.
    """
    try:
        return stdscr.get_wch()
    except curses.error:
        stdscr.timeout(0)
        return stdscr.getch()


def _position(ids: Iterable[str], wanted: str | None, fallback: int) -> int:
    """Return the index of ``wanted`` in ``ids``, or ``fallback`` when absent."""
    for index, candidate in enumerate(ids):
        if candidate == wanted:
            return index
    return fallback


class SnapshotWorker:
    """Run ``loader`` on a daemon thread so the curses loop never waits on I/O.

    The thread loads, publishes the result under a lock, then sleeps
    ``refresh_seconds`` on an event that :meth:`request` sets to reload early.
    A failing load keeps the previous snapshot and records the error text.
    ``updated`` is set after every attempt so callers (and tests) can wait
    deterministically instead of polling.
    """

    def __init__(self, loader: Callable[[float], Snapshot], refresh_seconds: float, clock: Callable[[], float] = time.time) -> None:
        """Bind the loader and interval without starting the thread."""
        self.loader = loader
        self.refresh_seconds = refresh_seconds
        self.clock = clock
        self.busy = False
        self.updated = threading.Event()
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stopped = False
        self._snapshot: Snapshot | None = None
        self._error: str | None = None
        self._observed_at = 0.0
        self._thread = threading.Thread(target=self._loop, name="agent-run-tui-loader", daemon=True)

    def start(self) -> None:
        """Start the loader thread; the first load begins immediately."""
        self._thread.start()

    def request(self) -> None:
        """Ask for a reload now instead of after the remaining interval."""
        self._wake.set()

    def stop(self) -> None:
        """Stop the loop and wait briefly for the thread; a stuck load is abandoned."""
        self._stopped = True
        self._wake.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def latest(self) -> tuple[Snapshot | None, str | None, float]:
        """Return ``(snapshot, error, observed_at)`` of the last attempt without blocking."""
        with self._lock:
            return self._snapshot, self._error, self._observed_at

    def _loop(self) -> None:
        """Load, publish, then wait for the interval or an early request, until stopped."""
        while not self._stopped:
            self._wake.clear()
            self.busy = True
            now = self.clock()
            snapshot: Snapshot | None = None
            error: str | None = None
            try:
                snapshot = self.loader(now)
            except Exception as exc:  # The injected loader is an external boundary.
                error = str(exc) or exc.__class__.__name__
            with self._lock:
                if snapshot is not None:
                    self._snapshot = snapshot
                self._error = error
                self._observed_at = now
            self.busy = False
            self.updated.set()
            if not self._stopped:
                self._wake.wait(self.refresh_seconds)


class Dashboard:
    """Display snapshots supplied by ``loader`` in a small curses dashboard.

    ``loader`` receives epoch seconds and may raise; failures retain the prior
    snapshot and are displayed in the status row.  The loader runs only on a
    :class:`SnapshotWorker` thread every ``refresh_seconds``; the key loop
    polls at ``spinner_seconds`` and never blocks on it.  Selection follows the
    session/agent id across snapshots, not the row position.
    """

    def __init__(
        self,
        loader: Callable[[float], Snapshot],
        refresh_seconds: float = 2.0,
        spinner_seconds: float = 0.25,
    ) -> None:
        """Store loader and redraw intervals without performing any I/O."""
        self.loader = loader
        self.refresh_seconds = max(0.05, refresh_seconds)
        self.spinner_seconds = max(0.05, spinner_seconds)
        self.snapshot = Snapshot(observed_at=0.0)
        self.worker: SnapshotWorker | None = None
        self.screen = "sessions"
        self.session_index = 0
        self.agent_index = 0
        self.session_id: str | None = None
        self.agent_id: str | None = None
        self.finished_expanded = False
        self.scroll = 0
        self.card_rows: dict[int, int] = {}
        self.error: str | None = None
        self.tick = 0
        self.styles = dict(_PLAIN_STYLES)

    def run(self, stdscr: curses.window) -> None:
        """Run until quit, handling keyboard, mouse, reload, and resize events."""
        curses.curs_set(0)
        curses.mousemask(curses.ALL_MOUSE_EVENTS)
        stdscr.keypad(True)
        self.styles = init_styles()
        self.worker = SnapshotWorker(self.loader, self.refresh_seconds)
        self.worker.start()
        try:
            while True:
                self._pull()
                self._draw(stdscr)
                stdscr.timeout(max(1, int(self.spinner_seconds * 1000)))
                key = read_key(stdscr)
                if key == -1:
                    if any(agent.active for cards in self.snapshot.agents.values() for agent in cards):
                        self.tick += 1
                    continue
                if self._handle_key(key):
                    return
        finally:
            self.worker.stop()

    def _pull(self) -> None:
        """Adopt the worker's newest snapshot and error without waiting."""
        if self.worker is None:
            return
        snapshot, self.error, _ = self.worker.latest()
        if snapshot is not None and snapshot is not self.snapshot:
            self._apply(snapshot)

    def _apply(self, snapshot: Snapshot) -> None:
        """Swap in ``snapshot`` and re-find the selected session and agent by id."""
        self.snapshot = snapshot
        self.session_index = _position((card.session_id for card in snapshot.sessions), self.session_id, self.session_index)
        self._clamp_selection()
        self.agent_index = _position((card.agent_id for card in self._visible_agents()), self.agent_id, self.agent_index)

    def _remember_selection(self) -> None:
        """Record the ids under the cursor so later snapshots can follow them."""
        self.session_id = self._selected_session_id()
        visible = self._visible_agents()
        self.agent_id = visible[self.agent_index].agent_id if visible else None

    def _selected_session_id(self) -> str | None:
        """Return the selected session id, or ``None`` when no card exists."""
        if not self.snapshot.sessions:
            return None
        return self.snapshot.sessions[self.session_index].session_id

    def _visible_agents(self) -> tuple[AgentCard, ...]:
        """Return selectable cards of the selected session: active ones first, finished only when expanded."""
        session_id = self._selected_session_id()
        cards = self.snapshot.agents.get(session_id, ()) if session_id is not None else ()
        return cards if self.finished_expanded else tuple(card for card in cards if card.active)

    def _agent_count(self) -> int:
        """Return selectable cards for the current agent view."""
        return len(self._visible_agents())

    def _clamp_selection(self) -> None:
        """Keep indices valid after refreshes and section toggles."""
        sessions = len(self.snapshot.sessions)
        self.session_index = min(self.session_index, max(0, sessions - 1))
        self.agent_index = min(self.agent_index, max(0, self._agent_count() - 1))

    def _move(self, direction: int) -> None:
        """Move the current selection by ``direction`` within its visible cards."""
        count = len(self.snapshot.sessions) if self.screen == "sessions" else self._agent_count()
        if count:
            if self.screen == "sessions":
                self.session_index = min(max(0, self.session_index + direction), count - 1)
            else:
                self.agent_index = min(max(0, self.agent_index + direction), count - 1)

    def _handle_key(self, key: int | str) -> bool:
        """Apply a key event on either layout and return whether to exit."""
        action = normalize_key(key)
        if action == "quit":
            return True
        if action == "reload":
            if self.worker is not None:
                self.worker.request()
        elif action == "down":
            self._move(1)
        elif action == "up":
            self._move(-1)
        elif action == "toggle" and self.screen == "agents":
            self.finished_expanded = not self.finished_expanded
            self._clamp_selection()
        elif action == "open" and self.screen == "sessions":
            if self._selected_session_id() is not None:
                self._open_session()
        elif action == "back" and self.screen == "agents":
            self.screen = "sessions"
            self.scroll = 0
            self._set_focus(None)
        elif action == "mouse":
            self._handle_mouse()
        self._remember_selection()
        return False

    def _open_session(self) -> None:
        """Switch to the agents screen of the selected session and focus the loader on it."""
        self.screen = "agents"
        self.agent_index = 0
        self.finished_expanded = False
        self.scroll = 0
        self._set_focus(self._selected_session_id())
        if self.worker is not None:
            self.worker.request()

    def _set_focus(self, session_id: str | None) -> None:
        """Tell a focusable loader which session's finished agents to read; plain loaders ignore it."""
        set_focus = getattr(self.loader, "set_focus", None)
        if set_focus is not None:
            set_focus(session_id)

    def _handle_mouse(self) -> None:
        """Select the clicked card and open a clicked session card when applicable."""
        try:
            _, mouse_y, _, _, _ = curses.getmouse()
        except curses.error:
            return
        index = self.card_rows.get(mouse_y)
        if index is None:
            return
        if self.screen == "sessions":
            self.session_index = index
            self._open_session()
        else:
            self.agent_index = index

    def _lines(self, width: int) -> tuple[str, str, list[Line], int]:
        """Return header title, key hint, body lines and the selected card index."""
        loading = "⟳ " if self.worker is not None and self.worker.busy else ""
        if self.screen == "sessions":
            lines = render_sessions(self.snapshot, width, self.session_index)
            return "agent-run · sessions", loading + "q quit · r reload", lines, self.session_index
        session = self.snapshot.sessions[self.session_index] if self.snapshot.sessions else None
        session_id = session.session_id if session else ""
        left = f"◀ {session.title} · {host_runtime(session.transport)}" if session else "◀ agents"
        lines = render_agents(self.snapshot, session_id, width, self.agent_index, self.finished_expanded, self.tick)
        return left, loading + "h back · tab finished · r reload · q quit", lines, self.agent_index

    def _draw(self, stdscr: curses.window) -> None:
        """Draw the current screen, keeping the selected card inside the viewport.

        Row 0 is the header, row 1 blank, the last row the status line; body
        rows in between scroll.  ``card_rows`` maps drawn rows to card indices
        for mouse clicks.
        """
        height, width = stdscr.getmaxyx()
        content_width = min(width, MAX_WIDTH)
        stdscr.erase()
        if height < 4 or content_width < 1:
            stdscr.refresh()
            return
        left, hint, lines, selected = self._lines(content_width)
        rows = [index for index, line in enumerate(lines) if line.card == selected]
        first, span = (rows[0], len(rows)) if rows else (0, 1)
        available = height - 3
        self.scroll = self._visible_scroll(first, span, available, len(lines))
        self.card_rows = {}
        left, hint = render_header(left, hint, content_width)
        self._add(stdscr, 0, 0, left, self.styles["title"])
        if hint:
            self._add(stdscr, 0, content_width - display_width(hint), hint, self.styles["dim"])
        for source_row in range(self.scroll, min(len(lines), self.scroll + available)):
            row = source_row - self.scroll + 2
            line = lines[source_row]
            if line.card is not None:
                self.card_rows[row] = line.card
            self._add(stdscr, row, 0, line.text, self.styles.get(line.style, 0))
        if self.error:
            self._add(stdscr, height - 1, 0, truncate(f"✘ {self.error}", content_width), self.styles["err"])
        stdscr.refresh()

    def _visible_scroll(self, selected: int, card_height: int, available: int, total: int) -> int:
        """Return a scroll offset that keeps a selected card fully visible."""
        if selected < self.scroll:
            return selected
        if selected + card_height > self.scroll + available:
            return min(selected + card_height - available, max(0, total - available))
        return min(self.scroll, max(0, total - available))

    def _add(self, stdscr: curses.window, row: int, column: int, text: str, attr: int = 0) -> None:
        """Write safely to a curses row, ignoring terminal edge clipping errors."""
        try:
            stdscr.addstr(row, column, text, attr)
        except curses.error:
            pass
