"""Curses interaction loop for the isolated agent-run dashboard."""

from __future__ import annotations

import curses
import time
from collections.abc import Callable

from .model import Snapshot
from .render import MAX_WIDTH, render_agents, render_sessions, truncate


class Dashboard:
    """Display snapshots supplied by ``loader`` in a small curses dashboard.

    ``loader`` receives epoch seconds and may raise; failures retain the prior
    snapshot and are displayed in the status row.  Refresh and spinner periods
    are seconds and are clamped to a small positive timeout for curses polling.
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
        self.screen = "sessions"
        self.session_index = 0
        self.agent_index = 0
        self.finished_expanded = False
        self.scroll = 0
        self.card_rows: dict[int, int] = {}
        self.error: str | None = None
        self.tick = 0

    def run(self, stdscr: curses.window) -> None:
        """Run until quit, handling keyboard, mouse, reload, and resize events."""
        curses.curs_set(0)
        curses.mousemask(curses.ALL_MOUSE_EVENTS)
        stdscr.keypad(True)
        last_reload = 0.0
        while True:
            now = time.time()
            if now - last_reload >= self.refresh_seconds:
                self._reload(now)
                last_reload = now
            self._draw(stdscr)
            active = any(agent.active for cards in self.snapshot.agents.values() for agent in cards)
            timeout = self.spinner_seconds if active else self.refresh_seconds
            stdscr.timeout(max(1, int(timeout * 1000)))
            key = stdscr.getch()
            if key == -1:
                if active:
                    self.tick += 1
                continue
            if self._handle_key(key):
                return

    def _reload(self, now: float) -> None:
        """Refresh the snapshot, retaining the last usable one on loader failure."""
        try:
            self.snapshot = self.loader(now)
            self.error = None
        except Exception as exc:  # The injected loader is an external boundary.
            self.error = str(exc) or exc.__class__.__name__
        self._clamp_selection()

    def _selected_session_id(self) -> str | None:
        """Return the selected session id, or ``None`` when no card exists."""
        if not self.snapshot.sessions:
            return None
        return self.snapshot.sessions[self.session_index].session_id

    def _agent_count(self) -> int:
        """Return selectable cards for the current agent view."""
        session_id = self._selected_session_id()
        if session_id is None:
            return 0
        cards = self.snapshot.agents.get(session_id, ())
        return sum(not card.active for card in cards) + sum(card.active for card in cards) if self.finished_expanded else sum(card.active for card in cards)

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

    def _handle_key(self, key: int) -> bool:
        """Apply a curses key event and return whether the dashboard should exit."""
        if key in (ord("q"), ord("Q")):
            return True
        if key in (ord("r"), ord("R")):
            self._reload(time.time())
        elif key in (ord("j"), curses.KEY_DOWN):
            self._move(1)
        elif key in (ord("k"), curses.KEY_UP):
            self._move(-1)
        elif key in (9, ord(" ")) and self.screen == "agents":
            self.finished_expanded = not self.finished_expanded
            self._clamp_selection()
        elif key in (10, 13, curses.KEY_ENTER) and self.screen == "sessions":
            if self._selected_session_id() is not None:
                self.screen = "agents"
                self.agent_index = 0
                self.finished_expanded = False
                self.scroll = 0
        elif key in (27, curses.KEY_BACKSPACE, 127, ord("h")) and self.screen == "agents":
            self.screen = "sessions"
            self.scroll = 0
        elif key == curses.KEY_MOUSE:
            self._handle_mouse()
        return False

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
            self.screen = "agents"
            self.agent_index = 0
            self.finished_expanded = False
        else:
            self.agent_index = index

    def _draw(self, stdscr: curses.window) -> None:
        """Draw the current screen, keeping selection rows inside the viewport."""
        height, width = stdscr.getmaxyx()
        content_width = min(width, MAX_WIDTH)
        stdscr.erase()
        if height < 2 or content_width < 1:
            stdscr.refresh()
            return
        if self.screen == "sessions":
            title = "agent-run sessions · q quit · r reload"
            lines = render_sessions(self.snapshot, content_width, self.session_index)
            selected = self.session_index * 2
            card_height = 2
        else:
            session = self.snapshot.sessions[self.session_index] if self.snapshot.sessions else None
            title = f"{session.title if session else 'agents'} · h back · q quit · r reload"
            lines = render_agents(self.snapshot, session.session_id if session else "", content_width, self.agent_index, self.finished_expanded, self.tick)
            active = sum(card.active for card in self.snapshot.agents.get(session.session_id if session else "", ()))
            selected = self.agent_index * 3 if self.agent_index < active else active * 3 + 1 + (self.agent_index - active) * 3
            card_height = 3
        available = height - 2
        self.scroll = self._visible_scroll(selected, card_height, available, len(lines))
        self.card_rows = {}
        self._add(stdscr, 0, 0, title, curses.A_BOLD)
        for source_row in range(self.scroll, min(len(lines), self.scroll + available)):
            row = source_row - self.scroll + 1
            index = self._card_index(source_row, card_height)
            if index is not None:
                self.card_rows[row] = index
            attr = curses.A_REVERSE if index == (self.session_index if self.screen == "sessions" else self.agent_index) else 0
            if source_row % card_height == 0 and source_row != (active * 3 if self.screen == "agents" else -1):
                attr |= curses.A_BOLD
            self._add(stdscr, row, 0, lines[source_row], attr)
        self._add(stdscr, height - 1, 0, truncate(self.error or "", content_width), curses.A_REVERSE if self.error else 0)
        stdscr.refresh()

    def _visible_scroll(self, selected: int, card_height: int, available: int, total: int) -> int:
        """Return a scroll offset that keeps a selected card fully visible."""
        if selected < self.scroll:
            return selected
        if selected + card_height > self.scroll + available:
            return min(selected + card_height - available, max(0, total - available))
        return min(self.scroll, max(0, total - available))

    def _card_index(self, row: int, card_height: int) -> int | None:
        """Map a rendered content row to its selectable card index, if any."""
        if self.screen == "sessions":
            return row // card_height if row < len(self.snapshot.sessions) * card_height else None
        session_id = self._selected_session_id()
        cards = self.snapshot.agents.get(session_id or "", ())
        active = sum(card.active for card in cards)
        if row < active * 3:
            return row // 3
        if not self.finished_expanded or row <= active * 3:
            return None
        finished_row = row - active * 3 - 1
        return active + finished_row // 3

    def _add(self, stdscr: curses.window, row: int, column: int, text: str, attr: int = 0) -> None:
        """Write safely to a curses row, ignoring terminal edge clipping errors."""
        try:
            stdscr.addstr(row, column, text, attr)
        except curses.error:
            pass
