"""Tests for the pure key, selection and scrolling helpers of the dashboard loop."""

from __future__ import annotations

import curses
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_run_tui.app import Dashboard, normalize_key
from agent_run_tui.model import AgentCard, SessionCard, Snapshot


class FakeScreen:
    """Stand in for a curses window, recording the text written to each row."""

    def __init__(self, height: int, width: int) -> None:
        """Report a fixed ``height`` by ``width`` viewport with no output yet."""
        self.height, self.width = height, width
        self.rows: dict[int, str] = {}

    def getmaxyx(self) -> tuple[int, int]:
        """Return the fixed terminal size as curses does."""
        return (self.height, self.width)

    def erase(self) -> None:
        """Drop every recorded row, as a redraw does."""
        self.rows.clear()

    def addstr(self, row: int, column: int, text: str, attr: int = 0) -> None:
        """Append ``text`` to ``row``; ``column`` and ``attr`` are ignored."""
        self.rows[row] = self.rows.get(row, "") + text

    def refresh(self) -> None:
        """Accept the end-of-frame call without doing anything."""


def session(index: int) -> SessionCard:
    """Build one session card distinguishable by its zero-based ``index``."""
    return SessionCard(f"s{index}", "claude_uds", f"x{index}", f"session {index}", None, 1, 1, float(index))


def dashboard(snapshot: Snapshot, **state: object) -> Dashboard:
    """Build a dashboard holding ``snapshot`` with the given attributes applied."""
    view = Dashboard(lambda now: snapshot)
    view.snapshot = snapshot
    for name, value in state.items():
        setattr(view, name, value)
    return view


ACTIVE = AgentCard("a", "codex", None, None, "running", True, "active", 1.0, None, 1.0, None)
FINISHED = AgentCard("b", "codex", None, None, "failed", False, "finished", 1.0, 2.0, 1.0, None)


class KeyTests(unittest.TestCase):
    """Check key normalisation on Latin and Russian layouts."""

    def test_latin_and_cyrillic_keys_share_actions(self) -> None:
        """Each action is reachable from Latin, Cyrillic, and special keys alike."""
        cases = [
            ("q", "quit"), ("Q", "quit"), ("й", "quit"), ("Й", "quit"), (ord("q"), "quit"),
            ("r", "reload"), ("к", "reload"),
            ("h", "back"), ("р", "back"), (curses.KEY_LEFT, "back"), (8, "back"), (127, "back"), ("\x1b", "back"), (curses.KEY_BACKSPACE, "back"),
            ("j", "down"), ("о", "down"), (curses.KEY_DOWN, "down"),
            ("k", "up"), ("л", "up"), (curses.KEY_UP, "up"),
            ("\n", "open"), (13, "open"), (curses.KEY_ENTER, "open"), ("l", "open"), ("д", "open"), (curses.KEY_RIGHT, "open"),
            ("\t", "toggle"), (" ", "toggle"),
            (curses.KEY_RESIZE, "resize"), (curses.KEY_MOUSE, "mouse"),
            ("x", "none"), ("ы", "none"), (-1, "none"), ("", "none"),
        ]
        for key, action in cases:
            self.assertEqual(normalize_key(key), action, repr(key))

    def test_russian_layout_drives_the_dashboard(self) -> None:
        """Cyrillic keys move, open, go back and quit exactly like Latin ones."""
        view = dashboard(Snapshot(0.0, (session(0), session(1)), {"s1": (ACTIVE, FINISHED)}))
        self.assertFalse(view._handle_key("о"))
        self.assertEqual(view.session_index, 1)
        view._handle_key("д")
        self.assertEqual(view.screen, "agents")
        view._handle_key("\t")
        self.assertTrue(view.finished_expanded)
        view._handle_key("р")
        self.assertEqual(view.screen, "sessions")
        self.assertTrue(view._handle_key("й"))


class CardRowTests(unittest.TestCase):
    """Check the drawn row-to-card mapping on both screens."""

    def test_session_rows_map_five_per_card(self) -> None:
        """Each session owns its five box rows; blank separators select nothing."""
        view = dashboard(Snapshot(0.0, tuple(session(index) for index in range(3))))
        screen = FakeScreen(40, 60)
        view._draw(screen)
        self.assertEqual([view.card_rows.get(row) for row in range(2, 19)], [0] * 5 + [None] + [1] * 5 + [None] + [2] * 5)
        self.assertNotIn(0, view.card_rows)
        self.assertIn("agent-run · sessions", screen.rows[0])
        self.assertIn("q quit · r reload", screen.rows[0])

    def test_agent_rows_skip_labels_and_collapsed_finished(self) -> None:
        """Section labels are never selectable and finished cards need expansion."""
        view = dashboard(Snapshot(0.0, (session(0),), {"s0": (ACTIVE, FINISHED)}), screen="agents")
        view._draw(FakeScreen(40, 60))
        self.assertEqual([view.card_rows.get(row) for row in range(2, 10)], [None, 0, 0, 0, 0, None, None, None])
        view.finished_expanded = True
        view._draw(FakeScreen(40, 60))
        self.assertEqual([view.card_rows.get(row) for row in range(8, 14)], [None, 1, 1, 1, 1, None])


class ScrollTests(unittest.TestCase):
    """Check the scroll offset and the drawn row-to-card mapping."""

    def test_visible_scroll_keeps_the_selected_card_in_view(self) -> None:
        """Scrolling follows the selection up and down and never exceeds the content."""
        view = dashboard(Snapshot(0.0), scroll=5)
        self.assertEqual(view._visible_scroll(2, 3, 6, 30), 2)
        view.scroll = 0
        self.assertEqual(view._visible_scroll(0, 3, 6, 30), 0)
        self.assertEqual(view._visible_scroll(27, 3, 6, 30), 24)
        view.scroll = 25
        self.assertEqual(view._visible_scroll(0, 3, 6, 30), 0)
        self.assertEqual(view._visible_scroll(27, 3, 6, 30), 24)

    def test_drawn_rows_map_to_cards_after_scrolling(self) -> None:
        """A selection below the viewport scrolls and remaps the clickable rows."""
        snapshot = Snapshot(0.0, tuple(session(index) for index in range(6)))
        view = dashboard(snapshot, session_index=5)
        view._draw(FakeScreen(6, 40))
        self.assertEqual(view.scroll, 32)
        self.assertEqual(view.card_rows, {2: 5, 3: 5, 4: 5})


if __name__ == "__main__":
    unittest.main()
