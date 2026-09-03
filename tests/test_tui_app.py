"""Tests for the pure selection and scrolling helpers of the dashboard loop."""

from __future__ import annotations

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_run_tui.app import Dashboard
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
        """Record ``text`` written at ``row``; ``column`` and ``attr`` are ignored."""
        self.rows[row] = text

    def refresh(self) -> None:
        """Accept the end-of-frame call without doing anything."""


def session(index: int) -> SessionCard:
    """Build one session card distinguishable by its zero-based ``index``."""
    return SessionCard(f"s{index}", "socket", f"x{index}", f"session {index}", None, 1, 1, float(index))


def dashboard(snapshot: Snapshot, **state: object) -> Dashboard:
    """Build a dashboard holding ``snapshot`` with the given attributes applied."""
    view = Dashboard(lambda now: snapshot)
    view.snapshot = snapshot
    for name, value in state.items():
        setattr(view, name, value)
    return view


class CardIndexTests(unittest.TestCase):
    """Check the row-to-card mapping on both screens."""

    def test_session_rows_map_two_at_a_time(self) -> None:
        """Each session owns two rows and rows past the last card select nothing."""
        view = dashboard(Snapshot(0.0, tuple(session(index) for index in range(3))))
        self.assertEqual([view._card_index(row, 2) for row in range(6)], [0, 0, 1, 1, 2, 2])
        self.assertIsNone(view._card_index(6, 2))

    def test_agent_rows_skip_the_finished_divider(self) -> None:
        """Finished cards are selectable only when expanded, never the divider row."""
        active = AgentCard("a", "codex", None, None, "running", True, "active", 1.0, None, 1.0, None)
        finished = AgentCard("b", "codex", None, None, "failed", False, "finished", 1.0, 2.0, 1.0, None)
        view = dashboard(Snapshot(0.0, (session(0),), {"s0": (active, finished)}), screen="agents")
        self.assertEqual([view._card_index(row, 3) for row in range(3)], [0, 0, 0])
        self.assertIsNone(view._card_index(3, 3))
        self.assertIsNone(view._card_index(4, 3))
        view.finished_expanded = True
        self.assertIsNone(view._card_index(3, 3))
        self.assertEqual([view._card_index(row, 3) for row in range(4, 7)], [1, 1, 1])


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
        self.assertEqual(view.scroll, 8)
        self.assertEqual(view.card_rows, {1: 4, 2: 4, 3: 5, 4: 5})


if __name__ == "__main__":
    unittest.main()
