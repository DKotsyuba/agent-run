"""Contract tests for pure terminal dashboard rendering."""

from __future__ import annotations

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_run_tui.model import AgentCard, SessionCard, Snapshot
from agent_run_tui.render import render_agents, render_sessions


class RenderTests(unittest.TestCase):
    """Exercise renderer ordering, selection, width, and spinner contracts."""

    def setUp(self) -> None:
        """Create a snapshot with both active and finished card states."""
        session = SessionCard("one", "socket", "x", "A very useful session", "/tmp/project", 1, 2, 100.0)
        active = AgentCard("a", "codex", "gpt", "high", "running", True, "Active task", 90.0, None, 10, "long event")
        finished = AgentCard("b", "claude", None, None, "failed", False, "Finished task", 70.0, 80.0, 10, None, "timeout")
        self.snapshot = Snapshot(100.0, (session,), {"one": (active, finished)})

    def test_widths_are_bounded(self) -> None:
        """Every returned line fits each supported side-pane width."""
        for width in (36, 60, 120):
            for lines in (render_sessions(self.snapshot, width, 0), render_agents(self.snapshot, "one", width, 0, True, 0)):
                self.assertTrue(all(len(line) <= width for line in lines))

    def test_agent_order_and_expansion(self) -> None:
        """Active cards precede the divider and finished cards require expansion."""
        collapsed = render_agents(self.snapshot, "one", 60, 0, False, 0)
        expanded = render_agents(self.snapshot, "one", 60, 0, True, 0)
        divider = next(index for index, line in enumerate(collapsed) if "finished" in line)
        self.assertLess(next(index for index, line in enumerate(collapsed) if "Active task" in line), divider)
        self.assertFalse(any("Finished task" in line for line in collapsed))
        self.assertTrue(any("Finished task" in line for line in expanded))

    def test_spinner_and_selection_change(self) -> None:
        """Ticks advance active spinner frames and selection marks its card."""
        first = render_agents(self.snapshot, "one", 60, 0, False, 0)
        second = render_agents(self.snapshot, "one", 60, 0, False, 1)
        self.assertNotEqual(first[2], second[2])
        self.assertTrue(render_sessions(self.snapshot, 60, 0)[0].startswith("> "))

    def test_empty_snapshot_explains_itself(self) -> None:
        """No sessions receives an explicit empty-state line."""
        self.assertIn("no sessions", render_sessions(Snapshot(0.0), 36, 0)[0])
