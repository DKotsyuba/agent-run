"""Contract tests for pure terminal dashboard rendering."""

from __future__ import annotations

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_run_tui.model import AgentCard, SessionCard, Snapshot
from agent_run_tui.render import display_width, host_runtime, render_agents, render_header, render_sessions


HINT = "h back · tab finished · r reload · q quit"


class RenderTests(unittest.TestCase):
    """Exercise card framing, content, width, selection, and spinner contracts."""

    def setUp(self) -> None:
        """Create a snapshot with Cyrillic and emoji text plus every card state."""
        session = SessionCard("one", "claude_uds", "x", "Очень полезная сессия с длинным названием", "/tmp/project", 1, 2, 100.0)
        idle = SessionCard("two", "codex_queue", "y", "Idle", None, 0, 3, 50.0)
        active = AgentCard("a", "codex", "gpt", "high", "running", True, "Active task 🚀 с кириллицей и длинным хвостом", 90.0, None, 3725, "long event " * 10)
        failed = AgentCard("b", "claude", None, None, "failed", False, "Finished task", 70.0, 80.0, 10, None, "timeout")
        timed = AgentCard("c", "claude", None, None, "timed_out", False, "Slow task", 70.0, 80.0, 10, None)
        self.snapshot = Snapshot(100.0, (session, idle), {"one": (active, failed, timed)})

    def test_widths_are_bounded(self) -> None:
        """Every row and header fits each supported width in terminal cells."""
        for width in (36, 60, 120):
            for lines in (render_sessions(self.snapshot, width, 0), render_agents(self.snapshot, "one", width, 0, True, 0)):
                for line in lines:
                    self.assertLessEqual(display_width(line.text), width, line.text)
            left, hint = render_header("◀ " + self.snapshot.sessions[0].title + " · claude", HINT, width)
            self.assertLessEqual(display_width(left) + 1 + display_width(hint), min(width, 72))

    def test_cards_are_boxed_and_separated(self) -> None:
        """Cards are five-row boxes of equal width with one blank row between them."""
        lines = render_sessions(self.snapshot, 60, 1)
        self.assertEqual([line.card for line in lines], [0] * 5 + [None] + [1] * 5)
        self.assertEqual(lines[5].text, "")
        for top, bottom in ((0, 4), (6, 10)):
            self.assertTrue(lines[top].text.startswith(" ┌") and lines[top].text.endswith("┐"))
            self.assertTrue(lines[bottom].text.startswith(" └") and lines[bottom].text.endswith("┘"))
            self.assertEqual(display_width(lines[top].text), 60)
        self.assertTrue(all(line.text.startswith(" │ ") and line.text.endswith(" │") for line in lines[1:4]))
        self.assertEqual((lines[0].style, lines[6].style), ("dim", "selected"))
        self.assertTrue(lines[7].text.startswith(" │ ▶ Idle"))
        self.assertNotIn("▶", lines[1].text)

    def test_session_card_content(self) -> None:
        """Rows show the host runtime with the cwd basename and the child counts."""
        lines = render_sessions(self.snapshot, 60, 0)
        self.assertIn("claude · project", lines[2].text)
        self.assertEqual(lines[2].style, "dim")
        self.assertIn("● 1 active   ○ 1 done", lines[3].text)
        self.assertEqual(lines[3].style, "ok")
        self.assertIn("codex", lines[8].text)
        self.assertIn("● 0 active   ○ 3 done", lines[9].text)
        self.assertEqual(lines[9].style, "dim")

    def test_host_runtime_names(self) -> None:
        """Transports map to host runtime names; unknown ones pass through."""
        self.assertEqual([host_runtime(t) for t in ("claude_uds", "codex_queue", "unbound", "", "stdio")], ["claude", "codex", "—", "—", "stdio"])

    def test_agent_sections_and_expansion(self) -> None:
        """RUNNING precedes FINISHED and finished cards require expansion."""
        collapsed = render_agents(self.snapshot, "one", 60, 0, False, 0)
        expanded = render_agents(self.snapshot, "one", 60, 2, True, 0)
        self.assertEqual((collapsed[0].text, collapsed[0].style), (" RUNNING (1)", "title"))
        self.assertIn("FINISHED (2)  ▸ tab to expand", collapsed[-1].text)
        self.assertTrue(any("FINISHED (2)  ▾ tab to collapse" in line.text for line in expanded))
        self.assertFalse(any("Finished task" in line.text for line in collapsed))
        self.assertEqual(sorted({line.card for line in collapsed if line.card is not None}), [0])
        self.assertEqual(sorted({line.card for line in expanded if line.card is not None}), [0, 1, 2])
        texts = [line.text for line in expanded]
        self.assertTrue(any("✘ Finished task" in text for text in texts))
        self.assertTrue(any("✘ timeout" in text for text in texts))
        self.assertTrue(any("▶ ⏰ Slow task" in text for text in texts))
        self.assertEqual([line.style for line in expanded if "✘ Finished task" in line.text], ["err"])

    def test_active_card_rows(self) -> None:
        """An active card shows spinner, origin with right-aligned elapsed, and the last event."""
        lines = render_agents(self.snapshot, "one", 60, 0, False, 0)
        self.assertTrue(lines[2].text.startswith(" │ ▶ ⠋ Active task 🚀"))
        self.assertIn("codex · gpt · high", lines[3].text)
        self.assertTrue(lines[3].text.endswith("⏱ 1h02m │"))
        self.assertTrue(lines[4].text.startswith(" │ ↳ long event"))
        quiet = AgentCard("q", "codex", None, None, "running", True, "Quiet", 1.0, None, 5, None)
        lines = render_agents(Snapshot(0.0, (), {"s": (quiet,)}), "s", 60, 0, False, 0)
        self.assertEqual([line.card for line in lines[:5]], [None, 0, 0, 0, 0])
        self.assertTrue(lines[4].text.startswith(" └"))

    def test_spinner_advances_with_ticks(self) -> None:
        """Ticks change the active card's spinner frame."""
        first = render_agents(self.snapshot, "one", 60, 0, False, 0)
        second = render_agents(self.snapshot, "one", 60, 0, False, 1)
        self.assertNotEqual(first[2], second[2])

    def test_narrow_width_truncates_but_keeps_elapsed(self) -> None:
        """At 36 columns the summary is cut with an ellipsis and elapsed stays."""
        lines = render_agents(self.snapshot, "one", 36, 0, False, 0)
        self.assertIn("…", lines[2].text)
        self.assertIn("⏱ 1h02m", lines[3].text)

    def test_header_drops_hint_parts_when_narrow(self) -> None:
        """Hint parts vanish from the right until the whole title fits."""
        left, hint = render_header("◀ Implement dashboard · claude", HINT, 36)
        self.assertEqual(hint, "h back")
        self.assertEqual(display_width(left), 29)
        self.assertTrue(left.endswith("…"))
        self.assertEqual(render_header("◀ Implement dashboard · claude", HINT, 44), ("◀ Implement dashboard · claude", "h back"))
        self.assertEqual(render_header("◀ Implement dashboard · claude", HINT, 80), ("◀ Implement dashboard · claude", HINT))
        self.assertEqual(render_header("agent-run · sessions", "q quit · r reload", 80), ("agent-run · sessions", "q quit · r reload"))

    def test_empty_states_explain_themselves(self) -> None:
        """No sessions and no running agents get explicit dim rows."""
        self.assertEqual((render_sessions(Snapshot(0.0), 36, 0)[0].text.strip(), render_sessions(Snapshot(0.0), 36, 0)[0].style), ("no sessions", "dim"))
        lines = render_agents(self.snapshot, "missing", 36, 0, False, 0)
        self.assertEqual((lines[1].text.strip(), lines[1].style), ("no running agents", "dim"))


if __name__ == "__main__":
    unittest.main()
