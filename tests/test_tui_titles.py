"""Tests for local TUI session title resolution."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_run_tui.titles import TitleResolver


class TitleResolverTests(unittest.TestCase):
    """Check Claude/Codex metadata parsing, id validation, and incremental reads."""

    def test_claude_titles_history_and_incremental_reads(self) -> None:
        """The last custom title and cwd win; growth parses only appended bytes."""
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory) / "claude"
            path = home / "projects" / "slug" / "abcdefgh.jsonl"
            path.parent.mkdir(parents=True)
            path.write_text('{bad}\n{"cwd":"/work"}\n'
                            '{"type":"custom-title","customTitle":"old"}\n'
                            '{"type":"custom-title","customTitle":"new"}\n')
            resolver = TitleResolver(claude_home=home)
            self.assertEqual(resolver.resolve("claude_uds", "abcdefgh"), ("new", "/work"))
            # The malformed first line is skipped, leaving three parsed records.
            offset, parsed = resolver._positions[str(path)][0], resolver._records_by_path[str(path)]
            self.assertEqual((offset, len(parsed)), (path.stat().st_size, 3))

            with path.open("a", encoding="utf-8") as stream:
                stream.write('{"type":"custom-title","customTitle":"newest"}\n')
            self.assertEqual(resolver.resolve("claude_uds", "abcdefgh"), ("newest", "/work"))
            self.assertEqual(resolver._positions[str(path)][0], path.stat().st_size)
            self.assertGreater(resolver._positions[str(path)][0], offset)
            self.assertEqual(len(resolver._records_by_path[str(path)]), 4)

            (home / "history.jsonl").write_text('{"sessionId":"other","display":"no"}\n'
                                                '{"sessionId":"history1","display":"shown"}\n')
            self.assertEqual(resolver.resolve("claude_uds", "history1"), ("shown", None))

    def test_unsafe_session_ids_never_touch_the_filesystem(self) -> None:
        """A rejected id falls back to a short title without any path access."""
        resolver = TitleResolver(claude_home=Path("/nonexistent/claude"),
                                 codex_home=Path("/nonexistent/codex"))
        with patch.object(Path, "glob", side_effect=AssertionError("filesystem was read")):
            for session_id, title in (("a**b", "a**b"), ("../../etc/x", "../../et"),
                                      ("", "unbound")):
                for transport in ("claude_uds", "codex_queue", "unknown"):
                    with self.subTest(session_id=session_id, transport=transport):
                        self.assertEqual(resolver.resolve(transport, session_id), (title, None))
        self.assertEqual(resolver._positions, {})

    def test_codex_and_unknown_fallbacks(self) -> None:
        """Codex index and session metadata supply title/cwd; malformed data is ignored."""
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory) / "codex"
            home.mkdir()
            (home / "session_index.jsonl").write_text('{bad}\n' + json.dumps({"id": "abcdefgh", "thread_name": "Thread", "updated_at": 1}) + "\n")
            rollout = home / "sessions" / "x" / "rollout-x-abcdefgh.jsonl"
            rollout.parent.mkdir(parents=True)
            rollout.write_text(json.dumps({"type": "session_meta", "payload": {"cwd": "/codex"}}) + "\n")
            resolver = TitleResolver(codex_home=home)
            self.assertEqual(resolver.resolve("codex_queue", "abcdefgh"), ("Thread", "/codex"))
            self.assertEqual(resolver.resolve("unknown", "123456789"), ("12345678", None))


if __name__ == "__main__":
    unittest.main()
