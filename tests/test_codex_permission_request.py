"""Security-boundary tests for the Codex PermissionRequest MCP allow gate."""

from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.codex.permission_request import allows, main


class PermissionRequestTests(unittest.TestCase):
    """Prove the hook allows only configured MCP namespaces."""

    def test_allows_hyphen_and_underscore_namespace_variants(self) -> None:
        """Codex namespace normalization must not change the trust decision."""

        trusted = frozenset({"agent-run", "agent-ide"})
        for tool_name in (
            "mcp__agent-run__start",
            "mcp__agent_run__status",
            "mcp__agent-ide__context",
            "mcp__agent_ide__diff",
        ):
            with self.subTest(tool_name=tool_name):
                self.assertTrue(
                    allows(
                        {
                            "hook_event_name": "PermissionRequest",
                            "tool_name": tool_name,
                        },
                        trusted,
                    )
                )

    def test_declines_shell_unknown_and_malformed_requests(self) -> None:
        """Anything outside the exact MCP allowlist keeps normal review."""

        trusted = frozenset({"agent-run"})
        cases = (
            {"hook_event_name": "PermissionRequest", "tool_name": "Bash"},
            {"hook_event_name": "PermissionRequest", "tool_name": "mcp__github__push"},
            {"hook_event_name": "PreToolUse", "tool_name": "mcp__agent-run__start"},
            {"hook_event_name": "PermissionRequest"},
            [],
        )
        for payload in cases:
            with self.subTest(payload=payload):
                self.assertFalse(allows(payload, trusted))

    def test_main_emits_allow_only_for_a_trusted_mcp(self) -> None:
        """The executable protocol stays silent for residual reviewer work."""

        allowed_input = io.StringIO(
            json.dumps(
                {
                    "hook_event_name": "PermissionRequest",
                    "tool_name": "mcp__agent_run__answer",
                }
            )
        )
        allowed_output = io.StringIO()
        with patch("sys.stdin", allowed_input), patch("sys.stdout", allowed_output):
            self.assertEqual(main(["--allow-mcp", "agent-run"]), 0)
        self.assertEqual(
            json.loads(allowed_output.getvalue()),
            {
                "hookSpecificOutput": {
                    "hookEventName": "PermissionRequest",
                    "decision": {"behavior": "allow"},
                }
            },
        )

        denied_output = io.StringIO()
        with patch(
            "sys.stdin",
            io.StringIO(
                json.dumps(
                    {"hook_event_name": "PermissionRequest", "tool_name": "Bash"}
                )
            ),
        ), patch("sys.stdout", denied_output):
            self.assertEqual(main(["--allow-mcp", "agent-run"]), 0)
        self.assertEqual(denied_output.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
