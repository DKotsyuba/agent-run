"""Tests for reusable owner command-policy providers."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from agent_run.errors import ValidationError

from agent_run.adapters.command_policy import (
    materialize_refusal_commands,
    render_claude_denials,
    render_codex_denial_rules,
    render_qwen_denials,
    validate_denied_commands,
)


class CommandPolicyTest(unittest.TestCase):
    """Exercise safe shim lifecycle and deterministic native translations."""

    def test_refusal_shadows_only_denied_normal_invocation(self) -> None:
        """A PATH shim fails a denied name while an allowed fake git still runs."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            host = root / "host"
            host.mkdir()
            self._executable(host / "gh", "exit 0")
            self._executable(host / "git", "printf allowed")
            policy = materialize_refusal_commands(("gh",), root / "policy", search_paths=(host,))
            environment = {"PATH": f"{policy.directory}{os.pathsep}{host}"}
            denied = self._run(("gh",), environment)
            allowed = self._run(("git",), environment)
            self.assertEqual(denied.returncode, 126)
            self.assertIn("denied by owner policy", denied.stderr)
            self.assertEqual(allowed.stdout, "allowed")
            self.assertEqual(policy.resolved_commands, {"gh": host / "gh"})

    def test_refresh_removes_only_unchanged_managed_entries(self) -> None:
        """A changed declaration removes stale owned shims and adds new ones safely."""

        with tempfile.TemporaryDirectory() as temporary:
            policy_dir = Path(temporary) / "policy"
            materialize_refusal_commands(("gh", "hub"), policy_dir, search_paths=())
            materialize_refusal_commands(("hub",), policy_dir, search_paths=())
            self.assertFalse((policy_dir / "gh").exists())
            self.assertTrue((policy_dir / "hub").is_file())
            (policy_dir / "hub").write_text("user file", encoding="utf-8")
            with self.assertRaisesRegex(ValidationError, "unmanaged"):
                materialize_refusal_commands((), policy_dir, search_paths=())

    def test_rejects_traversal_and_never_follows_symlinks(self) -> None:
        """Malformed names and symlinked managed paths are rejected without mutation."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaises(ValidationError):
                validate_denied_commands(("../gh",))
            policy_dir = root / "policy"
            materialize_refusal_commands(("gh",), policy_dir, search_paths=())
            (policy_dir / "gh").unlink()
            (policy_dir / "gh").symlink_to(root / "target")
            with self.assertRaisesRegex(ValidationError, "unmanaged"):
                materialize_refusal_commands(("gh",), policy_dir, search_paths=())

    def test_native_rendering_is_deterministic_and_exact(self) -> None:
        """Native rules cover exact bare and absolute forms without prefix bleed."""

        rules = render_codex_denial_rules(("hub", "gh", "gh"), command_paths=("/tools/gh",))
        self.assertEqual(rules, render_codex_denial_rules(("gh", "hub"), command_paths=("/tools/gh",)))
        self.assertIn('pattern=["gh"]', rules)
        self.assertIn('pattern=["/tools/gh"]', rules)
        expected = ("Bash(/tools/gh)", "Bash(/tools/gh *)", "Bash(gh)", "Bash(gh *)", "Bash(hub)", "Bash(hub *)")
        self.assertEqual(render_claude_denials(("hub", "gh"), command_paths=("/tools/gh",)), expected)
        self.assertEqual(render_qwen_denials(("hub", "gh"), command_paths=("/tools/gh",)), expected)

    def test_marker_symlinks_are_rejected_before_refresh(self) -> None:
        """A substituted ownership marker cannot authorize writes or removals."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            policy_dir = root / "policy"
            materialize_refusal_commands(("gh",), policy_dir, search_paths=())
            marker = policy_dir / ".agent-run-command-policy.json"
            target = root / "foreign-marker.json"
            original = marker.read_bytes()
            target.write_bytes(original)
            marker.unlink()
            marker.symlink_to(target)
            with self.assertRaisesRegex(ValidationError, "regular file"):
                materialize_refusal_commands(("hub",), policy_dir, search_paths=())
            self.assertEqual(target.read_bytes(), original)
            self.assertTrue((policy_dir / "gh").is_file())
            self.assertFalse((policy_dir / "hub").exists())

    def test_native_rules_include_executable_symlink_and_target(self) -> None:
        """Both the normal absolute alias and its target receive native denials."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            host = root / "bin"
            host.mkdir()
            target = root / "actual-gh"
            self._executable(target, "exit 0")
            (host / "gh").symlink_to(target)
            policy = materialize_refusal_commands(("gh",), root / "policy", search_paths=(host,))
            self.assertEqual(policy.resolved_commands["gh"], host / "gh")
            rules = render_codex_denial_rules(("gh",), command_paths=tuple(policy.resolved_commands.values()))
            self.assertIn(str(host / "gh"), rules)
            self.assertIn(str(target), rules)

    @staticmethod
    def _executable(path: Path, body: str) -> None:
        """Create one local shell fixture executable with a portable shebang."""

        path.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
        path.chmod(0o700)

    @staticmethod
    def _run(arguments: tuple[str, ...], environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
        """Run a safe local fixture command with captured text output."""

        return subprocess.run(arguments, env=environment, text=True, capture_output=True, check=False)
