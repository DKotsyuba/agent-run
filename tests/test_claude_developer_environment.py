"""Compatibility checks for ignored Claude environment presets."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import MappingProxyType
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.claude.adapter import ADAPTER, ClaudeAdapter
from agent_run.config import EnvironmentConfig, McpConfig, RuntimeAuthConfig, RuntimeConfig
from agent_run.domain import StartRequest
from agent_run.profiles import AgentProfile


def _executable(path: Path, body: str = "exit 0") -> None:
    """Create one local shell fixture executable with a portable shebang."""

    path.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
    path.chmod(0o755)


class ClaudeDeveloperEnvironmentTests(unittest.TestCase):
    """Verify host inheritance while retaining legacy command denials."""

    def setUp(self) -> None:
        """Create one isolated home, agent directory, workdir, and environment."""
        self.adapter: ClaudeAdapter = ADAPTER
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name).resolve()
        self.home = self.root / "runtimes" / "claude" / "home"
        self.agent_dir = self.root / "agents" / "ag-1"
        self.agent_dir.mkdir(parents=True)
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.env_patch = patch.dict(
            "os.environ", {"AGENT_RUN_HOME": str(self.root), "PATH": "/usr/bin"}, clear=False
        )
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def runtime_config(self, **overrides) -> RuntimeConfig:
        """Build a valid Claude runtime config with optional test overrides."""

        values = dict(
            enabled=True,
            adapter="agent_run.adapters.claude.adapter:ADAPTER",
            binary=Path("/bin/echo"),
            home=self.home,
            models=("sonnet",),
            auth=RuntimeAuthConfig("environment", names=("ANTHROPIC_API_KEY",)),
        )
        values.update(overrides)
        return RuntimeConfig(**values)

    def profile(self, **overrides) -> AgentProfile:
        """Build the default Claude review profile with optional test overrides."""

        values = dict(write=False, read_roots=(), network=False)
        values.update(overrides)
        return AgentProfile("review", "Review carefully.", **values)

    def request(self, **overrides) -> StartRequest:
        """Build one default Claude start request with optional test overrides."""

        values = dict(
            runtime="claude", model="sonnet", profile="review", task="do the thing", workdir=self.workdir
        )
        values.update(overrides)
        return StartRequest(**values)

    def test_prepare_ignores_preset_and_inherits_host_values_for_mcp(self) -> None:
        """Use host PATH/variables and the already materialized MCP descriptor."""

        tools = self.root / "tools"
        tools.mkdir()
        _executable(tools / "node")
        config = self.runtime_config(
            environment=EnvironmentConfig(
                path=(tools,), variables=MappingProxyType({"PROJECT": "{workdir}"}), required_commands=("node",)
            ),
            mcp=("agent_lsp",),
        )
        servers = {"agent_lsp": McpConfig("stdio", Path("/bin/agent-lsp"), (), ("PATH", "PROJECT"))}
        self.adapter.materialize(
            config,
            self.home,
            mcp_servers=servers,
            skills_root=self.root / "skills" / "claude",
        )
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test", "PROJECT": "must-not-copy"}, clear=False):
            plan = self.adapter.prepare(
                self.request(), self.profile(), config, self.home, self.agent_dir, mcp_servers=servers
            )
        descriptor_path = self.home / "mcp" / "mcp-config.json"
        self.assertEqual(plan.argv[plan.argv.index("--mcp-config") + 1], str(descriptor_path))
        descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
        self.assertNotIn("env", descriptor["mcpServers"]["agent_lsp"])
        self.assertEqual(plan.environment["PROJECT"], "must-not-copy")
        self.assertEqual(plan.environment["PATH"], "/usr/bin")

    def test_prepare_does_not_probe_legacy_required_commands(self) -> None:
        """Ignore legacy required commands instead of gating runtime start."""

        config = self.runtime_config(environment=EnvironmentConfig(required_commands=("definitely-missing",)))
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}, clear=False):
            plan = self.adapter.prepare(
                self.request(), self.profile(), config, self.home, self.agent_dir, mcp_servers={}
            )
        self.assertEqual(plan.environment["PATH"], "/usr/bin")

    def test_prepare_denies_gh_bare_and_absolute_while_git_remains_permitted(self) -> None:
        """A denied command is refused bare and by absolute path; Git stays permitted."""

        host = self.root / "host"
        host.mkdir()
        _executable(host / "gh")
        _executable(host / "git")
        config = self.runtime_config(environment=EnvironmentConfig(path=(host,), denied_commands=("gh",)))
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}, clear=False):
            plan = self.adapter.prepare(
                self.request(write=True),
                self.profile(write=True, network=True),
                config,
                self.home,
                self.agent_dir,
                mcp_servers={},
            )
        disallowed = plan.argv[plan.argv.index("--disallowedTools") + 1]
        self.assertIn("Bash(gh)", disallowed)
        self.assertIn("Bash(gh *)", disallowed)
        self.assertNotIn(f"Bash({host / 'gh'})", disallowed)
        self.assertNotIn("Bash(git)", disallowed)
        self.assertNotIn("Bash(git *)", disallowed)

    def test_materialize_revision_ignores_legacy_preset_changes(self) -> None:
        """Do not bind unused legacy environment declarations into new assets."""

        skills_root = self.root / "skills" / "claude"
        base = self.runtime_config(environment=EnvironmentConfig(variables=MappingProxyType({"A": "1"})))
        changed = self.runtime_config(environment=EnvironmentConfig(variables=MappingProxyType({"A": "2"})))
        digest_base = self.adapter.materialize(base, self.home, mcp_servers={}, skills_root=skills_root)
        digest_changed = self.adapter.materialize(changed, self.home, mcp_servers={}, skills_root=skills_root)
        self.assertEqual(digest_base, digest_changed)


if __name__ == "__main__":
    unittest.main()
