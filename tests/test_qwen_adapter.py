"""Contract tests for the Qwen Code one-shot runtime adapter."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_run.adapters.qwen.adapter import ADAPTER, QwenAdapter
from agent_run.config import McpConfig, RuntimeAuthConfig, RuntimeConfig
from agent_run.domain import StartRequest
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile


class QwenAdapterTests(unittest.TestCase):
    """Exercise permissions, provider wiring, MCP settings, and role isolation."""

    def setUp(self) -> None:
        """Create isolated runtime, agent, and child work directories."""
        self.adapter: QwenAdapter = ADAPTER
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name).resolve()
        self.home = self.root / "runtime-home"
        self.agent_dir = self.root / "agents" / "ag-1"
        self.agent_dir.mkdir(parents=True)
        self.workdir = self.root / "work"
        self.workdir.mkdir()

    def config(self, **overrides: object) -> RuntimeConfig:
        """Return the smallest valid Qwen runtime configuration."""
        values: dict[str, object] = {
            "enabled": True,
            "adapter": "agent_run.adapters.qwen.adapter:ADAPTER",
            "binary": Path("/bin/echo"),
            "home": self.home,
            "models": ("qwen-test",),
            "skills": (),
            "mcp": (),
            "auth": RuntimeAuthConfig("environment", names=("OPENAI_API_KEY", "OPENAI_BASE_URL")),
            "hooks": (),
        }
        values.update(overrides)
        return RuntimeConfig(**values)

    def profile(self, *, write: bool = False, network: bool = False) -> AgentProfile:
        """Return a role profile with the requested permission bounds."""
        return AgentProfile("implement", "ROLE CONTRACT", write, (), network)

    def request(self, **overrides: object) -> StartRequest:
        """Return a Qwen start request rooted in the temporary workdir."""
        values: dict[str, object] = {
            "runtime": "qwen", "model": "qwen-test", "profile": "implement",
            "task": "do the thing", "workdir": self.workdir,
        }
        values.update(overrides)
        return StartRequest(**values)

    def prepare(self, *, write: bool = False, mcp: bool = False):
        """Prepare one plan with deterministic fake provider credentials."""
        servers = {"agent_lsp": McpConfig("stdio", Path("/bin/lsp"), ("--stdio",), ())}
        config = self.config(mcp=("agent_lsp",)) if mcp else self.config()
        with patch.dict(os.environ, {
            "PATH": "/usr/bin", "OPENAI_API_KEY": "secret", "OPENAI_BASE_URL": "https://provider/v1",
        }):
            return self.adapter.prepare(
                self.request(write=write), self.profile(write=write), config, self.home,
                self.agent_dir, mcp_servers=servers if mcp else {},
            )

    def test_read_only_and_write_modes_are_explicit_and_sandboxed(self) -> None:
        """Map read-only to plan and writes to auto-edit without dropping sandbox."""
        readonly = self.prepare()
        writable = self.prepare(write=True)
        for plan, expected in ((readonly, "plan"), (writable, "auto-edit")):
            self.assertEqual(plan.argv[plan.argv.index("--approval-mode") + 1], expected)
            self.assertIn("--sandbox", plan.argv)
            self.assertEqual(plan.cwd, self.workdir)
            self.assertEqual(plan.adapter_state["approval_mode"], expected)
            self.assertTrue(plan.adapter_state["sandbox"])

    def test_provider_model_mcp_and_role_are_isolated(self) -> None:
        """Carry provider values in env and materialize MCP plus the role under HOME."""
        plan = self.prepare(mcp=True)
        self.assertEqual(plan.environment["OPENAI_BASE_URL"], "https://provider/v1")
        self.assertEqual(plan.environment["OPENAI_MODEL"], "qwen-test")
        self.assertNotIn("secret", " ".join(plan.argv))
        settings = json.loads((self.home / ".qwen" / "settings.json").read_text(encoding="utf-8"))
        self.assertEqual(settings["mcpServers"]["agent_lsp"]["command"], "/bin/lsp")
        context = Path(settings["context"]["fileName"])
        self.assertEqual(context.read_text(encoding="utf-8"), "ROLE CONTRACT\n")
        self.assertTrue(context.is_relative_to(self.home))

    def test_network_profile_is_refused(self) -> None:
        """Reject network profiles because this adapter grants no Qwen web tools."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "x", "OPENAI_BASE_URL": "https://p/v1"}):
            with self.assertRaisesRegex(ValidationError, "network profiles"):
                self.adapter.prepare(
                    self.request(), self.profile(network=True), self.config(), self.home,
                    self.agent_dir, mcp_servers={},
                )


if __name__ == "__main__":
    unittest.main()
