"""Contract tests for the Qwen Code one-shot runtime adapter."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from agent_run.adapters.qwen import auth as qwen_auth
from agent_run.adapters.qwen.adapter import ADAPTER, QwenAdapter
from agent_run.adapters.base import Capability
from agent_run.config import McpConfig, RuntimeAuthConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.domain import StartRequest
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile


class QwenAdapterTests(unittest.TestCase):
    """Exercise permissions, provider wiring, MCP settings, and role isolation."""

    def setUp(self) -> None:
        """Create isolated runtime, agent, and child work directories.

        ``home`` sits at ``<root>/runtimes/qwen/home``, matching the real
        deployment layout (``<agent_run_home>/runtimes/qwen/home``), so
        ``materialize``'s default ``skills_root`` resolution
        (``home.parents[2] / "skills" / "qwen"``) lands at
        ``<root>/skills/qwen`` exactly as it does in production.
        """
        self.adapter: QwenAdapter = ADAPTER
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name).resolve()
        self.home = self.root / "runtimes" / "qwen" / "home"
        self.agent_dir = self.root / "agents" / "ag-1"
        self.agent_dir.mkdir(parents=True)
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.skills_root = self.root / "skills" / "qwen"
        qwen_auth.keychain_omniroute_api_key.cache_clear()
        self.addCleanup(qwen_auth.keychain_omniroute_api_key.cache_clear)

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

    def write_skill(self, name: str, body: str = "# demo skill\n") -> None:
        """Write one local skill's ``SKILL.md`` beneath the default skills root."""
        directory = self.skills_root / name
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "SKILL.md").write_text(body, encoding="utf-8")

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

    def prepare(self, *, write: bool = False, mcp: bool = False, config: RuntimeConfig | None = None):
        """Prepare one plan with deterministic fake provider credentials."""
        servers = {"agent_lsp": McpConfig("stdio", Path("/bin/lsp"), ("--stdio",), ())}
        if config is None:
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

    def test_process_env_credentials_win_over_keychain_and_default_base_url(self) -> None:
        """The process environment must win even when a keychain value exists."""
        with patch(
            "agent_run.adapters.qwen.adapter.keychain_omniroute_api_key",
            side_effect=AssertionError("keychain must not be consulted when env is set"),
        ):
            plan = self.prepare()
        self.assertEqual(plan.environment["OPENAI_API_KEY"], "secret")
        self.assertEqual(plan.environment["OPENAI_BASE_URL"], "https://provider/v1")

    def test_keychain_and_default_base_url_fill_missing_credentials(self) -> None:
        """A blank environment falls back to the keychain key and default router."""
        with patch.dict(os.environ, {"PATH": "/usr/bin"}, clear=True), patch(
            "agent_run.adapters.qwen.adapter.keychain_omniroute_api_key",
            return_value="kc-secret",
        ) as spy:
            plan = self.adapter.prepare(
                self.request(), self.profile(), self.config(), self.home,
                self.agent_dir, mcp_servers={},
            )
        spy.assert_called_once_with()
        self.assertEqual(plan.environment["OPENAI_API_KEY"], "kc-secret")
        self.assertEqual(plan.environment["OPENAI_BASE_URL"], "http://127.0.0.1:20128/v1")

    def test_missing_env_and_failed_keychain_raises(self) -> None:
        """Neither source resolving still raises the original missing-env error."""
        with patch.dict(os.environ, {"PATH": "/usr/bin"}, clear=True), patch(
            "agent_run.adapters.qwen.adapter.keychain_omniroute_api_key",
            return_value=None,
        ):
            with self.assertRaisesRegex(ValidationError, "OPENAI_API_KEY"):
                self.adapter.prepare(
                    self.request(), self.profile(), self.config(), self.home,
                    self.agent_dir, mcp_servers={},
                )

    def test_probe_reports_authenticated_via_keychain_fallback(self) -> None:
        """``probe`` must apply the same fallback rules ``prepare`` applies."""
        with patch.dict(os.environ, {}, clear=True), patch(
            "agent_run.adapters.qwen.adapter.keychain_omniroute_api_key",
            return_value="kc-secret",
        ):
            health = self.adapter.probe(self.config(binary=Path(__file__)), self.home)
        self.assertTrue(health.authenticated)

    def test_skills_capability_is_declared(self) -> None:
        """Qwen has no Skill tool, but the capability still advertises delivery."""
        self.assertIn(Capability.SKILLS, self.adapter.describe().capabilities)
        self.assertIn(Capability.HOOKS, self.adapter.describe().capabilities)

    def test_live_limits_capability_and_pool_samples_are_shared(self) -> None:
        """qwen's quota IS the OmniRoute opencode-go pool's quota."""
        self.assertIn(Capability.LIVE_LIMITS, self.adapter.describe().capabilities)
        rows = [
            {"window_key": "session", "remaining_percentage": 90.0,
             "next_reset_at": "2026-08-28T17:26:35.920Z",
             "created_at": "2026-08-28T14:06:01.922Z"},
        ]
        now = datetime.fromisoformat("2026-08-28T14:06:01.922Z").timestamp() + 60.0
        with patch(
            "agent_run.adapters.omniroute._docker_rows", lambda: rows
        ), patch(
            "agent_run.adapters.qwen.adapter.time.time", lambda: now
        ):
            samples = self.adapter.limits(self.config(), self.home)
        (sample,) = samples
        self.assertEqual(sample.window, "session_5h")
        self.assertEqual(sample.source, "omniroute_quota_pool")
        self.assertEqual(sample.target, "opencode-go:pool")
        self.assertEqual(sample.remaining_percent, 90.0)
        self.assertEqual(sample.valid_for_seconds, 900)

    def test_a_failing_row_source_reports_no_samples(self) -> None:
        """A docker failure is no evidence, never an exception."""
        with patch(
            "agent_run.adapters.omniroute._docker_rows", return_value=None
        ):
            self.assertEqual(self.adapter.limits(self.config(), self.home), ())

    def test_skills_are_materialized_and_context_notes_the_absolute_path(self) -> None:
        """Configured skills land under home/skills, and the context file points at them."""
        self.write_skill("role-implement", "# role-implement contract\n")
        plan = self.prepare(config=self.config(skills=("role-implement",)))
        delivered = self.home / "skills" / "role-implement" / "SKILL.md"
        self.assertEqual(delivered.read_text(encoding="utf-8"), "# role-implement contract\n")
        settings = json.loads((self.home / ".qwen" / "settings.json").read_text(encoding="utf-8"))
        context = Path(settings["context"]["fileName"])
        note = context.read_text(encoding="utf-8")
        self.assertIn(str(self.home / "skills"), note)
        self.assertIn("role-implement", note)
        self.assertIn("no Skill tool", note)
        self.assertEqual(plan.adapter_state["model"], "qwen-test")

    def test_settings_select_the_openai_auth_type(self) -> None:
        """A headless run fails closed with 'No auth type is selected' without this."""
        self.prepare()
        settings = json.loads((self.home / ".qwen" / "settings.json").read_text(encoding="utf-8"))
        self.assertEqual(settings["security"]["auth"]["selectedType"], "openai")

    def test_hooks_are_rendered_with_plugin_command_expansion(self) -> None:
        """A configured hook's ``{plugin:NAME}`` token resolves to the installed copy."""
        plugin_src = self.root / "lsp-guard-plugin"
        (plugin_src / "hooks").mkdir(parents=True)
        (plugin_src / "hooks" / "lsp_guard.py").write_text("# probe stub\n", encoding="utf-8")
        config = self.config(
            plugins=(plugin_src,),
            hooks=(
                RuntimeHookConfig(
                    event="PreToolUse",
                    matcher="^(read_file)$",
                    command=("/usr/bin/python3", "{plugin:lsp-guard-plugin}/hooks/lsp_guard.py"),
                ),
            ),
        )
        self.prepare(config=config)
        settings = json.loads((self.home / ".qwen" / "settings.json").read_text(encoding="utf-8"))
        groups = settings["hooks"]["PreToolUse"]
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["matcher"], "^(read_file)$")
        rendered_command = groups[0]["hooks"][0]["command"]
        installed = self.home / "plugins" / "lsp-guard-plugin" / "hooks" / "lsp_guard.py"
        self.assertIn(str(installed), rendered_command)
        self.assertTrue(installed.is_file())


if __name__ == "__main__":
    unittest.main()
