import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import Capability
from agent_run.adapters.claude.adapter import ADAPTER, ADAPTER_API_VERSION, ClaudeAdapter
from agent_run.config import McpConfig, RuntimeAuthConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.domain import StartRequest
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile


class ClaudeAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
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
            "os.environ",
            {"AGENT_RUN_HOME": str(self.root), "PATH": "/usr/bin"},
            clear=False,
        )
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        for name in ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_AUTH_TOKEN"):
            previous = os.environ.pop(name, None)
            if previous is not None:
                self.addCleanup(os.environ.__setitem__, name, previous)

    def runtime_config(self, **overrides) -> RuntimeConfig:
        values = dict(
            enabled=True,
            adapter="agent_run.adapters.claude.adapter:ADAPTER",
            binary=Path("/bin/echo"),
            home=self.home,
            models=("sonnet", "opus"),
            skills=(),
            mcp=(),
            auth=RuntimeAuthConfig("environment", names=("ANTHROPIC_API_KEY",)),
            hooks=(),
        )
        values.update(overrides)
        return RuntimeConfig(**values)

    def profile(self, *, write: bool = False, read_roots: tuple[Path, ...] = ()) -> AgentProfile:
        return AgentProfile("review", "Review carefully.", write, read_roots)

    def request(self, **overrides) -> StartRequest:
        values = dict(
            runtime="claude",
            model="sonnet",
            profile="review",
            task="do the thing",
            workdir=self.workdir,
        )
        values.update(overrides)
        return StartRequest(**values)

    def prepare(self, *args, mcp_servers: dict = {}, **kwargs):
        return self.adapter.prepare(*args, mcp_servers=mcp_servers, **kwargs)

    def materialize(self, *args, mcp_servers: dict = {}, **kwargs):
        return self.adapter.materialize(*args, mcp_servers=mcp_servers, **kwargs)

    # -- describe -----------------------------------------------------

    def test_describe_reports_api_version_and_excludes_live_limits(self) -> None:
        info = self.adapter.describe()
        self.assertEqual(info.name, "claude")
        self.assertEqual(info.adapter_api_version, ADAPTER_API_VERSION)
        self.assertIn(Capability.WRITE, info.capabilities)
        self.assertNotIn(Capability.LIVE_LIMITS, info.capabilities)

    # -- validate -------------------------------------------------------

    def test_validate_requires_environment_auth_from_the_known_names(self) -> None:
        self.adapter.validate(self.runtime_config())
        with self.assertRaisesRegex(ValidationError, "requires an auth bridge"):
            self.adapter.validate(self.runtime_config(auth=None))
        with self.assertRaisesRegex(ValidationError, "auth.kind must be"):
            self.adapter.validate(
                self.runtime_config(auth=RuntimeAuthConfig("file_link", source=Path("/tmp"), target="a"))
            )
        with self.assertRaisesRegex(ValidationError, "unsupported entries"):
            self.adapter.validate(self.runtime_config(auth=RuntimeAuthConfig("environment", names=("ROGUE_VAR",))))

    def test_validate_refuses_service_mode_and_unknown_hook_events(self) -> None:
        with self.assertRaisesRegex(ValidationError, "service_mode"):
            self.adapter.validate(self.runtime_config(service_mode="managed"))
        with self.assertRaisesRegex(ValidationError, "not a known Claude hook event"):
            self.adapter.validate(
                self.runtime_config(hooks=(RuntimeHookConfig("BogusEvent", ("echo",)),))
            )
        self.adapter.validate(
            self.runtime_config(hooks=(RuntimeHookConfig("PostToolUse", ("echo", "done"), "^Bash$"),))
        )

    # -- models / limits --------------------------------------------------

    def test_models_reflect_the_configured_roster_without_a_live_call(self) -> None:
        models = self.adapter.models(self.runtime_config(), self.home)
        self.assertEqual(tuple(model.id for model in models), ("sonnet", "opus"))

    def test_limits_never_makes_a_live_call(self) -> None:
        self.assertEqual(self.adapter.limits(self.runtime_config(), self.home), ())

    # -- probe -------------------------------------------------------------

    def test_probe_is_local_only_and_checks_named_auth_env_presence(self) -> None:
        missing_binary = self.runtime_config(binary=self.root / "no-such-claude-binary")
        health = self.adapter.probe(missing_binary, self.home)
        self.assertFalse(health.available)
        self.assertFalse(health.authenticated)

        available = self.runtime_config(binary=Path("/bin/echo"))
        health = self.adapter.probe(available, self.home)
        self.assertTrue(health.available)
        self.assertFalse(health.authenticated)
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            health = self.adapter.probe(available, self.home)
            self.assertTrue(health.authenticated)

    # -- mcp_servers is required --------------------------------------------

    def test_materialize_and_prepare_require_the_mcp_servers_keyword(self) -> None:
        config = self.runtime_config()
        with self.assertRaises(TypeError):
            self.adapter.materialize(config, self.home)
        with self.assertRaises(TypeError):
            self.adapter.prepare(self.request(), self.profile(), config, self.home, self.agent_dir)

    # -- materialize ---------------------------------------------------------

    def test_materialize_writes_only_declared_hooks_into_settings(self) -> None:
        config = self.runtime_config(hooks=(RuntimeHookConfig("UserPromptSubmit", ("agent-run", "hook")),))
        digest = self.materialize(config, self.home)
        self.assertTrue(digest)
        settings = json.loads((self.home / "settings.json").read_text(encoding="utf-8"))
        self.assertIn("UserPromptSubmit", settings["hooks"])
        self.assertEqual(len(settings["hooks"]), 1)

    def test_materialize_renders_hook_commands_shell_quoted(self) -> None:
        config = self.runtime_config(
            hooks=(RuntimeHookConfig("UserPromptSubmit", ("agent-run", "hook", "context abc")),)
        )
        self.materialize(config, self.home)
        settings = json.loads((self.home / "settings.json").read_text(encoding="utf-8"))
        rendered = settings["hooks"]["UserPromptSubmit"][0]["hooks"][0]["command"]
        self.assertEqual(rendered, "agent-run hook 'context abc'")

    def test_materialize_fails_closed_when_mcp_name_has_no_resolution(self) -> None:
        config = self.runtime_config(mcp=("agent_lsp",))
        with self.assertRaisesRegex(ValidationError, "no resolved MCP definition"):
            self.materialize(config, self.home)

    def test_materialize_renders_strict_mcp_config_when_resolved(self) -> None:
        config = self.runtime_config(mcp=("agent_lsp",))
        servers = {"agent_lsp": McpConfig("stdio", Path("/bin/agent-lsp"), ("--flag",), ("LSP_TOKEN",))}
        self.materialize(config, self.home, mcp_servers=servers)
        rendered = json.loads((self.home / "mcp" / "mcp-config.json").read_text(encoding="utf-8"))
        self.assertEqual(rendered["mcpServers"]["agent_lsp"]["command"], "/bin/agent-lsp")
        self.assertNotIn("LSP_TOKEN", json.dumps(rendered))

    def test_materialize_generates_plugin_dirs_only_for_selected_skills(self) -> None:
        skill_dir = self.root / "skills" / "claude" / "delegate"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text("Delegate work.", encoding="utf-8")
        scripts_dir = skill_dir / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "run.sh").write_text("#!/bin/sh\necho hi\n", encoding="utf-8")
        config = self.runtime_config(skills=("delegate",))
        self.materialize(config, self.home)
        plugin_manifest = self.home / "plugins" / "delegate" / ".claude-plugin" / "plugin.json"
        skill_link = self.home / "plugins" / "delegate" / "skills" / "delegate" / "SKILL.md"
        scripts_link = self.home / "plugins" / "delegate" / "skills" / "delegate" / "scripts"
        self.assertTrue(plugin_manifest.is_file())
        self.assertTrue(skill_link.is_symlink())
        self.assertEqual(skill_link.read_text(encoding="utf-8"), "Delegate work.")
        self.assertTrue(scripts_link.is_symlink())
        self.assertEqual((scripts_link / "run.sh").read_text(encoding="utf-8"), "#!/bin/sh\necho hi\n")

    def test_materialize_fails_closed_on_missing_skill(self) -> None:
        config = self.runtime_config(skills=("missing-skill",))
        with self.assertRaisesRegex(ValidationError, "skill not found"):
            self.materialize(config, self.home)

    # -- prepare -------------------------------------------------------------

    def test_prepare_fails_closed_on_a_model_outside_the_roster(self) -> None:
        with self.assertRaisesRegex(ValidationError, "not in the configured roster"):
            self.prepare(
                self.request(model="not-a-model"), self.profile(), self.runtime_config(), self.home, self.agent_dir
            )

    def test_prepare_fails_closed_when_no_declared_auth_env_is_set(self) -> None:
        with self.assertRaisesRegex(ValidationError, "auth requires one of"):
            self.prepare(self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir)

    def test_prepare_builds_an_isolated_launch_plan_for_a_read_only_profile(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            plan = self.prepare(
                self.request(), self.profile(write=False), self.runtime_config(), self.home, self.agent_dir
            )
        argv = plan.argv
        self.assertIn("--setting-sources", argv)
        self.assertEqual(argv[argv.index("--setting-sources") + 1], "")
        self.assertIn("--strict-mcp-config", argv)
        self.assertIn("--no-session-persistence", argv)
        self.assertIn("--permission-mode", argv)
        self.assertEqual(argv[argv.index("--permission-mode") + 1], "default")
        tools = argv[argv.index("--tools") + 1].split(",")
        self.assertEqual(set(tools), {"Read", "Grep", "Glob"})
        allowed = argv[argv.index("--allowedTools") + 1]
        self.assertIn("Read", allowed.split(","))
        self.assertNotIn("Write", allowed.split(","))
        self.assertNotIn("Bash", allowed.split(","))
        self.assertNotIn("Bash", tools)
        disallowed = argv[argv.index("--disallowedTools") + 1]
        self.assertIn("WebFetch", disallowed)
        self.assertNotIn("--mcp-config", argv)
        self.assertEqual(plan.cwd, self.workdir)
        self.assertEqual(plan.environment["HOME"], str(self.home))
        self.assertEqual(plan.runtime_stream_path, self.agent_dir / "runtime.jsonl")
        payload = json.loads(plan.initial_input)
        self.assertEqual(payload["message"]["content"][0]["text"], "do the thing")

    def test_prepare_grants_write_tools_only_when_profile_allows_write(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            plan = self.prepare(
                self.request(write=True),
                self.profile(write=True),
                self.runtime_config(),
                self.home,
                self.agent_dir,
            )
        allowed = plan.argv[plan.argv.index("--allowedTools") + 1].split(",")
        self.assertIn(f"Write({self.workdir}/**)", allowed)
        self.assertIn(f"Edit({self.workdir}/**)", allowed)
        self.assertNotIn("Bash", allowed)
        self.assertFalse(any(item == "Bash" or item.startswith("Bash(") for item in allowed))
        tools = plan.argv[plan.argv.index("--tools") + 1].split(",")
        self.assertIn("Write", tools)
        self.assertNotIn("Bash", tools)
        self.assertEqual(plan.argv[plan.argv.index("--permission-mode") + 1], "acceptEdits")

    def test_request_can_narrow_but_not_widen_profile_write(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            narrowed = self.prepare(
                self.request(write=False),
                self.profile(write=True),
                self.runtime_config(),
                self.home,
                self.agent_dir,
            )
            with self.assertRaisesRegex(ValidationError, "does not allow requested write"):
                self.prepare(
                    self.request(write=True),
                    self.profile(write=False),
                    self.runtime_config(),
                    self.home,
                    self.agent_dir,
                )
        tools = narrowed.argv[narrowed.argv.index("--tools") + 1].split(",")
        allowed = narrowed.argv[narrowed.argv.index("--allowedTools") + 1].split(",")
        self.assertNotIn("Write", tools)
        self.assertNotIn("Edit", tools)
        self.assertFalse(any(item.startswith(("Write(", "Edit(")) for item in allowed))
        self.assertEqual(
            narrowed.argv[narrowed.argv.index("--permission-mode") + 1], "default"
        )

    def test_prepare_fails_closed_when_write_mode_cannot_keep_a_read_root_read_only(self) -> None:
        nested = self.workdir / "nested-read-root"
        nested.mkdir()
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            cases = (
                (self.request(write=True, read_roots=(nested,)), self.profile(write=True)),
                (self.request(write=True), self.profile(write=True, read_roots=(nested,))),
            )
            for request, profile in cases:
                with self.subTest(request_roots=request.read_roots):
                    with self.assertRaisesRegex(
                        ValidationError, "cannot keep a read root read-only"
                    ):
                        self.prepare(
                            request,
                            profile,
                            self.runtime_config(),
                            self.home,
                            self.agent_dir,
                        )

    def test_prepare_normalizes_profile_and_request_roots_as_one_antichain(self) -> None:
        parent = self.root / "shared"
        nested = parent / "nested"
        request_only = self.root / "request-only"
        nested.mkdir(parents=True)
        request_only.mkdir()
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            plan = self.prepare(
                self.request(read_roots=(request_only, nested)),
                self.profile(read_roots=(parent,)),
                self.runtime_config(),
                self.home,
                self.agent_dir,
            )
        roots = tuple(
            plan.argv[index + 1]
            for index, value in enumerate(plan.argv)
            if value == "--add-dir"
        )
        self.assertEqual(set(roots), {str(self.workdir), str(parent), str(request_only)})
        self.assertNotIn(str(nested), roots)
        self.assertEqual(
            roots, tuple(sorted(roots, key=lambda value: (len(Path(value).parts), value)))
        )

    def test_prepare_sets_isolated_home_and_copies_only_declared_environment(self) -> None:
        ambient = {
            "HOME": "/ambient/home",
            "CLAUDE_CONFIG_DIR": "/ambient/claude",
            "UNRELATED_SECRET": "must-not-copy",
            "ANTHROPIC_API_KEY": "sk-test",
        }
        with patch.dict("os.environ", ambient, clear=False):
            plan = self.prepare(
                self.request(),
                self.profile(),
                self.runtime_config(),
                self.home,
                self.agent_dir,
            )
        self.assertEqual(
            dict(plan.environment),
            {"HOME": str(self.home), "PATH": "/usr/bin", "ANTHROPIC_API_KEY": "sk-test"},
        )
        self.assertNotIn("/ambient", " ".join(plan.argv))

    def test_prepare_adds_dirs_and_mcp_flags_and_never_leaks_secrets_into_argv(self) -> None:
        child = self.root / "child"
        child.mkdir()
        config = self.runtime_config(mcp=("agent_lsp",))
        servers = {"agent_lsp": McpConfig("stdio", Path("/bin/agent-lsp"), (), ())}
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-super-secret"}):
            plan = self.prepare(
                self.request(),
                self.profile(read_roots=(child,)),
                config,
                self.home,
                self.agent_dir,
                mcp_servers=servers,
            )
        self.assertIn(str(child), plan.argv)
        self.assertIn("--mcp-config", plan.argv)
        self.assertIn("mcp__agent_lsp", plan.argv[plan.argv.index("--allowedTools") + 1])
        self.assertNotIn("sk-super-secret", " ".join(plan.argv))
        self.assertEqual(plan.environment["ANTHROPIC_API_KEY"], "sk-super-secret")

    def test_prepare_fails_closed_when_an_mcp_definition_is_unresolved(self) -> None:
        config = self.runtime_config(mcp=("agent_lsp",))
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            with self.assertRaisesRegex(ValidationError, "no resolved MCP definition"):
                self.prepare(self.request(), self.profile(), config, self.home, self.agent_dir)

    def test_prepare_fails_closed_when_an_mcp_env_var_is_not_set(self) -> None:
        config = self.runtime_config(mcp=("agent_lsp",))
        servers = {"agent_lsp": McpConfig("stdio", Path("/bin/agent-lsp"), (), ("LSP_TOKEN",))}
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            os.environ.pop("LSP_TOKEN", None)
            with self.assertRaisesRegex(ValidationError, "LSP_TOKEN"):
                self.prepare(
                    self.request(), self.profile(), config, self.home, self.agent_dir, mcp_servers=servers
                )

    def test_prepare_validates_effort_and_passes_the_native_flag(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            plan = self.prepare(
                self.request(effort="high", output_schema={"type": "object"}),
                self.profile(),
                self.runtime_config(),
                self.home,
                self.agent_dir,
            )
            self.assertIn("--effort", plan.argv)
            self.assertEqual(plan.argv[plan.argv.index("--effort") + 1], "high")
            prompt = plan.argv[plan.argv.index("--append-system-prompt") + 1]
            self.assertNotIn("reasoning effort", prompt)
            self.assertIn('"type": "object"', prompt)

            with self.assertRaisesRegex(ValidationError, "effort must be one of"):
                self.prepare(
                    self.request(effort="bogus"), self.profile(), self.runtime_config(), self.home, self.agent_dir
                )


if __name__ == "__main__":
    unittest.main()
