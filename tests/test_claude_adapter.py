import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import Capability, LimitSample
from agent_run.adapters.claude import auth as claude_auth
from agent_run.adapters.claude.adapter import ADAPTER, ADAPTER_API_VERSION, ClaudeAdapter
from agent_run.config import McpConfig, RuntimeAuthConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.domain import StartRequest
from agent_run.errors import AuthError, ValidationError
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

    def profile(
        self, *, write: bool = False, read_roots: tuple[Path, ...] = (), network: bool = False
    ) -> AgentProfile:
        return AgentProfile("review", "Review carefully.", write, read_roots, network)

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
        skills_root = kwargs.pop(
            "skills_root", self.root / "skills" / "claude"
        )
        return self.adapter.materialize(
            *args,
            mcp_servers=mcp_servers,
            skills_root=skills_root,
            **kwargs,
        )

    # -- describe -----------------------------------------------------

    def test_describe_reports_api_version_and_supports_live_limits(self) -> None:
        info = self.adapter.describe()
        self.assertEqual(info.name, "claude")
        self.assertEqual(info.adapter_api_version, ADAPTER_API_VERSION)
        self.assertIn(Capability.WRITE, info.capabilities)
        self.assertIn(Capability.LIVE_LIMITS, info.capabilities)

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

    def write_agent_runtime(self, agent_id: str, text: str, *, mtime: float | None = None) -> Path:
        """Write one agent's runtime.jsonl, matching the real on-disk layout.

        Agent dirs sit at ``<agent_run_home>/agents/<id>/``, a sibling of
        this adapter's own ``<agent_run_home>/runtimes/claude/home`` (see
        ``self.home``) -- both are three levels below the same root.
        """

        agent_dir = self.root / "agents" / agent_id
        agent_dir.mkdir(parents=True, exist_ok=True)
        path = agent_dir / "runtime.jsonl"
        path.write_text(text, encoding="utf-8")
        if mtime is not None:
            os.utime(path, (mtime, mtime))
        return path

    def rate_limit_line(
        self,
        *,
        five_hour: float = 0.14,
        seven_day: float = 0.72,
        five_hour_reset: int = 4102444800,
        seven_day_reset: int = 4102444800,
        secret: str = "not-sensitive",
    ) -> str:
        """One line matching the real captured ``rate_limit_event`` schema.

        Shape confirmed from live canary transcripts (stream-json mode):
        ``{"rate_limit_info": {..., "unifiedWindows": {"five_hour": {...},
        "seven_day": {...}}}, "session_id": ..., "type": "rate_limit_event",
        "uuid": ...}``.
        """

        return json.dumps(
            {
                "rate_limit_info": {
                    "isUsingOverage": False,
                    "overageDisabledReason": "org_level_disabled",
                    "overageStatus": "rejected",
                    "rateLimitType": "five_hour",
                    "resetsAt": five_hour_reset,
                    "status": "allowed",
                    "unifiedWindows": {
                        "five_hour": {"resetsAt": five_hour_reset, "utilization": five_hour},
                        "seven_day": {"resetsAt": seven_day_reset, "utilization": seven_day},
                    },
                },
                "session_id": "f27ed2f4-309e-4aed-b322-97a0c46759f8",
                "type": "rate_limit_event",
                "ignored": secret,
            },
            sort_keys=True,
        )

    def test_limits_never_makes_a_live_call(self) -> None:
        self.assertEqual(self.adapter.limits(self.runtime_config(), self.home), ())

    def test_limits_missing_agents_dir_is_empty(self) -> None:
        self.home.mkdir(parents=True)
        self.assertEqual(self.adapter.limits(self.runtime_config(), self.home), ())

    def test_limits_reads_the_newest_rate_limit_event_into_two_window_samples(self) -> None:
        self.home.mkdir(parents=True)
        secret = "raw-secret-must-not-escape"
        self.write_agent_runtime("ag-1", self.rate_limit_line(secret=secret))

        samples = self.adapter.limits(self.runtime_config(), self.home)

        self.assertEqual(
            [(sample.lane, sample.window) for sample in samples],
            [("usage", "five_hour"), ("usage", "seven_day")],
        )
        five_hour, seven_day = samples
        self.assertAlmostEqual(five_hour.remaining_percent, 86.0)
        self.assertAlmostEqual(seven_day.remaining_percent, 28.0)
        self.assertEqual({sample.source for sample in samples}, {"runtime_stream_evidence"})
        self.assertTrue(all(sample.observed_at is not None for sample in samples))
        self.assertTrue(all(sample.reset_at is not None for sample in samples))
        self.assertTrue(all(sample.target is None for sample in samples))
        self.assertNotIn(secret, repr(samples))

    def test_limits_prefers_the_newest_agent_directory(self) -> None:
        self.home.mkdir(parents=True)
        now = time.time()
        self.write_agent_runtime(
            "ag-older", self.rate_limit_line(five_hour=0.9), mtime=now - 30
        )
        self.write_agent_runtime(
            "ag-newer", self.rate_limit_line(five_hour=0.1), mtime=now - 5
        )

        samples = self.adapter.limits(self.runtime_config(), self.home)

        five_hour = next(sample for sample in samples if sample.window == "five_hour")
        self.assertAlmostEqual(five_hour.remaining_percent, 90.0)

    def test_limits_stale_event_has_unknown_remaining(self) -> None:
        self.home.mkdir(parents=True)
        now = time.time()
        self.write_agent_runtime("ag-1", self.rate_limit_line(), mtime=now - 10_000)

        samples = self.adapter.limits(self.runtime_config(), self.home)

        self.assertEqual(len(samples), 2)
        self.assertTrue(all(sample.observed_at is not None for sample in samples))
        self.assertTrue(all(sample.remaining_percent is None for sample in samples))
        self.assertEqual({sample.source for sample in samples}, {"unknown"})

    def test_limits_malformed_or_shape_mismatched_events_yield_no_samples(self) -> None:
        self.home.mkdir(parents=True)
        cases = {
            "not-json": "not json at all",
            "wrong-type": json.dumps({"type": "assistant"}),
            "missing-info": json.dumps({"type": "rate_limit_event"}),
            "non-dict-windows": json.dumps(
                {"type": "rate_limit_event", "rate_limit_info": {"unifiedWindows": "nope"}}
            ),
        }
        for label, text in cases.items():
            with self.subTest(case=label):
                agent_dir = self.root / "agents" / f"ag-{label}"
                agent_dir.mkdir(parents=True)
                (agent_dir / "runtime.jsonl").write_text(text, encoding="utf-8")
                self.assertEqual(self.adapter.limits(self.runtime_config(), self.home), ())
                agent_dir.joinpath("runtime.jsonl").unlink()
                agent_dir.rmdir()

    def test_limits_never_leaks_unrelated_fields_into_samples(self) -> None:
        self.home.mkdir(parents=True)
        secret = "sk-super-secret-value"
        self.write_agent_runtime("ag-1", self.rate_limit_line(secret=secret))

        samples = self.adapter.limits(self.runtime_config(), self.home)

        for sample in samples:
            self.assertIsInstance(sample, LimitSample)
        self.assertNotIn(secret, repr(samples))
        self.assertNotIn("f27ed2f4", repr(samples))

    def test_limits_considers_only_the_newest_bounded_agent_files(self) -> None:
        self.home.mkdir(parents=True)
        now = time.time()
        self.write_agent_runtime("ag-old-valid", self.rate_limit_line(), mtime=now - 100)
        for index in range(24):
            self.write_agent_runtime(f"ag-new-{index}", "not json at all", mtime=now + index)

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

    def test_materialize_uses_only_each_explicit_service_skill_root(self) -> None:
        config = self.runtime_config(skills=("delegate",))
        observed = []
        for index, text in enumerate(("first service", "second service")):
            service_home = self.root / f"service-{index}"
            skills_root = service_home / "skills" / "claude"
            source = skills_root / "delegate" / "SKILL.md"
            source.parent.mkdir(parents=True)
            source.write_text(text, encoding="utf-8")
            runtime_home = service_home / "runtimes" / "claude" / "home"
            self.materialize(
                config, runtime_home, skills_root=skills_root
            )
            generated = (
                runtime_home
                / "plugins"
                / "delegate"
                / "skills"
                / "delegate"
                / "SKILL.md"
            )
            observed.append(generated.read_text(encoding="utf-8"))
        self.assertEqual(observed, ["first service", "second service"])

        missing = self.root / "missing" / "skills" / "claude"
        with self.assertRaisesRegex(ValidationError, "skill not found"):
            self.materialize(config, self.home, skills_root=missing)

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
        self.assertIn("Bash", allowed)
        self.assertFalse(any(item.startswith("Bash(") for item in allowed))
        tools = plan.argv[plan.argv.index("--tools") + 1].split(",")
        self.assertIn("Write", tools)
        self.assertIn("Bash", tools)
        self.assertFalse(any("sandbox" in item.lower() for item in plan.argv))
        self.assertIn("Bash", plan.adapter_state["allowed_tools"])
        self.assertEqual(plan.argv[plan.argv.index("--permission-mode") + 1], "acceptEdits")

    def test_prepare_grants_web_tools_only_to_network_profiles(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            plan = self.prepare(
                self.request(), self.profile(network=True), self.runtime_config(), self.home, self.agent_dir
            )
        tools = plan.argv[plan.argv.index("--tools") + 1].split(",")
        allowed = plan.argv[plan.argv.index("--allowedTools") + 1].split(",")
        disallowed = plan.argv[plan.argv.index("--disallowedTools") + 1].split(",")
        for tool in ("WebFetch", "WebSearch"):
            self.assertIn(tool, tools)
            self.assertIn(tool, allowed)
            self.assertNotIn(tool, disallowed)

    def test_declared_plugins_are_loaded_by_path_without_widening_tools(self) -> None:
        plugin = self.root / "compressor"
        (plugin / "hooks").mkdir(parents=True)
        (plugin / "hooks" / "hooks.json").write_text("{}", encoding="utf-8")
        config = self.runtime_config(plugins=(plugin,))

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            bare = self.prepare(
                self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir
            )
            plan = self.prepare(
                self.request(), self.profile(), config, self.home, self.agent_dir
            )

        self.assertNotIn("--plugin-dir", bare.argv)
        argv = list(plan.argv)
        self.assertEqual(argv[argv.index("--plugin-dir") + 1], str(plugin))
        # A plugin never buys the child a tool it was not already granted.
        tools = argv[argv.index("--tools") + 1].split(",")
        allowed = argv[argv.index("--allowedTools") + 1].split(",")
        self.assertEqual(set(tools), {"Read", "Grep", "Glob"})
        self.assertNotIn("Bash", allowed)

    def test_materialize_fingerprint_tracks_declared_plugins(self) -> None:
        plugin = self.root / "compressor"
        plugin.mkdir()
        bare = self.materialize(self.runtime_config(), self.home)
        declared = self.materialize(self.runtime_config(plugins=(plugin,)), self.home)
        self.assertNotEqual(bare, declared)
        # Plugins are loaded from their own directory, never copied into the
        # generated home, so no plugin file is materialized for them.
        self.assertFalse((self.home / "plugins" / "compressor").exists())

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
        self.assertNotIn("Bash", tools)
        self.assertFalse(any(item.startswith(("Write(", "Edit(")) for item in allowed))
        self.assertNotIn("Bash", allowed)
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

    def test_prepare_exposes_the_skill_tool_only_when_skills_are_configured(self) -> None:
        # --plugin-dir registers the skills, but the child can neither see
        # nor invoke them unless the built-in Skill tool is allowed too
        # (live regression: a child that listed its MCP server's prompt
        # skills and none of the three configured ones).
        skill_dir = self.root / "skills" / "claude" / "delegate"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text("Delegate work.", encoding="utf-8")
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            config = self.runtime_config(skills=("delegate",))
            self.materialize(config, self.home)
            plan = self.prepare(self.request(), self.profile(), config, self.home, self.agent_dir)
            argv = plan.argv
            self.assertIn("Skill", argv[argv.index("--tools") + 1].split(","))
            self.assertIn("Skill", argv[argv.index("--allowedTools") + 1].split(","))
            self.assertIn(str(self.home / "plugins" / "delegate"), argv)

            bare = self.prepare(
                self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir
            )
        self.assertNotIn("Skill", bare.argv[bare.argv.index("--tools") + 1].split(","))
        self.assertNotIn("Skill", bare.argv[bare.argv.index("--allowedTools") + 1].split(","))

    # -- keychain-sourced OAuth (T050) --------------------------------------

    OAUTH_NAMES = ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY")

    def credential(self, token: str, *, expires_in_seconds: float) -> str:
        """One Keychain payload shaped exactly like the CLI's own entry."""

        return json.dumps(
            {
                "claudeAiOauth": {
                    "accessToken": token,
                    "refreshToken": "sk-ant-ort-refresh",
                    # The CLI stores milliseconds since the epoch.
                    "expiresAt": int((time.time() + expires_in_seconds) * 1000),
                    "scopes": ["user:inference"],
                    "subscriptionType": "max",
                }
            }
        )

    def fake_keychain(self, *, initial: str | None) -> tuple[Path, Path, Path]:
        """Install a fake ``security`` binary over a writable credential file.

        Returns the credential file, the fake ``claude`` binary whose only
        job is to rewrite that file the way a real refresh would, and the
        file counting how many times the refresh actually ran.
        """

        cred = self.root / "credentials.json"
        if initial is not None:
            cred.write_text(initial, encoding="utf-8")
        calls = self.root / "refresh-calls"
        security = self.root / "fake-security"
        # Absolute tool paths: the suite pins PATH to /usr/bin.
        security.write_text(
            f'#!/bin/sh\n[ -f "{cred}" ] || exit 44\nexec /bin/cat "{cred}"\n', encoding="utf-8"
        )
        security.chmod(0o755)
        binary = self.root / "fake-claude"
        binary.write_text(
            "#!/bin/sh\n"
            f'echo x >> "{calls}"\n'
            f"/bin/cat > \"{cred}\" <<'JSON'\n"
            f"{self.credential('sk-ant-oat-refreshed', expires_in_seconds=86400)}\n"
            "JSON\n"
            "echo pong.\n",
            encoding="utf-8",
        )
        binary.chmod(0o755)
        patcher = patch.object(claude_auth, "_SECURITY_BIN", str(security))
        patcher.start()
        self.addCleanup(patcher.stop)
        return cred, binary, calls

    def refresh_count(self, calls: Path) -> int:
        return len(calls.read_text(encoding="utf-8").splitlines()) if calls.exists() else 0

    def test_prepare_reads_a_live_keychain_token_without_refreshing(self) -> None:
        _, binary, calls = self.fake_keychain(
            initial=self.credential("sk-ant-oat-live", expires_in_seconds=86400)
        )
        config = self.runtime_config(
            binary=binary, auth=RuntimeAuthConfig("environment", names=self.OAUTH_NAMES)
        )
        plan = self.prepare(self.request(), self.profile(), config, self.home, self.agent_dir)
        self.assertEqual(plan.environment["CLAUDE_CODE_OAUTH_TOKEN"], "sk-ant-oat-live")
        self.assertEqual(self.refresh_count(calls), 0)
        # The token is registered as a secret, so the stream sanitizer strips it.
        self.assertIn("CLAUDE_CODE_OAUTH_TOKEN", plan.adapter_state["secret_env_names"])
        self.assertNotIn("sk-ant-oat-live", " ".join(plan.argv))

    def test_prepare_refreshes_a_stale_keychain_token_exactly_once(self) -> None:
        cred, binary, calls = self.fake_keychain(
            initial=self.credential("sk-ant-oat-stale", expires_in_seconds=-60)
        )
        config = self.runtime_config(
            binary=binary, auth=RuntimeAuthConfig("environment", names=self.OAUTH_NAMES)
        )
        plan = self.prepare(self.request(), self.profile(), config, self.home, self.agent_dir)
        self.assertEqual(plan.environment["CLAUDE_CODE_OAUTH_TOKEN"], "sk-ant-oat-refreshed")
        self.assertEqual(self.refresh_count(calls), 1)
        self.assertNotIn("sk-ant-oat-stale", cred.read_text(encoding="utf-8"))

    def test_prepare_refreshes_once_when_the_keychain_entry_is_absent(self) -> None:
        _, binary, calls = self.fake_keychain(initial=None)
        config = self.runtime_config(
            binary=binary, auth=RuntimeAuthConfig("environment", names=self.OAUTH_NAMES)
        )
        plan = self.prepare(self.request(), self.profile(), config, self.home, self.agent_dir)
        self.assertEqual(plan.environment["CLAUDE_CODE_OAUTH_TOKEN"], "sk-ant-oat-refreshed")
        self.assertEqual(self.refresh_count(calls), 1)

    def test_prepare_fails_with_auth_error_when_one_refresh_does_not_renew(self) -> None:
        _, binary, calls = self.fake_keychain(
            initial=self.credential("sk-ant-oat-stale", expires_in_seconds=-60)
        )
        # A refresh that reports success but renews nothing: exactly one
        # attempt, then a clear failure -- never a silent retry loop.
        binary.write_text(f'#!/bin/sh\necho x >> "{calls}"\nexit 0\n', encoding="utf-8")
        binary.chmod(0o755)
        config = self.runtime_config(
            binary=binary, auth=RuntimeAuthConfig("environment", names=self.OAUTH_NAMES)
        )
        with self.assertRaisesRegex(AuthError, "missing or expired"):
            self.prepare(self.request(), self.profile(), config, self.home, self.agent_dir)
        self.assertEqual(self.refresh_count(calls), 1)

    def test_prepare_prefers_an_explicit_env_var_over_the_keychain(self) -> None:
        _, binary, calls = self.fake_keychain(
            initial=self.credential("sk-ant-oat-live", expires_in_seconds=86400)
        )
        config = self.runtime_config(
            binary=binary, auth=RuntimeAuthConfig("environment", names=self.OAUTH_NAMES)
        )
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-explicit"}):
            plan = self.prepare(self.request(), self.profile(), config, self.home, self.agent_dir)
        self.assertEqual(plan.environment["ANTHROPIC_API_KEY"], "sk-explicit")
        self.assertNotIn("CLAUDE_CODE_OAUTH_TOKEN", plan.environment)
        self.assertEqual(self.refresh_count(calls), 0)

    def test_keychain_refresh_child_never_inherits_an_auth_variable(self) -> None:
        cred, _, _ = self.fake_keychain(initial=None)
        dump = self.root / "refresh-env"
        binary = self.root / "env-dump-claude"
        binary.write_text(f'#!/bin/sh\nenv > "{dump}"\nexit 1\n', encoding="utf-8")
        binary.chmod(0o755)
        with patch.dict(
            "os.environ",
            {
                "CLAUDE_CODE_OAUTH_TOKEN": "sk-inherited",
                "ANTHROPIC_API_KEY": "sk-inherited",
                "ANTHROPIC_AUTH_TOKEN": "sk-inherited",
            },
        ):
            with self.assertRaises(AuthError):
                claude_auth.resolve_token(binary)
        rendered = dump.read_text(encoding="utf-8")
        for name in claude_auth.AUTH_ENV_NAMES:
            self.assertNotIn(name, rendered)
        self.assertNotIn("sk-inherited", rendered)

    def test_probe_reports_a_keychain_credential_when_the_env_is_bare(self) -> None:
        self.fake_keychain(initial=self.credential("sk-ant-oat-live", expires_in_seconds=86400))
        config = self.runtime_config(
            binary=Path("/bin/echo"), auth=RuntimeAuthConfig("environment", names=self.OAUTH_NAMES)
        )
        self.assertTrue(self.adapter.probe(config, self.home).authenticated)

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
