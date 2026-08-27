import inspect
import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import Capability, LaunchPlan, RuntimeAdapter
from agent_run.adapters.codex.adapter import ADAPTER, _rollout_limits
from agent_run.config import McpConfig, RuntimeAuthConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.domain import StartRequest
from agent_run.errors import PathEscapeError, ValidationError
from agent_run.profiles import AgentProfile


class CodexAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self._stack = []
        self.agent_run_root = Path(self._mkdtemp())
        (self.agent_run_root / "skills" / "codex" / "demo").mkdir(parents=True)
        (self.agent_run_root / "skills" / "codex" / "demo" / "SKILL.md").write_text(
            "demo skill", encoding="utf-8"
        )
        self._env_patch = patch.dict(os.environ, {"AGENT_RUN_HOME": str(self.agent_run_root)})
        self._env_patch.start()
        self.addCleanup(self._env_patch.stop)
        self.home = self.agent_run_root / "runtimes" / "codex" / "home"
        self.auth_source_dir = Path(self._mkdtemp()).resolve()
        self.auth_source = self.auth_source_dir / "auth.json"
        self.auth_source.write_text("{}", encoding="utf-8")
        self.workdir = Path(self._mkdtemp()).resolve()
        self.cache_dir = self.home / "cache"
        self.cache_dir.mkdir(parents=True)
        self.write_model_cache(
            """{"models": [
                {"id": "gpt-5.6-sol", "description": "fast", "efforts": ["low", "high"]},
                {"id": "gpt-5.6-terra", "description": "deep"}
            ]}"""
        )

    def write_model_cache(self, text: str) -> None:
        (self.cache_dir / "models.json").write_text(text, encoding="utf-8")

    def _mkdtemp(self) -> str:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return directory.name

    def runtime_config(self, **overrides) -> RuntimeConfig:
        values = dict(
            enabled=True,
            adapter="agent_run.adapters.codex.adapter:ADAPTER",
            binary=Path("/bin/echo"),
            home=self.home,
            models=("gpt-5.6-sol", "gpt-5.6-terra"),
            skills=("demo",),
            mcp=(),
            auth=RuntimeAuthConfig("file_link", self.auth_source, "auth.json"),
            hooks=(RuntimeHookConfig("UserPromptSubmit", ("agent-run", "hook", "context")),),
        )
        values.update(overrides)
        return RuntimeConfig(**values)

    def resolved_mcp(self) -> dict[str, McpConfig]:
        return {"agent_lsp": McpConfig("stdio", Path("/bin/echo"), ("serve",), ("PATH",))}

    def write_ambient_config_with_mcp(self) -> None:
        """An ambient config the adapter must never read on its own."""

        (self.agent_run_root / "config.toml").write_text(
            """
schema_version = 1
[mcp.agent_lsp]
transport = "stdio"
command = "/bin/ls"
args = ["ambient"]
env_from = ["PATH"]
""",
            encoding="utf-8",
        )

    # -- describe / validate -------------------------------------------------

    def test_describe_reports_expected_capabilities(self) -> None:
        info = ADAPTER.describe()
        self.assertEqual(info.name, "codex")
        self.assertIn(Capability.STEER, info.capabilities)
        self.assertIn(Capability.MCP, info.capabilities)
        self.assertNotIn(Capability.OUTPUT_SCHEMA, info.capabilities)

    def test_adapter_matches_the_keyword_only_protocol_calls(self) -> None:
        for name in ("materialize", "prepare"):
            with self.subTest(method=name):
                current = inspect.signature(getattr(ADAPTER, name))
                contract = inspect.signature(getattr(RuntimeAdapter, name))
                parameter = current.parameters["mcp_servers"]
                self.assertEqual(parameter.kind, inspect.Parameter.KEYWORD_ONLY)
                self.assertIs(parameter.default, inspect.Parameter.empty)
                self.assertEqual(
                    list(current.parameters), list(contract.parameters)[1:]  # minus self
                )
        with self.assertRaises(TypeError):
            ADAPTER.materialize(self.runtime_config(), self.home)

    def test_validate_requires_file_link_auth_and_no_service_mode(self) -> None:
        ADAPTER.validate(self.runtime_config())
        with self.assertRaisesRegex(ValidationError, "file_link auth bridge"):
            ADAPTER.validate(self.runtime_config(auth=RuntimeAuthConfig("environment", names=("TOKEN",))))
        with self.assertRaisesRegex(ValidationError, "service_mode"):
            ADAPTER.validate(self.runtime_config(service_mode="managed"))

    # -- materialize ----------------------------------------------------------

    def test_materialize_writes_declared_assets_and_preserves_runtime_state(self) -> None:
        config = self.runtime_config(mcp=("agent_lsp",))
        digest_one = ADAPTER.materialize(config, self.home, mcp_servers=self.resolved_mcp())

        self.assertEqual((self.home / "skills" / "demo" / "SKILL.md").read_text(encoding="utf-8"), "demo skill")
        generated = (self.home / "config.toml").read_text(encoding="utf-8")
        self.assertNotIn("skills =", generated)
        self.assertEqual(
            [path.name for path in (self.home / "skills").iterdir()],
            ["demo"],
        )
        self.assertIn("[mcp_servers.agent_lsp]", generated)
        # Codex reads config-level hooks as per-event array-of-tables groups and
        # runs none of them without a matching trust digest.
        self.assertIn("[[hooks.UserPromptSubmit]]", generated)
        self.assertIn("[[hooks.UserPromptSubmit.hooks]]", generated)
        self.assertIn('type = "command"', generated)
        self.assertIn(
            f'[hooks.state."{self.home / "config.toml"}:user_prompt_submit:0:0"]',
            generated,
        )
        self.assertIn("trusted_hash = \"sha256:", generated)
        bridge = self.home / "auth.json"
        self.assertTrue(bridge.is_symlink())
        self.assertEqual(bridge.resolve(strict=True), self.auth_source.resolve(strict=True))

        runtime_owned = self.home / "sessions" / "trust.json"
        runtime_owned.parent.mkdir(parents=True)
        runtime_owned.write_text('{"trusted": true}', encoding="utf-8")

        changed_config = self.runtime_config(
            mcp=("agent_lsp",),
            hooks=(RuntimeHookConfig("PostToolUse", ("agent-run", "hook", "bind"), matcher="^start$"),),
        )
        digest_two = ADAPTER.materialize(changed_config, self.home, mcp_servers=self.resolved_mcp())

        self.assertNotEqual(digest_one, digest_two)
        self.assertEqual(runtime_owned.read_text(encoding="utf-8"), '{"trusted": true}')
        self.assertIn("PostToolUse", (self.home / "config.toml").read_text(encoding="utf-8"))

    def make_plugin(self, **manifest) -> Path:
        """A plugin whose PreToolUse hook matches the real compressor byte for byte."""

        directory = Path(self._mkdtemp()).resolve() / "compressor"
        (directory / ".codex-plugin").mkdir(parents=True)
        (directory / "hooks").mkdir()
        (directory / ".git").mkdir()
        (directory / ".git" / "HEAD").write_text("ref: refs/heads/main", encoding="utf-8")
        payload = {"name": "agent-pipline-compressor", "version": "0.1.0+codex.20260825100009"}
        payload.update(manifest)
        (directory / ".codex-plugin" / "plugin.json").write_text(
            json.dumps(payload), encoding="utf-8"
        )
        (directory / "hooks" / "hooks.json").write_text(
            json.dumps(
                {
                    "hooks": {
                        "PreToolUse": [
                            {
                                "matcher": "^Bash$",
                                "hooks": [
                                    {
                                        "type": "command",
                                        "command": (
                                            "/Library/Frameworks/Python.framework/Versions/"
                                            "3.8/bin/python3 \"${PLUGIN_ROOT:-"
                                            "$CLAUDE_PLUGIN_ROOT}/hooks/pre_tool.py\""
                                        ),
                                        "timeout": 10,
                                    }
                                ],
                            }
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )
        (directory / "hooks" / "pre_tool.py").write_text("print(1)", encoding="utf-8")
        return directory

    def test_materialize_installs_declared_plugins_with_codex_own_trust_digest(self) -> None:
        plugin = self.make_plugin()
        config = self.runtime_config(plugins=(plugin,))
        digest = ADAPTER.materialize(config, self.home, mcp_servers={})

        installed = (
            self.home
            / "plugins"
            / "cache"
            / "personal"
            / "agent-pipline-compressor"
            / "0.1.0+codex.20260825100009"
        )
        # Real files, not a bridge: codex reports a symlinked version
        # directory as "not installed" and never loads its hooks.
        self.assertFalse(installed.is_symlink())
        self.assertEqual((installed / "hooks" / "pre_tool.py").read_text(encoding="utf-8"), "print(1)")
        self.assertFalse((installed / ".git").exists())

        catalog = json.loads(
            (self.home / ".agents" / "plugins" / "marketplace.json").read_text(encoding="utf-8")
        )
        self.assertEqual(catalog["name"], "personal")
        self.assertEqual(
            catalog["plugins"][0]["source"]["path"],
            "./plugins/cache/personal/agent-pipline-compressor/0.1.0+codex.20260825100009",
        )

        generated = (self.home / "config.toml").read_text(encoding="utf-8")
        self.assertIn('[plugins."agent-pipline-compressor@personal"]\nenabled = true', generated)
        # This exact digest was produced by codex itself and read back out of
        # the owner's live ~/.codex/config.toml; it pins the algorithm.
        self.assertIn(
            '[hooks.state."agent-pipline-compressor@personal:hooks/hooks.json:pre_tool_use:0:0"]\n'
            'trusted_hash = "sha256:'
            '12d96d056f33b8d47b4421084706b4a0a4e8f368a16911bd64a0290344017231"',
            generated,
        )
        self.assertNotEqual(digest, ADAPTER.materialize(self.runtime_config(), self.home, mcp_servers={}))

    def test_materialize_refuses_plugin_hooks_it_cannot_trust(self) -> None:
        no_manifest = Path(self._mkdtemp()).resolve()
        with self.assertRaisesRegex(ValidationError, "no usable manifest"):
            ADAPTER.materialize(
                self.runtime_config(plugins=(no_manifest,)), self.home, mcp_servers={}
            )

        plugin = self.make_plugin()
        hooks = plugin / "hooks" / "hooks.json"
        hooks.write_text(
            json.dumps({"hooks": {"PreToolUse": [{"hooks": [{"type": "prompt", "prompt": "hi"}]}]}}),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValidationError, "not a command hook"):
            ADAPTER.materialize(self.runtime_config(plugins=(plugin,)), self.home, mcp_servers={})

        hooks.write_text(json.dumps({"hooks": {"Bogus": []}}), encoding="utf-8")
        with self.assertRaisesRegex(ValidationError, "unsupported hook event"):
            ADAPTER.materialize(self.runtime_config(plugins=(plugin,)), self.home, mcp_servers={})

    def test_materialize_is_deterministic_for_identical_input(self) -> None:
        config = self.runtime_config()
        first = ADAPTER.materialize(config, self.home, mcp_servers={})
        second = ADAPTER.materialize(config, self.home, mcp_servers={})
        self.assertEqual(first, second)

    def test_materialize_uses_only_each_explicit_service_skill_root(self) -> None:
        config = self.runtime_config()
        generated = []
        for index, text in enumerate(("first isolated", "second isolated")):
            service_home = Path(self._mkdtemp()).resolve()
            skills_root = service_home / "skills" / "codex"
            skill = skills_root / "demo" / "SKILL.md"
            skill.parent.mkdir(parents=True)
            skill.write_text(text, encoding="utf-8")
            runtime_home = service_home / "runtimes" / "codex" / "home"
            ADAPTER.materialize(
                config,
                runtime_home,
                mcp_servers={},
                skills_root=skills_root,
            )
            generated.append(
                (runtime_home / "skills" / "demo" / "SKILL.md").read_text(
                    encoding="utf-8"
                )
            )
        self.assertEqual(generated, ["first isolated", "second isolated"])

        missing = Path(self._mkdtemp()).resolve() / "skills" / "codex"
        with self.assertRaisesRegex(ValidationError, "skill is not available"):
            ADAPTER.materialize(
                config, self.home, mcp_servers={}, skills_root=missing
            )

    def test_materialize_fails_for_missing_skill_or_unresolved_mcp(self) -> None:
        with self.assertRaisesRegex(ValidationError, "codex skill is not available"):
            ADAPTER.materialize(
                self.runtime_config(skills=("missing",)), self.home, mcp_servers={}
            )
        with self.assertRaisesRegex(ValidationError, "codex mcp reference is not configured"):
            ADAPTER.materialize(
                self.runtime_config(skills=(), mcp=("agent_lsp",)), self.home, mcp_servers={}
            )

    def test_materialize_ignores_ambient_config_and_uses_only_resolved_mcp(self) -> None:
        """The mcp definition exists in the ambient config, but was not handed in."""

        self.write_ambient_config_with_mcp()
        config = self.runtime_config(skills=(), mcp=("agent_lsp",))
        with self.assertRaisesRegex(ValidationError, "codex mcp reference is not configured"):
            ADAPTER.materialize(config, self.home, mcp_servers={})

        ADAPTER.materialize(config, self.home, mcp_servers=self.resolved_mcp())
        generated = (self.home / "config.toml").read_text(encoding="utf-8")
        # The ambient file declares /bin/ls; only the handed-in definition is used.
        self.assertIn('command = "/bin/echo"', generated)
        self.assertNotIn("/bin/ls", generated)
        self.assertIn('args = ["serve"]', generated)

    def test_materialize_requires_a_mapping_of_resolved_mcp_servers(self) -> None:
        with self.assertRaisesRegex(ValidationError, "resolved mcp_servers mapping"):
            ADAPTER.materialize(self.runtime_config(), self.home, mcp_servers=None)
        with self.assertRaisesRegex(ValidationError, "is not resolved"):
            ADAPTER.materialize(
                self.runtime_config(skills=(), mcp=("agent_lsp",)),
                self.home,
                mcp_servers={"agent_lsp": "/bin/echo"},
            )

    def test_materialize_prunes_only_adapter_owned_stale_skills(self) -> None:
        ADAPTER.materialize(self.runtime_config(), self.home, mcp_servers={})
        stale = self.home / "skills" / "gone"
        stale.mkdir(parents=True)
        (stale / "SKILL.md").write_text("stale skill", encoding="utf-8")
        keeper = self.home / "skills" / "runtime-owned"
        keeper.mkdir(parents=True)
        (keeper / "notes.json").write_text("{}", encoding="utf-8")
        held_open = self.home / "skills" / "held"
        held_open.mkdir(parents=True)
        (held_open / "SKILL.md").write_text("stale skill", encoding="utf-8")
        (held_open / "runtime.log").write_text("live", encoding="utf-8")

        ADAPTER.materialize(self.runtime_config(), self.home, mcp_servers={})

        self.assertFalse(stale.exists())
        self.assertTrue((keeper / "notes.json").is_file())
        self.assertFalse((held_open / "SKILL.md").exists())
        self.assertTrue((held_open / "runtime.log").is_file())
        self.assertTrue((self.home / "skills" / "demo" / "SKILL.md").is_file())

    def test_materialize_refuses_a_symlinked_skills_root(self) -> None:
        elsewhere = Path(self._mkdtemp())
        self.home.mkdir(parents=True, exist_ok=True)
        (self.home / "skills").symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaisesRegex(PathEscapeError, "skills root must not be a symlink"):
            ADAPTER.materialize(self.runtime_config(skills=()), self.home, mcp_servers={})

    # -- probe ------------------------------------------------------------

    def test_probe_reports_health_without_live_calls(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home, mcp_servers={})
        health = ADAPTER.probe(config, self.home)
        self.assertTrue(health.available)
        self.assertTrue(health.authenticated)

        missing_binary = self.runtime_config(binary=Path("/no/such/codex-binary"))
        unhealthy = ADAPTER.probe(missing_binary, self.home)
        self.assertFalse(unhealthy.available)
        self.assertIsNotNone(unhealthy.reason)

    def test_probe_refuses_a_bridge_pointing_away_from_the_configured_source(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home, mcp_servers={})
        impostor = self.auth_source_dir / "other-auth.json"
        impostor.write_text("{}", encoding="utf-8")
        bridge = self.home / "auth.json"
        bridge.unlink()
        bridge.symlink_to(impostor)

        health = ADAPTER.probe(config, self.home)
        self.assertFalse(health.authenticated)

    def test_probe_refuses_a_regular_file_in_place_of_the_bridge(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home, mcp_servers={})
        bridge = self.home / "auth.json"
        bridge.unlink()
        bridge.write_text("{}", encoding="utf-8")

        self.assertFalse(ADAPTER.probe(config, self.home).authenticated)

    # -- models -------------------------------------------------------------

    def test_models_intersects_allowlist_with_isolated_cache(self) -> None:
        config = self.runtime_config()
        self.write_model_cache(
            """{"models": [
                {"id": "gpt-5.6-sol", "description": "fast", "efforts": ["low", "high"]},
                {"id": "not-allowlisted", "description": "stale roster entry"}
            ]}"""
        )
        models = ADAPTER.models(config, self.home)
        self.assertEqual([model.id for model in models], ["gpt-5.6-sol"])
        sol = models[0]
        self.assertEqual(sol.description, "fast")
        self.assertEqual(sol.efforts, ("low", "high"))

    def test_models_normalizes_real_cache_and_keeps_present_cache_strict(self) -> None:
        config = self.runtime_config()
        self.write_model_cache(
            """{"models": [
                {"slug": "gpt-5.6-sol", "description": "real cache", "supported_reasoning_levels": [
                    {"effort": "low"}, {"effort": "high"}, {"effort": "high"}
                ]},
                {"slug": "not-allowlisted", "supported_reasoning_levels": [{"effort": "low"}]}
            ]}"""
        )
        models = ADAPTER.models(config, self.home)
        self.assertEqual([model.id for model in models], ["gpt-5.6-sol"])
        self.assertEqual(models[0].description, "real cache")
        self.assertEqual(models[0].efforts, ("low", "high"))

    def test_models_without_cache_falls_back_to_config_only(self) -> None:
        (self.cache_dir / "models.json").unlink()
        ambient = self.agent_run_root / "cache" / "models.json"
        ambient.parent.mkdir(exist_ok=True)
        ambient.write_text(
            '{"models": [{"slug": "ambient-only"}]}', encoding="utf-8"
        )
        models = ADAPTER.models(self.runtime_config(), self.home)
        self.assertEqual(
            [(model.id, model.description, model.efforts) for model in models],
            [
                ("gpt-5.6-sol", "", ()),
                ("gpt-5.6-terra", "", ()),
            ],
        )

    def test_models_with_unreadable_cache_falls_back_to_config_only(self) -> None:
        path = self.cache_dir / "models.json"
        invalid = (b'{"models": [{"id": "\xff\xfe"}]}', b"not json at all", b'{"models": {}}')
        for content in invalid:
            with self.subTest(content=content):
                path.write_bytes(content)
                models = ADAPTER.models(self.runtime_config(), self.home)
                self.assertEqual(
                    [model.id for model in models],
                    ["gpt-5.6-sol", "gpt-5.6-terra"],
                )
                self.assertTrue(
                    all(not model.description and not model.efforts for model in models)
                )

    # -- limits -------------------------------------------------------------

    def write_rollout(
        self, name: str, text: str, *, root: Path | None = None, mtime: float | None = None
    ) -> Path:
        session = (root or self.home) / "sessions" / "2026" / "08" / "26"
        session.mkdir(parents=True, exist_ok=True)
        path = session / f"rollout-{name}.jsonl"
        path.write_text(text, encoding="utf-8")
        if mtime is not None:
            os.utime(path, (mtime, mtime))
        return path

    def rollout_event(self, timestamp: str, *, secret: str = "not-sensitive") -> str:
        return (
            f'{{"timestamp":"{timestamp}","type":"event_msg","payload":{{'
            '"type":"token_count","rate_limits":{'
            '"primary":{"used_percent":25,"window_minutes":300,'
            '"resets_at":4102444800,"target":"gpt-5.6-sol"},'
            '"secondary":{"used_percent":125,"window_minutes":10080,'
            '"resets_at":4102444800,"target":"sk-unsafe-target"},'
            '"individual_limit":{"used_percent":-5,"window_minutes":60,'
            '"resets_at":"2099-12-31T00:00:00Z",'
            '"limit_name":"gpt-5.6-terra"}},'
            f'"ignored":"{secret}"}}}}'
        )

    @staticmethod
    def iso_time(epoch: float) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))

    #: One ``token_count`` line copied verbatim out of a real codex rollout on
    #: this machine -- ``.../canaries/M011-final/home/runtimes/codex/home/
    #: sessions/2026/08/26/rollout-2026-08-26T20-52-58-*.jsonl``, the newest of
    #: the 13 rollouts a live M011 run left behind. It is here because the
    #: engine's live shape is narrower than the synthetic one above: today it
    #: fills ``primary`` only, with ``secondary`` and ``individual_limit`` both
    #: null, and it carries ``credits`` / ``plan_type`` / ``limit_id`` keys the
    #: parser must ignore. So one real run yields exactly one weekly sample,
    #: and 100 - 57 = 43% is the number the collector recorded that day.
    LIVE_ROLLOUT_LINE = (
        '{"timestamp":"2026-08-26T18:53:02.949Z","type":"event_msg","payload":'
        '{"type":"token_count","info":{"total_token_usage":{"input_tokens":16444,'
        '"cached_input_tokens":9984,"cache_write_input_tokens":0,"output_tokens":7,'
        '"reasoning_output_tokens":0,"total_tokens":16451},"model_context_window":258400},'
        '"rate_limits":{"limit_id":"codex","limit_name":null,'
        '"primary":{"used_percent":57.0,"window_minutes":10080,"resets_at":1788272105},'
        '"secondary":null,"credits":{"has_credits":false,"unlimited":false,"balance":"0"},'
        '"individual_limit":null,"spend_control_reached":null,"plan_type":"pro",'
        '"rate_limit_reached_type":null}}}'
    )

    def test_limits_materialize_from_a_real_engine_rollout(self) -> None:
        """A real codex run's own rollout file yields a usable weekly sample."""

        observed = datetime(2026, 8, 26, 18, 53, 2, 949000, tzinfo=timezone.utc)
        self.write_rollout("live", self.LIVE_ROLLOUT_LINE + "\n")

        samples = _rollout_limits(self.home, ("gpt-5.6-sol",), observed.timestamp() + 60)

        self.assertEqual(len(samples), 1)
        sample = samples[0]
        self.assertEqual((sample.lane, sample.window), ("primary", "weekly"))
        self.assertEqual(sample.remaining_percent, 43.0)
        self.assertEqual(sample.source, "isolated_rollout_evidence")
        self.assertEqual(sample.observed_at, observed)
        self.assertEqual(
            sample.reset_at, datetime.fromtimestamp(1788272105, tz=timezone.utc)
        )
        # Past the freshness bound the same file must degrade, never fabricate.
        stale = _rollout_limits(
            self.home, ("gpt-5.6-sol",), observed.timestamp() + 86400
        )
        self.assertEqual(len(stale), 1)
        self.assertIsNone(stale[0].remaining_percent)
        self.assertEqual(stale[0].source, "unknown")
        self.assertEqual(stale[0].reset_at, sample.reset_at)

    def test_limits_missing_evidence_is_empty(self) -> None:
        self.assertEqual(ADAPTER.limits(self.runtime_config(), self.home), ())

    def test_limits_survives_unreadable_evidence(self) -> None:
        (self.cache_dir / "rollout_evidence.json").write_bytes(b"\xff\xfe not utf-8")
        self.assertEqual(ADAPTER.limits(self.runtime_config(), self.home), ())

    def test_limits_precomputed_evidence_wins_over_isolated_rollout(self) -> None:
        now = time.time()
        (self.cache_dir / "rollout_evidence.json").write_text(
            f'{{"samples":[{{"lane":"primary","window":"5h",'
            f'"remaining_percent":31,"observed_at":{now}}}]}}',
            encoding="utf-8",
        )
        self.write_rollout("newer", self.rollout_event(self.iso_time(now)))

        samples = ADAPTER.limits(self.runtime_config(), self.home)

        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].remaining_percent, 31)
        self.assertEqual(samples[0].source, "rollout_evidence")

    def test_limits_isolated_rollout_yields_three_normalized_samples(self) -> None:
        secret = "raw-secret-must-not-escape"
        self.write_rollout("valid", self.rollout_event(self.iso_time(time.time()), secret=secret))

        samples = ADAPTER.limits(self.runtime_config(), self.home)

        self.assertEqual(
            [(sample.lane, sample.window, sample.remaining_percent) for sample in samples],
            [
                ("primary", "session_5h", 75.0),
                ("secondary", "weekly", 0.0),
                ("individual_limit", "model_weekly", 100.0),
            ],
        )
        self.assertEqual(
            [sample.target for sample in samples],
            ["gpt-5.6-sol", None, "gpt-5.6-terra"],
        )
        self.assertEqual({sample.source for sample in samples}, {"isolated_rollout_evidence"})
        self.assertTrue(all(sample.reset_at is not None for sample in samples))
        self.assertNotIn(secret, repr(samples))
        self.assertNotIn("sk-unsafe-target", repr(samples))

    def test_limits_invalid_precomputed_evidence_falls_back_to_rollout(self) -> None:
        (self.cache_dir / "rollout_evidence.json").write_text("not json", encoding="utf-8")
        self.write_rollout("valid", self.rollout_event(self.iso_time(time.time())))

        self.assertEqual(len(ADAPTER.limits(self.runtime_config(), self.home)), 3)

    def test_limits_ignores_ambient_global_rollout_lookalikes(self) -> None:
        ambient_home = Path(self._mkdtemp())
        event = self.rollout_event(self.iso_time(time.time()))
        self.write_rollout("global", event, root=ambient_home / ".codex")
        self.write_rollout("crew", event, root=ambient_home / ".codex-crew")

        with patch.dict(os.environ, {"HOME": str(ambient_home)}):
            self.assertEqual(ADAPTER.limits(self.runtime_config(), self.home), ())

        (self.home / "sessions").symlink_to(
            ambient_home / ".codex" / "sessions", target_is_directory=True
        )
        self.assertEqual(ADAPTER.limits(self.runtime_config(), self.home), ())

    def test_limits_reads_only_a_bounded_rollout_tail(self) -> None:
        secret_prefix = "prefix-secret\n" * 30_000
        path = self.write_rollout(
            "large", secret_prefix + self.rollout_event(self.iso_time(time.time()))
        )
        self.assertGreater(path.stat().st_size, 262_144)

        samples = ADAPTER.limits(self.runtime_config(), self.home)

        self.assertEqual(len(samples), 3)
        self.assertNotIn("prefix-secret", repr(samples))

    def test_limits_newer_malformed_and_racing_files_fall_through(self) -> None:
        now = time.time()
        self.write_rollout(
            "older", self.rollout_event(self.iso_time(now)), mtime=now - 30
        )
        self.write_rollout(
            "malformed",
            '{"type":"event_msg","payload":{"type":"token_count",'
            '"rate_limits":"malformed-secret"',
            mtime=now - 20,
        )
        racing = self.write_rollout(
            "racing", self.rollout_event(self.iso_time(now)), mtime=now - 10
        )
        real_open = Path.open

        def racing_open(path, *args, **kwargs):
            if path == racing and args and args[0] == "rb":
                raise FileNotFoundError("rollout disappeared")
            return real_open(path, *args, **kwargs)

        with patch.object(Path, "open", racing_open):
            samples = ADAPTER.limits(self.runtime_config(), self.home)

        self.assertEqual(len(samples), 3)
        self.assertNotIn("malformed-secret", repr(samples))
        self.assertNotIn("rollout disappeared", repr(samples))

    def test_limits_considers_only_24_newest_rollout_files(self) -> None:
        now = time.time()
        self.write_rollout(
            "old-valid", self.rollout_event(self.iso_time(now)), mtime=now - 100
        )
        malformed = '{"type":"event_msg","payload":{"type":"token_count","rate_limits":'
        for index in range(24):
            self.write_rollout(f"new-{index}", malformed, mtime=now + index)

        self.assertEqual(ADAPTER.limits(self.runtime_config(), self.home), ())

    def test_limits_stale_isolated_rollout_has_unknown_remaining(self) -> None:
        self.write_rollout(
            "stale", self.rollout_event(self.iso_time(time.time() - 10_000))
        )

        samples = ADAPTER.limits(self.runtime_config(), self.home)

        self.assertEqual(len(samples), 3)
        self.assertTrue(all(sample.observed_at is not None for sample in samples))
        self.assertTrue(all(sample.remaining_percent is None for sample in samples))
        self.assertEqual({sample.source for sample in samples}, {"unknown"})

    def test_limits_marks_nonfinite_or_out_of_range_timestamps_unknown(self) -> None:
        (self.cache_dir / "rollout_evidence.json").write_text(
            """{"samples": [
                {"lane": "primary", "window": "5h", "remaining_percent": 42.0,
                 "observed_at": NaN, "reset_at": Infinity},
                {"lane": "primary", "window": "weekly", "remaining_percent": 10.0,
                 "observed_at": 1e30, "reset_at": -1e30}
            ]}""",
            encoding="utf-8",
        )
        samples = ADAPTER.limits(self.runtime_config(), self.home)
        self.assertEqual(len(samples), 2)
        for sample in samples:
            self.assertEqual(sample.source, "unknown")
            self.assertIsNone(sample.observed_at)
            self.assertIsNone(sample.reset_at)
            self.assertIsNone(sample.remaining_percent)

    def test_limits_marks_stale_or_missing_observations_unknown(self) -> None:
        cache_dir = self.cache_dir
        now = time.time()
        (cache_dir / "rollout_evidence.json").write_text(
            f"""{{"samples": [
                {{"lane": "primary", "window": "5h", "remaining_percent": 42.0, "observed_at": {now}}},
                {{"lane": "primary", "window": "weekly", "remaining_percent": 10.0, "observed_at": {now - 10000}}}
            ]}}""",
            encoding="utf-8",
        )
        samples = ADAPTER.limits(self.runtime_config(), self.home)
        fresh = next(sample for sample in samples if sample.window == "5h")
        stale = next(sample for sample in samples if sample.window == "weekly")
        self.assertEqual(fresh.source, "rollout_evidence")
        self.assertEqual(fresh.remaining_percent, 42.0)
        self.assertEqual(stale.source, "unknown")
        self.assertIsNone(stale.remaining_percent)

    # -- prepare -------------------------------------------------------------

    def start_request(self, **overrides) -> StartRequest:
        values = dict(
            runtime="codex",
            model="gpt-5.6-sol",
            profile="review",
            task="do the thing",
            workdir=self.workdir,
        )
        values.update(overrides)
        return StartRequest(**values)

    _UNSET = object()

    def prepare(self, request, profile, config=None, mcp_servers=_UNSET):
        runtime = self.runtime_config() if config is None else config
        return ADAPTER.prepare(
            request,
            profile,
            runtime,
            self.home,
            self.workdir,
            mcp_servers={} if mcp_servers is self._UNSET else mcp_servers,
        )

    def materialized(self, **overrides) -> RuntimeConfig:
        config = self.runtime_config(**overrides)
        ADAPTER.materialize(config, self.home, mcp_servers=self.resolved_mcp())
        return config

    def test_prepare_builds_launch_plan_for_a_valid_request(self) -> None:
        config = self.materialized()
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        plan = self.prepare(self.start_request(), profile, config)
        self.assertIsInstance(plan, LaunchPlan)
        self.assertEqual(plan.argv, (str(config.binary), "app-server"))
        self.assertEqual(plan.cwd, self.workdir)
        self.assertEqual(set(plan.environment), {"CODEX_HOME", "HOME", "PATH"})
        self.assertEqual(plan.adapter_state["sandbox_mode"], "read-only")
        self.assertEqual(plan.adapter_state["writable_roots"], ())
        self.assertEqual(
            sorted(plan.adapter_state["roots"]),
            sorted((str(self.workdir), str(self.auth_source_dir))),
        )

    def test_prepare_prefixes_the_task_with_the_profile_preamble(self) -> None:
        """codex takes no system prompt, so the preamble rides the first turn.

        Without it the child got the profile's sandbox but none of its wording
        -- and no role assignment could ever reach a codex child.
        """

        config = self.materialized()
        request = self.start_request()
        profile = AgentProfile("review", "Review only.", False, (self.auth_source_dir,))
        plan = self.prepare(request, profile, config)
        self.assertEqual(plan.initial_input, f"Review only.\n\n{request.task}")

    def test_prepare_pins_home_to_the_generated_home(self) -> None:
        """The engine reads ``~/.agents/skills`` from ``HOME``, not ``CODEX_HOME``.

        The launch environment fully replaces the parent's, so an absent
        ``HOME`` is not an unset one: the engine falls back to the passwd entry
        and picks up the operator's global skills (defect T20B).
        """

        config = self.materialized()
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with patch.dict(os.environ, {"HOME": "/Users/operator"}):
            plan = self.prepare(self.start_request(), profile, config)
        self.assertEqual(plan.environment["HOME"], str(self.home))
        self.assertEqual(plan.environment["CODEX_HOME"], str(self.home))
        self.assertFalse((self.home / ".agents" / "skills").exists())

    def test_prepare_unions_request_roots_into_a_normalized_antichain(self) -> None:
        config = self.materialized()
        base = Path(self._mkdtemp()).resolve()
        nested = base / "nested"
        nested.mkdir()
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        plan = self.prepare(
            self.start_request(read_roots=(base, nested)), profile, config
        )
        roots = plan.adapter_state["roots"]
        self.assertIn(str(base), roots)
        self.assertNotIn(str(nested), roots)  # collapsed into its parent
        self.assertIn(str(self.auth_source_dir), roots)
        self.assertIn(str(self.workdir), roots)

    def test_prepare_grants_write_root_only_when_the_request_asks_for_it(self) -> None:
        config = self.materialized()
        profile = AgentProfile("implement", "body", True, ())
        plan = self.prepare(self.start_request(write=True), profile, config)
        self.assertEqual(plan.adapter_state["sandbox_mode"], "workspace-write")
        self.assertEqual(plan.adapter_state["writable_roots"], (str(self.workdir),))

        read_only = self.prepare(self.start_request(write=False), profile, config)
        self.assertEqual(read_only.adapter_state["sandbox_mode"], "read-only")
        self.assertEqual(read_only.adapter_state["writable_roots"], ())

    def test_prepare_enables_network_in_the_child_sandbox(self) -> None:
        config = self.materialized()
        profile = AgentProfile("research", "body", False, (self.auth_source_dir,), True)
        plan = self.prepare(self.start_request(), profile, config)
        self.assertEqual(
            plan.adapter_state["sandbox"], {"type": "readOnly", "networkAccess": True}
        )

    def test_prepare_enables_the_post_execution_fallback_only_for_read_only_agents(self) -> None:
        """A read-only sandbox cannot spool before execution, so the outside
        PostToolUse hook has to do the replacing; a write-capable agent
        spools natively and must not get the fallback."""

        plugin = self.make_plugin()
        config = self.materialized(plugins=(plugin,))
        profile = AgentProfile("implement", "body", True, ())

        read_only = self.prepare(self.start_request(write=False), profile, config)
        self.assertEqual(read_only.environment["TOKENPIPE_POST_REPLACE"], "1")
        writable = self.prepare(self.start_request(write=True), profile, config)
        self.assertNotIn("TOKENPIPE_POST_REPLACE", writable.environment)

        # Undeclared means nothing is wired, for either sandbox mode.
        bare = self.prepare(self.start_request(write=False), profile, self.materialized())
        self.assertNotIn("TOKENPIPE_POST_REPLACE", bare.environment)

    def test_prepare_never_widens_the_writable_root_to_a_containing_read_root(self) -> None:
        config = self.materialized()
        base = Path(self._mkdtemp()).resolve()
        work = base / "work"
        work.mkdir()
        profile = AgentProfile("implement", "body", True, (base,))
        plan = ADAPTER.prepare(
            self.start_request(workdir=work, write=True),
            profile,
            config,
            self.home,
            self.workdir,
            mcp_servers={},
        )
        self.assertEqual(plan.adapter_state["roots"], (str(base),))
        self.assertEqual(plan.adapter_state["writable_roots"], (str(work),))

    def test_prepare_refuses_unknown_model(self) -> None:
        config = self.materialized()
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "model not allowed"):
            self.prepare(self.start_request(model="unlisted"), profile, config)

    def test_prepare_refuses_a_model_missing_from_the_discovered_roster(self) -> None:
        config = self.materialized()
        self.write_model_cache('{"models": [{"id": "gpt-5.6-terra"}]}')
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "not discovered"):
            self.prepare(self.start_request(), profile, config)

        (self.cache_dir / "models.json").unlink()
        plan = self.prepare(self.start_request(), profile, config)
        self.assertEqual(plan.adapter_state["model"], "gpt-5.6-sol")

    def test_prepare_refuses_output_schema(self) -> None:
        config = self.materialized()
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "output_schema"):
            self.prepare(self.start_request(output_schema={"type": "object"}), profile, config)

    def test_prepare_requires_an_explicitly_discovered_effort(self) -> None:
        config = self.materialized()
        self.write_model_cache(
            '{"models": [{"id": "gpt-5.6-sol", "efforts": ["low", "high"]},'
            ' {"id": "gpt-5.6-terra"}]}'
        )
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "effort"):
            self.prepare(self.start_request(effort="ultra"), profile, config)
        # A model whose roster entry lists no efforts cannot honour any effort.
        with self.assertRaisesRegex(ValidationError, "effort"):
            self.prepare(
                self.start_request(model="gpt-5.6-terra", effort="low"), profile, config
            )
        plan = self.prepare(self.start_request(effort="high"), profile, config)
        self.assertEqual(plan.adapter_state["effort"], "high")

    def test_prepare_bootstraps_without_cache_but_explicit_effort_stays_closed(self) -> None:
        config = self.materialized()
        (self.cache_dir / "models.json").unlink()
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))

        plan = self.prepare(self.start_request(effort=None), profile, config)
        self.assertEqual(plan.adapter_state["model"], "gpt-5.6-sol")
        with self.assertRaisesRegex(ValidationError, "effort"):
            self.prepare(self.start_request(effort="low"), profile, config)

    def test_prepare_refuses_write_beyond_profile_grant(self) -> None:
        config = self.materialized()
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "does not grant write"):
            self.prepare(self.start_request(write=True), profile, config)

    def test_prepare_refuses_no_filesystem_profile(self) -> None:
        config = self.materialized()
        profile = AgentProfile("blank", "body", False, ())
        with self.assertRaisesRegex(ValidationError, "no-filesystem profile"):
            self.prepare(self.start_request(), profile, config)

    def test_prepare_accepts_a_request_read_root_as_the_only_filesystem_grant(self) -> None:
        config = self.materialized()
        profile = AgentProfile("blank", "body", False, ())
        plan = self.prepare(
            self.start_request(read_roots=(self.auth_source_dir,)), profile, config
        )
        self.assertIn(str(self.auth_source_dir), plan.adapter_state["roots"])

    def test_prepare_refuses_unresolved_mcp_servers(self) -> None:
        config = self.materialized(mcp=("agent_lsp",))
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "codex mcp reference is not configured"):
            self.prepare(self.start_request(), profile, config, mcp_servers={})
        with self.assertRaisesRegex(ValidationError, "resolved mcp_servers mapping"):
            self.prepare(self.start_request(), profile, config, mcp_servers=None)
        plan = self.prepare(
            self.start_request(), profile, config, mcp_servers=self.resolved_mcp()
        )
        self.assertEqual(plan.adapter_state["mcp"], ("agent_lsp",))

    def test_prepare_requires_materialized_home(self) -> None:
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "not materialized"):
            self.prepare(self.start_request(), profile)

    def test_launch_timeout_terminates_the_partially_started_transport(self) -> None:
        from unittest.mock import Mock

        from agent_run.adapters.codex import adapter as codex_adapter

        transport = Mock()
        with patch.object(
            codex_adapter.app_server, "ProcessTransport", return_value=transport
        ), patch.object(
            codex_adapter.app_server,
            "start_session",
            side_effect=TimeoutError("startup timed out"),
        ):
            with self.assertRaisesRegex(TimeoutError, "startup timed out"):
                ADAPTER.launch(object(), object())
        transport.terminate.assert_called_once_with(1.0)


if __name__ == "__main__":
    unittest.main()
