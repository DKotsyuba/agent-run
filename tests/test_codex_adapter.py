import inspect
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import Capability, LaunchPlan, RuntimeAdapter
from agent_run.adapters.codex.adapter import ADAPTER
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
        self.assertIn("[mcp_servers.agent_lsp]", generated)
        self.assertIn("[[hooks]]", generated)
        self.assertIn("UserPromptSubmit", generated)
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

    def test_materialize_is_deterministic_for_identical_input(self) -> None:
        config = self.runtime_config()
        first = ADAPTER.materialize(config, self.home, mcp_servers={})
        second = ADAPTER.materialize(config, self.home, mcp_servers={})
        self.assertEqual(first, second)

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

    def test_models_without_cache_is_empty(self) -> None:
        (self.cache_dir / "models.json").unlink()
        self.assertEqual(ADAPTER.models(self.runtime_config(), self.home), ())

    def test_models_with_unreadable_cache_is_empty(self) -> None:
        (self.cache_dir / "models.json").write_bytes(b'{"models": [{"id": "\xff\xfe"}]}')
        self.assertEqual(ADAPTER.models(self.runtime_config(), self.home), ())
        (self.cache_dir / "models.json").write_text("not json at all", encoding="utf-8")
        self.assertEqual(ADAPTER.models(self.runtime_config(), self.home), ())
        (self.cache_dir / "models.json").write_text('{"models": {}}', encoding="utf-8")
        self.assertEqual(ADAPTER.models(self.runtime_config(), self.home), ())

    # -- limits -------------------------------------------------------------

    def test_limits_missing_evidence_is_empty(self) -> None:
        self.assertEqual(ADAPTER.limits(self.runtime_config(), self.home), ())

    def test_limits_survives_unreadable_evidence(self) -> None:
        (self.cache_dir / "rollout_evidence.json").write_bytes(b"\xff\xfe not utf-8")
        self.assertEqual(ADAPTER.limits(self.runtime_config(), self.home), ())

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
        self.assertEqual(set(plan.environment), {"CODEX_HOME", "PATH"})
        self.assertEqual(plan.adapter_state["sandbox_mode"], "read-only")
        self.assertEqual(plan.adapter_state["writable_roots"], ())
        self.assertEqual(
            sorted(plan.adapter_state["roots"]),
            sorted((str(self.workdir), str(self.auth_source_dir))),
        )

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
        with self.assertRaisesRegex(ValidationError, "not discovered"):
            self.prepare(self.start_request(), profile, config)

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


if __name__ == "__main__":
    unittest.main()
