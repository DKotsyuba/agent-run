import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import Capability, LaunchPlan
from agent_run.adapters.codex.adapter import ADAPTER
from agent_run.config import RuntimeAuthConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.domain import StartRequest
from agent_run.errors import ValidationError
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
        self.auth_source_dir = Path(self._mkdtemp())
        self.auth_source = self.auth_source_dir / "auth.json"
        self.auth_source.write_text("{}", encoding="utf-8")
        self.workdir = Path(self._mkdtemp()).resolve()

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

    def write_global_config_with_mcp(self) -> None:
        (self.agent_run_root / "config.toml").write_text(
            f"""
schema_version = 1
[mcp.agent_lsp]
transport = "stdio"
command = "/bin/echo"
args = ["serve"]
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

    def test_validate_requires_file_link_auth_and_no_service_mode(self) -> None:
        ADAPTER.validate(self.runtime_config())
        with self.assertRaisesRegex(ValidationError, "file_link auth bridge"):
            ADAPTER.validate(self.runtime_config(auth=RuntimeAuthConfig("environment", names=("TOKEN",))))
        with self.assertRaisesRegex(ValidationError, "service_mode"):
            ADAPTER.validate(self.runtime_config(service_mode="managed"))

    # -- materialize ----------------------------------------------------------

    def test_materialize_writes_declared_assets_and_preserves_runtime_state(self) -> None:
        self.write_global_config_with_mcp()
        config = self.runtime_config(mcp=("agent_lsp",))
        digest_one = ADAPTER.materialize(config, self.home)

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
        digest_two = ADAPTER.materialize(changed_config, self.home)

        self.assertNotEqual(digest_one, digest_two)
        self.assertEqual(runtime_owned.read_text(encoding="utf-8"), '{"trusted": true}')
        self.assertIn("PostToolUse", (self.home / "config.toml").read_text(encoding="utf-8"))

    def test_materialize_is_deterministic_for_identical_input(self) -> None:
        config = self.runtime_config()
        first = ADAPTER.materialize(config, self.home)
        second = ADAPTER.materialize(config, self.home)
        self.assertEqual(first, second)

    def test_materialize_fails_for_missing_skill_or_undeclared_mcp(self) -> None:
        with self.assertRaisesRegex(ValidationError, "codex skill is not available"):
            ADAPTER.materialize(self.runtime_config(skills=("missing",)), self.home)
        (self.agent_run_root / "config.toml").write_text("schema_version = 1\n", encoding="utf-8")
        with self.assertRaisesRegex(ValidationError, "codex mcp reference is not configured"):
            ADAPTER.materialize(self.runtime_config(skills=(), mcp=("agent_lsp",)), self.home)

    # -- probe ------------------------------------------------------------

    def test_probe_reports_health_without_live_calls(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home)
        health = ADAPTER.probe(config, self.home)
        self.assertTrue(health.available)
        self.assertTrue(health.authenticated)

        missing_binary = self.runtime_config(binary=Path("/no/such/codex-binary"))
        unhealthy = ADAPTER.probe(missing_binary, self.home)
        self.assertFalse(unhealthy.available)
        self.assertIsNotNone(unhealthy.reason)

    # -- models -------------------------------------------------------------

    def test_models_intersects_allowlist_with_isolated_cache(self) -> None:
        config = self.runtime_config()
        cache_dir = self.home / "cache"
        cache_dir.mkdir(parents=True)
        (cache_dir / "models.json").write_text(
            """{"models": [
                {"id": "gpt-5.6-sol", "description": "fast", "efforts": ["low", "high"]},
                {"id": "not-allowlisted", "description": "stale roster entry"}
            ]}""",
            encoding="utf-8",
        )
        models = ADAPTER.models(config, self.home)
        self.assertEqual([model.id for model in models], ["gpt-5.6-sol", "gpt-5.6-terra"])
        sol = models[0]
        self.assertEqual(sol.description, "fast")
        self.assertEqual(sol.efforts, ("low", "high"))
        terra = models[1]
        self.assertEqual(terra.description, "")
        self.assertEqual(terra.efforts, ())

    def test_models_without_cache_still_returns_allowlist(self) -> None:
        models = ADAPTER.models(self.runtime_config(), self.home)
        self.assertEqual([model.id for model in models], ["gpt-5.6-sol", "gpt-5.6-terra"])

    # -- limits -------------------------------------------------------------

    def test_limits_missing_evidence_is_empty(self) -> None:
        self.assertEqual(ADAPTER.limits(self.runtime_config(), self.home), ())

    def test_limits_marks_stale_or_missing_observations_unknown(self) -> None:
        cache_dir = self.home / "cache"
        cache_dir.mkdir(parents=True)
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

    def test_prepare_builds_launch_plan_for_a_valid_request(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home)
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        plan = ADAPTER.prepare(self.start_request(), profile, config, self.home, self.workdir)
        self.assertIsInstance(plan, LaunchPlan)
        self.assertEqual(plan.argv, (str(config.binary), "app-server"))
        self.assertEqual(plan.cwd, self.workdir)
        self.assertEqual(set(plan.environment), {"CODEX_HOME", "PATH"})
        self.assertEqual(plan.adapter_state["sandbox_mode"], "read-only")
        self.assertEqual(plan.adapter_state["writable_roots"], ())
        self.assertEqual(
            plan.adapter_state["roots"], (str(self.workdir), str(self.auth_source_dir))
        )

    def test_prepare_grants_write_root_only_when_profile_allows(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home)
        profile = AgentProfile("implement", "body", True, ())
        plan = ADAPTER.prepare(
            self.start_request(write=True), profile, config, self.home, self.workdir
        )
        self.assertEqual(plan.adapter_state["sandbox_mode"], "workspace-write")
        self.assertEqual(plan.adapter_state["writable_roots"], (str(self.workdir),))

    def test_prepare_refuses_unknown_model(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home)
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "model not allowed"):
            ADAPTER.prepare(
                self.start_request(model="unlisted"), profile, config, self.home, self.workdir
            )

    def test_prepare_refuses_output_schema(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home)
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "output_schema"):
            ADAPTER.prepare(
                self.start_request(output_schema={"type": "object"}),
                profile,
                config,
                self.home,
                self.workdir,
            )

    def test_prepare_refuses_effort_not_offered_by_the_model(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home)
        cache_dir = self.home / "cache"
        cache_dir.mkdir(parents=True)
        (cache_dir / "models.json").write_text(
            '{"models": [{"id": "gpt-5.6-sol", "efforts": ["low", "high"]}]}', encoding="utf-8"
        )
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "effort"):
            ADAPTER.prepare(
                self.start_request(effort="ultra"), profile, config, self.home, self.workdir
            )

    def test_prepare_refuses_write_beyond_profile_grant(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home)
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "does not grant write"):
            ADAPTER.prepare(
                self.start_request(write=True), profile, config, self.home, self.workdir
            )

    def test_prepare_refuses_no_filesystem_profile(self) -> None:
        config = self.runtime_config()
        ADAPTER.materialize(config, self.home)
        profile = AgentProfile("blank", "body", False, ())
        with self.assertRaisesRegex(ValidationError, "no-filesystem profile"):
            ADAPTER.prepare(self.start_request(), profile, config, self.home, self.workdir)

    def test_prepare_requires_materialized_home(self) -> None:
        config = self.runtime_config()
        profile = AgentProfile("review", "body", False, (self.auth_source_dir,))
        with self.assertRaisesRegex(ValidationError, "not materialized"):
            ADAPTER.prepare(self.start_request(), profile, config, self.home, self.workdir)


if __name__ == "__main__":
    unittest.main()
