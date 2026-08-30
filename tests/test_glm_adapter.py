import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.claude.adapter import ADAPTER as CLAUDE_ADAPTER
from agent_run.adapters.glm import adapter as glm_adapter
from agent_run.adapters.glm.adapter import ADAPTER, ADAPTER_API_VERSION, GlmAdapter
from agent_run.adapters.glm.auth import DEFAULT_BASE_URL, KEYCHAIN_ACCOUNT, KEYCHAIN_SERVICE
from agent_run.config import RuntimeAuthConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.domain import StartRequest
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile


class GlmAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter: GlmAdapter = ADAPTER
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name).resolve()
        self.home = self.root / "runtimes" / "glm" / "home"
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
        for name in ("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL", "ANTHROPIC_MODEL"):
            previous = os.environ.pop(name, None)
            if previous is not None:
                self.addCleanup(os.environ.__setitem__, name, previous)

    def runtime_config(self, **overrides) -> RuntimeConfig:
        values = dict(
            enabled=True,
            adapter="agent_run.adapters.glm.adapter:ADAPTER",
            binary=Path("/bin/echo"),
            home=self.home,
            models=("glm-5.3", "glm-5.3-flash"),
            skills=(),
            mcp=(),
            auth=RuntimeAuthConfig(
                "environment", names=("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL")
            ),
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
            runtime="glm",
            model="glm-5.3",
            profile="review",
            task="do the thing",
            workdir=self.workdir,
        )
        values.update(overrides)
        return StartRequest(**values)

    def prepare(self, *args, mcp_servers: dict = {}, **kwargs):
        return self.adapter.prepare(*args, mcp_servers=mcp_servers, **kwargs)

    # -- describe -----------------------------------------------------

    def test_describe_names_glm_with_claude_capabilities(self) -> None:
        info = self.adapter.describe()
        self.assertEqual(info.name, "glm")
        self.assertEqual(info.adapter_api_version, ADAPTER_API_VERSION)
        self.assertEqual(info.capabilities, CLAUDE_ADAPTER.describe().capabilities)

    # -- validate -------------------------------------------------------

    def test_validate_accepts_the_glm_auth_names(self) -> None:
        self.adapter.validate(self.runtime_config())
        self.adapter.validate(
            self.runtime_config(auth=RuntimeAuthConfig("environment", names=("ANTHROPIC_AUTH_TOKEN",)))
        )

    def test_validate_rejects_foreign_auth_names_and_kinds(self) -> None:
        with self.assertRaisesRegex(ValidationError, "requires an auth bridge"):
            self.adapter.validate(self.runtime_config(auth=None))
        with self.assertRaisesRegex(ValidationError, "auth.kind must be"):
            self.adapter.validate(
                self.runtime_config(
                    auth=RuntimeAuthConfig("file_link", source=Path("/tmp"), target="a")
                )
            )
        with self.assertRaisesRegex(ValidationError, "unsupported entries"):
            self.adapter.validate(
                self.runtime_config(auth=RuntimeAuthConfig("environment", names=("ROGUE_VAR",)))
            )
        with self.assertRaisesRegex(ValidationError, "unsupported entries"):
            self.adapter.validate(
                self.runtime_config(
                    auth=RuntimeAuthConfig("environment", names=("CLAUDE_CODE_OAUTH_TOKEN",))
                )
            )

    def test_validate_keeps_claude_service_mode_and_hook_semantics(self) -> None:
        with self.assertRaisesRegex(ValidationError, "service_mode"):
            self.adapter.validate(self.runtime_config(service_mode="managed"))
        with self.assertRaisesRegex(ValidationError, "not a known Claude hook event"):
            self.adapter.validate(
                self.runtime_config(hooks=(RuntimeHookConfig("BogusEvent", ("echo",)),))
            )
        self.adapter.validate(
            self.runtime_config(hooks=(RuntimeHookConfig("PostToolUse", ("echo", "done"), "^Bash$"),))
        )

    # -- prepare --------------------------------------------------------

    def test_prepare_with_full_environment(self) -> None:
        with patch.dict(
            "os.environ",
            {"ANTHROPIC_AUTH_TOKEN": "sk-test-token", "ANTHROPIC_BASE_URL": "https://example.test/api"},
            clear=False,
        ):
            plan = self.prepare(self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir)
        self.assertEqual(plan.environment["ANTHROPIC_AUTH_TOKEN"], "sk-test-token")
        self.assertEqual(plan.environment["ANTHROPIC_BASE_URL"], "https://example.test/api")
        self.assertEqual(plan.environment["ANTHROPIC_MODEL"], "glm-5.3")
        argv = list(plan.argv)
        self.assertEqual(argv[argv.index("--model") + 1], "glm-5.3")
        self.assertNotIn("sk-test-token", " ".join(argv))
        self.assertIn("ANTHROPIC_AUTH_TOKEN", plan.adapter_state["secret_env_names"])

    def test_prepare_falls_back_to_default_base_url(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_AUTH_TOKEN": "sk-test-token"}, clear=False):
            plan = self.prepare(self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir)
        self.assertEqual(plan.environment["ANTHROPIC_BASE_URL"], DEFAULT_BASE_URL)

    def test_prepare_consults_keychain_when_environment_is_empty(self) -> None:
        with patch.object(glm_adapter, "keychain_glm_key", return_value="sk-keychain") as keychain:
            plan = self.prepare(self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir)
        keychain.assert_called()
        self.assertEqual(plan.environment["ANTHROPIC_AUTH_TOKEN"], "sk-keychain")
        self.assertEqual(plan.environment["ANTHROPIC_BASE_URL"], DEFAULT_BASE_URL)

    def test_prepare_without_token_anywhere_is_a_validation_error(self) -> None:
        with patch.object(glm_adapter, "keychain_glm_key", return_value=None):
            with self.assertRaisesRegex(ValidationError, "glm requires ANTHROPIC_AUTH_TOKEN"):
                self.prepare(self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir)

    def test_process_environment_wins_over_keychain(self) -> None:
        def keychain_must_not_run():
            raise AssertionError("keychain consulted despite exported token")

        with patch.dict("os.environ", {"ANTHROPIC_AUTH_TOKEN": "sk-exported"}, clear=False):
            with patch.object(glm_adapter, "keychain_glm_key", side_effect=keychain_must_not_run):
                plan = self.prepare(
                    self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir
                )
        self.assertEqual(plan.environment["ANTHROPIC_AUTH_TOKEN"], "sk-exported")

    def test_prepare_restores_process_environment(self) -> None:
        with patch.object(glm_adapter, "keychain_glm_key", return_value="sk-keychain"):
            self.prepare(self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir)
        self.assertNotIn("ANTHROPIC_AUTH_TOKEN", os.environ)
        self.assertNotIn("ANTHROPIC_BASE_URL", os.environ)

    # -- probe ----------------------------------------------------------

    def test_probe_reports_credential_presence_without_network(self) -> None:
        config = self.runtime_config()
        with patch.object(glm_adapter, "keychain_glm_key", return_value=None):
            unauthenticated = self.adapter.probe(config, self.home)
        self.assertTrue(unauthenticated.available)
        self.assertFalse(unauthenticated.authenticated)
        with patch.object(glm_adapter, "keychain_glm_key", return_value="sk-keychain"):
            keychain_authenticated = self.adapter.probe(config, self.home)
        self.assertTrue(keychain_authenticated.authenticated)
        with patch.dict("os.environ", {"ANTHROPIC_AUTH_TOKEN": "sk-exported"}, clear=False):
            with patch.object(
                glm_adapter,
                "keychain_glm_key",
                side_effect=AssertionError("keychain consulted despite exported token"),
            ):
                env_authenticated = self.adapter.probe(config, self.home)
        self.assertTrue(env_authenticated.authenticated)

    # -- auth module ------------------------------------------------------

    def test_auth_module_exposes_the_keychain_contract(self) -> None:
        self.assertEqual(KEYCHAIN_SERVICE, "com.pluto.agent-run.glm")
        self.assertEqual(KEYCHAIN_ACCOUNT, "GLM_CODING_KEY")
        self.assertEqual(DEFAULT_BASE_URL, "https://api.z.ai/api/anthropic")


if __name__ == "__main__":
    unittest.main()
