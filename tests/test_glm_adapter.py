import os
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.claude.adapter import ADAPTER as CLAUDE_ADAPTER
from agent_run.adapters.glm import auth as glm_auth
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
        glm_auth.reset_keychain_cache()
        self.addCleanup(glm_auth.reset_keychain_cache)
        for name in ("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL", "ANTHROPIC_MODEL"):
            previous = os.environ.pop(name, None)
            if previous is not None:
                self.addCleanup(os.environ.__setitem__, name, previous)

    def runtime_config(self, **overrides: Any) -> RuntimeConfig:
        """Build a valid GLM runtime with explicitly typed test-only overrides."""

        values: dict[str, Any] = dict(
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

    def request(self, **overrides: Any) -> StartRequest:
        """Build a valid GLM request with explicitly typed test-only overrides."""

        values: dict[str, Any] = dict(
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

    def test_prepare_ignores_inherited_environment(self) -> None:
        """The orchestrator's own ANTHROPIC_* env must never leak into the child.

        Claude Code exports ANTHROPIC_BASE_URL into every shell it runs, and
        an inherited ANTHROPIC_AUTH_TOKEN would be the orchestrator's own
        credential — proven live 2026-08-30: env-wins sent the plan key to
        api.anthropic.com (401). Keychain wins; the base URL is the plan's.
        """
        with patch.dict(
            "os.environ",
            {"ANTHROPIC_AUTH_TOKEN": "sk-orchestrator", "ANTHROPIC_BASE_URL": "https://api.anthropic.com"},
            clear=False,
        ), patch.object(glm_adapter, "keychain_glm_key", return_value="sk-plan-key"):
            plan = self.prepare(self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir)
        self.assertEqual(plan.environment["ANTHROPIC_AUTH_TOKEN"], "sk-plan-key")
        self.assertEqual(plan.environment["ANTHROPIC_BASE_URL"], DEFAULT_BASE_URL)
        self.assertEqual(plan.environment["ANTHROPIC_MODEL"], "glm-5.3[1m]")
        argv = list(plan.argv)
        self.assertEqual(argv[argv.index("--model") + 1], "glm-5.3[1m]")
        self.assertNotIn("sk-plan-key", " ".join(argv))
        secret_names = plan.adapter_state["secret_env_names"]
        assert isinstance(secret_names, tuple)
        self.assertIn("ANTHROPIC_AUTH_TOKEN", secret_names)

    def test_million_context_models_carry_the_1m_suffix(self) -> None:
        """Claude Code clamps unknown models to 200k without the [1m] marker.

        Verified live 2026-08-30: bare glm-5.3 makes the CLI announce a 200k
        auto-compact ceiling; glm-5.3[1m] silences it and z.ai still answers.
        glm-5-turbo's real window is ~205k, so it must stay bare.
        """
        config = self.runtime_config(models=("glm-5.3", "glm-5.3-flash", "glm-5-turbo"))
        with patch.object(glm_adapter, "keychain_glm_key", return_value="sk-plan-key"):
            for model, expected in (
                ("glm-5.3", "glm-5.3[1m]"),
                ("glm-5.3-flash", "glm-5.3-flash[1m]"),
                ("glm-5-turbo", "glm-5-turbo"),
            ):
                with self.subTest(model=model):
                    plan = self.prepare(
                        self.request(model=model), self.profile(), config,
                        self.home, self.agent_dir,
                    )
                    argv = list(plan.argv)
                    self.assertEqual(argv[argv.index("--model") + 1], expected)
                    self.assertEqual(plan.environment["ANTHROPIC_MODEL"], expected)

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

    def test_injected_token_is_registered_and_redacted_for_sparse_auth_names(self) -> None:
        """Empty or partial auth declarations still redact only injected credentials."""

        secret = "SYNTHETIC_GLM_TOKEN_XYZ"
        script = (
            "import json, os, sys\n"
            "sys.stdin.readline()\n"
            "token = os.environ['ANTHROPIC_AUTH_TOKEN']\n"
            "url = os.environ['ANTHROPIC_BASE_URL']\n"
            "print(json.dumps({'type': 'assistant', 'message': {'role': 'assistant', "
            "'content': [{'type': 'text', 'text': token + ' ' + url}]}}))\n"
            "sys.stderr.write('failure ' + token + ' ' + url + '\\n')\n"
            "raise SystemExit(1)\n"
        )
        for index, names in enumerate(((), ("ANTHROPIC_BASE_URL",))):
            with self.subTest(names=names), patch.object(
                glm_adapter, "keychain_glm_key", return_value=secret
            ):
                config = self.runtime_config(
                    auth=RuntimeAuthConfig("environment", names=names)
                )
                plan = self.prepare(
                    self.request(), self.profile(), config, self.home, self.agent_dir
                )
                secret_names = plan.adapter_state["secret_env_names"]
                assert isinstance(secret_names, tuple)
                self.assertIn("ANTHROPIC_AUTH_TOKEN", secret_names)
                self.assertEqual("ANTHROPIC_BASE_URL" in secret_names, bool(names))
                log_path = self.agent_dir / f"runtime-{index}.jsonl"
                launched = replace(
                    plan,
                    argv=(sys.executable, "-u", "-c", script),
                    runtime_stream_path=log_path,
                )
                sink = Mock()
                outcome = self.adapter.launch(launched, sink).wait(timeout_seconds=5)

                self.assertIsNotNone(outcome)
                assert outcome is not None
                messages = " ".join(
                    call.args[0].content for call in sink.message.call_args_list
                )
                observed = " ".join(
                    (messages, outcome.failure_text or "", log_path.read_text())
                )
                self.assertNotIn(secret, observed)
                self.assertIn("<redacted>", observed)
                if names:
                    self.assertNotIn(DEFAULT_BASE_URL, observed)
                else:
                    self.assertIn(DEFAULT_BASE_URL, observed)

    def test_prepare_without_token_anywhere_is_a_validation_error(self) -> None:
        env_without_token = {
            k: v for k, v in os.environ.items() if k != "ANTHROPIC_AUTH_TOKEN"
        }
        with patch.dict("os.environ", env_without_token, clear=True):
            with patch.object(glm_adapter, "keychain_glm_key", return_value=None):
                with self.assertRaisesRegex(ValidationError, "glm requires the macOS Keychain"):
                    self.prepare(self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir)

    def test_keychain_wins_over_process_environment(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_AUTH_TOKEN": "sk-exported"}, clear=False):
            with patch.object(glm_adapter, "keychain_glm_key", return_value="sk-keychain"):
                plan = self.prepare(
                    self.request(), self.profile(), self.runtime_config(), self.home, self.agent_dir
                )
        self.assertEqual(plan.environment["ANTHROPIC_AUTH_TOKEN"], "sk-keychain")

    def test_environment_token_is_the_fallback_when_keychain_is_empty(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_AUTH_TOKEN": "sk-exported"}, clear=False):
            with patch.object(glm_adapter, "keychain_glm_key", return_value=None):
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

    def test_keychain_cache_retries_misses_reuses_success_rotates_and_resets(self) -> None:
        """Only successful lookups cache, expire by monotonic TTL, and reset cleanly."""

        lookup = Mock(side_effect=[None, "synthetic-first", "synthetic-rotated"])
        with (
            patch.object(glm_auth, "_lookup_keychain_key", lookup),
            patch.object(glm_auth.time, "monotonic", side_effect=[0, 1, 2, 302]),
        ):
            self.assertIsNone(glm_auth.keychain_glm_key())
            self.assertEqual(glm_auth.keychain_glm_key(), "synthetic-first")
            self.assertEqual(glm_auth.keychain_glm_key(), "synthetic-first")
            self.assertEqual(glm_auth.keychain_glm_key(), "synthetic-rotated")
        self.assertEqual(lookup.call_count, 3)

        glm_auth.reset_keychain_cache()
        with (
            patch.object(
                glm_auth, "_lookup_keychain_key", return_value="synthetic-after-reset"
            ) as after_reset,
            patch.object(glm_auth.time, "monotonic", return_value=303),
        ):
            self.assertEqual(glm_auth.keychain_glm_key(), "synthetic-after-reset")
        after_reset.assert_called_once_with()

    def test_concurrent_keychain_reads_share_one_successful_refresh(self) -> None:
        """Concurrent cache misses serialize one synthetic Keychain refresh."""

        entered = threading.Event()
        release = threading.Event()
        calls = []

        def lookup() -> str:
            """Block one synthetic lookup so a competing reader reaches the lock."""

            calls.append(None)
            entered.set()
            release.wait(1)
            return "synthetic-shared"

        results = []
        with patch.object(glm_auth, "_lookup_keychain_key", side_effect=lookup):
            first = threading.Thread(
                target=lambda: results.append(glm_auth.keychain_glm_key())
            )
            second = threading.Thread(
                target=lambda: results.append(glm_auth.keychain_glm_key())
            )
            first.start()
            self.assertTrue(entered.wait(1))
            second.start()
            release.set()
            first.join(1)
            second.join(1)

        self.assertEqual(calls, [None])
        self.assertEqual(results, ["synthetic-shared", "synthetic-shared"])


if __name__ == "__main__":
    unittest.main()
