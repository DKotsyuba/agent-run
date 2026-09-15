"""Declarative native_settings: parsing, ownership, merge, snapshots.

Covers the full pipeline the feature promises: strict TOML parsing into
immutable ``RuntimeConfig.native_settings``, fail-closed reserved control
roots per adapter family, deep-merge over packaged defaults into each
generated native config (Codex TOML round-trip, Claude/GLM settings.json,
Qwen .qwen/settings.json), scoped ``dataclasses.replace`` copies, and
config-snapshot identity that changes with the declared options.
"""

from __future__ import annotations

import json
import re
import tempfile
import tomllib
import unittest
from unittest.mock import patch
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from agent_run.adapters.claude.materialize import render_settings
from agent_run.adapters.codex.adapter import ADAPTER as CODEX_ADAPTER
from agent_run.adapters.claude.adapter import ADAPTER as CLAUDE_ADAPTER
from agent_run.adapters.glm.adapter import ADAPTER as GLM_ADAPTER
from agent_run.adapters.qwen.adapter import ADAPTER as QWEN_ADAPTER
from agent_run.adapters.snapshot_config import (
    _runtime_document,
    build_config_snapshot,
)
from agent_run.config import RuntimeAuthConfig, RuntimeConfig, load_config
from agent_run.domain import StartRequest
from agent_run.errors import ValidationError
from agent_run.native_settings import native_settings_json, validate_native_settings
from agent_run.profiles import AgentProfile
from role_helpers import resolved_role

CODEX_REF = "agent_run.adapters.codex.adapter:ADAPTER"
CLAUDE_REF = "agent_run.adapters.claude.adapter:ADAPTER"
GLM_REF = "agent_run.adapters.glm.adapter:ADAPTER"
QWEN_REF = "agent_run.adapters.qwen.adapter:ADAPTER"


class NativeSettingsTestCase(unittest.TestCase):
    """Shared fixtures whose temporary state is cleaned up per test."""

    def setUp(self) -> None:
        """Create one temporary fixture root and register its cleanup."""
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name).resolve()
        self._directories = 0

    def write_config(self, text: str) -> Path:
        """Write one TOML body to a temp file removed at test teardown."""

        handle = tempfile.NamedTemporaryFile(
            "w", suffix=".toml", delete=False, dir=self.root
        )
        with handle:
            handle.write(text)
        path = Path(handle.name)
        self.addCleanup(path.unlink)
        return path

    def workdir(self, name: str = "work") -> Path:
        """Create and return one unique cleaned-up directory below the root."""

        self._directories += 1
        directory = self.root / f"{name}-{self._directories}"
        directory.mkdir()
        return directory

    def codex_runtime(self, **overrides: object) -> RuntimeConfig:
        """Return a minimal materialize-capable Codex runtime configuration."""

        values: dict[str, object] = {
            "enabled": True,
            "adapter": CODEX_REF,
            "binary": Path("/bin/echo"),
            "home": self.workdir("home"),
            "models": ("gpt-5",),
            "skills": (),
            "mcp": (),
            "hooks": (),
        }
        values.update(overrides)
        return RuntimeConfig(**values)

    def codex_table(self, settings: str) -> str:
        """Return TOML str appending settings str and create a test-owned home.

        The unique temporary directory is cleaned up with this fixture and is
        deep enough for the adapter's default skill-root resolution on Linux.
        """

        return (
            "schema_version = 1\n"
            "[runtimes.codex]\n"
            f"enabled = true\nadapter = \"{CODEX_REF}\"\n"
            f"binary = \"/bin/echo\"\nhome = {json.dumps(str(self.workdir('codex-home')))}\n"
            "models = [\"gpt-5\"]\n"
            f"{settings}"
        )


class NativeSettingsParsing(NativeSettingsTestCase):
    """Strict type, key, adapter, and reserved-root validation at load time."""

    def test_valid_tree_parses_immutable(self) -> None:
        """Parsed nested options preserve values and reject in-place mutation."""
        path = self.write_config(
            self.codex_table(
                "[runtimes.codex.native_settings]\n"
                "model_context_window = 500000\n"
                "ratio = 0.5\n"
                "labels = [\"a\", \"b\"]\n"
                "[runtimes.codex.native_settings.tuning]\n"
                "retries = 3\n"
                "quiet = true\n"
            )
        )
        runtime = load_config(path).runtimes["codex"]
        settings = runtime.native_settings
        self.assertEqual(settings["model_context_window"], 500000)
        self.assertEqual(settings["labels"], ("a", "b"))
        self.assertEqual(settings["tuning"]["retries"], 3)
        self.assertIsInstance(settings, MappingProxyType)
        self.assertIsInstance(settings["tuning"], Mapping)

    def test_empty_declaration_preserves_previous_defaults(self) -> None:
        """Omitting the table entirely keeps the pre-feature empty mapping."""

        runtime = load_config(self.write_config(self.codex_table(""))).runtimes["codex"]
        self.assertEqual(dict(runtime.native_settings), {})

    def test_dotted_key_literal_is_rejected(self) -> None:
        """A literal dotted key cannot splice namespaces when rendered back."""

        path = self.write_config(
            self.codex_table('[runtimes.codex.native_settings]\n"a.b" = 1\n')
        )
        with self.assertRaises(ValidationError):
            load_config(path)

    def test_blank_key_is_rejected(self) -> None:
        """An empty native preference name fails before materialization."""
        path = self.write_config(self.codex_table('[runtimes.codex.native_settings]\n"" = 1\n'))
        with self.assertRaises(ValidationError):
            load_config(path)

    def test_date_value_is_rejected(self) -> None:
        """TOML dates are rejected because native JSON cannot preserve their type."""
        path = self.write_config(
            self.codex_table("[runtimes.codex.native_settings]\nseen = 2024-01-01\n")
        )
        with self.assertRaises(ValidationError):
            load_config(path)

    def test_nonfinite_and_exotic_values_are_rejected(self) -> None:
        """Only JSON/TOML round-trippable scalars pass direct validation."""

        for value in (float("inf"), float("nan"), None, object()):
            with self.assertRaises(ValidationError):
                validate_native_settings({"k": value}, "p.native_settings")
        with self.assertRaises(ValidationError):
            validate_native_settings(None, "p.native_settings")

    def test_unsupported_adapter_is_rejected(self) -> None:
        """A runtime without a native settings renderer cannot accept the table."""
        path = self.write_config(
            "schema_version = 1\n"
            "[runtimes.stub]\n"
            "enabled = true\nadapter = \"agent_run.adapters.stub:ADAPTER\"\n"
            "binary = \"/bin/echo\"\nhome = \"/tmp/ar-stub\"\nmodels = [\"m\"]\n"
            "[runtimes.stub.native_settings]\nkey = 1\n"
        )
        with self.assertRaises(ValidationError):
            load_config(path)

    def test_reserved_roots_fail_closed_per_adapter(self) -> None:
        """Known control surfaces are rejected with the config path named."""

        cases = [
            ("codex", CODEX_REF, "model"),
            ("codex", CODEX_REF, "shell_environment_policy"),
            ("codex", CODEX_REF, "notify"),
            ("codex", CODEX_REF, "hooks"),
            ("codex", CODEX_REF, "model_providers"),
            ("claude", CLAUDE_REF, "env"),
            ("claude", CLAUDE_REF, "apiKeyHelper"),
            ("claude", CLAUDE_REF, "permissions"),
            ("claude", CLAUDE_REF, "awsAuthRefresh"),
            ("claude", CLAUDE_REF, "enabledPlugins"),
            ("glm", GLM_REF, "disableAllHooks"),
            ("qwen", QWEN_REF, "tools"),
            ("qwen", QWEN_REF, "mcpServers"),
            ("qwen", QWEN_REF, "mcp"),
            ("qwen", QWEN_REF, "security"),
        ]
        for name, ref, key in cases:
            with self.subTest(runtime=name, key=key):
                path = self.write_config(
                    "schema_version = 1\n"
                    f"[runtimes.{name}]\n"
                    f"enabled = true\nadapter = \"{ref}\"\n"
                    "binary = \"/bin/echo\"\nhome = \"/tmp/ar-x\"\nmodels = [\"m\"]\n"
                    f"[runtimes.{name}.native_settings]\n{key} = 1\n"
                )
                with self.assertRaises(ValidationError):
                    load_config(path)

    def test_qwen_tools_sandbox_is_not_tunable(self) -> None:
        """The Qwen sandbox control fails closed even as a nested table."""

        path = self.write_config(
            "schema_version = 1\n"
            "[runtimes.qwen]\n"
            f"enabled = true\nadapter = \"{QWEN_REF}\"\n"
            "binary = \"/bin/echo\"\nhome = \"/tmp/ar-q\"\nmodels = [\"m\"]\n"
            "[runtimes.qwen.native_settings.tools]\nsandbox = false\n"
        )
        with self.assertRaises(ValidationError):
            load_config(path)

    def test_model_verbosity_is_ordinary_tuning(self) -> None:
        """Render ordinary model verbosity inside the test-owned temporary home."""

        path = self.write_config(
            self.codex_table(
                '[runtimes.codex.native_settings]\nmodel_verbosity = "low"\n'
            )
        )
        runtime = load_config(path).runtimes["codex"]
        self.assertTrue(runtime.home.is_relative_to(self.root))
        CODEX_ADAPTER.validate(runtime)
        CODEX_ADAPTER.materialize(runtime, runtime.home, mcp_servers={})
        document = tomllib.loads((runtime.home / "config.toml").read_text(encoding="utf-8"))
        self.assertEqual(document["model_verbosity"], "low")

    def test_documented_full_config_example_loads(self) -> None:
        """The operator guide's first complete TOML example parses cleanly."""

        guide = (
            Path(__file__).resolve().parents[1]
            / "src/agent_run/operator_guide/config.md"
        ).read_text(encoding="utf-8")
        block = re.search(r"```toml\n(.*?)```", guide, re.DOTALL)
        self.assertIsNotNone(block, "operator guide must keep a TOML example")
        config = load_config(self.write_config(block.group(1)))
        self.assertIn("codex", config.runtimes)


class CodexNativeSettingsMaterialize(NativeSettingsTestCase):
    """Declared settings merge into generated config.toml over defaults."""

    def materialize(self, **overrides: object) -> tuple[str, dict]:
        """Materialize one fresh-home Codex runtime and parse its config.toml."""

        config = self.codex_runtime(**overrides)
        revision = CODEX_ADAPTER.materialize(config, config.home, mcp_servers={})
        document = tomllib.loads((config.home / "config.toml").read_text(encoding="utf-8"))
        return revision, document

    def test_omitted_settings_preserve_current_defaults(self) -> None:
        """Existing Codex tuning remains unchanged when no options are declared."""
        _, document = self.materialize()
        self.assertEqual(document["model_context_window"], 1000000)
        self.assertEqual(document["model_auto_compact_token_limit"], 780000)
        self.assertEqual(document["model_auto_compact_token_limit_scope"], "total")

    def test_declared_settings_land_and_change_fingerprint(self) -> None:
        """Operator tuning changes both the generated values and content digest."""
        default_revision, _ = self.materialize()
        settings = {
            "model_context_window": 500000,
            "tuning": {"retries": 3, "labels": ("fast", "deep")},
        }
        tuned_revision, document = self.materialize(native_settings=settings)
        self.assertNotEqual(default_revision, tuned_revision)
        self.assertEqual(document["model_context_window"], 500000)
        self.assertEqual(document["model_auto_compact_token_limit"], 780000)
        self.assertEqual(document["tuning"], {"retries": 3, "labels": ["fast", "deep"]})

    def test_string_escaping_roundtrips_through_tomllib(self) -> None:
        """Quotes, slashes and controls survive the native TOML roundtrip."""
        nasty = 'quote " backslash \\ newline \n tab \t'
        _, document = self.materialize(native_settings={"label": nasty})
        self.assertEqual(document["label"], nasty)

    def test_validate_rejects_reserved_roots_for_programmatic_configs(self) -> None:
        """Configs built in Python get the same ownership check as parsed ones."""

        for key in ("model", "approval_policy", "sandbox_mode", "mcp_servers", "features"):
            with self.subTest(key=key):
                config = self.codex_runtime(native_settings={key: "x"})
                with self.assertRaises(ValidationError):
                    CODEX_ADAPTER.validate(config)

    def test_nested_tables_render_as_valid_inline_toml(self) -> None:
        """Nested mappings and arrays preserve their structure in generated TOML."""
        _, document = self.materialize(
            native_settings={"outer": {"leaf": 1, "inner": {"deep": [1, 2]}}}
        )
        self.assertEqual(document["outer"], {"leaf": 1, "inner": {"deep": [1, 2]}})

    def test_nested_settings_do_not_capture_following_agent_run_roots(self) -> None:
        """Inline-only rendering keeps later permission lines at root level.

        Regression: a ``[tui]`` section header would capture the adapter's
        following ``default_permissions`` root key into the operator's table,
        dropping the managed Projects selector. Nested settings must render
        inline and leave agent-run-owned keys at the document root.
        """

        tree = self.workdir("tree")
        config = self.codex_runtime(
            home=self.workdir("home-projects"),
            workspace_root=tree,
            native_settings={"tui": {"theme": "dark"}},
        )
        with patch("agent_run.adapters.codex.permissions.system_projects", return_value=None):
            CODEX_ADAPTER.materialize(config, config.home, mcp_servers={})
        document = tomllib.loads((config.home / "config.toml").read_text(encoding="utf-8"))
        self.assertEqual(document["tui"], {"theme": "dark"})
        self.assertEqual(document["default_permissions"], "Projects")
        self.assertNotIn("default_permissions", document["tui"])

    def test_known_routing_and_reviewer_aliases_fail_closed(self) -> None:
        """Known provider, auth and capability controls stay adapter-owned."""
        for key in (
            "model_providers",
            "openai_base_url",
            "chatgpt_base_url",
            "approvals_reviewer",
            "openai_api_key",
            "cli_auth_credentials_store",
            "mcp_oauth_credentials_store",
            "forced_login_method",
            "forced_chatgpt_workspace_id",
            "skills",
            "tools",
            "agents",
            "apps",
            "web_search",
        ):
            with self.subTest(key=key):
                config = self.codex_runtime(native_settings={key: {"x": 1}})
                with self.assertRaises(ValidationError):
                    CODEX_ADAPTER.validate(config)


class ClaudeNativeSettings(NativeSettingsTestCase):
    """Claude and GLM settings.json merge with shared reserved ownership."""

    def claude_runtime(self, **overrides: object) -> RuntimeConfig:
        """Return a minimal Claude runtime configuration for direct calls."""

        values: dict[str, object] = {
            "enabled": True,
            "adapter": CLAUDE_REF,
            "binary": Path("/bin/echo"),
            "home": self.workdir("home"),
            "models": ("sonnet",),
            "skills": (),
            "mcp": (),
            "hooks": (),
        }
        values.update(overrides)
        return RuntimeConfig(**values)

    def test_declared_settings_and_hooks_coexist_in_settings_json(self) -> None:
        """Claude tuning is added without replacing generated hook declarations."""
        config = self.claude_runtime(native_settings={"spinnerTipsEnabled": False})
        CLAUDE_ADAPTER.materialize(config, config.home, mcp_servers={})
        document = json.loads((config.home / "settings.json").read_text(encoding="utf-8"))
        self.assertIs(document["spinnerTipsEnabled"], False)

    def test_reserved_roots_rejected_for_claude_and_glm(self) -> None:
        """Both adapters sharing the Claude renderer reject the same roots."""

        for adapter, ref in ((CLAUDE_ADAPTER, CLAUDE_REF), (GLM_ADAPTER, GLM_REF)):
            for key in ("hooks", "env", "statusLine", "disableAllHooks", "model", "agent", "autoMemoryDirectory"):
                with self.subTest(adapter=adapter.describe().name, key=key):
                    config = self.claude_runtime(
                        adapter=ref, native_settings={key: {"anything": 1}}
                    )
                    with self.assertRaises(ValidationError):
                        adapter.validate(config)

    def test_render_settings_keeps_hooks_when_declared_settings_empty(self) -> None:
        """Omitted settings leave the previous hooks-only document untouched."""

        from agent_run.config import RuntimeHookConfig

        home = self.workdir("render-home")
        hooks = (RuntimeHookConfig("PreToolUse", ("/bin/echo", "hook")),)
        render_settings(home, hooks)
        document = json.loads((home / "settings.json").read_text(encoding="utf-8"))
        self.assertIn("hooks", document)
        self.assertNotIn("native_settings", document)


class QwenNativeSettings(NativeSettingsTestCase):
    """Qwen settings JSON merge keeps tools.sandbox ownership protected."""

    def qwen_runtime(self, **overrides: object) -> RuntimeConfig:
        """Return a minimal materialize-capable Qwen runtime configuration."""

        values: dict[str, object] = {
            "enabled": True,
            "adapter": QWEN_REF,
            "binary": Path("/bin/echo"),
            "home": self.workdir("home"),
            "models": ("qwen-test",),
            "skills": (),
            "mcp": (),
            "auth": RuntimeAuthConfig(
                "environment", names=("OPENAI_API_KEY", "OPENAI_BASE_URL")
            ),
            "hooks": (),
        }
        values.update(overrides)
        return RuntimeConfig(**values)

    def test_declared_settings_land_and_sandbox_stays_owned(self) -> None:
        """Qwen receives tuning while retaining its required sandbox grant."""
        config = self.qwen_runtime(native_settings={"advance": {"thinking": True}})
        QWEN_ADAPTER.materialize(
            config, config.home, mcp_servers={}, skills_root=self.root / "skills"
        )
        document = json.loads(
            (config.home / ".qwen" / "settings.json").read_text(encoding="utf-8")
        )
        self.assertIs(document["tools"]["sandbox"], True)
        self.assertIs(document["advance"]["thinking"], True)

    def test_security_and_capability_roots_rejected(self) -> None:
        """Qwen cannot reroute providers or enable capabilities via tuning."""
        for key in ("tools", "security", "context", "permissions", "skills", "model", "modelProviders", "providers", "extensions"):
            with self.subTest(key=key):
                config = self.qwen_runtime(native_settings={key: {"x": 1}})
                with self.assertRaises(ValidationError):
                    QWEN_ADAPTER.validate(config)


class SnapshotAndScopedCopies(NativeSettingsTestCase):
    """Snapshots record declared settings; scoped replace copies keep them."""

    def runtime(self, settings: Mapping[str, object]) -> RuntimeConfig:
        """Return a Codex runtime carrying the given declared settings."""

        return self.codex_runtime(native_settings=settings)

    def test_runtime_document_records_sorted_settings(self) -> None:
        """Snapshots store a stable plain-JSON representation of declared values."""
        document = _runtime_document(
            self.runtime({"z": 1, "a": {"nested": [1, "x"]}})
        )
        self.assertEqual(
            document["native_settings"], {"a": {"nested": [1, "x"]}, "z": 1}
        )

    def test_runtime_document_omits_empty_settings(self) -> None:
        """Pre-feature snapshots keep their canonical shape when nothing is declared."""

        document = _runtime_document(self.runtime({}))
        self.assertNotIn("native_settings", document)

    def test_settings_change_changes_document_identity(self) -> None:
        """Changing one preference changes canonical snapshot bytes."""
        first = json.dumps(_runtime_document(self.runtime({"k": 1})), sort_keys=True)
        second = json.dumps(_runtime_document(self.runtime({"k": 2})), sort_keys=True)
        self.assertNotEqual(first, second)

    def test_parse_scoped_snapshot_roundtrip_materializes(self) -> None:
        """Declared settings survive parse, replace, snapshot JSON, materialize.

        One chained path over the real boundaries: the common config parses
        into ``RuntimeConfig``, preparation-style ``replace`` scopes a launch
        copy, the persisted snapshot JSON carries the settings, and the
        materialized config.toml applies the values read back from that
        document.
        """

        path = self.write_config(
            self.codex_table(
                "[runtimes.codex.native_settings]\n"
                "model_context_window = 250000\n"
                "[runtimes.codex.native_settings.tuning]\n"
                "retries = 3\n"
            )
        )
        parsed = load_config(path).runtimes["codex"]
        scoped = replace(parsed, home=self.workdir("scoped-home"))
        profile = AgentProfile("review", "Review carefully.", False, (), False)
        role = resolved_role(
            StartRequest(
                runtime="codex",
                model="gpt-5",
                profile="review",
                task="check",
                workdir=self.workdir("work"),
            ),
            profile,
            scoped,
            {},
        )
        snapshot = build_config_snapshot(
            runtime="codex",
            adapter_api_version=2,
            schema_version=1,
            materialize_revision="a" * 64,
            snapshot_index_sha256="b" * 64,
            config=scoped,
            profile=role,
        )
        restored = json.loads(snapshot.document)["runtime_config"]["native_settings"]
        self.assertEqual(restored, {"model_context_window": 250000, "tuning": {"retries": 3}})
        restored_config = replace(scoped, native_settings=restored)
        CODEX_ADAPTER.materialize(restored_config, restored_config.home, mcp_servers={})
        document = tomllib.loads((scoped.home / "config.toml").read_text(encoding="utf-8"))
        self.assertEqual(document["model_context_window"], 250000)
        self.assertEqual(document["tuning"], {"retries": 3})

    def test_json_conversion_is_deterministic_for_snapshot_bytes(self) -> None:
        """native_settings_json output is key-sorted plain JSON data."""

        self.assertEqual(
            native_settings_json({"z": (1, 2), "a": {"b": True}}),
            {"a": {"b": True}, "z": [1, 2]},
        )


if __name__ == "__main__":
    unittest.main()
