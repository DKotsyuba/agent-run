"""Focused contracts for declarative developer-environment assembly."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from types import MappingProxyType

from agent_run.adapters.developer_environment import configured_environment_keys, developer_environment, environment_digest
from agent_run.config import EnvironmentConfig, RuntimeConfig, load_config
from agent_run.errors import ValidationError


def _runtime(environment: EnvironmentConfig | None) -> RuntimeConfig:
    """Build a minimal runtime with the supplied environment selection."""

    return RuntimeConfig(True, "example.adapter:ADAPTER", Path("/bin/echo"), Path("/tmp/home"), ("test",), environment=environment)


class DeveloperEnvironmentTests(unittest.TestCase):
    """Verify the provider's pure, bounded environment behavior."""

    def test_paths_templates_and_required_commands(self) -> None:
        """Configured paths lead PATH and approved templates use baseline HOME."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / "tools"
            tools.mkdir()
            executable = tools / "node"
            executable.write_text("", encoding="utf-8")
            executable.chmod(0o755)
            config = _runtime(EnvironmentConfig((tools,), MappingProxyType({"PROJECT": "{workdir}/x:{home}"}), ("node",), ("gh",)))
            result = developer_environment({"HOME": "/isolated", "PATH": f"{tools}{os.pathsep}/bin"}, config, root)
            self.assertEqual(result["PATH"], f"{tools}{os.pathsep}/bin")
            self.assertEqual(result["PROJECT"], f"{root}/x:/isolated")
            self.assertEqual(result["HOME"], "/isolated")
            self.assertEqual(configured_environment_keys(config), ("PATH", "PROJECT"))

    def test_rejects_protected_templates_and_missing_required_command(self) -> None:
        """Protected values, unknown templates, and absent commands fail clearly."""

        for variables, expected in (({"HOME": "other"}, "protected"), ({"X": "{nope}"}, "unsupported")):
            with self.assertRaisesRegex(ValidationError, expected):
                developer_environment({"HOME": "/isolated", "PATH": "/bin"}, _runtime(EnvironmentConfig(variables=MappingProxyType(variables))), Path("/tmp"))
        with self.assertRaisesRegex(ValidationError, "missing executable: missing"):
            developer_environment({"HOME": "/isolated", "PATH": "/bin"}, _runtime(EnvironmentConfig(required_commands=("missing",))), Path("/tmp"))

    def test_rejects_rustup_toolchain_pin_and_auth_name_overrides(self) -> None:
        """A preset may not plant the Rust toolchain pin or shadow auth names."""
        from agent_run.config import RuntimeAuthConfig

        with self.assertRaisesRegex(ValidationError, "protected keys: RUSTUP_TOOLCHAIN"):
            developer_environment(
                {"HOME": "/isolated", "PATH": "/bin"},
                _runtime(EnvironmentConfig(variables=MappingProxyType({"RUSTUP_TOOLCHAIN": "stable"}))),
                Path("/tmp"),
            )
        config = _runtime(EnvironmentConfig(variables=MappingProxyType({"OPENAI_API_KEY": "x"})))
        from dataclasses import replace
        config = replace(config, auth=RuntimeAuthConfig(kind="environment", names=("OPENAI_API_KEY",)))
        with self.assertRaisesRegex(ValidationError, "protected keys: OPENAI_API_KEY"):
            developer_environment({"HOME": "/isolated", "PATH": "/bin"}, config, Path("/tmp"))

    def test_config_resolves_preset_and_validates_command_overlap(self) -> None:
        """Runtime selections resolve objects and invalid command policy is rejected."""

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text('''schema_version = 1
[environments.developer]
path = ["/bin"]
variables = { PROJECT = "{workdir}" }
required_commands = ["sh"]
denied_commands = ["gh"]
[runtimes.test]
enabled = true
adapter = "example.adapter:ADAPTER"
binary = "/bin/echo"
home = "/tmp/test-home"
models = ["test"]
environment = "developer"
''', encoding="utf-8")
            runtime = load_config(path).runtimes["test"]
            self.assertEqual(runtime.environment.path, (Path("/bin").resolve(),))
            self.assertEqual(len(environment_digest(runtime)), 64)
            self.assertEqual(environment_digest(_runtime(None)), "")
            path.write_text(path.read_text(encoding="utf-8").replace('denied_commands = ["gh"]', 'denied_commands = ["sh"]'), encoding="utf-8")
            with self.assertRaisesRegex(ValidationError, "both required and denied"):
                load_config(path)

    def test_legacy_rust_propagation_and_revision(self) -> None:
        """Keep explicitly configured legacy Rust visible to MCP and revisions."""
        from dataclasses import replace
        from agent_run.config import RustConfig
        config = replace(_runtime(None), rust=RustConfig(Path("/rustup"), Path("/cargo")))
        self.assertEqual(set(configured_environment_keys(config)), {"PATH", "RUSTUP_HOME", "CARGO_HOME", "RUSTUP_AUTO_INSTALL"})
        self.assertEqual(len(environment_digest(config)), 64)
        changed = replace(config, rust=RustConfig(Path("/other-rustup"), Path("/cargo")))
        self.assertNotEqual(environment_digest(config), environment_digest(changed))

    def test_old_config_positional_constructor_remains_compatible(self) -> None:
        """Appending environments must not move the existing runtimes argument."""
        from agent_run.config import Config
        expected = Config(schema_version=1, runtimes={"demo": _runtime(None)})
        actual = Config(expected.schema_version, expected.core, expected.capacity, expected.delivery, expected.profiles, expected.mcp, expected.runtimes)
        self.assertEqual(actual.runtimes, expected.runtimes)
        self.assertEqual(dict(actual.environments), {})

    def test_command_names_and_environment_values_are_validated(self) -> None:
        """Reject shell syntax and NUL values while allowing empty env strings."""
        import json
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            for command in ("two words", "gh;echo", "$tool", "../tool"):
                with self.subTest(command=command):
                    path.write_text("schema_version = 1\n[environments.dev]\nrequired_commands = " + json.dumps([command]) + "\n")
                    with self.assertRaises(ValidationError):
                        load_config(path)
            path.write_text('schema_version = 1\n[environments.dev.variables]\nEMPTY = ""\n')
            self.assertEqual(load_config(path).environments["dev"].variables["EMPTY"], "")
            path.write_text("schema_version = 1\n[environments.dev.variables]\nBAD = " + json.dumps("a\x00b") + "\n")
            with self.assertRaises(ValidationError):
                load_config(path)
