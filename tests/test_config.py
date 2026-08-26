import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.config import load_config
from agent_run.errors import ValidationError


class ConfigTests(unittest.TestCase):
    def load(self, text: str):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(text, encoding="utf-8")
            return load_config(path)

    def test_minimal_and_consumed_configuration_load(self) -> None:
        minimal = self.load("schema_version = 1\n")
        self.assertEqual(minimal.core.default_timeout_seconds, 480)
        self.assertEqual(dict(minimal.runtimes), {})

        config = self.load(
            """
schema_version = 1
[core]
default_timeout_seconds = 30
max_active_agents = 2
warning_fraction = 0.8
[capacity]
collect_interval_seconds = 10
sample_retention = 20
context_max_chars = 100
[delivery]
retry_base_seconds = 1
retry_cap_seconds = 5
max_attempts = 3
[profiles]
directory = "/tmp/profiles"
[mcp.agent_lsp]
transport = "stdio"
command = "/bin/echo"
args = ["hello"]
env_from = ["PATH"]
[runtimes.fake]
enabled = true
adapter = "example.adapter:ADAPTER"
binary = "/bin/echo"
home = "/tmp/runtime-home"
models = ["test"]
skills = ["review"]
mcp = ["agent_lsp"]
max_active_agents = 1
[runtimes.fake.auth]
kind = "environment"
names = ["TEST_TOKEN"]
[[runtimes.fake.hooks]]
event = "PostToolUse"
matcher = "tool"
command = ["echo", "done"]
"""
        )
        self.assertEqual(config.core.max_active_agents, 2)
        self.assertEqual(config.mcp["agent_lsp"].env_from, ("PATH",))
        self.assertEqual(config.runtimes["fake"].models, ("test",))
        self.assertEqual(config.runtimes["fake"].auth.names, ("TEST_TOKEN",))

    def test_delivery_queue_binary_is_optional_and_absolute(self) -> None:
        self.assertIsNone(self.load("schema_version = 1\n").delivery.codex_queue_bin)
        configured = self.load(
            'schema_version = 1\n[delivery]\ncodex_queue_bin = "/bin/echo"\n'
        )
        self.assertEqual(configured.delivery.codex_queue_bin, Path("/bin/echo"))
        with self.assertRaisesRegex(ValidationError, "delivery.codex_queue_bin"):
            self.load(
                'schema_version = 1\n[delivery]\ncodex_queue_bin = "relative"\n'
            )

    def test_core_and_capacity_bounds_fail_during_load(self) -> None:
        invalid = (
            ("core", "warning_fraction", "true"),
            ("core", "warning_fraction", '"0.5"'),
            ("core", "warning_fraction", "0"),
            ("core", "warning_fraction", "1"),
            ("capacity", "collect_interval_seconds", "true"),
            ("capacity", "collect_interval_seconds", "0.5"),
            ("capacity", "collect_interval_seconds", "0"),
            ("capacity", "context_max_chars", "true"),
            ("capacity", "context_max_chars", "1.5"),
            ("capacity", "context_max_chars", "0"),
            ("capacity", "context_max_chars", "2501"),
        )
        for section, field, value in invalid:
            with self.subTest(field=field, value=value), self.assertRaisesRegex(
                ValidationError, field
            ):
                self.load(
                    f"schema_version = 1\n[{section}]\n{field} = {value}\n"
                )

        lower = self.load(
            """schema_version = 1
[core]
warning_fraction = 0.1
[capacity]
collect_interval_seconds = 1
context_max_chars = 1
"""
        )
        upper = self.load(
            """schema_version = 1
[core]
warning_fraction = 0.9
[capacity]
collect_interval_seconds = 300
context_max_chars = 2500
"""
        )
        self.assertEqual(lower.capacity.collect_interval_seconds, 1)
        self.assertEqual(lower.capacity.context_max_chars, 1)
        self.assertEqual(upper.capacity.context_max_chars, 2500)

    def test_unknown_fields_report_the_recursive_path(self) -> None:
        cases = (
            ("schema_version = 1\nextra = true\n", "extra"),
            ("schema_version = 1\n[core]\nextra = true\n", "core.extra"),
            (
                """schema_version = 1
[runtimes.fake]
enabled = true
adapter = "example:ADAPTER"
binary = "/bin/echo"
home = "/tmp/home"
models = ["test"]
[[runtimes.fake.hooks]]
event = "x"
command = ["x"]
extra = true
""",
                "runtimes.fake.hooks[0].extra",
            ),
        )
        for text, field in cases:
            with self.subTest(field=field), self.assertRaisesRegex(ValidationError, field.replace("[", r"\[").replace("]", r"\]")):
                self.load(text)

    def test_unsupported_versions_and_secret_literals_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValidationError, "schema_version"):
            self.load("schema_version = 2\n")
        with self.assertRaisesRegex(ValidationError, "schema_version"):
            self.load("schema_version = 1.0\n")
        with self.assertRaisesRegex(ValidationError, "not a secret value"):
            self.load(
                """schema_version = 1
[mcp.bad]
transport = "stdio"
command = "/bin/echo"
env_from = ["sk-literal-secret"]
"""
            )
        with self.assertRaisesRegex(ValidationError, r"runtimes.fake.mcp\[0\]"):
            self.load(
                """schema_version = 1
[runtimes.fake]
enabled = true
adapter = "example:ADAPTER"
binary = "/bin/echo"
home = "/tmp/home"
models = ["test"]
mcp = ["undeclared"]
"""
            )
        with self.assertRaisesRegex(ValidationError, "not a secret value"):
            self.load(
                """schema_version = 1
[runtimes.fake]
enabled = true
adapter = "example:ADAPTER"
binary = "/bin/echo"
home = "/tmp/home"
models = ["test"]
[runtimes.fake.auth]
kind = "environment"
names = ["literal-token"]
"""
            )
        secret = "ghp_abcdefghijklmnopqrstuvwxyz0123456789"
        for field, text in (
            ("env_from", f'''schema_version = 1
[mcp.bad]
transport = "stdio"
command = "/bin/echo"
env_from = ["{secret}"]
'''),
            ("auth names", f'''schema_version = 1
[runtimes.fake]
enabled = true
adapter = "example:ADAPTER"
binary = "/bin/echo"
home = "/tmp/home"
models = ["test"]
[runtimes.fake.auth]
kind = "environment"
names = ["{secret}"]
'''),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(ValidationError, "not a secret value"):
                self.load(text)

    def test_nonfinite_numeric_values_are_rejected(self) -> None:
        for field, text in (
            ("core.default_timeout_seconds", "schema_version = 1\n[core]\ndefault_timeout_seconds = nan\n"),
            ("core.warning_fraction", "schema_version = 1\n[core]\nwarning_fraction = nan\n"),
            ("capacity.collect_interval_seconds", "schema_version = 1\n[capacity]\ncollect_interval_seconds = nan\n"),
            ("delivery.retry_base_seconds", "schema_version = 1\n[delivery]\nretry_base_seconds = nan\n"),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(ValidationError, field):
                self.load(text)


if __name__ == "__main__":
    unittest.main()
