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


if __name__ == "__main__":
    unittest.main()
