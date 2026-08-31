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

    def runtime_with_limits_source(self, source_line: str = ""):
        return self.load(
            f"""schema_version = 1
[runtimes.fake]
enabled = true
adapter = "example.adapter:ADAPTER"
binary = "/bin/echo"
home = "/tmp/runtime-home"
models = ["test"]
{source_line}
"""
        )

    def test_accounts_parse_and_validate(self) -> None:
        config = self.load(
            '''schema_version = 1
[runtimes.codex]
enabled = true
adapter = "agent_run.adapters.codex:ADAPTER"
binary = "/bin/codex"
home = "/tmp/codex"
models = ["test"]
accounts = ["personal2", "work_1"]
default_account = "personal2"
[runtimes.codex.auth]
kind = "file_link"
source = "/tmp/auth.json"
target = "auth.json"
'''
        )
        runtime = config.runtimes["codex"]
        self.assertEqual(runtime.accounts, ("personal2", "work_1"))
        self.assertEqual(runtime.default_account, "personal2")
        for bad in ("Upper", "has space", "a" * 33, "../escape"):
            with self.subTest(bad=bad), self.assertRaisesRegex(ValidationError, "accounts"):
                self.load(
                    f'''schema_version = 1
[runtimes.codex]
enabled = true
adapter = "agent_run.adapters.codex:ADAPTER"
binary = "/bin/codex"
home = "/tmp/codex"
models = ["test"]
accounts = ["{bad}"]
[runtimes.codex.auth]
kind = "file_link"
source = "/tmp/auth.json"
target = "auth.json"
'''
                )

    def test_accounts_require_file_link_and_default_must_be_declared(self) -> None:
        base = '''schema_version = 1
[runtimes.fake]
enabled = true
adapter = "example.adapter:ADAPTER"
binary = "/bin/echo"
home = "/tmp/runtime-home"
models = ["test"]
accounts = ["personal"]
'''
        with self.assertRaisesRegex(ValidationError, "requires file_link"):
            self.load(base)
        with self.assertRaisesRegex(ValidationError, "default_account"):
            self.load(
                base
                + 'default_account = "missing"\n'
                + '[runtimes.fake.auth]\nkind = "file_link"\nsource = "/tmp/auth"\ntarget = "auth.json"\n'
            )

    def test_limits_source_defaults_to_native_and_is_validated(self) -> None:
        self.assertIsNone(self.runtime_with_limits_source().runtimes["fake"].limits_source)
        for source in ("native", "omniroute", "codexbar", "none"):
            with self.subTest(source=source):
                configured = self.runtime_with_limits_source(f'limits_source = "{source}"')
                self.assertEqual(
                    configured.runtimes["fake"].limits_source, source
                )
        for bad in ("codex", "native,evil", "Codexbar"):
            with self.subTest(bad=bad), self.assertRaisesRegex(
                ValidationError, r"runtimes\.fake\.limits_source"
            ):
                self.runtime_with_limits_source(f'limits_source = "{bad}"')

    def test_codexbar_binary_defaults_to_homebrew_and_must_be_absolute(self) -> None:
        self.assertEqual(
            self.load("schema_version = 1\n").capacity.codexbar_binary,
            Path("/opt/homebrew/bin/codexbar"),
        )
        configured = self.load('schema_version = 1\n[capacity]\ncodexbar_binary = "/bin/echo"\n')
        self.assertEqual(configured.capacity.codexbar_binary, Path("/bin/echo"))
        with self.assertRaisesRegex(ValidationError, "capacity.codexbar_binary"):
            self.load('schema_version = 1\n[capacity]\ncodexbar_binary = "relative"\n')

    def runtime_with_plugins(self, value: str, directory: Path):
        return self.load(
            f"""schema_version = 1
[runtimes.fake]
enabled = true
adapter = "example.adapter:ADAPTER"
binary = "/bin/echo"
home = "{directory}"
models = ["test"]
plugins = {value}
"""
        )

    def test_runtime_plugins_default_to_none_and_must_be_existing_directories(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            plugin = directory / "compressor"
            plugin.mkdir()
            regular = directory / "manifest.json"
            regular.write_text("{}", encoding="utf-8")

            self.assertEqual(self.runtime_with_plugins("[]", directory).runtimes["fake"].plugins, ())
            declared = self.runtime_with_plugins(f'["{plugin}"]', directory)
            self.assertEqual(declared.runtimes["fake"].plugins, (plugin,))

            for value in (f'["{directory / "missing"}"]', f'["{regular}"]'):
                with self.subTest(value=value), self.assertRaisesRegex(
                    ValidationError, r"runtimes\.fake\.plugins\[0\] must be an existing directory"
                ):
                    self.runtime_with_plugins(value, directory)
            with self.assertRaisesRegex(ValidationError, r"runtimes\.fake\.plugins\[0\]"):
                self.runtime_with_plugins('["relative/path"]', directory)
            with self.assertRaisesRegex(ValidationError, "must be an array"):
                self.runtime_with_plugins(f'"{plugin}"', directory)
            with self.assertRaisesRegex(ValidationError, "declared twice"):
                self.runtime_with_plugins(f'["{plugin}", "{plugin}"]', directory)

    def test_core_and_capacity_bounds_fail_during_load(self) -> None:
        invalid = (
            ("core", "warning_fraction", "true"),
            ("core", "warning_fraction", '"0.5"'),
            ("core", "warning_fraction", "0"),
            ("core", "warning_fraction", "1"),
            ("core", "stalled_after_seconds", "true"),
            ("core", "stalled_after_seconds", "-1"),
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
stalled_after_seconds = 0
[capacity]
collect_interval_seconds = 1
context_max_chars = 1
"""
        )
        self.assertEqual(lower.core.stalled_after_seconds, 0)
        self.assertEqual(
            self.load("schema_version = 1\n").core.stalled_after_seconds, 900
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
