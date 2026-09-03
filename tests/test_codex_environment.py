"""Regression coverage for the shared adapter-owned Codex child environment.

The launchd collector runs without an interactive shell, so its inherited
``PATH`` cannot resolve the interpreter a packaged ``codex`` launcher names in
its ``#!/usr/bin/env`` line.  These tests spawn a *real* local subprocess: a
stub executable whose shebang names an interpreter that only exists beside it,
speaking the ``initialize``/``account/rateLimits/read`` JSON-RPC handshake.  One
case loads the configured launcher from a real ``config.toml`` through
``load_config``, reproducing a version-managed ``bin/codex`` symlink.
Fictitious account ids and homes throughout; no provider or network access.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.codex import app_server
from agent_run.adapters.codex import environment as codex_environment
from agent_run.adapters.codex import model_cache as codex_model_cache
from agent_run.adapters.codex import rate_limits as codex_rate_limits
from agent_run.config import RuntimeConfig, load_config
from agent_run.errors import ValidationError

# Interpreter name resolvable only from the launcher directory itself.
_INTERPRETER_NAME = "agent-run-stub-python"
# File name of the configured Codex executable stub.
_STUB_BINARY_NAME = "stub-codex"
# launchd-like inherited PATH that cannot resolve the stub interpreter.
_MINIMAL_PATH = "/usr/bin:/bin"

# JSON-RPC body of the stub executable; its shebang names the Python
# interpreter symlinked beside it.  Every ``account/rateLimits/read`` answer
# echoes the child environment so the test can observe the HOME, CODEX_HOME,
# and effective PATH the engine actually ran with.
_STUB_BODY = """import json
import os
import sys

for line in sys.stdin:
    if not line.strip():
        continue
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        result = {"userAgent": {"name": "stub-codex", "version": "1"}}
    elif method == "account/rateLimits/read":
        path_entries = os.environ.get("PATH", "").split(os.pathsep)
        result = {
            "accountId": "acct-env",
            "home": os.environ.get("HOME"),
            "codexHome": os.environ.get("CODEX_HOME"),
            "pathHead": path_entries[0] if path_entries else None,
            "pathHasEmptyEntry": "" in path_entries,
        }
    else:
        result = {}
    reply = {"jsonrpc": "2.0", "id": message.get("id"), "result": result}
    sys.stdout.write(json.dumps(reply) + "\\n")
    sys.stdout.flush()
"""


def _write_stub(launcher: Path) -> Path:
    """Write the stub Codex executable and its private interpreter into ``launcher``.

    The executable carries a ``#!/usr/bin/env <interpreter>`` line exactly like
    a packaged CLI launcher, and the interpreter is symlinked beside it -- never
    onto the inherited PATH -- so the child can only resolve it through the
    launcher directory entry the shared helper is expected to prefix.  Returns
    the configured executable path.
    """

    launcher.mkdir(parents=True, exist_ok=True)
    (launcher / _INTERPRETER_NAME).symlink_to(sys.executable)
    executable = launcher / _STUB_BINARY_NAME
    executable.write_text(f"#!/usr/bin/env {_INTERPRETER_NAME}\n{_STUB_BODY}", encoding="utf-8")
    executable.chmod(0o755)
    return executable


def _write_packaged_launcher(root: Path) -> Path:
    """Lay out a version-managed launcher: a ``bin/codex`` symlink into a nested package.

    The configured executable is a symlink whose target lives in a nested
    package directory, while the shebang interpreter exists only beside the
    symlink -- the nvm-style layout in which the *resolved* target's directory
    cannot resolve the interpreter.  Returns the configured (linked) path.
    """

    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True)
    target = root / "lib" / "node_modules" / "@openai" / "codex" / "bin" / "codex.js"
    target.parent.mkdir(parents=True)
    target.write_text(f"#!/usr/bin/env {_INTERPRETER_NAME}\n{_STUB_BODY}", encoding="utf-8")
    target.chmod(0o755)
    (bin_dir / _INTERPRETER_NAME).symlink_to(sys.executable)
    linked = bin_dir / "codex"
    linked.symlink_to(target)
    return linked



def _config(binary: Path, home: Path) -> RuntimeConfig:
    """Build a minimal Codex runtime configuration for ``binary`` and ``home``."""

    return RuntimeConfig(
        enabled=True,
        adapter="fake:CODEX",
        binary=binary,
        home=home,
        models=("model-a",),
    )


class RealSubprocessRateLimitsTests(unittest.TestCase):
    """Run the collector probe against a real stub subprocess, not a mock."""

    def test_probe_resolves_the_bundled_interpreter_for_base_and_account_homes(self):
        """Both homes answer through the real launcher under a minimal PATH."""

        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            executable = _write_stub(root / "launcher")
            base_home = root / "home-base"
            account_home = root / "home-account"
            base_home.mkdir()
            account_home.mkdir()
            config = _config(executable, base_home)
            with mock.patch.dict(os.environ, {"PATH": _MINIMAL_PATH}):
                base = codex_rate_limits.read_rate_limits(config, base_home)
                account = codex_rate_limits.read_rate_limits(config, account_home)
        for observed, home in ((base, base_home), (account, account_home)):
            self.assertEqual(observed["accountId"], "acct-env")
            self.assertEqual(observed["home"], str(home))
            self.assertEqual(observed["codexHome"], str(home))
            self.assertEqual(observed["pathHead"], str(executable.parent))
            self.assertFalse(observed["pathHasEmptyEntry"])


class BuildEnvironmentTests(unittest.TestCase):
    """Verify PATH ordering, dedup, fallback, and home confinement."""

    def test_path_prefixes_the_binary_directory_and_keeps_inherited_order(self):
        """The launcher directory leads and inherited entries keep their order."""

        with mock.patch.dict(os.environ, {"PATH": "/usr/local/bin:/usr/bin:/bin"}):
            environment = codex_environment.build_environment(
                Path("/opt/tools/bin/codex"), Path("/tmp/home")
            )
        self.assertEqual(environment["PATH"], "/opt/tools/bin:/usr/local/bin:/usr/bin:/bin")

    def test_repeated_entries_are_deduplicated_without_reordering(self):
        """A first-seen entry survives exactly once, at its first position."""

        with mock.patch.dict(
            os.environ, {"PATH": "/opt/tools/bin:/usr/bin:/opt/tools/bin:/bin:/usr/bin"}
        ):
            environment = codex_environment.build_environment(
                Path("/opt/tools/bin/codex"), Path("/tmp/home")
            )
        self.assertEqual(environment["PATH"], "/opt/tools/bin:/usr/bin:/bin")

    def test_missing_or_empty_path_falls_back_without_empty_entries(self):
        """An absent or blank inherited PATH yields defpath entries only."""

        for inherited in (None, ""):
            patch = {} if inherited is None else {"PATH": inherited}
            with mock.patch.dict(os.environ, patch, clear=True):
                environment = codex_environment.build_environment(
                    Path("/opt/tools/bin/codex"), Path("/tmp/home")
                )
            entries = environment["PATH"].split(os.pathsep)
            self.assertEqual(entries[0], "/opt/tools/bin")
            self.assertEqual(
                entries[1:], [entry for entry in os.defpath.split(os.pathsep) if entry]
            )

    def test_relative_binary_is_rejected_instead_of_searching_the_workdir(self):
        """A relative executable fails loudly rather than gaining a PATH hole."""

        with self.assertRaises(ValidationError):
            codex_environment.build_environment(Path("codex"), Path("/tmp/home"))

    def test_home_and_codex_home_track_only_the_supplied_home(self):
        """Each supplied home is used verbatim and never mixed with another."""

        base = codex_environment.build_environment(Path("/opt/bin/codex"), Path("/tmp/base"))
        account = codex_environment.build_environment(Path("/opt/bin/codex"), Path("/tmp/plus"))
        self.assertEqual(base["HOME"], "/tmp/base")
        self.assertEqual(base["CODEX_HOME"], "/tmp/base")
        self.assertEqual(account["HOME"], "/tmp/plus")
        self.assertEqual(account["CODEX_HOME"], "/tmp/plus")
        self.assertEqual(set(base), {"HOME", "CODEX_HOME", "PATH"})


class SharedHelperCallersTests(unittest.TestCase):
    """Prove both collector probes launch through the shared environment."""

    def test_read_rate_limits_launches_with_the_shared_environment(self):
        """The rate-limit plan carries exactly ``build_environment`` output."""

        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            executable = _write_stub(root / "launcher")
            home = root / "home"
            home.mkdir()
            captured = {}

            def fake_fetch(plan, *, timeout_seconds):
                """Capture the launch plan and return a scripted response."""

                captured["plan"] = plan
                return {"accountId": "acct-env"}

            with mock.patch.object(codex_rate_limits, "fetch_rate_limits", fake_fetch):
                codex_rate_limits.read_rate_limits(_config(executable, home), home)
        self.assertEqual(
            captured["plan"].environment,
            codex_environment.build_environment(executable, home),
        )

    def test_refresh_models_launches_with_the_shared_environment(self):
        """The model-roster refresh plan carries exactly the same environment."""

        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            executable = _write_stub(root / "launcher")
            home = root / "home"
            home.mkdir()
            captured = {}

            def fake_fetch_models(plan, *, timeout_seconds):
                """Capture the refresh launch plan and return no models."""

                captured["plan"] = plan
                return ()

            with mock.patch.object(app_server, "fetch_models", fake_fetch_models):
                codex_model_cache.refresh_models(_config(executable, home), home)
        self.assertEqual(
            captured["plan"].environment,
            codex_environment.build_environment(executable, home),
        )


class LoadedConfigLauncherTests(unittest.TestCase):
    """Load the launcher from TOML and reproduce the version-managed nvm layout."""

    # Exact keys the stub answers with; any extra key would mean a payload or
    # credential leaked out of the child process into the collector.
    _RESPONSE_KEYS = {"accountId", "home", "codexHome", "pathHead", "pathHasEmptyEntry"}

    def test_loaded_symlinked_launcher_resolves_its_own_interpreter(self):
        """A TOML-loaded ``bin/codex`` symlink keeps answering for base and account homes."""

        with tempfile.TemporaryDirectory() as workspace:
            root = Path(workspace)
            linked = _write_packaged_launcher(root)
            base_home = root / "home-base"
            account_home = root / "home-account"
            base_home.mkdir()
            account_home.mkdir()
            config_home = root / "agent-run-home"
            config_home.mkdir()
            config_path = config_home / "config.toml"
            config_path.write_text(
                "\n".join(
                    [
                        "schema_version = 1",
                        "[runtimes.codex]",
                        "enabled = true",
                        'adapter = "example.adapter:ADAPTER"',
                        f'binary = "{linked}"',
                        f'home = "{base_home}"',
                        'models = ["model-a"]',
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            runtime = load_config(config_path).runtimes["codex"]
            self.assertEqual(runtime.binary, linked)
            self.assertTrue(runtime.binary.is_symlink())
            self.assertEqual(runtime.binary.parent, root / "bin")
            with mock.patch.dict(os.environ, {"PATH": _MINIMAL_PATH}):
                base = codex_rate_limits.read_rate_limits(runtime, base_home)
                account = codex_rate_limits.read_rate_limits(runtime, account_home)
        for observed, home in ((base, base_home), (account, account_home)):
            self.assertEqual(set(observed), self._RESPONSE_KEYS)
            self.assertEqual(observed["accountId"], "acct-env")
            self.assertEqual(observed["home"], str(home))
            self.assertEqual(observed["codexHome"], str(home))
            self.assertEqual(observed["pathHead"], str(root / "bin"))
            self.assertFalse(observed["pathHasEmptyEntry"])
            self.assertTrue(
                all("token" not in str(value).lower() for value in observed.values())
            )


if __name__ == "__main__":
    unittest.main()
