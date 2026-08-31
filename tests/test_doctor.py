import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from agent_run import doctor
from agent_run.config import Config, RuntimeAuthConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.doctor import run_doctor
from agent_run.domain import AgentStatus, StartRequest
from agent_run.launch_evidence import FAILURE_KIND_EXECUTABLE_MISSING
from agent_run.state import StateStore

from tests.test_launch import child_pythonpath


class DoctorTests(unittest.TestCase):
    def test_reports_bounded_metadata_without_mutating_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            runtime_home = home / "runtime-codex"
            runtime_home.mkdir()
            config = home / "config.toml"
            config.write_text(
                f'''schema_version = 1
[mcp.missing]
transport = "stdio"
command = "{home / 'missing-mcp'}"
[runtimes.codex]
enabled = true
adapter = "example:ADAPTER"
binary = "{home / 'missing-codex'}"
home = "{runtime_home}"
models = ["model"]
skills = ["review"]
mcp = ["missing"]
[runtimes.codex.auth]
kind = "file_link"
source = "{home / 'missing-auth'}"
target = "auth.json"
[[runtimes.codex.hooks]]
event = "PostToolUse"
command = ["relative-hook"]
[runtimes.claude]
enabled = true
adapter = "example:ADAPTER"
binary = "{home / 'missing-claude'}"
home = "{home / 'missing-home'}"
models = ["model"]
''',
                encoding="utf-8",
            )
            store = StateStore.initialize(home / "state.db")
            request = StartRequest(
                "codex", "model", "profile", "task", home, timeout_seconds=10
            )
            agent_id = store.create_agent(
                request, task_summary="task", config_revision="cfg", at=1
            ).agent_id
            store.transition(agent_id, AgentStatus.STARTING, at=2)
            store.record_supervisor(
                agent_id,
                pid=100,
                identity="expected",
                process_group_id=200,
                at=3,
            )
            store.transition(agent_id, AgentStatus.RUNNING, at=4)
            store.insert_capacity_sample(
                runtime="codex", lane="requests", window="5h", source="test",
                payload={}, observed_at=1, valid_until=2,
            )
            store.close()
            database = home / "state.db"
            before = (database.stat().st_mtime_ns, database.stat().st_mode)

            report = run_doctor(
                home,
                at=1_000,
                process_probe=lambda pid, pgid: (False, None, True),
            )

            codes = {finding.code for finding in report.findings}
            self.assertTrue(
                {
                    "mcp_executable_missing",
                    "runtime_binary_missing",
                    "runtime_home_missing",
                    "runtime_home_unsupported",
                    "runtime_skill_missing",
                    "hook_executable_missing",
                    "hook_untrusted",
                    "auth_source_missing",
                    "auth_bridge_missing",
                    "capacity_stale",
                    "dead_supervisor",
                    "suspected_orphan",
                }.issubset(codes)
            )
            self.assertFalse(report.ok)
            self.assertEqual(before, (database.stat().st_mtime_ns, database.stat().st_mode))
            for suffix in ("-wal", "-shm"):
                candidate = Path(f"{database}{suffix}")
                if candidate.exists():
                    self.assertEqual(candidate.stat().st_mode & 0o777, 0o600)
            reopened = StateStore.open(database)
            try:
                self.assertEqual(reopened.get_agent(agent_id)["status"], "running")
            finally:
                reopened.close()

    def test_plaintext_secret_is_named_but_never_returned(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "config.toml").write_text(
                'schema_version = 1\napi_key = "must-not-leak"\n', encoding="utf-8"
            )
            report = run_doctor(home, at=1)
            self.assertIn("plaintext_secret_config", {item.code for item in report.findings})
            self.assertNotIn("must-not-leak", repr(report))

    def _minimal_home(self, directory: str) -> Path:
        home = Path(directory).resolve()
        (home / "config.toml").write_text("schema_version = 1\n", encoding="utf-8")
        StateStore.initialize(home / "state.db").close()
        # Truncate the WAL sidecar so the snapshot's read-only URI open works
        # under every interpreter's bundled sqlite (3.12 refuses a ro open of
        # a WAL database whose -wal has not been checkpointed).
        import sqlite3 as _sqlite3

        checkpoint = _sqlite3.connect(home / "state.db")
        try:
            checkpoint.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            checkpoint.close()
        return home

    @unittest.skipUnless(hasattr(os, "fork") and hasattr(os, "setsid"), "POSIX only")
    def test_canary_handshake_ok_reports_a_completed_real_handshake(self) -> None:
        previous = os.environ.get("PYTHONPATH")
        os.environ["PYTHONPATH"] = child_pythonpath()
        try:
            with tempfile.TemporaryDirectory() as directory:
                home = self._minimal_home(directory)
                report = run_doctor(home, mcp_process_lister=lambda: [])
        finally:
            if previous is None:
                os.environ.pop("PYTHONPATH", None)
            else:
                os.environ["PYTHONPATH"] = previous

        canary = [item for item in report.findings if item.component == "canary"]
        self.assertEqual([item.code for item in canary], ["supervisor_canary_ok"])
        self.assertEqual(canary[0].severity, "info")

    def test_canary_handshake_fail_reports_the_bootstrap_failure_kind(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = self._minimal_home(directory)
            report = run_doctor(
                home,
                canary_executable=str(Path(directory) / "no-such-python"),
                mcp_process_lister=lambda: [],
            )

        canary = [item for item in report.findings if item.component == "canary"]
        self.assertEqual([item.code for item in canary], [FAILURE_KIND_EXECUTABLE_MISSING])
        self.assertEqual(canary[0].severity, "error")
        self.assertFalse(report.ok)

    def test_mcp_inventory_flags_processes_older_than_the_release_switch_and_excludes_self(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = self._minimal_home(directory)
            (home / "standalone").mkdir()
            current = home / "standalone" / "current"
            current.symlink_to(home / "standalone" / "releases" / "sha-new")
            switch_epoch = 2_000_000.0
            os.utime(current, (switch_epoch, switch_epoch), follow_symlinks=False)

            self_pid = os.getpid()
            stale_pid = self_pid + 10_000
            fresh_pid = self_pid + 10_001

            def lstart(epoch: float) -> str:
                return time.strftime("%a %b %d %H:%M:%S %Y", time.localtime(epoch))

            def fake_ps() -> list[tuple[int, str, str]]:
                return [
                    (self_pid, lstart(switch_epoch - 500), "python3 -m agent_run.cli mcp"),
                    (
                        stale_pid,
                        lstart(switch_epoch - 3600),
                        "/old/release/venv/bin/python3 /old/release/venv/bin/agent-run mcp",
                    ),
                    (
                        fresh_pid,
                        lstart(switch_epoch + 3600),
                        "/new/release/venv/bin/python3 /new/release/venv/bin/agent-run mcp",
                    ),
                ]

            report = run_doctor(
                home,
                canary_runner=lambda: 0.0,
                mcp_process_lister=fake_ps,
            )

        by_component = {item.component: item for item in report.findings}
        self.assertNotIn(f"mcp:{self_pid}", by_component)
        self.assertIn("mcp:self", by_component)

        stale = by_component[f"mcp:{stale_pid}"]
        self.assertEqual(stale.code, "mcp_process_older_release")
        self.assertEqual(stale.severity, "warning")
        self.assertIn("/old/release", stale.detail)

        fresh = by_component[f"mcp:{fresh_pid}"]
        self.assertEqual(fresh.code, "mcp_process")
        self.assertEqual(fresh.severity, "info")


class KeychainFallbackAuthTests(unittest.TestCase):
    """Environment auth is satisfied by a keychain item, not only by env."""

    def _runtime(self) -> RuntimeConfig:
        return RuntimeConfig(
            enabled=True,
            adapter="example:ADAPTER",
            binary=Path("/usr/bin/true"),
            home=Path("/tmp"),
            models=("model",),
            auth=RuntimeAuthConfig(kind="environment", names=("GLM_CODING_KEY",)),
        )

    def _auth_findings(self, name: str, runtime: RuntimeConfig) -> list:
        findings: list = []
        with mock.patch.dict(os.environ, {}, clear=True):
            doctor._auth(name, runtime, f"runtime:{name}", findings)
        return findings

    def test_a_present_keychain_item_suppresses_the_warning(self) -> None:
        # security prints the secret on stdout; it must be discarded unread.
        present = subprocess.CompletedProcess([], 0, stdout="super-secret-value\n", stderr="")
        with mock.patch.object(doctor.subprocess, "run", return_value=present) as probe:
            findings = self._auth_findings("glm", self._runtime())

        self.assertEqual([item.code for item in findings], [])
        self.assertEqual(probe.call_count, 1)
        argv = probe.call_args[0][0]
        self.assertEqual(
            argv,
            [
                "security",
                "find-generic-password",
                "-s",
                "com.pluto.agent-run.glm",
                "-a",
                "GLM_CODING_KEY",
                "-w",
            ],
        )
        self.assertTrue(
            all("super-secret-value" not in (item.detail or "") for item in findings)
        )

    def test_an_absent_keychain_item_keeps_the_warning(self) -> None:
        absent = subprocess.CompletedProcess([], 44, stdout="", stderr="could not be found")
        with mock.patch.object(doctor.subprocess, "run", return_value=absent):
            findings = self._auth_findings("qwen", self._runtime())

        self.assertEqual([item.code for item in findings], ["auth_environment_missing"])
        self.assertEqual(findings[0].severity, "warning")

    def test_a_runtime_without_a_fallback_is_never_probed(self) -> None:
        with mock.patch.object(doctor.subprocess, "run") as probe:
            findings = self._auth_findings("codex", self._runtime())

        self.assertEqual([item.code for item in findings], ["auth_environment_missing"])
        probe.assert_not_called()

    def test_a_failing_probe_counts_as_absent(self) -> None:
        with mock.patch.object(
            doctor.subprocess, "run", side_effect=OSError("security missing")
        ):
            findings = self._auth_findings("glm", self._runtime())

        self.assertEqual([item.code for item in findings], ["auth_environment_missing"])

    def test_an_item_without_a_fixed_account_is_probed_by_service_alone(self) -> None:
        # claude stores its credential with no fixed account, so the probe
        # must omit -a: an -a lookup of any account name would not resolve it.
        present = subprocess.CompletedProcess([], 0, stdout="super-secret-value\n", stderr="")
        with mock.patch.object(doctor.subprocess, "run", return_value=present) as probe:
            findings = self._auth_findings("claude", self._runtime())

        self.assertEqual([item.code for item in findings], [])
        self.assertEqual(
            probe.call_args[0][0],
            [
                "security",
                "find-generic-password",
                "-s",
                "Claude Code-credentials",
                "-w",
            ],
        )
        self.assertTrue(
            all("super-secret-value" not in (item.detail or "") for item in findings)
        )

    def test_an_item_without_a_fixed_account_keeps_the_warning_when_absent(self) -> None:
        absent = subprocess.CompletedProcess([], 44, stdout="", stderr="could not be found")
        with mock.patch.object(doctor.subprocess, "run", return_value=absent):
            findings = self._auth_findings("claude", self._runtime())

        self.assertEqual([item.code for item in findings], ["auth_environment_missing"])
        self.assertEqual(findings[0].severity, "warning")


class HookTrustTests(unittest.TestCase):
    """A trusted script behind a system interpreter is a trusted hook.

    Rendered hooks run [interpreter, script, ...]; the interpreter is a system
    path outside the trusted roots by design, so trust is judged from the
    script argument instead.
    """

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.home = Path(directory.name).resolve()
        self.root = self.home / "install"
        (self.root / "hooks").mkdir(parents=True)

    def _findings(
        self, command: tuple[str, ...], plugins: tuple[Path, ...] = ()
    ) -> list:
        runtime = RuntimeConfig(
            enabled=True,
            adapter="example:ADAPTER",
            binary=Path("/usr/bin/true"),
            home=self.home,
            models=("model",),
            hooks=(RuntimeHookConfig(event="PostToolUse", command=command),),
            plugins=plugins,
        )
        findings: list = []
        trusted = (self.root, (self.root / "standalone" / "current").resolve())
        doctor._hooks(runtime, "runtime:claude", trusted, findings)
        return findings

    def _codes(self, command: tuple[str, ...]) -> list:
        return [item.code for item in self._findings(command)]

    def test_a_script_inside_the_trusted_roots_is_trusted(self) -> None:
        script = self.root / "hooks" / "context.py"
        script.write_text("pass\n", encoding="utf-8")

        self.assertEqual(
            self._codes(("/usr/bin/python3", str(script), "--event", "PostToolUse")),
            [],
        )

    def test_a_script_outside_the_trusted_roots_is_untrusted(self) -> None:
        script = self.home / "outside" / "context.py"

        findings = self._findings(("/usr/bin/python3", str(script)))

        self.assertEqual([item.code for item in findings], ["hook_untrusted"])
        self.assertEqual(findings[0].severity, "warning")
        self.assertEqual(findings[0].detail, str(script))

    def test_an_interpreter_without_a_script_stays_untrusted(self) -> None:
        # No non-flag argument at all.
        self.assertEqual(
            self._codes(("/usr/bin/python3",)),
            ["hook_untrusted"],
        )
        # Flags only: the first non-flag word is a shell string, not a path.
        self.assertEqual(
            self._codes(("/bin/sh", "-c", "echo hi")),
            ["hook_untrusted"],
        )
        self.assertEqual(
            [item.detail for item in self._findings(("/bin/sh", "-c", "echo hi"))],
            ["echo hi"],
        )

    def test_a_declared_plugin_token_is_trusted(self) -> None:
        # Adapters expand {plugin:NAME} under the runtime home, each with its
        # own layout; the token plus the declaration is the trust evidence.
        self.assertEqual(
            self._codes_with_plugins(
                ("/usr/bin/python3", "{plugin:agent-lsp-plugin}/hooks/guard.py"),
                (Path("/anywhere/agent-lsp-plugin"),),
            ),
            [],
        )

    def test_an_undeclared_plugin_token_stays_untrusted(self) -> None:
        self.assertEqual(
            self._codes_with_plugins(
                ("/usr/bin/python3", "{plugin:agent-lsp-plugin}/hooks/guard.py"),
                (),
            ),
            ["hook_untrusted"],
        )

    def _codes_with_plugins(
        self, command: tuple[str, ...], plugins: tuple[Path, ...]
    ) -> list:
        return [item.code for item in self._findings(command, plugins)]


class CapacityStalenessTests(unittest.TestCase):
    """``valid_until`` decides staleness; age is only the fallback bound."""

    @staticmethod
    def _row(lane: str, observed_at: float, valid_until: float | None) -> dict:
        return {
            "runtime": "qwen",
            "lane": lane,
            "window": "5h",
            "target": None,
            "source": "omniroute",
            "observed_at": observed_at,
            "valid_until": valid_until,
        }

    def _lanes(self, rows: list[dict], at: float = 1_000) -> list[str]:
        findings: list = []
        doctor._capacity(Config(schema_version=1), rows, at, findings)
        return [item.detail.split("/")[0] for item in findings]

    def test_a_sample_still_within_its_validity_is_never_stale(self) -> None:
        # Older than twice the collection interval, so only valid_until
        # justifies calling it fresh.
        self.assertEqual(self._lanes([self._row("fresh", 100, 2_000)]), [])

    def test_an_expired_sample_is_stale_even_when_recently_observed(self) -> None:
        self.assertEqual(self._lanes([self._row("expired", 999, 500)]), ["expired"])

    def test_a_sample_without_validity_keeps_the_age_bound(self) -> None:
        rows = [self._row("aged", 100, None), self._row("recent", 900, None)]

        self.assertEqual(self._lanes(rows), ["aged"])

