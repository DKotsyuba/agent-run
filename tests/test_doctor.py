import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

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

