import tempfile
import unittest
from pathlib import Path

from agent_run.doctor import run_doctor
from agent_run.domain import AgentStatus, StartRequest
from agent_run.state import StateStore


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
