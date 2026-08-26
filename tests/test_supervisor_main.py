"""The exec'd supervisor entrypoint, including the fork-only SQLite regression."""

from __future__ import annotations

import glob
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import LaunchPlan
from agent_run.domain import AgentStatus, StartRequest
from agent_run.errors import ValidationError
from agent_run.launch import launch_detached
from agent_run.paths import create_agent_dir, state_db_path
from agent_run.state.store import StateStore
from agent_run.verify import DEFAULT_SENTINEL

from tests.test_launch import child_pythonpath


ENGINE = r'printf "%s\n%s\n" "$STUB_SECRET" "$STUB_SENTINEL" > "$1"'
SECRET = "opencode-server-password-2f7c"
LAUNCH_ROUNDS = 10


def _framework_python() -> str | None:
    """A macOS Framework-build interpreter, if one is installed.

    Only a Framework build re-execs into its ``Resources/Python.app`` binary
    on launch, which is what makes ``ps -o command=`` report a different
    path than the argv0 it was exec'd with -- the exact hazard this
    reproduces. A non-Framework interpreter (pyenv, Homebrew) leaves argv0
    alone, so it cannot trigger it.
    """
    for candidate in sorted(
        glob.glob("/Library/Frameworks/Python.framework/Versions/*/bin/python3.*")
    ):
        name = os.path.basename(candidate)
        suffix = name[len("python3.") :] if name.startswith("python3.") else ""
        if suffix.isdigit() and os.access(candidate, os.X_OK):
            return candidate
    return None


@unittest.skipUnless(hasattr(os, "fork") and hasattr(os, "setsid"), "POSIX only")
class SupervisorMainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.home = Path(self.temporary.name).resolve() / "home"
        self.workdir = self.home / "work"
        self.runtime_home = self.home / "runtime"
        for path in (self.home, self.workdir, self.runtime_home):
            path.mkdir(parents=True)
        (self.home / "config.toml").write_text(
            "schema_version = 1\n"
            "[core]\nwarning_fraction = 0.9\n"
            '[delivery]\ncodex_queue_bin = "/bin/true"\n'
            "[runtimes.fake]\n"
            "enabled = true\n"
            'adapter = "tests.stub_engine_adapter:ADAPTER"\n'
            'binary = "/bin/sh"\n'
            f'home = "{self.runtime_home}"\n'
            'models = ["model"]\n',
            encoding="utf-8",
        )
        # The parent keeps this connection open across every launch: that is the
        # exact state that made the fork-only child segfault inside sqlite3.
        self.store = StateStore.initialize(state_db_path(self.home))
        self.addCleanup(self.store.close)
        self.environment = dict(os.environ)
        self.environment["PYTHONPATH"] = child_pythonpath()

    def create_agent(self, sleep_seconds: float = 0.0) -> tuple[str, Path, LaunchPlan]:
        agent_id = self.store.create_agent(
            StartRequest(
                "fake", "model", "profile", "task", self.workdir, timeout_seconds=60
            ),
            task_summary="task",
            config_revision="rev-1",
        ).agent_id
        directory = create_agent_dir(agent_id, self.home)
        answer_path = directory / "answer.md"
        script = ENGINE if sleep_seconds <= 0 else f"{ENGINE}; sleep {sleep_seconds}"
        plan = LaunchPlan(
            ("/bin/sh", "-c", script, "sh", str(answer_path)),
            self.workdir,
            {"STUB_SECRET": SECRET, "STUB_SENTINEL": DEFAULT_SENTINEL},
            None,
            directory / "runtime.jsonl",
            {},
            answer_path,
        )
        return str(agent_id), directory, plan

    def payload(self, agent_id: str, directory: Path, plan: LaunchPlan) -> dict:
        return {
            "agent_id": agent_id,
            "home": str(self.home),
            "runtime": "fake",
            "timeout_seconds": 60.0,
            "answer_path": str(directory / "answer.md"),
            "agent_dir": str(directory),
            "warning_fraction": 0.9,
            "plan": plan.to_payload(),
        }

    def launch(self, payload: dict, *, executable: str | None = None, **kwargs) -> int:
        previous = os.environ.get("PYTHONPATH")
        os.environ["PYTHONPATH"] = self.environment["PYTHONPATH"]
        try:
            return launch_detached(
                payload,
                executable=executable or sys.executable,
                post_terminal_timeout_seconds=10.0,
                readiness_timeout_seconds=10.0,
                **kwargs,
            )
        finally:
            if previous is None:
                os.environ.pop("PYTHONPATH", None)
            else:
                os.environ["PYTHONPATH"] = previous

    def wait_for(self, predicate, timeout: float = 20.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("timed out waiting for the detached supervisor")
            time.sleep(0.01)

    def status(self, agent_id: str) -> AgentStatus:
        return AgentStatus(str(self.store.get_agent(agent_id)["status"]))

    def events(self, agent_id: str, kind: str) -> list:
        return list(
            self.store.connection.execute(
                "SELECT * FROM events WHERE agent_id = ? AND kind = ?",
                (agent_id, kind),
            )
        )

    @staticmethod
    def alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True

    def test_ten_consecutive_exec_launches_all_land_durably(self) -> None:
        for round_number in range(LAUNCH_ROUNDS):
            agent_id, directory, plan = self.create_agent()
            pid = self.launch(self.payload(agent_id, directory, plan))
            self.wait_for(
                lambda agent=agent_id: self.status(agent) is AgentStatus.SUCCEEDED,
                timeout=20.0,
            )
            self.wait_for(lambda child=pid: not self.alive(child))
            row = self.store.get_agent(agent_id)
            self.assertEqual(
                row["answer_path"], str(directory / "answer.md"), f"round {round_number}"
            )
            self.assertGreater(int(row["answer_bytes"]), 0)
            self.assertEqual(
                (directory / "answer.md").read_text(encoding="utf-8"),
                f"{SECRET}\n{DEFAULT_SENTINEL}\n",
            )
            self.assertEqual(len(self.events(agent_id, "stub_engine_launched")), 1)
            self.assertEqual(len(self.events(agent_id, "terminal")), 1)

    def test_recorded_identity_matches_the_exec_command_line(self) -> None:
        framework_python = _framework_python()
        if framework_python is None:
            self.skipTest("no macOS Framework Python build installed")

        # Exec through a symlink to a Framework build: it re-execs into its
        # own Resources/Python.app binary, so `ps -o command=` reports a
        # path that never appears in the argv passed to exec -- exactly the
        # hazard that broke reconciliation on the release venv.
        agent_id, directory, plan = self.create_agent(sleep_seconds=1.5)
        symlink = Path(self.temporary.name) / "python-symlink"
        symlink.symlink_to(framework_python)
        pid = self.launch(self.payload(agent_id, directory, plan), executable=str(symlink))
        self.wait_for(lambda: self.status(agent_id) is AgentStatus.RUNNING)

        recorded = str(self.store.get_agent(agent_id)["supervisor_identity"])
        observed = subprocess.run(
            ["/bin/ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
            timeout=5,
            env={"PATH": "/usr/bin:/bin"},
        ).stdout.strip()
        self.assertIn("agent_run.supervisor_main", recorded)
        self.assertEqual(recorded, observed)

        self.wait_for(lambda: self.status(agent_id) is AgentStatus.SUCCEEDED)
        self.wait_for(lambda: not self.alive(pid))

    def test_launch_plan_payload_round_trips_secrets_and_bytes(self) -> None:
        plan = LaunchPlan(
            ("engine", "--flag"),
            Path("/tmp/work"),
            {"OPENCODE_SERVER_PASSWORD": "s3cr3t", "CLAUDE_CODE_OAUTH_TOKEN": "tok"},
            b"\x00\xff binary prompt",
            Path("/tmp/runtime.jsonl"),
            {"thread": "th_1", "nested": {"n": 1}},
            Path("/tmp/answer.md"),
        )
        restored = LaunchPlan.from_payload(plan.to_payload())
        self.assertEqual(restored, plan)
        self.assertEqual(
            restored.environment["OPENCODE_SERVER_PASSWORD"], "s3cr3t"
        )

        text = LaunchPlan(
            ("engine",), Path("/tmp"), {}, "prompt", Path("/tmp/s"), {}, None
        )
        self.assertEqual(LaunchPlan.from_payload(text.to_payload()), text)

    def test_launch_plan_payload_fails_closed(self) -> None:
        payload = LaunchPlan(
            ("engine",), Path("/tmp"), {}, None, Path("/tmp/s"), {}, None
        ).to_payload()
        del payload["cwd"]
        with self.assertRaisesRegex(ValidationError, "malformed launch plan payload"):
            LaunchPlan.from_payload(payload)
        with self.assertRaisesRegex(ValidationError, "must be a mapping"):
            LaunchPlan.from_payload("nope")

    def test_unknown_runtime_fails_closed_before_ready(self) -> None:
        agent_id, directory, plan = self.create_agent()
        payload = self.payload(agent_id, directory, plan)
        payload["runtime"] = "missing"
        with self.assertRaisesRegex(ValidationError, "runtime is not configured"):
            self.launch(payload)
        self.assertIs(self.status(agent_id), AgentStatus.CREATED)

    def test_malformed_payload_exits_nonzero_with_a_ready_failure(self) -> None:
        token, code = self.run_entrypoint(b"{not json")
        self.assertTrue(token.startswith("fail:"), token)
        self.assertIn("malformed supervisor payload", token)
        self.assertNotEqual(code, 0)

        token, code = self.run_entrypoint(b'["a", "list"]')
        self.assertIn("must be a JSON object", token)
        self.assertNotEqual(code, 0)

    def run_entrypoint(self, blob: bytes) -> tuple[str, int]:
        payload_read, payload_write = os.pipe()
        ready_read, ready_write = os.pipe()
        identity_read, identity_write = os.pipe()
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "agent_run.supervisor_main",
                "--payload-fd",
                str(payload_read),
                "--ready-fd",
                str(ready_write),
                "--identity-fd",
                str(identity_write),
            ],
            pass_fds=(payload_read, ready_write, identity_write),
            env=self.environment,
            start_new_session=True,
        )
        for descriptor in (payload_read, ready_write, identity_write):
            os.close(descriptor)
        try:
            os.write(payload_write, blob)
        finally:
            os.close(payload_write)
        try:
            token = os.read(ready_read, 512).decode("utf-8", "replace").strip()
        finally:
            os.close(ready_read)
            os.close(identity_read)
        return token, process.wait(timeout=20)
