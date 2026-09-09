"""The exec'd supervisor entrypoint, including the fork-only SQLite regression."""

from __future__ import annotations

import glob
import json
import os
import site
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import LaunchPlan
from agent_run.domain import TERMINAL, AgentStatus, StartRequest
from agent_run.errors import ValidationError
from agent_run.launch import launch_detached
from agent_run.paths import agent_dir, state_db_path
from agent_run.preparation import PENDING_CONFIG_REVISION, request_payload
from agent_run.state.store import StateStore
from agent_run.verify import DEFAULT_SENTINEL

from tests.stub_engine_adapter import SECRET
from tests.test_launch import child_pythonpath
LAUNCH_ROUNDS = 10


def _framework_python() -> str | None:
    """A macOS Framework-build interpreter, if one is installed.

    Only a Framework build re-execs into its ``Resources/Python.app`` binary
    on launch, which is what makes ``ps -o command=`` report a different
    path than the argv0 it was exec'd with -- the exact hazard this
    reproduces. A non-Framework interpreter (including uv-managed runtimes)
    leaves argv0 alone, so it cannot trigger it. Only a supported Python 3.14
    Framework build is eligible; older installed Framework builds are skipped.
    """
    for candidate in sorted(
        glob.glob("/Library/Frameworks/Python.framework/Versions/*/bin/python3.*")
    ):
        name = os.path.basename(candidate)
        suffix = name[len("python3.") :] if name.startswith("python3.") else ""
        # The runtime now rejects old Framework interpreters before the
        # re-exec assertion can run; never select one as a test executable.
        if suffix == "14" and os.access(candidate, os.X_OK):
            return candidate
    return None


def _dependency_pythonpath() -> str:
    """Return repo imports plus this test interpreter's installed dependencies.

    A macOS Framework interpreter is deliberately used as the detached
    executable below, outside the project's virtual environment. It is the same
    supported CPython ABI but does not discover the environment's site-packages,
    so this explicit path preserves the re-exec identity hazard while letting
    the supervisor import the already-installed project dependency closure.
    """

    return os.pathsep.join(
        dict.fromkeys((child_pythonpath(), *site.getsitepackages()))
    )


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
        self.role_payload = json.loads(
            (Path(__file__).parent / "fixtures" / "role_plan_7bbd43b.json").read_text(
                encoding="utf-8"
            )
        )

    def create_agent(
        self,
        sleep_seconds: float = 0.0,
        *,
        runtime: str = "fake",
    ) -> tuple[str, Path, StartRequest]:
        """Create one admitted fixture and its matching supervisor request.

        The sleep_seconds value controls the deterministic stub engine duration,
        while runtime may select an intentionally missing runtime for failure tests.
        """

        request = StartRequest(
            runtime,
            "model",
            "profile",
            str(sleep_seconds),
            self.workdir,
            timeout_seconds=60,
        )
        agent_id = self.store.create_agent_limited(
            request,
            task_summary="task",
            config_revision=PENDING_CONFIG_REVISION,
            global_limit=LAUNCH_ROUNDS,
            runtime_limit=LAUNCH_ROUNDS,
            identity_json="{}",
        ).agent_id
        directory = agent_dir(agent_id, self.home)
        return str(agent_id), directory, request

    def payload(self, agent_id: str, request: StartRequest) -> dict[str, object]:
        """Serialize one admitted request and canonical role for the supervisor."""

        return {
            "agent_id": agent_id,
            "home": str(self.home),
            "request": request_payload(request),
            "role": self.role_payload,
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
            agent_id, directory, request = self.create_agent()
            pid = self.launch(self.payload(agent_id, request))
            self.wait_for(
                lambda agent=agent_id: self.status(agent) in TERMINAL,
                timeout=20.0,
            )
            self.assertIs(
                self.status(agent_id),
                AgentStatus.SUCCEEDED,
                f"round {round_number}",
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
        agent_id, directory, request = self.create_agent(sleep_seconds=1.5)
        symlink = Path(self.temporary.name) / "python-symlink"
        symlink.symlink_to(framework_python)
        self.environment["PYTHONPATH"] = _dependency_pythonpath()
        pid = self.launch(self.payload(agent_id, request), executable=str(symlink))
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

    def test_unknown_runtime_fails_durably_after_ready(self) -> None:
        """Own the process before persisting an unknown-runtime preparation failure."""

        agent_id, directory, request = self.create_agent(runtime="missing")
        pid = self.launch(self.payload(agent_id, request))
        self.wait_for(lambda: self.status(agent_id) in TERMINAL)
        self.assertIs(self.status(agent_id), AgentStatus.FAILED)
        self.wait_for(lambda: not self.alive(pid))
        row = self.store.get_agent(agent_id)
        self.assertEqual(row["failure_kind"], "prepare_runtime_failed")
        self.assertEqual(row["failure_text"], "runtime is not configured: missing")
        self.assertEqual(len(self.events(agent_id, "prepare_runtime_failed")), 1)
        self.assertFalse((directory / "answer.md").exists())

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
        error_read, error_write = os.pipe()
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
                "--error-fd",
                str(error_write),
            ],
            pass_fds=(payload_read, ready_write, identity_write, error_write),
            env=self.environment,
            start_new_session=True,
        )
        for descriptor in (payload_read, ready_write, identity_write, error_write):
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
            os.close(error_read)
        return token, process.wait(timeout=20)
