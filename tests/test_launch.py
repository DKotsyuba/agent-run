import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from agent_run.errors import ValidationError
from agent_run.launch import launch_detached


CHILD_MODULE = "tests.detached_child"


def child_pythonpath() -> str:
    """The exec'd child gets a fresh interpreter, so it needs its own path."""

    parts = [str(REPO_ROOT), str(REPO_ROOT / "src")]
    existing = os.environ.get("PYTHONPATH")
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)


@unittest.skipUnless(hasattr(os, "fork") and hasattr(os, "setsid"), "POSIX only")
class DetachedLaunchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        patched = mock.patch.dict(os.environ, {"PYTHONPATH": child_pythonpath()})
        patched.start()
        self.addCleanup(patched.stop)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def launch(self, payload: dict, **kwargs) -> int:
        return launch_detached(
            payload, executable=sys.executable, module=CHILD_MODULE, **kwargs
        )

    def wait_for(self, predicate, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("timed out waiting for detached process evidence")
            time.sleep(0.01)

    def test_parent_returns_after_ready_before_terminal_and_dispatches_once(self) -> None:
        events = self.root / "events"
        gate = self.root / "gate"
        dispatched = self.root / "dispatched"

        pid = self.launch(
            {
                "events": str(events),
                "gate": str(gate),
                "dispatch": str(dispatched),
            }
        )
        self.assertTrue(self.alive(pid))
        self.assertEqual(events.read_text(), "before-ready\n")
        self.assertFalse(dispatched.exists())

        gate.touch()
        self.wait_for(dispatched.exists)
        self.wait_for(lambda: not self.alive(pid))
        self.assertEqual(events.read_text(), "before-ready\nterminal\n")
        self.assertEqual(dispatched.read_text(), "once\n")

    def test_wrapper_and_grandchild_outlive_the_returning_caller(self) -> None:
        evidence = self.root / "pids"
        gate = self.root / "gate"

        wrapper = self.launch(
            {"evidence": str(evidence), "gate": str(gate), "grandchild": True}
        )
        self.wait_for(evidence.exists)
        reported_wrapper, grandchild = map(int, evidence.read_text().split())
        self.assertEqual(reported_wrapper, wrapper)
        self.assertTrue(self.alive(wrapper))
        self.assertTrue(self.alive(grandchild))

        gate.touch()
        self.wait_for(lambda: not self.alive(wrapper))
        self.wait_for(lambda: not self.alive(grandchild))

    def test_pre_ready_failure_is_reported_reaped_and_dispatches_once(self) -> None:
        pid_path = self.root / "pid"
        dispatched = self.root / "dispatched"

        with mock.patch("agent_run.launch.verify_process_group", return_value=None):
            with self.assertRaisesRegex(ValidationError, "RuntimeError: startup broke"):
                self.launch(
                    {
                        "evidence": str(pid_path),
                        "fail": "startup broke",
                        "dispatch": str(dispatched),
                    }
                )
        pid = int(pid_path.read_text())
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)
        self.assertEqual(dispatched.read_text(), "once\n")

    def test_identity_mismatch_is_refused_before_ready_is_awaited(self) -> None:
        with self.assertRaisesRegex(
            ValidationError, "reported the wrong process identity"
        ):
            self.launch({"report_pid": 2, "ready": False})

    def test_fast_ready_terminal_exit_does_not_require_a_live_group_sample(self) -> None:
        with mock.patch("agent_run.launch.verify_process_group", return_value=None):
            pid = self.launch({})
        self.wait_for(lambda: not self.alive(pid))

    def test_readiness_timeout_kills_verified_wrapper_and_grandchild_group(self) -> None:
        evidence = self.root / "pids"
        fail_safe_seconds = 12.0

        started = time.monotonic()
        try:
            self.launch(
                {
                    "evidence": str(evidence),
                    "grandchild": True,
                    "ready": False,
                    "fail_safe_seconds": fail_safe_seconds,
                },
                readiness_timeout_seconds=2.0,
                post_terminal_timeout_seconds=0.05,
                cleanup_grace_seconds=0.05,
                cleanup_kill_seconds=0.05,
            )
        except ValidationError as error:
            caught = error
        else:
            self.fail("readiness timeout unexpectedly succeeded")

        self.wait_for(evidence.exists)
        wrapper, grandchild = map(int, evidence.read_text().split())
        if str(caught).startswith("cannot signal process group "):
            os.waitpid(wrapper, 0)
            self.wait_for(lambda: not self.alive(grandchild))
            with self.assertRaises(ChildProcessError):
                os.waitpid(wrapper, os.WNOHANG)
            self.skipTest(
                "macOS/Codex sandbox returned EPERM for killpg on the verified child group"
            )

        self.assertRegex(str(caught), "ready in time")
        self.wait_for(lambda: not self.alive(wrapper))
        self.wait_for(lambda: not self.alive(grandchild))
        self.assertLess(time.monotonic() - started, fail_safe_seconds / 2)
        with self.assertRaises(ChildProcessError):
            os.waitpid(wrapper, os.WNOHANG)

    def test_post_terminal_dispatch_is_bounded_and_never_reruns_the_child(self) -> None:
        events = self.root / "events"
        dispatched = self.root / "dispatched"

        pid = self.launch(
            {
                "events": str(events),
                "dispatch": str(dispatched),
                "dispatch_sleep": 10,
            },
            post_terminal_timeout_seconds=0.05,
        )
        self.wait_for(dispatched.exists)
        self.wait_for(lambda: not self.alive(pid))
        self.assertEqual(events.read_text(), "before-ready\nterminal\n")
        self.assertEqual(dispatched.read_text(), "once\n")

    def test_post_reap_receives_exact_pid_and_wait_status(self) -> None:
        report = self.root / "reaped"

        def reaped(pid, status):
            report.write_text(f"{pid}:{status}", encoding="utf-8")

        pid = self.launch({}, post_reap=reaped, post_reap_timeout_seconds=1)
        self.wait_for(
            lambda: report.exists()
            and report.read_text().count(":") == 1
            and not report.read_text().endswith(":")
        )
        reported_pid, status = map(int, report.read_text().split(":"))
        self.assertEqual(reported_pid, pid)
        self.assertTrue(os.WIFEXITED(status))

    def test_payload_and_executable_are_validated_before_any_fork(self) -> None:
        with self.assertRaisesRegex(ValidationError, "payload must be a mapping"):
            launch_detached("not-a-mapping", executable=sys.executable)
        with self.assertRaisesRegex(ValidationError, "executable must be a nonblank"):
            launch_detached({}, executable="")
        with self.assertRaisesRegex(ValidationError, "payload is not JSON"):
            self.launch({"plan": object()})

    @staticmethod
    def alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True
