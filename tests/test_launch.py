import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from agent_run.errors import ValidationError
from agent_run.launch import launch_detached


@unittest.skipUnless(hasattr(os, "fork") and hasattr(os, "setsid"), "POSIX only")
class DetachedLaunchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def wait_for(self, predicate, timeout: float = 3.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("timed out waiting for detached process evidence")
            time.sleep(0.01)

    def test_parent_returns_after_ready_before_terminal_and_dispatches_once(self) -> None:
        events = self.root / "events"
        gate = self.root / "gate"
        dispatched = self.root / "dispatched"

        def child(ready) -> None:
            events.write_text("before-ready\n")
            ready.ready()
            while not gate.exists():
                time.sleep(0.01)
            with events.open("a") as stream:
                stream.write("terminal\n")

        def dispatch() -> None:
            with dispatched.open("a") as stream:
                stream.write("once\n")

        pid = launch_detached(child, post_terminal=dispatch)
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

        def child(ready) -> None:
            grandchild = os.fork()
            if grandchild == 0:
                while not gate.exists():
                    time.sleep(0.01)
                os._exit(0)
            evidence.write_text(f"{os.getpid()} {grandchild}")
            ready.ready()
            while not gate.exists():
                time.sleep(0.01)
            os.waitpid(grandchild, 0)

        wrapper = launch_detached(child)
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

        def child(_ready) -> None:
            pid_path.write_text(str(os.getpid()))
            raise RuntimeError("startup broke")

        def dispatch() -> None:
            with dispatched.open("a") as stream:
                stream.write("once\n")

        with mock.patch("agent_run.launch.verify_process_group", return_value=None):
            with self.assertRaisesRegex(ValidationError, "RuntimeError: startup broke"):
                launch_detached(child, post_terminal=dispatch, readiness_timeout_seconds=1)
        pid = int(pid_path.read_text())
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)
        self.assertEqual(dispatched.read_text(), "once\n")

    def test_fast_ready_terminal_exit_does_not_require_a_live_group_sample(self) -> None:
        def child(ready) -> None:
            ready.ready()

        with mock.patch("agent_run.launch.verify_process_group", return_value=None):
            pid = launch_detached(child)
        self.wait_for(lambda: not self.alive(pid))

    def test_readiness_timeout_kills_verified_wrapper_and_grandchild_group(self) -> None:
        evidence = self.root / "pids"
        fail_safe_seconds = 2.0

        def child(_ready) -> None:
            expires = time.monotonic() + fail_safe_seconds
            grandchild = os.fork()
            if grandchild == 0:
                while time.monotonic() < expires:
                    time.sleep(0.02)
                os._exit(0)
            evidence.write_text(f"{os.getpid()} {grandchild}")
            while time.monotonic() < expires:
                time.sleep(0.02)
            os.waitpid(grandchild, 0)

        started = time.monotonic()
        try:
            launch_detached(
                child,
                readiness_timeout_seconds=0.15,
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

    def test_post_terminal_callback_is_bounded_and_never_reruns_child(self) -> None:
        ran = self.root / "ran"
        dispatched = self.root / "dispatched"

        def child(ready) -> None:
            with ran.open("a") as stream:
                stream.write("once\n")
            ready.ready()

        def dispatch() -> None:
            with dispatched.open("a") as stream:
                stream.write("once\n")
            time.sleep(10)

        pid = launch_detached(
            child, post_terminal=dispatch, post_terminal_timeout_seconds=0.05
        )
        self.wait_for(dispatched.exists)
        self.wait_for(lambda: not self.alive(pid))
        self.assertEqual(ran.read_text(), "once\n")
        self.assertEqual(dispatched.read_text(), "once\n")

    @staticmethod
    def alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True
