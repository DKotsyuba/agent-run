import json
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
from agent_run.launch import (
    DEFAULT_READY_TIMEOUT_SECONDS,
    ChildReaper,
    _spawn_session_leader,
    launch_detached,
)
from agent_run.launch_evidence import (
    FAILURE_KIND_BOOTSTRAP,
    FAILURE_KIND_EXECUTABLE_MISSING,
    SupervisorBootstrapError,
    write_exec_failure,
)


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
    def test_default_ready_budget_covers_observed_launchd_startup(self) -> None:
        self.assertGreaterEqual(DEFAULT_READY_TIMEOUT_SECONDS, 30.0)

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

    def test_ready_wait_cancellation_kills_and_reaps_verified_group(self) -> None:
        """Cancellation during READY must reuse verified group cleanup."""

        evidence = self.root / "cancel-pids"
        try:
            self.launch(
                {
                    "evidence": str(evidence),
                    "grandchild": True,
                    "ready": False,
                    "fail_safe_seconds": 12.0,
                },
                readiness_timeout_seconds=10.0,
                post_terminal_timeout_seconds=0.05,
                cleanup_grace_seconds=0.05,
                cleanup_kill_seconds=0.05,
                cancel_requested=evidence.exists,
            )
        except ValidationError as error:
            caught = error
        else:
            self.fail("cancelled readiness wait unexpectedly succeeded")

        self.wait_for(evidence.exists)
        wrapper, grandchild = map(int, evidence.read_text().split())
        if str(caught).startswith("cannot signal process group "):
            os.waitpid(wrapper, 0)
            self.wait_for(lambda: not self.alive(grandchild))
            self.skipTest(
                "macOS/Codex sandbox returned EPERM for killpg on the verified child group"
            )
        self.assertRegex(str(caught), "cancelled before supervisor READY")
        self.wait_for(lambda: not self.alive(wrapper))
        self.wait_for(lambda: not self.alive(grandchild))
        with self.assertRaises(ChildProcessError):
            os.waitpid(wrapper, os.WNOHANG)

    def test_preexisting_cancellation_bounds_unread_payload_and_reaps(self) -> None:
        """Cancellation before payload delivery cannot block or orphan the child."""

        started = time.monotonic()
        pids = []
        with mock.patch(
            "agent_run.launch._spawn_session_leader",
            side_effect=lambda *args: pids.append(_spawn_session_leader(*args))
            or pids[-1],
        ):
            with self.assertRaisesRegex(ValidationError, "cancelled before supervisor READY"):
                launch_detached(
                    {"payload": "x" * (1024 * 1024)},
                    executable=sys.executable,
                    module="tests.unread_payload_child",
                    readiness_timeout_seconds=0.05,
                    post_terminal_timeout_seconds=0.05,
                    cleanup_grace_seconds=0.05,
                    cleanup_kill_seconds=0.05,
                    cancel_requested=lambda: True,
                )

        self.assertLess(time.monotonic() - started, 2)
        pid = pids[0]
        self.assertFalse(self.alive(pid))
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)

    def test_unread_payload_obeys_ready_deadline_and_reaps(self) -> None:
        """A full unread payload pipe expires on the READY deadline and is reaped."""

        started = time.monotonic()
        pids = []
        with mock.patch(
            "agent_run.launch._spawn_session_leader",
            side_effect=lambda *args: pids.append(_spawn_session_leader(*args))
            or pids[-1],
        ):
            with self.assertRaisesRegex(ValidationError, "ready in time"):
                launch_detached(
                    {"payload": "x" * (1024 * 1024)},
                    executable=sys.executable,
                    module="tests.unread_payload_child",
                    readiness_timeout_seconds=0.05,
                    post_terminal_timeout_seconds=0.05,
                    cleanup_grace_seconds=0.05,
                    cleanup_kill_seconds=0.05,
                )

        self.assertLess(time.monotonic() - started, 2)
        pid = pids[0]
        self.assertFalse(self.alive(pid))
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)

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

    def test_targeted_reaper_preserves_launch_evidence(self) -> None:
        report = self.root / "reaped"
        reaper = ChildReaper()
        self.addCleanup(reaper.close)

        def reaped(pid, status):
            report.write_text(f"{pid}:{status}", encoding="utf-8")

        pid = self.launch({}, post_reap=reaped, child_reaper=reaper)
        self.wait_for(lambda: report.exists())
        reported_pid, status = map(int, report.read_text().split(":"))
        self.assertEqual(reported_pid, pid)
        self.assertTrue(os.WIFEXITED(status))
        with self.assertRaises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)

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

    def test_missing_executable_is_refused_before_any_fork(self) -> None:
        missing = str(Path(self.root) / "gone" / "python3")
        with mock.patch("agent_run.launch.os.fork") as forked:
            with self.assertRaisesRegex(
                ValidationError, "reconnect/restart this MCP session"
            ) as caught:
                launch_detached({}, executable=missing)
        forked.assert_not_called()
        error = caught.exception
        self.assertIsInstance(error, SupervisorBootstrapError)
        self.assertEqual(error.failure_kind, FAILURE_KIND_EXECUTABLE_MISSING)
        self.assertIn(missing, str(error))

    def test_posix_spawn_requests_setsid_without_fork_fallback(self) -> None:
        """Supported spawn failures propagate without entering the unsafe fork path."""

        with (
            mock.patch(
                "agent_run.launch.os.posix_spawn",
                side_effect=OSError("spawn failed"),
            ) as spawned,
            mock.patch("agent_run.launch.os.fork") as forked,
        ):
            with self.assertRaisesRegex(ValidationError, "cannot spawn"):
                self.launch({})
        self.assertTrue(spawned.call_args.kwargs["setsid"])
        forked.assert_not_called()

    def test_explicitly_unsupported_setsid_uses_legacy_fork_path(self) -> None:
        """Only an explicit unsupported capability result permits the fork fallback."""

        with (
            mock.patch(
                "agent_run.launch.os.posix_spawn", side_effect=NotImplementedError
            ),
            mock.patch("agent_run.launch.os.fork", return_value=1234) as forked,
        ):
            self.assertEqual(
                _spawn_session_leader(sys.executable, [sys.executable], 91), 1234
            )
        forked.assert_called_once_with()

    def test_child_dies_pre_identity_with_evidence_is_diagnosed(self) -> None:
        record = {
            "stage": "import",
            "type": "ModuleNotFoundError",
            "message": "no module named agent_run.adapters",
            "traceback": "Traceback (most recent call last):\n...\n",
        }
        with self.assertRaises(SupervisorBootstrapError) as caught:
            self.launch({"die_with_evidence_before_identity": record})
        error = caught.exception
        self.assertEqual(error.failure_kind, FAILURE_KIND_BOOTSTRAP)
        self.assertEqual(error.failure_stage, "import")
        self.assertEqual(error.bootstrap_error_type, "ModuleNotFoundError")
        self.assertIn("no module named agent_run.adapters", str(error))
        self.assertFalse(error.proven)
        self.assertIsNotNone(error.provisional_pid)
        with self.assertRaises(ChildProcessError):
            os.waitpid(error.provisional_pid, os.WNOHANG)

    def test_child_dies_pre_identity_without_evidence_is_diagnosed(self) -> None:
        with self.assertRaises(SupervisorBootstrapError) as caught:
            self.launch({"die_silent_before_identity": True})
        error = caught.exception
        self.assertEqual(error.failure_kind, FAILURE_KIND_BOOTSTRAP)
        self.assertIsNone(error.failure_stage)
        self.assertFalse(error.proven)
        self.assertIsNotNone(error.provisional_pid)
        self.assertIn("no bootstrap evidence", str(error))
        with self.assertRaises(ChildProcessError):
            os.waitpid(error.provisional_pid, os.WNOHANG)

    def test_write_exec_failure_writes_a_bounded_stage_and_errno_record(self) -> None:
        read_fd, write_fd = os.pipe()
        try:
            write_exec_failure(
                write_fd, FileNotFoundError(2, "No such file or directory")
            )
            os.close(write_fd)
            write_fd = -1
            blob = os.read(read_fd, 4096)
        finally:
            if write_fd != -1:
                os.close(write_fd)
            os.close(read_fd)
        record = json.loads(blob.splitlines()[0])
        self.assertEqual(record["stage"], "exec")
        self.assertEqual(record["type"], "FileNotFoundError")
        self.assertEqual(record["errno"], 2)

    @staticmethod
    def alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True
