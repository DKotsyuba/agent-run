import os
import signal
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.errors import ValidationError
from agent_run.lifecycle import (
    Deadline,
    Phase,
    ReadyChannel,
    checked_pgid,
    install_signal_handlers,
    restore_signal_handlers,
    terminate_process_group,
    verify_process_group,
)


class FakeOps:
    """A process table where a group may hold a wrapper and its grandchild."""

    def __init__(
        self,
        groups: dict[int, set[int]],
        *,
        ignores_term: frozenset[int] = frozenset(),
        natural_exit_at: float | None = None,
    ):
        self.groups = {pgid: set(pids) for pgid, pids in groups.items()}
        self.ignores_term = ignores_term
        self.natural_exit_at = natural_exit_at
        self.clock = 0.0
        self.sent: list[tuple[int, int]] = []
        self.reaped: list[int] = []

    def monotonic(self) -> float:
        return self.clock

    def sleep(self, seconds: float) -> None:
        self.clock += max(0.0, seconds)
        if self.natural_exit_at is not None and self.clock >= self.natural_exit_at:
            for members in self.groups.values():
                members.clear()

    def process_group(self, pid: int) -> int | None:
        return next((pgid for pgid, members in self.groups.items() if pid in members), None)

    def signal_group(self, pgid: int, signal_number: int) -> bool:
        members = self.groups.get(checked_pgid(pgid))
        if not members:
            return False
        if signal_number == 0:
            return True
        self.sent.append((pgid, signal_number))
        if signal_number == signal.SIGTERM:
            for pid in list(members):
                if pid not in self.ignores_term:
                    members.discard(pid)
        elif signal_number == signal.SIGKILL:
            members.clear()
        return True

    def group_alive(self, pgid: int) -> bool:
        return bool(self.groups.get(checked_pgid(pgid)))

    def reap(self, pid: int) -> int | None:
        self.reaped.append(pid)
        return 0


class UnkillableOps(FakeOps):
    def signal_group(self, pgid: int, signal_number: int) -> bool:
        if signal_number != 0:
            self.sent.append((pgid, signal_number))
        return bool(self.groups.get(pgid))


class DeadlineTests(unittest.TestCase):
    def test_phases_cross_at_the_warning_fraction_and_the_hard_stop(self) -> None:
        deadline = Deadline(100.0, 200.0, 0.90)
        self.assertEqual(deadline.warning_at, 280.0)
        self.assertEqual(deadline.expires_at, 300.0)
        self.assertIs(deadline.phase(100.0), Phase.RUNNING)
        self.assertIs(deadline.phase(279.999), Phase.RUNNING)
        self.assertIs(deadline.phase(280.0), Phase.WARNING)
        self.assertIs(deadline.phase(299.999), Phase.WARNING)
        self.assertIs(deadline.phase(300.0), Phase.EXPIRED)
        self.assertIs(deadline.phase(1000.0), Phase.EXPIRED)

    def test_remaining_never_goes_negative(self) -> None:
        deadline = Deadline(0.0, 10.0)
        self.assertEqual(deadline.remaining(4.0), 6.0)
        self.assertEqual(deadline.remaining(99.0), 0.0)

    def test_invalid_budgets_are_refused(self) -> None:
        for kwargs in (
            {"started_at": 0.0, "timeout_seconds": 0.0},
            {"started_at": 0.0, "timeout_seconds": float("inf")},
            {"started_at": -1.0, "timeout_seconds": 5.0},
            {"started_at": 0.0, "timeout_seconds": 5.0, "warning_fraction": 0.0},
            {"started_at": 0.0, "timeout_seconds": 5.0, "warning_fraction": 1.5},
            {"started_at": 0.0, "timeout_seconds": True},
        ):
            with self.assertRaises(ValidationError):
                Deadline(**kwargs)


class TerminateProcessGroupTests(unittest.TestCase):
    def test_term_removes_wrapper_and_grandchild_together(self) -> None:
        ops = FakeOps({4242: {4242, 4243}})
        result = terminate_process_group(
            ops, verify_process_group(ops, 4242), grace_seconds=2.0, poll_seconds=0.5
        )
        self.assertEqual(result.signals, ("SIGTERM",))
        self.assertTrue(result.group_gone)
        self.assertEqual(ops.groups[4242], set())

    def test_a_grandchild_that_ignores_term_is_killed(self) -> None:
        ops = FakeOps({4242: {4242, 4243}}, ignores_term=frozenset({4243}))
        result = terminate_process_group(
            ops,
            verify_process_group(ops, 4242),
            grace_seconds=2.0,
            kill_grace_seconds=1.0,
            poll_seconds=0.5,
        )
        self.assertEqual(result.signals, ("SIGTERM", "SIGKILL"))
        self.assertTrue(result.group_gone)
        self.assertEqual(ops.groups[4242], set())
        self.assertEqual(
            [number for _, number in ops.sent], [signal.SIGTERM, signal.SIGKILL]
        )

    def test_an_already_dead_group_is_not_signalled(self) -> None:
        ops = FakeOps({4242: set()})
        result = terminate_process_group(
            ops, verify_process_group(ops, 4242), grace_seconds=1.0, poll_seconds=0.5
        )
        self.assertEqual(result.signals, ())
        self.assertTrue(result.group_gone)
        self.assertEqual(ops.sent, [])

    def test_a_surviving_group_is_reported_not_gone(self) -> None:
        ops = UnkillableOps({4242: {4242}})
        result = terminate_process_group(
            ops,
            verify_process_group(ops, 4242),
            grace_seconds=1.0,
            kill_grace_seconds=1.0,
            poll_seconds=0.5,
        )
        self.assertEqual(result.signals, ("SIGTERM", "SIGKILL"))
        self.assertFalse(result.group_gone)

    def test_dangerous_group_ids_are_refused(self) -> None:
        ops = FakeOps({4242: {4242}})
        for pgid in (0, 1, -1, True, "4242"):
            with self.assertRaises(ValidationError):
                verify_process_group(ops, pgid)
        with self.assertRaises(ValidationError):
            terminate_process_group(
                ops, verify_process_group(ops, 4242), grace_seconds=0
            )

    def test_foreign_nonleader_pid_is_never_signalled(self) -> None:
        ops = FakeOps({4242: {4242, 4243}})
        with self.assertRaisesRegex(ValidationError, "not its process group leader"):
            verify_process_group(ops, 4243)
        self.assertEqual(ops.sent, [])

    def test_natural_quiesce_reaps_before_signalling(self) -> None:
        ops = FakeOps({4242: {4242, 4243}}, natural_exit_at=0.5)
        result = terminate_process_group(
            ops,
            verify_process_group(ops, 4242),
            natural_grace_seconds=1.0,
            poll_seconds=0.25,
        )
        self.assertEqual(result.signals, ())
        self.assertTrue(result.group_gone)
        self.assertGreaterEqual(ops.reaped.count(4242), 2)


class SignalHandlerTests(unittest.TestCase):
    def test_handlers_are_installed_and_restored(self) -> None:
        received: list[int] = []
        previous = install_signal_handlers(lambda number: received.append(number))
        try:
            self.assertNotEqual(signal.getsignal(signal.SIGTERM), previous[signal.SIGTERM])
            os.kill(os.getpid(), signal.SIGTERM)
            self.assertEqual(received, [signal.SIGTERM])
        finally:
            restore_signal_handlers(previous)
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous[signal.SIGTERM])


class ReadyChannelTests(unittest.TestCase):
    def test_ready_token_is_reported_once(self) -> None:
        channel = ReadyChannel.open()
        self.addCleanup(channel.close_read)
        channel.ready()
        channel.ready()
        self.assertEqual(channel.wait(1.0), "ready")

    def test_failure_reason_is_raised_to_the_parent(self) -> None:
        channel = ReadyChannel.open()
        self.addCleanup(channel.close_read)
        channel.failed("home is not writable")
        with self.assertRaises(ValidationError) as caught:
            channel.wait(1.0)
        self.assertIn("home is not writable", str(caught.exception))

    def test_a_supervisor_that_dies_before_ready_is_detected(self) -> None:
        channel = ReadyChannel.open()
        self.addCleanup(channel.close_read)
        channel.close_write()
        with self.assertRaises(ValidationError) as caught:
            channel.wait(1.0)
        self.assertIn("exited before reporting ready", str(caught.exception))

    def test_a_silent_supervisor_times_out(self) -> None:
        channel = ReadyChannel.open()
        self.addCleanup(channel.close_read)
        self.addCleanup(channel.close_write)
        with self.assertRaises(ValidationError) as caught:
            channel.wait(0.05)
        self.assertIn("did not report ready", str(caught.exception))

    def test_blank_failure_reason_is_refused(self) -> None:
        channel = ReadyChannel.open()
        self.addCleanup(channel.close_read)
        self.addCleanup(channel.close_write)
        with self.assertRaises(ValidationError):
            channel.failed("  ")


if __name__ == "__main__":
    unittest.main()


class SystemGroupAliveTests(unittest.TestCase):
    """POSIX semantics of the real liveness probe."""

    def test_eperm_on_signal_zero_means_the_group_exists(self) -> None:
        # A foreign process that reused the pid answers EPERM to signal 0;
        # the probe must report "alive", not raise (a stale agent row must
        # never abort unrelated starts in the resident daemon).
        from unittest import mock

        from agent_run import lifecycle as lifecycle_module
        from agent_run.lifecycle import SystemProcessOps

        ops = SystemProcessOps()
        with mock.patch.object(lifecycle_module.os, "killpg", side_effect=PermissionError):
            self.assertTrue(ops.group_alive(4242))

    def test_esrch_on_signal_zero_means_gone(self) -> None:
        from unittest import mock

        from agent_run import lifecycle as lifecycle_module
        from agent_run.lifecycle import SystemProcessOps

        ops = SystemProcessOps()
        with mock.patch.object(lifecycle_module.os, "killpg", side_effect=ProcessLookupError):
            self.assertFalse(ops.group_alive(4242))
