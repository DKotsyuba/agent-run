import os
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LaunchPlan,
    RuntimeInfo,
)
from agent_run.domain import AgentStatus, Message, MessageRole, Outcome, StartRequest
from agent_run.lifecycle import ReadyChannel, terminate_process_group
from agent_run.state.store import StateStore
from agent_run.supervisor import (
    DEFAULT_WARNING_TEXT,
    StoreEventSink,
    Supervisor,
    SupervisorSettings,
    supervisor_identity,
)
from agent_run.verify import ANSWER_INCOMPLETE, DEFAULT_SENTINEL, NO_ANSWER

from agent_run.domain import TERMINAL
from agent_run.errors import StateTransitionError, ValidationError
from agent_run.lifecycle import verify_process_group
from agent_run.verify import GROUP_SURVIVED


ENGINE_PID = 4242
GRANDCHILD_PID = 4243


class FakeOps:
    """A fake process table plus a clock that only fake waits advance."""

    def __init__(
        self,
        *,
        members=(ENGINE_PID, GRANDCHILD_PID),
        ignores_term=frozenset(),
        natural_exit_at: float | None = None,
        on_sleep=None,
    ):
        self.groups = {ENGINE_PID: set(members)}
        self.ignores_term = ignores_term
        self.natural_exit_at = natural_exit_at
        self.on_sleep = on_sleep
        self.clock = 0.0
        self.sent: list[int] = []
        self.reaped: list[int] = []

    def monotonic(self) -> float:
        return self.clock

    def sleep(self, seconds: float) -> None:
        self.clock += max(0.0, seconds)
        if self.on_sleep is not None:
            self.on_sleep()
        if self.natural_exit_at is not None and self.clock >= self.natural_exit_at:
            self.groups[ENGINE_PID].clear()

    def process_group(self, pid: int) -> int | None:
        return next((pgid for pgid, members in self.groups.items() if pid in members), None)

    def signal_group(self, pgid: int, signal_number: int) -> bool:
        members = self.groups.get(pgid)
        if not members:
            return False
        if signal_number == 0:
            return True
        self.sent.append(signal_number)
        if signal_number == signal.SIGTERM:
            for pid in list(members):
                if pid not in self.ignores_term:
                    members.discard(pid)
        elif signal_number == signal.SIGKILL:
            members.clear()
        return True

    def group_alive(self, pgid: int) -> bool:
        return bool(self.groups.get(pgid))

    def reap(self, pid: int) -> int | None:
        self.reaped.append(pid)
        return 0

    def alive_members(self) -> set[int]:
        return set(self.groups[ENGINE_PID])


class UnkillableOps(FakeOps):
    def signal_group(self, pgid: int, signal_number: int) -> bool:
        if not self.groups.get(pgid):
            return False
        if signal_number != 0:
            self.sent.append(signal_number)
        return True


class FakeSession:
    """A fake engine whose wrapper and grandchild share one process group."""

    def __init__(
        self,
        ops: FakeOps,
        *,
        outcome: Outcome | None = None,
        exit_after_polls: int | None = None,
        native_cancel: bool = True,
        steer_error: str | None = None,
        on_wait=None,
        on_cancel=None,
        owns_process_group: bool = True,
        pid: int = ENGINE_PID,
    ):
        self.pid = pid
        self.owns_process_group = owns_process_group
        self._ops = ops
        self._outcome = outcome
        self._exit_after_polls = exit_after_polls
        self._native_cancel = native_cancel
        self._steer_error = steer_error
        self._on_wait = on_wait
        self._on_cancel = on_cancel
        self._exited = False
        self.polls = 0
        self.cancels = 0
        self.steers: list[str] = []

    def wait(self, timeout_seconds: float | None) -> Outcome | None:
        self._ops.clock += 0.0 if timeout_seconds is None else timeout_seconds
        self.polls += 1
        if self._on_wait is not None:
            self._on_wait(self.polls)
        if self._exited:
            return self._outcome
        if self._exit_after_polls is not None and self.polls >= self._exit_after_polls:
            self._exited = True
            if self.owns_process_group:
                self._ops.groups[ENGINE_PID].discard(self.pid)
            return self._outcome
        return None

    def steer(self, text: str) -> None:
        if self._steer_error is not None:
            raise RuntimeError(self._steer_error)
        self.steers.append(text)

    def cancel(self, grace_seconds: float) -> None:
        self.cancels += 1
        if self._on_cancel is not None:
            self._on_cancel()
        if self._native_cancel and self.owns_process_group:
            self._ops.groups[ENGINE_PID].discard(self.pid)


class FakeAdapter:
    def __init__(self, session: FakeSession | None, *, steerable: bool = True, error: str | None = None):
        self._session = session
        self._steerable = steerable
        self._error = error
        self.launches = 0
        self.on_launch = None

    def describe(self) -> RuntimeInfo:
        capabilities = frozenset({Capability.STEER}) if self._steerable else frozenset()
        return RuntimeInfo("fake", ADAPTER_API_VERSION, capabilities)

    def launch(self, plan: LaunchPlan, sink) -> FakeSession:
        self.launches += 1
        if self.on_launch is not None:
            self.on_launch()
        if self._error is not None:
            raise RuntimeError(self._error)
        assert self._session is not None
        return self._session


class CountingStore(StateStore):
    def __init__(self, connection: sqlite3.Connection):
        super().__init__(connection)
        self.heartbeats = 0
        self.fail_starting_once = False
        self.fail_running_once = False
        self.reject_terminal = False
        self.starting_error: Exception | None = None
        self.fail_event_kind: str | None = None

    def append_event(self, agent_id, kind, **kwargs):
        if kind == self.fail_event_kind:
            raise RuntimeError(f"{kind} write failed")
        return super().append_event(agent_id, kind, **kwargs)

    def transition(self, agent_id, target, **kwargs):
        if target is AgentStatus.STARTING and self.starting_error is not None:
            error = self.starting_error
            self.starting_error = None
            raise error
        if target is AgentStatus.STARTING and self.fail_starting_once:
            self.fail_starting_once = False
            raise RuntimeError("starting write failed")
        if target is AgentStatus.RUNNING and self.fail_running_once:
            self.fail_running_once = False
            raise RuntimeError("running write failed")
        if target in TERMINAL and self.reject_terminal:
            raise StateTransitionError("terminal write rejected")
        return super().transition(agent_id, target, **kwargs)

    def record_supervisor(self, agent_id, **kwargs) -> None:
        self.heartbeats += 1
        super().record_supervisor(agent_id, **kwargs)


class RecordingReady(ReadyChannel):
    def __init__(self, read_fd: int, write_fd: int, log: list):
        super().__init__(read_fd, write_fd)
        self.log = log
        self.observed_handler = None

    def ready(self) -> None:
        self.observed_handler = signal.getsignal(signal.SIGTERM)
        self.log.append("ready")
        super().ready()


class SupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.addCleanup(self.temporary.cleanup)
        self.store = CountingStore(StateStore.initialize(self.root / "state.db").connection)
        self.addCleanup(self.store.close)
        self.answer = self.root / "answer.md"
        self.agent_id = self.store.create_agent(
            StartRequest(
                "fake", "model", "profile", "task", self.root, timeout_seconds=480
            ),
            task_summary="task",
            config_revision="rev-1",
        ).agent_id

    def write_answer(self, text: str) -> None:
        self.answer.write_text(text, encoding="utf-8")

    def plan(self) -> LaunchPlan:
        return LaunchPlan((), self.root, {}, None, self.root / "runtime.jsonl", {})

    def supervisor(
        self,
        adapter: FakeAdapter,
        ops: FakeOps,
        *,
        timeout_seconds: float = 60.0,
        settings: SupervisorSettings | None = None,
        ready: ReadyChannel | None = None,
    ) -> Supervisor:
        return Supervisor(
            self.store,
            self.agent_id,
            adapter,
            self.plan(),
            answer_path=self.answer,
            timeout_seconds=timeout_seconds,
            settings=settings or SupervisorSettings(poll_seconds=0.5, grace_seconds=2.0),
            ops=ops,
            ready=ready,
        )

    def agent(self) -> dict:
        return self.store.get_agent(self.agent_id)

    def events(self, kind: str) -> list[sqlite3.Row]:
        return list(
            self.store.connection.execute(
                "SELECT * FROM events WHERE agent_id = ? AND kind = ? ORDER BY seq",
                (self.agent_id, kind),
            )
        )

    def test_ready_follows_handlers_and_durable_starting_and_precedes_launch(self) -> None:
        log: list[str] = []
        read_fd, write_fd = os.pipe()
        channel = RecordingReady(read_fd, write_fd, log)
        self.addCleanup(channel.close_read)
        statuses: list[str] = []
        ops = FakeOps()
        session = FakeSession(ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=1)
        adapter = FakeAdapter(session)
        adapter.on_launch = lambda: (log.append("launch"), statuses.append(str(self.agent()["status"])))
        before = signal.getsignal(signal.SIGTERM)
        self.write_answer(f"answer {DEFAULT_SENTINEL}")

        self.supervisor(adapter, ops, ready=channel).run()

        self.assertEqual(log, ["ready", "launch"])
        self.assertEqual(statuses, ["starting"])
        self.assertNotEqual(channel.observed_handler, before)
        self.assertNotIn(channel.observed_handler, (signal.SIG_DFL, signal.SIG_IGN))
        self.assertEqual(channel.wait(1.0), "ready")
        self.assertEqual(signal.getsignal(signal.SIGTERM), before)

    def ready_snapshot(self, seen: list) -> ReadyChannel:
        read_fd, write_fd = os.pipe()
        outer = self

        class SnapshotReady(ReadyChannel):
            def ready(self) -> None:
                seen.append(dict(outer.agent()))
                super().ready()

        channel = SnapshotReady(read_fd, write_fd)
        self.addCleanup(channel.close_read)
        return channel

    def test_identity_is_durable_before_ready_and_the_group_refines_once(self) -> None:
        seen: list[dict] = []
        channel = self.ready_snapshot(seen)
        ops = FakeOps()
        session = FakeSession(ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=1)
        self.write_answer(f"answer {DEFAULT_SENTINEL}")

        self.supervisor(FakeAdapter(session), ops, ready=channel).run()

        self.assertEqual(seen[0]["status"], "starting")
        self.assertEqual(seen[0]["supervisor_pid"], os.getpid())
        self.assertEqual(seen[0]["supervisor_identity"], supervisor_identity())
        self.assertEqual(seen[0]["process_group_id"], os.getpid())
        self.assertIsNotNone(seen[0]["heartbeat_at"])
        agent = self.agent()
        self.assertEqual(agent["supervisor_pid"], os.getpid())
        self.assertEqual(agent["supervisor_identity"], seen[0]["supervisor_identity"])
        self.assertEqual(agent["process_group_id"], ENGINE_PID)
        self.assertGreaterEqual(self.store.heartbeats, 2)

    def test_shared_service_row_keeps_the_supervisor_group(self) -> None:
        seen: list[dict] = []
        channel = self.ready_snapshot(seen)
        ops = FakeOps()
        session = FakeSession(
            ops,
            outcome=Outcome(AgentStatus.SUCCEEDED),
            exit_after_polls=1,
            owns_process_group=False,
            pid=9000,
        )
        self.write_answer(f"shared service done {DEFAULT_SENTINEL}")

        self.supervisor(FakeAdapter(session), ops, ready=channel).run()

        self.assertEqual(seen[0]["process_group_id"], os.getpid())
        self.assertEqual(self.agent()["process_group_id"], os.getpid())
        self.assertEqual(ops.sent, [])


    def test_cancel_queued_before_launch_cannot_orphan_the_engine(self) -> None:
        self.store.enqueue_command(self.agent_id, "cancel", {"reason": "user"})
        ops = FakeOps(ignores_term=frozenset({GRANDCHILD_PID}))
        session = FakeSession(ops, native_cancel=False)
        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.CANCELLED)
        self.assertEqual(self.agent()["status"], "cancelled")
        self.assertEqual(session.cancels, 1, "adapter-native cancel runs before signals")
        self.assertEqual(ops.sent, [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual(ops.alive_members(), set())
        command = self.store.connection.execute(
            "SELECT state, result_json FROM commands WHERE agent_id = ?", (self.agent_id,)
        ).fetchone()
        self.assertEqual(command["state"], "completed")
        self.assertIn('"accepted":true', command["result_json"])
        self.assertEqual(
            [row["from_status"] for row in self.events("cancelling")], ["running"]
        )

    def test_stuck_native_interrupt_cannot_block_group_enforcement(self) -> None:
        self.store.enqueue_command(self.agent_id, "cancel", {})
        ops = FakeOps()

        def stuck_interrupt() -> None:
            raise TimeoutError("interrupt timed out")

        session = FakeSession(
            ops, native_cancel=False, on_cancel=stuck_interrupt
        )
        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.CANCELLED)
        self.assertEqual(session.cancels, 1)
        self.assertEqual(ops.sent, [signal.SIGTERM])
        self.assertEqual(ops.alive_members(), set())

    def test_a_grandchild_is_killed_and_reaped_after_a_clean_exit(self) -> None:
        ops = FakeOps()
        session = FakeSession(
            ops,
            outcome=Outcome(AgentStatus.SUCCEEDED, exit_code=0, runtime_session_id="s-1"),
            exit_after_polls=2,
        )
        body = f"final answer\n{DEFAULT_SENTINEL}\n"
        self.write_answer(body)

        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(ops.sent, [signal.SIGTERM], "the surviving grandchild is signalled")
        self.assertEqual(ops.alive_members(), set())
        self.assertIn(ENGINE_PID, ops.reaped)
        agent = self.agent()
        self.assertEqual(agent["status"], "succeeded")
        self.assertEqual(agent["answer_bytes"], len(body.encode("utf-8")))
        self.assertEqual(agent["answer_path"], str(self.answer))
        self.assertEqual(self.events("process_group_terminated")[0]["kind"], "process_group_terminated")

    def test_one_warning_at_ninety_percent_then_a_hard_stop_at_one_hundred(self) -> None:
        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)
        settings = SupervisorSettings(
            poll_seconds=0.4, grace_seconds=1.0, heartbeat_seconds=1000.0
        )
        outcome = self.supervisor(
            FakeAdapter(session), ops, timeout_seconds=10.0, settings=settings
        ).run()

        self.assertEqual(session.steers, [DEFAULT_WARNING_TEXT])
        warnings = self.events("deadline_warning")
        self.assertEqual(len(warnings), 1)
        self.assertIn('"remaining_seconds":1.0', warnings[0]["data_json"])
        self.assertGreater(session.polls, 20, "the engine kept running through the band")
        self.assertIs(outcome.status, AgentStatus.TIMED_OUT)
        self.assertEqual(outcome.failure_kind, NO_ANSWER)
        self.assertEqual(outcome.failure_text, "silence=no_progress")
        self.assertEqual(self.agent()["status"], "timed_out")
        self.assertEqual(ops.alive_members(), set())

    def test_stalled_watchdog_kills_a_stream_silent_engine(self) -> None:
        """No stream events past the budget fails the run as stalled.

        The fake session never reports progress, so the baseline is the run
        start and the watchdog must fire long before the generous deadline.
        """

        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)
        settings = SupervisorSettings(
            poll_seconds=0.4,
            grace_seconds=1.0,
            heartbeat_seconds=1000.0,
            stalled_after_seconds=3.0,
        )
        outcome = self.supervisor(
            FakeAdapter(session), ops, timeout_seconds=1000.0, settings=settings
        ).run()

        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "stalled")
        self.assertIn("no stream events", outcome.failure_text)
        self.assertEqual(len(self.events("stalled")), 1)
        self.assertEqual(self.agent()["status"], "failed")
        self.assertEqual(ops.alive_members(), set())

    def test_stream_progress_defers_the_stall_until_the_deadline(self) -> None:
        """A live engine that keeps streaming is never killed as stalled."""

        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)
        settings = SupervisorSettings(
            poll_seconds=0.4,
            grace_seconds=1.0,
            heartbeat_seconds=1000.0,
            stalled_after_seconds=3.0,
        )
        supervisor = self.supervisor(
            FakeAdapter(session), ops, timeout_seconds=10.0, settings=settings
        )
        session._on_wait = lambda polls: setattr(
            supervisor._sink, "last_progress_at", ops.monotonic()
        )
        outcome = supervisor.run()

        self.assertIs(outcome.status, AgentStatus.TIMED_OUT)
        self.assertEqual(self.events("stalled"), [])

    def test_stalled_watchdog_disabled_at_zero(self) -> None:
        """stalled_after_seconds=0 disables the watchdog entirely."""

        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)
        settings = SupervisorSettings(
            poll_seconds=0.4,
            grace_seconds=1.0,
            heartbeat_seconds=1000.0,
            stalled_after_seconds=0.0,
        )
        outcome = self.supervisor(
            FakeAdapter(session), ops, timeout_seconds=10.0, settings=settings
        ).run()

        self.assertIs(outcome.status, AgentStatus.TIMED_OUT)
        self.assertEqual(self.events("stalled"), [])

    def test_deadline_expiry_wins_over_a_simultaneous_stall(self) -> None:
        """When both budgets elapse in one tick, the timeout outcome wins."""

        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)
        settings = SupervisorSettings(
            poll_seconds=0.4,
            grace_seconds=1.0,
            heartbeat_seconds=1000.0,
            stalled_after_seconds=10.0,
        )
        outcome = self.supervisor(
            FakeAdapter(session), ops, timeout_seconds=10.0, settings=settings
        ).run()

        self.assertIs(outcome.status, AgentStatus.TIMED_OUT)
        self.assertEqual(self.events("stalled"), [])

    def test_warning_text_demands_the_done_and_not_done_summary(self) -> None:
        """The 90% nudge must require the completion summary contract."""

        self.assertIn("COMPLETE", DEFAULT_WARNING_TEXT)
        self.assertIn("NOT done", DEFAULT_WARNING_TEXT)
        self.assertIn("continuation point", DEFAULT_WARNING_TEXT)

    def test_a_failing_stopping_bookkeeping_write_does_not_mask_the_timeout(self) -> None:
        """A durable-write hiccup on the "stopping" event must stay best-effort.

        stop_reason is already durable in memory once _stop() begins; losing
        the informational "stopping" record must not turn a real timed_out
        outcome into supervision_failed.
        """

        self.store.fail_event_kind = "stopping"
        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)
        settings = SupervisorSettings(
            poll_seconds=0.4, grace_seconds=1.0, heartbeat_seconds=1000.0
        )
        outcome = self.supervisor(
            FakeAdapter(session), ops, timeout_seconds=10.0, settings=settings
        ).run()

        self.assertIs(outcome.status, AgentStatus.TIMED_OUT)
        self.assertEqual(self.agent()["status"], "timed_out")
        self.assertEqual(self.events("stopping"), [])

    def test_a_failing_termination_bookkeeping_write_does_not_mask_the_timeout(self) -> None:
        """Same guarantee for the "process_group_terminated" write in _finish().

        _record_termination already supports ``best_effort`` (used by
        _fail_launched); _finish() must use it too so the group-kill record
        cannot turn a known outcome into supervision_failed.
        """

        self.store.fail_event_kind = "process_group_terminated"
        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)
        settings = SupervisorSettings(
            poll_seconds=0.4, grace_seconds=1.0, heartbeat_seconds=1000.0
        )
        outcome = self.supervisor(
            FakeAdapter(session), ops, timeout_seconds=10.0, settings=settings
        ).run()

        self.assertIs(outcome.status, AgentStatus.TIMED_OUT)
        self.assertEqual(self.agent()["status"], "timed_out")
        self.assertEqual(self.events("process_group_terminated"), [])

    def test_timeout_distinguishes_a_cut_off_answer_from_no_answer(self) -> None:
        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)
        self.write_answer("half of a thought, no sentinel")
        outcome = self.supervisor(
            FakeAdapter(session),
            ops,
            timeout_seconds=5.0,
            settings=SupervisorSettings(poll_seconds=1.0, grace_seconds=1.0),
        ).run()

        self.assertIs(outcome.status, AgentStatus.TIMED_OUT)
        self.assertEqual(outcome.failure_kind, ANSWER_INCOMPLETE)

    def test_a_warning_is_recorded_even_when_the_runtime_cannot_steer(self) -> None:
        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)
        outcome = self.supervisor(
            FakeAdapter(session, steerable=False),
            ops,
            timeout_seconds=5.0,
            settings=SupervisorSettings(poll_seconds=1.0, grace_seconds=1.0),
        ).run()

        self.assertEqual(session.steers, [])
        warnings = self.events("deadline_warning")
        self.assertEqual(len(warnings), 1)
        self.assertIn('"delivered":false', warnings[0]["data_json"])
        self.assertIs(outcome.status, AgentStatus.TIMED_OUT)

    def test_steer_commands_are_durably_answered_by_capability(self) -> None:
        self.store.enqueue_command(self.agent_id, "steer", {"text": "focus on tests"})
        ops = FakeOps()
        session = FakeSession(ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=2)
        self.write_answer(f"done {DEFAULT_SENTINEL}")
        self.supervisor(FakeAdapter(session), ops).run()
        self.assertEqual(session.steers, ["focus on tests"])

        results = list(
            self.store.connection.execute(
                "SELECT result_json FROM commands WHERE agent_id = ?", (self.agent_id,)
            )
        )
        self.assertIn('"accepted":true', results[0]["result_json"])

    def test_a_steer_without_the_capability_is_refused_not_dropped(self) -> None:
        self.store.enqueue_command(self.agent_id, "steer", {"text": "focus"})
        ops = FakeOps()
        session = FakeSession(ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=2)
        self.write_answer(f"done {DEFAULT_SENTINEL}")
        self.supervisor(FakeAdapter(session, steerable=False), ops).run()

        self.assertEqual(session.steers, [])
        row = self.store.connection.execute(
            "SELECT state, result_json FROM commands WHERE agent_id = ?", (self.agent_id,)
        ).fetchone()
        self.assertEqual(row["state"], "completed")
        self.assertIn("capability_unavailable", row["result_json"])

    def test_heartbeats_are_written_while_the_engine_runs(self) -> None:
        ops = FakeOps()
        samples: list[float] = []
        session = FakeSession(
            ops,
            outcome=Outcome(AgentStatus.SUCCEEDED),
            exit_after_polls=5,
            on_wait=lambda _poll: samples.append(float(self.agent()["heartbeat_at"])),
        )
        self.write_answer(f"done {DEFAULT_SENTINEL}")
        self.supervisor(
            FakeAdapter(session),
            ops,
            settings=SupervisorSettings(poll_seconds=1.0, heartbeat_seconds=1.0, grace_seconds=1.0),
        ).run()

        self.assertGreaterEqual(self.store.heartbeats, 4)
        self.assertEqual(samples, sorted(samples))
        self.assertEqual(self.agent()["supervisor_pid"], os.getpid())
        self.assertEqual(self.agent()["process_group_id"], ENGINE_PID)

    def test_a_failed_launch_is_durable_not_a_crash(self) -> None:
        ops = FakeOps()
        outcome = self.supervisor(FakeAdapter(None, error="engine missing"), ops).run()
        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "launch_failed")
        self.assertEqual(self.agent()["status"], "failed")
        self.assertEqual(self.agent()["failure_text"], "engine missing")

    def test_a_base_exception_during_launch_is_durable_and_re_raised(self) -> None:
        adapter = FakeAdapter(None)

        def interrupt() -> None:
            raise KeyboardInterrupt("supervisor killed")

        adapter.on_launch = interrupt
        with self.assertRaises(KeyboardInterrupt):
            self.supervisor(adapter, FakeOps()).run()
        self.assertEqual(self.agent()["status"], "failed")
        self.assertEqual(self.agent()["failure_kind"], "launch_failed")
        self.assertEqual(self.agent()["failure_text"], "supervisor killed")

    def test_a_base_exception_after_launch_is_durable_and_re_raised(self) -> None:
        ops = FakeOps()

        def interrupt(_polls: int) -> None:
            raise KeyboardInterrupt("supervisor killed mid-run")

        session = FakeSession(ops, on_wait=interrupt)
        with self.assertRaises(KeyboardInterrupt):
            self.supervisor(FakeAdapter(session), ops).run()
        self.assertEqual(self.agent()["status"], "failed")
        self.assertEqual(self.agent()["failure_kind"], "supervision_failed")

    def test_invalid_settings_are_refused_before_adapter_launch(self) -> None:
        adapter = FakeAdapter(FakeSession(FakeOps()))
        invalid = (
            {"heartbeat_seconds": 0},
            {"poll_seconds": float("nan")},
            {"grace_seconds": 0},
            {"kill_grace_seconds": float("inf")},
            {"natural_grace_seconds": -1},
            {"warning_fraction": 0},
            {"warning_fraction": 1},
            {"warning_fraction": float("nan")},
            {"silence_threshold_seconds": -1},
            {"warning_text": "  "},
            {"sentinel": ""},
        )
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(ValidationError):
                settings = SupervisorSettings(**values)
                self.supervisor(adapter, FakeOps(), settings=settings).run()
        self.assertEqual(adapter.launches, 0)

    def test_natural_quiesce_allows_answer_flush_without_term(self) -> None:
        body = f"flushed answer\n{DEFAULT_SENTINEL}\n"
        ops = FakeOps(
            natural_exit_at=0.5,
            on_sleep=lambda: self.write_answer(body),
        )
        session = FakeSession(
            ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=1
        )

        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(ops.sent, [])
        self.assertEqual(outcome.answer_bytes, len(body.encode()))

    def test_timeout_zero_after_launch_is_cleaned_and_failed(self) -> None:
        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)

        outcome = self.supervisor(
            FakeAdapter(session), ops, timeout_seconds=0
        ).run()

        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "supervision_failed")
        self.assertEqual(session.cancels, 1)
        self.assertEqual(ops.alive_members(), set())

    def test_non_group_leader_is_native_cancelled_but_never_group_signalled(self) -> None:
        ops = FakeOps()
        session = FakeSession(ops)
        session.pid = GRANDCHILD_PID

        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(session.cancels, 1)
        self.assertEqual(ops.sent, [])
        self.assertIn(GRANDCHILD_PID, ops.reaped)

    def test_surviving_nonleader_never_claims_group_gone(self) -> None:
        ops = FakeOps()
        session = FakeSession(
            ops, native_cancel=False, pid=GRANDCHILD_PID
        )

        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "engine_group_survived")
        self.assertEqual(ops.sent, [])
        self.assertIn(GRANDCHILD_PID, ops.alive_members())

    def test_shared_service_session_is_never_signalled_or_reaped(self) -> None:
        self.write_answer(f"shared service done\n{DEFAULT_SENTINEL}\n")
        ops = FakeOps()
        session = FakeSession(
            ops,
            outcome=Outcome(AgentStatus.SUCCEEDED),
            exit_after_polls=1,
            owns_process_group=False,
            pid=9000,
        )

        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(ops.sent, [])
        self.assertEqual(ops.reaped, [])

    def test_session_wait_exception_reaches_cleanup_and_durable_failure(self) -> None:
        ops = FakeOps()

        def fail_wait(_poll):
            raise RuntimeError("wait failed")

        session = FakeSession(ops, native_cancel=False, on_wait=fail_wait)
        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(self.agent()["status"], "failed")
        self.assertEqual(session.cancels, 1)
        self.assertEqual(ops.alive_members(), set())

    def test_running_transition_exception_reaches_cleanup_and_failure(self) -> None:
        self.store.fail_running_once = True
        ops = FakeOps()
        session = FakeSession(ops, native_cancel=False)

        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(self.agent()["status"], "failed")
        self.assertEqual(session.cancels, 1)
        self.assertEqual(ops.alive_members(), set())

    def test_unkillable_cancel_is_failed_not_coerced_to_cancelled(self) -> None:
        self.store.enqueue_command(self.agent_id, "cancel", {})
        ops = UnkillableOps()
        session = FakeSession(ops, native_cancel=False)

        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, GROUP_SURVIVED)
        self.assertEqual(self.agent()["status"], "failed")

    def test_final_drain_completes_late_cancel_steer_and_unknown(self) -> None:
        self.store.enqueue_command(self.agent_id, "cancel", {})

        def enqueue_late_commands():
            self.store.enqueue_command(self.agent_id, "cancel", {})
            self.store.enqueue_command(self.agent_id, "steer", {"text": "too late"})
            self.store.enqueue_command(self.agent_id, "other", {})

        ops = FakeOps()
        session = FakeSession(
            ops, native_cancel=False, on_cancel=enqueue_late_commands
        )
        self.supervisor(FakeAdapter(session), ops).run()

        rows = list(
            self.store.connection.execute(
                "SELECT kind, state, result_json FROM commands WHERE agent_id = ? ORDER BY id",
                (self.agent_id,),
            )
        )
        self.assertTrue(all(row["state"] == "completed" for row in rows))
        self.assertIn("already_stopping", rows[1]["result_json"])
        self.assertIn("agent_terminal", rows[2]["result_json"])
        self.assertIn("agent_terminal", rows[3]["result_json"])

    def test_startup_failure_reports_ready_failure_without_launch(self) -> None:
        self.store.fail_starting_once = True
        channel = ReadyChannel.open()
        self.addCleanup(channel.close_read)
        adapter = FakeAdapter(FakeSession(FakeOps()))

        with self.assertRaisesRegex(RuntimeError, "starting write failed"):
            self.supervisor(adapter, FakeOps(), ready=channel).run()
        with self.assertRaisesRegex(ValidationError, "supervisor failed to start"):
            channel.wait(1.0)
        self.assertEqual(adapter.launches, 0)

    def test_ready_accepts_prestarted_and_cancelling_rows(self) -> None:
        """Async admission must not trigger a STARTING-to-STARTING failure."""

        ops = FakeOps()
        supervisor = self.supervisor(FakeAdapter(FakeSession(ops)), ops)
        self.store.transition(self.agent_id, AgentStatus.STARTING)

        supervisor._report_ready()
        started = self.store.get_agent(self.agent_id)
        self.assertEqual(started["status"], AgentStatus.STARTING.value)
        self.assertIsNotNone(started["supervisor_pid"])
        self.assertIsNotNone(started["supervisor_identity"])

        self.store.transition(self.agent_id, AgentStatus.RUNNING)
        self.store.transition(self.agent_id, AgentStatus.CANCELLING)
        supervisor._report_ready()
        cancelling = self.store.get_agent(self.agent_id)
        self.assertEqual(cancelling["status"], AgentStatus.CANCELLING.value)
        self.assertIsNotNone(cancelling["process_group_id"])

    def test_blank_startup_error_uses_exception_type_in_ready_failure(self) -> None:
        self.store.starting_error = RuntimeError()
        channel = ReadyChannel.open()
        self.addCleanup(channel.close_read)
        adapter = FakeAdapter(FakeSession(FakeOps()))

        with self.assertRaises(RuntimeError):
            self.supervisor(adapter, FakeOps(), ready=channel).run()
        with self.assertRaisesRegex(ValidationError, "RuntimeError"):
            channel.wait(1.0)
        self.assertEqual(adapter.launches, 0)

    def test_answer_inspection_failure_after_cleanup_is_durable(self) -> None:
        self.answer.mkdir()
        ops = FakeOps()
        session = FakeSession(
            ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=1
        )

        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "answer_inspection_failed")
        self.assertEqual(self.agent()["status"], "failed")
        self.assertEqual(self.agent()["failure_kind"], "answer_inspection_failed")
        self.assertEqual(ops.alive_members(), set())

    def test_commit_rejection_is_not_swallowed_while_agent_is_active(self) -> None:
        self.store.reject_terminal = True
        ops = FakeOps()
        session = FakeSession(
            ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=1
        )
        self.write_answer(f"done {DEFAULT_SENTINEL}")

        with self.assertRaisesRegex(StateTransitionError, "terminal write rejected"):
            self.supervisor(FakeAdapter(session), ops).run()
        self.assertEqual(ops.alive_members(), set())
        self.assertNotIn(self.agent()["status"], {"succeeded", "cancelled"})

    def test_a_surviving_group_is_stopped_before_lost_is_committed(self) -> None:
        ops = FakeOps(ignores_term=frozenset({GRANDCHILD_PID}))
        self.store.transition(self.agent_id, AgentStatus.STARTING)
        self.store.record_supervisor(
            self.agent_id, pid=os.getpid(), identity="agent-run supervisor", process_group_id=ENGINE_PID
        )
        self.store.transition(self.agent_id, AgentStatus.RUNNING)

        termination = terminate_process_group(
            ops,
            verify_process_group(ops, ENGINE_PID),
            grace_seconds=1.0,
            kill_grace_seconds=1.0,
            poll_seconds=0.5,
        )
        self.assertTrue(termination.group_gone)
        self.assertEqual(ops.alive_members(), set(), "engine group is gone before lost")

        committed = self.store.reconcile(
            self.agent_id,
            verdict="dead",
            supervisor_pid=os.getpid(),
            process_group_id=ENGINE_PID,
            expected_identity="agent-run supervisor",
            alive=False,
            checked_at=time.time() + 1,
            reason="supervisor process vanished",
        )
        self.assertTrue(committed)
        agent = self.agent()
        self.assertEqual(agent["status"], "lost")
        self.assertEqual(agent["failure_kind"], "supervisor_dead")


class StoreEventSinkThreadingTests(unittest.TestCase):
    """A detached supervisor's engine adapter may call the sink from a
    background stream-reader thread (Claude's adapter does). The sink's
    connection was created on the supervisor's own thread, and sqlite3
    connections are thread-affine, so a naive shared connection raises
    ``sqlite3.ProgrammingError`` the first time a non-owner thread writes."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.addCleanup(self.temporary.cleanup)
        self.store = StateStore.initialize(self.root / "state.db")
        self.addCleanup(self.store.close)
        self.agent_id = self.store.create_agent(
            StartRequest(
                "fake", "model", "profile", "task", self.root, timeout_seconds=480
            ),
            task_summary="task",
            config_revision="rev-1",
        ).agent_id

    def events(self, kind: str) -> list[sqlite3.Row]:
        return list(
            self.store.connection.execute(
                "SELECT * FROM events WHERE agent_id = ? AND kind = ? ORDER BY seq",
                (self.agent_id, kind),
            )
        )

    def run_in_thread(self, target) -> threading.Thread:
        worker = threading.Thread(target=target)
        worker.start()
        worker.join(timeout=5)
        self.assertFalse(worker.is_alive(), "worker thread did not finish in time")
        return worker

    def test_sink_write_from_a_non_owner_thread_lands_durably(self) -> None:
        sink = StoreEventSink(self.store, self.agent_id, FakeOps())
        errors: list[BaseException] = []

        def call_from_worker() -> None:
            try:
                sink.event("worker_event", {"from": "worker"})
            except BaseException as error:  # captured, not swallowed by the thread
                errors.append(error)

        self.run_in_thread(call_from_worker)

        self.assertEqual(errors, [])
        rows = self.events("worker_event")
        self.assertEqual(len(rows), 1)

    def test_sink_message_and_session_from_a_non_owner_thread_land_durably(self) -> None:
        sink = StoreEventSink(self.store, self.agent_id, FakeOps())
        message = Message(at=1.0, role=MessageRole.ASSISTANT, content="hello")
        errors: list[BaseException] = []

        def call_from_worker() -> None:
            try:
                sink.session("runtime-session-1")
                sink.message(message)
            except BaseException as error:
                errors.append(error)

        self.run_in_thread(call_from_worker)

        self.assertEqual(errors, [])
        self.assertEqual(sink.runtime_session_id, "runtime-session-1")
        self.assertEqual(len(self.events("runtime_session")), 1)
        transcript = self.store.transcript(self.agent_id)
        self.assertEqual([row["content"] for row in transcript], ["hello"])

    def test_concurrent_sink_writes_from_two_threads_all_land_without_error(self) -> None:
        sink = StoreEventSink(self.store, self.agent_id, FakeOps())
        errors: list[BaseException] = []
        barrier = threading.Barrier(2)

        def call_from_worker(index: int) -> None:
            try:
                barrier.wait(timeout=5)
                sink.event(f"concurrent_event_{index}", {"index": index})
            except BaseException as error:
                errors.append(error)

        workers = [
            threading.Thread(target=call_from_worker, args=(index,)) for index in range(2)
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=5)

        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertEqual(errors, [])
        self.assertEqual(len(self.events("concurrent_event_0")), 1)
        self.assertEqual(len(self.events("concurrent_event_1")), 1)


class RunStatsSupervisorTests(unittest.TestCase):
    """The supervisor's terminal path snapshots one run_stats row, best-effort."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.addCleanup(self.temporary.cleanup)
        self.store = CountingStore(StateStore.initialize(self.root / "state.db").connection)
        self.addCleanup(self.store.close)
        self.answer = self.root / "answer.md"
        self.agent_id = self.store.create_agent(
            StartRequest(
                "fake", "model", "profile", "task", self.root, timeout_seconds=480
            ),
            task_summary="task",
            config_revision="rev-1",
        ).agent_id

    def write_answer(self, text: str) -> None:
        self.answer.write_text(text, encoding="utf-8")

    def plan(self) -> LaunchPlan:
        return LaunchPlan((), self.root, {}, None, self.root / "runtime.jsonl", {})

    def supervisor(self, adapter, ops) -> Supervisor:
        return Supervisor(
            self.store,
            self.agent_id,
            adapter,
            self.plan(),
            answer_path=self.answer,
            timeout_seconds=60.0,
            settings=SupervisorSettings(poll_seconds=0.5, grace_seconds=2.0),
            ops=ops,
        )

    def stats_row(self) -> dict | None:
        row = self.store.connection.execute(
            "SELECT * FROM run_stats WHERE agent_id = ?", (self.agent_id,)
        ).fetchone()
        return None if row is None else dict(row)

    def test_a_terminal_commit_writes_the_run_stats_row(self) -> None:
        self.write_answer(f"done {DEFAULT_SENTINEL}")
        ops = FakeOps()
        session = FakeSession(
            ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=1
        )

        outcome = self.supervisor(FakeAdapter(session), ops).run()

        self.assertIs(outcome.status, AgentStatus.SUCCEEDED)
        row = self.stats_row()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "succeeded")
        self.assertEqual(row["runtime"], "fake")
        self.assertEqual(row["usage_source"], "none")
        self.assertIsNotNone(row["started_at"])
        self.assertIsNotNone(row["finished_at"])
        self.assertIsNotNone(row["recorded_at"])

    def test_a_stats_failure_still_returns_the_committed_outcome(self) -> None:
        import agent_run.supervisor as supervisor_module

        self.write_answer(f"done {DEFAULT_SENTINEL}")
        ops = FakeOps()
        session = FakeSession(
            ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=1
        )
        original = supervisor_module.record_run_stats_best_effort

        def boom(store, agent_id, *, at=None):
            raise RuntimeError("stats broke")

        supervisor_module.record_run_stats_best_effort = boom
        try:
            outcome = self.supervisor(FakeAdapter(session), ops).run()
        finally:
            supervisor_module.record_run_stats_best_effort = original

        self.assertIs(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(str(self.store.get_agent(self.agent_id)["status"]), "succeeded")
        self.assertIsNone(self.stats_row())


if __name__ == "__main__":
    unittest.main()
