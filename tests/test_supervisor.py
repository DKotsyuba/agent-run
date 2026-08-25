import os
import signal
import sqlite3
import sys
import tempfile
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
from agent_run.domain import AgentStatus, Outcome, StartRequest
from agent_run.lifecycle import ReadyChannel, terminate_process_group
from agent_run.state.store import StateStore
from agent_run.supervisor import DEFAULT_WARNING_TEXT, Supervisor, SupervisorSettings
from agent_run.verify import ANSWER_INCOMPLETE, DEFAULT_SENTINEL, NO_ANSWER

ENGINE_PID = 4242
GRANDCHILD_PID = 4243


class FakeOps:
    """A fake process table plus a clock that only fake waits advance."""

    def __init__(self, *, members=(ENGINE_PID, GRANDCHILD_PID), ignores_term=frozenset()):
        self.groups = {ENGINE_PID: set(members)}
        self.ignores_term = ignores_term
        self.clock = 0.0
        self.sent: list[int] = []
        self.reaped: list[int] = []

    def monotonic(self) -> float:
        return self.clock

    def sleep(self, seconds: float) -> None:
        self.clock += max(0.0, seconds)

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
    ):
        self.pid = ENGINE_PID
        self._ops = ops
        self._outcome = outcome
        self._exit_after_polls = exit_after_polls
        self._native_cancel = native_cancel
        self._steer_error = steer_error
        self._on_wait = on_wait
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
            self._ops.groups[ENGINE_PID].discard(self.pid)
            return self._outcome
        return None

    def steer(self, text: str) -> None:
        if self._steer_error is not None:
            raise RuntimeError(self._steer_error)
        self.steers.append(text)

    def cancel(self, grace_seconds: float) -> None:
        self.cancels += 1
        if self._native_cancel:
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
            StartRequest("fake", "model", "profile", "task", self.root),
            task_summary="task",
            config_revision="rev-1",
        )

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
        self.assertEqual(ops.reaped, [ENGINE_PID])
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

    def test_a_surviving_group_is_stopped_before_lost_is_committed(self) -> None:
        ops = FakeOps(ignores_term=frozenset({GRANDCHILD_PID}))
        self.store.transition(self.agent_id, AgentStatus.STARTING)
        self.store.record_supervisor(
            self.agent_id, pid=os.getpid(), identity="agent-run supervisor", process_group_id=ENGINE_PID
        )
        self.store.transition(self.agent_id, AgentStatus.RUNNING)

        termination = terminate_process_group(
            ops, ENGINE_PID, grace_seconds=1.0, kill_grace_seconds=1.0, poll_seconds=0.5
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


if __name__ == "__main__":
    unittest.main()
