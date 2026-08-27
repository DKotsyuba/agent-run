import json
import os
import signal
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from agent_run.errors import StateTransitionError, ValidationError
from agent_run.launch import launch_detached
from agent_run.paths import state_db_path
from agent_run.state import StateStore, step_key, workflow_owner_identity
from agent_run.workflow_run import plan_sha, resume_workflow, start_workflow, validate_plan
from agent_run.workflow_runner_main import execute_plan, runner_identity


# A pid that cannot be running this command: reconciliation must lose its run
# whether the pid is dead or has been reused by something else entirely.
DEAD_OWNER = workflow_owner_identity(999_999, "agent-run-runner-that-never-ran")


def child_pythonpath() -> str:
    """The exec'd runner gets a fresh interpreter, so it needs its own path."""

    parts = [str(REPO_ROOT), str(REPO_ROOT / "src")]
    existing = os.environ.get("PYTHONPATH")
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)


class _SignallingStore:
    """A store that raises one real SIGTERM as the nth step starts."""

    def __init__(self, store: StateStore, position: int) -> None:
        self._store = store
        self._position = position
        self._started = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._store, name)

    def record_step_start(self, *args: object, **kwargs: object) -> None:
        self._store.record_step_start(*args, **kwargs)
        self._started += 1
        if self._started == self._position:
            os.kill(os.getpid(), signal.SIGTERM)


class PlanValidationTests(unittest.TestCase):
    def test_plan_shape_is_refused_before_anything_durable_happens(self) -> None:
        with self.assertRaisesRegex(ValidationError, "sequence of steps"):
            validate_plan("sleep")
        with self.assertRaisesRegex(ValidationError, "1 to 100 steps"):
            validate_plan([])
        with self.assertRaisesRegex(ValidationError, "must be a mapping"):
            validate_plan([1])
        with self.assertRaisesRegex(ValidationError, "kind must be one of"):
            validate_plan([{"kind": "shell"}])
        with self.assertRaisesRegex(ValidationError, "seconds must be between"):
            validate_plan([{"kind": "sleep", "seconds": -1}])
        with self.assertRaisesRegex(ValidationError, "seconds must be between"):
            validate_plan([{"kind": "sleep", "seconds": True}])
        with self.assertRaisesRegex(ValidationError, "key_hint must be a string"):
            validate_plan([{"kind": "sleep", "seconds": 0, "key_hint": 1}])

    def test_normalized_steps_are_json_safe_and_hash_deterministically(self) -> None:
        steps = validate_plan([{"kind": "sleep", "seconds": 1}])
        self.assertEqual(steps, ({"kind": "sleep", "seconds": 1.0, "key_hint": ""},))
        self.assertEqual(
            plan_sha(steps), plan_sha(validate_plan([{"kind": "sleep", "seconds": 1.0}]))
        )
        self.assertNotEqual(
            plan_sha(steps),
            plan_sha(validate_plan([{"kind": "sleep", "seconds": 1, "key_hint": "b"}])),
        )


class RunnerLoopTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.store = StateStore.initialize(self.root / "state.db")

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def plan(self, *hints: str, seconds: float = 0) -> tuple[dict[str, object], ...]:
        return validate_plan(
            [{"kind": "sleep", "seconds": seconds, "key_hint": hint} for hint in hints]
        )

    def test_every_step_is_journalled_in_order_and_the_run_succeeds(self) -> None:
        steps = self.plan("first", "second")
        run_id = self.store.create_workflow_run("wf", plan_sha(steps))

        status = execute_plan(self.store, run_id, steps, identity="7 runner")

        self.assertEqual(status, "succeeded")
        report = self.store.workflow_run_status(run_id)
        self.assertEqual(report["run"]["status"], "succeeded")
        self.assertEqual(report["run"]["owner_pid_identity"], "7 runner")
        self.assertEqual(
            [step["step_key"] for step in report["steps"]],
            [step_key(steps[0], 0), step_key(steps[1], 1)],
        )
        self.assertEqual({step["status"] for step in report["steps"]}, {"succeeded"})
        self.assertEqual(
            self.store.cached_step_result(run_id, step_key(steps[0], 0)),
            {"slept_seconds": 0.0},
        )

    def test_ready_is_reported_only_after_ownership_is_durable(self) -> None:
        steps = self.plan("only")
        run_id = self.store.create_workflow_run("wf", plan_sha(steps))
        observed: list[dict[str, object]] = []
        store = self.store

        class _Ready:
            def ready(self) -> None:
                observed.append(dict(store.workflow_run_status(run_id)["run"]))

        self.assertEqual(
            execute_plan(store, run_id, steps, identity="7 runner", ready=_Ready()),
            "succeeded",
        )
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0]["status"], "running")
        self.assertEqual(observed[0]["owner_pid_identity"], "7 runner")

    def test_a_run_another_identity_owns_is_never_taken_over(self) -> None:
        steps = self.plan("only")
        run_id = self.store.create_workflow_run(
            "wf", plan_sha(steps), owner_identity=DEAD_OWNER
        )

        with self.assertRaisesRegex(StateTransitionError, "already owned"):
            execute_plan(self.store, run_id, steps, identity="7 runner")

        report = self.store.workflow_run_status(run_id)
        self.assertEqual(report["run"]["status"], "created")
        self.assertEqual(report["steps"], [])

    def test_a_stop_signal_fails_the_in_flight_step_and_cancels_the_run(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            self.skipTest("signal handlers are only installable on the main thread")
        steps = validate_plan(
            [
                {"kind": "sleep", "seconds": 0, "key_hint": "first"},
                {"kind": "sleep", "seconds": 30, "key_hint": "second"},
            ]
        )
        run_id = self.store.create_workflow_run("wf", plan_sha(steps))
        previous = signal.getsignal(signal.SIGTERM)
        started = time.monotonic()

        status = execute_plan(
            _SignallingStore(self.store, 2), run_id, steps, identity="7 runner"
        )

        self.assertEqual(status, "cancelled")
        self.assertLess(time.monotonic() - started, 30)
        self.assertIs(signal.getsignal(signal.SIGTERM), previous)
        report = self.store.workflow_run_status(run_id)
        self.assertEqual(report["run"]["status"], "cancelled")
        self.assertEqual(
            [step["status"] for step in report["steps"]], ["succeeded", "failed"]
        )
        self.assertEqual(report["steps"][1]["failure_kind"], "runner_cancelled")
        row = self.store.connection.execute(
            """SELECT failure_params_json FROM workflow_steps
               WHERE run_id = ? AND step_key = ?""",
            (run_id, step_key(steps[1], 1)),
        ).fetchone()
        self.assertEqual(json.loads(row["failure_params_json"]), {"signal": "SIGTERM"})


class WorkflowReconciliationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name).resolve() / "state.db"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_store_open_loses_only_runs_whose_owner_is_gone(self) -> None:
        store = StateStore.initialize(self.database)
        abandoned = store.create_workflow_run("wf", "sha-1", at=1)
        store.claim_workflow_run(abandoned, DEAD_OWNER)
        unclaimed = store.create_workflow_run("wf", "sha-1", at=2)
        mine = store.create_workflow_run("wf", "sha-1", at=3)
        store.claim_workflow_run(mine, runner_identity())
        store.close()

        reopened = StateStore.open(self.database)
        self.addCleanup(reopened.close)

        statuses = {run["id"]: run["status"] for run in reopened.list_workflow_runs()}
        self.assertEqual(statuses[abandoned], "lost")
        self.assertEqual(statuses[unclaimed], "created")
        self.assertEqual(statuses[mine], "running")


@unittest.skipUnless(hasattr(os, "fork") and hasattr(os, "setsid"), "POSIX only")
class DetachedWorkflowRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        patched = mock.patch.dict(os.environ, {"PYTHONPATH": child_pythonpath()})
        patched.start()
        self.addCleanup(patched.stop)
        StateStore.initialize(state_db_path(self.root)).close()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def wait_for(self, predicate, timeout: float = 20.0) -> object:
        deadline = time.monotonic() + timeout
        while True:
            value = predicate()
            if value:
                return value
            if time.monotonic() >= deadline:
                self.fail("timed out waiting for the detached workflow runner")
            time.sleep(0.02)

    def test_detached_runner_owns_journals_and_finishes_a_two_step_plan(self) -> None:
        plan = [
            {"kind": "sleep", "seconds": 0.05, "key_hint": "first"},
            {"kind": "sleep", "seconds": 0.05, "key_hint": "second"},
        ]
        launched: list[int] = []

        def spy(payload, **kwargs):
            pid = launch_detached(payload, **kwargs)
            launched.append(pid)
            return pid

        with mock.patch("agent_run.workflow_run.launch_detached", spy):
            run_id = start_workflow(
                self.root, "demo", plan, readiness_timeout_seconds=15.0
            )

        store = StateStore.open(state_db_path(self.root))
        self.addCleanup(store.close)
        # start_workflow returned, so READY was observed -- and READY is only
        # reported once the ownership claim is durable.
        claimed = store.workflow_run_status(run_id)["run"]
        self.assertIn(claimed["status"], {"running", "succeeded"})
        owner = str(claimed["owner_pid_identity"])
        self.assertEqual(int(owner.split(" ", 1)[0]), launched[0])
        self.assertIn("workflow_runner_main", owner)

        def finished() -> object:
            report = store.workflow_run_status(run_id)
            return report if report["run"]["status"] == "succeeded" else None

        report = self.wait_for(finished)
        steps = validate_plan(plan)
        self.assertEqual(
            [step["step_key"] for step in report["steps"]],
            [step_key(steps[0], 0), step_key(steps[1], 1)],
        )
        self.assertEqual({step["status"] for step in report["steps"]}, {"succeeded"})

        self.wait_for(lambda: not self.alive(launched[0]))
        self.assertEqual(
            store.workflow_run_status(run_id)["run"]["owner_pid_identity"], owner
        )

    def test_resume_reconciles_a_dead_owner_then_replays_and_finishes(self) -> None:
        plan = [
            {"kind": "sleep", "seconds": 0.05, "key_hint": "first"},
            {"kind": "sleep", "seconds": 0.05, "key_hint": "second"},
        ]
        steps = validate_plan(plan)

        # A prior runner claimed the run, finished step 1, then vanished
        # without a trace -- exactly what a dead owner_pid_identity looks like.
        store = StateStore.open(state_db_path(self.root))
        run_id = store.create_workflow_run(
            "demo", plan_sha(steps), plan=[dict(step) for step in steps]
        )
        store.claim_workflow_run(run_id, DEAD_OWNER)
        store.record_step_start(run_id, step_key(steps[0], 0), steps[0])
        store.finish_step(
            run_id, step_key(steps[0], 0), "succeeded", result={"slept_seconds": 0.05}
        )
        store.close()

        launched: list[int] = []

        def spy(payload, **kwargs):
            pid = launch_detached(payload, **kwargs)
            launched.append(pid)
            return pid

        with mock.patch("agent_run.workflow_run.launch_detached", spy):
            resume_workflow(self.root, run_id, readiness_timeout_seconds=15.0)

        store = StateStore.open(state_db_path(self.root))
        self.addCleanup(store.close)
        claimed = store.workflow_run_status(run_id)["run"]
        owner = str(claimed["owner_pid_identity"])
        self.assertEqual(int(owner.split(" ", 1)[0]), launched[0])
        self.assertNotEqual(owner, DEAD_OWNER)

        def finished() -> object:
            report = store.workflow_run_status(run_id)
            return report if report["run"]["status"] == "succeeded" else None

        report = self.wait_for(finished)
        self.assertEqual(
            [step["step_key"] for step in report["steps"]],
            [step_key(steps[0], 0), step_key(steps[1], 1)],
        )
        self.assertEqual({step["status"] for step in report["steps"]}, {"succeeded"})

    @staticmethod
    def alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True


if __name__ == "__main__":
    unittest.main()
