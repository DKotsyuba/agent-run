"""Resume: relaunch a detached runner for a run, replaying cached steps."""

import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from agent_run.errors import StateTransitionError, ValidationError
from agent_run.state import StateStore, step_key
from agent_run.workflow_run import plan_sha, resume_workflow, validate_plan
from agent_run.workflow_runner_main import execute_plan, runner_identity


class _CountingStore:
    """Wraps a StateStore and counts every step actually started through it."""

    def __init__(self, store: StateStore) -> None:
        self._store = store
        self.started = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._store, name)

    def record_step_start(self, *args: object, **kwargs: object) -> None:
        self.started += 1
        self._store.record_step_start(*args, **kwargs)


class ResumeExecutionTests(unittest.TestCase):
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

    def create_run(self, steps: tuple[dict[str, object], ...]) -> str:
        return self.store.create_workflow_run(
            "wf", plan_sha(steps), plan=[dict(step) for step in steps]
        )

    def test_resumed_run_replays_a_succeeded_step_and_executes_the_rest(self) -> None:
        steps = self.plan("first", "second")
        run_id = self.create_run(steps)

        # A first pass claims the run, finishes step 1, then dies before step 2
        # -- the run is left "failed" the way an unhandled crash would leave it.
        self.store.claim_workflow_run(run_id, "7 runner")
        self.store.record_step_start(run_id, step_key(steps[0], 0), steps[0])
        self.store.finish_step(
            run_id, step_key(steps[0], 0), "succeeded", result={"slept_seconds": 0.0}
        )
        self.store.finish_workflow_run(run_id, "failed")

        counting = _CountingStore(self.store)
        status = execute_plan(counting, run_id, steps, identity="8 runner", resume=True)

        self.assertEqual(status, "succeeded")
        # Only step 2 was actually started this pass -- step 1 was replayed.
        self.assertEqual(counting.started, 1)
        report = self.store.workflow_run_status(run_id)
        self.assertEqual(report["run"]["status"], "succeeded")
        self.assertEqual(report["run"]["owner_pid_identity"], "8 runner")
        self.assertEqual(
            [step["step_key"] for step in report["steps"]],
            [step_key(steps[0], 0), step_key(steps[1], 1)],
        )
        self.assertEqual({step["status"] for step in report["steps"]}, {"succeeded"})

    def test_a_fully_cached_resume_finishes_without_starting_any_step(self) -> None:
        steps = self.plan("only")
        run_id = self.create_run(steps)
        self.store.claim_workflow_run(run_id, "7 runner")
        self.store.record_step_start(run_id, step_key(steps[0], 0), steps[0])
        self.store.finish_step(
            run_id, step_key(steps[0], 0), "succeeded", result={"slept_seconds": 0.0}
        )
        self.store.finish_workflow_run(run_id, "cancelled")

        counting = _CountingStore(self.store)
        status = execute_plan(counting, run_id, steps, identity="9 runner", resume=True)

        self.assertEqual(status, "succeeded")
        self.assertEqual(counting.started, 0)


class ResumeValidationTests(unittest.TestCase):
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

    def create_run(self, steps: tuple[dict[str, object], ...]) -> str:
        return self.store.create_workflow_run(
            "wf", plan_sha(steps), plan=[dict(step) for step in steps]
        )

    def test_a_succeeded_run_refuses_to_resume(self) -> None:
        steps = self.plan("only")
        run_id = self.create_run(steps)
        execute_plan(self.store, run_id, steps, identity="7 runner")

        with self.assertRaisesRegex(ValidationError, "already succeeded"):
            resume_workflow(self.root, run_id)

    def test_a_run_with_a_live_owner_refuses_to_resume(self) -> None:
        steps = self.plan("only", seconds=30)
        run_id = self.create_run(steps)
        # This test process is alive under its own identity, so the run looks
        # owned by a live process to reconciliation.
        self.store.claim_workflow_run(run_id, runner_identity())

        with self.assertRaisesRegex(StateTransitionError, "live owner"):
            resume_workflow(self.root, run_id)


if __name__ == "__main__":
    unittest.main()
