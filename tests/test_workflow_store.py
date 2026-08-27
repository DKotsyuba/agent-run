import json
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.errors import StateTransitionError, ValidationError
from agent_run.state import StateStore, step_key


class WorkflowStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.store = StateStore.initialize(self.root / "state.db")

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def running_run(self, *, name: str = "wf", script_sha: str = "sha-1", at: float = 1) -> str:
        run_id = self.store.create_workflow_run(name, script_sha, at=at)
        self.store.start_workflow_run(run_id)
        return run_id

    def test_step_key_is_a_deterministic_hash_of_spec_and_position(self) -> None:
        spec = {"kind": "shell", "cmd": "echo hi", "args": [1, 2]}
        # This exact-hexdigest pin has no execution sandbox available to derive
        # it here; the first real test run fails once and reports the actual
        # sha256 hexdigest to paste in -- that value is then permanent, since
        # resume identity depends on step_key never silently changing shape.
        self.assertEqual(
            step_key(spec, 3),
            "1663b4fe75ad09ef9379da2f2472289aea5d52fa26e7cba31dc1248fd78cbcd4",
        )
        reordered = {"args": [1, 2], "kind": "shell", "cmd": "echo hi"}
        self.assertEqual(step_key(spec, 3), step_key(reordered, 3))
        self.assertNotEqual(step_key(spec, 3), step_key(spec, 4))
        self.assertNotEqual(step_key(spec, 3), step_key({"kind": "shell"}, 3))
        with self.assertRaises(ValidationError):
            step_key(spec, -1)

    def test_create_and_lifecycle_transitions_are_enforced(self) -> None:
        with self.assertRaisesRegex(ValidationError, "nonblank"):
            self.store.create_workflow_run("", "sha-1")
        run_id = self.store.create_workflow_run(
            "wf", "sha-1", owner_identity="pid-1:start", at=1
        )
        self.assertTrue(run_id.startswith("wf_"))
        with self.assertRaisesRegex(ValidationError, "unknown workflow run"):
            self.store.start_workflow_run("wf_missing")
        self.store.start_workflow_run(run_id)
        with self.assertRaisesRegex(StateTransitionError, "cannot start from status: running"):
            self.store.start_workflow_run(run_id)
        with self.assertRaisesRegex(ValidationError, "must be one of"):
            self.store.finish_workflow_run(run_id, "running")
        self.store.finish_workflow_run(run_id, "succeeded", at=2)
        with self.assertRaisesRegex(StateTransitionError, "already finished"):
            self.store.finish_workflow_run(run_id, "failed", at=3)
        status = self.store.workflow_run_status(run_id)
        self.assertEqual(status["run"]["status"], "succeeded")
        self.assertEqual(status["run"]["finished_at"], 2)

    def test_finished_run_refuses_new_steps_and_unstarted_run_refuses_steps(self) -> None:
        run_id = self.store.create_workflow_run("wf", "sha-1", at=1)
        key = step_key({"op": "noop"}, 0)
        with self.assertRaisesRegex(StateTransitionError, "must be running"):
            self.store.record_step_start(run_id, key, {"op": "noop"})
        self.store.start_workflow_run(run_id)
        self.store.finish_workflow_run(run_id, "cancelled", at=2)
        with self.assertRaisesRegex(StateTransitionError, "must be running"):
            self.store.record_step_start(run_id, key, {"op": "noop"})

    def test_step_terminal_status_never_flips_back(self) -> None:
        run_id = self.running_run()
        key = step_key({"op": "noop"}, 0)
        self.store.record_step_start(run_id, key, {"op": "noop"})
        self.store.finish_step(run_id, key, "succeeded", result={"ok": True})
        with self.assertRaisesRegex(StateTransitionError, "already finished: succeeded"):
            self.store.record_step_start(run_id, key, {"op": "noop"})
        with self.assertRaisesRegex(StateTransitionError, "step is not running: succeeded"):
            self.store.finish_step(run_id, key, "failed", failure_kind="x")
        with self.assertRaisesRegex(ValidationError, "unknown workflow step"):
            self.store.finish_step(run_id, "missing-key", "succeeded")

    def test_failure_params_round_trip_and_status_validation(self) -> None:
        run_id = self.running_run()
        key = step_key({"op": "fail"}, 0)
        self.store.record_step_start(run_id, key, {"op": "fail"})
        with self.assertRaisesRegex(ValidationError, "nonblank"):
            self.store.finish_step(run_id, key, "failed")
        with self.assertRaisesRegex(ValidationError, "only valid when status is failed"):
            self.store.finish_step(run_id, key, "succeeded", failure_kind="boom")
        self.store.finish_step(
            run_id, key, "failed", failure_kind="timeout", failure_params={"seconds": 30}
        )
        status = self.store.workflow_run_status(run_id)
        step = status["steps"][0]
        self.assertEqual(step["status"], "failed")
        self.assertEqual(step["failure_kind"], "timeout")
        row = self.store.connection.execute(
            "SELECT failure_params_json FROM workflow_steps WHERE run_id = ? AND step_key = ?",
            (run_id, key),
        ).fetchone()
        self.assertEqual(json.loads(row["failure_params_json"]), {"seconds": 30})
        self.assertIsNone(self.store.cached_step_result(run_id, key))

    def test_cached_step_result_is_scoped_to_its_run(self) -> None:
        run_a = self.running_run(name="wf-a")
        run_b = self.running_run(name="wf-b")
        key = step_key({"op": "build"}, 0)
        self.assertIsNone(self.store.cached_step_result(run_a, key))
        self.store.record_step_start(run_a, key, {"op": "build"})
        self.store.finish_step(run_a, key, "succeeded", result={"artifact": "a.bin"})
        self.assertEqual(self.store.cached_step_result(run_a, key), {"artifact": "a.bin"})
        self.assertIsNone(self.store.cached_step_result(run_b, key))
        self.assertEqual(self.store.cached_step_result(run_a, key), {"artifact": "a.bin"})

    def test_list_workflow_runs_pagination_bounds_and_active_filter(self) -> None:
        ids = [self.store.create_workflow_run(f"wf-{i}", "sha-1", at=i) for i in range(1, 4)]
        self.store.start_workflow_run(ids[0])
        self.store.finish_workflow_run(ids[1], "failed", at=10)
        self.store.start_workflow_run(ids[2])

        all_runs = self.store.list_workflow_runs(limit=100)
        self.assertEqual([row["id"] for row in all_runs], list(reversed(ids)))

        active = self.store.list_workflow_runs(active_only=True)
        self.assertEqual({row["id"] for row in active}, {ids[0], ids[2]})

        first_page = self.store.list_workflow_runs(limit=1)
        self.assertEqual([row["id"] for row in first_page], [ids[2]])
        second_page = self.store.list_workflow_runs(limit=1, offset=1)
        self.assertEqual([row["id"] for row in second_page], [ids[1]])

        for bad in (0, -1, True):
            with self.assertRaisesRegex(ValidationError, "integer"):
                self.store.list_workflow_runs(limit=bad)
        for bad in (-1, True):
            with self.assertRaisesRegex(ValidationError, "integer"):
                self.store.list_workflow_runs(offset=bad)

    def test_workflow_run_status_reports_bounded_step_summaries(self) -> None:
        run_id = self.running_run()
        keys = [step_key({"i": i}, i) for i in range(3)]
        for i, key in enumerate(keys):
            self.store.record_step_start(run_id, key, {"i": i})
            self.store.finish_step(run_id, key, "succeeded", result={"i": i})

        status = self.store.workflow_run_status(run_id, step_limit=2)
        self.assertEqual(status["run"]["id"], run_id)
        self.assertEqual(len(status["steps"]), 2)
        self.assertEqual([s["step_key"] for s in status["steps"]], keys[:2])

        with self.assertRaisesRegex(ValidationError, "integer"):
            self.store.workflow_run_status(run_id, step_limit=0)
        with self.assertRaisesRegex(ValidationError, "unknown workflow run"):
            self.store.workflow_run_status("wf_missing")

    def test_concurrent_finish_step_is_serialized_and_exactly_one_wins(self) -> None:
        run_id = self.running_run()
        key = step_key({"op": "noop"}, 0)
        self.store.record_step_start(run_id, key, {"op": "noop"})
        database = self.root / "state.db"
        barrier = Barrier(2)

        def finish_once(label: str) -> bool:
            store = StateStore.open(database)
            try:
                barrier.wait()
                store.finish_step(run_id, key, "succeeded", result={"by": label})
                return True
            except StateTransitionError:
                return False
            finally:
                store.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(finish_once, label) for label in ("a", "b")]
            results = [future.result() for future in futures]

        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 1)
        stored = self.store.cached_step_result(run_id, key)
        self.assertIn(stored, ({"by": "a"}, {"by": "b"}))


if __name__ == "__main__":
    unittest.main()
