import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.domain import AgentStatus, OrchestratorRef, Outcome, StartRequest
from agent_run.errors import ValidationError
from agent_run.state import StateStore


class StateOutboxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.store = StateStore.initialize(self.root / "state.db")

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def create(self):
        request = StartRequest("codex", "model", "profile", "task", self.root)
        return self.store.create_agent(
            request, task_summary="summary", config_revision="cfg-1", at=1
        )

    def finish(self, agent_id) -> None:
        self.store.transition(agent_id, AgentStatus.STARTING, at=2)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=3)
        self.store.transition(
            agent_id,
            AgentStatus.SUCCEEDED,
            outcome=Outcome(AgentStatus.SUCCEEDED),
            at=4,
        )

    def test_terminal_before_binding_activates_once_and_expired_lease_reclaims_once(self) -> None:
        agent_id = self.create()
        self.finish(agent_id)
        delivery = self.store.connection.execute(
            "SELECT * FROM deliveries WHERE agent_id = ?", (agent_id,)
        ).fetchone()
        self.assertEqual(delivery["state"], "waiting_binding")
        self.store.bind_orchestrator(
            agent_id, OrchestratorRef("codex_queue", "session", "turn"), at=5
        )

        first = self.store.claim_delivery("worker-1", at=5, lease_seconds=10)
        self.assertEqual(first["id"], delivery["id"])
        self.assertEqual(first["attempts"], 1)
        self.assertEqual(first["transport"], "codex_queue")
        self.assertIsNone(self.store.claim_delivery("worker-2", at=14))
        with self.assertRaises(ValidationError):
            self.store.complete_delivery(delivery["id"], "worker-1", at=15)
        reclaimed = self.store.claim_delivery("worker-2", at=15, lease_seconds=10)
        self.assertEqual(reclaimed["id"], delivery["id"])
        self.assertEqual(reclaimed["attempts"], 2)
        self.assertIsNone(self.store.claim_delivery("worker-3", at=15))
        with self.assertRaises(ValidationError):
            self.store.complete_delivery(delivery["id"], "worker-1", at=16)
        self.store.complete_delivery(
            delivery["id"], "worker-2", remote_message_id="remote-1", at=16
        )

        self.assertEqual(reclaimed["agent_status"], "succeeded")

    def test_retry_backoff_and_cancellation_preserve_terminal_result(self) -> None:
        agent_id = self.create()
        self.finish(agent_id)
        self.store.bind_orchestrator(
            agent_id, OrchestratorRef("codex_queue", "session", "turn"), at=5
        )
        delivery = self.store.claim_delivery("worker", at=5, lease_seconds=10)
        next_at = self.store.retry_delivery(
            delivery["id"], "worker", "ambiguous timeout", at=6, ambiguous_result=True
        )
        self.assertEqual(next_at, 7)
        self.assertIsNone(self.store.claim_delivery("worker", at=6.9))
        retried = self.store.claim_delivery("worker", at=7)
        self.assertEqual(retried["attempts"], 2)
        self.assertTrue(self.store.cancel_delivery(retried["id"]))
        self.assertEqual(self.store.get_agent(agent_id)["status"], "succeeded")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT ambiguous_result FROM deliveries WHERE id = ?", (retried["id"],)
            ).fetchone()[0],
            1,
        )
        self.assertIsNone(self.store.claim_delivery("other", at=1000))

    def test_failed_delivery_requires_live_owned_lease(self) -> None:
        agent_id = self.create()
        self.finish(agent_id)
        self.store.bind_orchestrator(
            agent_id, OrchestratorRef("codex_queue", "session", "turn"), at=5
        )
        delivery = self.store.claim_delivery("worker-1", at=5, lease_seconds=10)

        with self.assertRaisesRegex(ValidationError, "lease is not owned"):
            self.store.fail_delivery(delivery["id"], "worker-2", "wrong owner", at=6)
        with self.assertRaisesRegex(ValidationError, "lease is not owned"):
            self.store.fail_delivery(delivery["id"], "worker-1", "expired", at=15)
        unchanged = self.store.connection.execute(
            "SELECT state, last_error, ambiguous_result, lease_owner FROM deliveries WHERE id = ?",
            (delivery["id"],),
        ).fetchone()
        self.assertEqual(tuple(unchanged), ("sending", None, 0, "worker-1"))

        reclaimed = self.store.claim_delivery("worker-2", at=15, lease_seconds=10)
        self.store.fail_delivery(
            reclaimed["id"], "worker-2", "permanent", at=16, ambiguous_result=True
        )
        failed = self.store.connection.execute(
            """SELECT state, last_error, ambiguous_result, lease_owner, lease_until
               FROM deliveries WHERE id = ?""",
            (delivery["id"],),
        ).fetchone()
        self.assertEqual(tuple(failed), ("failed", "permanent", 1, None, None))

    def test_context_receipt_upserts_only_changed_keys(self) -> None:
        agent_id = self.create()
        session_id = self.store.bind_orchestrator(
            agent_id, OrchestratorRef("codex_queue", "session", "turn"), at=5
        )

        self.assertTrue(self.store.record_context_receipt(session_id, "first", at=6))
        self.assertFalse(self.store.record_context_receipt(session_id, "first", at=7))
        receipt = self.store.connection.execute(
            "SELECT context_key, injected_at FROM context_receipts WHERE orchestrator_session_id = ?",
            (session_id,),
        ).fetchone()
        self.assertEqual(tuple(receipt), ("first", 6))
        self.assertTrue(self.store.record_context_receipt(session_id, "second", at=8))
        receipt = self.store.connection.execute(
            "SELECT context_key, injected_at FROM context_receipts WHERE orchestrator_session_id = ?",
            (session_id,),
        ).fetchone()
        self.assertEqual(tuple(receipt), ("second", 8))
        with self.assertRaisesRegex(Exception, "FOREIGN KEY constraint failed"):
            self.store.record_context_receipt("unknown-session", "key", at=9)

    def test_reconciliation_requires_supplied_proof_before_persisting_lost(self) -> None:
        agent_id = self.create()
        with self.assertRaises(ValidationError):
            self.store.reconcile(agent_id, verdict="dead")
        self.store.record_supervisor(
            agent_id, pid=100, identity="pid100:start1", process_group_id=100, at=2
        )
        with self.assertRaises(ValidationError):
            self.store.record_supervisor(
                agent_id, pid=100, identity="pid100:start2", process_group_id=100, at=2
            )
        proof = {
            "supervisor_pid": 100,
            "process_group_id": 100,
            "expected_identity": "pid100:start1",
            "alive": True,
            "checked_at": 3,
        }
        self.assertFalse(self.store.reconcile(agent_id, verdict="alive", **proof))
        with self.assertRaises(ValidationError):
            self.store.reconcile(agent_id, verdict="dead", **proof)
        with self.assertRaises(ValidationError):
            self.store.reconcile(agent_id, verdict="alive", **(proof | {"checked_at": 1}))
        with self.assertRaises(ValidationError):
            self.store.reconcile(
                agent_id, verdict="alive", **(proof | {"supervisor_pid": 101})
            )
        with self.assertRaises(ValidationError):
            self.store.reconcile(
                agent_id,
                verdict="identity_mismatch",
                observed_identity="pid100:start1",
                **proof,
            )
        self.assertEqual(self.store.get_agent(agent_id)["status"], "created")
        self.assertTrue(
            self.store.reconcile(
                agent_id,
                verdict="identity_mismatch",
                observed_identity="pid100:start2",
                reason="PID reused",
                **proof,
            )
        )
        self.assertEqual(self.store.get_agent(agent_id)["status"], "lost")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT state FROM deliveries WHERE agent_id = ?", (agent_id,)
            ).fetchone()[0],
            "waiting_binding",
        )

        dead_agent = self.create()
        self.store.record_supervisor(
            dead_agent, pid=200, identity="pid200:start1", process_group_id=200, at=4
        )
        self.assertTrue(
            self.store.reconcile(
                dead_agent,
                verdict="dead",
                supervisor_pid=200,
                process_group_id=200,
                expected_identity="pid200:start1",
                alive=False,
                checked_at=5,
            )
        )

    def test_capacity_samples_return_only_recent_matching_rows(self) -> None:
        self.store.insert_capacity_sample(
            runtime="codex",
            lane="main",
            window="five-hour",
            source="runtime",
            remaining_percent=10,
            observed_at=1,
            valid_until=5,
            payload={"stale": True},
        )
        recent_id = self.store.insert_capacity_sample(
            runtime="codex",
            lane="main",
            window="five-hour",
            source="runtime",
            remaining_percent=80,
            observed_at=9,
            valid_until=20,
            payload={"stale": False},
        )
        rows = self.store.recent_capacity_samples(at=10, runtime="codex")
        self.assertEqual([row["id"] for row in rows], [recent_id])


if __name__ == "__main__":
    unittest.main()
