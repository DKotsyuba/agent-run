import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.domain import AgentStatus, OrchestratorRef, Outcome, StartRequest
from agent_run.errors import ValidationError
from agent_run.state import (
    StateStore,
    reconcile_active_agents,
    reconcile_reaped_agent,
    reconcile_reaped_supervisor,
)


class StateOutboxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.store = StateStore.initialize(self.root / "state.db")

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def create(self, at: float = 1):
        request = StartRequest(
            "codex", "model", "profile", "task", self.root, timeout_seconds=480
        )
        return self.store.create_agent(
            request, task_summary="summary", config_revision="cfg-1", at=at
        ).agent_id

    def supervised(self, pid: int, identity: str, *, pgid=None, created_at=1, heartbeat_at=6):
        """One running row with a complete supervisor identity to sweep."""

        agent_id = self.create(at=created_at)
        self.store.transition(agent_id, AgentStatus.STARTING, at=5)
        self.store.record_supervisor(
            agent_id,
            pid=pid,
            identity=identity,
            process_group_id=pid if pgid is None else pgid,
            at=heartbeat_at,
        )
        self.store.transition(agent_id, AgentStatus.RUNNING, at=7)
        return agent_id

    def probing(self, replies):
        """Answer the sweep from a fake process table, recording every probe."""

        probed: list = []

        def probe(pid, pgid):
            probed.append((pid, pgid))
            reply = replies(pid) if callable(replies) else replies[pid]
            return reply

        return mock.patch("agent_run.doctor._probe_process", probe), probed

    def forbid_signals(self):
        def refuse(*_args, **_kwargs):
            raise AssertionError("the sweep must never signal a recorded group")

        return mock.patch.multiple("os", kill=refuse, killpg=refuse)

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

    def test_context_receipt_for_ref_creates_and_reuses_session(self) -> None:
        ref = OrchestratorRef("codex_queue", "bookkeeping", "turn-1")
        session_id, changed = self.store.record_context_receipt_for_ref(
            ref, "first", at=5
        )
        self.assertTrue(changed)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM orchestrator_sessions"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            tuple(
                self.store.connection.execute(
                    """SELECT context_key, injected_at FROM context_receipts
                       WHERE orchestrator_session_id = ?""",
                    (session_id,),
                ).fetchone()
            ),
            ("first", 5),
        )

        repeated_id, changed = self.store.record_context_receipt_for_ref(
            OrchestratorRef("codex_queue", "bookkeeping"), "first", at=6
        )
        self.assertEqual(repeated_id, session_id)
        self.assertFalse(changed)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT injected_at FROM context_receipts WHERE orchestrator_session_id = ?",
                (session_id,),
            ).fetchone()[0],
            5,
        )
        self.assertEqual(
            self.store.record_context_receipt_for_ref(ref, "second", at=7),
            (session_id, True),
        )

        agent_id = self.create()
        self.assertEqual(
            self.store.bind_orchestrator(
                agent_id, OrchestratorRef("codex_queue", "bookkeeping", "turn-2"), at=8
            ),
            session_id,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM orchestrator_sessions"
            ).fetchone()[0],
            1,
        )

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

    def test_reaped_supervisor_reconciles_only_its_active_rows(self) -> None:
        dead = self.create()
        other = self.create()
        terminal = self.create()
        for agent_id, pid in ((dead, 100), (other, 200), (terminal, 100)):
            self.store.transition(agent_id, AgentStatus.STARTING, at=2)
            self.store.record_supervisor(
                agent_id,
                pid=pid,
                identity=f"pid-{pid}",
                process_group_id=pid,
                at=3,
            )
            self.store.transition(agent_id, AgentStatus.RUNNING, at=4)
        self.store.transition(
            terminal,
            AgentStatus.FAILED,
            outcome=Outcome(AgentStatus.FAILED),
            at=5,
        )

        self.assertEqual(
            reconcile_reaped_supervisor(self.store, 100, at=6), (dead,)
        )
        self.assertEqual(self.store.get_agent(dead)["status"], "lost")
        self.assertEqual(self.store.get_agent(other)["status"], "running")
        self.assertEqual(self.store.get_agent(terminal)["status"], "failed")
        self.assertEqual(reconcile_reaped_supervisor(self.store, 100, at=7), ())

    def test_reaped_agent_closes_the_pre_identity_starting_window(self) -> None:
        agent_id = self.create()
        self.store.transition(agent_id, AgentStatus.STARTING, at=2)

        self.assertTrue(reconcile_reaped_agent(self.store, agent_id, 321, at=3))
        self.assertEqual(self.store.get_agent(agent_id)["status"], "lost")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT state FROM deliveries WHERE agent_id = ?", (agent_id,)
            ).fetchone()[0],
            "waiting_binding",
        )
        self.assertFalse(reconcile_reaped_agent(self.store, agent_id, 321, at=4))

        other = self.create()
        self.store.transition(other, AgentStatus.STARTING, at=5)
        self.store.record_supervisor(
            other, pid=999, identity="pid-999", process_group_id=999, at=6
        )
        self.assertFalse(reconcile_reaped_agent(self.store, other, 321, at=7))
        self.assertEqual(self.store.get_agent(other)["status"], "starting")

    def test_supervisor_group_refines_once_from_the_supervisors_own_group(self) -> None:
        agent_id = self.supervised(100, "pid-100", created_at=1)

        self.store.record_supervisor(
            agent_id, pid=100, identity="pid-100", process_group_id=4242, at=8
        )
        self.assertEqual(self.store.get_agent(agent_id)["process_group_id"], 4242)

        for pid, identity, pgid in (
            (100, "pid-100", 4243),
            (100, "pid-100", 100),
            (101, "pid-100", 4242),
            (100, "other", 4242),
        ):
            with self.assertRaisesRegex(ValidationError, "immutable"):
                self.store.record_supervisor(
                    agent_id, pid=pid, identity=identity, process_group_id=pgid, at=9
                )
        agent = self.store.get_agent(agent_id)
        self.assertEqual(
            (agent["supervisor_pid"], agent["supervisor_identity"], agent["process_group_id"]),
            (100, "pid-100", 4242),
        )

    def test_sweep_closes_dead_supervisors_with_live_groups_without_signalling(self) -> None:
        surviving = self.supervised(100, "pid-100", pgid=4242, created_at=1)
        foreign = self.supervised(101, "pid-101", pgid=1, created_at=2)
        terminal = self.supervised(102, "pid-102", created_at=3)
        self.store.transition(
            terminal, AgentStatus.FAILED, outcome=Outcome(AgentStatus.FAILED), at=8
        )
        patch, probed = self.probing(lambda pid: (False, None, True))

        with patch, self.forbid_signals():
            self.assertEqual(
                reconcile_active_agents(self.store, at=10), (surviving, foreign)
            )

        self.assertEqual(probed, [(100, 4242), (101, 1)])
        for agent_id in (surviving, foreign):
            agent = self.store.get_agent(agent_id)
            self.assertEqual(agent["status"], "lost")
            self.assertEqual(agent["failure_kind"], "supervisor_dead")
        self.assertEqual(self.store.get_agent(terminal)["status"], "failed")

    def test_sweep_reconciles_only_a_proven_live_identity_mismatch(self) -> None:
        exact = self.supervised(200, "agent-run supervisor", created_at=1)
        boundary = self.supervised(201, "agent-run supervisor", created_at=2)
        unavailable = self.supervised(202, "agent-run supervisor", created_at=3)
        mismatch = self.supervised(203, "agent-run supervisor", created_at=4)
        patch, probed = self.probing(
            {
                200: (True, "agent-run supervisor", False),
                201: (True, "/usr/bin/python3 agent-run supervisor", True),
                202: (True, None, False),
                203: (True, "/usr/bin/vim notes.md", False),
            }
        )

        with patch, self.forbid_signals():
            self.assertEqual(reconcile_active_agents(self.store, at=10), (mismatch,))

        self.assertEqual(len(probed), 4)
        for agent_id in (exact, boundary, unavailable):
            self.assertEqual(self.store.get_agent(agent_id)["status"], "running")
        agent = self.store.get_agent(mismatch)
        self.assertEqual(agent["status"], "lost")
        self.assertEqual(agent["failure_kind"], "supervisor_identity_mismatch")

    def test_one_stale_row_does_not_abort_the_rest_of_the_sweep(self) -> None:
        stale = self.supervised(300, "pid-300", created_at=1)
        healthy = self.supervised(301, "pid-301", created_at=2)
        self.store.record_supervisor(
            stale, pid=300, identity="pid-300", process_group_id=300, at=100
        )
        patch, probed = self.probing(lambda pid: (False, None, False))

        with patch, self.forbid_signals():
            self.assertEqual(reconcile_active_agents(self.store, at=50), (healthy,))

        self.assertEqual(len(probed), 2)
        self.assertEqual(self.store.get_agent(stale)["status"], "running")
        self.assertEqual(self.store.get_agent(healthy)["status"], "lost")

    def test_each_row_is_timed_after_its_own_probe(self) -> None:
        now = time.time()
        first = self.supervised(400, "pid-400", created_at=1, heartbeat_at=now - 1)
        second = self.supervised(401, "pid-401", created_at=2, heartbeat_at=now - 1)

        def replies(pid):
            if pid == 400:
                # A heartbeat lands after the sweep began but before the next probe.
                time.sleep(0.01)
                self.store.record_supervisor(
                    second, pid=401, identity="pid-401", process_group_id=401, at=time.time()
                )
                time.sleep(0.01)
            return False, None, False

        patch, probed = self.probing(replies)
        with patch, self.forbid_signals():
            self.assertEqual(reconcile_active_agents(self.store), (first, second))

        self.assertEqual(probed, [(400, 400), (401, 401)])
        self.assertEqual(self.store.get_agent(second)["status"], "lost")


    def test_capacity_retention_is_global_deterministic_and_keeps_expired_rows(self) -> None:
        first = self.store.insert_capacity_sample(
            runtime="codex", lane="requests", window="5h", source="one",
            payload={}, observed_at=1, valid_until=1,
        )
        second = self.store.insert_capacity_sample(
            runtime="claude", lane="tokens", window="daily", source="two",
            payload={}, observed_at=2, valid_until=1,
        )
        third = self.store.insert_capacity_sample(
            runtime="opencode", lane="requests", window="weekly", source="three",
            payload={}, observed_at=2, valid_until=1,
        )

        rows = self.store.capacity_sample_history(retention=2)
        self.assertEqual([row["id"] for row in rows], [third, second])
        self.assertEqual(self.store.prune_capacity_samples(2), 1)
        self.assertEqual(
            [row["id"] for row in self.store.capacity_sample_history(retention=10)],
            [third, second],
        )
        self.assertNotIn(first, {row["id"] for row in rows})
        for retention in (True, 0, 1.5):
            with self.subTest(retention=retention), self.assertRaisesRegex(
                ValidationError, "positive integer"
            ):
                self.store.prune_capacity_samples(retention)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
