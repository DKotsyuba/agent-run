import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.domain import AgentStatus, OrchestratorRef, Outcome, StartRequest
from agent_run.state import StateStore
from agent_run.hooks.bind import BindHookError, bind, run_hook


PAYLOAD = {
    "transport": "codex_queue",
    "external_session_id": "session-1",
    "external_turn_id": "turn-1",
}


class BindHookTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.store = StateStore.initialize(self.root / "state.db")

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def create(self) -> str:
        request = StartRequest(
            "codex", "model", "profile", "task", self.root, timeout_seconds=480
        )
        return self.store.create_agent(
            request, task_summary="summary", config_revision="cfg-1", at=1
        ).agent_id

    def finish(self, agent_id: str) -> None:
        self.store.transition(agent_id, AgentStatus.STARTING, at=2)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=3)
        self.store.transition(
            agent_id, AgentStatus.SUCCEEDED, outcome=Outcome(AgentStatus.SUCCEEDED), at=4
        )

    def deliveries(self, agent_id: str) -> list[dict]:
        return [
            dict(row)
            for row in self.store.connection.execute(
                "SELECT * FROM deliveries WHERE agent_id = ?", (agent_id,)
            )
        ]

    def test_binding_is_immutable_empty_then_same_target_but_never_another(self) -> None:
        agent_id = self.create()
        first = run_hook(self.store, {"agent_id": agent_id, **PAYLOAD}, at=5)
        self.assertEqual(first.agent_id, agent_id)
        self.assertIn(first.session_id, str(self.store.get_agent(agent_id)))
        self.assertIn("bound to session", first.message())

        same = run_hook(self.store, {"agent_id": agent_id, **PAYLOAD}, at=6)
        self.assertEqual(same.session_id, first.session_id)

        with self.assertRaises(BindHookError) as caught:
            run_hook(
                self.store,
                {"agent_id": agent_id, **PAYLOAD, "external_session_id": "session-2"},
                at=7,
            )
        message = str(caught.exception)
        self.assertIn("NOT confirmed", message)
        self.assertIn(agent_id, message)
        self.assertIn("immutable", message)
        self.assertEqual(
            self.store.get_agent(agent_id)["orchestrator_session_id"], first.session_id
        )

    def test_bound_agent_gets_exactly_one_notice_and_a_rebind_never_resurrects_it(self) -> None:
        agent_id = self.create()
        # An orchestrator-backed start binds before the agent finishes, so the
        # notice is created pending rather than waiting for a binding.
        result = bind(self.store, agent_id, OrchestratorRef(**PAYLOAD), at=1)
        self.finish(agent_id)
        activated = self.deliveries(agent_id)
        self.assertEqual(len(activated), 1)
        self.assertEqual(activated[0]["state"], "pending")
        self.assertEqual(activated[0]["next_attempt_at"], 4)

        # A repeated bind must not resurrect or duplicate an already sent notice.
        claimed = self.store.claim_delivery("worker", at=5)
        self.store.complete_delivery(claimed["id"], "worker", remote_message_id="r1", at=6)
        repeated = bind(self.store, agent_id, OrchestratorRef(**PAYLOAD), at=7)
        self.assertEqual(repeated.session_id, result.session_id)
        rows = self.deliveries(agent_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["state"], "delivered")

    def test_unbound_terminal_agent_never_gets_a_notice_row(self) -> None:
        agent_id = self.create()
        self.finish(agent_id)
        self.assertEqual(self.deliveries(agent_id), [])

    def test_unbound_running_agent_keeps_running_without_a_delivery(self) -> None:
        agent_id = self.create()
        self.store.transition(agent_id, AgentStatus.STARTING, at=2)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=3)
        self.assertEqual(self.deliveries(agent_id), [])
        bind(self.store, agent_id, OrchestratorRef(**PAYLOAD), at=5)
        self.assertEqual(self.store.get_agent(agent_id)["status"], "running")
        self.assertEqual(self.deliveries(agent_id), [])

    def test_every_hook_failure_is_loud_and_reports_the_unconfirmed_notification(self) -> None:
        agent_id = self.create()
        unusable = [
            {},
            {"agent_id": agent_id},
            {"agent_id": agent_id, "transport": "codex_queue"},
            {"agent_id": agent_id, **PAYLOAD, "surprise": 1},
            {"agent_id": "not-an-agent", **PAYLOAD},
            {"agent_id": "ag-20260825-120000-0123456789", **PAYLOAD},
            [("agent_id", agent_id)],
        ]
        for payload in unusable:
            with self.assertRaises(BindHookError) as caught:
                run_hook(self.store, payload, at=5)  # type: ignore[arg-type]
            self.assertIn("NOT confirmed", str(caught.exception))
            self.assertIn("keep this turn alive", str(caught.exception))
        self.assertIsNone(self.store.get_agent(agent_id)["orchestrator_session_id"])

    def test_bind_rejects_arguments_that_are_not_the_declared_contract(self) -> None:
        agent_id = self.create()
        with self.assertRaises(Exception):
            bind(object(), agent_id, OrchestratorRef(**PAYLOAD))  # type: ignore[arg-type]
        with self.assertRaises(Exception):
            bind(self.store, agent_id, PAYLOAD)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
