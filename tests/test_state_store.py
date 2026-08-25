import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.domain import (
    AgentStatus,
    Message,
    MessageRole,
    OrchestratorRef,
    Outcome,
    StartRequest,
)
from agent_run.errors import StateTransitionError, ValidationError
from agent_run.state import StateStore


class StateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.store = StateStore.initialize(self.root / "state.db")

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def request(
        self,
        *,
        request_id: str | None = None,
        orchestrator: OrchestratorRef | None = None,
        task: str = "do work",
    ) -> StartRequest:
        return StartRequest(
            "codex",
            "model",
            "profile",
            task,
            self.root,
            request_id=request_id,
            orchestrator=orchestrator,
        )

    def create(self, request: StartRequest | None = None):
        return self.store.create_agent(
            request or self.request(), task_summary="summary", config_revision="cfg-1", at=1
        )

    def test_session_request_id_is_idempotent_and_binding_is_immutable(self) -> None:
        ref = OrchestratorRef("codex_queue", "session-1", "turn-1")
        request = self.request(request_id="request-1", orchestrator=ref)
        first = self.create(request)
        second = self.create(request)
        self.assertEqual(first, second)
        self.assertEqual(len(self.store.list_agents()), 1)
        self.assertEqual(self.store.bind_orchestrator(first, ref, at=2), self.store.get_agent(first)["orchestrator_session_id"])

        with self.assertRaises(ValidationError):
            self.store.bind_orchestrator(
                first, OrchestratorRef("codex_queue", "different-session"), at=3
            )
        self.assertEqual(len(self.store.list_agents()), 1)

    def test_guarded_transitions_attempts_and_atomic_terminal_outbox(self) -> None:
        agent_id = self.create()
        attempt_id = self.store.create_attempt(
            agent_id, state="starting", adapter_state={"pid": 7}, at=2
        )
        self.store.transition(agent_id, AgentStatus.STARTING, attempt_id=attempt_id, at=3)
        self.store.transition(agent_id, AgentStatus.RUNNING, attempt_id=attempt_id, at=4)
        with self.assertRaises(StateTransitionError):
            self.store.transition(agent_id, AgentStatus.CREATED, at=5)
        self.store.finish_attempt(agent_id, attempt_id, state="finished", at=6)

        event_seq = self.store.transition(
            agent_id,
            AgentStatus.SUCCEEDED,
            outcome=Outcome(AgentStatus.SUCCEEDED, exit_code=0),
            at=7,
        )
        agent = self.store.get_agent(agent_id)
        delivery = self.store.connection.execute(
            "SELECT * FROM deliveries WHERE agent_id = ?", (agent_id,)
        ).fetchone()
        self.assertEqual(agent["status"], "succeeded")
        self.assertEqual(agent["finished_at"], 7)
        self.assertEqual(delivery["terminal_event_seq"], event_seq)
        self.assertEqual(delivery["state"], "waiting_binding")
        with self.assertRaises(StateTransitionError):
            self.store.transition(agent_id, AgentStatus.RUNNING, at=8)

    def test_terminal_update_rolls_back_when_delivery_activation_fails(self) -> None:
        ref = OrchestratorRef("codex_queue", "session", "turn")
        agent_id = self.create(self.request(orchestrator=ref))
        self.store.transition(agent_id, AgentStatus.STARTING, at=2)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=3)
        self.store.connection.execute(
            """CREATE TRIGGER reject_delivery BEFORE INSERT ON deliveries
               BEGIN SELECT RAISE(ABORT, 'delivery rejected'); END"""
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.store.transition(
                agent_id,
                AgentStatus.FAILED,
                outcome=Outcome(AgentStatus.FAILED),
                at=4,
            )

        self.assertEqual(self.store.get_agent(agent_id)["status"], "running")
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM events WHERE agent_id = ? AND to_status = 'failed'",
                (agent_id,),
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM deliveries WHERE agent_id = ?", (agent_id,)
            ).fetchone()[0],
            0,
        )

    def test_transcript_order_exact_active_count_and_command_ownership(self) -> None:
        first = self.create()
        second = self.create(self.request(task="second"))
        self.assertEqual(self.store.active_count(), 2)
        self.assertEqual(len(self.store.list_agents(limit=1)), 1)
        self.store.append_message(
            first, Message(3, MessageRole.TOOL_RESULT, "second", raw_ref="raw/2.json")
        )
        first_seq = self.store.append_message(first, Message(2, MessageRole.USER, "first"))
        transcript = self.store.transcript(first)
        self.assertEqual([item["content"] for item in transcript], ["second", "first"])
        self.assertEqual(self.store.transcript(first, after_seq=first_seq), [])
        self.assertEqual(transcript[0]["raw_ref"], "raw/2.json")

        first_command = self.store.enqueue_command(first, "cancel", {}, at=4)
        second_command = self.store.enqueue_command(first, "steer", {"text": "finish"}, at=5)
        self.assertEqual(self.store.claim_command(first, at=6)["id"], first_command)
        with self.assertRaises(ValidationError):
            self.store.complete_command(first_command, second, {}, at=7)
        self.store.complete_command(first_command, first, {"ok": True}, at=7)
        self.assertEqual(self.store.claim_command(first, at=8)["id"], second_command)

    def test_message_storage_refuses_oversized_inline_content_and_escaping_refs(self) -> None:
        class HundredMegabyteText(str):
            def encode(self, *_: object, **__: object):
                class ReportedBytes(bytes):
                    def __len__(self) -> int:
                        return 100 * 1024 * 1024

                return ReportedBytes()

        agent_id = self.create()
        self.store.append_message(
            agent_id, Message(1, MessageRole.USER, "x" * (32 * 1024))
        )
        for content in ("x" * (32 * 1024 + 1), HundredMegabyteText("externalize")):
            with self.subTest(size=len(content)), self.assertRaisesRegex(
                ValidationError, "32 KiB"
            ):
                self.store.append_message(agent_id, Message(2, MessageRole.USER, content))
        with self.assertRaisesRegex(ValidationError, "normalized relative path"):
            self.store.append_message(
                agent_id,
                Message(3, MessageRole.TOOL_RESULT, "external", raw_ref="../../outside"),
            )
        self.assertEqual(len(self.store.transcript(agent_id)), 1)


if __name__ == "__main__":
    unittest.main()
