import sqlite3
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

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
        ).agent_id

    def test_session_request_id_is_idempotent_and_binding_is_immutable(self) -> None:
        ref = OrchestratorRef("codex_queue", "session-1", "turn-1")
        request = self.request(request_id="request-1", orchestrator=ref)
        first = self.store.create_agent(
            request, task_summary="summary", config_revision="cfg-1", at=1
        )
        second = self.store.create_agent(
            request, task_summary="summary", config_revision="cfg-1", at=1
        )
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.agent_id, second.agent_id)
        self.assertEqual(len(self.store.list_agents()), 1)
        self.assertEqual(
            self.store.bind_orchestrator(first.agent_id, ref, at=2),
            self.store.get_agent(first.agent_id)["orchestrator_session_id"],
        )

        with self.assertRaises(ValidationError):
            self.store.bind_orchestrator(
                first.agent_id,
                OrchestratorRef("codex_queue", "different-session"),
                at=3,
            )
        self.assertEqual(len(self.store.list_agents()), 1)

    def test_unbound_request_id_is_globally_concurrent_and_exact(self) -> None:
        request = self.request(request_id="shared-request")
        barrier = Barrier(2)
        database = self.root / "state.db"

        def create_once():
            store = StateStore.open(database)
            try:
                barrier.wait()
                return store.create_agent(
                    request, task_summary="summary", config_revision="cfg-1", at=2
                )
            finally:
                store.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(create_once) for _ in range(2)]
            results = [future.result() for future in futures]

        self.assertEqual([result.created for result in results].count(True), 1)
        self.assertEqual(results[0].agent_id, results[1].agent_id)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM agents WHERE request_id = 'shared-request'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                """SELECT COUNT(*) FROM events
                   WHERE agent_id = ? AND kind = 'created'""",
                (results[0].agent_id,),
            ).fetchone()[0],
            1,
        )
        self.assertFalse(
            self.store.create_agent(
                request, task_summary="summary", config_revision="cfg-1", at=3
            ).created
        )
        with self.assertRaises(ValidationError):
            self.store.create_agent(
                self.request(request_id="shared-request", task="different"),
                task_summary="summary",
                config_revision="cfg-1",
            )
        with self.assertRaises(ValidationError):
            self.store.create_agent(
                request, task_summary="different", config_revision="cfg-1"
            )
        with self.assertRaises(ValidationError):
            self.store.create_agent(
                request, task_summary="summary", config_revision="cfg-2"
            )

        first = self.store.create_agent(
            self.request(task="non-idempotent-1"),
            task_summary="summary",
            config_revision="cfg-1",
        )
        second = self.store.create_agent(
            self.request(task="non-idempotent-2"),
            task_summary="summary",
            config_revision="cfg-1",
        )
        self.assertTrue(first.created and second.created)
        self.assertNotEqual(first.agent_id, second.agent_id)

    def test_concurrent_limited_creation_allows_exactly_one_process(self) -> None:
        requests = (
            self.request(
                request_id="cap-a",
                orchestrator=OrchestratorRef("codex_queue", "cap-a"),
            ),
            self.request(
                request_id="cap-b",
                orchestrator=OrchestratorRef("codex_queue", "cap-b"),
            ),
        )
        barrier = Barrier(2)
        database = self.root / "state.db"

        def create_once(request):
            store = StateStore.open(database)
            try:
                barrier.wait()
                return store.create_agent_limited(
                    request,
                    task_summary="summary",
                    config_revision="cfg-1",
                    global_limit=1,
                    runtime_limit=1,
                    at=2,
                )
            finally:
                store.close()

        created = []
        refused = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(create_once, request) for request in requests]
            for future in futures:
                try:
                    created.append(future.result())
                except ValidationError as error:
                    refused.append(str(error))

        self.assertEqual(len(created), 1)
        self.assertEqual(refused, ["global active agent limit reached"])
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM agents").fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM events WHERE kind = 'created'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM orchestrator_sessions"
            ).fetchone()[0],
            1,
        )

    def test_limited_creation_checks_idempotency_before_caps_and_validates_limits(self) -> None:
        request = self.request(
            request_id="limited-duplicate",
            orchestrator=OrchestratorRef("codex_queue", "limited-session"),
        )
        first = self.store.create_agent_limited(
            request,
            task_summary="summary",
            config_revision="cfg-1",
            global_limit=1,
            runtime_limit=1,
        )
        duplicate = self.store.create_agent_limited(
            request,
            task_summary="summary",
            config_revision="cfg-1",
            global_limit=1,
            runtime_limit=1,
        )
        self.assertTrue(first.created)
        self.assertFalse(duplicate.created)
        self.assertEqual(first.agent_id, duplicate.agent_id)
        with self.assertRaisesRegex(ValidationError, "request_id was reused"):
            self.store.create_agent_limited(
                self.request(request_id="limited-duplicate", task="different"),
                task_summary="summary",
                config_revision="cfg-1",
                global_limit=1,
                runtime_limit=1,
            )
        for name, global_limit, runtime_limit in (
            ("global bool", True, 1),
            ("global fraction", 1.5, 1),
            ("global zero", 0, 1),
            ("runtime bool", 1, True),
        ):
            with self.subTest(name=name), self.assertRaisesRegex(
                ValidationError, "must be a positive integer"
            ):
                self.store.create_agent_limited(
                    self.request(task=name),
                    task_summary="summary",
                    config_revision="cfg-1",
                    global_limit=global_limit,  # type: ignore[arg-type]
                    runtime_limit=runtime_limit,  # type: ignore[arg-type]
                )
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM agents").fetchone()[0],
            1,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM orchestrator_sessions"
            ).fetchone()[0],
            1,
        )

    def test_global_runtime_caps_and_terminal_rows_release_capacity(self) -> None:
        def request_for(runtime: str, task: str) -> StartRequest:
            return StartRequest(runtime, "model", "profile", task, self.root)

        first = self.store.create_agent_limited(
            request_for("codex", "first"),
            task_summary="first",
            config_revision="cfg-1",
            global_limit=2,
            runtime_limit=1,
        ).agent_id
        with self.assertRaisesRegex(ValidationError, "runtime active agent limit"):
            self.store.create_agent_limited(
                request_for("codex", "runtime-refused"),
                task_summary="runtime-refused",
                config_revision="cfg-1",
                global_limit=2,
                runtime_limit=1,
            )
        self.store.create_agent_limited(
            request_for("claude", "other"),
            task_summary="other",
            config_revision="cfg-1",
            global_limit=2,
            runtime_limit=1,
        )
        with self.assertRaisesRegex(ValidationError, "global active agent limit"):
            self.store.create_agent_limited(
                request_for("opencode", "global-refused"),
                task_summary="global-refused",
                config_revision="cfg-1",
                global_limit=2,
                runtime_limit=1,
            )

        self.store.transition(first, AgentStatus.STARTING, at=2)
        self.store.transition(first, AgentStatus.RUNNING, at=3)
        self.store.transition(
            first,
            AgentStatus.FAILED,
            outcome=Outcome(AgentStatus.FAILED),
            at=4,
        )
        replacement = self.store.create_agent_limited(
            request_for("codex", "replacement"),
            task_summary="replacement",
            config_revision="cfg-1",
            global_limit=2,
            runtime_limit=1,
        )
        self.assertTrue(replacement.created)
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM agents").fetchone()[0],
            3,
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM events WHERE kind = 'created'"
            ).fetchone()[0],
            3,
        )

    def test_session_lookup_and_agent_filter_are_read_only_and_composable(self) -> None:
        first = self.create(self.request(task="first"))
        second = self.create(self.request(task="second"))
        other = self.create(self.request(task="other"))
        ref = OrchestratorRef("codex_queue", "session-a", "turn-a")
        updated_ref = OrchestratorRef("codex_queue", "session-a", "turn-b")
        other_ref = OrchestratorRef("other_transport", "session-a", "turn-c")
        session_id = self.store.bind_orchestrator(first, ref, at=2)
        self.assertEqual(
            self.store.bind_orchestrator(second, updated_ref, at=3), session_id
        )
        self.assertEqual(
            self.store.bind_orchestrator(
                first, OrchestratorRef("codex_queue", "session-a"), at=4
            ),
            session_id,
        )
        self.store.bind_orchestrator(other, other_ref, at=2)
        stored_session = self.store.connection.execute(
            """SELECT external_turn_id, last_seen_at FROM orchestrator_sessions
               WHERE id = ?""",
            (session_id,),
        ).fetchone()
        self.assertEqual(tuple(stored_session), ("turn-b", 4))
        self.assertEqual(
            self.store.connection.execute(
                """SELECT COUNT(*) FROM orchestrator_sessions
                   WHERE transport = 'codex_queue' AND external_session_id = 'session-a'"""
            ).fetchone()[0],
            1,
        )

        session_count = self.store.connection.execute(
            "SELECT COUNT(*) FROM orchestrator_sessions"
        ).fetchone()[0]
        self.assertIsNone(
            self.store.find_orchestrator_session(
                OrchestratorRef("codex_queue", "unknown-session")
            )
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM orchestrator_sessions"
            ).fetchone()[0],
            session_count,
        )
        self.assertEqual(self.store.find_orchestrator_session(ref), session_id)
        self.assertEqual(
            {row["id"] for row in self.store.list_agents(orchestrator_session_id=session_id)},
            {first, second},
        )
        self.assertEqual(len(self.store.list_agents()), 3)

        self.store.transition(first, AgentStatus.STARTING, at=5)
        self.assertEqual(
            [
                row["id"]
                for row in self.store.list_agents(
                    statuses=[AgentStatus.CREATED],
                    orchestrator_session_id=session_id,
                )
            ],
            [second],
        )
        with self.assertRaises(ValidationError):
            self.store.list_agents(orchestrator_session_id=" ")

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
