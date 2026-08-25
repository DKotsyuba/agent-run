import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.config import DeliveryConfig
from agent_run.domain import AgentStatus, OrchestratorRef
from agent_run.errors import ValidationError
from agent_run.delivery.base import (
    AmbiguousDeliveryError,
    CompletionNotice,
    DeliveryError,
)
from agent_run.delivery.codex_queue import CodexQueueTransport


AGENT_ID = "ag-20260825-120000-0123456789"


class RecordingQueue:
    """A queue that records sends and can only reach existing sessions."""

    def __init__(self, sessions=("session-1",), outcome=None):
        self.sessions = set(sessions)
        self.outcome = outcome
        self.sent: list[tuple[str, str]] = []

    def __call__(self, session_id: str, text: str) -> str | None:
        self.sent.append((session_id, text))
        if self.outcome is not None:
            raise self.outcome
        if session_id not in self.sessions:
            raise LookupError(f"no such session: {session_id}")
        return f"remote-{len(self.sent)}"


class CodexQueueTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.notice = CompletionNotice("ntf_abc", AGENT_ID, AgentStatus.SUCCEEDED)
        self.target = OrchestratorRef("codex_queue", "session-1", "turn-1")

    def test_send_queues_the_fixed_trusted_message_and_returns_the_remote_id(self) -> None:
        queue = RecordingQueue()
        receipt = CodexQueueTransport(queue).send(self.target, self.notice)
        self.assertEqual(queue.sent, [("session-1", self.notice.render())])
        self.assertEqual(receipt.remote_message_id, "remote-1")
        self.assertFalse(receipt.ambiguous)

    def test_ambiguous_timeout_is_at_least_once_not_a_failure(self) -> None:
        queue = RecordingQueue(outcome=TimeoutError("no ack"))
        with self.assertRaises(AmbiguousDeliveryError) as caught:
            CodexQueueTransport(queue).send(self.target, self.notice)
        self.assertIn("ntf_abc", str(caught.exception))
        self.assertIsInstance(caught.exception, DeliveryError)
        self.assertEqual(len(queue.sent), 1)

    def test_missing_session_fails_and_no_replacement_is_started(self) -> None:
        queue = RecordingQueue(sessions=())
        transport = CodexQueueTransport(queue)
        with self.assertRaises(DeliveryError) as caught:
            transport.send(OrchestratorRef("codex_queue", "gone"), self.notice)
        self.assertNotIsInstance(caught.exception, AmbiguousDeliveryError)
        self.assertIn("never opens a replacement", str(caught.exception))
        # The transport exposes no way to open a session or start an agent.
        for forbidden in ("start", "open", "create", "spawn", "launch"):
            self.assertFalse(
                any(forbidden in name for name in dir(transport)),
                f"transport must not expose a {forbidden} path",
            )

    def test_unreachable_queue_and_foreign_target_are_delivery_errors(self) -> None:
        with self.assertRaises(DeliveryError):
            CodexQueueTransport(RecordingQueue(outcome=OSError("socket"))).send(
                self.target, self.notice
            )
        with self.assertRaises(DeliveryError):
            CodexQueueTransport(RecordingQueue()).send(
                OrchestratorRef("slack", "session-1"), self.notice
            )

    def test_validate_and_arguments_are_checked(self) -> None:
        transport = CodexQueueTransport(RecordingQueue())
        transport.validate(DeliveryConfig())
        with self.assertRaises(ValidationError):
            transport.validate(object())  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            transport.validate(DeliveryConfig(max_attempts=-1))
        with self.assertRaises(ValidationError):
            transport.validate(DeliveryConfig(retry_base_seconds=0))
        with self.assertRaises(ValidationError):
            CodexQueueTransport("not callable")  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            transport.send("session-1", self.notice)  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            transport.send(self.target, "done")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
