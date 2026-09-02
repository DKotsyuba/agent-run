import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.config import DeliveryConfig
from agent_run.domain import AgentStatus, OrchestratorRef
from agent_run.errors import ValidationError
from agent_run.delivery.base import (
    AmbiguousDeliveryError,
    CompletionNotice,
    DeliveryAttemptEvidence,
    DeliveryError,
)
from agent_run.delivery.codex_queue import CodexQueueSender, CodexQueueTransport


AGENT_ID = "ag-20260825-120000-0123456789"


from agent_run.delivery.codex_queue import SessionGoneError


from agent_run.delivery.codex_desktop_relay import CodexDesktopRelayClient


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
            raise SessionGoneError(f"no such session: {session_id}")
        return f"remote-{len(self.sent)}"


class FakeRelay(CodexDesktopRelayClient):
    """A relay-only test double that records sends without a queue capability."""

    def __init__(self, outcome: object = True) -> None:
        """Set the next outcome; exceptions model relay delivery failures."""
        self.outcome = outcome
        self.calls: list[tuple[OrchestratorRef, CompletionNotice]] = []
        self.last_evidence: DeliveryAttemptEvidence | None = None

    def send(self, target: OrchestratorRef, notice: CompletionNotice) -> bool:
        """Record one relay send, returning acceptance or raising its outcome."""
        self.calls.append((target, notice))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        self.last_evidence = DeliveryAttemptEvidence(
            classifier="relay_accepted", executable="codex_desktop_relay",
            argv_shape=("relay",), duration_ms=1,
        )
        return bool(self.outcome)
class CodexQueueTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.notice = CompletionNotice("ntf_abc", AGENT_ID, AgentStatus.SUCCEEDED)
        self.target = OrchestratorRef("codex_queue", "session-1", "turn-1")

    def test_send_uses_the_relay_without_a_queue_message_id(self) -> None:
        """Return relay acceptance for the exact trusted target and notice."""
        relay = FakeRelay()
        receipt = CodexQueueTransport(relay).send(self.target, self.notice)
        self.assertEqual(relay.calls, [(self.target, self.notice)])
        self.assertIsNone(receipt.remote_message_id)
        self.assertFalse(receipt.ambiguous)

    def test_ambiguous_timeout_is_at_least_once_not_a_failure(self) -> None:
        """Propagate uncertain acceptance without invoking another transport."""
        relay = FakeRelay(AmbiguousDeliveryError("relay acceptance is unknown"))
        with self.assertRaises(AmbiguousDeliveryError) as caught:
            CodexQueueTransport(relay).send(self.target, self.notice)
        self.assertIn("relay", str(caught.exception))
        self.assertIsInstance(caught.exception, DeliveryError)
        self.assertEqual(len(relay.calls), 1)

    def test_missing_session_fails_and_no_replacement_is_started(self) -> None:
        """A relay refusal cannot create a replacement session or agent."""
        relay = FakeRelay(DeliveryError("relay rejected"))
        transport = CodexQueueTransport(relay)
        with self.assertRaises(DeliveryError) as caught:
            transport.send(OrchestratorRef("codex_queue", "gone"), self.notice)
        self.assertNotIsInstance(caught.exception, AmbiguousDeliveryError)
        self.assertIn("relay", str(caught.exception))
        # The transport exposes no way to open a session or start an agent.
        for forbidden in ("start", "open", "create", "spawn", "launch"):
            self.assertFalse(
                any(forbidden in name for name in dir(transport)),
                f"transport must not expose a {forbidden} path",
            )

    def test_unavailable_relay_and_foreign_target_are_delivery_errors(self) -> None:
        """Reject unavailable relays and bindings for another transport."""
        with self.assertRaises(DeliveryError):
            CodexQueueTransport(FakeRelay(DeliveryError("relay unavailable"))).send(
                self.target, self.notice
            )
        with self.assertRaises(DeliveryError):
            CodexQueueTransport(FakeRelay()).send(
                OrchestratorRef("slack", "session-1"), self.notice
            )

    def test_unexpected_relay_exceptions_are_not_reclassified(self) -> None:
        """Unexpected client bugs remain distinguishable from known refusal."""
        for error in (KeyError("sender bug"), IndexError("sender bug")):
            with self.assertRaises(type(error)):
                CodexQueueTransport(FakeRelay(error)).send(
                    self.target, self.notice
                )

    def test_validate_and_arguments_are_checked(self) -> None:
        """Reject malformed transport configuration, clients and notices."""
        transport = CodexQueueTransport(FakeRelay())
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


    def test_relay_acceptance_bypasses_the_queue_without_a_remote_id(self) -> None:
        """Preserve static relay evidence and never call the UI queue."""
        queue = RecordingQueue()
        relay = mock.Mock()
        relay.send.return_value = True
        relay.last_evidence = DeliveryAttemptEvidence(
            classifier="relay_accepted",
            executable="codex_desktop_relay",
            argv_shape=("relay",),
            duration_ms=1,
        )
        receipt = CodexQueueTransport(relay).send(self.target, self.notice)
        relay.send.assert_called_once_with(self.target, self.notice)
        self.assertEqual(queue.sent, [])
        self.assertIsNone(receipt.remote_message_id)
        self.assertFalse(receipt.ambiguous)
        self.assertEqual(receipt.evidence.classifier, "relay_accepted")

    def test_relay_rejection_never_falls_back_to_the_queue(self) -> None:
        """A false acceptance result raises rather than being marked delivered."""
        queue = RecordingQueue()
        relay = mock.Mock()
        relay.send.return_value = False
        with self.assertRaises(DeliveryError):
            CodexQueueTransport(relay).send(self.target, self.notice)
        relay.send.assert_called_once_with(self.target, self.notice)
        self.assertEqual(queue.sent, [])

    def test_relay_ambiguity_never_falls_back_to_the_queue(self) -> None:
        """Post-write uncertainty propagates without a second delivery path."""
        queue = RecordingQueue()
        relay = mock.Mock()
        relay.send.side_effect = AmbiguousDeliveryError("relay acceptance is unknown")
        with self.assertRaises(AmbiguousDeliveryError):
            CodexQueueTransport(relay).send(self.target, self.notice)
        self.assertEqual(queue.sent, [])

class RecordingRunner:
    def __init__(self, *, returncode=0, stdout="", stderr="", error=None):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.error = error
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if self.error is not None:
            raise self.error
        return subprocess.CompletedProcess(argv, self.returncode, self.stdout, self.stderr)


class CodexQueueSenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.executable = Path(self.temporary.name) / "codex"
        self.executable.write_text("fake executable")
        os.chmod(self.executable, 0o700)
        self.environment = {
            "SAFE": "present",
            "CLAUDE_CODE_OAUTH_TOKEN": "secret",
            "ANTHROPIC_API_KEY": "secret",
            "ANTHROPIC_AUTH_TOKEN": "secret",
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def sender(self, runner, **kwargs) -> CodexQueueSender:
        return CodexQueueSender(
            self.executable,
            runner=runner,
            environment=self.environment,
            **kwargs,
        )

    def test_exact_legacy_argv_no_stdin_and_sanitized_environment(self) -> None:
        runner = RecordingRunner(stdout="Queued message remote-1 for thread session-1.\n")
        sender = self.sender(runner)
        message = "Trusted completion notice"

        self.assertEqual(sender("session-1", message), "remote-1")
        self.assertEqual(len(runner.calls), 1)
        argv, kwargs = runner.calls[0]
        self.assertEqual(
            argv,
            [
                str(self.executable.resolve()),
                "queue",
                "--thread",
                "session-1",
                "--message",
                message,
            ],
        )
        self.assertEqual(kwargs["env"], {"SAFE": "present"})
        self.assertEqual(kwargs["timeout"], 30.0)
        self.assertEqual(
            {name: kwargs[name] for name in ("capture_output", "text", "check", "shell")},
            {"capture_output": True, "text": True, "check": False, "shell": False},
        )
        self.assertNotIn("stdin", kwargs)
        self.assertNotIn("input", kwargs)

    def test_zero_exit_without_message_id_is_still_proven_acceptance(self) -> None:
        runner = RecordingRunner(stdout="message accepted")
        self.assertIsNone(self.sender(runner)("session-1", "trusted message"))

    def test_timeout_is_ambiguous_for_sender_and_transport(self) -> None:
        runner = RecordingRunner(error=subprocess.TimeoutExpired(["codex"], 1))
        sender = self.sender(runner)
        with self.assertRaises(TimeoutError):
            sender("session-1", "trusted message")

        transport = CodexQueueTransport(
            FakeRelay(AmbiguousDeliveryError("relay acceptance is unknown"))
        )
        notice = CompletionNotice("ntf_sender", AGENT_ID, AgentStatus.SUCCEEDED)
        with self.assertRaises(AmbiguousDeliveryError):
            transport.send(OrchestratorRef("codex_queue", "session-1"), notice)

    def test_exact_missing_session_result_is_narrowly_classified(self) -> None:
        runner = RecordingRunner(
            returncode=1,
            stderr="warning: harmless\nError: thread not found: gone\n",
        )
        with self.assertRaises(SessionGoneError):
            self.sender(runner)("gone", "trusted message")

        generic = RecordingRunner(returncode=1, stderr="thread not found: someone-else")
        with self.assertRaises(DeliveryError):
            self.sender(generic)("gone", "trusted message")

    def test_generic_and_io_failures_are_stable_and_do_not_leak_output(self) -> None:
        runner = RecordingRunner(returncode=7, stderr="credential-shaped secret")
        with self.assertRaises(DeliveryError) as caught:
            self.sender(runner)("session-1", "trusted message")
        self.assertEqual(str(caught.exception), "codex queue exited with status 7")
        self.assertNotIn("secret", str(caught.exception))

        io_error = OSError("launch failed")
        with self.assertRaises(OSError) as caught_io:
            self.sender(RecordingRunner(error=io_error))("session-1", "trusted message")
        self.assertIs(caught_io.exception, io_error)

    def test_attempt_evidence_is_bounded_redacted_and_exactly_classified(self) -> None:
        """Preserve exit, timeout, and success facts without private values."""

        self.environment["SERVICE_TOKEN"] = "token-secret"
        private = "trusted message"
        stderr = f"session-1 {private} token-secret " + ("é" * 3000)
        sender = self.sender(
            RecordingRunner(returncode=127, stdout="stdout-safe", stderr=stderr)
        )
        with self.assertRaises(DeliveryError) as caught:
            sender("session-1", private)
        evidence = caught.exception.evidence
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual((evidence.classifier, evidence.returncode), ("exit", 127))
        self.assertEqual(evidence.stdout_tail, "stdout-safe")
        self.assertEqual(evidence.stderr_bytes, len(stderr.encode("utf-8")))
        self.assertTrue(evidence.stderr_truncated)
        self.assertLessEqual(len(evidence.stderr_tail.encode("utf-8")), 4096)
        for secret in ("session-1", private, "token-secret"):
            self.assertNotIn(secret, evidence.stderr_tail)

        accepted = self.sender(
            RecordingRunner(stdout="Queued message remote-1 for thread session-1.\n")
        )
        self.assertEqual(accepted("session-1", private), "remote-1")
        assert accepted.last_evidence is not None
        self.assertTrue(accepted.last_evidence.message_id_present)

        timed_out = self.sender(
            RecordingRunner(error=subprocess.TimeoutExpired(["codex"], 1))
        )
        with self.assertRaises(TimeoutError):
            timed_out("session-1", private)
        assert timed_out.last_evidence is not None
        self.assertEqual(timed_out.last_evidence.classifier, "timeout")

        spawned = self.sender(RecordingRunner(error=OSError(2, "missing")))
        with self.assertRaises(OSError):
            spawned("session-1", private)
        assert spawned.last_evidence is not None
        self.assertEqual(
            (spawned.last_evidence.classifier, spawned.last_evidence.spawn_errno),
            ("spawn", 2),
        )

    def test_constructor_inputs_and_outputs_are_bounded_without_replacement_paths(self) -> None:
        with self.assertRaises(ValidationError):
            CodexQueueSender("codex")
        os.chmod(self.executable, 0o600)
        with self.assertRaises(ValidationError):
            CodexQueueSender(self.executable)
        os.chmod(self.executable, 0o700)
        for kwargs in ({"timeout_seconds": 0}, {"max_output_bytes": 0}):
            with self.assertRaises(ValidationError):
                CodexQueueSender(self.executable, **kwargs)

        sender = self.sender(RecordingRunner())
        for session in ("", "x" * 513, "bad\x00session"):
            with self.assertRaises(ValidationError):
                sender(session, "trusted message")
        with self.assertRaises(ValidationError):
            sender("session-1", "x" * 4097)
        with self.assertRaises(DeliveryError):
            self.sender(RecordingRunner(stdout="x" * 9), max_output_bytes=8)(
                "session-1", "trusted message"
            )
        for forbidden in ("start", "open", "create", "spawn", "launch"):
            self.assertFalse(any(forbidden in name for name in dir(sender)))


if __name__ == "__main__":
    unittest.main()
