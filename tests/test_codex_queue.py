import os
import subprocess
import sys
import tempfile
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
from agent_run.delivery.codex_queue import CodexQueueSender, CodexQueueTransport


AGENT_ID = "ag-20260825-120000-0123456789"


from agent_run.delivery.codex_queue import SessionGoneError


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

    def test_only_explicit_session_gone_is_classified_as_missing(self) -> None:
        for error in (KeyError("sender bug"), IndexError("sender bug")):
            with self.assertRaises(type(error)):
                CodexQueueTransport(RecordingQueue(outcome=error)).send(
                    self.target, self.notice
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

        transport = CodexQueueTransport(sender)
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
