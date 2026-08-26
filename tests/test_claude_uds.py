import json
import os
import socket
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.config import DeliveryConfig
from agent_run.domain import AgentStatus, OrchestratorRef
from agent_run.errors import ValidationError
from agent_run.delivery.base import (
    AmbiguousDeliveryError,
    CompletionNotice,
    DeliveryError,
)
from agent_run.delivery.claude_uds import (
    TRANSPORT_NAME,
    ClaudeSessionSender,
    ClaudeUdsTransport,
    SessionGoneError,
)


AGENT_ID = "ag-20260825-120000-0123456789"
SESSION = "54aec320-9c9a-405d-9cd4-cb01b47f50e0"


class RecordingInbox:
    """A sender that records injections and can only reach existing sessions."""

    def __init__(self, sessions=(SESSION,), outcome=None):
        self.sessions = set(sessions)
        self.outcome = outcome
        self.sent: list[tuple[str, str]] = []

    def __call__(self, session_id: str, text: str) -> str | None:
        self.sent.append((session_id, text))
        if self.outcome is not None:
            raise self.outcome
        if session_id not in self.sessions:
            raise SessionGoneError(f"no such session: {session_id}")
        return None


class ClaudeUdsTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.notice = CompletionNotice("ntf_abc", AGENT_ID, AgentStatus.SUCCEEDED)
        self.target = OrchestratorRef(TRANSPORT_NAME, SESSION, "turn-1")

    def test_send_injects_the_fixed_trusted_message_without_a_remote_id(self) -> None:
        inbox = RecordingInbox()
        receipt = ClaudeUdsTransport(inbox).send(self.target, self.notice)
        self.assertEqual(inbox.sent, [(SESSION, self.notice.render())])
        self.assertIsNone(receipt.remote_message_id)
        self.assertFalse(receipt.ambiguous)

    def test_ambiguous_timeout_is_at_least_once_not_a_failure(self) -> None:
        inbox = RecordingInbox(outcome=TimeoutError("no ack"))
        with self.assertRaises(AmbiguousDeliveryError) as caught:
            ClaudeUdsTransport(inbox).send(self.target, self.notice)
        self.assertIn("ntf_abc", str(caught.exception))
        self.assertIsInstance(caught.exception, DeliveryError)
        self.assertEqual(len(inbox.sent), 1)

    def test_missing_session_fails_and_no_replacement_is_started(self) -> None:
        transport = ClaudeUdsTransport(RecordingInbox(sessions=()))
        with self.assertRaises(DeliveryError) as caught:
            transport.send(OrchestratorRef(TRANSPORT_NAME, "gone"), self.notice)
        self.assertNotIsInstance(caught.exception, AmbiguousDeliveryError)
        self.assertIn("never opens a replacement", str(caught.exception))
        for forbidden in ("start", "open", "create", "spawn", "launch"):
            self.assertFalse(
                any(forbidden in name for name in dir(transport)),
                f"transport must not expose a {forbidden} path",
            )

    def test_unreachable_socket_and_foreign_target_are_delivery_errors(self) -> None:
        with self.assertRaises(DeliveryError):
            ClaudeUdsTransport(RecordingInbox(outcome=OSError("socket"))).send(
                self.target, self.notice
            )
        with self.assertRaises(DeliveryError):
            ClaudeUdsTransport(RecordingInbox()).send(
                OrchestratorRef("codex_queue", SESSION), self.notice
            )

    def test_only_explicit_session_gone_is_classified_as_missing(self) -> None:
        for error in (KeyError("sender bug"), IndexError("sender bug")):
            with self.assertRaises(type(error)):
                ClaudeUdsTransport(RecordingInbox(outcome=error)).send(
                    self.target, self.notice
                )

    def test_validate_and_arguments_are_checked(self) -> None:
        transport = ClaudeUdsTransport(RecordingInbox())
        transport.validate(DeliveryConfig())
        with self.assertRaises(ValidationError):
            transport.validate(object())  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            transport.validate(DeliveryConfig(max_attempts=-1))
        with self.assertRaises(ValidationError):
            transport.validate(DeliveryConfig(retry_base_seconds=0))
        with self.assertRaises(ValidationError):
            ClaudeUdsTransport("not callable")  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            transport.send(SESSION, self.notice)  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            transport.send(self.target, "done")  # type: ignore[arg-type]


class FakeInboxServer:
    """A Unix socket that reads one connection of newline-delimited JSON.

    One connection is all a delivery makes, so the thread serves exactly one
    and exits; that keeps ``close`` a plain join with no accept race.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lines: list[dict] = []
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(str(path))
        # A backlog, so probe-connects after the served one still succeed.
        self._server.listen(16)
        self._server.settimeout(2.0)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        try:
            connection, _ = self._server.accept()
        except (OSError, socket.timeout):
            return
        with connection:
            connection.settimeout(5.0)
            buffer = b""
            try:
                while True:
                    chunk = connection.recv(4096)
                    if not chunk:
                        break
                    buffer += chunk
            except (OSError, socket.timeout):
                pass
        for line in buffer.splitlines():
            if line.strip():
                self.lines.append(json.loads(line))

    def close(self) -> None:
        """Stop listening but leave the socket file, exactly as a crash does."""

        try:
            self._server.close()
        except OSError:
            pass
        self._thread.join(timeout=5.0)


class ClaudeSessionSenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.registry = Path(self.temporary.name) / "sessions"
        self.registry.mkdir()
        self.socket_dir = Path(self.temporary.name) / "socks"
        self.socket_dir.mkdir()
        self.servers: list[FakeInboxServer] = []

    def tearDown(self) -> None:
        for server in self.servers:
            server.close()
        self.temporary.cleanup()

    def descriptor(self, pid=4242, session=SESSION, **overrides) -> Path:
        socket_path = self.socket_dir / f"{pid}.sock"
        document = {
            "pid": pid,
            "sessionId": session,
            "messagingSocketPath": str(socket_path),
            "peerProtocol": 1,
            "peerFeatures": ["notify_idle"],
        }
        document.update(overrides)
        path = self.registry / f"{pid}.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        return path

    def key(self, pid=4242, token="peer-token-1") -> Path:
        path = self.registry / f"{pid}.{'a' * 64}.key"
        path.write_text(json.dumps({"peerToken": token, "procStart": "x"}), encoding="utf-8")
        os.chmod(path, 0o600)
        return path

    def listen(self, pid=4242) -> FakeInboxServer:
        server = FakeInboxServer(self.socket_dir / f"{pid}.sock")
        self.servers.append(server)
        return server

    def sender(self, **kwargs) -> ClaudeSessionSender:
        return ClaudeSessionSender(self.registry, **kwargs)

    def test_clean_send_writes_the_auth_line_then_the_user_line(self) -> None:
        self.descriptor()
        self.key()
        server = self.listen()
        message = "agent-run: agent finished"

        self.assertIsNone(self.sender()(SESSION, message))
        server.close()
        self.assertEqual(
            server.lines,
            [
                {"token": "peer-token-1", "type": "auth"},
                {
                    "message": {"content": message, "role": "user"},
                    "type": "user",
                },
            ],
        )

    def test_registry_miss_is_session_gone_not_ambiguous(self) -> None:
        self.descriptor(session="a-different-session")
        self.key()
        self.listen()
        with self.assertRaises(SessionGoneError):
            self.sender()(SESSION, "trusted message")

    def test_stale_descriptor_with_a_dead_socket_is_session_gone(self) -> None:
        # A SIGKILLed session leaves the descriptor *and* the socket file
        # behind, so file presence proves nothing; only the probe decides.
        self.descriptor()
        self.key()
        server = self.listen()
        server.close()
        socket_path = self.socket_dir / "4242.sock"
        self.assertTrue(socket_path.exists())
        with self.assertRaises(SessionGoneError):
            self.sender()(SESSION, "trusted message")

        # A descriptor whose socket file is gone entirely is equally gone.
        socket_path.unlink()
        with self.assertRaises(SessionGoneError):
            self.sender()(SESSION, "trusted message")

    def test_a_write_that_never_completes_becomes_an_ambiguous_delivery(self) -> None:
        self.descriptor()
        self.key()
        self.listen()
        sender = self.sender()
        # A peer that stops reading mid-write may already hold the auth line
        # and part of the message, so acceptance is genuinely unknown.
        for failure in (socket.timeout("slow peer"), TimeoutError, BrokenPipeError):
            with patch.object(socket.socket, "sendall", side_effect=failure):
                with self.assertRaises(TimeoutError):
                    sender(SESSION, "trusted message")

        with patch.object(socket.socket, "sendall", side_effect=BrokenPipeError):
            with self.assertRaises(AmbiguousDeliveryError):
                ClaudeUdsTransport(sender).send(
                    OrchestratorRef(TRANSPORT_NAME, SESSION),
                    CompletionNotice("ntf_stall", AGENT_ID, AgentStatus.SUCCEEDED),
                )

    def test_missing_or_unreadable_auth_token_is_a_clean_refusal(self) -> None:
        self.descriptor()
        self.listen()
        with self.assertRaises(DeliveryError) as caught:
            self.sender()(SESSION, "trusted message")
        self.assertNotIsInstance(caught.exception, AmbiguousDeliveryError)
        self.assertIn("auth key", str(caught.exception))

        unreadable = self.registry / f"4242.{'b' * 64}.key"
        unreadable.write_text("not json at all", encoding="utf-8")
        with self.assertRaises(DeliveryError):
            self.sender()(SESSION, "trusted message")

    def test_malformed_descriptors_are_skipped_not_fatal(self) -> None:
        (self.registry / "1.json").write_text("{ not json", encoding="utf-8")
        (self.registry / "2.json").write_text(json.dumps([1, 2]), encoding="utf-8")
        (self.registry / "3.json").write_text(
            json.dumps({"sessionId": SESSION, "pid": 3}), encoding="utf-8"
        )
        self.descriptor()
        self.key()
        server = self.listen()
        self.assertIsNone(self.sender()(SESSION, "trusted message"))
        server.close()
        self.assertEqual(len(server.lines), 2)

    def test_constructor_and_arguments_are_bounded_without_replacement_paths(self) -> None:
        with self.assertRaises(ValidationError):
            ClaudeSessionSender("relative/sessions")
        for kwargs in ({"timeout_seconds": 0}, {"probe_seconds": -1}, {"timeout_seconds": True}):
            with self.assertRaises(ValidationError):
                ClaudeSessionSender(self.registry, **kwargs)

        sender = self.sender()
        for session in ("", "x" * 513, "bad\x00session"):
            with self.assertRaises(ValidationError):
                sender(session, "trusted message")
        with self.assertRaises(ValidationError):
            sender(SESSION, "x" * 4097)
        for forbidden in ("start", "open", "create", "spawn", "launch"):
            self.assertFalse(any(forbidden in name for name in dir(sender)))


if __name__ == "__main__":
    unittest.main()
