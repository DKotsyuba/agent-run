"""Focused protocol tests for the volatile Codex Desktop relay."""

import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from agent_run.delivery.base import AmbiguousDeliveryError, CompletionNotice
from agent_run.delivery.codex_desktop_relay import (
    CodexDesktopRelayClient,
    CodexDesktopRelayServer,
    _frame,
    _read_frame,
)
from agent_run.domain import AgentStatus, OrchestratorRef
from agent_run.errors import ValidationError

AGENT_ID = "ag-20260825-120000-0123456789"

def _bind_host(path: str) -> socket.socket:
    """Bind a listening Unix socket that acts as the Desktop host pipe."""

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen(1)
    return listener

def _serve_host(listener: socket.socket, success: bool) -> None:
    """Answer one host tools/list then tools/call over the framed protocol."""

    connection, _ = listener.accept()
    with connection:
        listed = _read_frame(connection)
        connection.sendall(_frame({
            "jsonrpc": "2.0",
            "id": listed["id"],
            "result": {
                "tools": [{"name": "send_message_to_thread", "namespace": "codex"}]
            },
        }))
        called = _read_frame(connection)
        connection.sendall(_frame({
            "jsonrpc": "2.0",
            "id": called["id"],
            "result": {"success": success, "contentItems": []},
        }))
    listener.close()

def _scripted_relay(path: Path, script) -> threading.Thread:
    """Serve one scripted relay connection from a listening Unix socket."""

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(path))
    listener.listen(1)

    def run() -> None:
        connection, _ = listener.accept()
        with connection:
            script(connection)
        listener.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread

class CodexDesktopRelayProtocolTests(unittest.TestCase):
    """Check the bounded frame and tool discovery contracts without a live host."""

    def test_frame_round_trip_uses_little_endian_json(self) -> None:
        """A relay frame preserves one JSON object across a local socket pair."""

        reader, writer = socket.socketpair()
        try:
            writer.sendall(_frame({"outcome": "accepted"}))
            self.assertEqual(_read_frame(reader), {"outcome": "accepted"})
        finally:
            reader.close()
            writer.close()

    def test_namespace_requires_the_exact_host_tool(self) -> None:
        """Discovery accepts only the advertised completion tool namespace, echoing the expected id."""

        expected_id = "list-1"
        self.assertEqual(
            CodexDesktopRelayServer._namespace(
                {
                    "jsonrpc": "2.0",
                    "id": expected_id,
                    "result": {"tools": [{"name": "send_message_to_thread", "namespace": "codex"}]},
                },
                expected_id,
            ),
            "codex",
        )
        self.assertIsNone(
            CodexDesktopRelayServer._namespace(
                {"jsonrpc": "2.0", "id": expected_id, "result": {"tools": []}},
                expected_id,
            )
        )
        self.assertIsNone(
            CodexDesktopRelayServer._namespace(
                {
                    "jsonrpc": "2.0",
                    "id": "mismatched-id",
                    "result": {"tools": [{"name": "send_message_to_thread", "namespace": "codex"}]},
                },
                expected_id,
            )
        )
        self.assertIsNone(CodexDesktopRelayServer._namespace("not a dict", expected_id))
        self.assertIsNone(
            CodexDesktopRelayServer._namespace(
                {"jsonrpc": "2.0", "id": expected_id, "result": "not a dict"},
                expected_id,
            )
        )


class CodexDesktopRelayServerTests(unittest.TestCase):
    """The relay server starts only when the host pipe path is injected."""

    def test_start_from_environment_gates_on_the_pipe_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            pipe = str(home / "host.sock")
            with mock.patch.dict(os.environ, {}, clear=True):
                self.assertIsNone(CodexDesktopRelayServer.start_from_environment(home))
            with mock.patch.dict(
                os.environ, {"CODEX_APP_TOOLS_PIPE_PATH": pipe}, clear=True
            ), mock.patch.object(
                CodexDesktopRelayServer, "start"
            ) as start, mock.patch.object(CodexDesktopRelayServer, "close"):
                server = CodexDesktopRelayServer.start_from_environment(home)
                self.assertIsInstance(server, CodexDesktopRelayServer)
                self.assertEqual(server._host_pipe_path, pipe)
                start.assert_called_once_with()

class CodexDesktopRelayDeliverTests(unittest.TestCase):
    """The relay validates its own request and rebuilds the trusted notice."""

    def setUp(self) -> None:
        self.server = CodexDesktopRelayServer(
            Path(tempfile.gettempdir()), "/nonexistent/host.sock"
        )

    def tearDown(self) -> None:
        self.server._socket.close()

    def request(self, **overrides) -> dict:
        base = {
            "version": 1,
            "op": "completion",
            "thread_id": "session-1",
            "notification_id": "ntf_relay",
            "agent_id": AGENT_ID,
            "status": "succeeded",
        }
        base.update(overrides)
        return base

    def test_deliver_rejects_a_non_terminal_status(self) -> None:
        """A valid but active status never reaches the host pipe."""

        with self.assertRaises(ValidationError):
            self.server._deliver(self.request(status="running"))

    def test_deliver_rejects_an_unknown_status(self) -> None:
        """An unrecognised status is rejected as a bad request."""

        with self.assertRaises(ValidationError):
            self.server._deliver(self.request(status="bogus"))

    def test_deliver_rejects_an_unknown_agent_id(self) -> None:
        """A malformed agent id fails the trusted notice before any host call."""

        with self.assertRaises(ValidationError):
            self.server._deliver(self.request(agent_id="not-an-agent"))

class CodexDesktopRelayClientTests(unittest.TestCase):
    """The client classifies each relay attempt and never crosses the fallback."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.home = Path(self.temporary.name)
        self.notice = CompletionNotice("ntf_relay", AGENT_ID, AgentStatus.SUCCEEDED)
        self.target = OrchestratorRef("codex_queue", "session-1", "turn-1")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_acceptance_returns_true_with_static_evidence(self) -> None:
        path = self.home / "ar-cdx-accept.sock"
        thread = _scripted_relay(path, lambda conn: (
            _read_frame(conn),
            conn.sendall(_frame({"outcome": "accepted"})),
        ))
        try:
            client = CodexDesktopRelayClient(self.home)
            self.assertTrue(client.send(self.target, self.notice))
            self.assertIsNotNone(client.last_evidence)
            self.assertEqual(client.last_evidence.classifier, "relay_accepted")
            self.assertEqual(client.last_evidence.executable, "codex_desktop_relay")
            self.assertEqual(client.last_evidence.argv_shape, ("relay",))
        finally:
            thread.join(timeout=2.0)

    def test_explicit_rejection_falls_through_to_the_queue(self) -> None:
        path = self.home / "ar-cdx-reject.sock"
        thread = _scripted_relay(path, lambda conn: (
            _read_frame(conn),
            conn.sendall(_frame({"outcome": "rejected"})),
        ))
        try:
            client = CodexDesktopRelayClient(self.home)
            self.assertFalse(client.send(self.target, self.notice))
        finally:
            thread.join(timeout=2.0)

    def test_post_write_eof_is_ambiguous_and_never_falls_back(self) -> None:
        path = self.home / "ar-cdx-eof.sock"
        thread = _scripted_relay(path, lambda conn: _read_frame(conn))
        try:
            client = CodexDesktopRelayClient(self.home)
            with self.assertRaises(AmbiguousDeliveryError):
                client.send(self.target, self.notice)
            self.assertIsNotNone(client.last_evidence)
            self.assertEqual(client.last_evidence.classifier, "relay_ambiguous")
        finally:
            thread.join(timeout=2.0)

    def test_unreachable_socket_is_skipped_and_cleaned_up(self) -> None:
        stale = self.home / "ar-cdx-stale.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(stale))
        listener.close()
        client = CodexDesktopRelayClient(self.home)
        self.assertFalse(client.send(self.target, self.notice))
        self.assertFalse(stale.exists())

class CodexDesktopRelayEndToEndTests(unittest.TestCase):
    """One real relay server backed by a scripted host, driven by the client."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.home = Path(self.temporary.name)
        self.notice = CompletionNotice("ntf_relay", AGENT_ID, AgentStatus.SUCCEEDED)
        self.target = OrchestratorRef("codex_queue", "session-1", "turn-1")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run(self, success: bool) -> bool:
        host_path = str(self.home / "host.sock")
        listener = _bind_host(host_path)
        host_thread = threading.Thread(
            target=_serve_host, args=(listener, success), daemon=True
        )
        host_thread.start()
        relay = CodexDesktopRelayServer(self.home, host_path)
        relay.start()
        try:
            return CodexDesktopRelayClient(self.home).send(self.target, self.notice)
        finally:
            relay.close()
            host_thread.join(timeout=2.0)

    def test_live_relay_accepts_and_suppresses_the_queue(self) -> None:
        self.assertTrue(self._run(True))

    def test_host_failure_is_an_explicit_rejection_for_queue_fallback(self) -> None:
        self.assertFalse(self._run(False))

if __name__ == "__main__":
    unittest.main()
