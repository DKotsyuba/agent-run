import json
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_run.broker_client import BrokerClient
from agent_run.domain import StartRequest
from agent_run.errors import AgentRunError, BrokerUnavailable, ValidationError


class FakeSocketApi:
    def __init__(self, path: Path, responses=None):
        self.path = path
        self.responses = responses or (lambda request: {"jsonrpc": "2.0", "id": request["id"], "result": {"ok": True}})
        self.server = None
        self.thread = None
        self.clients = []
        self.stop = False

    def start(self):
        self.stop = False
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(str(self.path))
        self.server.listen()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        assert self.server is not None
        while True:
            try:
                client, _ = self.server.accept()
            except (AttributeError, OSError):
                return
            self.clients.append(client)
            with client:
                stream = client.makefile("rb")
                for line in stream:
                    request = json.loads(line)
                    response = self.responses(request)
                    client.sendall(json.dumps(response).encode() + b"\n")

    def close(self):
        for client in self.clients:
            client.close()
        self.clients.clear()
        if self.server is not None:
            self.server.close()
            self.server = None
        if self.thread is not None:
            self.thread.join(timeout=1)
            self.thread = None
        self.path.unlink(missing_ok=True)


class BrokerClientTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tempdir.name) / "api.sock"
        self.addCleanup(self.tempdir.cleanup)

    def test_round_trip_and_monotonic_ids(self):
        seen = []
        server = FakeSocketApi(self.path, lambda request: (seen.append(request) or {
            "jsonrpc": "2.0", "id": request["id"], "result": {"value": request["params"]}
        }))
        server.start()
        self.addCleanup(server.close)
        client = BrokerClient(self.path)
        self.addCleanup(client.close)
        self.assertEqual(client.call("limits", {"x": 1}), {"value": {"x": 1}})
        self.assertEqual(client.call("status", {"x": 2}), {"value": {"x": 2}})
        self.assertEqual([item["id"] for item in seen], [1, 2])

    def test_start_serializes_request_and_rejects_malformed_results(self):
        """Serialize starts and reject invalid requests or broker results."""

        seen = []

        def respond(request):
            """Return one valid start result, then one malformed result."""

            seen.append(request)
            result = {"agent_id": "ag-test", "created": True} if len(seen) == 1 else {}
            return {"jsonrpc": "2.0", "id": request["id"], "result": result}

        server = FakeSocketApi(self.path, respond)
        server.start()
        self.addCleanup(server.close)
        client = BrokerClient(self.path)
        self.addCleanup(client.close)
        request = StartRequest("codex", "model", "review", "task", Path(self.tempdir.name))

        result = client.start(request)

        self.assertEqual((result.agent_id, result.created), ("ag-test", True))
        self.assertEqual(seen[0]["method"], "start")
        self.assertEqual(seen[0]["params"]["workdir"], str(request.workdir))
        with self.assertRaisesRegex(AgentRunError, "invalid start result"):
            client.start(request)
        with self.assertRaisesRegex(ValidationError, "request must be a StartRequest"):
            client.start(object())  # type: ignore[arg-type]

    def test_reconnects_once_after_server_restart(self):
        first = FakeSocketApi(self.path)
        first.start()
        client = BrokerClient(self.path)
        self.addCleanup(client.close)
        self.assertTrue(client.ping())
        client._socket.shutdown(socket.SHUT_RDWR)
        first.close()
        second = FakeSocketApi(self.path, lambda request: {"jsonrpc": "2.0", "id": request["id"], "result": {"restarted": True}})
        second.start()
        self.addCleanup(second.close)
        self.assertEqual(client.call("limits"), {"restarted": True})

    def test_unavailable_has_actionable_message(self):
        with self.assertRaisesRegex(BrokerUnavailable, "agent-run broker is not running"):
            BrokerClient(self.path).call("limits")

    def test_invalid_deadlines_are_rejected_before_socket_creation(self) -> None:
        """Nonpositive and nonfinite client deadlines never reach transport."""

        for value in (0, -1, True, "1", float("inf"), float("nan")):
            with self.subTest(value=value), patch(
                "agent_run.broker_client.socket.socket"
            ) as socket_factory:
                with self.assertRaisesRegex(ValidationError, "positive and finite"):
                    BrokerClient(self.path).call("limits", timeout=value)
                socket_factory.assert_not_called()

    def test_validation_error_mapping(self):
        server = FakeSocketApi(self.path, lambda request: {
            "jsonrpc": "2.0", "id": request["id"], "error": {"code": -32602, "message": "bad params"}
        })
        server.start()
        self.addCleanup(server.close)
        with self.assertRaisesRegex(ValidationError, "bad params"):
            BrokerClient(self.path).call("limits")

    def test_agent_error_mapping_preserves_data_code(self):
        server = FakeSocketApi(self.path, lambda request: {
            "jsonrpc": "2.0", "id": request["id"], "error": {
                "code": -32000, "message": "domain failure",
                "data": {"code": "AuthError", "message": "domain failure"},
            }
        })
        server.start()
        self.addCleanup(server.close)
        with self.assertRaises(AgentRunError) as context:
            BrokerClient(self.path).call("limits")
        self.assertEqual(str(context.exception), "domain failure")
        self.assertEqual(context.exception.broker_error_code, "AuthError")

    def test_abort_interrupts_each_connect_without_retry_or_worker_leak(self):
        """Cancel repeated blocking connects and release every socket and worker."""

        for _ in range(8):
            entered = threading.Event()
            closed = threading.Event()
            sockets = []
            failures = []

            class ConnectingSocket:
                """Controlled socket whose connect blocks until abort closes it."""

                def __init__(self, *_args):
                    """Record this single connection attempt."""
                    sockets.append(self)

                def settimeout(self, _timeout):
                    """Accept the client timeout without changing controlled timing."""

                def connect(self, _path):
                    """Block like an in-progress connect until another thread aborts it."""
                    entered.set()
                    if not closed.wait(1):
                        raise TimeoutError("abort did not interrupt connect")
                    raise OSError("connect interrupted")

                def shutdown(self, _how):
                    """Wake the controlled connect operation."""
                    closed.set()

                def close(self):
                    """Release the controlled socket and wake its connect operation."""
                    closed.set()

            client = BrokerClient(self.path)

            def call():
                """Capture the terminal cancellation raised by the client worker."""
                try:
                    client.call("limits")
                except BaseException as error:
                    failures.append(error)

            with patch("agent_run.broker_client.socket.socket", ConnectingSocket):
                worker = threading.Thread(target=call)
                worker.start()
                self.assertTrue(entered.wait(1))
                client.abort()
                worker.join(timeout=1)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(sockets), 1, "an aborted connect must not retry")
            self.assertTrue(closed.is_set())
            self.assertEqual(len(failures), 1)
            self.assertIsInstance(failures[0], ConnectionError)

        entered = threading.Event()
        closed = threading.Event()
        sockets = []
        client = BrokerClient(self.path)
        client.abort()
        with patch("agent_run.broker_client.socket.socket", ConnectingSocket):
            with self.assertRaisesRegex(ConnectionError, "cancelled"):
                client.call("limits")
        self.assertFalse(entered.is_set(), "pre-cancelled clients must not call connect")
        self.assertEqual(len(sockets), 1)
        self.assertTrue(closed.is_set())


if __name__ == "__main__":
    unittest.main()
