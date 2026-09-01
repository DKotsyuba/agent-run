import json
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from agent_run.broker_client import BrokerClient
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


if __name__ == "__main__":
    unittest.main()
