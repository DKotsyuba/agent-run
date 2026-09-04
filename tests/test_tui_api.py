"""Tests for the standalone TUI JSON-RPC client."""

from __future__ import annotations

import json
import socket
import tempfile
import threading
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from agent_run_tui.api import ApiClient, ApiError, ApiUnavailable


#: Reply script sentinel: read the request, then hold the connection open
#: without answering until the server is released.
HOLD = object()


class RpcServer:
    """Serve newline-delimited JSON-RPC replies from a temporary Unix socket.

    Each entry of ``replies`` is consumed by one accepted connection: a mapping
    is answered as a JSON-RPC reply with the request's id, ``bytes`` are sent
    verbatim, :data:`HOLD` keeps the connection open until ``released`` is set
    or two seconds pass, and ``None`` closes it without answering.
    """

    def __init__(self, path: Path, replies: list[object]) -> None:
        """Bind ``path`` and consume one configured reply per received request."""
        self.path, self.replies, self.requests = path, replies, []
        self.ready = threading.Event()
        self.released = threading.Event()
        self.thread = threading.Thread(target=self._serve, daemon=True)

    def start(self) -> None:
        """Start the server and wait until its socket accepts clients."""
        self.thread.start()
        self.ready.wait(2)

    def join(self) -> None:
        """Release any held connection and wait for the finite reply script."""
        self.released.set()
        self.thread.join(2)

    def _serve(self) -> None:
        """Accept scripted clients, recording each decoded request before replying."""
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(self.path))
            server.listen()
            self.ready.set()
            for reply in self.replies:
                with server.accept()[0] as connection:
                    request = json.loads(connection.makefile("rb").readline())
                    self.requests.append(request)
                    if reply is HOLD:
                        self.released.wait(2)
                    elif isinstance(reply, bytes):
                        connection.sendall(reply)
                    elif reply is not None:
                        reply = dict(reply)
                        reply["id"] = request["id"]
                        connection.sendall(json.dumps(reply).encode() + b"\n")


class ApiClientTests(unittest.TestCase):
    """Exercise client framing, errors, availability, and reconnection."""

    def test_round_trip_and_reconnect(self) -> None:
        """A dropped first connection is retried with the same request id."""
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            server = RpcServer(Path(directory) / "api.sock", [None, {"jsonrpc": "2.0", "result": {"ok": True}}])
            server.start()
            with self.assertRaises(ApiUnavailable):
                ApiClient(server.path).call("ping", {"value": 1})
            server.join()
            self.assertEqual(server.requests, [{"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {"value": 1}}])

    def test_rpc_errors_and_missing_socket(self) -> None:
        """Server errors retain their code/data and absence names the recovery command."""
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            path = Path(directory) / "api.sock"
            with self.assertRaises(ApiUnavailable) as missing:
                ApiClient(path).call("ping")
            self.assertIn("agent-run api serve", str(missing.exception))
            server = RpcServer(path, [{"jsonrpc": "2.0", "error": {"code": -32000, "message": "bad", "data": {"code": "broken"}}}])
            server.start()
            with self.assertRaises(ApiError) as raised:
                ApiClient(path).call("ping")
            self.assertEqual((raised.exception.code, raised.exception.data), (-32000, {"code": "broken"}))
            server.join()

    def test_malformed_replies_are_api_errors(self) -> None:
        """A resultless object, a JSON array, and non-JSON text all fail as ApiError."""
        cases = (("missing result", {"jsonrpc": "2.0"}), ("json array", b"[1, 2]\n"),
                 ("not json", b"nonsense\n"))
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            for index, (name, reply) in enumerate(cases):
                with self.subTest(reply=name):
                    server = RpcServer(Path(directory) / f"api{index}.sock", [reply])
                    server.start()
                    with self.assertRaises(ApiError):
                        ApiClient(server.path).call("ping")
                    server.join()
                    self.assertEqual(len(server.requests), 1)

    def test_read_timeout_after_a_send_is_not_resent(self) -> None:
        """A reply that never arrives fails without repeating the delivered request."""
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            server = RpcServer(Path(directory) / "api.sock", [HOLD])
            server.start()
            with self.assertRaises(ApiUnavailable) as raised:
                ApiClient(server.path, timeout=0.25).call("ping")
            self.assertIn("agent-run api serve", str(raised.exception))
            server.join()
            self.assertEqual(len(server.requests), 1)


if __name__ == "__main__":
    unittest.main()
