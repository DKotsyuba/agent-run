import json
import os
import socket
import tempfile
import threading
import time
import unittest
from dataclasses import dataclass
from pathlib import Path

from agent_run.api_socket import ApiServer, MAX_LINE_BYTES, METHOD_NAMES
from agent_run.domain import AgentStatus
from agent_run.dispatch import TOOL_NAMES, TOOLS


class StubService:
    def __init__(self) -> None:
        self.calls = []
        self.call_threads = []
        self.created_in = threading.get_ident()

    def limits(self):
        self.calls.append("limits")
        self.call_threads.append(threading.get_ident())
        return {"ok": True}


@dataclass(frozen=True)
class _AgentView:
    status: AgentStatus


class _WaitService:
    def __init__(self, statuses, *, started=None):
        self.statuses = iter(statuses)
        self.last_status = None
        self.started = started

    def get(self, agent_id):
        if self.started is not None:
            self.started.set()
        self.last_status = next(self.statuses, self.last_status)
        return _AgentView(self.last_status)

    def answer(self, agent_id):
        return {
            "agent_id": agent_id,
            "status": self.last_status,
            "available": True,
            "content": "done",
        }


class _Factory:
    def __init__(self, dispatcher_service, wait_service):
        self.services = [dispatcher_service, wait_service]

    def __call__(self):
        return self.services.pop(0)


class ApiSocketTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tempdir.name) / "api.sock"
        self.service = StubService()
        self.server = ApiServer(self.path, lambda: self.service)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.addCleanup(self.close_server)

    def close_server(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()
        self.path.unlink(missing_ok=True)
        self.tempdir.cleanup()

    def replace_server(self, factory):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()
        self.path.unlink(missing_ok=True)
        self.server = ApiServer(self.path, factory)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def request(self, request):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(self.path))
            client.sendall(json.dumps(request).encode() + b"\n")
            return json.loads(client.makefile("rb").readline())

    def test_ping_and_tools_discovery(self):
        self.assertEqual(self.request({"jsonrpc": "2.0", "id": 1, "method": "ping"})["result"], {"ok": True})
        response = self.request({"jsonrpc": "2.0", "id": 2, "method": "tools"})
        self.assertEqual(response["result"], list(TOOLS))

    def test_successful_tool_round_trip(self):
        response = self.request({"jsonrpc": "2.0", "id": 1, "method": "limits", "params": {}})
        self.assertEqual(response["result"], {"ok": True})

    def test_unknown_method_and_validation_error(self):
        unknown = self.request({"jsonrpc": "2.0", "id": 1, "method": "missing"})
        self.assertEqual(unknown["error"]["code"], -32601)
        invalid = self.request({"jsonrpc": "2.0", "id": 2, "method": "status", "params": {}})
        self.assertEqual(invalid["error"]["code"], -32602)
        self.assertIn("missing arguments", invalid["error"]["message"])

    def test_notification_produces_no_reply(self):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(0.2)
            client.connect(str(self.path))
            client.sendall(b'{"jsonrpc":"2.0","method":"ping"}\n')
            with self.assertRaises(socket.timeout):
                client.recv(1)

    def test_oversized_line_is_rejected(self):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(1)
            client.connect(str(self.path))
            client.sendall(b"{" + b"x" * MAX_LINE_BYTES + b"}\n")
            response = json.loads(client.makefile("rb").readline())
        self.assertEqual(response["error"]["code"], -32700)

    def test_connections_have_isolated_sessions(self):
        def exchange(lines):
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(str(self.path))
                stream = client.makefile("rb")
                for line in lines:
                    client.sendall(json.dumps(line).encode() + b"\n")
                return [json.loads(stream.readline()) for _ in lines]

        first = exchange([
            {"jsonrpc": "2.0", "id": 1, "method": "fast", "params": {"runtime": "codex", "enabled": True}},
            {"jsonrpc": "2.0", "id": 2, "method": "fast", "params": {}},
        ])
        second = exchange([{"jsonrpc": "2.0", "id": 3, "method": "fast", "params": {}}])
        self.assertEqual(first[1]["result"], {"codex": True})
        self.assertEqual(second[0]["result"], {"codex": False})

    def test_socket_mode_and_stale_socket_replacement(self):
        mode = os.stat(self.path).st_mode & 0o777
        self.assertEqual(mode, 0o600)
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()
        stale = self.path
        replacement = ApiServer(stale, lambda: self.service)
        self.assertTrue(stale.exists())
        replacement.server_close()
        stale.unlink(missing_ok=True)

    def test_surface_is_dispatch_tools_plus_control_methods(self):
        self.assertEqual(
            METHOD_NAMES, TOOL_NAMES | {"tools", "ping", "wait", "workflow_wait"}
        )

    def test_wait_returns_the_answer_envelope_when_agent_finishes(self):
        wait_service = _WaitService([AgentStatus.RUNNING, AgentStatus.SUCCEEDED])
        self.replace_server(_Factory(StubService(), wait_service))
        response = self.request({
            "jsonrpc": "2.0", "id": 1, "method": "wait",
            "params": {"agent_id": "ag-20260826-120000-0123456789", "timeout_seconds": 2},
        })
        self.assertEqual(response["result"]["content"], "done")
        self.assertEqual(response["result"]["status"], "succeeded")

    def test_wait_timeout_is_a_normal_timed_out_result(self):
        wait_service = _WaitService([AgentStatus.RUNNING])
        self.replace_server(_Factory(StubService(), wait_service))
        response = self.request({
            "jsonrpc": "2.0", "id": 1, "method": "wait",
            "params": {"agent_id": "ag-20260826-120000-0123456789", "timeout_seconds": 0.05},
        })
        self.assertEqual(response["result"]["timed_out"], True)
        self.assertEqual(response["result"]["status"], "running")

    def test_wait_timeout_validation(self):
        for value in (0, -1, True, "1", float("inf")):
            with self.subTest(value=value):
                response = self.request({
                    "jsonrpc": "2.0", "id": 1, "method": "wait",
                    "params": {"agent_id": "ag-20260826-120000-0123456789", "timeout_seconds": value},
                })
                self.assertEqual(response["error"]["code"], -32602)

    def test_wait_does_not_block_other_connections(self):
        started = threading.Event()
        wait_service = _WaitService([AgentStatus.RUNNING], started=started)
        self.replace_server(_Factory(StubService(), wait_service))
        pending = {}
        waiter = threading.Thread(target=lambda: pending.setdefault("response", self.request({
            "jsonrpc": "2.0", "id": 1, "method": "wait",
            "params": {"agent_id": "ag-20260826-120000-0123456789", "timeout_seconds": 0.2},
        })))
        waiter.start()
        self.assertTrue(started.wait(1))
        began = time.monotonic()
        self.assertEqual(self.request({"jsonrpc": "2.0", "id": 2, "method": "ping"})["result"], {"ok": True})
        self.assertEqual(self.request({"jsonrpc": "2.0", "id": 3, "method": "limits", "params": {}})["result"], {"ok": True})
        self.assertLess(time.monotonic() - began, 0.5)
        waiter.join(timeout=2)
        self.assertTrue(pending["response"]["result"]["timed_out"])

    def test_all_dispatch_runs_on_the_service_owning_thread(self):
        # SQLite connections are thread-affine: every tool call must execute
        # on the dispatcher thread, never on per-connection handler threads.
        for request_id in (1, 2):
            self.request({"jsonrpc": "2.0", "id": request_id, "method": "limits", "params": {}})
        self.assertEqual(len(set(self.service.call_threads)), 1)
        self.assertNotIn(threading.get_ident(), self.service.call_threads)


if __name__ == "__main__":
    unittest.main()
