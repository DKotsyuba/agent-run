import json
import os
import socket
import tempfile
import threading
import time
import unittest
from dataclasses import dataclass
from pathlib import Path

from agent_run.api_socket import (
    ApiServer,
    MAX_LINE_BYTES,
    METHOD_NAMES,
    _Dispatcher,
    _DispatcherClosed,
)
from agent_run.domain import AgentStatus
from agent_run.dispatch import TOOL_NAMES, TOOLS, Session
from agent_run.errors import ValidationError


class StubService:
    def __init__(self) -> None:
        self.calls = []
        self.call_threads = []
        self.created_in = threading.get_ident()

    def limits(self):
        self.calls.append("limits")
        self.call_threads.append(threading.get_ident())
        return {"ok": True}

    def cancel(self, agent_id):
        """Return an immediate durable-control substitute for the agent."""

        return {"agent_id": agent_id, "status": "cancelling"}

    def list_orchestrators(self, *, limit=100):
        """Return a deterministic response used to prove socket dispatch."""

        self.calls.append(("list_orchestrators", limit))
        return {"limit": limit}

    def resolve_account(self, runtime, account):
        """Return the stub's personal label for a str/None account or reject it."""
        if account not in {None, "personal"}:
            raise ValueError("unknown account")
        return account or "personal"


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
    """Provide two lane services followed by one isolated wait service."""

    def __init__(self, dispatcher_service, wait_service) -> None:
        """Retain the supplied lane substitute and dedicated wait substitute."""

        self.services = [dispatcher_service, dispatcher_service, wait_service]

    def __call__(self):
        """Return the next service in ApiServer construction/call order."""

        return self.services.pop(0)


class DispatcherShutdownTests(unittest.TestCase):
    """Deterministic owner-queue shutdown races without a Unix socket."""

    def test_close_and_submit_cannot_cross_the_shutdown_sentinel(self) -> None:
        """A submit queued behind close fails immediately rather than timing out."""

        dispatcher = _Dispatcher(
            lambda: StubService(), max_pending=1, request_timeout=1
        )
        errors = []
        dispatcher._close_lock.acquire()
        closer = threading.Thread(target=lambda: dispatcher.close(1))
        caller = threading.Thread(
            target=lambda: self._capture_call_error(dispatcher, errors)
        )
        closer.start()
        time.sleep(0.01)
        caller.start()
        dispatcher._close_lock.release()
        closer.join(timeout=1)
        caller.join(timeout=1)
        self.assertFalse(closer.is_alive() or caller.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], _DispatcherClosed)

    @staticmethod
    def _capture_call_error(dispatcher, errors) -> None:
        """Append the exception from one call racing dispatcher shutdown."""

        try:
            dispatcher.call("limits", {}, Session())
        except BaseException as error:
            errors.append(error)


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

    def replace_server(self, factory, **options):
        """Replace the active server with ``factory`` and constructor options."""

        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()
        self.path.unlink(missing_ok=True)
        self.server = ApiServer(self.path, factory, **options)
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

    def test_list_orchestrators_round_trip(self):
        response = self.request({
            "jsonrpc": "2.0", "id": 1, "method": "list_orchestrators", "params": {"limit": 7},
        })
        self.assertEqual(response["result"], {"limit": 7})

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

    def test_partial_frame_hits_idle_deadline_and_releases_connection(self) -> None:
        """A client without a newline cannot retain a handler indefinitely."""

        self.replace_server(lambda: StubService(), idle_timeout=0.05)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(1)
            client.connect(str(self.path))
            client.sendall(b'{"jsonrpc":"2.0"')
            self.assertEqual(client.recv(1), b"")

    def test_connection_limit_returns_explicit_overload(self) -> None:
        """A partial first client makes the bounded second slot reject clearly."""

        self.replace_server(
            lambda: StubService(), max_connections=1, idle_timeout=1
        )
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as blocked:
            blocked.connect(str(self.path))
            blocked.sendall(b"{")
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(1)
                client.connect(str(self.path))
                response = json.loads(client.makefile("rb").readline())
        self.assertEqual(response["error"]["code"], -32001)

    def test_control_slot_survives_saturated_long_read_connections(self) -> None:
        """Reserved bounded capacity admits cancel while ordinary reads are full."""

        self.replace_server(
            lambda: self.service, max_connections=4, idle_timeout=2
        )
        blocked = []
        try:
            for _ in range(3):
                client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                client.connect(str(self.path))
                client.sendall(b"{")
                blocked.append(client)
            response = self.request(
                {
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "cancel",
                    "params": {"agent_id": "ag-20260826-120000-0123456789"},
                }
            )
            self.assertEqual(response["result"]["status"], "cancelling")
        finally:
            for client in blocked:
                client.close()

    def test_slow_bytes_cannot_extend_reserved_first_frame_deadline(self) -> None:
        """A slowloris loses the reserve, letting a later cancel finish boundedly."""

        self.replace_server(
            lambda: self.service, max_connections=2, idle_timeout=2
        )
        ordinary = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        slow = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        ordinary.connect(str(self.path))
        ordinary.sendall(b"{")
        slow.connect(str(self.path))

        def drip() -> None:
            """Send bytes below the per-recv timeout until the server closes."""

            for _ in range(10):
                try:
                    slow.sendall(b"{")
                except OSError:
                    return
                time.sleep(0.1)

        worker = threading.Thread(target=drip)
        worker.start()
        time.sleep(0.7)
        started = time.monotonic()
        response = self.request(
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "cancel",
                "params": {"agent_id": "ag-20260826-120000-0123456789"},
            }
        )
        elapsed = time.monotonic() - started
        ordinary.close()
        slow.close()
        worker.join(timeout=1)
        self.assertEqual(response["result"]["status"], "cancelling")
        self.assertLess(elapsed, 1.0)

    def test_live_slow_socket_is_never_reclaimed_as_stale(self) -> None:
        """A successful connect proves ownership even when no ping reply arrives."""

        path = Path(self.tempdir.name) / "slow-owner.sock"
        owner = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        owner.bind(str(path))
        owner.listen()
        accepted = threading.Event()

        def hold_connection() -> None:
            """Accept the ownership probe and deliberately send no response."""

            connection, _ = owner.accept()
            accepted.set()
            with connection:
                time.sleep(0.2)

        worker = threading.Thread(target=hold_connection, daemon=True)
        worker.start()
        try:
            with self.assertRaisesRegex(ValidationError, "already in use"):
                ApiServer(path, lambda: StubService())
            self.assertTrue(accepted.wait(1))
            self.assertTrue(path.exists())
        finally:
            owner.close()
            worker.join(timeout=1)
            path.unlink(missing_ok=True)

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

    def test_account_fast_toggle_round_trip(self):
        """One socket session can set and query an account-specific override."""
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(self.path))
            stream = client.makefile("rb")
            for request in (
                {"jsonrpc": "2.0", "id": 1, "method": "fast", "params": {"runtime": "codex", "account": "personal", "enabled": True}},
                {"jsonrpc": "2.0", "id": 2, "method": "fast", "params": {}},
            ):
                client.sendall(json.dumps(request).encode() + b"\n")
            self.assertEqual(json.loads(stream.readline())["result"], {"codex": False, "accounts": {"personal": True}})
            self.assertEqual(json.loads(stream.readline()), {
                "jsonrpc": "2.0", "id": 2,
                "result": {"codex": False, "accounts": {"personal": True}},
            })

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

    def test_old_server_release_does_not_unlink_replacement_socket(self):
        path = Path(self.tempdir.name) / "ownership.sock"
        old = ApiServer(path, lambda: StubService())
        replacement = None
        try:
            self.assertTrue(old.release_socket_path())
            replacement = ApiServer(path, lambda: StubService())
            self.assertFalse(old.release_socket_path())
            self.assertTrue(path.exists())
        finally:
            old.server_close()
            if replacement is not None:
                replacement.server_close()
                replacement.release_socket_path()

    def test_surface_is_dispatch_tools_plus_control_methods(self):
        self.assertEqual(
            METHOD_NAMES, TOOL_NAMES | {"tools", "ping", "wait"}
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

    def test_request_deadline_and_queue_overload_are_explicit(self) -> None:
        """One running and one queued read bound waiting time and capacity."""

        entered = threading.Event()
        release = threading.Event()

        class SlowService(StubService):
            """Hold every limits call until the test releases the read lane."""

            def limits(self):
                """Expose one deterministic slow read request."""

                entered.set()
                release.wait(2)
                return {"ok": True}

        service = SlowService()
        self.replace_server(
            lambda: service, max_pending_requests=1, request_timeout=0.15
        )
        responses: list[dict] = []

        def request_limits(request_id: int) -> None:
            """Record one limits response from a separate connection."""

            responses.append(
                self.request(
                    {"jsonrpc": "2.0", "id": request_id, "method": "limits"}
                )
            )

        first = threading.Thread(target=request_limits, args=(1,))
        second = threading.Thread(target=request_limits, args=(2,))
        first.start()
        self.assertTrue(entered.wait(1))
        second.start()
        deadline = time.monotonic() + 1
        while self.server._dispatchers[1]._queue.qsize() != 1:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.005)
        overloaded = self.request(
            {"jsonrpc": "2.0", "id": 3, "method": "limits"}
        )
        self.assertEqual(overloaded["error"]["code"], -32001)
        first.join(timeout=1)
        second.join(timeout=1)
        self.assertEqual(
            sorted(response["error"]["code"] for response in responses),
            [-32002, -32002],
        )
        release.set()

    def test_slow_models_lane_does_not_delay_durable_cancel(self) -> None:
        """Cancel stays responsive while model probing occupies the read owner."""

        entered = threading.Event()
        release = threading.Event()

        class LaneService(StubService):
            """Expose one slow read and one fast durable control operation."""

            def models(self):
                """Block model discovery until the latency sample completes."""

                entered.set()
                release.wait(2)
                return {}

            def cancel(self, agent_id):
                """Return an immediate cancellation projection for the agent."""

                return {"agent_id": agent_id, "status": "cancelling"}

        service = LaneService()
        self.replace_server(lambda: service, request_timeout=2)
        model_response: list[dict] = []
        model_worker = threading.Thread(
            target=lambda: model_response.append(
                self.request({"jsonrpc": "2.0", "id": 1, "method": "models"})
            )
        )
        model_worker.start()
        self.assertTrue(entered.wait(1))
        durations = []
        for request_id in range(2, 22):
            started = time.monotonic()
            response = self.request(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "cancel",
                    "params": {"agent_id": "ag-20260826-120000-0123456789"},
                }
            )
            durations.append(time.monotonic() - started)
            self.assertEqual(response["result"]["status"], "cancelling")
        p95 = sorted(durations)[18]
        self.assertLess(p95, 1.0)
        release.set()
        model_worker.join(timeout=2)
        self.assertEqual(model_response[0]["result"], {})

    def test_shutdown_closes_each_owner_service_once_in_its_thread(self) -> None:
        """Shutdown closes both thread-affine services exactly once."""

        services = []

        class ClosingService(StubService):
            """Record construction and close ownership for one dispatcher lane."""

            def __init__(self) -> None:
                """Capture the dispatcher thread that owns this service."""

                super().__init__()
                self.created_in = threading.get_ident()
                self.closed_in: list[int] = []

            def close(self) -> None:
                """Record the sole owner-context close call."""

                self.closed_in.append(threading.get_ident())

        def factory() -> ClosingService:
            """Create and retain one distinct lane service."""

            service = ClosingService()
            services.append(service)
            return service

        self.replace_server(factory)
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()

        self.assertEqual(len(services), 2)
        self.assertTrue(
            all(service.closed_in == [service.created_in] for service in services)
        )

    def test_second_owner_boot_failure_closes_the_first_owner(self) -> None:
        """A partial two-lane startup closes the service already constructed."""

        closed = threading.Event()
        calls = 0

        class FirstService(StubService):
            """Record owner-context cleanup after the second factory call fails."""

            def close(self) -> None:
                """Expose cleanup of the first successfully created service."""

                closed.set()

        def factory():
            """Return one service, then fail the second owner construction."""

            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("second owner failed")
            return FirstService()

        path = Path(self.tempdir.name) / "boot-failure.sock"
        with self.assertRaisesRegex(RuntimeError, "second owner failed"):
            ApiServer(path, factory)
        self.assertTrue(closed.wait(1))

    def test_all_dispatch_runs_on_the_service_owning_thread(self):
        # SQLite connections are thread-affine: every tool call must execute
        # on the dispatcher thread, never on per-connection handler threads.
        for request_id in (1, 2):
            self.request({"jsonrpc": "2.0", "id": request_id, "method": "limits", "params": {}})
        self.assertEqual(len(set(self.service.call_threads)), 1)
        self.assertNotIn(threading.get_ident(), self.service.call_threads)


if __name__ == "__main__":
    unittest.main()
