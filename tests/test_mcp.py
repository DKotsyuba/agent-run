"""Integration coverage for the official MCP SDK stdio transport."""

from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from agent_run.broker_client import MAX_LINE_BYTES, BrokerClient
from agent_run.mcp import _BoundedInput, serve


_ROOT = Path(__file__).resolve().parents[1]
_SERVER = """
from agent_run.errors import AgentRunError
from agent_run.mcp import serve

class Broker:
    \"\"\"Controlled resident-broker fixture for one SDK stdio process.\"\"\"

    def call(self, method, params=None, timeout=600.0):
        \"\"\"Return one deterministic result or one typed domain failure.\"\"\"
        if method == "limits":
            raise AgentRunError("controlled broker failure")
        return {"method": method, "arguments": params or {}}

serve(lambda: Broker())
"""


class McpSdkTests(unittest.TestCase):
    """Exercise a real official MCP client against the official SDK stdio server."""

    def test_official_client_negotiates_lists_and_calls_over_stdio(self) -> None:
        """Verify the SDK owns handshake and wire parsing while the broker stays thin."""

        async def exercise() -> None:
            """Connect one official client and assert discovery plus structured tool output."""
            environment = {**os.environ, "PYTHONPATH": str(_ROOT / "src")}
            parameters = StdioServerParameters(
                command=sys.executable,
                args=["-c", _SERVER],
                env=environment,
                cwd=_ROOT,
            )
            with anyio.fail_after(10):
                async with stdio_client(parameters) as (read_stream, write_stream):
                    async with ClientSession(read_stream, write_stream) as session:
                        initialized = await session.initialize()
                        self.assertEqual(initialized.server_info.name, "agent-run")
                        tools = await session.list_tools()
                        self.assertIn("models", {tool.name for tool in tools.tools})
                        result = await session.call_tool(
                            "models", {"orchestrator": {"id": "root", "name": "root"}}
                        )
            self.assertFalse(result.is_error)
            self.assertEqual(result.structured_content["method"], "models")
            self.assertEqual(
                result.structured_content["arguments"]["orchestrator"]["id"], "root"
            )

        anyio.run(exercise)

    def test_domain_error_is_an_official_tool_error_result(self) -> None:
        """Keep broker domain failures in MCP tool results instead of raw JSON-RPC errors."""

        async def exercise() -> None:
            """Call the controlled failing broker method through the official SDK client."""
            environment = {**os.environ, "PYTHONPATH": str(_ROOT / "src")}
            parameters = StdioServerParameters(
                command=sys.executable,
                args=["-c", _SERVER],
                env=environment,
                cwd=_ROOT,
            )
            with anyio.fail_after(10):
                async with stdio_client(parameters) as (read_stream, write_stream):
                    async with ClientSession(read_stream, write_stream) as session:
                        await session.initialize()
                        result = await session.call_tool("limits")
            self.assertTrue(result.is_error)
            self.assertEqual(result.structured_content["error"]["message"], "controlled broker failure")

        anyio.run(exercise)

    def test_cancelled_caller_does_not_cancel_admitted_broker_call(self) -> None:
        """Abort the real client worker while server-owned admitted work continues."""

        temporary = tempfile.TemporaryDirectory()
        socket_path = Path(temporary.name) / "broker.sock"
        done_path = Path(temporary.name) / "worker.done"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen()
        listener.settimeout(0.05)
        admitted = threading.Event()
        release = threading.Event()
        stop = threading.Event()
        connections = []
        handlers = []

        def handle(client: socket.socket) -> None:
            """Own admitted work until release even after its caller disconnects."""
            with client:
                request = json.loads(client.makefile("rb").readline())
                admitted.set()
                if not release.wait(3):
                    return
                response = {"jsonrpc": "2.0", "id": request["id"], "result": {"ok": True}}
                try:
                    client.sendall(json.dumps(response).encode() + b"\n")
                except OSError:
                    pass

        def accept() -> None:
            """Accept every client connection so a forbidden retry is observable."""
            while not stop.is_set():
                try:
                    client, _ = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    return
                connections.append(client)
                handler = threading.Thread(target=handle, args=(client,), daemon=True)
                handlers.append(handler)
                handler.start()

        accept_thread = threading.Thread(target=accept, daemon=True)
        accept_thread.start()
        server_code = """
import os
from pathlib import Path
from agent_run.broker_client import BrokerClient
from agent_run.mcp import serve

class ObservedBrokerClient(BrokerClient):
    \"\"\"Signal when the actual callback worker has left BrokerClient.call.\"\"\"

    def call(self, method, params=None, timeout=600.0):
        \"\"\"Run the real broker call and publish its worker-finally marker.\"\"\"
        try:
            return super().call(method, params, timeout)
        finally:
            Path(os.environ[\"AGENT_RUN_TEST_DONE\"]).touch()

serve(lambda: ObservedBrokerClient(Path(os.environ[\"AGENT_RUN_TEST_SOCKET\"])))
"""

        async def exercise() -> None:
            """Cancel one official MCP request and observe real broker resources."""
            environment = {
                **os.environ,
                "PYTHONPATH": str(_ROOT / "src"),
                "AGENT_RUN_TEST_SOCKET": str(socket_path),
                "AGENT_RUN_TEST_DONE": str(done_path),
            }
            parameters = StdioServerParameters(
                command=sys.executable,
                args=["-c", server_code],
                env=environment,
                cwd=_ROOT,
            )
            with anyio.fail_after(10):
                async with stdio_client(parameters) as (read_stream, write_stream):
                    async with ClientSession(read_stream, write_stream) as session:
                        await session.initialize()
                        scope = anyio.CancelScope()
                        finished = anyio.Event()

                        async def call() -> None:
                            """Run one cancellable request through the official client."""
                            try:
                                with scope:
                                    await session.call_tool("models")
                            finally:
                                finished.set()

                        async with anyio.create_task_group() as group:
                            group.start_soon(call)
                            self.assertTrue(await anyio.to_thread.run_sync(admitted.wait, 2))
                            scope.cancel()
                            await finished.wait()
                            while not done_path.exists():
                                await anyio.sleep(0.01)
                            await anyio.sleep(0.1)
                            self.assertFalse(release.is_set())
                            self.assertEqual(len(connections), 1, "aborted calls must not retry")
                            release.set()
                            group.cancel_scope.cancel()

        try:
            anyio.run(exercise)
        finally:
            release.set()
            stop.set()
            listener.close()
            accept_thread.join(timeout=1)
            for handler in handlers:
                handler.join(timeout=1)
            temporary.cleanup()

    def test_oversized_stdio_frame_stays_bounded_and_uses_sdk_error_path(self) -> None:
        """Reject a frame above the former one-MiB ceiling without manual MCP parsing."""

        class Broker:
            """Fixture that is never reached when the bounded input rejects a frame."""

            def call(self, method: str, params: dict | None = None, timeout: float = 600.0) -> object:
                """Fail if an oversized protocol frame reaches the broker boundary."""
                raise AssertionError("oversized frame reached broker")

        stdout = StringIO()
        self.assertEqual(MAX_LINE_BYTES, 1024 * 1024)
        with patch("agent_run.mcp.MAX_LINE_BYTES", 64):
            self.assertEqual(serve(lambda: Broker(), StringIO("x" * 65 + "\n"), stdout), 0)
        self.assertEqual(stdout.getvalue(), "")

    def test_pipe_reader_preserves_a_split_utf8_frame(self) -> None:
        """Keep reading when an incremental UTF-8 decoder needs the next pipe chunk."""
        read_fd, write_fd = os.pipe()
        reader = os.fdopen(read_fd, "r", encoding="utf-8", errors="replace")

        def write_split_frame() -> None:
            """Write one JSON frame with the two bytes of é separated by a delay."""
            os.write(write_fd, b'{"text":"\xc3')
            time.sleep(0.05)
            os.write(write_fd, b'\xa9"}\n')
            os.close(write_fd)

        async def consume() -> list[str]:
            """Collect the bounded reader's complete SDK input frames."""
            return [line async for line in _BoundedInput(reader)]

        writer = threading.Thread(target=write_split_frame)
        writer.start()
        try:
            self.assertEqual(anyio.run(consume), ['{"text":"é"}'])
        finally:
            writer.join(timeout=1)
            reader.close()

    def test_full_one_mib_frame_completes_within_bounded_deadline(self) -> None:
        """Read the exact protocol ceiling without truncation or pathological delay."""
        read_fd, write_fd = os.pipe()
        reader = os.fdopen(read_fd, "r", encoding="utf-8", errors="replace")
        payload = b"x" * MAX_LINE_BYTES + b"\n"

        def write_frame() -> None:
            """Feed the ceiling-sized frame through a real bounded pipe."""
            try:
                view = memoryview(payload)
                while view:
                    view = view[os.write(write_fd, view) :]
            finally:
                os.close(write_fd)

        async def consume() -> list[str]:
            """Collect one ceiling-sized frame under a sane five-second deadline."""
            with anyio.fail_after(5):
                return [line async for line in _BoundedInput(reader)]

        writer = threading.Thread(target=write_frame)
        writer.start()
        try:
            frames = anyio.run(consume)
            self.assertEqual(len(frames), 1)
            self.assertEqual(len(frames[0]), MAX_LINE_BYTES)
        finally:
            writer.join(timeout=1)
            reader.close()

    def test_idle_pipe_cancellation_releases_the_raw_read_worker(self) -> None:
        """Exit a cancelled SDK reader even while the peer keeps its pipe open and idle."""
        read_fd, write_fd = os.pipe()
        reader = os.fdopen(read_fd, "r", encoding="utf-8", errors="replace")
        worker_done = threading.Event()
        real_read = os.read

        def tracked_read(fd: int, size: int) -> bytes:
            """Record when the genuine blocking raw read worker actually exits."""
            try:
                return real_read(fd, size)
            finally:
                worker_done.set()

        async def consume() -> None:
            """Block on an idle bounded input until the enclosing lifecycle cancels it."""
            async for _ in _BoundedInput(reader):
                pass

        async def cancel_reader() -> None:
            """Cancel the server-side reader under a bounded parent deadline."""
            with anyio.fail_after(1):
                async with anyio.create_task_group() as group:
                    group.start_soon(consume)
                    await anyio.sleep(0.05)
                    group.cancel_scope.cancel()

        try:
            with patch("agent_run.mcp.os.read", tracked_read):
                anyio.run(cancel_reader)
            self.assertTrue(worker_done.wait(1), "cancelled raw read worker stayed blocked")
        finally:
            os.close(write_fd)
            try:
                reader.close()
            except OSError:
                pass
