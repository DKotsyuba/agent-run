"""Integration coverage for the official MCP SDK stdio transport."""

from __future__ import annotations

import os
import sys
import threading
import time
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from agent_run.broker_client import MAX_LINE_BYTES
from agent_run.mcp import _BoundedInput, _call_tool, serve


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
        """Release the MCP wait while the callback-owned durable broker call completes."""

        class BlockingBroker:
            """Controlled broker whose accepted call finishes only after test release."""

            def __init__(self, entered: threading.Event, release: threading.Event, done: threading.Event) -> None:
                """Store the synchronization events owned by the enclosing test."""
                self.entered = entered
                self.release = release
                self.done = done

            def call(self, method: str, params: dict | None = None, timeout: float = 600.0) -> object:
                """Wait for release, then report the durable call result without cancellation."""
                self.entered.set()
                if not self.release.wait(timeout=2):
                    raise TimeoutError("test did not release the admitted broker call")
                self.done.set()
                return {"method": method, "arguments": params or {}}

        async def exercise() -> None:
            """Cancel the caller wait, then prove its abandoned worker still finishes."""
            entered = threading.Event()
            release = threading.Event()
            done = threading.Event()
            with anyio.move_on_after(0.1) as scope:
                await _call_tool(
                    lambda: BlockingBroker(entered, release, done), "models", {}
                )
            self.assertTrue(scope.cancel_called)
            self.assertTrue(entered.is_set())
            release.set()
            self.assertTrue(await anyio.to_thread.run_sync(done.wait, 2))

        anyio.run(exercise)

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

    def test_idle_pipe_cancellation_releases_the_raw_read_worker(self) -> None:
        """Exit a cancelled SDK reader even while the peer keeps its pipe open and idle."""
        read_fd, write_fd = os.pipe()
        reader = os.fdopen(read_fd, "r", encoding="utf-8", errors="replace")

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
            anyio.run(cancel_reader)
        finally:
            os.close(write_fd)
            try:
                reader.close()
            except OSError:
                pass
