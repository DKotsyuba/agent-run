"""Integration coverage for the official MCP SDK stdio transport."""

from __future__ import annotations

import os
import sys
import threading
import unittest
from pathlib import Path

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from agent_run.mcp import _call_tool


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
