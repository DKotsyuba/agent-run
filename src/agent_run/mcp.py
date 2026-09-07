"""Official MCP SDK stdio transport over the resident broker boundary."""

from __future__ import annotations

import codecs
import json
import logging
import os
import sys
import time
from collections.abc import AsyncIterator, Callable, Mapping
from typing import IO, Protocol, cast

import anyio
import mcp.types as mcp_types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server

from .broker_client import MAX_LINE_BYTES, BrokerClient
from .dispatch import TOOL_NAMES, TOOLS, _bounded, _jsonable
from .errors import AgentRunError
from .launch_evidence import bootstrap_error_fields

_logger = logging.getLogger("agent_run.mcp")


class _Broker(Protocol):
    """Describe the narrow broker operation exercised by one MCP tool callback."""

    def call(self, method: str, params: dict | None = None, timeout: float = 600.0) -> object:
        """Forward one validated tool call and return its JSON-compatible result."""


class _BoundedInput:
    """Feed complete text frames to the SDK without buffering an unbounded stdin line."""

    def __init__(self, stream: IO[str]) -> None:
        """Bind the bounded iterator to one input stream and select its fastest safe reader."""
        self._stream = stream
        try:
            self._fd = stream.fileno()
        except (AttributeError, OSError):
            self._fd = None
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    async def _read_chunk(self) -> str | None:
        """Read one bounded chunk, returning None only after actual input EOF."""
        if self._fd is None:
            return await anyio.to_thread.run_sync(self._stream.read, 8192) or None
        data = await anyio.to_thread.run_sync(
            os.read, self._fd, 8192, abandon_on_cancel=True
        )
        if not data:
            return self._decoder.decode(b"", final=True) or None
        return self._decoder.decode(data)

    def _abort_read(self) -> None:
        """Close a real input fd so an abandoned blocking raw read returns promptly."""
        if self._fd is not None:
            fd, self._fd = self._fd, None
            try:
                os.close(fd)
            except OSError:
                pass

    async def __aiter__(self) -> AsyncIterator[str]:
        """Yield complete frames up to one MiB and turn oversized frames into SDK parse errors."""
        parts: list[str] = []
        byte_count = 0
        dropping = False
        try:
            while True:
                chunk = await self._read_chunk()
                if chunk is None:
                    break
                if not chunk:
                    continue
                for character in chunk:
                    if character == "\n":
                        yield "{" if dropping else "".join(parts)
                        parts.clear()
                        byte_count = 0
                        dropping = False
                        continue
                    if dropping:
                        continue
                    byte_count += len(character.encode("utf-8"))
                    if byte_count > MAX_LINE_BYTES:
                        parts.clear()
                        dropping = True
                    else:
                        parts.append(character)
            if dropping:
                yield "{"
            elif parts:
                yield "".join(parts)
        except BaseException:
            self._abort_read()
            raise


def serve(
    broker: BrokerClient | Callable[[], _Broker],
    stdin: IO[str] | None = None,
    stdout: IO[str] | None = None,
) -> int:
    """Serve MCP over the process stdio streams until the SDK observes EOF.

    Args:
        broker: Broker client used only to derive a new client per tool callback, or
            a controlled factory used by integration tests.
        stdin: Optional injected input stream for the CLI's testable stdio boundary.
        stdout: Optional injected output stream paired with ``stdin``.

    Returns:
        Zero after the official SDK ends its stdio session.

    Side Effects:
        Runs the official MCP server lifecycle, including protocol negotiation,
        cancellation notifications, invalid-request handling, and EOF cleanup.
    """
    anyio.run(
        _serve,
        broker if callable(broker) else _broker_factory(broker),
        sys.stdin if stdin is None else stdin,
        sys.stdout if stdout is None else stdout,
    )
    return 0


def _broker_factory(broker: BrokerClient) -> Callable[[], _Broker]:
    """Create independent broker clients so concurrent callbacks share no connection.

    Args:
        broker: CLI-created client whose socket path identifies the resident broker.

    Returns:
        A zero-argument factory that creates one fresh client for each callback.
    """
    return lambda: BrokerClient(broker.socket_path)


async def _serve(
    broker_factory: Callable[[], _Broker], stdin: IO[str], stdout: IO[str]
) -> None:
    """Run one official low-level MCP server with an isolated broker factory.

    Args:
        broker_factory: Creates a callback-owned broker client or controlled test fake.
        stdin: Text stream passed to the official SDK stdio adapter.
        stdout: Text stream passed to the official SDK stdio adapter.
    """
    server = _make_server(broker_factory)
    async with stdio_server(
        cast(anyio.AsyncFile[str], _BoundedInput(stdin)),
        anyio.wrap_file(stdout),
    ) as (
        read_stream,
        write_stream,
    ):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
            raise_exceptions=False,
        )


def _make_server(broker_factory: Callable[[], _Broker]) -> Server[object]:
    """Build the official server around the shared dispatch tool definitions.

    Args:
        broker_factory: Creates one broker client for a single tool callback.

    Returns:
        A low-level SDK server whose tool list and call handlers use the one dispatch
        tool table and the resident-broker boundary.
    """

    async def list_tools(
        _context: object, _params: mcp_types.PaginatedRequestParams | None
    ) -> mcp_types.ListToolsResult:
        """Expose the complete, unpaginated shared dispatch tool table."""
        return mcp_types.ListToolsResult(
            tools=[mcp_types.Tool.model_validate(tool) for tool in TOOLS]
        )

    async def call_tool(
        _context: object, params: mcp_types.CallToolRequestParams
    ) -> mcp_types.CallToolResult:
        """Invoke one tool without sharing a mutable broker client across callbacks."""
        arguments = params.arguments or {}
        return await _call_tool(broker_factory, params.name, arguments)

    return Server(
        "agent-run",
        version="1",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )


async def _call_tool(
    broker_factory: Callable[[], _Broker], name: str, arguments: dict[str, object]
) -> mcp_types.CallToolResult:
    """Run a broker call in a disposable worker thread and map domain errors to MCP.

    Args:
        broker_factory: Produces a connection owned solely by this tool callback.
        name: Dispatch tool name selected by the official SDK.
        arguments: SDK-validated object arguments, including any product orchestrator
            binding carried within the tool arguments.

    Returns:
        Official SDK content containing either structured success data or a bounded
        domain-error result.

    Side Effects:
        A disconnected or cancelled MCP caller abandons its wait while the already
        admitted broker operation continues; it is never cancelled by this transport.
    """
    agent_id = arguments.get("agent_id")
    _logger.info(
        "tool_call in name=%s agent_id=%s",
        name,
        agent_id if isinstance(agent_id, str) else None,
    )
    started = time.monotonic()
    if name not in TOOL_NAMES:
        result = _tool_error("unknown_tool", f"unknown tool: {name}")
        outcome = "unknown_tool"
    else:
        broker = broker_factory()
        try:
            try:
                value = await anyio.to_thread.run_sync(
                    _invoke_broker,
                    broker,
                    name,
                    arguments,
                    abandon_on_cancel=True,
                )
            except BaseException:
                _abort_broker(broker)
                raise
            result = _tool_result(value)
            outcome = "ok"
        except AgentRunError as error:
            result = _tool_error(
                str(getattr(error, "broker_error_code", type(error).__name__)),
                str(error),
                extra=bootstrap_error_fields(error),
            )
            outcome = type(error).__name__
        except Exception as error:
            result = _tool_error("internal_error", f"internal error: {type(error).__name__}")
            outcome = "internal_error"
    duration_ms = (time.monotonic() - started) * 1000
    (_logger.info if outcome == "ok" else _logger.warning)(
        "tool_call out name=%s agent_id=%s outcome=%s duration_ms=%.1f",
        name,
        agent_id if isinstance(agent_id, str) else None,
        outcome,
        duration_ms,
    )
    return mcp_types.CallToolResult.model_validate(result)


def _invoke_broker(broker: _Broker, name: str, arguments: dict[str, object]) -> object:
    """Call one callback-owned broker client and close it only after completion.

    Args:
        broker: Connection owned by this worker and its cancelling MCP callback.
        name: Shared dispatch tool name.
        arguments: Tool argument object forwarded unchanged to the broker dispatcher.

    Returns:
        Broker result for conversion to MCP structured content.
    """
    try:
        return broker.call(name, arguments)
    finally:
        close = getattr(broker, "close", None)
        if callable(close):
            close()


def _abort_broker(broker: _Broker) -> None:
    """Interrupt one caller-owned broker socket without issuing a broker cancellation."""
    abort = getattr(broker, "abort", None)
    if callable(abort):
        abort()


def _tool_result(value: object) -> dict[str, object]:
    """Convert a broker result into the repository's structured MCP success shape."""
    data = _jsonable(value)
    return {
        "content": [{"type": "text", "text": "result in structuredContent"}],
        "structuredContent": data,
        "isError": False,
    }


def _tool_error(
    code: str, message: str, *, extra: Mapping[str, object] | None = None
) -> dict[str, object]:
    """Convert a domain failure into an SDK-recognized MCP tool-error result."""
    data = {"error": {"code": code, "message": _bounded(message), **(extra or {})}}
    return {
        "content": [{"type": "text", "text": json.dumps(data, separators=(",", ":"))}],
        "structuredContent": data,
        "isError": True,
    }
