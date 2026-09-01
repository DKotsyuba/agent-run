"""Minimal bounded JSON-RPC 2.0 stdio transport over the resident broker."""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import IO, Mapping

from .broker_client import BrokerClient
from .dispatch import TOOL_NAMES, TOOLS, _bounded, _emit, _error, _jsonable, _valid_id
from .errors import AgentRunError
from .launch_evidence import bootstrap_error_fields


_logger = logging.getLogger("agent_run.mcp")

MAX_LINE_BYTES = 1024 * 1024
_PROTOCOL_VERSION = "2025-06-18"
_MISSING = object()
#: Never log a full task; a truncated preview is still useful for a timeline
#: and never long enough to be the sensitive payload itself.
_TASK_LOG_CHARS = 120


def serve(
    broker: BrokerClient,
    stdin: IO[str] = sys.stdin,
    stdout: IO[str] = sys.stdout,
) -> int:
    """Serve newline-delimited JSON-RPC until EOF; stdout carries JSON only."""
    while True:
        line = stdin.readline(MAX_LINE_BYTES + 1)
        if line == "":
            return 0
        if len(line) > MAX_LINE_BYTES:
            while line and not line.endswith("\n"):
                line = stdin.readline(MAX_LINE_BYTES + 1)
            _emit(stdout, _error(None, -32700, "request exceeds maximum size"))
            continue
        try:
            encoded = line.encode("utf-8")
        except UnicodeEncodeError:
            _emit(stdout, _error(None, -32700, "request is not valid UTF-8"))
            continue
        if len(encoded) > MAX_LINE_BYTES:
            _emit(stdout, _error(None, -32700, "request exceeds maximum size"))
            continue
        try:
            request = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            _emit(stdout, _error(None, -32700, "parse error"))
            continue
        response = _handle(broker, request)
        if response is not None:
            _emit(stdout, response)


def _handle(broker: BrokerClient, request: object) -> dict | None:
    if not isinstance(request, dict):
        return _error(None, -32600, "invalid request")
    request_id = request.get("id", _MISSING)
    if request_id is not _MISSING and not _valid_id(request_id):
        return _error(None, -32600, "invalid request id")
    if request.get("jsonrpc") != "2.0" or not isinstance(request.get("method"), str):
        return _error(None if request_id is _MISSING else request_id, -32600, "invalid request")
    method = request["method"]
    params = request.get("params", {})
    if not isinstance(params, dict):
        if request_id is _MISSING:
            return None
        return _error(request_id, -32602, "params must be an object")
    if request_id is _MISSING:
        return None
    try:
        if method == "initialize":
            version = params.get("protocolVersion", _PROTOCOL_VERSION)
            if not isinstance(version, str) or not version.strip():
                return _error(request_id, -32602, "protocolVersion must be a string")
            result = {
                "protocolVersion": version,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "agent-run", "version": "1"},
            }
        elif method == "notifications/initialized":
            result = {}
        elif method == "tools/list":
            # Pagination params (e.g. cursor) are spec-legal but irrelevant: we
            # always serve the single, complete page.
            result = {"tools": list(TOOLS)}
        elif method == "tools/call":
            return _tool_call(broker, request_id, params)
        else:
            return _error(request_id, -32601, "method not found")
    except Exception as error:
        return _error(request_id, -32603, f"internal error: {type(error).__name__}")
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _tool_call(broker: BrokerClient, request_id: object, params: dict) -> dict:
    name = params.get("name")
    arguments = params.get("arguments", {})
    if not isinstance(name, str) or not isinstance(arguments, dict):
        return _error(request_id, -32602, "tools/call requires name and object arguments")
    # Tolerate standard extra fields (e.g. _meta) per JSON-RPC/MCP robustness;
    # only name/arguments carry tool-call semantics and stay strictly checked.
    agent_id = arguments.get("agent_id")
    if not isinstance(agent_id, str):
        agent_id = None
    task = arguments.get("task")
    _logger.info(
        "tool_call in name=%s request_id=%s agent_id=%s%s",
        name, request_id, agent_id,
        "" if not isinstance(task, str) else f" task={task[:_TASK_LOG_CHARS]!r}",
    )
    started = time.monotonic()
    if name not in TOOL_NAMES:
        result = _tool_error("unknown_tool", f"unknown tool: {name}")
        outcome = "unknown_tool"
    else:
        try:
            value = broker.call(name, arguments)
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
    log = _logger.info if outcome == "ok" else _logger.warning
    log(
        "tool_call out name=%s request_id=%s agent_id=%s outcome=%s duration_ms=%.1f",
        name, request_id, agent_id, outcome, duration_ms,
    )
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _tool_result(value: object) -> dict:
    data = _jsonable(value)
    return {
        "content": [{"type": "text", "text": "result in structuredContent"}],
        "structuredContent": data,
        "isError": False,
    }


def _tool_error(code: str, message: str, *, extra: Mapping[str, object] | None = None) -> dict:
    data = {"error": {"code": code, "message": _bounded(message), **(extra or {})}}
    return {
        "content": [{"type": "text", "text": json.dumps(data, separators=(",", ":"))}],
        "structuredContent": data,
        "isError": True,
    }
