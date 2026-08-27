"""Minimal bounded JSON-RPC 2.0 stdio transport for :class:`AgentService`."""

from __future__ import annotations

import json
import sys
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import IO, Mapping

from .domain import OrchestratorRef, StartRequest
from .errors import AgentRunError, ValidationError
from .service import AgentQuery, AgentService


MAX_LINE_BYTES = 1024 * 1024
_MAX_ERROR_CHARS = 512
_PROTOCOL_VERSION = "2025-06-18"
_MISSING = object()


def _schema(properties: dict, required: tuple[str, ...] = ()) -> dict:
    result = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        result["required"] = list(required)
    return result


_ID = {"type": "string"}
_ORCHESTRATOR = _schema(
    {
        "transport": {"type": "string"},
        "external_session_id": {"type": "string"},
        "external_turn_id": {"type": ["string", "null"]},
    },
    ("transport", "external_session_id"),
)
_TOOLS = (
    {
        "name": "start",
        "description": "Start one asynchronous durable agent.",
        "inputSchema": _schema(
            {
                "runtime": {"type": "string"},
                "model": {"type": "string"},
                "profile": {"type": "string"},
                "task": {"type": "string"},
                "workdir": {"type": "string"},
                "write": {"type": "boolean"},
                "effort": {"type": ["string", "null"]},
                "timeout_seconds": {"type": "number"},
                "read_roots": {"type": "array", "items": {"type": "string"}},
                "output_schema": {"type": ["object", "null"]},
                "orchestrator": {"anyOf": [_ORCHESTRATOR, {"type": "null"}]},
                "request_id": {"type": ["string", "null"]},
            },
            ("runtime", "model", "profile", "task", "workdir"),
        ),
    },
    {
        "name": "cancel",
        "description": "Durably request agent cancellation.",
        "inputSchema": _schema({"agent_id": _ID}, ("agent_id",)),
    },
    {
        "name": "steer",
        "description": "Durably steer an active capable agent.",
        "inputSchema": _schema(
            {"agent_id": _ID, "text": {"type": "string"}},
            ("agent_id", "text"),
        ),
    },
    {
        "name": "status",
        "description": "Get one agent's current durable status.",
        "inputSchema": _schema({"agent_id": _ID}, ("agent_id",)),
    },
    {
        "name": "list_agents",
        "description": "List a bounded page with an exact total.",
        "inputSchema": _schema(
            {
                "active": {"type": "boolean"},
                "orchestrator": {"anyOf": [_ORCHESTRATOR, {"type": "null"}]},
                "offset": {"type": "integer"},
                "limit": {"type": "integer"},
            }
        ),
    },
    {
        "name": "summary",
        "description": "Summarize exactly one agent or orchestration session.",
        "inputSchema": _schema(
            {
                "agent_id": {"type": ["string", "null"]},
                "orchestrator": {"anyOf": [_ORCHESTRATOR, {"type": "null"}]},
            }
        ),
    },
    {
        "name": "transcript",
        "description": "Read one explicit cursor page; raw_ref stays a reference.",
        "inputSchema": _schema(
            {
                "agent_id": _ID,
                "cursor": {"type": "integer"},
                "limit": {"type": "integer"},
            },
            ("agent_id",),
        ),
    },
    {
        "name": "answer",
        "description": "Read verified bounded answer metadata and optional inline text.",
        "inputSchema": _schema({"agent_id": _ID}, ("agent_id",)),
    },
    {
        "name": "models",
        "description": "List enabled runtime model rosters.",
        "inputSchema": _schema({}),
    },
    {
        "name": "limits",
        "description": "Read stored fresh capacity projections without provider calls.",
        "inputSchema": _schema({}),
    },
    {
        "name": "doc",
        "description": "Read one operator guide topic, or the index when omitted.",
        "inputSchema": _schema({"topic": {"type": ["string", "null"]}}),
    },
)
_TOOL_NAMES = frozenset(tool["name"] for tool in _TOOLS)


def serve(
    service: AgentService,
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
        response = _handle(service, request)
        if response is not None:
            _emit(stdout, response)


def _handle(service: AgentService, request: object) -> dict | None:
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
            result = {"tools": list(_TOOLS)}
        elif method == "tools/call":
            return _tool_call(service, request_id, params)
        else:
            return _error(request_id, -32601, "method not found")
    except Exception as error:
        return _error(request_id, -32603, f"internal error: {type(error).__name__}")
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _tool_call(service: AgentService, request_id: object, params: dict) -> dict:
    name = params.get("name")
    arguments = params.get("arguments", {})
    if not isinstance(name, str) or not isinstance(arguments, dict):
        return _error(request_id, -32602, "tools/call requires name and object arguments")
    # Tolerate standard extra fields (e.g. _meta) per JSON-RPC/MCP robustness;
    # only name/arguments carry tool-call semantics and stay strictly checked.
    if name not in _TOOL_NAMES:
        result = _tool_error("unknown_tool", f"unknown tool: {name}")
    else:
        try:
            result = _tool_result(_call_tool(service, name, arguments))
        except AgentRunError as error:
            result = _tool_error(type(error).__name__, str(error))
        except Exception as error:
            result = _tool_error("internal_error", f"internal error: {type(error).__name__}")
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _call_tool(service: AgentService, name: str, raw: dict) -> object:
    if name == "start":
        args = _arguments(
            raw,
            {
                "runtime", "model", "profile", "task", "workdir", "write",
                "effort", "timeout_seconds", "read_roots", "output_schema",
                "orchestrator", "request_id",
            },
            {"runtime", "model", "profile", "task", "workdir"},
        )
        write = args.get("write", False)
        if not isinstance(write, bool):
            raise ValidationError("write must be a boolean")
        roots = args.get("read_roots", [])
        if not isinstance(roots, list) or not all(isinstance(item, str) for item in roots):
            raise ValidationError("read_roots must be an array of strings")
        schema = args.get("output_schema")
        if schema is not None and not isinstance(schema, dict):
            raise ValidationError("output_schema must be an object or null")
        timeout = (
            {}
            if "timeout_seconds" not in args
            else {"timeout_seconds": args["timeout_seconds"]}
        )
        return service.start(
            StartRequest(
                _string(args, "runtime"),
                _string(args, "model"),
                _string(args, "profile"),
                _string(args, "task"),
                Path(_string(args, "workdir")),
                write=write,
                effort=_optional_string(args, "effort"),
                **timeout,
                read_roots=tuple(Path(item) for item in roots),
                output_schema=schema,
                orchestrator=_optional_orchestrator(args.get("orchestrator")),
                request_id=_optional_string(args, "request_id"),
            )
        )
    if name in {"cancel", "status", "answer"}:
        args = _arguments(raw, {"agent_id"}, {"agent_id"})
        agent_id = _string(args, "agent_id")
        return {
            "cancel": service.cancel,
            "status": service.get,
            "answer": service.answer,
        }[name](agent_id)
    if name == "steer":
        args = _arguments(raw, {"agent_id", "text"}, {"agent_id", "text"})
        return service.steer(_string(args, "agent_id"), _string(args, "text"))
    if name == "list_agents":
        args = _arguments(raw, {"active", "orchestrator", "offset", "limit"})
        active = args.get("active", False)
        if not isinstance(active, bool):
            raise ValidationError("active must be a boolean")
        return service.list(
            AgentQuery(
                active=active,
                orchestrator=_optional_orchestrator(args.get("orchestrator")),
                offset=args.get("offset", 0),
                limit=args.get("limit", 100),
            )
        )
    if name == "summary":
        args = _arguments(raw, {"agent_id", "orchestrator"})
        return service.summary(
            agent_id=_optional_string(args, "agent_id"),
            orchestrator=_optional_orchestrator(args.get("orchestrator")),
        )
    if name == "transcript":
        args = _arguments(raw, {"agent_id", "cursor", "limit"}, {"agent_id"})
        return service.transcript(
            _string(args, "agent_id"),
            cursor=args.get("cursor", 0),
            limit=args.get("limit", 200),
        )
    if name == "doc":
        from .doc import topic_text

        args = _arguments(raw, {"topic"})
        topic = _optional_string(args, "topic")
        return {"topic": topic or "index", "text": topic_text(topic)}
    args = _arguments(raw, set())
    return service.models() if name == "models" else service.limits()


def _arguments(raw: dict, allowed: set[str], required: set[str] = set()) -> dict:
    unknown = set(raw) - allowed
    missing = required - set(raw)
    if unknown:
        raise ValidationError(f"unknown arguments: {sorted(unknown)}")
    if missing:
        raise ValidationError(f"missing arguments: {sorted(missing)}")
    return raw


def _string(args: Mapping[str, object], name: str) -> str:
    value = args.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonblank string")
    return value


def _optional_string(args: Mapping[str, object], name: str) -> str | None:
    value = args.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonblank string or null")
    return value


def _optional_orchestrator(value: object) -> OrchestratorRef | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValidationError("orchestrator must be an object or null")
    args = _arguments(
        value,
        {"transport", "external_session_id", "external_turn_id"},
        {"transport", "external_session_id"},
    )
    return OrchestratorRef(
        _string(args, "transport"),
        _string(args, "external_session_id"),
        _optional_string(args, "external_turn_id"),
    )


def _tool_result(value: object) -> dict:
    data = _jsonable(value)
    return {
        "content": [{"type": "text", "text": "result in structuredContent"}],
        "structuredContent": data,
        "isError": False,
    }


def _tool_error(code: str, message: str) -> dict:
    data = {"error": {"code": code, "message": _bounded(message)}}
    return {
        "content": [{"type": "text", "text": json.dumps(data, separators=(",", ":"))}],
        "structuredContent": data,
        "isError": True,
    }


def _jsonable(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _emit(stdout: IO[str], response: dict) -> None:
    encoded = json.dumps(response, separators=(",", ":"), ensure_ascii=False)
    if len(encoded.encode("utf-8")) > MAX_LINE_BYTES:
        encoded = json.dumps(
            _error(response.get("id"), -32603, "response exceeds maximum size"),
            separators=(",", ":"),
        )
    stdout.write(encoded + "\n")
    stdout.flush()


def _error(request_id: object, code: int, message: str) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": code, "message": _bounded(message)},
    }


def _valid_id(value: object) -> bool:
    return value is None or (
        not isinstance(value, bool) and isinstance(value, (str, int, float))
    )


def _bounded(value: object) -> str:
    text = str(value).strip() or type(value).__name__
    return text[:_MAX_ERROR_CHARS]
