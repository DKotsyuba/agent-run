"""Transport-neutral tool dispatch and generic line-JSON helpers."""

from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import IO, Mapping

from .domain import OrchestratorRef, StartRequest
from .effective_policy import Constraint
from .errors import ValidationError
from .service import AgentQuery, AgentService

_MAX_ERROR_CHARS = 512
_MAX_LINE_BYTES = 1024 * 1024


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
_REQUIRED_CONSTRAINTS = {
    "type": "array",
    "items": {"type": "string", "enum": [item.value for item in Constraint]},
    "uniqueItems": True,
}
_ORCHESTRATOR = _schema(
    {
        "transport": {"type": "string"},
        "external_session_id": {"type": "string"},
        "external_turn_id": {"type": ["string", "null"]},
    },
    ("transport", "external_session_id"),
)
TOOLS = (
    {
        "name": "capacity_order",
        "description": "Return the committed, transport-neutral capacity routing order.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
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
                "fast": {"type": "boolean"},
                "effort": {"type": ["string", "null"]},
                "timeout_seconds": {"type": "number"},
                "read_roots": {"type": "array", "items": {"type": "string"}},
                "output_schema": {"type": ["object", "null"]},
                "orchestrator": {"anyOf": [_ORCHESTRATOR, {"type": "null"}]},
                "request_id": {"type": ["string", "null"]},
                "account": {"type": ["string", "null"]},
                "required_constraints": _REQUIRED_CONSTRAINTS,
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
        "name": "list_agents",
        "description": "List a bounded page with an exact total.",
        "inputSchema": _schema(
            {
                "active": {"type": "boolean"},
                "orchestrator": {"anyOf": [_ORCHESTRATOR, {"type": "null"}]},
                "offset": {"type": "integer"},
                "limit": {"type": "integer"},
                "after_revision": {"type": ["integer", "null"]},
                "wait_seconds": {"type": "number"},
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
        "name": "resume",
        "description": "Continue a terminal agent's native context as a new durable run with inherited identity and permissions.",
        "inputSchema": _schema({
            "agent_id": _ID, "task": {"type": "string"},
            "timeout_seconds": {"type": ["number", "null"]},
            "request_id": {"type": ["string", "null"]},
            "orchestrator": {"anyOf": [_ORCHESTRATOR, {"type": "null"}]},
        }, ("agent_id", "task")),
    },
)
TOOL_NAMES = frozenset(tool["name"] for tool in TOOLS)


def call_tool(service: AgentService, name: str, raw: dict) -> object:
    """Validate and dispatch one of the eight public tools."""

    if name not in TOOL_NAMES:
        raise ValidationError(f"unknown tool: {name}")
    if name == "resume":
        args = _arguments(raw, {"agent_id", "task", "timeout_seconds", "request_id", "orchestrator"}, {"agent_id", "task"})
        return service.resume(
            _string(args, "agent_id"), _string(args, "task"),
            timeout_seconds=args.get("timeout_seconds"),
            request_id=_optional_string(args, "request_id"),
            orchestrator=_optional_orchestrator(args.get("orchestrator")),
        )
    if name == "start":
        args = _arguments(
            raw,
            {
                "runtime", "model", "profile", "task", "workdir", "write",
                "effort", "timeout_seconds", "read_roots", "output_schema",
                "orchestrator", "request_id", "fast", "account",
                "required_constraints",
            },
            {"runtime", "model", "profile", "task", "workdir"},
        )
        write = args.get("write", False)
        if not isinstance(write, bool):
            raise ValidationError("write must be a boolean")
        runtime_name = _string(args, "runtime")
        account = _optional_string(args, "account")
        fast = args.get("fast", False)
        if not isinstance(fast, bool):
            raise ValidationError("fast must be a boolean")
        roots = args.get("read_roots", [])
        if not isinstance(roots, list) or not all(isinstance(item, str) for item in roots):
            raise ValidationError("read_roots must be an array of strings")
        schema = args.get("output_schema")
        if schema is not None and not isinstance(schema, dict):
            raise ValidationError("output_schema must be an object or null")
        required_values = args.get("required_constraints", [])
        if (
            not isinstance(required_values, list)
            or any(not isinstance(item, str) for item in required_values)
            or len(set(required_values)) != len(required_values)
        ):
            raise ValidationError(
                "required_constraints must be an array of unique constraint names"
            )
        try:
            required_constraints = frozenset(
                Constraint(item) for item in required_values
            )
        except ValueError as error:
            raise ValidationError("required_constraints contains an unknown name") from error
        timeout = (
            {}
            if "timeout_seconds" not in args
            else {"timeout_seconds": args["timeout_seconds"]}
        )
        return service.start(
            StartRequest(
                runtime_name,
                _string(args, "model"),
                _string(args, "profile"),
                _string(args, "task"),
                Path(_string(args, "workdir")),
                fast=fast,
                write=write,
                effort=_optional_string(args, "effort"),
                **timeout,
                read_roots=tuple(Path(item) for item in roots),
                output_schema=schema,
                orchestrator=_optional_orchestrator(args.get("orchestrator")),
                request_id=_optional_string(args, "request_id"),
                account=account,
                required_constraints=required_constraints,
            )
        )
    if name in {"cancel", "answer"}:
        args = _arguments(raw, {"agent_id"}, {"agent_id"})
        agent_id = _string(args, "agent_id")
        if name == "cancel":
            return service.cancel(agent_id)
        return service.answer(agent_id)
    if name == "steer":
        args = _arguments(raw, {"agent_id", "text"}, {"agent_id", "text"})
        return service.steer(_string(args, "agent_id"), _string(args, "text"))
    if name == "list_agents":
        args = _arguments(
            raw,
            {"active", "orchestrator", "offset", "limit", "after_revision", "wait_seconds"},
        )
        active = args.get("active", False)
        if not isinstance(active, bool):
            raise ValidationError("active must be a boolean")
        return service.list(
            AgentQuery(
                active=active,
                orchestrator=_optional_orchestrator(args.get("orchestrator")),
                offset=args.get("offset", 0),
                limit=args.get("limit", 100),
                after_revision=args.get("after_revision"),
                wait_seconds=args.get("wait_seconds", 0.0),
            )
        )
    if name == "transcript":
        args = _arguments(raw, {"agent_id", "cursor", "limit"}, {"agent_id"})
        return service.transcript(
            _string(args, "agent_id"),
            cursor=args.get("cursor", 0),
            limit=args.get("limit", 200),
        )
    args = _arguments(raw, set())
    return service.capacity_order()


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
    if len(encoded.encode("utf-8")) > _MAX_LINE_BYTES:
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
