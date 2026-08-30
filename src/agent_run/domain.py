"""Frozen types shared by configuration, adapters, and storage."""

from __future__ import annotations

import math
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Final, Mapping, NewType

from .errors import StateTransitionError, ValidationError


AgentId = NewType("AgentId", str)
_MAX_EXTERNAL_ID_LENGTH = 512
_LOWER_HEX = frozenset("0123456789abcdef")


class AgentStatus(str, Enum):
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    LOST = "lost"


ACTIVE: Final = frozenset(
    {
        AgentStatus.CREATED,
        AgentStatus.STARTING,
        AgentStatus.RUNNING,
        AgentStatus.CANCELLING,
    }
)
TERMINAL: Final = frozenset(
    {
        AgentStatus.SUCCEEDED,
        AgentStatus.FAILED,
        AgentStatus.TIMED_OUT,
        AgentStatus.CANCELLED,
        AgentStatus.LOST,
    }
)
TRANSITIONS: Final[Mapping[AgentStatus, frozenset[AgentStatus]]] = MappingProxyType(
    {
        AgentStatus.CREATED: frozenset(
            {
                AgentStatus.STARTING,
                AgentStatus.CANCELLED,
                AgentStatus.FAILED,
                AgentStatus.LOST,
            }
        ),
        AgentStatus.STARTING: frozenset(
            {
                AgentStatus.RUNNING,
                AgentStatus.CANCELLED,
                AgentStatus.FAILED,
                AgentStatus.LOST,
            }
        ),
        AgentStatus.RUNNING: frozenset(
            {
                AgentStatus.SUCCEEDED,
                AgentStatus.FAILED,
                AgentStatus.TIMED_OUT,
                AgentStatus.CANCELLING,
                AgentStatus.LOST,
            }
        ),
        AgentStatus.CANCELLING: frozenset(
            {AgentStatus.CANCELLED, AgentStatus.LOST}
        ),
        **{status: frozenset() for status in TERMINAL},
    }
)


def validate_transition(current: AgentStatus, target: AgentStatus) -> None:
    if (
        not isinstance(current, AgentStatus)
        or not isinstance(target, AgentStatus)
        or target not in TRANSITIONS.get(current, ())
    ):
        raise StateTransitionError(f"invalid agent transition: {current} -> {target}")


def validate_agent_id(value: str) -> AgentId:
    if not isinstance(value, str) or len(value) != 29:
        raise ValidationError("agent_id must match ag-YYYYMMDD-HHMMSS-<10 lowercase hex>")
    if not value.startswith("ag-") or value[18] != "-":
        raise ValidationError("agent_id must match ag-YYYYMMDD-HHMMSS-<10 lowercase hex>")
    try:
        datetime.strptime(value[3:18], "%Y%m%d-%H%M%S")
    except ValueError as error:
        raise ValidationError(
            "agent_id must match ag-YYYYMMDD-HHMMSS-<10 lowercase hex>"
        ) from error
    if any(character not in _LOWER_HEX for character in value[19:]):
        raise ValidationError("agent_id must match ag-YYYYMMDD-HHMMSS-<10 lowercase hex>")
    return AgentId(value)


def new_agent_id() -> AgentId:
    value = f"ag-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{secrets.token_hex(5)}"
    return validate_agent_id(value)


def _nonblank(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonblank string")


def _bounded_id(name: str, value: str) -> None:
    _nonblank(name, value)
    if len(value) > _MAX_EXTERNAL_ID_LENGTH:
        raise ValidationError(f"{name} must be at most {_MAX_EXTERNAL_ID_LENGTH} characters")


def _existing_directory(name: str, value: Path) -> Path:
    if not isinstance(value, Path) or not value.is_absolute():
        raise ValidationError(f"{name} must be an absolute existing directory")
    try:
        resolved = value.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValidationError(f"{name} must be an absolute existing directory") from error
    if not resolved.is_dir():
        raise ValidationError(f"{name} must be an absolute existing directory")
    return resolved


@dataclass(frozen=True)
class OrchestratorRef:
    transport: str
    external_session_id: str
    external_turn_id: str | None = None

    def __post_init__(self) -> None:
        _bounded_id("transport", self.transport)
        _bounded_id("external_session_id", self.external_session_id)
        if self.external_turn_id is not None:
            _bounded_id("external_turn_id", self.external_turn_id)


@dataclass(frozen=True)
class StartRequest:
    runtime: str
    model: str
    profile: str
    task: str
    workdir: Path
    write: bool = False
    effort: str | None = None
    timeout_seconds: float | None = None
    read_roots: tuple[Path, ...] = ()
    output_schema: dict | None = None
    orchestrator: OrchestratorRef | None = None
    request_id: str | None = None
    fast: bool = False

    def __post_init__(self) -> None:
        for name in ("runtime", "model", "profile", "task"):
            _nonblank(name, getattr(self, name))
        if not isinstance(self.fast, bool):
            raise ValidationError("fast must be a boolean")
        if self.timeout_seconds is not None:
            if isinstance(self.timeout_seconds, bool) or not isinstance(
                self.timeout_seconds, (int, float)
            ):
                raise ValidationError("timeout_seconds must be positive and finite")
            if self.timeout_seconds <= 0 or not math.isfinite(self.timeout_seconds):
                raise ValidationError("timeout_seconds must be positive and finite")
        if not isinstance(self.read_roots, tuple):
            raise ValidationError("read_roots must be a tuple")
        workdir = _existing_directory("workdir", self.workdir)
        read_roots = tuple(
            _existing_directory(f"read_roots[{index}]", path)
            for index, path in enumerate(self.read_roots)
        )
        if len(set(read_roots)) != len(read_roots):
            raise ValidationError("read_roots must not contain duplicates")
        if self.effort is not None:
            _nonblank("effort", self.effort)
        if self.output_schema is not None and not isinstance(self.output_schema, dict):
            raise ValidationError("output_schema must be a dict or None")
        if self.orchestrator is not None and not isinstance(
            self.orchestrator, OrchestratorRef
        ):
            raise ValidationError("orchestrator must be an OrchestratorRef or None")
        if self.request_id is not None:
            _bounded_id("request_id", self.request_id)
        object.__setattr__(self, "workdir", workdir)
        object.__setattr__(self, "read_roots", read_roots)



class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    SYSTEM = "system"


@dataclass(frozen=True, slots=True)
class Message:
    at: float
    role: MessageRole
    content: str
    name: str | None = None
    raw_ref: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.at, bool) or not isinstance(self.at, (int, float)):
            raise ValidationError("message at must be finite and nonnegative")
        if self.at < 0 or not math.isfinite(self.at):
            raise ValidationError("message at must be finite and nonnegative")
        if not isinstance(self.role, MessageRole):
            raise ValidationError("message role must be a MessageRole")
        _nonblank("message content", self.content)


@dataclass(frozen=True, slots=True)
class Outcome:
    status: AgentStatus
    exit_code: int | None = None
    failure_kind: str | None = None
    failure_text: str | None = None
    runtime_session_id: str | None = None
    answer_path: Path | None = None
    answer_bytes: int | None = None
    answer_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, AgentStatus) or self.status not in TERMINAL:
            raise ValidationError("outcome status must be terminal")
        if self.answer_bytes is not None and (
            isinstance(self.answer_bytes, bool)
            or not isinstance(self.answer_bytes, int)
            or self.answer_bytes < 0
        ):
            raise ValidationError("answer_bytes must be a nonnegative integer or None")
