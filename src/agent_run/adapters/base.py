"""Frozen runtime adapter API version 1."""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Mapping, Protocol

from ..config import McpConfig, RuntimeConfig
from ..domain import Message, Outcome, StartRequest
from ..errors import ValidationError
from ..profiles import AgentProfile


ADAPTER_API_VERSION = 1


class Capability(str, Enum):
    STEER = "steer"
    EFFORT = "effort"
    OUTPUT_SCHEMA = "output_schema"
    READ_ROOTS = "read_roots"
    WRITE = "write"
    TRANSCRIPT = "transcript"
    MODEL_ROSTER = "model_roster"
    LIVE_LIMITS = "live_limits"
    MCP = "mcp"
    SKILLS = "skills"
    HOOKS = "hooks"


@dataclass(frozen=True)
class RuntimeInfo:
    name: str
    adapter_api_version: int
    capabilities: frozenset[Capability]


@dataclass(frozen=True)
class RuntimeHealth:
    available: bool
    version: str | None
    authenticated: bool | None
    reason: str | None


@dataclass(frozen=True)
class ModelInfo:
    id: str
    description: str
    efforts: tuple[str, ...] = ()


@dataclass(frozen=True)
class LimitSample:
    lane: str
    window: str
    remaining_percent: float | None
    reset_at: datetime | None
    observed_at: datetime | None
    source: str
    target: str | None = None
    valid_for_seconds: int | None = None


@dataclass(frozen=True)
class LaunchPlan:
    """Immutable adapter-to-supervisor process launch contract.

    ``argv`` and ``cwd`` select the child command; ``environment`` may contain
    live secrets and remains pipe-only. ``initial_input`` preserves text,
    binary, or absent input. The stream and optional answer paths hold durable
    evidence, while ``adapter_state`` carries JSON-safe adapter metadata.
    """

    argv: tuple[str, ...]
    cwd: Path
    environment: Mapping[str, str]
    initial_input: str | bytes | None
    runtime_stream_path: Path
    adapter_state: Mapping[str, object]
    answer_path: Path | None = None

    def to_payload(self) -> dict[str, object]:
        """JSON form handed to the exec'd supervisor over a private pipe.

        ``environment`` carries live secrets, so this never reaches argv or disk.
        """

        return {
            "argv": [str(item) for item in self.argv],
            "cwd": str(self.cwd),
            "environment": {str(k): str(v) for k, v in self.environment.items()},
            "initial_input_b64": _encode_input(self.initial_input),
            "initial_input_is_bytes": isinstance(self.initial_input, bytes),
            "runtime_stream_path": str(self.runtime_stream_path),
            "adapter_state": dict(self.adapter_state),
            "answer_path": None if self.answer_path is None else str(self.answer_path),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> LaunchPlan:
        """Rebuild and validate one private parent-to-supervisor plan payload.

        Every field must retain the exact JSON shape emitted by :meth:`to_payload`;
        malformed or missing fields raise :class:`ValidationError` before any
        path, environment, or input value is constructed.
        """

        if not isinstance(payload, Mapping):
            raise ValidationError("launch plan payload must be a mapping")
        try:
            argv = payload["argv"]
            cwd = payload["cwd"]
            environment = payload["environment"]
            initial_input = payload["initial_input_b64"]
            initial_input_is_bytes = payload["initial_input_is_bytes"]
            runtime_stream_path = payload["runtime_stream_path"]
            adapter_state = payload["adapter_state"]
            answer_path = payload["answer_path"]
        except KeyError as error:
            raise ValidationError(f"malformed launch plan payload: {error}") from error
        if not isinstance(argv, list) or not all(
            isinstance(item, str) for item in argv
        ):
            raise ValidationError("launch plan argv must be a list of strings")
        if not isinstance(cwd, str) or not cwd:
            raise ValidationError("launch plan cwd must be a nonblank string")
        if not isinstance(environment, Mapping) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in environment.items()
        ):
            raise ValidationError("launch plan environment must map strings to strings")
        if initial_input is not None and not isinstance(initial_input, str):
            raise ValidationError("launch plan input must be a base64 string or null")
        if type(initial_input_is_bytes) is not bool:
            raise ValidationError("launch plan input byte flag must be a boolean")
        if not isinstance(runtime_stream_path, str) or not runtime_stream_path:
            raise ValidationError("launch plan stream path must be a nonblank string")
        if not isinstance(adapter_state, Mapping):
            raise ValidationError("launch plan adapter state must be a mapping")
        if answer_path is not None and (
            not isinstance(answer_path, str) or not answer_path
        ):
            raise ValidationError(
                "launch plan answer path must be a nonblank string or null"
            )
        try:
            return cls(
                tuple(argv),
                Path(cwd),
                dict(environment),
                _decode_input(initial_input, initial_input_is_bytes),
                Path(runtime_stream_path),
                dict(adapter_state),
                None if answer_path is None else Path(answer_path),
            )
        except (TypeError, ValueError, UnicodeError, binascii.Error) as error:
            raise ValidationError(f"malformed launch plan payload: {error}") from error


def _encode_input(value: object) -> str | None:
    if value is None:
        return None
    raw = value if isinstance(value, bytes) else str(value).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def _decode_input(value: object, is_bytes: object) -> str | bytes | None:
    if value is None:
        return None
    raw = base64.b64decode(str(value).encode("ascii"), validate=True)
    return raw if is_bytes else raw.decode("utf-8")


class EventSink(Protocol):
    def message(self, message: Message) -> None: ...

    def session(self, runtime_session_id: str) -> None: ...

    def event(self, kind: str, data: Mapping[str, object]) -> None: ...


class RuntimeSession(Protocol):
    @property
    def pid(self) -> int | None: ...

    @property
    def owns_process_group(self) -> bool: ...

    def wait(self, timeout_seconds: float | None) -> Outcome | None: ...

    def steer(self, text: str) -> None: ...

    def cancel(self, grace_seconds: float) -> None: ...


class RuntimeAdapter(Protocol):
    def describe(self) -> RuntimeInfo: ...

    def validate(self, config: RuntimeConfig) -> None: ...

    def materialize(
        self,
        config: RuntimeConfig,
        home: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
        skills_root: Path,
    ) -> str: ...

    def probe(self, config: RuntimeConfig, home: Path) -> RuntimeHealth: ...

    def models(self, config: RuntimeConfig, home: Path) -> tuple[ModelInfo, ...]: ...

    def limits(self, config: RuntimeConfig, home: Path) -> tuple[LimitSample, ...]: ...

    def prepare(
        self,
        request: StartRequest,
        profile: AgentProfile,
        config: RuntimeConfig,
        home: Path,
        agent_dir: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
    ) -> LaunchPlan: ...

    def launch(self, plan: LaunchPlan, sink: EventSink) -> RuntimeSession: ...
