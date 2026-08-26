"""Frozen runtime adapter API version 1."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Mapping, Protocol

from ..config import McpConfig, RuntimeConfig
from ..domain import Message, Outcome, StartRequest
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
    argv: tuple[str, ...]
    cwd: Path
    environment: Mapping[str, str]
    initial_input: str | None
    runtime_stream_path: Path
    adapter_state: Mapping[str, object]
    answer_path: Path | None = None


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
