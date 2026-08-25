"""OpenCode adapter: managed isolated v2 service only.

There is no CLI fallback and no attachment to a user-owned global service. When
isolation is unproven the runtime reports unavailable and refuses to prepare.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Sequence

from ...config import RuntimeConfig
from ...domain import AgentStatus, Message, MessageRole, Outcome, StartRequest
from ...errors import ValidationError
from ...profiles import AgentProfile
from ..base import (
    ADAPTER_API_VERSION,
    Capability,
    EventSink,
    LaunchPlan,
    LimitSample,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from ..home import write_managed_file
from .http import OpenCodeHttpClient, PollTimeout
from .service import ServiceIsolationError, build_service_plan, read_service_descriptor


RUNTIME_NAME = "opencode"
PRIMARY_AGENT = "agent-run"
VERIFY_AGENT = "agent-run-verify"
CONFIG_RELATIVE_PATH = "xdg/config/opencode/opencode.json"
CAPABILITIES = frozenset(
    {
        Capability.STEER,
        Capability.OUTPUT_SCHEMA,
        Capability.READ_ROOTS,
        Capability.WRITE,
        Capability.TRANSCRIPT,
        Capability.MODEL_ROSTER,
        Capability.MCP,
        Capability.SKILLS,
    }
)

EXTERNAL_DIRECTORY = "external_directory"
_SCHEMA_KEYS = frozenset(
    {
        "$schema",
        "title",
        "description",
        "type",
        "properties",
        "required",
        "items",
        "enum",
        "additionalProperties",
    }
)
_TERMINAL_STATES: Mapping[str, AgentStatus] = MappingProxyType(
    {
        "completed": AgentStatus.SUCCEEDED,
        "idle": AgentStatus.SUCCEEDED,
        "aborted": AgentStatus.CANCELLED,
        "cancelled": AgentStatus.CANCELLED,
        "error": AgentStatus.FAILED,
        "failed": AgentStatus.FAILED,
        "timeout": AgentStatus.TIMED_OUT,
        "timed_out": AgentStatus.TIMED_OUT,
    }
)
_ACTIVE_STATES = frozenset({"running", "busy", "pending", "queued", "streaming"})
_ROLES: Mapping[str, MessageRole] = MappingProxyType(
    {
        "user": MessageRole.USER,
        "assistant": MessageRole.ASSISTANT,
        "system": MessageRole.SYSTEM,
    }
)


# --- permissions ---------------------------------------------------------


@dataclass(frozen=True)
class PermissionDecision:
    permission_id: str
    granted: bool
    reason: str


@dataclass
class PermissionBroker:
    """Auto-reject every interactive permission except one contained grant."""

    read_roots: tuple[Path, ...] = ()
    _granted: str | None = field(default=None, init=False)
    _blocked: dict[str, int] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        for index, root in enumerate(self.read_roots):
            if not isinstance(root, Path) or not root.is_absolute():
                raise ValidationError(f"read_roots[{index}] must be an absolute path")
        self.read_roots = tuple(self.read_roots)

    @property
    def granted_directory(self) -> str | None:
        return self._granted

    def decide(self, permission: Mapping[str, object]) -> PermissionDecision:
        if not isinstance(permission, Mapping):
            raise ValidationError("permission must be a mapping")
        identifier = permission.get("id")
        if not isinstance(identifier, str) or not identifier.strip():
            raise ValidationError("permission id must be a nonblank string")
        kind = permission.get("type")
        if kind != EXTERNAL_DIRECTORY:
            return self._block(identifier, str(kind), f"permission type is auto-rejected: {kind!r}")
        if self._granted is not None:
            return self._block(
                identifier, kind, "external_directory was already granted once for this agent"
            )
        resolved = _resolve_directory(permission.get("path"))
        if resolved is None:
            return self._block(identifier, kind, "external_directory path is not usable")
        if not any(_contains(root, resolved) for root in self.read_roots):
            return self._block(
                identifier, kind, f"external_directory {resolved} is outside the resolved read roots"
            )
        self._granted = str(resolved)
        return PermissionDecision(identifier, True, f"external_directory contained by read roots: {resolved}")

    def _block(self, identifier: str, kind: str, reason: str) -> PermissionDecision:
        self._blocked[kind] = self._blocked.get(kind, 0) + 1
        return PermissionDecision(identifier, False, reason)

    def blocked_summary(self) -> Mapping[str, int]:
        """Bounded counts of rejected permissions, by requested type."""

        return MappingProxyType(dict(sorted(self._blocked.items())))

    def reply(self, decision: PermissionDecision) -> Mapping[str, object]:
        return MappingProxyType(
            {"response": "once" if decision.granted else "reject", "reason": decision.reason}
        )


def _contains(root: Path, candidate: Path) -> bool:
    return candidate == root or candidate.is_relative_to(root)


def _resolve_directory(value: object) -> Path | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        path = Path(value).expanduser()
        if not path.is_absolute():
            return None
        return path.resolve()
    except (OSError, RuntimeError):
        return None


# --- deterministic normalization ----------------------------------------


def normalize_models(payload: Mapping[str, object], allowed: Sequence[str]) -> tuple[ModelInfo, ...]:
    """Intersect the reported roster with the configured allowlist, in order."""

    if not isinstance(payload, Mapping):
        raise ValidationError("opencode model roster must be a mapping")
    reported: dict[str, str] = {}
    for provider in _sequence(payload.get("providers")):
        if not isinstance(provider, Mapping):
            raise ValidationError("opencode provider entry must be a mapping")
        models = provider.get("models")
        items = models.values() if isinstance(models, Mapping) else _sequence(models)
        for model in items:
            if not isinstance(model, Mapping):
                raise ValidationError("opencode model entry must be a mapping")
            identifier = model.get("id")
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValidationError("opencode model id must be a nonblank string")
            name = model.get("name")
            reported.setdefault(identifier, name if isinstance(name, str) else identifier)
    return tuple(
        ModelInfo(identifier, reported[identifier])
        for identifier in allowed
        if identifier in reported
    )


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(value)
    raise ValidationError("opencode payload expected an array")


def _text_parts(item: Mapping[str, object]) -> str:
    chunks: list[str] = []
    for part in _sequence(item.get("parts")):
        if not isinstance(part, Mapping):
            raise ValidationError("opencode message part must be a mapping")
        if part.get("type") != "text":
            continue
        text = part.get("text")
        if isinstance(text, str) and text.strip():
            chunks.append(text.strip())
    return "\n\n".join(chunks)


def _at(item: Mapping[str, object]) -> float:
    time_value = item.get("time")
    raw = time_value.get("created") if isinstance(time_value, Mapping) else item.get("created")
    if isinstance(raw, bool) or not isinstance(raw, (int, float)) or raw < 0:
        return 0.0
    return float(raw)


def _role(item: Mapping[str, object]) -> MessageRole:
    raw = item.get("role")
    try:
        return _ROLES[raw]  # type: ignore[index]
    except (KeyError, TypeError) as error:
        raise ValidationError(f"unknown opencode message role: {raw!r}") from error


def _agent(item: Mapping[str, object]) -> str:
    value = item.get("agent")
    return value if isinstance(value, str) and value.strip() else PRIMARY_AGENT


def _is_real_user(item: Mapping[str, object]) -> bool:
    return _role(item) is MessageRole.USER and item.get("synthetic") is not True


def normalize_transcript(
    payload: Mapping[str, object] | Sequence[object], *, raw_ref: str | None = None
) -> tuple[Message, ...]:
    """Turn a captured message page into domain messages, dropping empty text."""

    messages: list[Message] = []
    for item in _messages(payload):
        content = _text_parts(item)
        if not content:
            continue
        messages.append(
            Message(_at(item), _role(item), content, name=_agent(item), raw_ref=raw_ref)
        )
    return tuple(messages)


def _messages(payload: Mapping[str, object] | Sequence[object]) -> tuple[Mapping[str, object], ...]:
    items = payload.get("messages") if isinstance(payload, Mapping) else payload
    result = []
    for item in _sequence(items):
        if not isinstance(item, Mapping):
            raise ValidationError("opencode message must be a mapping")
        result.append(item)
    return tuple(result)


def extract_answer(
    payload: Mapping[str, object] | Sequence[object], *, agent: str = PRIMARY_AGENT
) -> str:
    """All verified-agent assistant text after the last real user message.

    Retried turns are kept: a synthetic user message inserted by the runtime
    between attempts does not start a new answer, and sub-agent output is not
    mistaken for the primary agent's answer.
    """

    items = _messages(payload)
    start = 0
    for index, item in enumerate(items):
        if _is_real_user(item):
            start = index + 1
    chunks = [
        text
        for item in items[start:]
        if _role(item) is MessageRole.ASSISTANT and _agent(item) == agent
        for text in (_text_parts(item),)
        if text
    ]
    return "\n\n".join(chunks)


def normalize_outcome(
    state: Mapping[str, object], *, runtime_session_id: str | None = None
) -> Outcome:
    """Map a reported session state to exactly one terminal outcome."""

    if not isinstance(state, Mapping):
        raise ValidationError("opencode session state must be a mapping")
    raw = state.get("state", state.get("status"))
    if not isinstance(raw, str) or raw not in _TERMINAL_STATES:
        raise ValidationError(f"opencode session state is not terminal: {raw!r}")
    status = _TERMINAL_STATES[raw]
    error = state.get("error")
    kind = None
    text = None
    if isinstance(error, Mapping):
        name = error.get("name")
        message = error.get("message")
        kind = name if isinstance(name, str) else "opencode_error"
        text = message if isinstance(message, str) else None
    elif isinstance(error, str) and error.strip():
        kind = "opencode_error"
        text = error
    if kind is not None and status is AgentStatus.SUCCEEDED:
        status = AgentStatus.FAILED
    return Outcome(
        status=status,
        failure_kind=kind if status is not AgentStatus.SUCCEEDED else None,
        failure_text=text if status is not AgentStatus.SUCCEEDED else None,
        runtime_session_id=runtime_session_id,
    )


def is_settled(state: Mapping[str, object]) -> bool:
    raw = state.get("state", state.get("status")) if isinstance(state, Mapping) else None
    return isinstance(raw, str) and raw not in _ACTIVE_STATES and raw in _TERMINAL_STATES


# --- request refusals ----------------------------------------------------


def _check_schema(schema: object) -> None:
    if schema is None:
        return
    if not isinstance(schema, dict):
        raise ValidationError("output_schema must be a JSON object schema")
    unknown = sorted(set(schema) - _SCHEMA_KEYS)
    if unknown:
        raise ValidationError(
            f"output_schema contains unsupported keys: {', '.join(unknown)}; "
            "opencode accepts no raw runtime passthrough"
        )
    if schema.get("type") != "object" or not isinstance(schema.get("properties"), dict):
        raise ValidationError("output_schema must declare type 'object' with properties")


def _check_request(request: StartRequest, profile: AgentProfile, config: RuntimeConfig) -> None:
    if not isinstance(request, StartRequest) or not isinstance(profile, AgentProfile):
        raise ValidationError("opencode prepare requires a StartRequest and an AgentProfile")
    if request.runtime != RUNTIME_NAME:
        raise ValidationError(f"request targets runtime {request.runtime!r}, not {RUNTIME_NAME!r}")
    if request.model not in config.models:
        raise ValidationError(f"model is not in the configured allowlist: {request.model}")
    if request.effort is not None:
        raise ValidationError("opencode does not support effort levels")
    if request.write and not profile.write:
        raise ValidationError(f"profile {profile.name} does not permit write")
    _check_schema(request.output_schema)


def _write_roots(request: StartRequest, profile: AgentProfile) -> tuple[Path, ...]:
    if not (request.write and profile.write):
        return ()
    for root in profile.read_roots:
        if _contains(root, request.workdir):
            raise ValidationError(
                f"read root {root} contains the write root {request.workdir}; "
                "read roots never become writable"
            )
    return (request.workdir,)


# --- adapter -------------------------------------------------------------


class OpenCodeAdapter:
    """Runtime adapter API version 1 for the managed OpenCode v2 service."""

    def describe(self) -> RuntimeInfo:
        return RuntimeInfo(RUNTIME_NAME, ADAPTER_API_VERSION, CAPABILITIES)

    def validate(self, config: RuntimeConfig) -> None:
        if not isinstance(config, RuntimeConfig):
            raise ValidationError("opencode validate requires a RuntimeConfig")
        if config.service_mode != "managed":
            raise ValidationError(
                "runtimes.opencode.service_mode must be 'managed'; "
                "the CLI and the global service are not supported"
            )
        if config.hooks:
            raise ValidationError("opencode does not support hooks")
        if config.auth is not None and config.auth.kind != "environment":
            raise ValidationError("runtimes.opencode.auth.kind must be 'environment'")
        if not config.binary.is_absolute():
            raise ValidationError("runtimes.opencode.binary must be an absolute path")

    def materialize(self, config: RuntimeConfig, home: Path) -> str:
        """Render the generated service config; nothing outside home is touched."""

        self.validate(config)
        document = {
            "$schema": "https://opencode.ai/config.json",
            "agent": {
                PRIMARY_AGENT: {"mode": "primary", "model": list(config.models)},
                VERIFY_AGENT: {"mode": "subagent", "model": list(config.models)},
            },
            "model": list(config.models),
            "mcp": list(config.mcp),
            "skills": list(config.skills),
            "permission": {
                "bash": "deny",
                "edit": "deny",
                "webfetch": "deny",
                EXTERNAL_DIRECTORY: "ask",
            },
            "share": "disabled",
        }
        content = json.dumps(document, indent=2, sort_keys=True) + "\n"
        return write_managed_file(home, CONFIG_RELATIVE_PATH, content)

    def probe(self, config: RuntimeConfig, home: Path) -> RuntimeHealth:
        """Report health from recorded proof only; never start or attach."""

        try:
            self.validate(config)
        except ValidationError as error:
            return RuntimeHealth(False, None, None, str(error))
        if not config.binary.is_file():
            return RuntimeHealth(False, None, None, f"binary does not exist: {config.binary}")
        try:
            descriptor = read_service_descriptor(home)
        except ValidationError as error:
            return RuntimeHealth(False, None, None, str(error))
        if descriptor is None:
            return RuntimeHealth(
                False,
                None,
                None,
                "managed opencode service isolation is unproven; refusing to attach to a "
                "global service or fall back to the CLI",
            )
        return RuntimeHealth(True, descriptor.version, None, None)

    def models(self, config: RuntimeConfig, home: Path) -> tuple[ModelInfo, ...]:
        """Report the roster only from a proven service, never a global one."""

        descriptor = read_service_descriptor(home)
        if descriptor is None:
            return ()
        client = OpenCodeHttpClient(descriptor.base_url, home)
        return normalize_models(client.providers(), config.models)

    def limits(self, config: RuntimeConfig, home: Path) -> tuple[LimitSample, ...]:
        return ()

    def prepare(
        self,
        request: StartRequest,
        profile: AgentProfile,
        config: RuntimeConfig,
        home: Path,
        agent_dir: Path,
    ) -> LaunchPlan:
        self.validate(config)
        _check_request(request, profile, config)
        descriptor = read_service_descriptor(home)
        if descriptor is None:
            raise ServiceIsolationError(
                "managed opencode service isolation is unproven; the runtime is unavailable "
                "and there is no global or CLI fallback"
            )
        plan = build_service_plan(config, home, port=descriptor.port)
        write_roots = _write_roots(request, profile)
        state = {
            "service": {
                "base_url": descriptor.base_url,
                "config_home": str(descriptor.config_home),
                "data_home": str(descriptor.data_home),
            },
            "agent": PRIMARY_AGENT,
            "verify_agent": VERIFY_AGENT,
            "model": request.model,
            "workdir": str(request.workdir),
            "write_roots": [str(root) for root in write_roots],
            "read_roots": [str(root) for root in profile.read_roots],
            "output_schema": request.output_schema,
            "response_dir": str(agent_dir),
            "timeout_seconds": request.timeout_seconds,
        }
        return LaunchPlan(
            argv=plan.argv,
            cwd=request.workdir,
            environment=plan.environment,
            initial_input=f"{profile.body}\n\n{request.task}",
            runtime_stream_path=agent_dir / "runtime.jsonl",
            adapter_state=MappingProxyType(state),
        )

    def launch(
        self, plan: LaunchPlan, sink: EventSink, client: OpenCodeHttpClient | None = None
    ) -> "OpenCodeRuntimeSession":
        """Open one session on the already-running private service and prompt it."""

        state = plan.adapter_state
        service = state.get("service")
        if not isinstance(service, Mapping) or not isinstance(service.get("base_url"), str):
            raise ServiceIsolationError("launch plan carries no proven opencode service")
        if client is None:
            client = OpenCodeHttpClient(str(service["base_url"]), str(state["response_dir"]))
        opened = client.create_session({"agent": PRIMARY_AGENT, "title": str(state["model"])})
        session_id = opened.get("id")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValidationError("opencode service returned no session id")
        roots = tuple(Path(str(root)) for root in _sequence(state.get("read_roots")))
        session = OpenCodeRuntimeSession(
            client, session_id, sink, broker=PermissionBroker(roots)
        )
        client.prompt(
            session_id,
            {
                "agent": PRIMARY_AGENT,
                "model": state["model"],
                "parts": [{"type": "text", "text": plan.initial_input}],
            },
        )
        return session


class OpenCodeRuntimeSession:
    """One prompt on the managed service, steered and interrupted over HTTP."""

    def __init__(
        self,
        client: OpenCodeHttpClient,
        session_id: str,
        sink: EventSink,
        *,
        broker: PermissionBroker | None = None,
        agent: str = PRIMARY_AGENT,
    ) -> None:
        self._client = client
        self._session_id = session_id
        self._sink = sink
        self._broker = broker if broker is not None else PermissionBroker()
        self._agent = agent
        sink.session(session_id)

    @property
    def pid(self) -> int | None:
        return None  # the managed service owns the process, not this session

    def wait(self, timeout_seconds: float | None) -> Outcome | None:
        deadline = 480.0 if timeout_seconds is None else float(timeout_seconds)
        try:
            state = self._client.poll(
                f"/session/{self._session_id}", is_settled, deadline_seconds=deadline
            )
        except PollTimeout:
            return None
        outcome = normalize_outcome(state, runtime_session_id=self._session_id)
        for message in normalize_transcript(self._client.messages(self._session_id).mapping()):
            self._sink.message(message)
        blocked = self._broker.blocked_summary()
        if blocked:
            self._sink.event("permissions_blocked", blocked)
        return outcome

    def steer(self, text: str) -> None:
        if not isinstance(text, str) or not text.strip():
            raise ValidationError("steer text must be a nonblank string")
        self._client.steer(self._session_id, {"parts": [{"type": "text", "text": text}]})

    def cancel(self, grace_seconds: float) -> None:
        self._client.interrupt(self._session_id)

    def resolve_permissions(self, payload: Mapping[str, object]) -> tuple[PermissionDecision, ...]:
        decisions = []
        for permission in _sequence(payload.get("permissions")):
            if not isinstance(permission, Mapping):
                raise ValidationError("permission must be a mapping")
            decision = self._broker.decide(permission)
            self._client.answer_permission(
                self._session_id, decision.permission_id, self._broker.reply(decision)
            )
            decisions.append(decision)
        return tuple(decisions)


ADAPTER = OpenCodeAdapter()
