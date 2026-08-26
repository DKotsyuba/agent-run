"""OpenCode adapter: managed isolated v2 service only.

There is no CLI fallback and no attachment to a user-owned global service. When
isolation is unproven the runtime reports unavailable and refuses to prepare.
The adapter never writes: OpenCode is used as a read-and-answer runtime, so
every write permission is denied in the generated config and refused in
``prepare`` before an agent row exists.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Sequence

from ...config import McpConfig, RuntimeConfig
from ...domain import AgentStatus, Message, MessageRole, Outcome, StartRequest
from ...errors import ValidationError
from ...profiles import AgentProfile, normalize_read_roots
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
from ..home import content_hash, write_managed_file
from .http import POLL_INTERVAL_SECONDS, OpenCodeHttpClient
from .service import (
    ServiceIsolationError,
    attach_service,
    build_service_plan,
    resolve_environment_names,
)


RUNTIME_NAME = "opencode"
PRIMARY_AGENT = "agent-run"
VERIFY_AGENT = "agent-run-verify"
CONFIG_RELATIVE_PATH = "xdg/config/opencode/opencode.json"
ANSWER_NAME = "answer.md"
DEFAULT_WAIT_SECONDS = 480.0
CAPABILITIES = frozenset(
    {
        Capability.STEER,
        Capability.OUTPUT_SCHEMA,
        Capability.READ_ROOTS,
        Capability.TRANSCRIPT,
        Capability.MODEL_ROSTER,
        Capability.MCP,
        Capability.SKILLS,
    }
)

EXTERNAL_DIRECTORY = "external_directory"

#: Ordered on purpose: the contained one-time grant is the last word, so no
#: earlier entry can widen it and no later entry can shadow it.
SYSTEM_PERMISSION: tuple[tuple[str, str], ...] = (
    ("bash", "deny"),
    ("edit", "deny"),
    ("write", "deny"),
    ("webfetch", "deny"),
    (EXTERNAL_DIRECTORY, "ask"),
)
PRIMARY_PERMISSION = SYSTEM_PERMISSION
#: A sub-agent never gets even the contained grant; only the primary may ask.
SUBAGENT_PERMISSION: tuple[tuple[str, str], ...] = (
    ("bash", "deny"),
    ("edit", "deny"),
    ("write", "deny"),
    ("webfetch", "deny"),
    (EXTERNAL_DIRECTORY, "deny"),
)

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
#: ``retrying`` is work, not a settled turn: the service is between attempts.
_ACTIVE_STATES = frozenset(
    {"running", "busy", "pending", "queued", "streaming", "retry", "retrying"}
)
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
        identifier = permission_id(permission)
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
        """The exact body the v2 permission reply endpoint accepts."""

        return MappingProxyType({"response": "once" if decision.granted else "reject"})


def permission_id(permission: Mapping[str, object]) -> str:
    identifier = permission.get("id")
    if not isinstance(identifier, str) or not identifier.strip():
        raise ValidationError("permission id must be a nonblank string")
    return identifier


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


# --- models --------------------------------------------------------------


def split_model(value: object) -> tuple[str, str]:
    """Split a canonical ``providerID/modelID`` identifier, or refuse it."""

    if not isinstance(value, str) or value.count("/") != 1:
        raise ValidationError(
            f"opencode model must be canonical 'providerID/modelID', not {value!r}"
        )
    provider, model = value.split("/", 1)
    if not provider.strip() or not model.strip():
        raise ValidationError(
            f"opencode model must be canonical 'providerID/modelID', not {value!r}"
        )
    return provider, model


def model_reference(value: str) -> Mapping[str, str]:
    """The v2 prompt body's model shape."""

    provider, model = split_model(value)
    return {"providerID": provider, "modelID": model}


def normalize_models(payload: Mapping[str, object], allowed: Sequence[str]) -> tuple[ModelInfo, ...]:
    """Intersect the reported roster with the configured allowlist, in order."""

    if not isinstance(payload, Mapping):
        raise ValidationError("opencode model roster must be a mapping")
    reported: dict[str, str] = {}
    for provider in _sequence(payload.get("providers")):
        if not isinstance(provider, Mapping):
            raise ValidationError("opencode provider entry must be a mapping")
        provider_id = provider.get("id")
        models = provider.get("models")
        items = models.values() if isinstance(models, Mapping) else _sequence(models)
        for model in items:
            if not isinstance(model, Mapping):
                raise ValidationError("opencode model entry must be a mapping")
            identifier = model.get("id")
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValidationError("opencode model id must be a nonblank string")
            if isinstance(provider_id, str) and provider_id.strip():
                identifier = f"{provider_id}/{identifier}"
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


# --- transcript ----------------------------------------------------------


def _info(item: Mapping[str, object]) -> Mapping[str, object]:
    """A v2 message is ``{info, parts}``; the metadata lives in ``info``."""

    info = item.get("info")
    if isinstance(info, Mapping):
        return info
    if info is not None:
        raise ValidationError("opencode message info must be a mapping")
    return item


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
    info = _info(item)
    time_value = info.get("time")
    raw = time_value.get("created") if isinstance(time_value, Mapping) else info.get("created")
    if isinstance(raw, bool) or not isinstance(raw, (int, float)) or raw < 0:
        return 0.0
    return float(raw)


def _role(item: Mapping[str, object]) -> MessageRole:
    raw = _info(item).get("role")
    try:
        return _ROLES[raw]  # type: ignore[index]
    except (KeyError, TypeError) as error:
        raise ValidationError(f"unknown opencode message role: {raw!r}") from error


def _agent(item: Mapping[str, object]) -> str:
    value = _info(item).get("agent")
    return value if isinstance(value, str) and value.strip() else PRIMARY_AGENT


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
    """Every assistant text this agent produced in the session, in order.

    One session is one agent run, so nothing in it predates the task. Steering
    inserts a real user message mid-run and retries insert a synthetic one;
    neither starts a new answer, so text written before them is preserved. A
    sub-agent's output is never mistaken for the primary agent's answer.
    """

    return "\n\n".join(
        text
        for item in _messages(payload)
        if _role(item) is MessageRole.ASSISTANT and _agent(item) == agent
        for text in (_text_parts(item),)
        if text
    )


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


def session_state(status: Mapping[str, object]) -> str | None:
    raw = status.get("state", status.get("status")) if isinstance(status, Mapping) else None
    return raw if isinstance(raw, str) else None


def is_settled(state: Mapping[str, object]) -> bool:
    raw = session_state(state)
    return raw is not None and raw not in _ACTIVE_STATES and raw in _TERMINAL_STATES


def is_working(state: Mapping[str, object]) -> bool:
    return session_state(state) in _ACTIVE_STATES


# --- generated config ----------------------------------------------------


def _mcp_servers(
    config: RuntimeConfig,
    mcp_servers: Mapping[str, McpConfig],
    inherited: Mapping[str, str],
) -> dict[str, object]:
    """Render the full stdio definition of every selected MCP, and only those."""

    if not isinstance(mcp_servers, Mapping):
        raise ValidationError("mcp_servers must be a mapping of resolved MCP definitions")
    servers: dict[str, object] = {}
    for name in config.mcp:
        definition = mcp_servers.get(name)
        if definition is None:
            raise ValidationError(
                f"runtimes.opencode.mcp names {name!r}, which is not in the resolved "
                "mcp_servers mapping; adapters never read ambient config"
            )
        if not isinstance(definition, McpConfig):
            raise ValidationError(f"resolved MCP {name!r} must be an McpConfig")
        if definition.transport != "stdio":
            raise ValidationError(
                f"MCP {name!r} uses transport {definition.transport!r}; the opencode "
                "adapter accepts stdio servers only"
            )
        servers[name] = {
            "type": "local",
            "command": [str(definition.command), *definition.args],
            "environment": resolve_environment_names(
                definition.env_from, inherited, what=f"mcp.{name}.env_from"
            ),
            "enabled": True,
        }
    return servers


def render_config(
    config: RuntimeConfig,
    mcp_servers: Mapping[str, McpConfig],
    *,
    inherited_environment: Mapping[str, str] | None = None,
) -> str:
    """Render the generated v2 service config as exact bytes-to-be."""

    inherited = os.environ if inherited_environment is None else inherited_environment
    default_model = config.models[0]
    for model in config.models:
        split_model(model)
    document = {
        "$schema": "https://opencode.ai/config.json",
        "share": "disabled",
        "autoupdate": False,
        "model": default_model,
        "permission": dict(SYSTEM_PERMISSION),
        "agents": {
            PRIMARY_AGENT: {
                "mode": "primary",
                "model": default_model,
                "permission": dict(PRIMARY_PERMISSION),
            },
            VERIFY_AGENT: {
                "mode": "subagent",
                "model": default_model,
                "permission": dict(SUBAGENT_PERMISSION),
            },
        },
        "skills": list(config.skills),
        "mcp": {"servers": _mcp_servers(config, mcp_servers, inherited)},
    }
    # Key order is meaningful here: permissions are read in the order written.
    return json.dumps(document, indent=2, sort_keys=False) + "\n"


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
    split_model(request.model)
    if request.effort is not None:
        raise ValidationError("opencode does not support effort levels")
    if request.write:
        raise ValidationError(
            "the opencode runtime has no write capability; every write permission is denied"
        )
    _check_schema(request.output_schema)


def read_roots_for(request: StartRequest, profile: AgentProfile) -> tuple[Path, ...]:
    """The union of the profile's and the request's roots, as one antichain."""

    return normalize_read_roots([*profile.read_roots, *request.read_roots])


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
        for model in config.models:
            split_model(model)

    def materialize(
        self,
        config: RuntimeConfig,
        home: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
        inherited_environment: Mapping[str, str] | None = None,
    ) -> str:
        """Render the generated service config; nothing outside home is touched."""

        self.validate(config)
        content = render_config(
            config, mcp_servers, inherited_environment=inherited_environment
        )
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
            descriptor = attach_service(home, Path(home) / CONFIG_RELATIVE_PATH)
        except ValidationError as error:
            return RuntimeHealth(False, None, None, str(error))
        return RuntimeHealth(True, descriptor.version, None, None)

    def models(self, config: RuntimeConfig, home: Path) -> tuple[ModelInfo, ...]:
        """Report the roster only from a proven service, never a global one."""

        try:
            descriptor = attach_service(home, Path(home) / CONFIG_RELATIVE_PATH)
        except ValidationError:
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
        *,
        mcp_servers: Mapping[str, McpConfig],
        inherited_environment: Mapping[str, str] | None = None,
    ) -> LaunchPlan:
        self.validate(config)
        _check_request(request, profile, config)
        inherited = os.environ if inherited_environment is None else inherited_environment
        descriptor = attach_service(home, Path(home) / CONFIG_RELATIVE_PATH)
        rendered = render_config(config, mcp_servers, inherited_environment=inherited)
        if content_hash(rendered) != descriptor.config_hash:
            raise ServiceIsolationError(
                "the running opencode service was proven with a different generated "
                "config; refusing to prompt it before it is re-materialized and re-proven"
            )
        # The proven service already serves this endpoint: never a second serve.
        plan = build_service_plan(
            config,
            home,
            port=descriptor.port,
            inherited_environment=inherited,
            argv=(),
        )
        state = {
            "service": {
                "base_url": descriptor.base_url,
                "config_home": str(descriptor.config_home),
                "data_home": str(descriptor.data_home),
                "pid": descriptor.pid,
                "config_hash": descriptor.config_hash,
            },
            "agent": PRIMARY_AGENT,
            "verify_agent": VERIFY_AGENT,
            "model": dict(model_reference(request.model)),
            "workdir": str(request.workdir),
            "read_roots": [str(root) for root in read_roots_for(request, profile)],
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
        response_dir = Path(str(state["response_dir"]))
        if client is None:
            client = OpenCodeHttpClient(str(service["base_url"]), response_dir)
        model = state["model"]
        if not isinstance(model, Mapping) or "modelID" not in model:
            raise ValidationError("launch plan carries no canonical opencode model")
        opened = client.create_session({"agent": PRIMARY_AGENT, "title": str(model["modelID"])})
        session_id = opened.get("id")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValidationError("opencode service returned no session id")
        roots = tuple(Path(str(root)) for root in _sequence(state.get("read_roots")))
        session = OpenCodeRuntimeSession(
            client,
            session_id,
            sink,
            broker=PermissionBroker(roots),
            pid=service.get("pid"),
            response_dir=response_dir,
            model=dict(model),
        )
        client.prompt_async(
            session_id,
            {
                "agent": PRIMARY_AGENT,
                "model": dict(model),
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
        pid: object = None,
        response_dir: Path | None = None,
        model: Mapping[str, str] | None = None,
        sleep=time.sleep,
        monotonic=time.monotonic,
        interval_seconds: float = POLL_INTERVAL_SECONDS,
    ) -> None:
        self._client = client
        self._session_id = session_id
        self._sink = sink
        self._broker = broker if broker is not None else PermissionBroker()
        self._agent = agent
        self._pid = pid if isinstance(pid, int) and not isinstance(pid, bool) else None
        self._response_dir = response_dir
        self._model = dict(model) if model else None
        self._sleep = sleep
        self._monotonic = monotonic
        self._interval = float(interval_seconds)
        self._answered: set[str] = set()
        sink.session(session_id)

    @property
    def pid(self) -> int | None:
        """The proven service's pid; the session does not own a process."""

        return self._pid

    def wait(self, timeout_seconds: float | None) -> Outcome | None:
        """Settle this session, answering its permissions while it works.

        The service reports ``idle`` for a session it has not begun working on,
        so an initial idle is not an outcome: the turn is only settled once the
        session has been seen busy or retrying, or the primary agent has
        actually produced output.
        """

        deadline = DEFAULT_WAIT_SECONDS if timeout_seconds is None else float(timeout_seconds)
        if deadline <= 0:
            raise ValidationError("opencode wait deadline must be positive")
        started = self._monotonic()
        working = False
        while True:
            self.resolve_permissions()
            status = self._status()
            if is_working(status):
                working = True
            elif is_settled(status):
                capture = self._client.messages(self._session_id)
                try:
                    payload = capture.json()
                    final = working or bool(extract_answer(payload, agent=self._agent))
                except BaseException:
                    capture.release()
                    raise
                if final:
                    return self._finish(status, payload, capture)
                capture.release()
            if self._monotonic() - started >= deadline:
                return None
            self._sleep(self._interval)

    def _status(self) -> Mapping[str, object]:
        statuses = self._client.session_status()
        if not isinstance(statuses, Mapping):
            raise ValidationError("opencode session status must map session id to status")
        entry = statuses.get(self._session_id)
        if entry is None:
            return {}
        if isinstance(entry, str):
            return {"state": entry}
        if not isinstance(entry, Mapping):
            raise ValidationError("opencode session status entry must be a mapping or a string")
        return entry

    def _finish(self, status, payload, capture) -> Outcome:
        """Emit the transcript, record answer.md, and keep only this capture."""

        outcome = normalize_outcome(status, runtime_session_id=self._session_id)
        for message in normalize_transcript(payload, raw_ref=capture.raw_ref):
            self._sink.message(message)
        answer = extract_answer(payload, agent=self._agent)
        if answer and self._response_dir is not None:
            data = answer.encode("utf-8")
            digest = write_managed_file(self._response_dir, ANSWER_NAME, data)
            outcome = replace(
                outcome,
                answer_path=Path(self._response_dir) / ANSWER_NAME,
                answer_bytes=len(data),
                answer_sha256=digest,
            )
        blocked = self._broker.blocked_summary()
        if blocked:
            self._sink.event("permissions_blocked", blocked)
        return outcome

    def steer(self, text: str) -> None:
        if not isinstance(text, str) or not text.strip():
            raise ValidationError("steer text must be a nonblank string")
        payload: dict[str, object] = {
            "agent": self._agent,
            "parts": [{"type": "text", "text": text}],
        }
        if self._model is not None:
            payload["model"] = dict(self._model)
        self._client.prompt_async(self._session_id, payload)

    def cancel(self, grace_seconds: float) -> None:
        self._client.abort(self._session_id)

    def resolve_permissions(self) -> tuple[PermissionDecision, ...]:
        """Answer each pending permission of this session exactly once."""

        capture = self._client.permissions(self._session_id)
        try:
            payload = capture.json()
        finally:
            capture.release()
        decisions = []
        for permission in _permission_items(payload):
            identifier = permission_id(permission)
            if identifier in self._answered:
                continue
            owner = permission.get("sessionID")
            if isinstance(owner, str) and owner != self._session_id:
                continue
            decision = self._broker.decide(permission)
            self._answered.add(identifier)
            self._client.answer_permission(
                self._session_id, identifier, self._broker.reply(decision)
            )
            decisions.append(decision)
        return tuple(decisions)


def _permission_items(payload: object) -> tuple[Mapping[str, object], ...]:
    if payload is None:
        return ()
    items = payload.get("permissions") if isinstance(payload, Mapping) else payload
    result = []
    for item in _sequence(items):
        if not isinstance(item, Mapping):
            raise ValidationError("permission must be a mapping")
        result.append(item)
    return tuple(result)


ADAPTER = OpenCodeAdapter()
