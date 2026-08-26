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
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from ...config import McpConfig, RuntimeConfig
from ...domain import Outcome, StartRequest
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
from ..home import content_hash, seal_answer, write_managed_file
from .http import POLL_INTERVAL_SECONDS, OpenCodeHttpClient
from .normalize import (
    PRIMARY_AGENT,
    _sequence,
    extract_answer,
    is_settled,
    is_working,
    model_reference,
    normalize_models,
    normalize_outcome,
    normalize_transcript,
    session_state,
    split_model,
)
from .permissions import (
    EXTERNAL_DIRECTORY,
    PermissionBroker,
    PermissionDecision,
    permission_id,
    permission_items as _permission_items,
)
from .service import (
    ServiceIsolationError,
    attach_service,
    build_service_plan,
    resolve_environment_names,
)


RUNTIME_NAME = "opencode"
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
            "disabled": False,
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
        "default_agent": PRIMARY_AGENT,
        "providers": {
            "omniroute": {
                "name": "OmniRoute",
                "env": ["OMNIROUTE_API_KEY"],
                "package": "@opencode-ai/ai/providers/openai-compatible",
                "settings": {"baseURL": "http://127.0.0.1:20128/v1", "apiKey": "{env:OMNIROUTE_API_KEY}"},
                "models": {
                    model.split("/", 1)[1]: {"modelID": f"opencode/{model.split('/', 1)[1]}"}
                    for model in config.models
                },
            }
        },
        "agents": {
            PRIMARY_AGENT: {
                "mode": "primary",
                "model": default_model,
                "permissions": [
                    {"action": action, "resource": resource, "effect": "allow"}
                    for action, resource in PRIMARY_PERMISSION
                ],
            },
            VERIFY_AGENT: {
                "mode": "subagent",
                "model": default_model,
                "permissions": [
                    {"action": action, "resource": resource, "effect": "allow"}
                    for action, resource in SUBAGENT_PERMISSION
                ],
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
        skills_root: Path | None = None,
        inherited_environment: Mapping[str, str] | None = None,
    ) -> str:
        """Render the generated service config; nothing outside home is touched."""

        self.validate(config)
        if skills_root is None:
            skills_root = Path(home)
        if not isinstance(skills_root, Path) or not skills_root.is_absolute():
            raise ValidationError("opencode skills_root must be absolute")
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
            answer_path=agent_dir / ANSWER_NAME,
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
        opened = client.create_session(
            {"agent": PRIMARY_AGENT, "model": dict(model), "title": str(model["modelID"])}
        )
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
                "text": plan.initial_input,
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

    @property
    def owns_process_group(self) -> bool:
        return False

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
            path = Path(self._response_dir) / ANSWER_NAME
            size, digest = seal_answer(path, answer)
            outcome = replace(
                outcome,
                answer_path=path,
                answer_bytes=size,
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
        if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), list):
            raise ValidationError("opencode permission reply must contain a data array")
        decisions = []
        for permission in _permission_items(payload["data"]):
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


ADAPTER = OpenCodeAdapter()
