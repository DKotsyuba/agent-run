"""Run one workflow ``agent(...)`` step through the normal agent service."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Callable, Mapping, Protocol

from agent_run.domain import AgentStatus, StartRequest
from agent_run.errors import ValidationError
from agent_run.service import AgentService
from agent_run.state.store import StateStore


POLL_SECONDS = 0.05
_RAW_ANSWER_LIMIT = 4096
_SPEC_KEYS = frozenset(
    {
        "runtime",
        "model",
        "profile",
        "task",
        "workdir",
        "write",
        "read_roots",
        "timeout_seconds",
        "output_schema",
    }
)
_REQUIRED_SPEC_KEYS = frozenset({"runtime", "model", "profile", "task", "workdir"})


class _Stop(Protocol):
    """Expose whether the runner has received a cancellation signal."""

    @property
    def requested(self) -> bool:
        """Return whether the enclosing runner should cancel its active work."""


def validate_agent_spec(spec: object) -> StartRequest:
    """Convert one strict workflow mapping into the service's start request.

    The workflow language intentionally exposes only the stable subset of
    ``StartRequest`` fields. Paths and the optional schema are left to the
    domain object and service to validate with their normal policy checks.
    """

    if type(spec) is not dict:
        raise ValidationError("workflow agent spec must be a plain dict")
    unknown = sorted(set(spec) - _SPEC_KEYS)
    if unknown:
        raise ValidationError(
            f"workflow agent spec contains unknown keys: {', '.join(unknown)}"
        )
    missing = sorted(_REQUIRED_SPEC_KEYS - set(spec))
    if missing:
        raise ValidationError(
            f"workflow agent spec is missing required keys: {', '.join(missing)}"
        )
    read_roots = spec.get("read_roots", [])
    if type(read_roots) is not list:
        raise ValidationError("workflow agent spec read_roots must be a list")
    return StartRequest(
        runtime=spec["runtime"],  # type: ignore[arg-type]
        model=spec["model"],  # type: ignore[arg-type]
        profile=spec["profile"],  # type: ignore[arg-type]
        task=spec["task"],  # type: ignore[arg-type]
        workdir=Path(spec["workdir"]),  # type: ignore[arg-type]
        write=spec.get("write", False),  # type: ignore[arg-type]
        read_roots=tuple(Path(path) for path in read_roots),
        timeout_seconds=spec.get("timeout_seconds"),  # type: ignore[arg-type]
        output_schema=spec.get("output_schema"),  # type: ignore[arg-type]
    )


def validate_output(answer_text: str, schema: object) -> None:
    """Validate JSON answer text against the workflow's narrow schema subset.

    Supported schemas use ``type``, ``properties``, and ``required`` for
    objects, plus ``items`` for arrays. Values may use the JSON primitive
    types accepted by the runtime adapter. Invalid JSON and mismatches raise
    ``ValidationError`` with a concise, caller-facing explanation.
    """

    try:
        value = json.loads(answer_text)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValidationError("agent answer is not valid JSON") from error
    _validate_value(value, schema, "$")


def _validate_value(value: object, schema: object, path: str) -> None:
    """Check one JSON value recursively against a supported schema mapping."""

    if not isinstance(schema, dict):
        raise ValidationError("output_schema must be a JSON object schema")
    allowed = {"type", "properties", "required", "items"}
    unknown = sorted(set(schema) - allowed)
    if unknown:
        raise ValidationError(f"output_schema contains unsupported keys: {', '.join(unknown)}")
    kind = schema.get("type")
    if not isinstance(kind, str):
        raise ValidationError("output_schema type must be a string")
    checks: dict[str, Callable[[object], bool]] = {
        "object": lambda item: isinstance(item, dict),
        "array": lambda item: isinstance(item, list),
        "string": lambda item: isinstance(item, str),
        "number": lambda item: isinstance(item, (int, float)) and not isinstance(item, bool),
        "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "boolean": lambda item: isinstance(item, bool),
        "null": lambda item: item is None,
    }
    check = checks.get(kind)
    if check is None:
        raise ValidationError(f"output_schema type is unsupported: {kind}")
    if not check(value):
        raise ValidationError(f"agent answer at {path} must be {kind}")
    if kind == "object":
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        if not isinstance(properties, dict) or not isinstance(required, list):
            raise ValidationError("output_schema object properties and required must be mappings and lists")
        for name in required:
            if not isinstance(name, str):
                raise ValidationError("output_schema required entries must be strings")
            if name not in value:
                raise ValidationError(f"agent answer is missing required property: {path}.{name}")
        for name, child in properties.items():
            if not isinstance(name, str):
                raise ValidationError("output_schema property names must be strings")
            if name in value:
                _validate_value(value[name], child, f"{path}.{name}")
    elif kind == "array" and "items" in schema:
        for index, item in enumerate(value):
            _validate_value(item, schema["items"], f"{path}[{index}]")


class WorkflowStepExecutor:
    """Journal, launch, poll, and validate the agent behind workflow steps."""

    def __init__(
        self,
        home: str | Path,
        store: StateStore,
        run_id: str,
        *,
        service: AgentService,
        stop: _Stop | None = None,
        poll_seconds: float = POLL_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Bind execution to one run and injectable service/time dependencies."""

        self._home = Path(home).resolve()
        self._owner_thread_id = threading.get_ident()
        self._owner_store = store
        self._database_path = store.path()
        self._thread_local = threading.local()
        self._run_id = run_id
        self._service = service
        self._stop = stop
        self._poll_seconds = poll_seconds
        self._monotonic = monotonic
        self._sleep = sleep
        self.failed = False
        self.active_agent_id: str | None = None

    def _store_for_caller(self) -> StateStore:
        """Return a SQLite store owned by the current calling thread."""

        if threading.get_ident() == self._owner_thread_id:
            return self._owner_store
        store = getattr(self._thread_local, "store", None)
        if store is None:
            store = StateStore.open(self._database_path)
            self._thread_local.store = store
        return store

    def __call__(self, step_key: str, spec: dict[str, object]) -> dict[str, object]:
        """Execute and journal one spec, converting executor errors to failed results.

        Journal I/O errors are deliberately allowed to escape so a tolerant
        parallel script cannot turn a missing durable record into success.
        """

        try:
            return self._execute(step_key, spec)
        except BaseException as error:
            return self._fail(
                step_key, "step_executor_failed", {"exception": repr(error)}
            )

    def _execute(self, step_key: str, spec: dict[str, object]) -> dict[str, object]:
        """Execute one spec after establishing its durable journal row."""

        cached = self._store_for_caller().cached_step_result(self._run_id, step_key)
        if cached is not None:
            return cached  # type: ignore[return-value]
        self._store_for_caller().record_step_start(self._run_id, step_key, spec)
        try:
            request = validate_agent_spec(spec)
        except ValidationError as error:
            return self._fail(step_key, "step_spec_invalid", {"message": str(error)})
        try:
            started = self._service.start(request)
        except BaseException as error:
            return self._fail(step_key, "step_start_failed", {"exception": repr(error)})
        agent_id = str(started.agent_id)
        self.active_agent_id = agent_id
        deadline = None if request.timeout_seconds is None else self._monotonic() + request.timeout_seconds
        try:
            while True:
                if self._stop is not None and self._stop.requested:
                    self._cancel(agent_id)
                    return self._fail(
                        step_key, "runner_cancelled", {"agent_id": agent_id}
                    )
                if deadline is not None and self._monotonic() >= deadline:
                    self._cancel(agent_id)
                    return self._fail(
                        step_key,
                        "step_timeout",
                        {"agent_id": agent_id, "timeout_seconds": request.timeout_seconds},
                    )
                agent = self._service.get(agent_id)
                status = getattr(agent.status, "value", agent.status)
                if status in {item.value for item in AgentStatus if item not in {AgentStatus.CREATED, AgentStatus.STARTING, AgentStatus.RUNNING, AgentStatus.CANCELLING}}:
                    return self._terminal(step_key, agent_id, str(status), request.output_schema, agent)
                self._sleep(self._poll_seconds)
        finally:
            self.active_agent_id = None

    def _terminal(
        self, step_key: str, agent_id: str, status: str, schema: object, agent: object
    ) -> dict[str, object]:
        """Persist a terminal service view, validating successful inline output."""

        result: dict[str, object] = {"agent_id": agent_id, "status": status}
        answer = self._service.answer(agent_id)
        if getattr(answer, "available", False) and getattr(answer, "content", None) is not None:
            result["answer"] = answer.content
        if status == AgentStatus.SUCCEEDED.value:
            if schema is not None:
                try:
                    validate_output(result.get("answer", ""), schema)
                except ValidationError as error:
                    raw = result.get("answer", "")
                    params = {
                        "message": str(error),
                        "raw_answer": str(raw)[:_RAW_ANSWER_LIMIT],
                        "answer_path": None
                        if getattr(answer, "path", None) is None
                        else str(answer.path),
                    }
                    return self._fail(step_key, "step_output_invalid", params, result)
            self._store_for_caller().finish_step(
                self._run_id, step_key, "succeeded", result=result
            )
            return result
        kind = getattr(agent, "failure_kind", None) or "agent_failed"
        params = getattr(agent, "failure_params", None) or {"agent_id": agent_id, "status": status}
        result["failure_kind"] = kind
        result["failure_params"] = params
        return self._fail(step_key, str(kind), params, result)

    def _fail(
        self, step_key: str, kind: str, params: object, result: dict[str, object] | None = None
    ) -> dict[str, object]:
        """Finish a failed journal row and expose the same failure to scripts."""

        self.failed = True
        output = result or {"status": "failed"}
        output["failure_kind"] = kind
        output["failure_params"] = params
        self._store_for_caller().finish_step(
            self._run_id, step_key, "failed", failure_kind=kind, failure_params=params
        )
        return output

    def _cancel(self, agent_id: str) -> None:
        """Best-effort enqueue of the service cancellation command for one agent."""

        try:
            self._service.cancel(agent_id)
        except BaseException:
            pass


def make_step_executor(
    home: str | Path,
    store: StateStore,
    run_id: str,
    *,
    service: AgentService,
    stop: _Stop | None = None,
) -> WorkflowStepExecutor:
    """Create the callable that ``run_script`` invokes for this workflow run."""

    return WorkflowStepExecutor(home, store, run_id, service=service, stop=stop)
