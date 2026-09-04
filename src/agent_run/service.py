"""Typed application service shared by CLI and MCP transports."""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping, TypeAlias

from .adapters.base import Capability, LaunchPlan, ModelInfo, RuntimeAdapter
from .adapters.registry import AdapterRegistry
from .accounts import account_auth_source, account_runtime_home
from .capacity.advice import CapacityAdvice, build_advice
from .capacity.forecast import build_forecasts
from .capacity.history import load_series
from .config import Config, McpConfig, RuntimeConfig, load_config
from .domain import (
    ACTIVE,
    TERMINAL,
    AgentId,
    AgentStatus,
    OrchestratorRef,
    Outcome,
    StartRequest,
    new_agent_id,
    validate_agent_id,
)
from .errors import AuthError, StateTransitionError, ValidationError
from .delivery.base import DeliveryAttemptEvidence
from .launch import launch_cancellation
from .launch_evidence import SupervisorBootstrapError, bootstrap_event_data
from .paths import agent_dir, config_path, create_agent_dir, runtime_skills_dir, state_db_path
from . import workflow_facade
from .profiles import assign_role, load_profile
from .start_coordinator import StartCoordinator
from .state.reconciliation import workflow_owner_identity
from .supervisor import supervisor_identity
from .state.store import StateStore


_logger = logging.getLogger("agent_run.service")

_SUMMARY_LIMIT = 50
_TASK_SUMMARY_CHARS = 160
_DEFAULT_INLINE_ANSWER_BYTES = 1024 * 1024
_MAX_PAGE_SIZE = 1000
_FAILURE_TEXT_CHARS = 512
_CHUNK = 65536
_PENDING_CONFIG_REVISION = "pending:materialization"

LaunchAgent: TypeAlias = Callable[
    [AgentId, StartRequest, RuntimeAdapter, LaunchPlan, Path], None
]


def _page_limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValidationError("limit must be a positive integer")
    if value > _MAX_PAGE_SIZE:
        raise ValidationError(f"limit must not exceed {_MAX_PAGE_SIZE}")
    return value


def _failure_text(error: BaseException) -> str:
    return (str(error).strip() or type(error).__name__)[:_FAILURE_TEXT_CHARS]


@dataclass(frozen=True, slots=True)
class DeliveryView:
    """Current delivery state plus the latest bounded subprocess evidence."""

    agent_id: AgentId
    bound: bool
    orchestrator_session_id: str | None
    notification_id: str | None
    state: str
    attempts: int
    ambiguous: bool
    last_error: str | None
    last_attempt: DeliveryAttemptEvidence | None


@dataclass(frozen=True, slots=True)
class AgentView:
    agent_id: AgentId
    runtime: str
    model: str
    profile: str
    task_summary: str
    status: AgentStatus
    created_at: float
    started_at: float | None
    finished_at: float | None
    elapsed_seconds: float
    last_progress_at: float | None
    silence_seconds: float | None
    warned: bool
    failure_kind: str | None
    failure_text: str | None
    answer_available: bool
    answer_bytes: int | None
    answer_sha256: str | None
    delivery: DeliveryView


@dataclass(frozen=True, slots=True)
class StartResult:
    agent_id: AgentId
    created: bool
    agent: AgentView


@dataclass(frozen=True, slots=True)
class AgentQuery:
    active: bool = False
    orchestrator: OrchestratorRef | None = None
    offset: int = 0
    limit: int = 100

    def __post_init__(self) -> None:
        if not isinstance(self.active, bool):
            raise ValidationError("active must be a boolean")
        if self.orchestrator is not None and not isinstance(
            self.orchestrator, OrchestratorRef
        ):
            raise ValidationError("orchestrator must be an OrchestratorRef or None")
        if (
            isinstance(self.offset, bool)
            or not isinstance(self.offset, int)
            or self.offset < 0
        ):
            raise ValidationError("offset must be a nonnegative integer")
        _page_limit(self.limit)


@dataclass(frozen=True, slots=True)
class AgentPage:
    items: tuple[AgentView, ...]
    total: int
    offset: int
    limit: int
    next_offset: int | None
    complete: bool


@dataclass(frozen=True, slots=True)
class CommandView:
    command_id: int
    agent_id: AgentId
    kind: str
    state: str = "pending"


@dataclass(frozen=True, slots=True)
class MessageView:
    seq: int
    at: float
    role: str
    name: str | None
    content: str
    raw_ref: str | None


@dataclass(frozen=True, slots=True)
class TranscriptPage:
    agent_id: AgentId
    messages: tuple[MessageView, ...]
    cursor: int
    limit: int
    next_cursor: int | None
    complete: bool


@dataclass(frozen=True, slots=True)
class AnswerView:
    agent_id: AgentId
    status: AgentStatus
    available: bool
    path: Path | None
    size_bytes: int | None
    sha256: str | None
    content: str | None
    inline_complete: bool


@dataclass(frozen=True, slots=True)
class WorkSummary:
    scope: str
    agent_id: AgentId | None
    orchestrator: OrchestratorRef | None
    agents: tuple[AgentView, ...]
    total: int
    complete: bool


@dataclass(frozen=True, slots=True)
class CapacityReport:
    observed_at: float
    items: tuple[CapacityAdvice, ...]


@dataclass(frozen=True, slots=True)
class RuntimeModels:
    """One runtime's model discovery snapshot: roster plus capability/health context.

    ``models`` keeps the pre-existing :class:`ModelInfo` shape unchanged so
    current consumers of individual model entries do not break. ``capabilities``
    is the adapter's declared :class:`Capability` set (sorted string values), so
    a router can check whether a runtime supports a requested right (e.g.
    ``write``) before selecting it. ``available`` is ``False`` whenever the
    adapter is unhealthy or its roster is empty; ``reason`` carries the
    adapter-supplied explanation, falling back to ``"roster empty"`` when the
    adapter reports itself healthy but the roster still came back empty.
    """

    models: tuple[ModelInfo, ...]
    capabilities: tuple[str, ...]
    available: bool
    reason: str | None


class AgentService:
    """Validate once, persist once, and expose stable transport-neutral views."""

    def __init__(
        self,
        config: Config,
        store: StateStore,
        home: str | Path,
        *,
        launch: LaunchAgent,
        now: Callable[[], float] = time.time,
        max_inline_answer_bytes: int = _DEFAULT_INLINE_ANSWER_BYTES,
    ) -> None:
        """Create the service and its thread-isolated start coordinator.

        The supplied store remains on the service thread. Start workers retain
        only its database path and open their own thread-affine connections.
        """

        if not isinstance(config, Config):
            raise ValidationError("config must be a Config")
        if not isinstance(store, StateStore):
            raise ValidationError("store must be a StateStore")
        if not callable(launch) or not callable(now):
            raise ValidationError("launch and now must be callable")
        if (
            isinstance(max_inline_answer_bytes, bool)
            or not isinstance(max_inline_answer_bytes, int)
            or max_inline_answer_bytes < 1
        ):
            raise ValidationError("max_inline_answer_bytes must be a positive integer")
        self._config = config
        self._store = store
        self._home = Path(home).expanduser().resolve()
        self._registry = AdapterRegistry(config)
        self._launch = launch
        self._now = now
        self._max_inline_answer_bytes = max_inline_answer_bytes
        self._starts = StartCoordinator(
            store.path(), max_workers=config.core.max_active_agents
        )

    @classmethod
    def from_home(
        cls,
        home: str | Path,
        *,
        launch: LaunchAgent,
        now: Callable[[], float] = time.time,
        max_inline_answer_bytes: int = _DEFAULT_INLINE_ANSWER_BYTES,
    ) -> AgentService:
        """Compose the shared service around an initialized agent-run home."""

        root = Path(home).expanduser().resolve()
        return cls(
            load_config(config_path(root)),
            StateStore.open(state_db_path(root)),
            root,
            launch=launch,
            now=now,
            max_inline_answer_bytes=max_inline_answer_bytes,
        )

    def close(self) -> None:
        """Signal pre-ownership starts before closing the service store."""

        self._starts.close()
        self._store.close()

    def start(self, request: StartRequest) -> StartResult:
        """Durably accept one start and return before slow runtime bootstrap.

        Validation, replay, capacity admission, row creation, and ``STARTING``
        transition are synchronous. Materialization, authentication, prepare,
        detached spawn, and READY run on a thread-isolated coordinator worker.
        """

        if not isinstance(request, StartRequest):
            raise ValidationError("request must be a StartRequest")
        _logger.info(
            "start runtime=%s model=%s profile=%s write=%s request_id=%s",
            request.runtime, request.model, request.profile, request.write,
            request.request_id,
        )
        if request.timeout_seconds is None:
            request = replace(
                request,
                timeout_seconds=self._config.core.default_timeout_seconds,
            )
        runtime = self._runtime_config(request.runtime)
        label = request.account or runtime.default_account
        if request.account is not None and not runtime.accounts:
            raise ValidationError(f"runtime {request.runtime} declares no accounts")
        if label is not None and label not in runtime.accounts:
            known = ", ".join(runtime.accounts) or "none"
            raise ValidationError(
                f"account {label!r} is not declared for runtime {request.runtime}; known accounts: {known}"
            )
        if label is not None:
            if runtime.auth is None or runtime.auth.target is None:
                raise ValidationError(f"runtime {request.runtime} account auth is not configured")
        adapter = self._registry.load(
            request.runtime, self._required_capabilities(request, runtime)
        )
        adapter.validate(runtime)
        _logger.debug("start gate=capabilities ok runtime=%s", request.runtime)
        if request.model not in runtime.models:
            _logger.warning(
                "start gate=model_configured failed runtime=%s model=%s",
                request.runtime, request.model,
            )
            raise ValidationError(
                f"model is not configured for runtime {request.runtime}: {request.model}"
            )
        candidate = new_agent_id()
        accepted_at = self._now()
        creation = self._store.create_agent_limited(
            request,
            task_summary=self._task_summary(request.task),
            config_revision=_PENDING_CONFIG_REVISION,
            global_limit=self._config.core.max_active_agents,
            runtime_limit=runtime.max_active_agents,
            agent_id=candidate,
            at=accepted_at,
        )
        if not creation.created:
            _logger.info("start agent_id=%s created=False (idempotent replay)", creation.agent_id)
            return StartResult(
                creation.agent_id, False, self.get(creation.agent_id)
            )
        _logger.info("start agent_id=%s created=True", creation.agent_id)
        self._store.transition(
            creation.agent_id,
            AgentStatus.STARTING,
            kind="start_accepted",
            at=accepted_at,
        )
        try:
            self._store.claim_startup(
                creation.agent_id,
                workflow_owner_identity(os.getpid(), supervisor_identity()),
                at=accepted_at,
            )
            self._starts.submit(
                creation.agent_id,
                lambda worker_store, cancelled: self._continue_start(
                    worker_store,
                    cancelled,
                    creation.agent_id,
                    request,
                    runtime,
                    label,
                ),
            )
        except Exception as error:
            _logger.warning(
                "start agent_id=%s failed stage=submit error_kind=%s",
                creation.agent_id, type(error).__name__,
            )
            self._fail_created_start(
                creation.agent_id, error, "start_submit_failed", store=self._store
            )
        return StartResult(
            creation.agent_id, True, self.get(creation.agent_id)
        )


    def _continue_start(
        self,
        store: StateStore,
        cancelled: threading.Event,
        agent_id: AgentId,
        request: StartRequest,
        runtime: RuntimeConfig,
        account_label: str | None,
    ) -> None:
        """Materialize and launch one already accepted start.

        ``store`` belongs to this worker thread. Cancellation is checked before
        work, after authentication/materialization/prepare, and before spawn.
        Post-accept failures are always converted to durable outcomes.
        """

        failure_kind = "prepare_failed"
        try:
            if self._cancel_accepted_start(store, cancelled, agent_id):
                return
            effective_runtime = runtime
            effective_home = runtime.home
            if account_label is not None:
                if runtime.auth is None or runtime.auth.target is None:
                    raise ValidationError(
                        f"runtime {request.runtime} account auth is not configured"
                    )
                effective_source = account_auth_source(
                    self._home, request.runtime, account_label, runtime.auth.target
                )
                if not effective_source.is_file():
                    raise ValidationError(
                        f"account {account_label!r} is not authenticated; "
                        f"run agent-run auth {account_label} {request.runtime}"
                    )
                effective_home = account_runtime_home(runtime.home, account_label)
                effective_home.mkdir(mode=0o700, parents=True, exist_ok=True)
                effective_home.chmod(0o700)
                effective_runtime = replace(
                    runtime,
                    home=effective_home,
                    auth=replace(runtime.auth, source=effective_source),
                )
            if self._cancel_accepted_start(store, cancelled, agent_id):
                return

            adapter = AdapterRegistry(self._config).load(
                request.runtime,
                self._required_capabilities(request, effective_runtime),
            )
            adapter.validate(effective_runtime)
            roster = adapter.models(effective_runtime, effective_home)
            if request.model not in {model.id for model in roster}:
                raise ValidationError(
                    f"model is not available for runtime {request.runtime}: {request.model}"
                )
            profile = assign_role(
                load_profile(
                    self._config.profiles,
                    request.profile,
                    requested_write=request.write,
                    read_roots=request.read_roots,
                ),
                request.runtime,
                effective_runtime.skills,
            )
            mcp_servers = self._mcp_servers(effective_runtime)
            revision = adapter.materialize(
                effective_runtime,
                effective_home,
                mcp_servers=mcp_servers,
                skills_root=runtime_skills_dir(request.runtime, self._home),
            )
            store.replace_config_revision(
                agent_id, _PENDING_CONFIG_REVISION, revision
            )
            _logger.info(
                "start materialized runtime=%s revision=%s",
                request.runtime,
                revision,
            )
            if self._cancel_accepted_start(store, cancelled, agent_id):
                return

            candidate_dir = create_agent_dir(agent_id, self._home)
            plan = adapter.prepare(
                request,
                profile,
                effective_runtime,
                effective_home,
                candidate_dir,
                mcp_servers=mcp_servers,
            )
            if self._cancel_accepted_start(store, cancelled, agent_id):
                return
            with launch_cancellation(
                lambda: cancelled.is_set() or store.has_pending_cancel(agent_id)
            ):
                if self._cancel_accepted_start(store, cancelled, agent_id):
                    return
                if not store.startup_preparation_live(agent_id, at=self._now()):
                    _logger.warning("start agent_id=%s expired before supervisor spawn", agent_id)
                    return
                failure_kind = "supervisor_start_failed"
                self._launch(agent_id, request, adapter, plan, candidate_dir)
            _logger.info("start agent_id=%s done", agent_id)
        except BaseException as error:
            if self._cancel_accepted_start(store, cancelled, agent_id):
                return
            _logger.warning(
                "start agent_id=%s failed asynchronous error_kind=%s",
                agent_id,
                type(error).__name__,
            )
            self._fail_created_start(
                agent_id, error, failure_kind, store=store
            )

    def _cancel_accepted_start(
        self,
        store: StateStore,
        cancelled: threading.Event,
        agent_id: AgentId,
    ) -> bool:
        """Commit pre-ownership cancellation when either signal is durable."""

        if not cancelled.is_set() and not store.has_pending_cancel(agent_id):
            return False
        status = AgentStatus(str(store.get_agent(agent_id)["status"]))
        if status in TERMINAL:
            return True
        if status not in {
            AgentStatus.CREATED,
            AgentStatus.STARTING,
            AgentStatus.CANCELLING,
        }:
            return True
        try:
            store.transition(
                agent_id,
                AgentStatus.CANCELLED,
                outcome=Outcome(AgentStatus.CANCELLED),
                kind="start_cancelled",
                at=self._now(),
            )
        except StateTransitionError:
            if AgentStatus(str(store.get_agent(agent_id)["status"])) not in TERMINAL:
                raise
        return True

    def _fail_created_start(
        self,
        agent_id: AgentId,
        error: BaseException,
        kind: str,
        *,
        store: StateStore | None = None,
    ) -> tuple[str, str | None, str]:
        """Persist an accepted-start failure through the caller-owned store."""

        # A missing or unrenewable credential is its own diagnosis, not a
        # generic prepare/launch fault: name it so the row says what to fix.
        stage: str | None = None
        if isinstance(error, AuthError):
            kind = "auth_failed"
        elif isinstance(error, SupervisorBootstrapError):
            kind, stage = error.failure_kind, error.failure_stage
        outcome = Outcome(
            AgentStatus.FAILED, failure_kind=kind, failure_text=_failure_text(error)
        )
        target_store = self._store if store is None else store
        target_store.transition(
            agent_id,
            AgentStatus.FAILED,
            outcome=outcome,
            kind=kind,
            data=bootstrap_event_data(agent_id, error),
            at=self._now(),
        )
        return kind, stage, outcome.failure_text

    def bind(
        self, agent_id: str | AgentId, orchestrator: OrchestratorRef
    ) -> DeliveryView:
        if not isinstance(orchestrator, OrchestratorRef):
            raise ValidationError("orchestrator must be an OrchestratorRef")
        session_id = self._store.bind_orchestrator(
            agent_id, orchestrator, at=self._now()
        )
        _logger.info("bind agent_id=%s transport=%s", agent_id, orchestrator.transport)
        return self._delivery_view(validate_agent_id(agent_id), session_id)

    def cancel(self, agent_id: str | AgentId) -> AgentView:
        """Persist cancellation before signalling a pre-ownership worker."""

        self._store.enqueue_command(agent_id, "cancel", {}, at=self._now())
        self._starts.cancel(agent_id)
        _logger.info("cancel agent_id=%s", agent_id)
        return self.get(agent_id)

    def workflow_start(
        self,
        name: str,
        script: str,
        args: dict | None = None,
        orchestrator: OrchestratorRef | None = None,
    ) -> dict[str, str]:
        """Launch a script workflow. See `workflow_facade.workflow_start`."""

        return workflow_facade.workflow_start(self._home, name, script, args, orchestrator)

    def workflow_status(self, run_id: str) -> dict[str, object]:
        """Return one workflow run's journal summary. See `workflow_facade.workflow_status`."""

        return workflow_facade.workflow_status(self._store, run_id)

    def workflow_resume(self, run_id: str) -> dict[str, str]:
        """Resume a failed or lost run. See `workflow_facade.workflow_resume`."""

        return workflow_facade.workflow_resume(self._home, self._store, run_id)

    def workflow_cancel(self, run_id: str) -> dict[str, object]:
        """Request cancellation of a live workflow run. See `workflow_facade.workflow_cancel`."""

        return workflow_facade.workflow_cancel(self._store, run_id)

    def workflow_answer(self, run_id: str) -> dict[str, object]:
        """Return a terminal workflow run's last result. See `workflow_facade.workflow_answer`."""

        return workflow_facade.workflow_answer(self._store, run_id)


    def steer(self, agent_id: str | AgentId, text: str) -> CommandView:
        if not isinstance(text, str) or not text.strip():
            raise ValidationError("steer text must be a nonblank string")
        checked = validate_agent_id(agent_id)
        agent = self._store.get_agent(checked)
        self._registry.load(str(agent["runtime"]), (Capability.STEER,))
        command_id = self._store.enqueue_command(
            checked, "steer", {"text": text}, at=self._now()
        )
        _logger.info("steer agent_id=%s command_id=%s", checked, command_id)
        return CommandView(command_id, checked, "steer")

    def get(self, agent_id: str | AgentId) -> AgentView:
        _logger.debug("status agent_id=%s", agent_id)
        return self._agent_view(self._store.get_agent(agent_id), self._now())

    def list(self, query: AgentQuery = AgentQuery()) -> AgentPage:
        if not isinstance(query, AgentQuery):
            raise ValidationError("query must be an AgentQuery")
        statuses = ACTIVE if query.active else None
        session_id = (
            None
            if query.orchestrator is None
            else self._store.find_orchestrator_session(query.orchestrator)
        )
        if query.orchestrator is not None and session_id is None:
            return AgentPage((), 0, query.offset, query.limit, None, True)
        rows = self._store.list_agents(
            statuses=statuses,
            orchestrator_session_id=session_id,
            limit=query.limit,
            offset=query.offset,
        )
        total = self._count_agents(statuses, session_id)
        items = tuple(self._agent_view(row, self._now()) for row in rows)
        consumed = query.offset + len(items)
        complete = consumed >= total
        return AgentPage(
            items,
            total,
            query.offset,
            query.limit,
            None if complete else consumed,
            complete,
        )

    def transcript(
        self, agent_id: str | AgentId, cursor: int = 0, limit: int = 200
    ) -> TranscriptPage:
        checked = validate_agent_id(agent_id)
        if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0:
            raise ValidationError("cursor must be a nonnegative integer")
        _page_limit(limit)
        rows = self._store.transcript(
            checked, after_seq=cursor, limit=limit + 1
        )
        complete = len(rows) <= limit
        selected = rows[:limit]
        messages = tuple(
            MessageView(
                int(row["seq"]),
                float(row["at"]),
                str(row["role"]),
                None if row["name"] is None else str(row["name"]),
                str(row["content"]),
                None if row["raw_ref"] is None else str(row["raw_ref"]),
            )
            for row in selected
        )
        next_cursor = None if complete or not messages else messages[-1].seq
        return TranscriptPage(
            checked, messages, cursor, limit, next_cursor, complete
        )

    def answer(self, agent_id: str | AgentId) -> AnswerView:
        checked = validate_agent_id(agent_id)
        row = self._store.get_agent(checked)
        status = AgentStatus(str(row["status"]))
        if row["answer_path"] is None:
            _logger.debug("answer agent_id=%s available=False", checked)
            return AnswerView(checked, status, False, None, None, None, None, True)
        if row["answer_bytes"] is None or row["answer_sha256"] is None:
            raise ValidationError("stored answer proof is incomplete")
        size = int(row["answer_bytes"])
        expected_sha = str(row["answer_sha256"])
        path = Path(str(row["answer_path"]))
        try:
            resolved = path.resolve(strict=True)
        except OSError as error:
            raise ValidationError(f"cannot resolve stored answer: {error}") from error
        root = agent_dir(checked, self._home).resolve()
        if not resolved.is_relative_to(root) or not resolved.is_file():
            raise ValidationError("stored answer path is outside the agent directory")
        if resolved.stat().st_size != size:
            raise ValidationError("stored answer size does not match the sealed file")
        digest = hashlib.sha256()
        content = bytearray() if size <= self._max_inline_answer_bytes else None
        counted = 0
        try:
            with resolved.open("rb") as stream:
                while chunk := stream.read(_CHUNK):
                    counted += len(chunk)
                    digest.update(chunk)
                    if content is not None:
                        content.extend(chunk)
        except OSError as error:
            raise ValidationError(f"cannot read stored answer: {error}") from error
        if counted != size or digest.hexdigest() != expected_sha:
            raise ValidationError("stored answer hash does not match the sealed file")
        try:
            text = None if content is None else bytes(content).decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValidationError("stored answer is not valid UTF-8") from error
        _logger.debug("answer agent_id=%s available=True bytes=%d", checked, size)
        return AnswerView(
            checked,
            status,
            True,
            resolved,
            size,
            expected_sha,
            text,
            content is not None,
        )

    def summary(
        self,
        *,
        agent_id: str | AgentId | None = None,
        orchestrator: OrchestratorRef | None = None,
    ) -> WorkSummary:
        if (agent_id is None) == (orchestrator is None):
            raise ValidationError("summary requires exactly one of agent_id or orchestrator")
        if agent_id is not None:
            agent = self.get(agent_id)
            return WorkSummary("agent", agent.agent_id, None, (agent,), 1, True)
        if not isinstance(orchestrator, OrchestratorRef):
            raise ValidationError("orchestrator must be an OrchestratorRef")
        page = self.list(
            AgentQuery(active=True, orchestrator=orchestrator, limit=_SUMMARY_LIMIT)
        )
        return WorkSummary(
            "orchestrator", None, orchestrator, page.items, page.total, page.complete
        )

    def models(self) -> Mapping[str, RuntimeModels]:
        result: dict[str, RuntimeModels] = {}
        for name in sorted(self._config.runtimes):
            runtime = self._config.runtimes[name]
            if not runtime.enabled:
                continue
            adapter = self._registry.load(name, (Capability.MODEL_ROSTER,))
            adapter.validate(runtime)
            allowed = set(runtime.models)
            roster = tuple(
                model
                for model in adapter.models(runtime, runtime.home)
                if model.id in allowed
            )
            capabilities = tuple(
                sorted(capability.value for capability in adapter.describe().capabilities)
            )
            health = adapter.probe(runtime, runtime.home)
            available = health.available and bool(roster)
            reason = health.reason
            if not roster and reason is None:
                reason = "roster empty"
            _logger.debug(
                "models runtime=%s available=%s count=%d reason=%s",
                name, available, len(roster), reason,
            )
            result[name] = RuntimeModels(roster, capabilities, available, reason)
        _logger.info("models runtimes=%d", len(result))
        return MappingProxyType(result)

    def limits(self) -> CapacityReport:
        observed_at = self._now()
        enabled = {
            name for name, runtime in self._config.runtimes.items() if runtime.enabled
        }
        series = tuple(
            item
            for item in load_series(
                self._store, retention=self._config.capacity.sample_retention
            )
            if item.key.runtime in enabled
        )
        items = build_advice(build_forecasts(series, now=observed_at))
        ordered = tuple(
            sorted(
                items,
                key=lambda item: (
                    item.key.runtime,
                    item.key.lane,
                    item.key.window,
                    item.key.target or "",
                    item.key.source,
                ),
            )
        )
        return CapacityReport(observed_at, ordered)

    def capacity_order(self) -> "CapacityOrder":
        """Return enabled runtimes' deterministic capacity routing order.

        Reads one clock value and the committed capacity snapshot only. Disabled
        runtimes are removed from routes and all evidence. Enabled configured
        runtimes with no fresh topology evidence are reported unavailable.
        """

        from .capacity.order import build_capacity_order

        return build_capacity_order(self._store, self._config, now=self._now())

    def _runtime_config(self, name: str) -> RuntimeConfig:
        try:
            return self._config.runtimes[name]
        except KeyError as error:
            raise ValidationError(f"runtime is not configured: {name}") from error

    def _mcp_servers(self, runtime: RuntimeConfig) -> Mapping[str, McpConfig]:
        try:
            return MappingProxyType(
                {name: self._config.mcp[name] for name in runtime.mcp}
            )
        except KeyError as error:
            raise ValidationError(
                f"runtime references unknown MCP server: {error.args[0]}"
            ) from error

    @staticmethod
    def _required_capabilities(
        request: StartRequest, runtime: RuntimeConfig
    ) -> frozenset[Capability]:
        required = {Capability.MODEL_ROSTER, Capability.TRANSCRIPT}
        if request.write:
            required.add(Capability.WRITE)
        if request.read_roots:
            required.add(Capability.READ_ROOTS)
        if request.effort is not None:
            required.add(Capability.EFFORT)
        if request.output_schema is not None:
            required.add(Capability.OUTPUT_SCHEMA)
        if runtime.mcp:
            required.add(Capability.MCP)
        if runtime.skills:
            required.add(Capability.SKILLS)
        if runtime.hooks:
            required.add(Capability.HOOKS)
        return frozenset(required)

    @staticmethod
    def _task_summary(task: str) -> str:
        return " ".join(task.split())[:_TASK_SUMMARY_CHARS]

    def _count_agents(
        self,
        statuses: frozenset[AgentStatus] | None,
        orchestrator_session_id: str | None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if statuses is not None:
            values = tuple(status.value for status in statuses)
            clauses.append(f"status IN ({','.join('?' for _ in values)})")
            params.extend(values)
        if orchestrator_session_id is not None:
            clauses.append("orchestrator_session_id = ?")
            params.append(orchestrator_session_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        row = self._store.connection.execute(
            f"SELECT COUNT(*) AS total FROM agents{where}", params
        ).fetchone()
        return int(row["total"])

    def _agent_view(self, row: Mapping[str, object], now: float) -> AgentView:
        agent_id = validate_agent_id(str(row["id"]))
        status = AgentStatus(str(row["status"]))
        created_at = float(row["created_at"])
        started_at = None if row["started_at"] is None else float(row["started_at"])
        finished_at = None if row["finished_at"] is None else float(row["finished_at"])
        progress_row = self._store.connection.execute(
            "SELECT MAX(at) AS at FROM messages WHERE agent_id = ?", (agent_id,)
        ).fetchone()
        progress = None if progress_row["at"] is None else float(progress_row["at"])
        warned = bool(row["warned"]) or self._store.connection.execute(
            "SELECT 1 FROM events WHERE agent_id = ? AND kind = 'deadline_warning' LIMIT 1",
            (agent_id,),
        ).fetchone() is not None
        silence = (
            None
            if row["silent_seconds"] is None
            else float(row["silent_seconds"])
        )
        if silence is None and status in ACTIVE:
            silence = max(0.0, now - (progress or started_at or created_at))
        end = finished_at if finished_at is not None else now
        answer_bytes = (
            None if row["answer_bytes"] is None else int(row["answer_bytes"])
        )
        answer_sha = (
            None if row["answer_sha256"] is None else str(row["answer_sha256"])
        )
        return AgentView(
            agent_id,
            str(row["runtime"]),
            str(row["model"]),
            str(row["profile"]),
            str(row["task_summary"]),
            status,
            created_at,
            started_at,
            finished_at,
            max(0.0, end - (started_at or created_at)),
            progress,
            silence,
            warned,
            None if row["failure_kind"] is None else str(row["failure_kind"]),
            None if row["failure_text"] is None else str(row["failure_text"]),
            row["answer_path"] is not None,
            answer_bytes,
            answer_sha,
            self._delivery_view(
                agent_id,
                None
                if row["orchestrator_session_id"] is None
                else str(row["orchestrator_session_id"]),
            ),
        )

    def _delivery_view(
        self, agent_id: AgentId, session_id: str | None
    ) -> DeliveryView:
        row = self._store.connection.execute(
            """SELECT id, state, attempts, ambiguous_result, last_error
               FROM deliveries WHERE agent_id = ?
               ORDER BY terminal_event_seq DESC LIMIT 1""",
            (agent_id,),
        ).fetchone()
        if row is None:
            return DeliveryView(
                agent_id, session_id is not None, session_id, None,
                "not_created", 0, False, None, None,
            )
        last_attempt = self._store.latest_delivery_attempt(str(row["id"]))
        return DeliveryView(
            agent_id,
            session_id is not None,
            session_id,
            str(row["id"]),
            str(row["state"]),
            int(row["attempts"]),
            bool(row["ambiguous_result"]),
            None if row["last_error"] is None else str(row["last_error"]),
            last_attempt,
        )
