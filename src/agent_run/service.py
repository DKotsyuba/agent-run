"""Typed application service shared by CLI and MCP transports."""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping, TypeAlias

from .adapters.base import Capability, LaunchPlan, ModelInfo, RuntimeAdapter
from .adapters.registry import AdapterRegistry
from .adapters.home import write_managed_file
from .adapters.snapshots import (
    CONFIG_SNAPSHOT_FILENAME,
    build_config_snapshot,
    inspect_config_snapshot,
    inspect_runtime_snapshots,
    runtime_snapshot_index_sha256,
)
from .accounts import account_auth_source, account_runtime_home
from .capacity.advice import CapacityAdvice, build_advice
from .capacity.forecast import build_forecasts
from .capacity.history import load_series
from .capacity.ranking import CapacityOrder
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
from .effective_policy import (
    Constraint,
    ConstraintEvidence,
    DeclaredCapability,
    EffectivePolicy,
    Enforcement,
    admission_decision,
    effective_policy,
)
from .delivery.base import DeliveryAttemptEvidence
from .delivery.dispatch import _effort_from_request_json
from .launch import DEFAULT_STARTUP_HANDOFF_SECONDS, launch_cancellation
from .launch_evidence import SupervisorBootstrapError, bootstrap_event_data
from .paths import agent_dir, config_path, create_agent_dir, runtime_skills_dir, state_db_path
from .profiles import AgentProfile, assign_role, load_profile
from .process_identity import capture_process_birth
from .resume import (
    identity_snapshot, inherited_request, proven_identity, record_profile_grants,
    replayed_resume,
)
from .start_coordinator import StartCoordinator
from .state.reconciliation import process_owner_identity
from .supervisor import supervisor_identity
from .state.store import StateStore
from .verify import (
    ANSWER_FORMAT_LEGACY,
    ANSWER_FORMAT_PROOF,
    ANSWER_KIND,
    ANSWER_MEDIA_TYPE,
    MAX_ANSWER_PAYLOAD_BYTES,
    load_answer_proof,
    read_answer_payload,
)


_logger = logging.getLogger("agent_run.service")

_SUMMARY_LIMIT = 50
_TASK_SUMMARY_CHARS = 160
_DEFAULT_INLINE_ANSWER_BYTES = 1024 * 1024
_MAX_PAGE_SIZE = 1000
_FAILURE_TEXT_CHARS = 512
_PENDING_CONFIG_REVISION = "pending:materialization"
_SNAPSHOT_CONFIG_REVISION = "snapshot:v1:"


def _log_start_preparation_stage(
    agent_id: AgentId, stage: str, started_at: float
) -> None:
    """Record one bounded startup-preparation stage without request payloads.

    ``agent_id`` (AgentId) identifies the accepted run; ``stage`` (str) is a
    caller-supplied fixed internal label, never request or exception text.
    ``started_at`` (float) is the worker's monotonic preparation start in seconds.
    Returns None after emitting the id, stage and cumulative elapsed seconds to
    the existing logger. Successive entries identify each stage's duration;
    the entry is emitted before work so a blocked stage remains identifiable.
    This helper does not alter deadlines or state and uses no request payloads.
    """
    _logger.info(
        "start preparation agent_id=%s stage=%s elapsed_seconds=%.3f",
        agent_id,
        stage,
        time.monotonic() - started_at,
    )

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


def _policy_payload(policy: EffectivePolicy) -> dict[str, object]:
    """Serialize one trusted effective policy for immutable identity storage."""

    return {
        "runtime_name": policy.runtime_name,
        "platform": policy.platform,
        "constraints": [
            {
                "constraint": item.constraint.value,
                "enforcement": item.enforcement.value,
                "supported": item.supported,
                "required": item.required,
                "scope": item.scope,
                "platform": item.platform,
                "reason": item.reason,
            }
            for item in policy.constraints
        ],
    }


def _policy_from_identity(value: object) -> EffectivePolicy | None:
    """Restore stored policy evidence or return None for historical rows.

    The value is the agent row's canonical identity JSON. A present policy must
    have the exact emitted shape and typed enum/scalar values; malformed
    evidence raises ValidationError rather than producing a partial claim.
    """

    if value is None:
        return None
    try:
        identity = json.loads(str(value))
        raw = identity.get("effective_policy")
    except (AttributeError, TypeError, ValueError) as error:
        raise ValidationError("invalid effective-policy identity evidence") from error
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) != {
        "runtime_name",
        "platform",
        "constraints",
    }:
        raise ValidationError("invalid effective-policy identity evidence")
    items = raw["constraints"]
    if not isinstance(items, list):
        raise ValidationError("invalid effective-policy identity evidence")
    constraints = []
    expected = {
        "constraint",
        "enforcement",
        "supported",
        "required",
        "scope",
        "platform",
        "reason",
    }
    try:
        for item in items:
            if not isinstance(item, dict) or set(item) != expected:
                raise ValidationError("invalid effective-policy identity evidence")
            if (
                type(item["supported"]) is not bool
                or type(item["required"]) is not bool
                or any(
                    not isinstance(item[name], str) or not item[name].strip()
                    for name in ("scope", "platform", "reason")
                )
            ):
                raise ValidationError("invalid effective-policy identity evidence")
            constraints.append(
                ConstraintEvidence(
                    Constraint(item["constraint"]),
                    Enforcement(item["enforcement"]),
                    item["supported"],
                    item["required"],
                    item["scope"],
                    item["platform"],
                    item["reason"],
                )
            )
    except (TypeError, ValueError) as error:
        raise ValidationError("invalid effective-policy identity evidence") from error
    if tuple(item.constraint for item in constraints) != tuple(Constraint):
        raise ValidationError("invalid effective-policy identity evidence")
    runtime_name, platform = raw["runtime_name"], raw["platform"]
    if (
        not isinstance(runtime_name, str)
        or not runtime_name.strip()
        or not isinstance(platform, str)
        or not platform.strip()
    ):
        raise ValidationError("invalid effective-policy identity evidence")
    return EffectivePolicy(runtime_name, platform, tuple(constraints))


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
class CleanupView:
    """Latest bounded evidence that an agent's owned processes were cleaned up.

    ``signals`` names attempted cleanup signals and ``scope`` whether observation
    covered a process group or verified descendants. ``descendants_gone`` is
    absent when that set was unavailable. ``confirmed`` is true only when the
    original group and every readable pre-signal owned descendant were observed
    gone; ``process_group_id`` is diagnostic and may be unavailable.
    """

    signals: tuple[str, ...]
    scope: str
    group_gone: bool
    descendants_gone: bool | None
    confirmed: bool
    process_group_id: int | None


@dataclass(frozen=True, slots=True)
class AgentView:
    """One agent's read-only status, lineage, and admitted policy evidence.

    ``parent_agent_id`` is the agent this one resumed (``None`` for a fresh
    run), ``root_agent_id`` the chain's first agent (itself when it has no
    parent), and ``sequence`` its 1-based position in that chain. ``policy``
    is the immutable effective enforcement snapshot accepted for this run, or
    ``None`` for historical rows.
    """

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
    effort: str | None
    delivery: DeliveryView
    parent_agent_id: AgentId | None = None
    root_agent_id: AgentId | None = None
    sequence: int = 1
    cleanup: CleanupView | None = None
    policy: EffectivePolicy | None = None


@dataclass(frozen=True, slots=True)
class OrchestratorView:
    """Read-only aggregate for one launching orchestrator session."""

    session_id: str | None
    transport: str
    external_session_id: str
    external_turn_id: str | None
    created_at: float
    last_seen_at: float
    active: int
    total: int


@dataclass(frozen=True, slots=True)
class OrchestratorPage:
    """A bounded read-only orchestrator-session page with an exact total."""

    items: tuple[OrchestratorView, ...]
    total: int
    limit: int
    complete: bool


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
class ChainPage:
    """One bounded, chronological page of a resume chain.

    ``items`` are the chain's agents ordered by ``sequence``. ``cursor`` is the
    1-based sequence this page started at, ``limit`` the requested page size,
    and ``next_cursor`` the sequence to pass back for the following page, or
    ``None`` when this page reached the end. ``complete`` mirrors that: it is
    ``True`` only when no further page exists.
    """

    items: tuple[AgentView, ...]
    cursor: int
    limit: int
    next_cursor: int | None
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
    """Descriptor for one stored answer artifact.

    ``kind`` and ``media_type`` classify the artifact, ``relative_path`` is
    its owned path beneath the agent directory, and
    ``size_bytes``/``sha256`` always describe the stored artifact exactly as
    recorded at seal time -- for historical artifacts that includes the
    terminal sentinel frame. ``proof_version`` names the explicit on-disk
    proof format (``ANSWER_FORMAT_LEGACY`` or ``ANSWER_FORMAT_PROOF``).
    ``content`` is presentation only: at most ``max_inline_answer_bytes`` of
    decoded payload with any legacy terminal frame stripped once; it is never
    completion or integrity evidence.
    """

    agent_id: AgentId
    status: AgentStatus
    available: bool
    path: Path | None
    size_bytes: int | None
    sha256: str | None
    content: str | None
    inline_complete: bool
    relative_path: str | None
    kind: str | None
    media_type: str | None
    proof_version: int | None


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
        label = self.resolve_account(request.runtime, request.account)
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
        policy = self._admission_policy(request, runtime)
        return self._admit(request, runtime, label, policy=policy)

    def _admit(
        self,
        request: StartRequest,
        runtime: RuntimeConfig,
        label: str | None,
        *,
        parent_agent_id: AgentId | None = None,
        policy: EffectivePolicy,
    ) -> StartResult:
        """Durably accept one already validated start and hand it to a worker.

        Shared by :meth:`start` and :meth:`resume`; everything runtime- and
        model-specific has been checked by the caller. ``parent_agent_id`` is
        set only for a resume, and joins the parent's chain atomically.
        ``policy`` is the already-admitted effective enforcement evidence;
        it is copied into immutable identity JSON in the same admission write.

        The effective identity this start resolved to (``label``, runtime home,
        auth target, granted permissions, ``fast``) is persisted alongside the
        row but outside ``request_json``, so a later resume can prove what this
        run used without changing what idempotent replay compares.
        A continuation preserves its parent's completed grant snapshot, which
        the preparation worker compares with the actual loaded profile.
        The resident coordinator's process birth time and bounded startup claim
        commit atomically with ``STARTING`` and both acceptance events, before a
        worker is registered or this method can return.

        The native session a resumed child attaches to is read back from the
        row that was just committed, never from the caller: store and adapter
        cannot then disagree about which session was accepted. Returns the
        :class:`StartResult` for the new -- or idempotently replayed -- agent.
        """

        candidate = new_agent_id()
        accepted_at = self._now()
        startup_owner = process_owner_identity(
            os.getpid(), supervisor_identity()
        )
        startup_birth = capture_process_birth(os.getpid())
        identity = (
            identity_snapshot(request.runtime, runtime, label, request)
            if parent_agent_id is None
            else self._store.get_agent(parent_agent_id)["identity_json"]
        )
        try:
            identity_payload = json.loads(str(identity))
        except (TypeError, ValueError) as error:
            raise ValidationError("invalid effective-identity snapshot") from error
        if not isinstance(identity_payload, dict):
            raise ValidationError("invalid effective-identity snapshot")
        identity_payload["effective_policy"] = _policy_payload(policy)
        creation = self._store.create_agent_limited(
            request,
            task_summary=self._task_summary(request.task),
            config_revision=_PENDING_CONFIG_REVISION,
            global_limit=self._config.core.max_active_agents,
            runtime_limit=runtime.max_active_agents,
            agent_id=candidate,
            at=accepted_at,
            parent_agent_id=parent_agent_id,
            identity_json=json.dumps(
                identity_payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            startup_owner_identity=startup_owner,
            startup_owner_birth_time=startup_birth,
            startup_deadline_seconds=120.0,
        )
        if not creation.created:
            _logger.info("start agent_id=%s created=False (idempotent replay)", creation.agent_id)
            return StartResult(
                creation.agent_id, False, self.get(creation.agent_id)
            )
        _logger.info("start agent_id=%s created=True", creation.agent_id)
        resume_session_id = (
            None
            if parent_agent_id is None
            else self._store.get_agent(creation.agent_id)[
                "resume_of_runtime_session_id"
            ]
        )
        try:
            self._starts.submit(
                creation.agent_id,
                lambda worker_store, cancelled: self._continue_start(
                    worker_store,
                    cancelled,
                    creation.agent_id,
                    request,
                    runtime,
                    label,
                    startup_owner,
                    None if resume_session_id is None else str(resume_session_id),
                    parent_agent_id,
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
        startup_owner: str,
        resume_session_id: str | None = None,
        parent_agent_id: AgentId | None = None,
    ) -> None:
        """Materialize and launch one already accepted start.

        ``store`` belongs to this worker thread. Cancellation is checked before
        work, after authentication/materialization/prepare, and before spawn.
        ``startup_owner`` is the immutable coordinator identity whose live
        preparation claim is atomically renewed for the bounded READY and
        cleanup handoff. Post-accept failures become durable outcomes.

        ``resume_session_id`` is the parent's native runtime session for a
        resumed start, or ``None`` for a fresh one. It is stamped onto the
        adapter's plan after ``prepare`` and before the plan is serialized for
        launch, so an adapter builds its plan without needing to know it is a
        resume. Attachment is only requested here; the runtime session the
        supervisor later records is whatever the adapter's sink actually emits.
        ``parent_agent_id`` additionally selects snapshot-v1 lineage state: a
        new-format continuation verifies and reuses its parent's runtime home
        without rematerializing; an unprefixed historical parent retains the
        shared-home compatibility path.
        """

        failure_kind = "prepare_failed"
        preparation_started = time.monotonic()
        stage = "account"
        _log_start_preparation_stage(agent_id, stage, preparation_started)
        try:
            if self._cancel_accepted_start(store, cancelled, agent_id):
                return
            candidate_dir = create_agent_dir(agent_id, self._home)
            configured_home = runtime.home
            effective_auth = runtime.auth
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
                configured_home = account_runtime_home(runtime.home, account_label)
                effective_auth = replace(runtime.auth, source=effective_source)

            parent_revision = None
            snapshot_resume = False
            lineage_agent_id = parent_agent_id
            if parent_agent_id is not None:
                parent_row = store.get_agent(parent_agent_id)
                parent_revision = str(parent_row["config_revision"])
                lineage_agent_id = validate_agent_id(
                    str(parent_row["root_agent_id"] or parent_agent_id)
                )
                snapshot_resume = parent_revision.startswith(
                    _SNAPSHOT_CONFIG_REVISION
                )
            if parent_agent_id is None:
                effective_home = candidate_dir / "runtime-home"
                effective_home.mkdir(mode=0o700)
            elif snapshot_resume:
                assert lineage_agent_id is not None
                effective_home = agent_dir(lineage_agent_id, self._home) / "runtime-home"
            else:
                effective_home = configured_home
                effective_home.mkdir(mode=0o700, parents=True, exist_ok=True)
            if not snapshot_resume:
                effective_home.chmod(0o700)
            effective_runtime = replace(
                runtime,
                home=effective_home,
                auth=effective_auth,
                default_account=account_label,
            )
            if self._cancel_accepted_start(store, cancelled, agent_id):
                return

            stage = "adapter"
            _log_start_preparation_stage(agent_id, stage, preparation_started)
            adapter = AdapterRegistry(self._config).load(
                request.runtime,
                self._required_capabilities(request, effective_runtime),
            )
            adapter.validate(effective_runtime)
            stage = "profile"
            _log_start_preparation_stage(agent_id, stage, preparation_started)
            profile = self._effective_profile(request, effective_runtime)
            self._policy_for_profile(request, effective_runtime, profile)
            record_profile_grants(store.connection, agent_id, profile)
            mcp_servers = self._mcp_servers(effective_runtime)
            stored_snapshot = None
            if snapshot_resume:
                assert lineage_agent_id is not None and parent_revision is not None
                expected_sha256 = parent_revision.removeprefix(
                    _SNAPSHOT_CONFIG_REVISION
                )
                stored_snapshot = inspect_config_snapshot(
                    agent_dir(lineage_agent_id, self._home), expected_sha256
                )
                runtime_snapshot = inspect_runtime_snapshots(
                    effective_home,
                    stored_snapshot.materialize_revision,
                    expected_sha256=stored_snapshot.snapshot_index_sha256,
                )
                if not runtime_snapshot.verified:
                    raise ValidationError(
                        "runtime snapshot is incomplete or changed: "
                        f"missing={len(runtime_snapshot.missing)} "
                        f"mismatched={len(runtime_snapshot.mismatched)} "
                        f"temps={len(runtime_snapshot.owned_temps)} "
                        f"orphans={len(runtime_snapshot.orphans)}"
                    )
                current_snapshot = build_config_snapshot(
                    runtime=request.runtime,
                    adapter_api_version=adapter.describe().adapter_api_version,
                    schema_version=self._config.schema_version,
                    materialize_revision=stored_snapshot.materialize_revision,
                    snapshot_index_sha256=stored_snapshot.snapshot_index_sha256,
                    config=effective_runtime,
                    profile=profile,
                    runtime_version=stored_snapshot.runtime_version,
                )
                if current_snapshot.sha256 != expected_sha256:
                    raise ValidationError(
                        "effective runtime or profile changed since the parent snapshot"
                    )
                revision = parent_revision
                _logger.info(
                    "start reused runtime snapshot runtime=%s revision=%s",
                    request.runtime,
                    revision,
                )
            else:
                stage = "materialize"
                _log_start_preparation_stage(
                    agent_id, stage, preparation_started
                )
                materialize_revision = adapter.materialize(
                    effective_runtime,
                    effective_home,
                    mcp_servers=mcp_servers,
                    skills_root=runtime_skills_dir(request.runtime, self._home),
                )
                revision = materialize_revision
            if self._cancel_accepted_start(store, cancelled, agent_id):
                return

            stage = "models"
            _log_start_preparation_stage(agent_id, stage, preparation_started)
            roster = adapter.models(effective_runtime, effective_home)
            if request.model not in {model.id for model in roster}:
                raise ValidationError(
                    f"model is not available for runtime {request.runtime}: {request.model}"
                )
            stage = "prepare"
            _log_start_preparation_stage(agent_id, stage, preparation_started)
            plan = adapter.prepare(
                request,
                profile,
                effective_runtime,
                effective_home,
                candidate_dir,
                mcp_servers=mcp_servers,
                resume_session_id=resume_session_id,
            )
            if plan.resume_session_id != resume_session_id:
                raise ValidationError("adapter returned a mismatched resume session")
            if snapshot_resume:
                assert stored_snapshot is not None
                if plan.materialize_revision is not None:
                    raise ValidationError(
                        "snapshot resume must not rematerialize its runtime home"
                    )
                runtime_snapshot = inspect_runtime_snapshots(
                    effective_home,
                    stored_snapshot.materialize_revision,
                    expected_sha256=stored_snapshot.snapshot_index_sha256,
                )
                if not runtime_snapshot.verified:
                    raise ValidationError(
                        "adapter prepare changed the verified runtime snapshot"
                    )
            else:
                revision = plan.materialize_revision or revision
            if parent_agent_id is None:
                snapshot_index_sha256 = runtime_snapshot_index_sha256(
                    effective_home, revision
                )
                runtime_snapshot = inspect_runtime_snapshots(
                    effective_home,
                    revision,
                    expected_sha256=snapshot_index_sha256,
                )
                if not runtime_snapshot.verified:
                    raise ValidationError(
                        "fresh runtime snapshot is incomplete after prepare"
                    )
                runtime_version = adapter.probe(
                    effective_runtime, effective_home
                ).version
                config_snapshot = build_config_snapshot(
                    runtime=request.runtime,
                    adapter_api_version=adapter.describe().adapter_api_version,
                    schema_version=self._config.schema_version,
                    materialize_revision=revision,
                    snapshot_index_sha256=snapshot_index_sha256,
                    config=effective_runtime,
                    profile=profile,
                    runtime_version=runtime_version,
                )
                write_managed_file(
                    candidate_dir,
                    CONFIG_SNAPSHOT_FILENAME,
                    config_snapshot.document,
                )
                revision = _SNAPSHOT_CONFIG_REVISION + config_snapshot.sha256
            stage = "config_revision"
            _log_start_preparation_stage(agent_id, stage, preparation_started)
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
            with launch_cancellation(
                lambda: cancelled.is_set() or store.has_pending_cancel(agent_id)
            ):
                if self._cancel_accepted_start(store, cancelled, agent_id):
                    return
                stage = "handoff"
                _log_start_preparation_stage(agent_id, stage, preparation_started)
                if not store.begin_supervisor_handoff(
                    agent_id,
                    startup_owner,
                    at=self._now(),
                    deadline_seconds=DEFAULT_STARTUP_HANDOFF_SECONDS,
                ):
                    _logger.warning("start agent_id=%s expired before supervisor spawn", agent_id)
                    return
                failure_kind = "supervisor_start_failed"
                stage = "supervisor"
                _log_start_preparation_stage(agent_id, stage, preparation_started)
                self._launch(agent_id, request, adapter, plan, candidate_dir)
            _logger.info("start agent_id=%s done", agent_id)
        except BaseException as error:
            if self._cancel_accepted_start(store, cancelled, agent_id):
                return
            _logger.warning(
                "start agent_id=%s failed stage=%s elapsed_seconds=%.3f error_kind=%s",
                agent_id,
                stage,
                time.monotonic() - preparation_started,
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

    def resume(
        self,
        agent_id: str | AgentId,
        task: str,
        *,
        timeout_seconds: float | None = None,
        request_id: str | None = None,
        orchestrator: OrchestratorRef | None = None,
    ) -> StartResult:
        """Continue one finished agent's native session as a new durable agent.

        ``agent_id`` names the exact predecessor to resume; it must be the
        latest node of its chain, be finished in a :data:`RESUMABLE
        <agent_run.state.start.RESUMABLE>` status, and have recorded a native
        runtime session. ``task`` is the new prompt and the only inherited
        field that changes; ``timeout_seconds`` overrides the parent's when
        given and inherits it when omitted. Everything else -- runtime, model,
        profile, effort, fast, account, workdir, read roots, write right and
        output schema -- comes from the parent's persisted request, so config
        or default-account drift cannot silently redirect the continuation.

        ``request_id`` makes the call idempotent: repeating it with the same
        inputs returns the child already accepted, even after the parent has
        stopped being a valid source; reusing it with different inputs raises
        :class:`ValidationError`. ``orchestrator`` binds notifications for the
        new agent only -- the parent's binding is untouched, as are its
        artifacts, transcript and answer.

        Returns a :class:`StartResult` whose ``agent_id`` is a *new* durable
        agent. Raises :class:`ValidationError` when the parent is unknown,
        unfinished, lost, already resumed, has no session to attach to, when
        the inherited identity can no longer be proved against configuration,
        when an inherited directory no longer exists, or when the runtime
        adapter does not declare :attr:`Capability.RESUME`. Attachment is only
        requested here: the runtime session actually established is whatever
        the adapter reports later.
        """

        parent_id = validate_agent_id(agent_id)
        row = self._store.get_agent(parent_id)
        replay = replayed_resume(
            self._store.connection, row, task, timeout_seconds, request_id, orchestrator
        )
        if replay is not None:
            return StartResult(replay, False, self.get(replay))
        runtime_name = str(row["runtime"])
        runtime = self._runtime_config(runtime_name)
        label, snapshot = proven_identity(
            parent_id, row["identity_json"], runtime_name, runtime
        )
        request = inherited_request(
            row, snapshot, label, task, timeout_seconds, request_id, orchestrator
        )
        _logger.info(
            "resume parent_agent_id=%s runtime=%s request_id=%s",
            parent_id, runtime_name, request_id,
        )
        adapter = self._registry.load(
            request.runtime,
            self._required_capabilities(request, runtime) | {Capability.RESUME},
        )
        adapter.validate(runtime)
        if request.model not in runtime.models:
            raise ValidationError(
                f"model is no longer configured for runtime {request.runtime}: "
                f"{request.model}"
            )
        policy = self._admission_policy(request, runtime)
        return self._admit(
            request,
            runtime,
            label,
            parent_agent_id=parent_id,
            policy=policy,
        )

    def chain(
        self,
        agent_id: str | AgentId,
        *,
        cursor: int | None = None,
        limit: int = _SUMMARY_LIMIT,
    ) -> ChainPage:
        """Page one resume chain in chronological order.

        ``agent_id`` may be any member of the chain; the whole chain is
        returned regardless of which node was named. ``cursor`` is the 1-based
        ``sequence`` to resume paging from, defaulting to the chain's first
        agent, and ``limit`` bounds the page at :data:`_MAX_PAGE_SIZE`.

        Returns a :class:`ChainPage` whose ``next_cursor`` is the sequence of
        the first unreturned agent, or ``None`` when the page ends the chain.
        Raises :class:`ValidationError` for a non-positive cursor or an
        out-of-range limit.
        """

        start = 1 if cursor is None else cursor
        if isinstance(start, bool) or not isinstance(start, int) or start < 1:
            raise ValidationError("cursor must be a positive integer")
        bounded = _page_limit(limit)
        rows = self._store.resume_chain(agent_id, cursor=start, limit=bounded)
        now = self._now()
        selected = rows[:bounded]
        projection = self._store.agent_projection(
            str(row["id"]) for row in selected
        )
        items = tuple(
            self._agent_view(row, now, projection[str(row["id"])])
            for row in selected
        )
        next_cursor = (
            int(rows[bounded]["sequence"]) if len(rows) > bounded else None
        )
        return ChainPage(items, start, bounded, next_cursor, next_cursor is None)

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
        row = self._store.get_agent(agent_id)
        projection = self._store.agent_projection((str(row["id"]),))
        return self._agent_view(row, self._now(), projection[str(row["id"])])

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
        projection = self._store.agent_projection(str(row["id"]) for row in rows)
        now = self._now()
        items = tuple(
            self._agent_view(row, now, projection[str(row["id"])]) for row in rows
        )
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

    def list_orchestrators(self, *, limit: int = 100) -> OrchestratorPage:
        """Return up to ``limit`` session aggregates, ordered by activity.

        ``limit`` must be a positive integer no greater than 1000.  The page
        includes bound sessions with agents and, when applicable, one null-ID
        aggregate for unbound agents; no state is changed.
        """

        _page_limit(limit)
        rows = self._store.list_orchestrator_sessions(limit=limit)
        total = 0 if not rows else int(rows[0]["page_total"])
        items = tuple(
            OrchestratorView(
                None if row["id"] is None else str(row["id"]),
                str(row["transport"]),
                str(row["external_session_id"]),
                None if row["external_turn_id"] is None else str(row["external_turn_id"]),
                float(row["created_at"]),
                float(row["last_seen_at"]),
                int(row["active"]),
                int(row["total"]),
            )
            for row in rows
        )
        return OrchestratorPage(items, total, limit, len(items) == total)

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
        """Return the verified descriptor for one agent's stored answer.

        The artifact's recorded size and hash are verified before content is
        returned. A durable directory marker pins clean new payloads to the
        proof format even when their required sidecar is missing or corrupt.
        Historical sentinel-framed payloads keep their stored byte count and
        hash while the exact terminal frame is stripped once for presentation.
        Every payload and metadata component is opened without following links,
        relative to the owning agent directory. Payloads above the inline cutoff
        are streamed for size, hash, and UTF-8 validation without retaining text.
        """

        checked = validate_agent_id(agent_id)
        row = self._store.get_agent(checked)
        status = AgentStatus(str(row["status"]))
        if row["answer_path"] is None:
            _logger.debug("answer agent_id=%s available=False", checked)
            return AnswerView(
                checked, status, False, None, None, None, None, True,
                None, None, None, None,
            )
        if row["answer_bytes"] is None or row["answer_sha256"] is None:
            raise ValidationError("stored answer proof is incomplete")
        size = int(row["answer_bytes"])
        expected_sha = str(row["answer_sha256"])
        path = Path(str(row["answer_path"]))
        root = agent_dir(checked, self._home).resolve()
        if not path.is_absolute():
            raise ValidationError("stored answer path must be absolute")
        try:
            relative = path.relative_to(root)
        except ValueError:
            raise ValidationError("stored answer path is outside the agent directory")
        if not relative.parts or ".." in relative.parts:
            raise ValidationError("stored answer path is outside the agent directory")
        resolved = root / relative
        proof = load_answer_proof(
            resolved,
            expected_bytes=size,
            expected_sha256=expected_sha,
            owned_root=root,
        )
        proof_version = ANSWER_FORMAT_LEGACY if proof is None else ANSWER_FORMAT_PROOF
        text = read_answer_payload(
            resolved,
            expected_bytes=size,
            expected_sha256=expected_sha,
            max_bytes=MAX_ANSWER_PAYLOAD_BYTES,
            strip_legacy=proof_version == ANSWER_FORMAT_LEGACY,
            owned_root=root,
            return_content=size <= self._max_inline_answer_bytes,
        )
        _logger.debug("answer agent_id=%s available=True bytes=%d", checked, size)
        return AnswerView(
            checked,
            status,
            True,
            resolved,
            size,
            expected_sha,
            text,
            text is not None,
            str(resolved.relative_to(root)),
            ANSWER_KIND,
            ANSWER_MEDIA_TYPE,
            proof_version,
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

    def capacity_order(self) -> CapacityOrder:
        """Return enabled runtimes' deterministic capacity routing order.

        Reads one clock value and the committed capacity snapshot only. Disabled
        runtimes are removed from routes and all evidence. Enabled configured
        runtimes with no fresh topology evidence are reported unavailable.
        """

        from .capacity.order import build_capacity_order

        return build_capacity_order(self._store, self._config, now=self._now())

    def resolve_account(self, runtime: str, account: str | None) -> str | None:
        """Return a configured account label (str), or None for an unlabelled run.

        ``runtime`` is a configured runtime-name str; ``account`` is an explicit
        label str or None to use its configured default. Raise ValidationError
        for an unknown runtime, undeclared explicit label, or invalid default.
        This lookup performs no I/O and does not mutate configuration.
        """
        config = self._runtime_config(runtime)
        if account is not None and not config.accounts:
            raise ValidationError(f"runtime {runtime} declares no accounts")
        label = account if account is not None else config.default_account
        if label is not None and label not in config.accounts:
            known = ", ".join(config.accounts) or "none"
            raise ValidationError(
                f"account {label!r} is not declared for runtime {runtime}; known accounts: {known}"
            )
        return label

    def _runtime_config(self, name: str) -> RuntimeConfig:
        """Return a configured runtime or explain OpenCode's removal.

        Legacy OpenCode rows remain readable from the state store, but new
        launches must fail before adapter resolution with migration guidance.
        """

        if name == "opencode":
            raise ValidationError(
                "runtime 'opencode' is no longer supported; remove [runtimes.opencode] from config.toml"
            )
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

    def _effective_profile(
        self, request: StartRequest, runtime: RuntimeConfig
    ) -> AgentProfile:
        """Load and role-assign the exact profile used for policy and launch."""

        return assign_role(
            load_profile(
                self._config.profiles,
                request.profile,
                requested_write=request.write,
                read_roots=request.read_roots,
            ),
            request.runtime,
            runtime.skills,
        )

    @staticmethod
    def _policy_capabilities(
        runtime_name: str, runtime: RuntimeConfig
    ) -> Mapping[Constraint, DeclaredCapability]:
        """Return only enforcement claims proved by current materialization."""

        plugins = tuple(runtime.plugins)
        fully_snapshotted = not plugins or (
            runtime_name in {"claude", "glm"}
            and all(plugin.name in runtime.plugin_snapshot_assets for plugin in plugins)
        )
        if not fully_snapshotted:
            return MappingProxyType({})
        scope = (
            "no configured plugin assets"
            if not plugins
            else "all explicitly declared assets of configured plugins"
        )
        return MappingProxyType(
            {
                Constraint.PLUGIN_IMMUTABILITY: DeclaredCapability(
                    Enforcement.RUNTIME_ENFORCED,
                    scope,
                    "attempt materialization publishes and verifies selected plugin snapshots",
                )
            }
        )

    def _policy_for_profile(
        self,
        request: StartRequest,
        runtime: RuntimeConfig,
        profile: AgentProfile,
    ) -> EffectivePolicy:
        """Resolve and enforce one request's explicit policy requirements."""

        policy = effective_policy(
            profile,
            request.runtime,
            sys.platform,
            self._policy_capabilities(request.runtime, runtime),
            required=request.required_constraints,
        )
        decision = admission_decision(policy)
        if not decision.allowed:
            names = ", ".join(item.value for item in decision.unsupported_required)
            raise ValidationError(f"required policy constraints are not enforced: {names}")
        return policy

    def _admission_policy(
        self, request: StartRequest, runtime: RuntimeConfig
    ) -> EffectivePolicy:
        """Validate required policy synchronously before durable admission."""

        return self._policy_for_profile(
            request, runtime, self._effective_profile(request, runtime)
        )

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

    def _agent_view(
        self,
        row: Mapping[str, object],
        now: float,
        projection: Mapping[str, object],
    ) -> AgentView:
        """Build one public view from an agent row and batched projection row."""

        agent_id = validate_agent_id(str(row["id"]))
        status = AgentStatus(str(row["status"]))
        created_at = float(row["created_at"])
        started_at = None if row["started_at"] is None else float(row["started_at"])
        finished_at = None if row["finished_at"] is None else float(row["finished_at"])
        progress = (
            None
            if projection["last_progress_at"] is None
            else float(projection["last_progress_at"])
        )
        warned = bool(row["warned"]) or bool(projection["deadline_warned"])
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
        cleanup = self._cleanup_view(projection["cleanup_json"])
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
            _effort_from_request_json(row["request_json"]),
            self._delivery_view(
                agent_id,
                None
                if row["orchestrator_session_id"] is None
                else str(row["orchestrator_session_id"]),
                projection,
            ),
            None
            if row["parent_agent_id"] is None
            else AgentId(str(row["parent_agent_id"])),
            AgentId(str(row["root_agent_id"] or agent_id)),
            int(row["sequence"]),
            cleanup,
            _policy_from_identity(row["identity_json"]),
        )

    def _delivery_view(
        self,
        agent_id: AgentId,
        session_id: str | None,
        projection: Mapping[str, object] | None = None,
    ) -> DeliveryView:
        """Build delivery state from a supplied or single-agent projection."""

        if projection is None:
            projection = self._store.agent_projection((agent_id,))[str(agent_id)]
        if projection["delivery_id"] is None:
            return DeliveryView(
                agent_id, session_id is not None, session_id, None,
                "not_created", 0, False, None, None,
            )
        evidence_json = projection["evidence_json"]
        last_attempt = None
        if evidence_json is not None:
            try:
                evidence = json.loads(str(evidence_json))
            except ValueError as error:
                raise ValidationError("invalid stored delivery attempt evidence") from error
            last_attempt = DeliveryAttemptEvidence.from_payload(evidence)
        return DeliveryView(
            agent_id,
            session_id is not None,
            session_id,
            str(projection["delivery_id"]),
            str(projection["delivery_state"]),
            int(projection["delivery_attempts"]),
            bool(projection["delivery_ambiguous"]),
            None
            if projection["delivery_last_error"] is None
            else str(projection["delivery_last_error"]),
            last_attempt,
        )

    @staticmethod
    def _cleanup_view(value: object) -> CleanupView | None:
        """Validate one latest process-cleanup JSON value into a public view."""

        if value is None:
            return None
        try:
            payload = json.loads(str(value))
        except ValueError as error:
            raise ValidationError("invalid stored process cleanup evidence") from error
        expected = {
            "signals",
            "scope",
            "group_gone",
            "descendants_gone",
            "confirmed",
            "process_group_id",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise ValidationError("invalid stored process cleanup evidence")
        if payload["scope"] not in {"process_group", "verified_descendants"}:
            raise ValidationError("invalid stored process cleanup evidence")
        if (
            not isinstance(payload["signals"], list)
            or any(not isinstance(signal, str) for signal in payload["signals"])
            or type(payload["group_gone"]) is not bool
            or type(payload["confirmed"]) is not bool
            or (
                payload["descendants_gone"] is not None
                and type(payload["descendants_gone"]) is not bool
            )
            or (
                payload["process_group_id"] is not None
                and (
                    type(payload["process_group_id"]) is not int
                    or payload["process_group_id"] <= 0
                )
            )
            or payload["confirmed"]
            != (
                payload["group_gone"]
                and payload["descendants_gone"] is True
            )
        ):
            raise ValidationError("invalid stored process cleanup evidence")
        return CleanupView(
            tuple(payload["signals"]),
            str(payload["scope"]),
            payload["group_gone"],
            payload["descendants_gone"],
            payload["confirmed"],
            payload["process_group_id"],
        )
