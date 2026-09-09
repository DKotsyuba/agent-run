"""Typed application service shared by CLI and MCP transports."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping, TypeAlias

from .adapters.base import Capability
from .adapters.registry import AdapterRegistry
from .capacity.ranking import CapacityOrder
from .config import Config, RuntimeConfig, load_config
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
from .launch_evidence import SupervisorBootstrapError, bootstrap_event_data
from .paths import agent_dir, config_path, runtime_skills_dir, state_db_path
from .profiles import AgentProfile, load_profile
from .role_plan import ResolvedRolePlan, resolve_role_plan
from .process_identity import capture_process_birth, observe_process
from .resume import identity_snapshot, inherited_request, proven_identity, replayed_resume
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

_TASK_SUMMARY_CHARS = 160
_DEFAULT_INLINE_ANSWER_BYTES = 1024 * 1024
_MAX_PAGE_SIZE = 1000
_FAILURE_TEXT_CHARS = 512
_PENDING_CONFIG_REVISION = "pending:materialization"
_SNAPSHOT_CONFIG_REVISION = "snapshot:v1:"


LaunchAgent: TypeAlias = Callable[[AgentId, StartRequest, ResolvedRolePlan], None]


def _effort_from_request_json(raw: object) -> str | None:
    """Return a bounded stored effort, or ``None`` for historical bad data."""

    if not isinstance(raw, str):
        return None
    try:
        parsed = json.loads(raw)
    except ValueError:
        return None
    effort = parsed.get("effort") if isinstance(parsed, dict) else None
    return effort if isinstance(effort, str) and effort.strip() and len(effort) <= 128 else None


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
    parent_agent_id: AgentId | None = None
    root_agent_id: AgentId | None = None
    sequence: int = 1
    cleanup: CleanupView | None = None
    policy: EffectivePolicy | None = None
    phase: str = "accepted"
    phase_started_at: float = 0.0
    process_state: str = "not_started"
    observed_at: float = 0.0
    runtime_outcome: str | None = None
    acceptance: str = "pending"




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
    after_revision: int | None = None
    wait_seconds: float = 0.0

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
        if self.after_revision is not None and (
            type(self.after_revision) is not int or self.after_revision < 0
        ):
            raise ValidationError("after_revision must be a nonnegative integer or None")
        if (
            isinstance(self.wait_seconds, bool)
            or not isinstance(self.wait_seconds, (int, float))
            or not 0 <= self.wait_seconds <= 60
        ):
            raise ValidationError("wait_seconds must be from 0 to 60")


@dataclass(frozen=True, slots=True)
class AgentPage:
    items: tuple[AgentView, ...]
    total: int
    offset: int
    limit: int
    next_offset: int | None
    complete: bool
    revision: int = 0
    observed_at: float = 0.0




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
        """Create the service around one thread-affine state connection."""

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
        """Close the service's owning state connection."""

        self._store.close()

    def start(self, request: StartRequest) -> StartResult:
        """Durably accept one start and return before slow runtime bootstrap.

        Validation, replay, capacity admission, row creation, and ``STARTING``
        transition are synchronous. The detached supervisor is spawned
        immediately; this method returns after its durable ownership READY,
        while authentication/materialization/prepare continue in that process.
        Labelled file-link runtimes require their configured bridge; labelled
        Claude environment-auth runtimes instead use their private sibling
        credential home and are admitted without a bridge file.
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
        profile = self._effective_profile(request, runtime)
        label = self.resolve_account(request.runtime, request.account)
        runtime, role_plan = self._runtime_for_profile(
            runtime,
            profile,
            label,
            request.required_constraints,
            request.runtime,
        )
        request = replace(
            request,
            write=role_plan.write,
            read_roots=role_plan.read_roots,
            required_constraints=role_plan.required_constraints,
        )
        if label is not None:
            claude_state = runtime.adapter in {
                "agent_run.adapters.claude:ADAPTER",
                "agent_run.adapters.claude.adapter:ADAPTER",
            }
            file_state = runtime.auth is not None and runtime.auth.target is not None
            codex_state = request.runtime == "codex"
            if not (claude_state or file_state or codex_state):
                raise ValidationError(
                    f"runtime {request.runtime} does not support separate accounts"
                )
        if request.model not in runtime.models:
            _logger.warning(
                "start gate=model_configured failed runtime=%s model=%s",
                request.runtime, request.model,
            )
            raise ValidationError(
                f"model is not configured for runtime {request.runtime}: {request.model}"
            )
        policy = self._policy_for_profile(request, runtime, profile)
        return self._admit(
            request, runtime, label, policy=policy, role_plan=role_plan,
        )

    def _admit(
        self,
        request: StartRequest,
        runtime: RuntimeConfig,
        label: str | None,
        *,
        parent_agent_id: AgentId | None = None,
        policy: EffectivePolicy,
        role_plan: ResolvedRolePlan,
    ) -> StartResult:
        """Durably accept one validated start and immediately spawn its supervisor.

        Shared by :meth:`start` and :meth:`resume`; everything runtime- and
        model-specific has been checked by the caller. ``parent_agent_id`` is
        set only for a resume, and joins the parent's chain atomically.
        ``policy`` is the already-admitted effective enforcement evidence;
        it is copied into immutable identity JSON in the same admission write.

        The effective identity this start resolved to (``label``, runtime home,
        auth target, granted permissions, ``fast``) is persisted alongside the
        row but outside ``request_json``, so a later resume can prove what this
        run used without changing what idempotent replay compares.
        A continuation preserves its parent's completed grant snapshot. The
        broker PID/birth claim commits atomically with ``STARTING`` and both
        acceptance events, covering only the admission-to-READY spawn gap.

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
        )
        if not creation.created:
            _logger.info("start agent_id=%s created=False (idempotent replay)", creation.agent_id)
            return StartResult(
                creation.agent_id, False, self.get(creation.agent_id)
            )
        _logger.info("start agent_id=%s created=True", creation.agent_id)
        try:
            self._launch(creation.agent_id, request, role_plan)
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

    def cancel(self, agent_id: str | AgentId) -> AgentView:
        """Persist cancellation for the owning supervisor to observe."""

        self._store.enqueue_command(agent_id, "cancel", {}, at=self._now())
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
        :class:`ValidationError`. ``orchestrator`` scopes the new agent's caller
        identity; the parent's artifacts, transcript and answer remain untouched.

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
        profile = self._effective_profile(request, runtime)
        runtime, role_plan = self._runtime_for_profile(
            runtime,
            profile,
            label,
            request.required_constraints,
            request.runtime,
        )
        request = replace(
            request,
            write=role_plan.write,
            read_roots=role_plan.read_roots,
            required_constraints=role_plan.required_constraints,
        )
        _logger.info(
            "resume parent_agent_id=%s runtime=%s request_id=%s",
            parent_id, runtime_name, request_id,
        )
        if request.model not in runtime.models:
            raise ValidationError(
                f"model is no longer configured for runtime {request.runtime}: "
                f"{request.model}"
            )
        policy = self._policy_for_profile(request, runtime, profile)
        return self._admit(
            request,
            runtime,
            label,
            parent_agent_id=parent_id,
            policy=policy,
            role_plan=role_plan,
        )



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
        """Return one consistent factual page, optionally waiting for revision."""

        if not isinstance(query, AgentQuery):
            raise ValidationError("query must be an AgentQuery")
        deadline = time.monotonic() + query.wait_seconds
        while True:
            page = self._list_once(query)
            if (
                query.after_revision is None
                or page.revision > query.after_revision
                or time.monotonic() >= deadline
            ):
                return page
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))

    def _list_once(self, query: AgentQuery) -> AgentPage:
        """Read agents, projection, count, and event revision in one snapshot."""

        statuses = ACTIVE if query.active else None
        connection = self._store.connection
        connection.execute("BEGIN")
        try:
            revision = self._store.events_revision()
            session_id = (
                None
                if query.orchestrator is None
                else self._store.find_orchestrator_session(query.orchestrator)
            )
            rows = [] if query.orchestrator is not None and session_id is None else self._store.list_agents(
                statuses=statuses,
                orchestrator_session_id=session_id,
                limit=query.limit,
                offset=query.offset,
            )
            total = 0 if query.orchestrator is not None and session_id is None else self._count_agents(statuses, session_id)
            projection = self._store.agent_projection(str(row["id"]) for row in rows)
        finally:
            connection.rollback()
        observed_at = self._now()
        items = tuple(
            self._agent_view(row, observed_at, projection[str(row["id"])]) for row in rows
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
            revision,
            observed_at,
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



    def capacity_order(self) -> CapacityOrder:
        """Return enabled runtimes' deterministic capacity routing order.

        Reads one clock value and the committed capacity snapshot only. Disabled
        runtimes are removed from routes and all evidence. Enabled configured
        runtimes with no fresh topology evidence are reported unavailable.
        """

        from .capacity.order import build_capacity_order

        return build_capacity_order(self._store, self._config, now=self._now())

    def resolve_account(self, runtime: str, account: str | None) -> str | None:
        """Return an explicit account label or ``None`` for native global auth.

        ``default_account`` is ignored as a readable legacy declaration. An
        explicit label must remain declared; lookup performs no credential I/O.
        """
        config = self._runtime_config(runtime)
        if account is not None and not config.accounts:
            raise ValidationError(f"runtime {runtime} declares no accounts")
        label = account
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

    def _effective_profile(
        self, request: StartRequest, runtime: RuntimeConfig
    ) -> AgentProfile:
        """Load the exact complete role or legacy compatibility profile."""

        del runtime
        return load_profile(
            self._config.profiles,
            request.profile,
            requested_write=request.write,
            read_roots=request.read_roots,
        )

    def _runtime_for_profile(
        self,
        runtime: RuntimeConfig,
        profile: AgentProfile,
        account_label: str | None,
        required_constraints: frozenset[Constraint],
        runtime_name: str,
    ) -> tuple[RuntimeConfig, ResolvedRolePlan]:
        """Resolve legacy or canonical input once into one adapter role plan."""

        skills_root = self._config.skills_directory
        if profile.canonical and (runtime.skills or runtime.mcp):
            raise ValidationError(
                "canonical role cannot be mixed with runtime skills or MCP declarations"
            )
        if not profile.canonical:
            skills_root = runtime_skills_dir(runtime_name, self._home)
            profile = replace(
                profile,
                revision="legacy",
                skills=runtime.skills,
                mcp=runtime.mcp,
                required_constraints=required_constraints,
                canonical=True,
            )
        role_plan = resolve_role_plan(
            profile,
            skills_root=skills_root,
            mcp_catalog=self._config.mcp,
            auth_mode="global" if account_label is None else "account",
            auth_reference=account_label,
        )
        return replace(runtime, skills=profile.skills, mcp=profile.mcp), role_plan

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
            required=(
                profile.required_constraints
                if profile.canonical
                else request.required_constraints
            ),
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
        phase = "accepted"
        phase_started_at = created_at
        if status in TERMINAL:
            phase = "terminal"
            phase_started_at = finished_at or created_at
        elif status is AgentStatus.CANCELLING:
            phase = "stopping"
            phase_started_at = started_at or created_at
        elif status is AgentStatus.RUNNING:
            phase = "running"
            phase_started_at = started_at or created_at
        elif projection["phase_json"] is not None:
            try:
                phase_payload = json.loads(str(projection["phase_json"]))
            except ValueError:
                phase_payload = None
            candidate = phase_payload.get("phase") if isinstance(phase_payload, dict) else None
            if candidate in {"preparing", "spawning"}:
                phase = candidate
                phase_started_at = float(projection["phase_started_at"])
        supervisor_pid = row["supervisor_pid"]
        if supervisor_pid is None:
            process_state = "not_started"
        else:
            birth = row["supervisor_birth_time"]
            process_state = observe_process(
                int(supervisor_pid),
                None if birth is None else float(birth),
            ).state.value
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
            None
            if row["parent_agent_id"] is None
            else AgentId(str(row["parent_agent_id"])),
            AgentId(str(row["root_agent_id"] or agent_id)),
            int(row["sequence"]),
            cleanup,
            _policy_from_identity(row["identity_json"]),
            phase,
            phase_started_at,
            process_state,
            now,
            status.value if status in TERMINAL else None,
            "pending",
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
