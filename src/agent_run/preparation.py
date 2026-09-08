"""Supervisor-owned runtime preparation after durable READY."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .accounts import account_auth_source, account_runtime_home
from .adapters.base import Capability, LaunchPlan, RuntimeAdapter
from .adapters.home import write_managed_file
from .adapters.registry import AdapterRegistry
from .adapters.snapshots import (
    CONFIG_SNAPSHOT_FILENAME,
    build_config_snapshot,
    inspect_config_snapshot,
    inspect_runtime_snapshots,
    runtime_snapshot_index_sha256,
)
from .config import Config, McpConfig, RuntimeAuthConfig, RuntimeConfig
from .domain import TERMINAL, AgentId, AgentStatus, OrchestratorRef, Outcome, StartRequest, validate_agent_id
from .effective_policy import Constraint
from .errors import AuthError, StateTransitionError, ValidationError
from .paths import agent_dir, create_agent_dir, runtime_skills_dir
from .resume import record_profile_grants
from .role_plan import ResolvedRolePlan
from .state.db import request_json
from .state.store import StateStore


PENDING_CONFIG_REVISION = "pending:materialization"
SNAPSHOT_CONFIG_REVISION = "snapshot:v1:"


@dataclass(frozen=True, slots=True)
class PreparedLaunch:
    """Adapter and in-memory launch plan produced inside the owned supervisor."""

    adapter: RuntimeAdapter
    plan: LaunchPlan
    answer_path: Path


class PreparationFailure(Exception):
    """One stage-specific preparation failure with its original cause."""

    def __init__(self, stage: str, cause: BaseException) -> None:
        """Retain the non-secret stage name and original exception."""

        super().__init__(str(cause).strip() or type(cause).__name__)
        self.stage = stage
        self.cause = cause


class PreparationCancelled(Exception):
    """Signal that durable cancel terminated preparation before engine launch."""


def request_payload(request: StartRequest) -> dict[str, object]:
    """Return the existing canonical StartRequest JSON object."""

    return json.loads(request_json(request))


def request_from_payload(value: object) -> StartRequest:
    """Strictly reconstruct a serialized StartRequest for supervisor preparation."""

    keys = {
        "runtime", "model", "profile", "task", "workdir", "write", "effort",
        "timeout_seconds", "read_roots", "output_schema", "orchestrator",
        "request_id", "fast", "account", "required_constraints",
    }
    if type(value) is not dict or set(value) != keys:
        raise ValidationError("supervisor request payload has an invalid shape")
    payload = value
    orchestrator = payload["orchestrator"]
    if orchestrator is not None:
        expected = {"transport", "external_session_id", "external_turn_id"}
        if type(orchestrator) is not dict or set(orchestrator) != expected:
            raise ValidationError("supervisor request orchestrator has an invalid shape")
        orchestrator = OrchestratorRef(**orchestrator)
    roots = payload["read_roots"]
    constraints = payload["required_constraints"]
    if not isinstance(roots, list) or not isinstance(constraints, list):
        raise ValidationError("supervisor request roots and constraints must be lists")
    try:
        request = StartRequest(
            runtime=payload["runtime"],
            model=payload["model"],
            profile=payload["profile"],
            task=payload["task"],
            workdir=Path(payload["workdir"]),
            write=payload["write"],
            effort=payload["effort"],
            timeout_seconds=payload["timeout_seconds"],
            read_roots=tuple(Path(item) for item in roots),
            output_schema=payload["output_schema"],
            orchestrator=orchestrator,
            request_id=payload["request_id"],
            fast=payload["fast"],
            account=payload["account"],
            required_constraints=frozenset(Constraint(item) for item in constraints),
        )
    except (TypeError, ValueError) as error:
        raise ValidationError("supervisor request payload is invalid") from error
    if request_payload(request) != payload:
        raise ValidationError("supervisor request payload is not canonical")
    return request


def _required_capabilities(request: StartRequest, runtime: RuntimeConfig) -> frozenset[Capability]:
    """Return adapter capabilities required by one normalized request."""

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


def _checkpoint(store: StateStore, agent_id: AgentId, stage: str) -> None:
    """Publish one preparation stage and honor a durable pending cancel."""

    store.append_event(agent_id, "preparation_stage", data={"stage": stage})
    if not store.has_pending_cancel(agent_id):
        return
    status = AgentStatus(str(store.get_agent(agent_id)["status"]))
    if status not in TERMINAL:
        store.transition(
            agent_id,
            AgentStatus.CANCELLED,
            outcome=Outcome(AgentStatus.CANCELLED),
            kind="preparation_cancelled",
        )
    raise PreparationCancelled()


def _mcp_servers(config: Config, runtime: RuntimeConfig) -> Mapping[str, McpConfig]:
    """Resolve selected MCP definitions or reject a stale role reference."""

    try:
        return MappingProxyType({name: config.mcp[name] for name in runtime.mcp})
    except KeyError as error:
        raise ValidationError(f"runtime references unknown MCP server: {error.args[0]}") from error


def prepare_launch(
    store: StateStore,
    home: Path,
    config: Config,
    agent_id: str | AgentId,
    request: StartRequest,
    role: ResolvedRolePlan,
) -> PreparedLaunch:
    """Prepare one admitted run after supervisor ownership is durable.

    Every externally meaningful stage publishes an event and checks durable
    cancel. Adapter/config/auth failures are wrapped with their exact stage;
    credentials remain only in the returned in-memory ``LaunchPlan``.
    """

    checked = validate_agent_id(agent_id)
    row = store.get_agent(checked)
    if request_json(request) != row["request_json"]:
        raise PreparationFailure("request", ValidationError("supervisor request differs from admission"))
    try:
        runtime = config.runtimes[request.runtime]
    except KeyError as error:
        raise PreparationFailure("runtime", ValidationError(f"runtime is not configured: {request.runtime}")) from error
    runtime = replace(
        runtime,
        skills=tuple(skill.id for skill in role.skills),
        mcp=tuple(server.id for server in role.mcp),
    )
    candidate_dir = create_agent_dir(checked, home)
    account_label = role.auth_reference if role.auth_mode == "account" else None
    stage = "account"
    try:
        _checkpoint(store, checked, stage)
        configured_home = runtime.home
        effective_auth = runtime.auth
        if account_label is not None:
            if runtime.adapter in {
                "agent_run.adapters.claude:ADAPTER",
                "agent_run.adapters.claude.adapter:ADAPTER",
            }:
                configured_home = account_runtime_home(runtime.home, account_label)
                effective_auth = None
            else:
                target = runtime.auth.target if runtime.auth and runtime.auth.target else "auth.json"
                source = account_auth_source(home, request.runtime, account_label, target)
                if not source.is_file():
                    raise AuthError(
                        f"account {account_label!r} is not authenticated; run agent-run auth {account_label} {request.runtime}"
                    )
                configured_home = account_runtime_home(runtime.home, account_label)
                effective_auth = RuntimeAuthConfig("file_link", source=source, target=target)

        parent_id = None if row["parent_agent_id"] is None else validate_agent_id(str(row["parent_agent_id"]))
        parent_revision = None
        lineage_id = parent_id
        snapshot_resume = False
        if parent_id is not None:
            parent = store.get_agent(parent_id)
            parent_revision = str(parent["config_revision"])
            lineage_id = validate_agent_id(str(parent["root_agent_id"] or parent_id))
            snapshot_resume = parent_revision.startswith(SNAPSHOT_CONFIG_REVISION)
        if parent_id is None:
            effective_home = candidate_dir / "runtime-home"
            effective_home.mkdir(mode=0o700)
        elif snapshot_resume:
            assert lineage_id is not None
            effective_home = agent_dir(lineage_id, home) / "runtime-home"
        else:
            effective_home = configured_home
            effective_home.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not snapshot_resume:
            effective_home.chmod(0o700)
        runtime = replace(
            runtime,
            home=effective_home,
            auth=effective_auth,
            default_account=None,
            credential_state_home=configured_home if account_label is not None else None,
        )

        stage = "adapter"
        _checkpoint(store, checked, stage)
        required = _required_capabilities(request, runtime)
        if row["parent_agent_id"] is not None:
            required = required | {Capability.RESUME}
        adapter = AdapterRegistry(config).load(request.runtime, required)
        adapter.validate(runtime)
        record_profile_grants(store.connection, checked, role)
        servers = _mcp_servers(config, runtime)

        stored_snapshot = None
        if snapshot_resume:
            stage = "snapshot"
            _checkpoint(store, checked, stage)
            assert lineage_id is not None and parent_revision is not None
            expected_sha = parent_revision.removeprefix(SNAPSHOT_CONFIG_REVISION)
            stored_snapshot = inspect_config_snapshot(agent_dir(lineage_id, home), expected_sha)
            inspection = inspect_runtime_snapshots(
                effective_home,
                stored_snapshot.materialize_revision,
                expected_sha256=stored_snapshot.snapshot_index_sha256,
            )
            if not inspection.verified:
                raise ValidationError("runtime snapshot is incomplete or changed")
            comparison = runtime if stored_snapshot.credential_state_home_bound else replace(runtime, credential_state_home=None)
            current = build_config_snapshot(
                runtime=request.runtime,
                adapter_api_version=adapter.describe().adapter_api_version,
                schema_version=config.schema_version,
                materialize_revision=stored_snapshot.materialize_revision,
                snapshot_index_sha256=stored_snapshot.snapshot_index_sha256,
                config=comparison,
                profile=role,
                runtime_version=stored_snapshot.runtime_version,
                legacy_profile_shape=not stored_snapshot.role_plan_bound,
            )
            if current.sha256 != expected_sha:
                raise ValidationError("effective runtime or profile changed since the parent snapshot")
            revision = parent_revision
        else:
            stage = "materialize"
            _checkpoint(store, checked, stage)
            skills_root = (
                runtime_skills_dir(request.runtime, home)
                if role.role_revision == "legacy"
                else config.skills_directory
            )
            revision = adapter.materialize(
                runtime, effective_home, mcp_servers=servers, skills_root=skills_root
            )

        stage = "models"
        _checkpoint(store, checked, stage)
        if request.model not in {model.id for model in adapter.models(runtime, effective_home)}:
            raise ValidationError(f"model is not available for runtime {request.runtime}: {request.model}")

        stage = "prepare"
        _checkpoint(store, checked, stage)
        resume_id = str(row["resume_of_runtime_session_id"]) if snapshot_resume else None
        plan = adapter.prepare(
            request, role, runtime, effective_home, candidate_dir, resume_session_id=resume_id
        )
        if plan.resume_session_id != resume_id:
            raise ValidationError("adapter returned a mismatched resume session")
        if row["resume_of_runtime_session_id"] is not None and not snapshot_resume:
            plan = replace(plan, resume_session_id=str(row["resume_of_runtime_session_id"]))
        if snapshot_resume:
            assert stored_snapshot is not None
            if plan.materialize_revision is not None:
                raise ValidationError("snapshot resume must not rematerialize its runtime home")
            inspection = inspect_runtime_snapshots(
                effective_home,
                stored_snapshot.materialize_revision,
                expected_sha256=stored_snapshot.snapshot_index_sha256,
            )
            if not inspection.verified:
                raise ValidationError("adapter prepare changed the verified runtime snapshot")
        else:
            revision = plan.materialize_revision or revision

        if parent_id is None:
            stage = "snapshot"
            _checkpoint(store, checked, stage)
            index_sha = runtime_snapshot_index_sha256(effective_home, revision)
            if not inspect_runtime_snapshots(effective_home, revision, expected_sha256=index_sha).verified:
                raise ValidationError("fresh runtime snapshot is incomplete after prepare")
            snapshot = build_config_snapshot(
                runtime=request.runtime,
                adapter_api_version=adapter.describe().adapter_api_version,
                schema_version=config.schema_version,
                materialize_revision=revision,
                snapshot_index_sha256=index_sha,
                config=runtime,
                profile=role,
                runtime_version=adapter.probe(runtime, effective_home).version,
            )
            write_managed_file(candidate_dir, CONFIG_SNAPSHOT_FILENAME, snapshot.document)
            revision = SNAPSHOT_CONFIG_REVISION + snapshot.sha256
        stage = "config_revision"
        _checkpoint(store, checked, stage)
        store.replace_config_revision(checked, PENDING_CONFIG_REVISION, revision)
        return PreparedLaunch(adapter, plan, candidate_dir / "answer.md")
    except PreparationCancelled:
        raise
    except BaseException as error:
        raise PreparationFailure(stage, error) from error


def fail_preparation(store: StateStore, agent_id: str | AgentId, error: PreparationFailure) -> Outcome:
    """Commit one stage-specific terminal preparation failure."""

    kind = "auth_failed" if isinstance(error.cause, AuthError) else f"prepare_{error.stage}_failed"
    outcome = Outcome(
        AgentStatus.FAILED,
        failure_kind=kind,
        failure_text=(str(error.cause).strip() or type(error.cause).__name__)[:1000],
    )
    try:
        store.transition(agent_id, AgentStatus.FAILED, outcome=outcome, kind=kind)
    except StateTransitionError:
        if AgentStatus(str(store.get_agent(agent_id)["status"])) not in TERMINAL:
            raise
    return outcome
