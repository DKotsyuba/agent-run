"""Effective-identity snapshots and inherited requests for agent resume.

A resume must continue the run the parent actually had, not the run current
configuration would produce today. Two things make that possible:

* the parent's ``request_json`` -- the caller's canonical request, replayed
  verbatim except for task and timeout;
* the parent's ``identity_json`` -- the secret-free snapshot of what the
  service *resolved* that request into (account label, runtime home, auth
  target, granted permissions).

They are stored separately on purpose. ``request_json`` is what idempotent
replay compares, so recording resolved identity must never touch it.

Nothing here serializes a credential, an environment, or an argv: the snapshot
holds identifiers and paths only, and is compared, never replayed.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Mapping

from .accounts import account_runtime_home
from .config import RuntimeConfig
from .domain import AgentId, OrchestratorRef, StartRequest
from .errors import ValidationError
from .profiles import AgentProfile
from .state.db import agent_row, idempotent_agent, immediate, nonblank, positive_number


def record_profile_grants(
    connection: sqlite3.Connection, agent_id: AgentId, profile: AgentProfile
) -> None:
    """Pin the loaded profile's effective grants before any native launch.

    The connection belongs to the preparation worker. Fresh runs record their
    actual write, network and read-root grants; continuations must exactly match
    their inherited snapshot, including after an earlier preparation failure.
    Missing or changed grants raise ValidationError without changing the row.
    The single transaction updates only this run's secret-free identity JSON.
    """
    grants = {
        "write": profile.write,
        "network": profile.network,
        "read_roots": sorted(str(path) for path in profile.read_roots),
    }
    with immediate(connection):
        row = agent_row(connection, agent_id)
        try:
            snapshot = json.loads(row["identity_json"])
        except (TypeError, ValueError) as error:
            raise ValidationError("invalid effective-identity snapshot") from error
        if not isinstance(snapshot, dict):
            raise ValidationError("invalid effective-identity snapshot")
        if row["parent_agent_id"] is not None and snapshot.get("profile_grants") != grants:
            raise ValidationError("profile grants changed or were not recorded; refusing resume")
        snapshot["profile_grants"] = grants
        connection.execute(
            "UPDATE agents SET identity_json = ? WHERE id = ?",
            (json.dumps(snapshot, sort_keys=True, separators=(",", ":")), agent_id),
        )


def replayed_resume(
    connection: sqlite3.Connection, parent: Mapping[str, object], task: str,
    timeout_seconds: float | None, request_id: str | None,
    orchestrator: OrchestratorRef | None,
) -> AgentId | None:
    """Return an accepted matching resume before inspecting mutable resources.

    Validate the caller-controlled task, timeout and notification reference.
    A known request ID must match its immutable parent, prompt, effective timeout
    and caller; conflicts raise ValidationError. Unknown or absent IDs return
    None and normal admission still performs its atomic replay/race check.
    This read-only lookup never resolves filesystem paths or current config.
    """
    nonblank("task", task)
    timeout = positive_number(
        "timeout_seconds", parent["timeout_seconds"] if timeout_seconds is None else timeout_seconds
    )
    if orchestrator is not None and not isinstance(orchestrator, OrchestratorRef):
        raise ValidationError("orchestrator must be an OrchestratorRef or None")
    if request_id is None:
        return None
    nonblank("request_id", request_id)
    existing = idempotent_agent(connection, request_id)
    if existing is None:
        return None
    stored = json.loads(existing["request_json"])
    caller = None if orchestrator is None else asdict(orchestrator)
    if (
        existing["parent_agent_id"] != parent["id"]
        or stored.get("task") != task
        or stored.get("timeout_seconds") != timeout
        or stored.get("orchestrator") != caller
    ):
        raise ValidationError("request_id was reused for a different request")
    return AgentId(str(existing["id"]))


def effective_identity(
    runtime_name: str, runtime: RuntimeConfig, label: str | None
) -> dict[str, object]:
    """Return the identity a start under ``label`` resolves to right now.

    ``label`` is the resolved account, or ``None`` for a runtime with no
    accounts *or* for the unlabelled base account; the returned ``home``
    distinguishes those two. Used both to record a new start's identity and to
    recompute today's identity when checking a resume for drift, so the two can
    never disagree about what "the same identity" means.

    Contains no secret: ``auth_target`` is the configuration key naming where a
    credential lives, not the credential.
    """

    return {
        "runtime": runtime_name,
        "account": label,
        "home": str(
            runtime.home
            if label is None
            else account_runtime_home(runtime.home, label)
        ),
        "auth_target": None if runtime.auth is None else runtime.auth.target,
    }


def identity_snapshot(
    runtime_name: str,
    runtime: RuntimeConfig,
    label: str | None,
    request: StartRequest,
) -> str:
    """Serialize what one new start is actually being launched under.

    Extends :func:`effective_identity` with the permission and routing facts a
    later resume must inherit rather than re-derive: the requested profile and
    its ``write``/``read_roots`` grants, and ``fast``. Those inherited fields
    are recorded here rather than in ``request_json`` because that payload is
    what idempotent replay compares byte-for-byte.

    Returns canonical JSON with sorted keys. Preparation completes this routing
    snapshot with actual profile grants through :func:`record_profile_grants`.
    A continuation carries its parent's completed snapshot instead of resetting
    those grants when preparation fails before attaching to the native session.
    """

    return json.dumps(
        {
            **effective_identity(runtime_name, runtime, label),
            "profile": request.profile,
            "write": request.write,
            "read_roots": sorted(str(path) for path in request.read_roots),
            "fast": request.fast,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def proven_identity(
    parent_id: AgentId,
    identity_json: object,
    runtime_name: str,
    runtime: RuntimeConfig,
) -> tuple[str | None, dict[str, object]]:
    """Return the account a resume may reuse, or refuse to guess one.

    ``identity_json`` is the parent row's snapshot column: ``None`` for a run
    that predates snapshots, otherwise the JSON written by
    :func:`identity_snapshot`.

    A missing snapshot is an explicit refusal -- ``account: null`` in a legacy
    row is indistinguishable from "whatever the default was", so it can never
    be proved. A *present* snapshot with ``account: null`` is proof of the
    unlabelled base account and is honoured even when the runtime now declares
    other labels, and even when its ``default_account`` has since changed.

    The snapshot's home and auth target must still resolve to the same values
    today: a label that now points at a different runtime home, or a runtime
    whose auth target moved, is drift and is refused rather than silently
    redirected.

    Returns the proven account label (or ``None`` for the base/no-account
    case) together with the parsed snapshot, whose ``profile``/``write``/
    ``read_roots``/``fast`` entries the caller inherits.

    Raise :class:`ValidationError` for a missing, unparseable, or drifted
    snapshot, or for a label the runtime no longer declares.
    """

    if identity_json is None:
        raise ValidationError(
            f"agent {parent_id} recorded no effective-identity snapshot; its "
            f"original {runtime_name} account and home cannot be proved from "
            f"persisted state"
        )
    try:
        snapshot = json.loads(str(identity_json))
    except ValueError as error:
        raise ValidationError(
            f"agent {parent_id} has an unreadable identity snapshot"
        ) from error
    if not isinstance(snapshot, dict):
        raise ValidationError(
            f"agent {parent_id} has an unreadable identity snapshot"
        )
    label = snapshot.get("account")
    if label is not None:
        if not isinstance(label, str) or label not in runtime.accounts:
            known = ", ".join(runtime.accounts) or "none"
            raise ValidationError(
                f"account {label!r} used by agent {parent_id} is no longer "
                f"declared for runtime {runtime_name}; known accounts: {known}"
            )
    current = effective_identity(runtime_name, runtime, label)
    recorded = {key: snapshot.get(key) for key in current}
    if recorded != current:
        raise ValidationError(
            f"runtime {runtime_name} identity changed since agent {parent_id} "
            f"ran ({recorded} -> {current}); refusing to resume it under a "
            f"different home or credential"
        )
    return label, snapshot


def inherited_request(
    row: Mapping[str, object],
    snapshot: Mapping[str, object],
    label: str | None,
    task: str,
    timeout_seconds: float | None,
    request_id: str | None,
    orchestrator: OrchestratorRef | None,
) -> StartRequest:
    """Rebuild a parent's start request for its resumed child.

    ``row`` is the parent agent row; its ``request_json`` is the only source of
    inherited request fields, and ``snapshot`` -- the identity already proved
    by :func:`proven_identity` -- supplies the resolved profile, permissions
    and ``fast`` routing, with ``label`` the proven account. Current
    configuration is never consulted, so it cannot redirect the child.
    ``task`` replaces the prompt, ``timeout_seconds``
    overrides the parent's when not ``None`` and inherits it when ``None``, and
    ``request_id`` and ``orchestrator`` belong to *this* call -- the new agent's
    notifications bind to the resuming caller, never to the original one.

    Raise :class:`ValidationError` -- through :class:`StartRequest` validation
    -- when the inherited workdir or a read root no longer exists. Such a
    directory is never recreated or substituted: an unprovable workspace is a
    refusal, not a new one.
    """

    stored = json.loads(str(row["request_json"]))
    return StartRequest(
        runtime=str(row["runtime"]),
        model=str(row["model"]),
        profile=str(snapshot.get("profile") or row["profile"]),
        task=task,
        workdir=Path(str(stored["workdir"])),
        write=bool(snapshot.get("write", False)),
        effort=stored.get("effort"),
        timeout_seconds=(
            float(row["timeout_seconds"])
            if timeout_seconds is None
            else positive_number("timeout_seconds", timeout_seconds)
        ),
        read_roots=tuple(Path(str(path)) for path in snapshot.get("read_roots") or ()),
        output_schema=stored.get("output_schema"),
        orchestrator=orchestrator,
        request_id=request_id,
        fast=bool(snapshot.get("fast", False)),
        account=label,
    )
