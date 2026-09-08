"""Bounded reconciliation driven by exact detached-child exit proof."""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

from agent_run.domain import ACTIVE, AgentId, AgentStatus, Outcome
from agent_run.errors import StateTransitionError, ValidationError
from agent_run.process_identity import ProcessState, observe_process

from .db import immediate, integer, nonblank, timestamp

if TYPE_CHECKING:
    from .store import StateStore

_logger = logging.getLogger("agent_run.state")
DEFAULT_UNOWNED_STARTING_GRACE_SECONDS = 30.0


def _fair_rows(
    store: StateStore,
    name: str,
    select: str,
    params: tuple[object, ...],
    limit: int,
) -> list[object]:
    """Return and advance one persisted keyset window over ``select``.

    ``select`` is an internal fixed SQL prefix ending in a complete WHERE
    predicate over ``agents``; ``params`` binds that predicate. Rows are ordered
    by ``created_at, id``, continue strictly after the stored cursor, then wrap
    once to the beginning. Advancing to the last selected row makes live early
    entries unable to starve later candidates across process restarts.
    """

    cursor = store.connection.execute(
        "SELECT created_at, agent_id FROM reconciliation_cursors WHERE name = ?",
        (name,),
    ).fetchone()
    suffix = " ORDER BY created_at, id LIMIT ?"
    if cursor is None:
        rows = list(store.connection.execute(select + suffix, (*params, limit)))
    else:
        created_at, agent_id = float(cursor["created_at"]), str(cursor["agent_id"])
        rows = list(
            store.connection.execute(
                select
                + " AND (created_at > ? OR (created_at = ? AND id > ?))"
                + suffix,
                (*params, created_at, created_at, agent_id, limit),
            )
        )
        if len(rows) < limit:
            rows.extend(
                store.connection.execute(
                    select
                    + " AND (created_at < ? OR (created_at = ? AND id <= ?))"
                    + suffix,
                    (
                        *params,
                        created_at,
                        created_at,
                        agent_id,
                        limit - len(rows),
                    ),
                )
            )
    if rows:
        last = rows[-1]
        with immediate(store.connection):
            store.connection.execute(
                """INSERT INTO reconciliation_cursors(name, created_at, agent_id)
                   VALUES (?, ?, ?)
                   ON CONFLICT(name) DO UPDATE SET
                     created_at = excluded.created_at, agent_id = excluded.agent_id""",
                (name, float(last["created_at"]), str(last["id"])),
            )
    return rows


def reconcile_reaped_agent(
    store: StateStore,
    agent_id: str | AgentId,
    supervisor_pid: int,
    *,
    at: float | None = None,
) -> bool:
    """Close the exact agent whose detached supervisor was reaped by waitpid."""

    from .store import StateStore

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    return store.reconcile_reaped(agent_id, supervisor_pid, checked_at=at)


def reconcile_reaped_supervisor(
    store: StateStore,
    supervisor_pid: int,
    *,
    at: float | None = None,
    limit: int = 100,
) -> tuple[AgentId, ...]:
    """Mark active rows owned by one reaped supervisor lost; never signal."""

    from .store import StateStore

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    integer("supervisor_pid", supervisor_pid, minimum=1)
    integer("limit", limit, minimum=1)
    if limit > 1_000:
        raise ValidationError("limit must not exceed 1000")
    checked_at = timestamp(at)
    statuses = tuple(sorted(status.value for status in ACTIVE))
    placeholders = ",".join("?" for _ in statuses)
    rows = list(
        store.connection.execute(
            f"""SELECT id, supervisor_pid, process_group_id, supervisor_identity
                FROM agents WHERE supervisor_pid = ?
                  AND status IN ({placeholders})
                ORDER BY created_at, id LIMIT ?""",
            (supervisor_pid, *statuses, limit),
        )
    )
    changed = []
    for row in rows:
        pid = row["supervisor_pid"]
        pgid = row["process_group_id"]
        identity = row["supervisor_identity"]
        if not isinstance(pid, int) or not isinstance(pgid, int) or not isinstance(identity, str):
            continue
        try:
            committed = store.reconcile(
                str(row["id"]),
                verdict="dead",
                supervisor_pid=pid,
                process_group_id=pgid,
                expected_identity=identity,
                alive=False,
                checked_at=checked_at,
                reason="detached supervisor exited",
            )
        except (ValidationError, StateTransitionError):
            continue  # one stale or concurrently changed row never aborts the sweep
        if committed:
            changed.append(AgentId(str(row["id"])))
    _logger.debug(
        "reconcile_reaped_supervisor pid=%d candidates=%d changed=%d",
        supervisor_pid, len(rows), len(changed),
    )
    return tuple(changed)


def reconcile_unowned_starting(
    store: StateStore,
    *,
    at: float | None = None,
    grace_seconds: float = DEFAULT_UNOWNED_STARTING_GRACE_SECONDS,
    limit: int = 100,
) -> tuple[AgentId, ...]:
    """Converge stale or expired ``STARTING`` rows to ``LOST``.

    Candidate selection and process probes run outside the short terminal
    transition transaction. Recent, supervisor-owned and non-``STARTING`` rows are untouched. A coordinator
    owner protects preparation and spawn handoff only until its durable deadline; probes run
    outside the short transition transactions so a slow ``ps`` cannot block
    writers.
    """

    from .store import StateStore

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    if (
        isinstance(grace_seconds, bool)
        or not isinstance(grace_seconds, (int, float))
        or not math.isfinite(grace_seconds)
        or grace_seconds < 0
    ):
        raise ValidationError("grace_seconds must be nonnegative and finite")
    integer("limit", limit, minimum=1)
    if limit > 1_000:
        raise ValidationError("limit must not exceed 1000")
    checked_at = timestamp(at)
    cutoff = checked_at - float(grace_seconds)
    changed: list[AgentId] = []
    rows = _fair_rows(
        store,
        "unowned_starting",
        """SELECT id, created_at, startup_owner_pid_identity,
                  startup_owner_birth_time, startup_deadline_at
           FROM agents
           WHERE status = ? AND supervisor_pid IS NULL
             AND process_group_id IS NULL AND supervisor_identity IS NULL
             AND (created_at <= ? OR startup_deadline_at <= ?)""",
        (AgentStatus.STARTING.value, cutoff, checked_at),
        limit,
    )
    for row in rows:
        agent_id = AgentId(str(row["id"]))
        deadline = row["startup_deadline_at"]
        owner_live = False
        if isinstance(deadline, (int, float)) and deadline > checked_at:
            owner = row["startup_owner_pid_identity"]
            if isinstance(owner, str):
                pid = _owner_pid(owner)
                if pid is not None:
                    birth = row["startup_owner_birth_time"]
                    observation = observe_process(
                        pid, float(birth) if isinstance(birth, (int, float)) else None
                    )
                    if observation.state in {
                        ProcessState.ALIVE,
                        ProcessState.UNKNOWN,
                        ProcessState.DENIED,
                    }:
                        owner_live = True
        if owner_live:
            continue
        with immediate(store.connection):
            current = store.connection.execute(
                """SELECT status, supervisor_pid, process_group_id, supervisor_identity,
                          startup_deadline_at FROM agents WHERE id = ?""",
                (agent_id,),
            ).fetchone()
            if current is None or current["status"] != AgentStatus.STARTING.value:
                continue
            if any(current[field] is not None for field in ("supervisor_pid", "process_group_id", "supervisor_identity")):
                continue
            current_deadline = current["startup_deadline_at"]
            if current_deadline != deadline:
                continue
            store._transition(
                agent_id,
                AgentStatus.LOST,
                checked_at,
                outcome=Outcome(
                    AgentStatus.LOST,
                    failure_kind="unowned_starting",
                    failure_text="accepted start exceeded its startup ownership deadline",
                ),
                attempt_id=None,
                kind="reconciled_lost",
                data={"verdict": "unowned_starting"},
            )
            changed.append(agent_id)
    return tuple(changed)


def reconcile_active_agents(
    store,
    *,
    at: float | None = None,
    limit: int = 100,
) -> tuple[AgentId, ...]:
    """Boundedly converge abandoned starts and proven-dead supervisors.

    ``store`` is the owning state connection, ``at`` is the optional wall-clock
    proof time, and ``limit`` caps committed rows. Missing birth evidence may
    prove an absent legacy PID dead, but a present PID remains unknown; matching
    births stay active, while differing births prove reuse. Returns changed IDs
    in storage order and never signals any PID or process group.
    """

    changed = list(reconcile_unowned_starting(store, at=at, limit=limit))
    remaining = limit - len(changed)
    if remaining <= 0:
        return tuple(changed)

    statuses = tuple(sorted(status.value for status in ACTIVE))
    placeholders = ",".join("?" for _ in statuses)
    rows = _fair_rows(
        store,
        "active_supervisors",
        f"""SELECT id, created_at, supervisor_pid, process_group_id,
                   supervisor_identity, supervisor_birth_time FROM agents
            WHERE status IN ({placeholders})""",
        statuses,
        remaining,
    )
    for row in rows:
        pid, pgid, expected, birth = row["supervisor_pid"], row["process_group_id"], row["supervisor_identity"], row["supervisor_birth_time"]
        if not isinstance(pid, int) or not isinstance(pgid, int) or not isinstance(expected, str):
            continue
        # The recorded group is never signalled here: it may be reused, foreign,
        # or shared, and orphan reporting stays diagnostic in the doctor.
        observation = observe_process(
            pid, float(birth) if isinstance(birth, (int, float)) else None
        )
        # Time the proof after this row's probe, so a slow probe cannot be judged
        # against a clock captured before the sweep started.
        checked_at = timestamp(at)
        if observation.state is ProcessState.ALIVE:
            continue
        if observation.state not in {ProcessState.DEAD, ProcessState.REUSED}:
            continue
        try:
            committed = store.reconcile(
                str(row["id"]), verdict="dead" if observation.state is ProcessState.DEAD else "identity_mismatch",
                supervisor_pid=pid, process_group_id=pgid, expected_identity=expected,
                expected_birth_time=float(birth) if isinstance(birth, (int, float)) else None,
                alive=observation.state is not ProcessState.DEAD,
                observed_birth_time=observation.create_time,
                checked_at=checked_at,
                reason="periodic detached supervisor reconciliation",
            )
        except (ValidationError, StateTransitionError):
            continue  # one stale or concurrently changed row never aborts the sweep
        if committed:
            changed.append(AgentId(str(row["id"])))
    if changed:
        _logger.info("reconcile_active_agents candidates=%d changed=%d", len(rows), len(changed))
    else:
        _logger.debug("reconcile_active_agents candidates=%d changed=%d", len(rows), len(changed))
    return tuple(changed)


def process_owner_identity(pid: int, identity: str) -> str:
    """Return a process owner's PID plus diagnostic command text.

    Reconciliation uses the PID only with the separately persisted process
    birth time; command text is never ownership authority.
    """

    integer("pid", pid, minimum=1)
    return f"{pid} {nonblank('identity', identity).strip()}"


def _owner_pid(owner: str) -> int | None:
    """Return the positive PID encoded by a process owner string."""

    head, _, _rest = owner.partition(" ")
    try:
        pid = int(head)
    except ValueError:
        return None
    return pid if pid > 1 else None
