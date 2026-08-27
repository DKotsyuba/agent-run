"""Bounded reconciliation driven by exact detached-child exit proof."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agent_run.domain import ACTIVE, AgentId
from agent_run.errors import StateTransitionError, ValidationError

from .db import integer, nonblank, timestamp
from .workflow import finish_workflow_run

if TYPE_CHECKING:
    from .store import StateStore


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
    return tuple(changed)


def reconcile_active_agents(store, *, at: float | None = None, limit: int = 100) -> tuple[AgentId, ...]:
    """Boundedly close dead detached supervisors during an independent sweep."""
    from ..doctor import _probe_process

    statuses = tuple(sorted(status.value for status in ACTIVE))
    placeholders = ",".join("?" for _ in statuses)
    rows = list(
        store.connection.execute(
            f"SELECT id, supervisor_pid, process_group_id, supervisor_identity FROM agents "
            f"WHERE status IN ({placeholders}) ORDER BY created_at, id LIMIT ?",
            (*statuses, limit),
        )
    )
    changed = []
    for row in rows:
        pid, pgid, expected = row["supervisor_pid"], row["process_group_id"], row["supervisor_identity"]
        if not isinstance(pid, int) or not isinstance(pgid, int) or not isinstance(expected, str):
            continue
        # The recorded group is never signalled here: it may be reused, foreign,
        # or shared, and orphan reporting stays diagnostic in the doctor.
        alive, identity, _group_alive = _probe_process(pid, pgid)
        # Time the proof after this row's probe, so a slow probe cannot be judged
        # against a clock captured before the sweep started.
        checked_at = timestamp(at)
        if alive and identity is None:
            continue  # a live supervisor without identity is unknown, not a verdict
        matches = identity == expected or (isinstance(identity, str) and identity.endswith(f" {expected}"))
        if alive and matches:
            continue
        try:
            committed = store.reconcile(
                str(row["id"]), verdict="dead" if not alive else "identity_mismatch",
                supervisor_pid=pid, process_group_id=pgid, expected_identity=expected,
                alive=alive, observed_identity=identity, checked_at=checked_at,
                reason="periodic detached supervisor reconciliation",
            )
        except (ValidationError, StateTransitionError):
            continue  # one stale or concurrently changed row never aborts the sweep
        if committed:
            changed.append(AgentId(str(row["id"])))
    return tuple(changed)


def workflow_owner_identity(pid: int, identity: str) -> str:
    """The owner string a workflow runner records: its pid, then its ps command.

    ``workflow_runs`` carries a single ownership column, so the pid has to
    travel inside the identity for reconciliation to have anything to probe;
    :func:`reconcile_workflow_runs` splits it back off here.
    """

    integer("pid", pid, minimum=1)
    return f"{pid} {nonblank('identity', identity).strip()}"


def _split_owner(owner: str) -> tuple[int | None, str]:
    head, _, rest = owner.partition(" ")
    try:
        pid = int(head)
    except ValueError:
        return None, owner
    return (pid if pid > 1 else None), rest.strip()


def reconcile_workflow_runs(store, *, at: float | None = None, limit: int = 100) -> tuple[str, ...]:
    """Flip every run whose owning runner is gone to ``lost``; never resume one.

    Mirrors :func:`reconcile_active_agents`: the identity the runner claimed the
    run with carries its pid, so the same ps probe settles liveness.  A run no
    runner has claimed yet is nobody's to lose, and a live owner whose identity
    cannot be read is unknown rather than dead.
    """

    from ..doctor import _probe_process

    integer("limit", limit, minimum=1)
    rows = list(
        store.connection.execute(
            """SELECT id, owner_pid_identity FROM workflow_runs
               WHERE status IN ('created', 'running') AND owner_pid_identity IS NOT NULL
               ORDER BY created_at, id LIMIT ?""",
            (limit,),
        )
    )
    changed = []
    for row in rows:
        owner = row["owner_pid_identity"]
        if not isinstance(owner, str):
            continue
        pid, expected = _split_owner(owner)
        if pid is None or not expected:
            continue  # an owner identity that cannot be probed is not a verdict
        alive, identity, _group_alive = _probe_process(pid, None)
        if alive and identity is None:
            continue  # a live owner without identity is unknown, not a verdict
        matches = identity == expected or (
            isinstance(identity, str) and identity.endswith(f" {expected}")
        )
        if alive and matches:
            continue
        try:
            finish_workflow_run(store.connection, str(row["id"]), "lost", at=at)
        except (ValidationError, StateTransitionError):
            continue  # one stale or concurrently changed row never aborts the sweep
        changed.append(str(row["id"]))
    return tuple(changed)

