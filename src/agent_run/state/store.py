"""Guarded durable state operations for agent-run."""

from __future__ import annotations

import json
import logging
import math
import sqlite3
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

_logger = logging.getLogger("agent_run.state")

from agent_run.domain import (
    ACTIVE,
    TERMINAL,
    AgentId,
    AgentStatus,
    Message,
    OrchestratorRef,
    Outcome,
    StartRequest,
    validate_agent_id,
    validate_transition,
)
from agent_run.errors import StateTransitionError, ValidationError

if TYPE_CHECKING:
    from agent_run.delivery.base import DeliveryAttemptEvidence

from . import capacity, delivery, workflow
from .db import (
    _upsert_context_receipt,
    agent_row,
    checked_supervisor_proof,
    connection_path,
    count_agents,
    immediate,
    integer,
    insert_capacity_row,
    initialize_database,
    insert_event,
    json_text,
    message_rows,
    nonblank,
    open_database,
    recent_capacity_rows,
    require_attempt,
    resolve_message_storage,
    row_dict,
    session_for_ref,
    timestamp,
)

_MAX_DELIVERY_EVIDENCE_JSON_BYTES = 16384


def _delivery_evidence_json(
    evidence: DeliveryAttemptEvidence | None,
) -> str | None:
    """Return bounded canonical JSON for typed evidence, or ``None``."""

    if evidence is None:
        return None
    from agent_run.delivery.base import DeliveryAttemptEvidence

    if not isinstance(evidence, DeliveryAttemptEvidence):
        raise ValidationError("evidence must be DeliveryAttemptEvidence or None")
    encoded = json_text(evidence.payload())
    if len(encoded.encode("utf-8")) > _MAX_DELIVERY_EVIDENCE_JSON_BYTES:
        raise ValidationError("delivery attempt evidence exceeds 16384 bytes")
    return encoded
from .start import AgentCreation, create_agent as create_agent_record


class StateStore:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.connection.row_factory = sqlite3.Row

    @classmethod
    def initialize(cls, database: str | Path) -> StateStore:
        return cls(initialize_database(database))

    @classmethod
    def open(cls, database: str | Path) -> StateStore:
        """Open an existing state database, sweeping abandoned workflow runs.

        Only a live process can notice that a detached workflow runner is gone,
        so every open pays for one bounded reconciliation pass. An abandoned run
        is flipped to ``lost`` there -- it is never silently resumed.
        """

        from .reconciliation import reconcile_workflow_runs

        store = cls(open_database(database))
        _logger.debug("db=%s open", database)
        try:
            reconcile_workflow_runs(store)
        except BaseException:
            store.close()
            raise
        return store

    def close(self) -> None:
        self.connection.close()

    def path(self) -> Path:
        """The database file this store's connection was opened from.

        Lets a caller open its own connection to the same database from a
        different thread, since sqlite3 connections are thread-affine.
        """

        return connection_path(self.connection)

    def create_agent(
        self,
        request: StartRequest,
        *,
        task_summary: str,
        config_revision: str,
        agent_id: str | AgentId | None = None,
        at: float | None = None,
    ) -> AgentCreation:
        if isinstance(request, StartRequest) and request.timeout_seconds is None:
            raise ValidationError("timeout_seconds must be resolved before persistence")
        return create_agent_record(
            self.connection,
            request,
            task_summary=task_summary,
            config_revision=config_revision,
            agent_id=agent_id,
            at=at,
        )

    def create_agent_limited(
        self,
        request: StartRequest,
        *,
        task_summary: str,
        config_revision: str,
        global_limit: int,
        runtime_limit: int | None,
        agent_id: str | AgentId | None = None,
        at: float | None = None,
    ) -> AgentCreation:
        if isinstance(request, StartRequest) and request.timeout_seconds is None:
            raise ValidationError("timeout_seconds must be resolved before persistence")
        return create_agent_record(
            self.connection,
            request,
            task_summary=task_summary,
            config_revision=config_revision,
            global_limit=global_limit,
            runtime_limit=runtime_limit,
            agent_id=agent_id,
            at=at,
        )

    def replace_config_revision(
        self,
        agent_id: str | AgentId,
        expected_revision: str,
        replacement_revision: str,
    ) -> bool:
        """Atomically replace one pending configuration revision.

        Returning ``True`` means this call replaced ``expected_revision``;
        ``False`` means the requested replacement was already durable. Any
        other current revision is rejected.
        """

        checked = validate_agent_id(agent_id)
        nonblank("expected config revision", expected_revision)
        nonblank("replacement config revision", replacement_revision)
        with immediate(self.connection):
            row = agent_row(self.connection, checked)
            current = str(row["config_revision"])
            if current == replacement_revision:
                return False
            if current != expected_revision:
                raise ValidationError("config revision changed concurrently")
            updated = self.connection.execute(
                """UPDATE agents SET config_revision = ?
                   WHERE id = ? AND config_revision = ?""",
                (replacement_revision, checked, expected_revision),
            ).rowcount
            if updated != 1:
                raise ValidationError("config revision changed concurrently")
        return True

    def has_pending_cancel(self, agent_id: str | AgentId) -> bool:
        """Return whether a pending or claimed durable cancel command exists."""

        checked = validate_agent_id(agent_id)
        agent_row(self.connection, checked)
        row = self.connection.execute(
            """SELECT 1 FROM commands
               WHERE agent_id = ? AND kind = 'cancel'
                 AND state IN ('pending', 'claimed')
               ORDER BY id LIMIT 1""",
            (checked,),
        ).fetchone()
        return row is not None

    def bind_orchestrator(
        self,
        agent_id: str | AgentId,
        ref: OrchestratorRef,
        *,
        at: float | None = None,
    ) -> str:
        bound_at = timestamp(at)
        agent_id = validate_agent_id(agent_id)
        with immediate(self.connection):
            agent = agent_row(self.connection, agent_id)
            session_id = session_for_ref(self.connection, ref, bound_at)
            current = agent["orchestrator_session_id"]
            if current is not None and current != session_id:
                raise ValidationError("agent orchestration binding is immutable")
            if current is None:
                self.connection.execute(
                    "UPDATE agents SET orchestrator_session_id = ? WHERE id = ?",
                    (session_id, agent_id),
                )
                self.connection.execute(
                    """UPDATE deliveries
                       SET orchestrator_session_id = ?, state = 'pending',
                           next_attempt_at = ?
                       WHERE agent_id = ? AND state = 'waiting_binding'""",
                    (session_id, bound_at, agent_id),
                )
        return session_id

    def find_orchestrator_session(self, ref: OrchestratorRef) -> str | None:
        if not isinstance(ref, OrchestratorRef):
            raise ValidationError("orchestrator must be an OrchestratorRef")
        row = self.connection.execute(
            """SELECT id FROM orchestrator_sessions
               WHERE transport = ? AND external_session_id = ?""",
            (ref.transport, ref.external_session_id),
        ).fetchone()
        return None if row is None else str(row["id"])

    def record_context_receipt(
        self,
        orchestrator_session_id: str,
        context_key: str,
        *,
        at: float | None = None,
    ) -> bool:
        nonblank("orchestrator_session_id", orchestrator_session_id)
        nonblank("context_key", context_key)
        with immediate(self.connection):
            return _upsert_context_receipt(
                self.connection, orchestrator_session_id, context_key, timestamp(at)
            )

    def record_context_receipt_for_ref(
        self, ref: OrchestratorRef, context_key: str, *, at: float | None = None
    ) -> tuple[str, bool]:
        if not isinstance(ref, OrchestratorRef):
            raise ValidationError("orchestrator must be an OrchestratorRef")
        nonblank("context_key", context_key)
        injected_at = timestamp(at)
        with immediate(self.connection):
            session_id = session_for_ref(self.connection, ref, injected_at)
            return session_id, _upsert_context_receipt(
                self.connection, session_id, context_key, injected_at
            )

    def record_context_components_for_ref(
        self,
        ref: OrchestratorRef,
        components: dict[str, str],
        *,
        at: float | None = None,
    ) -> tuple[str, frozenset[str]]:
        """Find-or-create the ref's session, then atomically compare-and-store
        per-component context fingerprints.

        ``components`` maps nonblank names to nonblank fingerprint strings and
        must be a non-empty dict. Returns ``(session_id, changed_names)``:
        ``changed_names`` holds exactly the components whose stored
        fingerprint differed, and is empty when nothing was rewritten. The
        session lookup and the receipt compare/update share one immediate
        transaction, so concurrent callers can never interleave the read with
        the write; a missing or legacy receipt row reports every component as
        changed and is rewritten in place in the versioned encoding.
        """

        from .db import record_context_component_receipt

        if not isinstance(ref, OrchestratorRef):
            raise ValidationError("orchestrator must be an OrchestratorRef")
        if not isinstance(components, dict) or not components:
            raise ValidationError("components must be a non-empty mapping")
        checked: dict[str, str] = {}
        for name, value in components.items():
            nonblank("component name", name)
            nonblank(f"component {name}", value)
            checked[name] = value
        injected_at = timestamp(at)
        with immediate(self.connection):
            session_id = session_for_ref(self.connection, ref, injected_at)
            return session_id, record_context_component_receipt(
                self.connection, session_id, checked, injected_at
            )

    def get_agent(self, agent_id: str | AgentId) -> dict[str, object]:
        return dict(agent_row(self.connection, validate_agent_id(agent_id)))

    def list_agents(
        self,
        *,
        statuses: Iterable[AgentStatus] | None = None,
        orchestrator_session_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        integer("limit", limit, minimum=1)
        integer("offset", offset, minimum=0)
        if orchestrator_session_id is not None:
            nonblank("orchestrator_session_id", orchestrator_session_id)
        params: list[object] = []
        where = ""
        if statuses is not None:
            selected = tuple(statuses)
            if not all(isinstance(status, AgentStatus) for status in selected):
                raise ValidationError("statuses must contain AgentStatus values")
            values = tuple(status.value for status in selected)
            if not values:
                return []
            where = f"WHERE status IN ({','.join('?' for _ in values)})"
            params.extend(values)
        if orchestrator_session_id is not None:
            where += " AND " if where else "WHERE "
            where += "orchestrator_session_id = ?"
            params.append(orchestrator_session_id)
        params.extend((limit, offset))
        rows = self.connection.execute(
            f"""SELECT * FROM agents {where}
                ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?""",
            params,
        )
        return [dict(row) for row in rows]

    def active_count(self) -> int:
        values = tuple(status.value for status in ACTIVE)
        return count_agents(self.connection, values)

    def create_attempt(
        self,
        agent_id: str | AgentId,
        *,
        state: str,
        adapter_state: object = None,
        attempt_id: str | None = None,
        at: float | None = None,
    ) -> str:
        agent_id = validate_agent_id(agent_id)
        nonblank("attempt state", state)
        attempt_id = attempt_id or f"att_{uuid.uuid4().hex}"
        nonblank("attempt_id", attempt_id)
        created_at = timestamp(at)
        serialized = json_text({} if adapter_state is None else adapter_state)
        with immediate(self.connection):
            agent_row(self.connection, agent_id)
            number = int(
                self.connection.execute(
                    "SELECT COALESCE(MAX(number), 0) + 1 FROM attempts WHERE agent_id = ?",
                    (agent_id,),
                ).fetchone()[0]
            )
            self.connection.execute(
                """INSERT INTO attempts
                   (id, agent_id, number, state, adapter_state_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (attempt_id, agent_id, number, state, serialized, created_at),
            )
        return attempt_id

    def finish_attempt(
        self,
        agent_id: str | AgentId,
        attempt_id: str,
        *,
        state: str,
        at: float | None = None,
    ) -> None:
        agent_id = validate_agent_id(agent_id)
        nonblank("attempt state", state)
        with immediate(self.connection):
            updated = self.connection.execute(
                """UPDATE attempts SET state = ?, finished_at = ?
                   WHERE id = ? AND agent_id = ? AND finished_at IS NULL""",
                (state, timestamp(at), attempt_id, agent_id),
            ).rowcount
            if updated != 1:
                raise ValidationError("attempt is unknown, finished, or owned by another agent")

    def append_event(
        self,
        agent_id: str | AgentId,
        kind: str,
        *,
        data: object = None,
        attempt_id: str | None = None,
        at: float | None = None,
    ) -> int:
        agent_id = validate_agent_id(agent_id)
        nonblank("event kind", kind)
        with immediate(self.connection):
            agent_row(self.connection, agent_id)
            require_attempt(self.connection, agent_id, attempt_id)
            return insert_event(
                self.connection,
                agent_id,
                timestamp(at),
                kind,
                attempt_id=attempt_id,
                data=data,
            )

    def append_message(
        self,
        agent_id: str | AgentId,
        message: Message,
        *,
        attempt_id: str | None = None,
    ) -> int:
        agent_id = validate_agent_id(agent_id)
        if not isinstance(message, Message):
            raise ValidationError("message must be a Message")
        content, raw_ref = resolve_message_storage(
            message.content,
            message.raw_ref,
            agent_id=agent_id,
            home=connection_path(self.connection).parent,
        )
        with immediate(self.connection):
            agent_row(self.connection, agent_id)
            require_attempt(self.connection, agent_id, attempt_id)
            cursor = self.connection.execute(
                """INSERT INTO messages
                   (agent_id, attempt_id, at, role, name, content, raw_ref)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    agent_id,
                    attempt_id,
                    message.at,
                    message.role.value,
                    message.name,
                    content,
                    raw_ref,
                ),
            )
        return int(cursor.lastrowid)

    def transcript(
        self,
        agent_id: str | AgentId,
        *,
        after_seq: int = 0,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        agent_id = validate_agent_id(agent_id)
        integer("after_seq", after_seq, minimum=0)
        integer("limit", limit, minimum=1)
        agent_row(self.connection, agent_id)
        rows = message_rows(self.connection, agent_id, after_seq, limit)
        return [dict(row) for row in rows]

    def record_supervisor(
        self,
        agent_id: str | AgentId,
        *,
        pid: int,
        identity: str,
        process_group_id: int,
        at: float | None = None,
    ) -> None:
        agent_id = validate_agent_id(agent_id)
        if (
            isinstance(pid, bool)
            or not isinstance(pid, int)
            or pid <= 0
            or isinstance(process_group_id, bool)
            or not isinstance(process_group_id, int)
            or process_group_id <= 0
        ):
            raise ValidationError("process ids must be positive integers")
        nonblank("supervisor identity", identity)
        with immediate(self.connection):
            agent = agent_row(self.connection, agent_id)
            if AgentStatus(agent["status"]) in TERMINAL:
                raise StateTransitionError("terminal agent cannot bind a supervisor")
            fields = ("supervisor_pid", "supervisor_identity", "process_group_id")
            stored = tuple(agent[field] for field in fields)
            if any(value is not None for value in stored) and stored != (
                pid, identity, process_group_id
            ):
                # The pre-ready row records the detached supervisor's own group
                # (it is its own group leader), so refining that one value once to
                # the verified engine group is the only permitted rewrite.
                if stored != (pid, identity, pid) or process_group_id == pid:
                    raise ValidationError("supervisor identity is immutable")
            self.connection.execute(
                """UPDATE agents SET supervisor_pid = ?, supervisor_identity = ?,
                   process_group_id = ?, heartbeat_at = ? WHERE id = ?""",
                (pid, identity, process_group_id, timestamp(at), agent_id),
            )

    def transition(
        self,
        agent_id: str | AgentId,
        target: AgentStatus,
        *,
        outcome: Outcome | None = None,
        attempt_id: str | None = None,
        kind: str = "status",
        data: object = None,
        at: float | None = None,
    ) -> int:
        agent_id = validate_agent_id(agent_id)
        if target is AgentStatus.LOST:
            raise StateTransitionError("lost is committed only by reconciliation")
        if not isinstance(target, AgentStatus):
            raise ValidationError("target must be an AgentStatus")
        changed_at = timestamp(at)
        with immediate(self.connection):
            return self._transition(
                agent_id,
                target,
                changed_at,
                outcome=outcome,
                attempt_id=attempt_id,
                kind=kind,
                data=data,
            )

    def expire_unbound_deliveries(
        self, *, at: float | None = None,
        max_age_seconds: float = delivery.BINDING_WINDOW_SECONDS,
    ) -> list[str]:
        """Expire never-bound completion deliveries for terminal agents.

        Delegates to :func:`agent_run.state.delivery.expire_unbound_deliveries`
        and returns the ids it expired, oldest first.  The counterpart of the
        delivery insert in :meth:`_transition`: it retires the rows that insert
        can no longer produce and that no bind hook will ever attach to.
        """

        return delivery.expire_unbound_deliveries(
            self.connection, at=at, max_age_seconds=max_age_seconds
        )

    def _transition(
        self,
        agent_id: AgentId,
        target: AgentStatus,
        at: float,
        *,
        outcome: Outcome | None,
        attempt_id: str | None,
        kind: str,
        data: object,
    ) -> int:
        nonblank("event kind", kind)
        agent = agent_row(self.connection, agent_id)
        require_attempt(self.connection, agent_id, attempt_id)
        current = AgentStatus(agent["status"])
        validate_transition(current, target)
        if target in TERMINAL:
            outcome = outcome or Outcome(target)
            if not isinstance(outcome, Outcome) or outcome.status is not target:
                raise ValidationError("outcome status must match terminal target")
            self.connection.execute(
                """UPDATE agents SET status = ?, finished_at = ?, exit_code = ?,
                   failure_kind = ?, failure_text = ?, runtime_session_id = ?,
                   answer_path = ?, answer_bytes = ?, answer_sha256 = ? WHERE id = ?""",
                (
                    target.value,
                    at,
                    outcome.exit_code,
                    outcome.failure_kind,
                    outcome.failure_text,
                    outcome.runtime_session_id,
                    None if outcome.answer_path is None else str(outcome.answer_path),
                    outcome.answer_bytes,
                    outcome.answer_sha256,
                    agent_id,
                ),
            )
        else:
            started_at = at if target in {AgentStatus.STARTING, AgentStatus.RUNNING} else None
            self.connection.execute(
                """UPDATE agents SET status = ?, started_at = COALESCE(started_at, ?)
                   WHERE id = ?""",
                (target.value, started_at, agent_id),
            )
        event_seq = insert_event(
            self.connection,
            agent_id,
            at,
            kind,
            attempt_id=attempt_id,
            from_status=current.value,
            to_status=target.value,
            data=data,
        )
        if target in TERMINAL:
            # A completion notice can only reach a chat through the orchestrator
            # session that asked for it.  A start with no session reference never
            # fires a bind hook, so a waiting_binding row here would be rescanned
            # forever; such an agent is reported with delivery state not_created.
            session_id = agent["orchestrator_session_id"]
            if session_id is not None:
                self.connection.execute(
                    """INSERT INTO deliveries
                       (id, agent_id, orchestrator_session_id, terminal_event_seq,
                        state, next_attempt_at)
                       VALUES (?, ?, ?, ?, 'pending', ?)""",
                    (f"ntf_{uuid.uuid4().hex}", agent_id, session_id, event_seq, at),
                )
        return event_seq

    def reconcile(
        self,
        agent_id: str | AgentId,
        *,
        verdict: str,
        supervisor_pid: int | None = None,
        process_group_id: int | None = None,
        expected_identity: str | None = None,
        alive: bool | None = None,
        checked_at: float | None = None,
        observed_identity: str | None = None,
        reason: str | None = None,
    ) -> bool:
        agent_id = validate_agent_id(agent_id)
        if verdict not in {"alive", "dead", "identity_mismatch"}:
            raise ValidationError("invalid reconciliation verdict")
        with immediate(self.connection):
            agent = agent_row(self.connection, agent_id)
            changed_at, failure_kind = checked_supervisor_proof(
                agent,
                verdict=verdict,
                supervisor_pid=supervisor_pid,
                process_group_id=process_group_id,
                expected_identity=expected_identity,
                alive=alive,
                checked_at=checked_at,
                observed_identity=observed_identity,
            )
            if AgentStatus(agent["status"]) in TERMINAL:
                return False
            if failure_kind is None:
                return False
            self._transition(
                agent_id,
                AgentStatus.LOST,
                changed_at,
                outcome=Outcome(
                    AgentStatus.LOST,
                    failure_kind=failure_kind,
                    failure_text=reason,
                ),
                attempt_id=None,
                kind="reconciled_lost",
                data={"verdict": verdict, "observed_identity": observed_identity},
            )
        return True

    def reconcile_reaped(
        self,
        agent_id: str | AgentId,
        supervisor_pid: int,
        *,
        checked_at: float | None = None,
    ) -> bool:
        """Commit exact waitpid proof, including the pre-identity STARTING window."""

        checked = validate_agent_id(agent_id)
        integer("supervisor_pid", supervisor_pid, minimum=1)
        changed_at = timestamp(checked_at)
        with immediate(self.connection):
            agent = agent_row(self.connection, checked)
            if AgentStatus(str(agent["status"])) in TERMINAL:
                return False
            recorded_pid = agent["supervisor_pid"]
            if recorded_pid is not None and int(recorded_pid) != supervisor_pid:
                return False
            self._transition(
                checked,
                AgentStatus.LOST,
                changed_at,
                outcome=Outcome(
                    AgentStatus.LOST,
                    failure_kind="supervisor_dead",
                    failure_text="detached supervisor exited",
                ),
                attempt_id=None,
                kind="reconciled_lost",
                data={"verdict": "reaped", "supervisor_pid": supervisor_pid},
            )
        return True

    def enqueue_command(
        self,
        agent_id: str | AgentId,
        kind: str,
        payload: object,
        *,
        at: float | None = None,
    ) -> int:
        agent_id = validate_agent_id(agent_id)
        nonblank("command kind", kind)
        with immediate(self.connection):
            agent = agent_row(self.connection, agent_id)
            if AgentStatus(agent["status"]) in TERMINAL:
                raise StateTransitionError("terminal agent cannot receive commands")
            cursor = self.connection.execute(
                """INSERT INTO commands
                   (agent_id, kind, payload_json, state, created_at)
                   VALUES (?, ?, ?, 'pending', ?)""",
                (agent_id, kind, json_text(payload), timestamp(at)),
            )
        return int(cursor.lastrowid)

    def claim_command(
        self, agent_id: str | AgentId, *, at: float | None = None
    ) -> dict[str, object] | None:
        agent_id = validate_agent_id(agent_id)
        claimed_at = timestamp(at)
        with immediate(self.connection):
            agent_row(self.connection, agent_id)
            row = self.connection.execute(
                """SELECT * FROM commands
                   WHERE agent_id = ? AND state = 'pending' ORDER BY id LIMIT 1""",
                (agent_id,),
            ).fetchone()
            if row is None:
                return None
            updated = self.connection.execute(
                """UPDATE commands SET state = 'claimed', claimed_at = ?
                   WHERE id = ? AND agent_id = ? AND state = 'pending'""",
                (claimed_at, row["id"], agent_id),
            ).rowcount
            if updated != 1:
                return None
            row = self.connection.execute(
                "SELECT * FROM commands WHERE id = ?", (row["id"],)
            ).fetchone()
        return row_dict(row)

    def complete_command(
        self,
        command_id: int,
        agent_id: str | AgentId,
        result: object,
        *,
        at: float | None = None,
    ) -> None:
        agent_id = validate_agent_id(agent_id)
        with immediate(self.connection):
            updated = self.connection.execute(
                """UPDATE commands SET state = 'completed', completed_at = ?, result_json = ?
                   WHERE id = ? AND agent_id = ? AND state = 'claimed'""",
                (timestamp(at), json_text(result), command_id, agent_id),
            ).rowcount
            if updated != 1:
                raise ValidationError("command is unclaimed or owned by another agent")

    def insert_capacity_sample(
        self,
        *,
        runtime: str,
        lane: str,
        window: str,
        source: str,
        payload: object,
        target: str | None = None,
        remaining_percent: float | None = None,
        reset_at: float | None = None,
        observed_at: float | None = None,
        valid_until: float | None = None,
    ) -> int:
        for name, value in (
            ("runtime", runtime),
            ("lane", lane),
            ("window", window),
            ("source", source),
        ):
            nonblank(name, value)
        if remaining_percent is not None and (
            isinstance(remaining_percent, bool)
            or not isinstance(remaining_percent, (int, float))
            or not math.isfinite(remaining_percent)
            or not 0 <= remaining_percent <= 100
        ):
            raise ValidationError("remaining_percent must be between 0 and 100")
        return insert_capacity_row(
            self.connection,
            runtime=runtime,
            lane=lane,
            window=window,
            target=target,
            source=source,
            remaining_percent=remaining_percent,
            reset_at=None if reset_at is None else timestamp(reset_at),
            observed_at=None if observed_at is None else timestamp(observed_at),
            valid_until=None if valid_until is None else timestamp(valid_until),
            payload_json=json_text(payload),
        )

    def recent_capacity_samples(
        self,
        *,
        at: float | None = None,
        runtime: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        integer("limit", limit, minimum=1)
        now = timestamp(at)
        if runtime is not None:
            nonblank("runtime", runtime)
        rows = recent_capacity_rows(self.connection, now, runtime, limit)
        return [dict(row) for row in rows]

    def prune_capacity_samples(self, retention: int) -> int:
        return capacity.prune_capacity_samples(self.connection, retention)

    def capacity_sample_history(
        self, *, retention: int, runtime: str | None = None
    ) -> list[dict[str, object]]:
        return [
            dict(row)
            for row in capacity.capacity_sample_history(
                self.connection, retention=retention, runtime=runtime
            )
        ]

    def append_capacity_samples(
        self,
        samples: Iterable[dict[str, object]],
        *,
        runtime: str,
        scope_id: str,
        observed_at: float,
        valid_until: float,
        payload: object,
    ) -> None:
        """Atomically persist samples and one route topology snapshot.

        ``samples`` is consumed once; each mapping must belong to ``runtime``.
        ``scope_id`` must be nonblank, timestamps must be finite and ordered
        with expiry no earlier than observation, and the JSON ``payload`` is
        limited to 65,536 UTF-8 bytes. The store commits all rows and the
        snapshot together or leaves both unchanged. Validation errors and
        SQLite failures are propagated according to the StateStore contract.
        """

        capacity.append_capacity_samples(
            self.connection, samples, runtime=runtime, scope_id=scope_id,
            observed_at=observed_at, valid_until=valid_until, payload=payload,
        )

    def capacity_route_snapshots(
        self, *, runtime: str | None = None
    ) -> list[dict[str, object]]:
        """Return route topology snapshots in deterministic key order.

        An omitted runtime returns all snapshots; a supplied runtime filters
        the result. The returned dictionaries are detached copies ordered by
        ``(runtime, scope_id)`` and this read does not mutate the store.
        """

        return [dict(row) for row in capacity.capacity_route_snapshots(
            self.connection, runtime=runtime
        )]

    def claim_delivery(
        self, owner: str, *, at: float | None = None, lease_seconds: float = 30
    ) -> dict[str, object] | None:
        return delivery.claim_delivery(self.connection, owner, at=at, lease_seconds=lease_seconds)

    def complete_delivery(
        self, delivery_id: str, owner: str, *,
        remote_message_id: str | None = None,
        ambiguous_result: bool = False,
        evidence: DeliveryAttemptEvidence | None = None,
        at: float | None = None,
    ) -> None:
        """Complete an owned claim and atomically record optional evidence."""

        delivery.complete_delivery(
            self.connection, delivery_id, owner,
            remote_message_id=remote_message_id,
            ambiguous_result=ambiguous_result,
            evidence_json=_delivery_evidence_json(evidence), at=at,
        )

    def fail_delivery(
        self, delivery_id: str, owner: str, error: str, *,
        at: float | None = None, ambiguous_result: bool = False,
        evidence: DeliveryAttemptEvidence | None = None,
    ) -> None:
        """Fail an owned claim and atomically record optional evidence."""

        delivery.fail_delivery(
            self.connection, delivery_id, owner, error,
            at=at, ambiguous_result=ambiguous_result,
            evidence_json=_delivery_evidence_json(evidence),
        )

    def retry_delivery(
        self, delivery_id: str, owner: str, error: str, *,
        at: float | None = None, ambiguous_result: bool = False,
        evidence: DeliveryAttemptEvidence | None = None,
        base_delay: float = 1, max_delay: float = 300,
    ) -> float:
        """Schedule an owned retry and atomically record optional evidence."""

        return delivery.retry_delivery(
            self.connection, delivery_id, owner, error,
            at=at, ambiguous_result=ambiguous_result,
            evidence_json=_delivery_evidence_json(evidence),
            base_delay=base_delay, max_delay=max_delay,
        )

    def latest_delivery_attempt(
        self, delivery_id: str
    ) -> DeliveryAttemptEvidence | None:
        """Return the latest validated evidence for ``delivery_id``, if any."""

        nonblank("delivery_id", delivery_id)
        row = self.connection.execute(
            """SELECT evidence_json FROM delivery_attempt_evidence
               WHERE delivery_id = ? ORDER BY attempt DESC LIMIT 1""",
            (delivery_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(str(row["evidence_json"]))
        except (TypeError, ValueError) as error:
            raise ValidationError("invalid stored delivery attempt evidence") from error
        from agent_run.delivery.base import DeliveryAttemptEvidence

        return DeliveryAttemptEvidence.from_payload(payload)

    def cancel_delivery(self, delivery_id: str) -> bool:
        return delivery.cancel_delivery(self.connection, delivery_id)

    def claim_workflow_delivery(
        self, owner: str, *, at: float | None = None, lease_seconds: float = 30
    ) -> dict[str, object] | None:
        """Claim one due workflow notice for an owner until its lease expires."""

        nonblank("lease owner", owner)
        now = timestamp(at)
        with immediate(self.connection):
            row = self.connection.execute(
                """SELECT wd.*, wr.status AS run_status, os.transport,
                          os.external_session_id, os.external_turn_id
                   FROM workflow_deliveries wd
                   JOIN workflow_runs wr ON wr.id = wd.run_id
                   JOIN orchestrator_sessions os ON os.id = wd.orchestrator_session_id
                   WHERE wd.state IN ('pending', 'retry_wait')
                     AND wd.next_attempt_at <= ?
                     AND (wd.lease_until IS NULL OR wd.lease_until <= ?)
                   ORDER BY wd.next_attempt_at, wd.id LIMIT 1""",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            updated = self.connection.execute(
                """UPDATE workflow_deliveries
                   SET state = 'sending', lease_owner = ?, lease_until = ?,
                       attempts = attempts + 1
                   WHERE id = ? AND state IN ('pending', 'retry_wait')""",
                (owner, now + lease_seconds, row["id"]),
            ).rowcount
            if updated != 1:
                return None
            row = self.connection.execute(
                """SELECT wd.*, wr.status AS run_status, os.transport,
                          os.external_session_id, os.external_turn_id
                   FROM workflow_deliveries wd
                   JOIN workflow_runs wr ON wr.id = wd.run_id
                   JOIN orchestrator_sessions os ON os.id = wd.orchestrator_session_id
                   WHERE wd.id = ?""",
                (row["id"],),
            ).fetchone()
        return dict(row)

    def complete_workflow_delivery(
        self, delivery_id: str, owner: str, *, remote_message_id: str | None = None,
        at: float | None = None,
    ) -> None:
        """Complete a workflow delivery only while its lease belongs to the caller."""

        with immediate(self.connection):
            updated = self.connection.execute(
                """UPDATE workflow_deliveries SET state = 'delivered', lease_owner = NULL,
                       lease_until = NULL, remote_message_id = ?
                   WHERE id = ? AND state = 'sending' AND lease_owner = ?""",
                (remote_message_id, delivery_id, owner),
            ).rowcount
            if updated != 1:
                raise ValidationError("workflow delivery lease is not owned by caller")

    def retry_workflow_delivery(
        self, delivery_id: str, owner: str, error: str, *, at: float | None = None,
        base_delay: float = 1, max_delay: float = 300,
    ) -> float:
        """Release an owned workflow notice claim with capped exponential delay."""

        now = timestamp(at)
        with immediate(self.connection):
            row = self.connection.execute(
                "SELECT attempts FROM workflow_deliveries WHERE id = ? AND state = 'sending' AND lease_owner = ?",
                (delivery_id, owner),
            ).fetchone()
            if row is None:
                raise ValidationError("workflow delivery lease is not owned by caller")
            delay = min(max_delay, base_delay * (2 ** min(int(row["attempts"]) - 1, 20)))
            next_attempt = now + delay
            self.connection.execute(
                """UPDATE workflow_deliveries SET state = 'retry_wait', lease_owner = NULL,
                       lease_until = NULL, last_error = ?, next_attempt_at = ? WHERE id = ?""",
                (error, next_attempt, delivery_id),
            )
        return next_attempt

    def fail_workflow_delivery(
        self, delivery_id: str, owner: str, error: str, *, at: float | None = None
    ) -> None:
        """Permanently fail a workflow notice whose lease belongs to the caller."""

        with immediate(self.connection):
            updated = self.connection.execute(
                """UPDATE workflow_deliveries SET state = 'failed', lease_owner = NULL,
                       lease_until = NULL, last_error = ?
                   WHERE id = ? AND state = 'sending' AND lease_owner = ?""",
                (error, delivery_id, owner),
            ).rowcount
            if updated != 1:
                raise ValidationError("workflow delivery lease is not owned by caller")

    def create_workflow_run(
        self,
        name: str,
        script_sha: str,
        *,
        owner_identity: str | None = None,
        run_id: str | None = None,
        at: float | None = None,
        plan: object = None,
        orchestrator: object = None,
    ) -> str:
        return workflow.create_workflow_run(
            self.connection,
            name,
            script_sha,
            owner_identity=owner_identity,
            run_id=run_id,
            at=at,
            plan=plan,
            orchestrator=orchestrator,
        )

    def start_workflow_run(self, run_id: str) -> None:
        workflow.start_workflow_run(self.connection, run_id)

    def claim_workflow_run(self, run_id: str, owner_identity: str) -> None:
        workflow.claim_workflow_run(self.connection, run_id, owner_identity)

    def resume_workflow_run(self, run_id: str, owner_identity: str) -> None:
        workflow.resume_workflow_run(self.connection, run_id, owner_identity)

    def finish_workflow_run(
        self, run_id: str, status: str, *, result: object = None, at: float | None = None
    ) -> None:
        """Persist a terminal workflow status and optional JSON-safe result."""

        workflow.finish_workflow_run(
            self.connection, run_id, status, result=result, at=at
        )

    def record_step_start(
        self,
        run_id: str,
        step_key: str,
        spec: object,
        *,
        agent_id: str | None = None,
    ) -> None:
        workflow.record_step_start(
            self.connection, run_id, step_key, spec, agent_id=agent_id
        )

    def finish_step(
        self,
        run_id: str,
        step_key: str,
        status: str,
        *,
        result: object = None,
        failure_kind: str | None = None,
        failure_params: object = None,
    ) -> None:
        workflow.finish_step(
            self.connection,
            run_id,
            step_key,
            status,
            result=result,
            failure_kind=failure_kind,
            failure_params=failure_params,
        )

    def cached_step_result(self, run_id: str, step_key: str) -> object | None:
        return workflow.cached_step_result(self.connection, run_id, step_key)

    def workflow_run_status(
        self, run_id: str, *, step_limit: int = 100
    ) -> dict[str, object]:
        return workflow.workflow_run_status(self.connection, run_id, step_limit=step_limit)

    def list_workflow_runs(
        self, *, active_only: bool = False, limit: int = 100, offset: int = 0
    ) -> list[dict[str, object]]:
        return workflow.list_workflow_runs(
            self.connection, active_only=active_only, limit=limit, offset=offset
        )
