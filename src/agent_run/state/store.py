"""Guarded durable state operations for agent-run."""

from __future__ import annotations

import math
import sqlite3
import uuid
from pathlib import Path
from typing import Iterable

from agent_run.domain import (
    ACTIVE,
    TERMINAL,
    AgentId,
    AgentStatus,
    Message,
    OrchestratorRef,
    Outcome,
    StartRequest,
    new_agent_id,
    validate_agent_id,
    validate_transition,
)
from agent_run.errors import StateTransitionError, ValidationError

from .db import (
    agent_row,
    checked_supervisor_proof,
    claim_delivery_row,
    count_agents,
    finish_delivery_claim,
    idempotent_agent,
    immediate,
    integer,
    insert_agent_row,
    insert_capacity_row,
    initialize_database,
    insert_event,
    json_text,
    message_rows,
    nonblank,
    open_database,
    owned_delivery_attempts,
    positive_number,
    request_json,
    recent_capacity_rows,
    require_attempt,
    row_dict,
    session_for_ref,
    timestamp,
    validate_message_storage,
)


class StateStore:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.connection.row_factory = sqlite3.Row

    @classmethod
    def initialize(cls, database: str | Path) -> StateStore:
        return cls(initialize_database(database))

    @classmethod
    def open(cls, database: str | Path) -> StateStore:
        return cls(open_database(database))

    def close(self) -> None:
        self.connection.close()

    def create_agent(
        self,
        request: StartRequest,
        *,
        task_summary: str,
        config_revision: str,
        agent_id: str | AgentId | None = None,
        at: float | None = None,
    ) -> AgentId:
        """Insert an agent and its initial event, or return its idempotent peer."""

        if not isinstance(request, StartRequest):
            raise ValidationError("request must be a StartRequest")
        nonblank("task_summary", task_summary)
        nonblank("config_revision", config_revision)
        created_at = timestamp(at)
        candidate = new_agent_id() if agent_id is None else validate_agent_id(agent_id)
        serialized = request_json(request)
        with immediate(self.connection):
            session_id = (
                None
                if request.orchestrator is None
                else session_for_ref(self.connection, request.orchestrator, created_at)
            )
            if session_id is not None and request.request_id is not None:
                existing = idempotent_agent(
                    self.connection, session_id, request.request_id
                )
                if existing is not None:
                    if (
                        existing["request_json"] != serialized
                        or existing["task_summary"] != task_summary
                        or existing["config_revision"] != config_revision
                    ):
                        raise ValidationError("request_id was reused for a different request")
                    return AgentId(str(existing["id"]))
            insert_agent_row(
                self.connection,
                candidate,
                request,
                session_id,
                task_summary,
                serialized,
                config_revision,
                created_at,
            )
            insert_event(
                self.connection,
                candidate,
                created_at,
                "created",
                to_status=AgentStatus.CREATED.value,
            )
        return candidate

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

    def get_agent(self, agent_id: str | AgentId) -> dict[str, object]:
        return dict(agent_row(self.connection, validate_agent_id(agent_id)))

    def list_agents(
        self,
        *,
        statuses: Iterable[AgentStatus] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        integer("limit", limit, minimum=1)
        integer("offset", offset, minimum=0)
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
        validate_message_storage(message.content, message.raw_ref)
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
                    message.content,
                    message.raw_ref,
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
            session_id = agent["orchestrator_session_id"]
            state = "pending" if session_id is not None else "waiting_binding"
            self.connection.execute(
                """INSERT INTO deliveries
                   (id, agent_id, orchestrator_session_id, terminal_event_seq,
                    state, next_attempt_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    f"ntf_{uuid.uuid4().hex}",
                    agent_id,
                    session_id,
                    event_seq,
                    state,
                    at if session_id is not None else None,
                ),
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

    def claim_delivery(
        self,
        owner: str,
        *,
        at: float | None = None,
        lease_seconds: float = 30,
    ) -> dict[str, object] | None:
        nonblank("lease owner", owner)
        now = timestamp(at)
        lease_seconds = positive_number("lease_seconds", lease_seconds)
        with immediate(self.connection):
            row = claim_delivery_row(
                self.connection, owner, now, now + lease_seconds
            )
        return row_dict(row)

    def complete_delivery(
        self,
        delivery_id: str,
        owner: str,
        *,
        remote_message_id: str | None = None,
        ambiguous_result: bool = False,
        at: float | None = None,
    ) -> None:
        nonblank("delivery_id", delivery_id)
        nonblank("lease owner", owner)
        now = timestamp(at)
        with immediate(self.connection):
            if not finish_delivery_claim(
                self.connection,
                delivery_id,
                owner,
                "delivered",
                now=now,
                remote_message_id=remote_message_id,
                ambiguous_result=ambiguous_result,
            ):
                raise ValidationError("delivery lease is not owned by caller")

    def retry_delivery(
        self,
        delivery_id: str,
        owner: str,
        error: str,
        *,
        at: float | None = None,
        ambiguous_result: bool = False,
        base_delay: float = 1,
        max_delay: float = 300,
    ) -> float:
        nonblank("delivery_id", delivery_id)
        nonblank("lease owner", owner)
        nonblank("delivery error", error)
        base_delay = positive_number("base_delay", base_delay)
        max_delay = positive_number("max_delay", max_delay)
        now = timestamp(at)
        with immediate(self.connection):
            attempts = owned_delivery_attempts(self.connection, delivery_id, owner, now)
            if attempts is None:
                raise ValidationError("delivery lease is not owned by caller")
            delay = min(max_delay, base_delay * (2 ** min(attempts - 1, 20)))
            next_attempt_at = now + delay
            finish_delivery_claim(
                self.connection,
                delivery_id,
                owner,
                "retry_wait",
                now=now,
                last_error=error,
                ambiguous_result=ambiguous_result,
                next_attempt_at=next_attempt_at,
            )
        return next_attempt_at

    def cancel_delivery(self, delivery_id: str) -> bool:
        nonblank("delivery_id", delivery_id)
        with immediate(self.connection):
            updated = self.connection.execute(
                """UPDATE deliveries SET state = 'cancelled', lease_owner = NULL,
                   lease_until = NULL, next_attempt_at = NULL
                   WHERE id = ? AND state NOT IN ('delivered', 'cancelled')""",
                (delivery_id,),
            ).rowcount
        return updated == 1
