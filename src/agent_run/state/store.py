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

from . import capacity, delivery
from .db import (
    _upsert_context_receipt,
    agent_row,
    checked_supervisor_proof,
    connection_path,
    count_agents,
    immediate,
    integer,
    initialize_database,
    insert_event,
    json_text,
    message_rows,
    nonblank,
    open_database,
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
        """Open and return an existing migrated state database."""

        store = cls(open_database(database))
        _logger.debug("db=%s open", database)
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
        parent_agent_id: str | AgentId | None = None,
        identity_json: str | None = None,
        startup_owner_identity: str | None = None,
        startup_owner_birth_time: float | None = None,
        startup_deadline_seconds: float | None = None,
    ) -> AgentCreation:
        """Admit one capped agent. See :func:`agent_run.state.start.create_agent`.

        ``parent_agent_id`` is the agent this start resumes, or ``None`` for a
        fresh run; it is validated and claimed inside the same transaction.
        ``identity_json`` is the effective-identity snapshot for this run.
        Supplying startup owner identity, optional birth proof, and a finite
        deadline atomically persists service admission as ``STARTING``.
        Omitting all three retains low-level ``CREATED`` fixture behavior.
        """

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
            parent_agent_id=parent_agent_id,
            identity_json=identity_json,
            startup_owner_identity=startup_owner_identity,
            startup_owner_birth_time=startup_owner_birth_time,
            startup_deadline_seconds=startup_deadline_seconds,
        )

    def resume_chain(
        self,
        agent_id: str | AgentId,
        *,
        cursor: int = 1,
        limit: int = 50,
    ) -> list[sqlite3.Row]:
        """Return one resume chain's rows in chronological (``sequence``) order.

        ``agent_id`` may name any member of the chain; its ``root_agent_id``
        selects the whole chain. ``cursor`` is the 1-based ``sequence`` to
        start at and ``limit`` the maximum number of rows returned. Returns
        ``limit + 1`` rows at most, so the caller can detect a further page
        without a second count query; an unknown agent raises
        :class:`agent_run.errors.NotFoundError` via :meth:`get_agent`.
        """

        root = self.get_agent(agent_id)["root_agent_id"]
        return list(
            self.connection.execute(
                """SELECT * FROM agents
                   WHERE root_agent_id = ? AND sequence >= ?
                   ORDER BY sequence LIMIT ?""",
                (root, cursor, limit + 1),
            )
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

    def list_orchestrator_sessions(self, *, limit: int) -> list[dict[str, object]]:
        """Return the most active bound sessions and one aggregate unbound row.

        ``limit`` is a positive row bound.  Each returned mapping contains the
        session metadata, active and total agent counts, and ``page_total`` for
        the exact number of available rows before the bound; the synthetic
        unbound mapping has a null ``id`` and is omitted when no agents are
        unbound.  The read does not mutate state.
        """

        integer("limit", limit, minimum=1)
        active = tuple(status.value for status in ACTIVE)
        placeholders = ",".join("?" for _ in active)
        rows = self.connection.execute(
            f"""WITH bound AS (
                    SELECT sessions.id, sessions.transport,
                           sessions.external_session_id, sessions.external_turn_id,
                           sessions.created_at, sessions.last_seen_at,
                           SUM(CASE WHEN agents.status IN ({placeholders})
                                    THEN 1 ELSE 0 END) AS active,
                           COUNT(agents.id) AS total
                    FROM orchestrator_sessions AS sessions
                    JOIN agents ON agents.orchestrator_session_id = sessions.id
                    GROUP BY sessions.id
                ), unbound AS (
                    SELECT NULL AS id, '' AS transport, '' AS external_session_id,
                           NULL AS external_turn_id, MIN(created_at) AS created_at,
                           MAX(created_at) AS last_seen_at,
                           SUM(CASE WHEN status IN ({placeholders})
                                    THEN 1 ELSE 0 END) AS active,
                           COUNT(*) AS total
                    FROM agents
                    WHERE orchestrator_session_id IS NULL
                    HAVING COUNT(*) > 0
                ), all_sessions AS (
                    SELECT * FROM bound UNION ALL SELECT * FROM unbound
                )
                SELECT *, COUNT(*) OVER () AS page_total
                FROM all_sessions
                ORDER BY active DESC, last_seen_at DESC
                LIMIT ?""",
            (*active, *active, limit),
        )
        return [dict(row) for row in rows]

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

    def events_revision(self) -> int:
        """Return the global committed event sequence, or zero for an empty store."""

        row = self.connection.execute(
            "SELECT COALESCE(MAX(seq), 0) AS revision FROM events"
        ).fetchone()
        return int(row["revision"])

    def agent_projection(
        self, agent_ids: Iterable[str | AgentId]
    ) -> dict[str, dict[str, object]]:
        """Return batched read metadata for the exact distinct ``agent_ids``.

        Progress time, deadline warning, latest delivery and attempt evidence,
        and latest process-cleanup event are resolved in one SQL statement.
        Input order is irrelevant; unknown IDs are omitted, duplicates collapse,
        and an empty iterable performs no query. Stored JSON remains raw for the
        service boundary to validate into its public typed views.
        """

        selected = tuple(dict.fromkeys(str(validate_agent_id(value)) for value in agent_ids))
        if not selected:
            return {}
        values = ",".join("(?)" for _ in selected)
        rows = self.connection.execute(
            f"""WITH selected(id) AS (VALUES {values}),
                progress AS (
                    SELECT agent_id, MAX(at) AS last_progress_at
                    FROM messages WHERE agent_id IN (SELECT id FROM selected)
                    GROUP BY agent_id
                ), warnings AS (
                    SELECT DISTINCT agent_id, 1 AS deadline_warned
                    FROM events WHERE agent_id IN (SELECT id FROM selected)
                      AND kind = 'deadline_warning'
                ), delivery_seq AS (
                    SELECT agent_id, MAX(terminal_event_seq) AS terminal_event_seq
                    FROM deliveries WHERE agent_id IN (SELECT id FROM selected)
                    GROUP BY agent_id
                ), latest_delivery AS (
                    SELECT deliveries.* FROM deliveries
                    JOIN delivery_seq USING (agent_id, terminal_event_seq)
                ), evidence_attempt AS (
                    SELECT delivery_id, MAX(attempt) AS attempt
                    FROM delivery_attempt_evidence
                    WHERE delivery_id IN (SELECT id FROM latest_delivery)
                    GROUP BY delivery_id
                ), latest_evidence AS (
                    SELECT evidence.delivery_id, evidence.evidence_json
                    FROM delivery_attempt_evidence AS evidence
                    JOIN evidence_attempt USING (delivery_id, attempt)
                ), cleanup_seq AS (
                    SELECT agent_id, MAX(seq) AS seq
                    FROM events WHERE agent_id IN (SELECT id FROM selected)
                      AND kind = 'process_cleanup'
                    GROUP BY agent_id
                ), latest_cleanup AS (
                    SELECT events.agent_id, events.data_json AS cleanup_json
                    FROM events JOIN cleanup_seq USING (agent_id, seq)
                ), phase_seq AS (
                    SELECT agent_id, MAX(seq) AS seq
                    FROM events WHERE agent_id IN (SELECT id FROM selected)
                      AND kind = 'phase'
                    GROUP BY agent_id
                ), latest_phase AS (
                    SELECT events.agent_id, events.at AS phase_started_at,
                           events.data_json AS phase_json
                    FROM events JOIN phase_seq USING (agent_id, seq)
                )
                SELECT selected.id, progress.last_progress_at,
                       COALESCE(warnings.deadline_warned, 0) AS deadline_warned,
                       latest_delivery.id AS delivery_id,
                       latest_delivery.state AS delivery_state,
                       latest_delivery.attempts AS delivery_attempts,
                       latest_delivery.ambiguous_result AS delivery_ambiguous,
                       latest_delivery.last_error AS delivery_last_error,
                       latest_evidence.evidence_json,
                       latest_cleanup.cleanup_json,
                       latest_phase.phase_started_at,
                       latest_phase.phase_json
                FROM selected
                LEFT JOIN progress ON progress.agent_id = selected.id
                LEFT JOIN warnings ON warnings.agent_id = selected.id
                LEFT JOIN latest_delivery ON latest_delivery.agent_id = selected.id
                LEFT JOIN latest_evidence
                  ON latest_evidence.delivery_id = latest_delivery.id
                LEFT JOIN latest_cleanup ON latest_cleanup.agent_id = selected.id
                LEFT JOIN latest_phase ON latest_phase.agent_id = selected.id""",
            selected,
        )
        return {str(row["id"]): dict(row) for row in rows}

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
        birth_time: float | None = None,
        at: float | None = None,
    ) -> None:
        """Record the detached supervisor's immutable ownership proof.

        A ``STARTING`` agent may bind whenever its supervisor reaches READY;
        elapsed wall time does not invalidate ownership. Terminal rows and
        conflicting identities remain rejected. ``birth_time`` is optional only
        for legacy callers; once present it is immutable across later refinements.
        """

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
        if birth_time is not None and (
            isinstance(birth_time, bool)
            or not isinstance(birth_time, (int, float))
            or not math.isfinite(birth_time)
            or birth_time < 0
        ):
            raise ValidationError("supervisor birth time must be finite and nonnegative")
        nonblank("supervisor identity", identity)
        with immediate(self.connection):
            agent = agent_row(self.connection, agent_id)
            if AgentStatus(agent["status"]) in TERMINAL:
                raise StateTransitionError("terminal agent cannot bind a supervisor")
            fields = (
                "supervisor_pid",
                "supervisor_identity",
                "process_group_id",
                "supervisor_birth_time",
            )
            stored = tuple(agent[field] for field in fields)
            if any(value is not None for value in stored) and stored != (
                pid, identity, process_group_id, birth_time
            ):
                # The pre-ready row records the detached supervisor's own group
                # (it is its own group leader), so refining that one value once to
                # the verified engine group is the only permitted rewrite.
                if stored != (pid, identity, pid, birth_time) or process_group_id == pid:
                    raise ValidationError("supervisor identity is immutable")
            self.connection.execute(
                """UPDATE agents SET supervisor_pid = ?, supervisor_identity = ?,
                   process_group_id = ?, supervisor_birth_time = ?, heartbeat_at = ? WHERE id = ?""",
                (pid, identity, process_group_id, birth_time, timestamp(at), agent_id),
            )

    def claim_startup(
        self,
        agent_id: str | AgentId,
        owner_identity: str,
        *,
        owner_birth_time: float | None = None,
        at: float | None = None,
        deadline_seconds: float = 120.0,
    ) -> None:
        """Durably bind one accepted ``STARTING`` row to its live coordinator owner.

        ``owner_identity`` is a nonblank ``"<pid> <command>"`` diagnostic and
        ``owner_birth_time`` is the optional process-creation proof. The binding
        is immutable and is valid only until ``deadline_seconds`` after ``at``.
        A terminal row cannot be claimed. The fixed deadline prevents a live but
        wedged broker from exempting an abandoned start forever.
        """

        checked = validate_agent_id(agent_id)
        nonblank("startup owner identity", owner_identity)
        if owner_birth_time is not None and (
            isinstance(owner_birth_time, bool)
            or not isinstance(owner_birth_time, (int, float))
            or not math.isfinite(owner_birth_time)
            or owner_birth_time < 0
        ):
            raise ValidationError("startup owner birth time must be finite and nonnegative")
        if (
            isinstance(deadline_seconds, bool)
            or not isinstance(deadline_seconds, (int, float))
            or not math.isfinite(deadline_seconds)
            or deadline_seconds <= 0
        ):
            raise ValidationError("startup deadline must be positive and finite")
        claimed_at = timestamp(at)
        with immediate(self.connection):
            agent = agent_row(self.connection, checked)
            if AgentStatus(agent["status"]) in TERMINAL:
                raise StateTransitionError("terminal agent cannot claim startup")
            if AgentStatus(agent["status"]) is not AgentStatus.STARTING:
                raise StateTransitionError("startup owner requires starting agent")
            current = agent["startup_owner_pid_identity"]
            current_birth = agent["startup_owner_birth_time"]
            if current is not None and (
                current != owner_identity or current_birth != owner_birth_time
            ):
                raise StateTransitionError("startup is already owned")
            if current is None:
                self.connection.execute(
                    """UPDATE agents SET startup_owner_pid_identity = ?,
                       startup_owner_birth_time = ?, startup_deadline_at = ? WHERE id = ?""",
                    (
                        owner_identity,
                        owner_birth_time,
                        claimed_at + float(deadline_seconds),
                        checked,
                    ),
                )

    def begin_supervisor_handoff(
        self,
        agent_id: str | AgentId,
        owner_identity: str,
        *,
        at: float | None = None,
        deadline_seconds: float,
    ) -> bool:
        """Atomically transfer live preparation into a bounded spawn handoff.

        ``owner_identity`` must match the immutable coordinator claim and its
        preparation deadline must still be live. Returning ``True`` extends the
        deadline by ``deadline_seconds`` from ``at`` so READY failure cleanup
        finishes before reconciliation may release capacity. The coordinator
        calls this once immediately before spawn; a repeated matching call can
        renew the deadline and must not be used as an unbounded retry. Terminal,
        non-starting, expired, conflicting, or supervisor-owned rows return
        ``False`` without mutation.
        """

        checked = validate_agent_id(agent_id)
        nonblank("startup owner identity", owner_identity)
        if (
            isinstance(deadline_seconds, bool)
            or not isinstance(deadline_seconds, (int, float))
            or not math.isfinite(deadline_seconds)
            or deadline_seconds <= 0
        ):
            raise ValidationError("startup deadline must be positive and finite")
        handoff_at = timestamp(at)
        with immediate(self.connection):
            row = agent_row(self.connection, checked)
            deadline = row["startup_deadline_at"]
            if (
                row["status"] != AgentStatus.STARTING.value
                or row["startup_owner_pid_identity"] != owner_identity
                or not isinstance(deadline, (int, float))
                or deadline <= handoff_at
                or any(
                    row[field] is not None
                    for field in (
                        "supervisor_pid",
                        "process_group_id",
                        "supervisor_identity",
                    )
                )
            ):
                return False
            self.connection.execute(
                "UPDATE agents SET startup_deadline_at = ? WHERE id = ?",
                (handoff_at + float(deadline_seconds), checked),
            )
            return True

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
            if target in {AgentStatus.SUCCEEDED, AgentStatus.TIMED_OUT}:
                current = AgentStatus(agent_row(self.connection, agent_id)["status"])
                pending_cancel = self.connection.execute(
                    """SELECT id FROM commands
                       WHERE agent_id = ? AND kind = 'cancel' AND state = 'pending'
                       ORDER BY id LIMIT 1""",
                    (agent_id,),
                ).fetchone()
                if current is AgentStatus.RUNNING and pending_cancel is not None:
                    self._transition(
                        agent_id,
                        AgentStatus.CANCELLING,
                        changed_at,
                        outcome=None,
                        attempt_id=attempt_id,
                        kind="cancelling",
                        data={"source": "pending_cancel"},
                    )
                    original = outcome or Outcome(target)
                    target = AgentStatus.CANCELLED
                    outcome = Outcome(
                        target,
                        exit_code=original.exit_code,
                        failure_kind=original.failure_kind,
                        failure_text=original.failure_text,
                        runtime_session_id=original.runtime_session_id,
                        answer_path=original.answer_path,
                        answer_bytes=original.answer_bytes,
                        answer_sha256=original.answer_sha256,
                    )
                    self.connection.execute(
                        """UPDATE commands
                           SET state = 'completed', claimed_at = ?,
                               completed_at = ?, result_json = ?
                           WHERE id = ? AND state = 'pending'""",
                        (
                            changed_at,
                            changed_at,
                            json_text(
                                {
                                    "accepted": True,
                                    "reason": "terminal_cancel",
                                }
                            ),
                            pending_cancel["id"],
                        ),
                    )
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
        expected_birth_time: float | None = None,
        alive: bool | None = None,
        checked_at: float | None = None,
        observed_birth_time: float | None = None,
        reason: str | None = None,
    ) -> bool:
        """Apply an exact liveness or PID-reuse proof to one active agent.

        Stored PID, process group and diagnostic identity select the immutable
        supervisor row. Dead proofs may cover legacy rows; reuse proofs require
        matching expected birth evidence plus a different observed birth time.
        The method returns whether it committed ``LOST`` and never signals a
        process. Invalid, incomplete, or stale evidence raises
        :class:`ValidationError`.
        """

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
                expected_birth_time=expected_birth_time,
                alive=alive,
                checked_at=checked_at,
                observed_birth_time=observed_birth_time,
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
                data={"verdict": verdict, "observed_birth_time": observed_birth_time},
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
                   WHERE agent_id = ? AND state = 'pending'
                   ORDER BY CASE kind WHEN 'cancel' THEN 0 ELSE 1 END, id
                   LIMIT 1""",
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

    def replace_capacity_snapshot(
        self,
        *,
        runtime: str,
        scope_id: str,
        observed_at: float,
        valid_until: float,
        payload: object,
    ) -> None:
        """Atomically replace one current route and sample snapshot.

        ``scope_id`` must be nonblank, timestamps must be finite and ordered,
        and ``payload`` is bounded JSON containing the latest provider values.
        """

        capacity.replace_capacity_snapshot(
            self.connection, runtime=runtime, scope_id=scope_id,
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
